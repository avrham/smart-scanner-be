"""The external-signal webhook gateway.

This is the first INTERNET-FACING WRITE PATH in the repository. Every other
component either pulls from a provider we chose, or exposes a read-only
surface. Here an unauthenticated stranger can reach a socket that ends in an
INSERT, so the shape of this module is dictated by that one fact.

THE ORDER OF THE CHECKS IS THE DESIGN
-------------------------------------
Each stage is cheaper than the one after it, and each is refused before the
next is paid for:

    1. token            constant-time compare, no DB, no parse
    2. size             bytes, before JSON is decoded
    3. rate limit       in-process, before JSON is decoded
    4. JSON decode      bounded input only
    5. contract + shape pure validation (app/external_adapters.py)
    6. clock skew       replay window
    7. replay           body fingerprint, enforced by a UNIQUE constraint
    8. symbol scope     one bounded query
    9. INSERT           idempotency enforced by a UNIQUE constraint

A body that fails at step 1 never reaches a JSON parser; a body that fails at
step 3 never reaches the database. That ordering is what makes an unguarded
public endpoint affordable.

WHAT AN ERROR RESPONSE MAY SAY
------------------------------
A short, stable reason CODE and nothing else. No stack trace, no exception
string, no SQL, no table name, no column name, no DSN, no role and nothing
derived from the caller's token. `_safe_error` is the only way a failure
becomes a response, so the rule is enforced in one place rather than at a
dozen call sites.

WHY THE RATE LIMIT IS IN-PROCESS, AND WHY THAT IS HONEST
--------------------------------------------------------
It is a per-process sliding window, not a distributed one. The ingress runs as
a single small machine by design, so per-process IS per-deployment today. If
that ever stops being true the limit degrades to N-times-looser rather than
failing open, and the UNIQUE constraints below — not the rate limiter — are
what actually protect data integrity. Saying so here is cheaper than someone
later discovering it during an incident.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Sequence, Set, Tuple

from app.external_adapters import (
    MAX_PAYLOAD_BYTES, PayloadRejected, bound_metadata, normalize, redact,
)
from app.external_signals import (
    SOURCE_STATE_PREFIX, WEBHOOK_SOURCES, source_state_key,
)

logger = logging.getLogger(__name__)

# ---- delivery outcomes ------------------------------------------------------ #
DELIVERY_ACCEPTED = "accepted"
DELIVERY_DUPLICATE = "duplicate"
DELIVERY_REJECTED = "rejected"

# ---- symbol scope (see migration 022) --------------------------------------- #
SCOPE_UNIVERSE = "scanner_universe"
SCOPE_DISCOVERY = "external_discovery"

# ---- source state (same vocabulary as the catalyst ingestions) -------------- #
STATE_OK = "ok"
STATE_UNAVAILABLE = "unavailable"
STATE_ERROR = "error"
STATE_NEVER_RUN = "never_run"

#: The frozen experimental universe. Read once per delivery from the immutable
#: universe tables rather than hard-coded, so the boundary in migration 022 can
#: never drift from the universe the scanner actually runs on.
SCANNER_UNIVERSE_CODE = "WYCKOFF-HISTORY-WARMUP-QUALIFICATION"


class IngressRejected(Exception):
    """A request refused before it could become a delivery.

    Distinct from `PayloadRejected`: that one means "we understood you and the
    content is wrong" (and IS recorded as a rejected delivery). This one means
    "we are not going to look at this at all" — bad token, oversized body,
    rate limited — and is deliberately NOT written to the database, because
    recording unauthenticated traffic would hand an anonymous caller a way to
    fill our tables.
    """

    def __init__(self, reason: str, status_code: int = 400):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


# --------------------------------------------------------------------------- #
# authentication
# --------------------------------------------------------------------------- #

def verify_ingress_token(supplied: Optional[str], expected: Optional[str]) -> bool:
    """Constant-time token comparison.

    `hmac.compare_digest` rather than `==` so response timing cannot be used to
    recover the token one character at a time. An unset expected token returns
    False — the endpoint fails CLOSED, and a deployment that forgot to set the
    secret rejects everything instead of accepting everyone.
    """
    if not expected:
        return False
    if not supplied:
        return False
    return hmac.compare_digest(str(supplied), str(expected))


# --------------------------------------------------------------------------- #
# rate limiting
# --------------------------------------------------------------------------- #

class SlidingWindowLimiter:
    """A bounded per-key sliding window over a monotonic clock.

    Uses `time.monotonic()` so an NTP correction or a DST change cannot
    accidentally widen the window. Memory is bounded by construction: each
    key's deque never holds more than `limit` timestamps, and keys come from a
    closed set (the registry's webhook sources plus one global key), so an
    attacker cannot grow this map by varying anything they control.
    """

    def __init__(self, limit: int, window_seconds: float = 60.0):
        self.limit = max(1, int(limit))
        self.window_seconds = float(window_seconds)
        self._hits: Dict[str, Deque[float]] = {}

    def allow(self, key: str, *, now: Optional[float] = None) -> bool:
        moment = time.monotonic() if now is None else now
        hits = self._hits.setdefault(key, deque())
        cutoff = moment - self.window_seconds
        while hits and hits[0] <= cutoff:
            hits.popleft()
        if len(hits) >= self.limit:
            return False
        hits.append(moment)
        return True

    def reset(self) -> None:
        self._hits.clear()


# --------------------------------------------------------------------------- #
# fingerprints
# --------------------------------------------------------------------------- #

def body_fingerprint(raw_body: bytes) -> str:
    """SHA-256 over the EXACT bytes received.

    Deliberately over the bytes and not over the decoded object: two payloads
    that differ only in key order or whitespace are different deliveries from
    the source's point of view, and collapsing them would hide a sender that
    changed its serialisation.
    """
    return hashlib.sha256(raw_body).hexdigest()


def idempotency_key(signal: Dict[str, Any], *, fingerprint: str) -> str:
    """A deterministic identity for one OBSERVATION.

    Three cases, in order, each chosen because of what it makes possible:

      1. the source supplied its own id  -> use it. The source is the authority
         on whether two alerts are the same alert.
      2. the source supplied a timestamp -> the semantic tuple plus that
         timestamp. This is what makes "repeated same-state alerts" behave
         correctly: the same state fired an hour later is a NEW observation
         (different timestamp, different key) rather than a suppressed
         duplicate, while a retried delivery of the same firing collapses.
      3. neither                          -> the body fingerprint, so
         idempotency degrades exactly into replay protection instead of
         silently admitting duplicates.
    """
    source = signal.get("source") or ""
    source_signal_id = signal.get("source_signal_id")
    if source_signal_id:
        material = f"{source}|id|{source_signal_id}"
    elif isinstance(signal.get("observed_at"), datetime):
        material = "|".join([
            source, "sem",
            str(signal.get("symbol") or ""),
            str(signal.get("timeframe") or ""),
            str(signal.get("signal_type") or ""),
            str(signal.get("direction") or ""),
            str(signal.get("indicator") or ""),
            str(signal.get("alert_id") or ""),
            signal["observed_at"].astimezone(timezone.utc).isoformat(),
        ])
    else:
        material = f"{source}|body|{fingerprint}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# persistence
# --------------------------------------------------------------------------- #

UNIVERSE_SQL = """
SELECT s.symbol
FROM public.history_warmup_universe_symbols s
JOIN public.history_warmup_universes u ON u.id = s.universe_id
WHERE u.universe_code = $1
"""

INSERT_DELIVERY_SQL = """
INSERT INTO public.external_signal_deliveries (
    source, transport, received_at, body_fingerprint, payload_bytes,
    status, rejection_reason, raw_payload, signal_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (source, body_fingerprint) DO NOTHING
RETURNING id
"""

INSERT_SIGNAL_SQL = """
INSERT INTO public.external_signals (
    source, delivery_id, source_signal_id, symbol, symbol_scope,
    observed_at, received_at, effective_at, clock_skew_seconds,
    timeframe, timeframe_normalized, signal_type, signal_type_normalized,
    direction, direction_normalized, confidence, confidence_scale,
    indicator, indicator_version, alert_id, contract_version,
    source_payload_version, source_metadata, supersedes_signal_id,
    idempotency_key)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
        $20,$21,$22,$23::jsonb,$24,$25)
ON CONFLICT (source, idempotency_key) DO NOTHING
RETURNING id
"""

SOURCE_STATE_SQL = """
INSERT INTO public.catalyst_source_state (
    source, status, last_refresh_at, last_success_at,
    symbols_covered, events_upserted, detail, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
ON CONFLICT (source) DO UPDATE SET
    status = EXCLUDED.status,
    last_refresh_at = EXCLUDED.last_refresh_at,
    last_success_at = COALESCE(EXCLUDED.last_success_at,
                               catalyst_source_state.last_success_at),
    symbols_covered = EXCLUDED.symbols_covered,
    events_upserted = catalyst_source_state.events_upserted
                      + EXCLUDED.events_upserted,
    detail = EXCLUDED.detail,
    updated_at = NOW()
"""


async def fetch_scanner_universe(conn) -> Set[str]:
    """The frozen experimental universe, uppercased.

    A failure here must NOT reject the delivery: losing the universe read means
    we cannot classify scope, not that the signal is invalid. The caller
    degrades to `external_discovery`, which is the conservative direction — a
    signal wrongly marked research-only is invisible, whereas one wrongly
    marked in-universe would enter the experiment's surface.
    """
    rows = await conn.fetch(UNIVERSE_SQL, SCANNER_UNIVERSE_CODE)
    return {str(r["symbol"]).strip().upper() for r in rows if r["symbol"]}


def classify_symbol_scope(symbol: str, universe: Optional[Set[str]]) -> str:
    """Which side of the experiment boundary this symbol falls on."""
    if not universe:
        return SCOPE_DISCOVERY
    return (SCOPE_UNIVERSE if symbol.upper() in universe else SCOPE_DISCOVERY)


async def record_delivery(conn, *, source: str, received_at: datetime,
                          fingerprint: str, payload_bytes: int, status: str,
                          rejection_reason: Optional[str] = None,
                          raw_payload: Optional[Dict[str, Any]] = None,
                          signal_count: int = 0,
                          transport: str = "webhook") -> Optional[str]:
    """Write the delivery audit row. Returns its id, or None if it is a replay.

    `ON CONFLICT DO NOTHING` returning no row IS the replay detection: the
    UNIQUE constraint on (source, body_fingerprint) is the authority, not a
    prior SELECT, so two concurrent identical deliveries cannot both win.
    """
    encoded = None
    if raw_payload is not None:
        try:
            encoded = json.dumps(redact(raw_payload), default=str)
        except (TypeError, ValueError):
            encoded = None
    row = await conn.fetchrow(
        INSERT_DELIVERY_SQL, source, transport, received_at, fingerprint,
        payload_bytes, status, rejection_reason, encoded, signal_count)
    return str(row["id"]) if row else None


async def insert_signal(conn, signal: Dict[str, Any], *, delivery_id: str,
                        symbol_scope: str, key: str) -> Optional[str]:
    """Append one signal. Returns its id, or None when it was a duplicate.

    There is no UPDATE branch and there will not be one: `external_signals` is
    append-only by design (migration 022), which is what allows the ingest role
    to hold INSERT here and no UPDATE or DELETE on any external table.
    """
    row = await conn.fetchrow(
        INSERT_SIGNAL_SQL,
        signal["source"], delivery_id, signal.get("source_signal_id"),
        signal["symbol"], symbol_scope,
        signal.get("observed_at"), signal["received_at"], signal["effective_at"],
        signal.get("clock_skew_seconds"),
        signal.get("timeframe"), signal.get("timeframe_normalized"),
        signal["signal_type"], signal["signal_type_normalized"],
        signal.get("direction"), signal["direction_normalized"],
        signal.get("confidence"), signal.get("confidence_scale"),
        signal.get("indicator"), signal.get("indicator_version"),
        signal.get("alert_id"), signal["contract_version"],
        signal.get("source_payload_version"),
        json.dumps(signal.get("source_metadata") or {}, default=str),
        signal.get("supersedes_signal_id"), key)
    return str(row["id"]) if row else None


async def record_source_state(conn, source: str, status: str, *,
                              signals_written: int = 0, detail: str = "",
                              now: Optional[datetime] = None) -> None:
    """One row per external source in `catalyst_source_state`.

    Without this an empty `external_signals` table is unreadable: "nobody has
    connected TradingView", "the indicator has not fired this week" and "the
    gateway is broken" all look identical, and only the third is a problem.

    `events_upserted` ACCUMULATES here rather than being overwritten (unlike
    the pulled sources, where each refresh reports that run's count). A pushed
    delivery carries one signal, so a per-delivery count would always read 1
    and tell nobody anything; the running total answers "has this source ever
    actually delivered".
    """
    moment = now or datetime.now(timezone.utc)
    await conn.execute(
        SOURCE_STATE_SQL, source_state_key(source), status, moment,
        moment if status == STATE_OK else None,
        0, signals_written, detail[:400] or None)


# --------------------------------------------------------------------------- #
# the gateway
# --------------------------------------------------------------------------- #

def decode_body(raw_body: bytes, *,
                max_bytes: int = MAX_PAYLOAD_BYTES) -> Dict[str, Any]:
    """Size-check, then decode. In that order, always.

    The size check precedes the parse so a hostile body is refused before any
    parser touches it, and the parse is wrapped so a malformed body produces a
    stable code rather than a decoder's exception text.
    """
    if len(raw_body) > max_bytes:
        raise IngressRejected("payload_too_large", status_code=413)
    if not raw_body.strip():
        raise IngressRejected("empty_payload", status_code=400)
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise IngressRejected("malformed_json", status_code=400)
    if not isinstance(payload, dict):
        raise IngressRejected("payload_not_object", status_code=400)
    return payload


async def ingest_delivery(conn, raw_body: bytes, *,
                          received_at: Optional[datetime] = None,
                          max_bytes: int = MAX_PAYLOAD_BYTES,
                          ) -> Dict[str, Any]:
    """One authenticated, rate-limited delivery -> a bounded result summary.

    Authentication and rate limiting happen in the ROUTER, before this is
    called, so this function's contract is "the caller is allowed to be here".
    Everything after that point is recorded, including rejections — a gateway
    that logs only its successes cannot answer "the alert fired, why is
    nothing showing?", which is the question that will actually be asked.

    Never raises for a bad payload. A caller error is a RESULT, not an
    exception, so one malformed alert can never take the endpoint down.
    """
    moment = received_at or datetime.now(timezone.utc)
    payload = decode_body(raw_body, max_bytes=max_bytes)
    fingerprint = body_fingerprint(raw_body)
    payload_bytes = len(raw_body)

    # The declared source decides which state row a failure is reported
    # against. Validated against the closed set so an unknown value cannot
    # create an arbitrary `catalyst_source_state` row.
    declared = str(payload.get("source") or "").strip().lower()
    source = declared if declared in WEBHOOK_SOURCES else "tradingview"

    try:
        signal = normalize(payload, received_at=moment)
    except PayloadRejected as exc:
        await record_delivery(
            conn, source=source, received_at=moment, fingerprint=fingerprint,
            payload_bytes=payload_bytes, status=DELIVERY_REJECTED,
            rejection_reason=exc.reason, raw_payload=payload)
        await record_source_state(
            conn, source, STATE_ERROR,
            detail=f"rejected: {exc.reason}", now=moment)
        return {"status": DELIVERY_REJECTED, "reason": exc.reason,
                "detail": exc.detail or None, "signal_id": None}

    source = signal["source"]

    delivery_id = await record_delivery(
        conn, source=source, received_at=moment, fingerprint=fingerprint,
        payload_bytes=payload_bytes, status=DELIVERY_ACCEPTED,
        raw_payload=payload, signal_count=1)
    if delivery_id is None:
        # The UNIQUE constraint refused it: we have seen these exact bytes for
        # this source before. Idempotent by contract — the caller is told it
        # succeeded, because from its point of view it did.
        return {"status": DELIVERY_DUPLICATE, "reason": "duplicate_delivery",
                "signal_id": None}

    # Scope classification must never be able to fail the delivery. Degrading
    # to research-only is the conservative direction (see fetch_scanner_universe).
    try:
        universe = await fetch_scanner_universe(conn)
    except Exception:
        logger.warning("external ingest: universe lookup unavailable",
                       exc_info=False)
        universe = None
    scope = classify_symbol_scope(signal["symbol"], universe)

    key = idempotency_key(signal, fingerprint=fingerprint)
    signal_id = await insert_signal(conn, signal, delivery_id=delivery_id,
                                    symbol_scope=scope, key=key)

    await record_source_state(conn, source, STATE_OK,
                              signals_written=1 if signal_id else 0,
                              now=moment)

    if signal_id is None:
        # Different bytes, same observation — e.g. the source re-sent with a
        # new envelope. The delivery is genuine and is recorded; the signal is
        # not duplicated.
        return {"status": DELIVERY_DUPLICATE, "reason": "duplicate_signal",
                "signal_id": None, "delivery_id": delivery_id}

    return {"status": DELIVERY_ACCEPTED, "reason": None,
            "signal_id": signal_id, "delivery_id": delivery_id,
            "source": source, "symbol": signal["symbol"],
            "symbol_scope": scope,
            "direction": signal["direction_normalized"],
            "signal_type": signal["signal_type_normalized"],
            "timeframe": signal.get("timeframe_normalized"),
            "effective_at": signal["effective_at"].isoformat()}


def audit_log_fields(result: Dict[str, Any], *, source: str,
                     fingerprint: str, payload_bytes: int) -> Dict[str, Any]:
    """Structured, secret-free log fields for one delivery.

    Deliberately excludes the token, the raw body, the metadata object and the
    caller's address. The fingerprint is a hash, so it correlates retries in
    the logs without reproducing anything the caller sent.
    """
    return {
        "event": "external_signal_delivery",
        "source": source,
        "status": result.get("status"),
        "reason": result.get("reason"),
        "symbol": result.get("symbol"),
        "symbol_scope": result.get("symbol_scope"),
        "signal_type": result.get("signal_type"),
        "direction": result.get("direction"),
        "payload_bytes": payload_bytes,
        "body_fingerprint": fingerprint[:16],
    }


__all__ = [
    "DELIVERY_ACCEPTED", "DELIVERY_DUPLICATE", "DELIVERY_REJECTED",
    "SCOPE_UNIVERSE", "SCOPE_DISCOVERY", "SCANNER_UNIVERSE_CODE",
    "STATE_OK", "STATE_UNAVAILABLE", "STATE_ERROR", "STATE_NEVER_RUN",
    "SOURCE_STATE_PREFIX", "IngressRejected", "verify_ingress_token",
    "SlidingWindowLimiter", "body_fingerprint", "idempotency_key",
    "fetch_scanner_universe", "classify_symbol_scope", "record_delivery",
    "insert_signal", "record_source_state", "decode_body", "ingest_delivery",
    "audit_log_fields",
]
