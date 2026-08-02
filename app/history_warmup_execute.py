"""Bounded history-warmup EXECUTE: server-authoritative selection, validation,
idempotency identity, retry plan, failure taxonomy, and canonical daily/4H
persistence.

Design mirrors the maintenance execute path (advisory lock + persisted cooldown
+ server-selected batch + durable pre-provider run marker), but scoped to the
history-warmer role and the local daily_bars / market_bars_4h stores. This
module is import-safe: it constructs NO provider and performs NO network access.
The provider is injected by the caller (production via the existing
`get_market_data_provider()` abstraction; tests via a deterministic fake).

Canonical 4H persistence (migration 014, Option A): provider-native ADJUSTED
bars are persisted with a deterministically computed bar_end, session_date
(America/New_York date of bar_end), is_completed, is_regular_session and
content_fingerprint. Only COMPLETED bars are stored (the currently-forming
bucket is excluded and counted); invalid provider rows produce a bounded safe
failure, never a coerced row.
"""

from __future__ import annotations

import hashlib
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# Canonical daily upsert SQL reused verbatim (no second incompatible impl).
from app.workers.market_store import UPSERT_DAILY_BAR_SQL

EXECUTE_CONTRACT_VERSION = "history_warmup_execute.v2"
PREFLIGHT_V2_CONTRACT_VERSION = "history_warmup_preflight.v2"
PREFLIGHT_V3_CONTRACT_VERSION = "history_warmup_preflight.v3"
EXECUTE_RESULT_CONTRACT_VERSION = "history_warmup_execute_result.v1"
UNIVERSE_CREATE_CONTRACT_VERSION = "history_warmup_universe_create.v1"

MODE_NORMAL = "normal"
MODE_RETRY = "retry"

# Provider-activity lifecycle (durable, fail-closed cooldown source).
PROVIDER_ACTIVITY_NONE = "none"
PROVIDER_ACTIVITY_STARTED = "started"
PROVIDER_ACTIVITY_COMPLETED = "completed"

# Universe lifecycle.
UNIVERSE_DRAFT = "draft"
UNIVERSE_FROZEN = "frozen"
UNIVERSE_SUPERSEDED = "superseded"

# Distinct fixed advisory-lock key ('WRMU'); only ONE warmup execution may hold
# it at a time (single-Machine, single-process assumption). Session-scoped on the
# request connection, always released in `finally`.
HISTORY_WARMUP_ADVISORY_LOCK_KEY = 0x57524D55  # 1465341013

EXCHANGE_TZ = "America/New_York"
BAR_DURATION_HOURS = 4.0
PROVIDER = "massive"
PROVIDER_ADJUSTMENT = "split_dividend_adjusted"

# Request fields a caller may NEVER supply (broadening / unsafe selectors).
FORBIDDEN_REQUEST_FIELDS = (
    "provider", "provider_options", "pacing", "spacing", "retry_count",
    "retries", "table", "table_name", "adjustment", "adjustment_mode",
    "from_date", "to_date", "start", "end", "date_range", "timeseries",
    "run_id", "batch_index", "pending", "background", "run_in_background",
)

# ---- failure taxonomy ------------------------------------------------------ #
RETRYABLE = "retryable"
TERMINAL = "terminal"
OPERATOR_ERROR = "operator_error"

FAILURE_TAXONOMY: Dict[str, str] = {
    "provider_rate_limited": RETRYABLE,
    "provider_unavailable": RETRYABLE,
    "provider_timeout": RETRYABLE,
    "provider_auth_error": OPERATOR_ERROR,
    "provider_invalid_payload": TERMINAL,
    "daily_persistence_error": TERMINAL,
    "four_hour_persistence_error": TERMINAL,
    "stale_manifest": OPERATOR_ERROR,
    "stale_retry_plan": OPERATOR_ERROR,
    "stale_next_batch": OPERATOR_ERROR,
    "history_warmup_execution_locked": OPERATOR_ERROR,
    "provider_cooldown_active": OPERATOR_ERROR,
}


def error_class(code: str) -> str:
    return FAILURE_TAXONOMY.get(code, TERMINAL)


def is_retryable(code: str) -> bool:
    return error_class(code) == RETRYABLE


class HistoryWarmupPayloadError(ValueError):
    """A provider bar that cannot be safely persisted. Carries a bounded reason
    code — never a raw payload."""

    def __init__(self, code: str, detail: Optional[str] = None):
        self.code = code
        self.detail = detail
        super().__init__(f"{code}" + (f": {detail}" if detail else ""))


def map_provider_error(exc: BaseException) -> Tuple[str, str]:
    """Map a provider/persistence exception to a bounded (safe_code, class).
    Never returns a raw message or trace."""
    name = type(exc).__name__
    status = getattr(exc, "status_code", None)
    if name == "MassiveApiError":
        if status == 429:
            return "provider_rate_limited", RETRYABLE
        if status in (401, 403):
            return "provider_auth_error", OPERATOR_ERROR
        msg = str(getattr(exc, "excerpt", "") or exc).lower()
        if status is None or "timeout" in msg or "timed out" in msg:
            return "provider_timeout", RETRYABLE
        return "provider_unavailable", RETRYABLE
    if name in ("ProviderConfigError", "IntradayHistoryUnsupportedError"):
        return "provider_auth_error", OPERATOR_ERROR
    if name in ("HistoryWarmupPayloadError", "FourHourFrameRejection"):
        return "provider_invalid_payload", TERMINAL
    # Unknown provider-call failure: retryable (bounded — retries are MANUAL,
    # operator-triggered, never automatic beyond the client's own retry budget).
    return "provider_unavailable", RETRYABLE


def _sha(obj: Any) -> str:
    import json
    return "sha256:" + hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


# --------------------------------------------------------------------------- #
# Server-authoritative retry plan + batch selection
# --------------------------------------------------------------------------- #
def compute_retry_plan(latest_items: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Deterministic retry plan from the LATEST run item per symbol.

    `latest_items` maps symbol -> its most recent history_warmup_run_items row
    (or absent). A symbol is retryable iff its latest item failed with a
    retryable safe error; terminal iff its latest item failed terminally /
    operator_error. Ready/completed symbols never appear.
    """
    retryable: List[str] = []
    terminal: List[str] = []
    entries: List[Dict[str, Any]] = []
    for sym in sorted(latest_items):
        item = latest_items[sym]
        if (item.get("status") != "failed"):
            continue
        code = item.get("error_code")
        klass = item.get("error_class") or error_class(code or "")
        if klass == RETRYABLE and bool(item.get("retryable")):
            retryable.append(sym)
            entries.append({"symbol": sym, "error_code": code,
                            "error_class": klass, "attempt": item.get("attempt")})
        else:
            terminal.append(sym)
    retry_plan_hash = _sha({"retryable": sorted(retryable)})
    return {
        "retryable_symbols": sorted(retryable),
        "terminal_symbols": sorted(terminal),
        "retry_plan_hash": retry_plan_hash,
        "entries": entries,
    }


def _readiness_by_symbol(readiness: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {r["symbol"]: r for r in readiness.get("symbols", [])}


def compute_progress(readiness: Dict[str, Any],
                     retry_plan: Dict[str, Any]) -> Dict[str, Any]:
    """Split the universe into normal-pending / retryable / terminal / ready."""
    by = _readiness_by_symbol(readiness)
    retryable = set(retry_plan["retryable_symbols"])
    terminal = set(retry_plan["terminal_symbols"])
    normal_pending: List[str] = []
    for sym in sorted(by):
        if sym in retryable or sym in terminal:
            continue
        if not by[sym].get("both_ready"):
            normal_pending.append(sym)
    return {
        "normal_pending_symbols": normal_pending,
        "retryable_symbols": sorted(retryable),
        "terminal_symbols": sorted(terminal),
        "normal_complete": len(normal_pending) == 0,
    }


def select_next_batch(readiness: Dict[str, Any], retry_plan: Dict[str, Any],
                      progress: Dict[str, Any], *, max_batch: int) -> Dict[str, Any]:
    """Server-selected next batch (<= max_batch symbols). Progression is
    RETRY-FIRST: while any retryable item exists, normal progression hard-stops
    and the next batch is the first retryable symbol; retry requires explicit
    retry mode. Otherwise the next normal-pending symbol is selected. When only
    ready/terminal symbols remain the batch is unavailable."""
    cap = max(1, int(max_batch))
    universe_hash = readiness["universe_hash"]

    def _batch(mode, plan_hash, symbols, reason=None):
        symbols = list(symbols[:cap])
        available = bool(symbols)
        # per-symbol provider budget: 1 daily + 1 4H (benchmarks handled by the
        # readiness of shared symbols, not per-execute; kept explicit + bounded).
        daily_required = available
        four_hour_required = available
        est = (1 + 1) * len(symbols) if available else 0
        nb = {
            "available": available,
            "mode": mode,
            "reason": reason,
            "symbol_count": len(symbols),
            "symbols": symbols,
            "daily_required": daily_required,
            "four_hour_required": four_hour_required,
            "estimated_provider_requests": est,
        }
        nb["next_batch_hash"] = _sha({
            "mode": mode, "universe_hash": universe_hash,
            "plan_hash": plan_hash, "symbols": symbols})
        return nb

    if retry_plan["retryable_symbols"]:
        return _batch(MODE_RETRY, retry_plan["retry_plan_hash"],
                      retry_plan["retryable_symbols"])
    if progress["normal_pending_symbols"]:
        return _batch(MODE_NORMAL, readiness["combined_readiness_manifest_hash"],
                      progress["normal_pending_symbols"])
    reason = ("only_terminal_symbols_remain"
              if retry_plan["terminal_symbols"] else "all_symbols_launch_ready")
    return _batch(MODE_NORMAL, readiness["combined_readiness_manifest_hash"], [],
                  reason=reason)


def build_preflight_v2(readiness: Dict[str, Any],
                       latest_items: Dict[str, Dict[str, Any]],
                       cooldown: Dict[str, Any], *, max_batch: int) -> Dict[str, Any]:
    """history_warmup_preflight.v2: readiness v2 + server-selected next batch +
    retry plan + cooldown. Provider-free."""
    retry_plan = compute_retry_plan(latest_items)
    progress = compute_progress(readiness, retry_plan)
    next_batch = select_next_batch(readiness, retry_plan, progress, max_batch=max_batch)
    allowed = bool(cooldown.get("execution_allowed_by_cooldown"))
    return {
        "contract_version": PREFLIGHT_V2_CONTRACT_VERSION,
        "provider_called": False,
        "provider_constructed": False,
        "universe_hash": readiness["universe_hash"],
        "config_hash": readiness["config_hash"],
        "combined_readiness_manifest_hash": readiness["combined_readiness_manifest_hash"],
        "daily_manifest_hash": readiness["daily_manifest_hash"],
        "four_hour_manifest_hash": readiness["four_hour_manifest_hash"],
        "normal_pending_symbols": progress["normal_pending_symbols"],
        "retryable_symbols": progress["retryable_symbols"],
        "terminal_symbols": progress["terminal_symbols"],
        "normal_complete": progress["normal_complete"],
        "retry_plan_hash": retry_plan["retry_plan_hash"],
        "retry_plan_entries": retry_plan["entries"],
        "execution_allowed_by_cooldown": allowed,
        "next_execution_not_before": cooldown.get("next_execution_not_before"),
        "cooldown_remaining_seconds": cooldown.get("cooldown_remaining_seconds"),
        "next_batch": next_batch,
        "readiness": readiness,
        "max_symbols_per_batch": int(max_batch),
    }


def build_preflight_v3(*, universe: Dict[str, Any], readiness: Dict[str, Any],
                       latest_items: Dict[str, Dict[str, Any]], cooldown: Dict[str, Any],
                       execution_state: Dict[str, Any], max_batch: int) -> Dict[str, Any]:
    """history_warmup_preflight.v3: frozen-universe identity + readiness v2
    four-state + server-selected next batch + retry plan + provider-activity
    cooldown + active/abandoned execution state. Provider-free."""
    retry_plan = compute_retry_plan(latest_items)
    progress = compute_progress(readiness, retry_plan)
    next_batch = select_next_batch(readiness, retry_plan, progress, max_batch=max_batch)
    return {
        "contract_version": PREFLIGHT_V3_CONTRACT_VERSION,
        "provider_called": False,
        "provider_constructed": False,
        "executable": True,
        "universe_id": universe["universe_id"],
        "universe_code": universe["universe_code"],
        "universe_version": universe["universe_version"],
        "universe_hash": universe["universe_hash"],
        "universe_status": universe["status"],
        "symbol_count": universe["symbol_count"],
        "config_hash": readiness["config_hash"],
        "combined_readiness_manifest_hash": readiness["combined_readiness_manifest_hash"],
        "daily_manifest_hash": readiness["daily_manifest_hash"],
        "four_hour_manifest_hash": readiness["four_hour_manifest_hash"],
        "normal_pending_symbols": progress["normal_pending_symbols"],
        "retryable_symbols": progress["retryable_symbols"],
        "terminal_symbols": progress["terminal_symbols"],
        "normal_complete": progress["normal_complete"],
        "retry_plan_hash": retry_plan["retry_plan_hash"],
        "retry_plan_entries": retry_plan["entries"],
        "execution_allowed_by_cooldown": bool(cooldown.get("execution_allowed_by_cooldown")),
        "next_execution_not_before": cooldown.get("next_execution_not_before"),
        "cooldown_remaining_seconds": cooldown.get("cooldown_remaining_seconds"),
        "execution_state": execution_state,
        "next_batch": next_batch,
        "readiness": readiness,
        "max_symbols_per_batch": int(max_batch),
    }


# --------------------------------------------------------------------------- #
# Idempotency identity + request validation
# --------------------------------------------------------------------------- #
def execution_identity(*, mode: str, universe_hash: str, config_hash: str,
                       plan_hash: str, next_batch_hash: str,
                       symbols: List[str], universe_id: Optional[str] = None) -> str:
    """Deterministic idempotency identity for one execute request. Includes the
    immutable universe_id (v2) so the identity is bound to a specific frozen
    universe, not just its hash."""
    blob = "|".join([
        EXECUTE_CONTRACT_VERSION, str(mode), str(universe_id), str(universe_hash),
        str(config_hash), str(plan_hash), str(next_batch_hash),
        ",".join(sorted(str(s) for s in symbols)),
    ])
    return "hwx:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Frozen-universe identity
# --------------------------------------------------------------------------- #
class UniverseError(ValueError):
    """Bounded, safe universe validation failure (no secrets, no payloads)."""

    def __init__(self, code: str, detail: Optional[str] = None):
        self.code = code
        self.detail = detail
        super().__init__(f"{code}" + (f": {detail}" if detail else ""))


def normalize_universe_symbols(raw: Any, *, max_symbols: int) -> Dict[str, Any]:
    """Upper/strip/dedupe (first-seen order) a requested symbol list. Returns
    {symbols, duplicates_removed, invalid}. Raises UniverseError on empty / over
    cap / malformed entries."""
    if not isinstance(raw, list) or not raw:
        raise UniverseError("symbols_required")
    seen: set = set()
    out: List[str] = []
    dupes = 0
    invalid: List[str] = []
    for s in raw:
        if not isinstance(s, str):
            invalid.append(str(s)[:16]); continue
        v = s.strip().upper()
        if not v or not v.isascii() or len(v) > 16 or any(c.isspace() for c in v):
            invalid.append(v[:16]); continue
        if v in seen:
            dupes += 1; continue
        seen.add(v); out.append(v)
    if invalid:
        raise UniverseError("invalid_symbols", str(sorted(set(invalid))[:10]))
    if not out:
        raise UniverseError("symbols_required")
    if len(out) > int(max_symbols):
        raise UniverseError("too_many_symbols", f"{len(out)}>{max_symbols}")
    return {"symbols": out, "duplicates_removed": dupes}


def compute_universe_hash(*, universe_code: str, universe_version: int,
                          symbols_in_ordinal_order: List[str]) -> str:
    """Deterministic frozen-universe hash over code + version + ORDERED members."""
    return _sha({"contract": UNIVERSE_CREATE_CONTRACT_VERSION,
                 "code": str(universe_code), "version": int(universe_version),
                 "symbols": [str(s) for s in symbols_in_ordinal_order]})


def provider_activity_reference(run: Optional[Dict[str, Any]]):
    """The provider-activity timestamp that establishes cooldown, or None when
    the run had NO provider activity (a pre-provider crash never blocks)."""
    if not run or run.get("provider_activity_state") in (None, PROVIDER_ACTIVITY_NONE):
        return None
    return run.get("last_provider_activity_at") or run.get("provider_activity_started_at")


def _fail(reason: str) -> Dict[str, Any]:
    return {"ok": False, "reason": reason}


def validate_execute_request(body: Any, preflight: Dict[str, Any], *,
                             max_batch: int) -> Dict[str, Any]:
    """Validate a history_warmup_execute.v1 request against the LIVE preflight.

    The server, never the client, chose the batch: the requested symbols must
    EXACTLY equal the current server-selected next batch, mode must match, and
    every identity hash must match the freshly recomputed values. Returns a
    verdict {ok, reason, mode, symbols, batch_identity, execution_identity,
    plan_hash}."""
    if not isinstance(body, dict):
        return _fail("body_must_be_object")
    present = [f for f in FORBIDDEN_REQUEST_FIELDS if body.get(f) is not None]
    if present:
        return _fail(f"forbidden_request_fields:{sorted(present)}")
    if body.get("contract_version") != EXECUTE_CONTRACT_VERSION:
        return _fail("bad_contract_version")
    mode = body.get("mode")
    if mode not in (MODE_NORMAL, MODE_RETRY):
        return _fail("bad_mode")

    nb = preflight["next_batch"]
    if not nb.get("available"):
        return _fail("no_next_batch")
    if mode != nb["mode"]:
        return _fail("mode_not_current_batch")

    # v2: the immutable universe identity must match the loaded frozen universe
    pf_universe_id = preflight.get("universe_id")
    if pf_universe_id is not None:
        if str(body.get("universe_id")) != str(pf_universe_id):
            return _fail("universe_id_mismatch")

    # identity hashes must match the freshly recomputed plan
    if body.get("universe_hash") != preflight["universe_hash"]:
        return _fail("stale_universe_hash")
    if body.get("config_hash") != preflight["config_hash"]:
        return _fail("stale_config_hash")
    if mode == MODE_NORMAL:
        if body.get("readiness_manifest_hash") != preflight["combined_readiness_manifest_hash"]:
            return _fail("stale_manifest")
        plan_hash = preflight["combined_readiness_manifest_hash"]
    else:
        if body.get("retry_plan_hash") != preflight["retry_plan_hash"]:
            return _fail("stale_retry_plan")
        plan_hash = preflight["retry_plan_hash"]
    if body.get("next_batch_hash") != nb["next_batch_hash"]:
        return _fail("stale_next_batch")

    symbols = body.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        return _fail("symbols_required")
    symbols = [str(s).strip().upper() for s in symbols]
    if len(symbols) != len(set(symbols)):
        return _fail("duplicate_symbols")
    if not (1 <= len(symbols) <= max(1, int(max_batch))):
        return _fail("batch_size_out_of_range")
    if body.get("limit") != len(symbols):
        return _fail("limit_must_equal_symbol_count")
    if symbols != list(nb["symbols"]):
        return _fail("symbols_not_server_selected_batch")

    ident = execution_identity(
        mode=mode, universe_id=pf_universe_id, universe_hash=preflight["universe_hash"],
        config_hash=preflight["config_hash"], plan_hash=plan_hash,
        next_batch_hash=nb["next_batch_hash"], symbols=symbols)
    return {"ok": True, "reason": None, "mode": mode, "symbols": symbols,
            "plan_hash": plan_hash, "batch_identity": nb["next_batch_hash"],
            "execution_identity": ident}


# --------------------------------------------------------------------------- #
# Bar normalization + persistence (canonical)
# --------------------------------------------------------------------------- #
def normalize_daily_bars(raw_bars: List[Dict[str, Any]], *, now: datetime
                         ) -> List[Dict[str, Any]]:
    """Filter provider daily bars to COMPLETED sessions (trading_date strictly
    before today UTC). Bars arrive already canonical from provider.get_daily_bars
    ({symbol, trading_date(date), open, high, low, close, volume, ...})."""
    today = now.astimezone(timezone.utc).date()
    out: List[Dict[str, Any]] = []
    for b in raw_bars or []:
        td = b.get("trading_date")
        if isinstance(td, str):
            td = date.fromisoformat(td[:10])
        if td is None or td >= today:
            continue  # not a completed session
        out.append({**b, "trading_date": td, "symbol": str(b["symbol"]).upper()})
    return out


def _content_fingerprint(o: float, h: float, l: float, c: float, v: float) -> str:
    return "sha256:" + hashlib.sha256(
        f"{o!r}|{h!r}|{l!r}|{c!r}|{v!r}".encode()).hexdigest()


def _is_regular_session(bar_start_utc: datetime) -> bool:
    et = bar_start_utc.astimezone(ZoneInfo(EXCHANGE_TZ))
    minutes = et.hour * 60 + et.minute
    return (9 * 60 + 30) <= minutes < (16 * 60)


def normalize_4h_bars(payload: Optional[Dict[str, Any]], *, symbol: str,
                      now: datetime) -> List[Dict[str, Any]]:
    """Normalize provider-native intraday bars into COMPLETED canonical 4H rows.

    Validates OHLC/volume/timestamps; computes bar_end, session_date (ET of
    bar_end), is_completed (True for stored rows), is_regular_session and
    content_fingerprint. Only completed bars (bar_end <= now) are returned; the
    forming bucket is excluded. Raises HistoryWarmupPayloadError on an invalid
    provider row (never coerces)."""
    now = now.astimezone(timezone.utc)
    raw_bars = (payload or {}).get("bars") or []
    span = timedelta(hours=BAR_DURATION_HOURS)
    seen_starts: set = set()
    rows: List[Dict[str, Any]] = []
    for i, raw in enumerate(raw_bars):
        start = raw.get("start_utc")
        if isinstance(start, datetime):
            if start.tzinfo is None:
                raise HistoryWarmupPayloadError("naive_timestamp", f"row {i}")
            start_dt = start.astimezone(timezone.utc)
        else:
            try:
                parsed = datetime.fromisoformat(str(start))
            except (ValueError, TypeError):
                raise HistoryWarmupPayloadError("unparseable_timestamp", f"row {i}")
            if parsed.tzinfo is None:
                raise HistoryWarmupPayloadError("naive_timestamp", f"row {i}")
            start_dt = parsed.astimezone(timezone.utc)

        if start_dt in seen_starts:
            raise HistoryWarmupPayloadError("duplicate_bar_start", start_dt.isoformat())
        seen_starts.add(start_dt)

        vals = {}
        for f in ("open", "high", "low", "close"):
            x = raw.get(f)
            if x is None or isinstance(x, bool):
                raise HistoryWarmupPayloadError("malformed_ohlc", f"row {i} {f}")
            try:
                fx = float(x)
            except (TypeError, ValueError):
                raise HistoryWarmupPayloadError("malformed_ohlc", f"row {i} {f}")
            if not math.isfinite(fx) or fx < 0:
                raise HistoryWarmupPayloadError("malformed_ohlc", f"row {i} {f}")
            vals[f] = fx
        o, h, l, c = vals["open"], vals["high"], vals["low"], vals["close"]
        if not (h >= l and h >= o and h >= c and l <= o and l <= c):
            raise HistoryWarmupPayloadError("ohlc_envelope", f"row {i}")
        vol = raw.get("volume")
        if vol is None:
            raise HistoryWarmupPayloadError("missing_volume", f"row {i}")
        try:
            v = float(vol)
        except (TypeError, ValueError):
            raise HistoryWarmupPayloadError("malformed_volume", f"row {i}")
        if not math.isfinite(v) or v < 0:
            raise HistoryWarmupPayloadError("malformed_volume", f"row {i}")

        bar_end = start_dt + span
        if bar_end > now:
            continue  # currently-forming / future bucket — not completed, skip
        session_date = bar_end.astimezone(ZoneInfo(EXCHANGE_TZ)).date()
        rows.append({
            "symbol": symbol.upper(), "bar_start": start_dt, "bar_end": bar_end,
            "session_date": session_date, "open": o, "high": h, "low": l, "close": c,
            "volume": v, "is_completed": True,
            "is_regular_session": _is_regular_session(start_dt),
            "provider": PROVIDER, "provider_adjustment": PROVIDER_ADJUSTMENT,
            "source_timestamp": now, "content_fingerprint": _content_fingerprint(o, h, l, c, v),
        })
    rows.sort(key=lambda r: r["bar_start"])
    return rows


_UPSERT_4H_SQL = """
INSERT INTO public.market_bars_4h(
  symbol, bar_start, bar_end, session_date, open, high, low, close, volume,
  is_completed, is_regular_session, provider, provider_adjustment,
  source_timestamp, content_fingerprint)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (symbol, bar_start, provider, provider_adjustment) DO UPDATE SET
  bar_end = EXCLUDED.bar_end, session_date = EXCLUDED.session_date,
  open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
  close = EXCLUDED.close, volume = EXCLUDED.volume,
  is_completed = EXCLUDED.is_completed,
  is_regular_session = EXCLUDED.is_regular_session,
  source_timestamp = EXCLUDED.source_timestamp,
  content_fingerprint = EXCLUDED.content_fingerprint, updated_at = NOW()
WHERE public.market_bars_4h.content_fingerprint IS DISTINCT FROM EXCLUDED.content_fingerprint
"""


async def upsert_daily_bars(conn, bars: List[Dict[str, Any]], *, source: str = PROVIDER
                            ) -> Dict[str, int]:
    """Canonical daily upsert (reuses UPSERT_DAILY_BAR_SQL) on the WARMER conn
    with inserted/updated/unchanged telemetry via a pre-read compare."""
    if not bars:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "completed_count": 0}
    syms = sorted({b["symbol"] for b in bars})
    dates = [b["trading_date"] for b in bars]
    existing = {}
    rows = await conn.fetch(
        "SELECT symbol, trading_date, open, high, low, close, volume "
        "FROM daily_bars WHERE symbol = ANY($1::text[]) AND trading_date = ANY($2::date[])",
        syms, dates)
    for r in rows:
        existing[(r["symbol"], r["trading_date"])] = (
            float(r["open"]), float(r["high"]), float(r["low"]),
            float(r["close"]), float(r["volume"]))
    ins = upd = unch = 0
    for b in bars:
        key = (b["symbol"], b["trading_date"])
        cur = (float(b["open"]), float(b["high"]), float(b["low"]),
               float(b["close"]), float(b["volume"]))
        if key not in existing:
            ins += 1
        elif existing[key] == cur:
            unch += 1
        else:
            upd += 1
        await conn.execute(
            UPSERT_DAILY_BAR_SQL, b["symbol"], b["trading_date"], b["open"],
            b["high"], b["low"], b["close"], b["volume"], b.get("vwap"),
            b.get("transaction_count"), source)
    completed = await conn.fetchval(
        "SELECT COUNT(*)::int FROM daily_bars WHERE symbol = ANY($1::text[])", syms)
    return {"inserted": ins, "updated": upd, "unchanged": unch,
            "completed_count": int(completed or 0)}


async def upsert_4h_bars(conn, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Canonical 4H upsert (fingerprint-guarded) with telemetry."""
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "completed_count": 0}
    syms = sorted({r["symbol"] for r in rows})
    starts = [r["bar_start"] for r in rows]
    existing = {}
    prev = await conn.fetch(
        "SELECT symbol, bar_start, provider, provider_adjustment, content_fingerprint "
        "FROM market_bars_4h WHERE symbol = ANY($1::text[]) AND bar_start = ANY($2::timestamptz[])",
        syms, starts)
    for r in prev:
        existing[(r["symbol"], r["bar_start"], r["provider"], r["provider_adjustment"])] = \
            r["content_fingerprint"]
    ins = upd = unch = 0
    for r in rows:
        key = (r["symbol"], r["bar_start"], r["provider"], r["provider_adjustment"])
        if key not in existing:
            ins += 1
        elif existing[key] == r["content_fingerprint"]:
            unch += 1
        else:
            upd += 1
        await conn.execute(
            _UPSERT_4H_SQL, r["symbol"], r["bar_start"], r["bar_end"], r["session_date"],
            r["open"], r["high"], r["low"], r["close"], r["volume"], r["is_completed"],
            r["is_regular_session"], r["provider"], r["provider_adjustment"],
            r["source_timestamp"], r["content_fingerprint"])
    completed = await conn.fetchval(
        "SELECT COUNT(*) FILTER (WHERE is_completed)::int FROM market_bars_4h "
        "WHERE symbol = ANY($1::text[])", syms)
    return {"inserted": ins, "updated": upd, "unchanged": unch,
            "completed_count": int(completed or 0)}


# ============================================================================ #
# Incremental daily-history refresh (history_incremental_refresh.v1)
# ============================================================================ #
# A DISTINCT mode from initial warmup (MODE_NORMAL/MODE_RETRY, which ensure
# sufficient historical DEPTH once and then stop — see select_next_batch's
# "all_symbols_launch_ready" terminal state). Incremental refresh applies even
# when a symbol is already launch_ready (`both_ready`/history_depth_ready):
# it finds each symbol's latest LOCAL completed daily bar, resolves the latest
# safely-completed US session via the SAME existing policy
# (app.prospective_session.resolve_latest_completed_session — never local
# calendar-day subtraction, never an assumed-current-date session), and
# fetches ONLY the bounded gap of missing trading sessions in between — never
# an unlimited backfill, never the still-forming current session.
#
# Bookkeeping deliberately reuses `history_warmup_runs` (mode='incremental')
# ONLY — never `history_warmup_run_items`, whose `mode` column has a hard
# CHECK constraint (`IN ('normal','retry')`, migration 015) that would reject
# an incremental value. `history_warmup_runs.mode` has no such constraint
# (free TEXT, default 'history_warmup'), and its existing single-symbol-per-
# run granularity (unchanged since migration 014) already matches the unit
# this mode needs — no migration is required.
INCREMENTAL_REFRESH_CONTRACT_VERSION = "history_incremental_refresh.v1"
INCREMENTAL_PREFLIGHT_CONTRACT_VERSION = "history_incremental_refresh_preflight.v1"
INCREMENTAL_EXECUTE_RESULT_CONTRACT_VERSION = "history_incremental_refresh_result.v1"
MODE_INCREMENTAL = "incremental"

# history_depth_ready mirrors the EXISTING both_ready/launch_ready verdict
# UNCHANGED (read from readiness v2, never recomputed here) — incremental
# freshness is a SEPARATE axis, never a redefinition of launch_ready.
STATE_HISTORY_DEPTH_READY = "history_depth_ready"
STATE_INCREMENTAL_CURRENT = "incremental_current"
STATE_INCREMENTAL_STALE = "incremental_stale"
STATE_INCREMENTAL_REFRESH_NEEDED = "incremental_refresh_needed"
STATE_INCREMENTAL_UNVERIFIABLE = "incremental_unverifiable"

_INCREMENTAL_MAX_SESSION_WALK = 400  # bounded forward-walk guard (~well over a year)


def missing_trading_sessions(latest_local_date: Optional[date],
                             target_session: Optional[date]) -> List[date]:
    """Trading sessions STRICTLY after `latest_local_date` through
    `target_session` (inclusive), using the existing is_trading_day policy
    (app.prospective_session — weekends + the fixed US holiday set, never
    reimplemented). Bounded: the gap between two real dates in this system is
    always small (days, at most a long holiday cluster)."""
    from app.prospective_session import is_trading_day
    if latest_local_date is None or target_session is None:
        return []
    if latest_local_date >= target_session:
        return []
    out: List[date] = []
    d = latest_local_date + timedelta(days=1)
    guard = 0
    while d <= target_session and guard < _INCREMENTAL_MAX_SESSION_WALK:
        if is_trading_day(d):
            out.append(d)
        d += timedelta(days=1)
        guard += 1
    return out


def classify_incremental_symbol_state(latest_local_date: Optional[date],
                                      target_session: Optional[date]) -> str:
    """Per-symbol freshness state. `target_session=None` (unresolvable) or
    `latest_local_date=None` (symbol never warmed — no baseline to refresh
    from) are BOTH unverifiable, never guessed around."""
    if target_session is None or latest_local_date is None:
        return STATE_INCREMENTAL_UNVERIFIABLE
    if latest_local_date >= target_session:
        return STATE_INCREMENTAL_CURRENT
    return STATE_INCREMENTAL_REFRESH_NEEDED


def incremental_refresh_identity(*, symbol: str, latest_local_session: Optional[date],
                                 target_completed_session: date,
                                 refresh_contract_version: str = INCREMENTAL_REFRESH_CONTRACT_VERSION
                                 ) -> str:
    """Deterministic per-(symbol, latest local session, target session)
    identity. A repeated call for the SAME identity (same symbol, same
    observed local starting point, same target) is idempotent — replay never
    creates a duplicate run. A LATER target session (more time has passed,
    resolve_latest_completed_session advanced) is a NEW valid identity, never
    a collision with a prior round."""
    blob = "|".join([
        refresh_contract_version, str(symbol).upper(),
        latest_local_session.isoformat() if latest_local_session else "none",
        target_completed_session.isoformat(),
    ])
    return "hwi:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def build_incremental_preflight(*, requested_symbol_count: int,
                                normalized_symbols: List[str], duplicates_removed: int,
                                latest_by_symbol: Dict[str, Optional[date]],
                                target_session: Optional[date],
                                depth_ready_by_symbol: Dict[str, bool],
                                cooldown: Dict[str, Any], max_batch: int,
                                provider_rate_limit_per_minute: int) -> Dict[str, Any]:
    """Read-only incremental-refresh preflight. Provider-free, no mutation.
    `depth_ready_by_symbol` is passed through UNCHANGED from the existing
    readiness v2 both_ready computation — history_depth_ready is reported,
    never recomputed or redefined here."""
    per_symbol: List[Dict[str, Any]] = []
    current: List[str] = []
    refresh_needed: List[str] = []
    unverifiable: List[str] = []
    total_missing = 0
    for sym in normalized_symbols:
        latest = latest_by_symbol.get(sym)
        state = classify_incremental_symbol_state(latest, target_session)
        missing = (missing_trading_sessions(latest, target_session)
                  if state == STATE_INCREMENTAL_REFRESH_NEEDED else [])
        per_symbol.append({
            "symbol": sym,
            "history_depth_ready": bool(depth_ready_by_symbol.get(sym, False)),
            "latest_local_daily_bar_date": latest.isoformat() if latest else None,
            "state": state,
            "missing_session_count": len(missing),
            "first_missing_session": missing[0].isoformat() if missing else None,
            "last_missing_session": missing[-1].isoformat() if missing else None,
        })
        if state == STATE_INCREMENTAL_CURRENT:
            current.append(sym)
        elif state == STATE_INCREMENTAL_REFRESH_NEEDED:
            refresh_needed.append(sym)
            total_missing += len(missing)
        else:
            unverifiable.append(sym)

    overall_state = (
        STATE_INCREMENTAL_STALE if refresh_needed
        else STATE_INCREMENTAL_UNVERIFIABLE if unverifiable
        else STATE_INCREMENTAL_CURRENT
    )
    allowed = bool(cooldown.get("execution_allowed_by_cooldown"))
    batch = refresh_needed[: max(1, int(max_batch))]
    reasons: List[str] = []
    if not refresh_needed:
        reasons.append("no_symbols_require_refresh")
    elif not allowed:
        reasons.append("provider_cooldown_active")
    return {
        "contract_version": INCREMENTAL_PREFLIGHT_CONTRACT_VERSION,
        "mode": MODE_INCREMENTAL,
        "requested_symbol_count": int(requested_symbol_count),
        "normalized_symbol_count": len(normalized_symbols),
        "duplicates_removed": int(duplicates_removed),
        "target_completed_session": target_session.isoformat() if target_session else None,
        "symbols": per_symbol,
        "symbols_current": current,
        "symbols_requiring_refresh": refresh_needed,
        "symbols_unverifiable": unverifiable,
        "state": overall_state,
        "total_missing_session_count": total_missing,
        "estimated_provider_requests": len(refresh_needed),
        "configured_provider_rate_limit_per_minute": int(provider_rate_limit_per_minute),
        "next_batch": {
            "available": bool(batch) and allowed,
            "symbols": batch,
            "symbol_count": len(batch),
        },
        "execution_allowed_by_cooldown": allowed,
        "cooldown_remaining_seconds": cooldown.get("cooldown_remaining_seconds"),
        "next_execution_not_before": cooldown.get("next_execution_not_before"),
        "unavailable_reasons": reasons,
        "provider_called": False,
        "provider_constructed": False,
    }


__all__ = [
    "EXECUTE_CONTRACT_VERSION", "PREFLIGHT_V2_CONTRACT_VERSION",
    "PREFLIGHT_V3_CONTRACT_VERSION", "EXECUTE_RESULT_CONTRACT_VERSION",
    "UNIVERSE_CREATE_CONTRACT_VERSION", "MODE_NORMAL", "MODE_RETRY",
    "PROVIDER_ACTIVITY_NONE", "PROVIDER_ACTIVITY_STARTED", "PROVIDER_ACTIVITY_COMPLETED",
    "UNIVERSE_DRAFT", "UNIVERSE_FROZEN", "UNIVERSE_SUPERSEDED",
    "HISTORY_WARMUP_ADVISORY_LOCK_KEY", "FORBIDDEN_REQUEST_FIELDS",
    "FAILURE_TAXONOMY", "RETRYABLE", "TERMINAL", "OPERATOR_ERROR",
    "error_class", "is_retryable", "map_provider_error", "HistoryWarmupPayloadError",
    "UniverseError", "normalize_universe_symbols", "compute_universe_hash",
    "provider_activity_reference", "compute_retry_plan", "compute_progress",
    "select_next_batch", "build_preflight_v2", "build_preflight_v3",
    "execution_identity", "validate_execute_request",
    "normalize_daily_bars", "normalize_4h_bars", "upsert_daily_bars", "upsert_4h_bars",
    "FOUR_HOUR_FETCH_CALENDAR_DAYS",
    "INCREMENTAL_REFRESH_CONTRACT_VERSION", "INCREMENTAL_PREFLIGHT_CONTRACT_VERSION",
    "INCREMENTAL_EXECUTE_RESULT_CONTRACT_VERSION", "MODE_INCREMENTAL",
    "STATE_HISTORY_DEPTH_READY", "STATE_INCREMENTAL_CURRENT", "STATE_INCREMENTAL_STALE",
    "STATE_INCREMENTAL_REFRESH_NEEDED", "STATE_INCREMENTAL_UNVERIFIABLE",
    "missing_trading_sessions", "classify_incremental_symbol_state",
    "incremental_refresh_identity", "build_incremental_preflight",
]

# Bounded fetch window for a symbol's 4H warmup (mirrors the shadow runner's
# frames_4h fetch window; ~30 calendar days comfortably covers the 11-bar gate).
FOUR_HOUR_FETCH_CALENDAR_DAYS = 30
# Daily warmup depth: enough completed sessions to satisfy the binding monthly-24
# gate (~504) + control 200; a bounded calendar window is derived from this.
DAILY_WARMUP_TARGET_SESSIONS = 520
