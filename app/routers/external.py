"""External-intelligence ingress — the one internet-facing write in this API.

Three routes, all on exact static paths so each can be listed verbatim in
`app.external_ingest_mode`'s allowlist for the isolated ingress app:

    POST /api/external/signals   the gateway (the only write)
    GET  /api/external/sources   the source registry (read-only)
    GET  /api/external/health    ingress liveness (read-only, no signal data)

WHY THE CREDENTIAL MAY TRAVEL IN THE URL
----------------------------------------
It is normally poor practice, and it is used here because the platform leaves
no alternative: a TradingView webhook posts a fixed body to a fixed URL and
cannot set a custom HTTP header. The mitigations are explicit — the token is
compared in constant time, it is accepted from a header FIRST (so any caller
that can send headers should), it is never read from the request body, it is
never logged, and it never appears in a response. A URL-borne credential that
is honest about being one is better than a body-borne credential that is not.

WHY A FAILURE HERE IS QUIET
---------------------------
TradingView gives a webhook THREE SECONDS and documents no retry, so a
delivery that is slow is a delivery that is lost. Every response below is
therefore a small fixed JSON object produced without a second round trip, and
every error path returns a short stable code — never a stack trace, never a
database message, never anything derived from the caller's token.

WHAT THIS ROUTER CANNOT DO
--------------------------
It cannot read or write any scanner relation. The role it connects as holds
INSERT on the two external signal tables, SELECT on those plus the registry
and the two immutable universe tables, and — confined by RLS to rows named
`external_%` — upsert on the shared freshness table. It holds no DELETE
anywhere and no privilege at all on any scanner relation.

So the worst a leaked ingress token achieves is rows appended to tables the
scanner does not read. It cannot change a verdict, an attention tier, an
evaluation or an outcome, and it cannot mark another dimension as failed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg
from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse

from app.config import settings
from app.deps import get_db
import app.external_ingest as ei
import app.external_signals as es
from app.source_scope import SCOPE_PRODUCT

logger = logging.getLogger(__name__)

router = APIRouter()

#: One limiter per process, shared by every request. Constructed at import so
#: the window survives across requests (a per-request limiter would limit
#: nothing at all).
_limiter = ei.SlidingWindowLimiter(
    getattr(settings, "EXTERNAL_INGEST_RATE_LIMIT_PER_MINUTE", 60))

#: A second, global key so total ingress volume is bounded regardless of how
#: many sources are configured. Sized above the per-source limit so a single
#: well-behaved source is never throttled by the existence of another.
_GLOBAL_KEY = "__all__"
_global_limiter = ei.SlidingWindowLimiter(
    max(60, getattr(settings, "EXTERNAL_INGEST_RATE_LIMIT_PER_MINUTE", 60) * 4))

INGRESS_TOKEN_HEADER = "X-Smart-Scanner-Token"


def _safe_error(status_code: int, reason: str,
                detail: Optional[str] = None) -> JSONResponse:
    """The ONLY way a failure becomes a response.

    A short stable code, and optionally a bounded public detail (contract
    version names only). Never an exception string, never SQL, never a table,
    column, role or DSN. Funnelling every error through one function is what
    makes that rule checkable rather than aspirational.
    """
    body: Dict[str, Any] = {"status": "rejected", "reason": reason}
    if detail:
        body["detail"] = detail[:200]
    return JSONResponse(status_code=status_code, content=body)


def _authenticate(request: Request, token_query: Optional[str],
                  token_header: Optional[str]) -> None:
    """Header first, query string second. Raises IngressRejected on failure.

    The header is preferred so that any caller ABLE to send one does, leaving
    the query parameter as the documented fallback for platforms that cannot
    — rather than as the normal path for everyone.
    """
    expected = (getattr(settings, "EXTERNAL_INGEST_TOKEN", "") or "").strip()
    supplied = (token_header or token_query or "").strip()
    if not ei.verify_ingress_token(supplied, expected):
        # Deliberately identical for "no token", "wrong token" and "no token
        # configured on the server". Distinguishing them would tell an
        # anonymous caller which of those is true.
        raise ei.IngressRejected("unauthorized", status_code=401)


def _check_source_ip(request: Request) -> None:
    """Optional network-level narrowing, off by default.

    TradingView publishes the fixed addresses its webhooks originate from, so
    an operator who wants defence in depth can pin them. It is OFF unless
    configured because pinning a third party's published addresses is a
    liability the day they change them without telling anyone — the token is
    the security boundary, and this is a bonus.

    Behind Fly's proxy the client address arrives in `Fly-Client-IP`; the raw
    socket peer is the proxy and would match nothing.
    """
    allowed = [ip.strip() for ip
               in (getattr(settings, "EXTERNAL_INGEST_ALLOWED_IPS", "") or "").split(",")
               if ip.strip()]
    if not allowed:
        return
    client_ip = (request.headers.get("Fly-Client-IP")
                 or (request.client.host if request.client else "") or "")
    if client_ip not in allowed:
        raise ei.IngressRejected("source_not_allowed", status_code=403)


@router.post("/external/signals")
async def ingest_external_signal(
    request: Request,
    token: Optional[str] = Query(
        None, description="Ingress credential when the caller cannot set headers"),
    x_smart_scanner_token: Optional[str] = Header(
        None, alias=INGRESS_TOKEN_HEADER),
    db: asyncpg.Connection = Depends(get_db),
):
    """Accept ONE external signal delivery.

    The check order is the design (see app/external_ingest.py): token, then
    network, then rate limit, then size, then parse, then content. Nothing
    expensive is paid for until everything cheap has passed.

    Idempotent by contract. A repeated delivery returns 200 with
    `status: duplicate` rather than an error, because from the sender's point
    of view the delivery did succeed and an error would invite a retry storm
    that changes nothing.
    """
    received_at = datetime.now(timezone.utc)
    try:
        _authenticate(request, token, x_smart_scanner_token)
        _check_source_ip(request)

        if not _global_limiter.allow(_GLOBAL_KEY):
            raise ei.IngressRejected("rate_limited", status_code=429)

        # Content-Length is checked before the body is read so an oversized
        # upload is refused without buffering it. It is a hint, not a promise
        # — `decode_body` re-checks the bytes we actually received.
        max_bytes = int(getattr(settings, "EXTERNAL_INGEST_MAX_PAYLOAD_BYTES",
                                8192))
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > max_bytes:
            raise ei.IngressRejected("payload_too_large", status_code=413)

        raw_body = await request.body()
    except ei.IngressRejected as exc:
        # Refused before it could become a delivery. Deliberately NOT written
        # to the database: recording unauthenticated traffic would hand an
        # anonymous caller a way to fill our tables.
        logger.info("external ingress refused", extra={"extra_data": {
            "event": "external_signal_refused", "reason": exc.reason}})
        return _safe_error(exc.status_code, exc.reason)

    fingerprint = ei.body_fingerprint(raw_body)
    try:
        payload_peek = raw_body[:64].decode("utf-8", "replace")
        source_hint = ("ai_edge" if '"ai_edge"' in payload_peek
                       else es.SOURCE_TRADINGVIEW)
        if not _limiter.allow(source_hint):
            raise ei.IngressRejected("rate_limited", status_code=429)

        result = await ei.ingest_delivery(
            db, raw_body, received_at=received_at, max_bytes=max_bytes)
    except ei.IngressRejected as exc:
        logger.info("external ingress refused", extra={"extra_data": {
            "event": "external_signal_refused", "reason": exc.reason}})
        return _safe_error(exc.status_code, exc.reason)
    except Exception:
        # The delivery was authentic and we failed to store it. The sender is
        # told so with a 503 so a retry is meaningful, and the cause stays in
        # our logs — `exc_info=False` because an internet-facing log line must
        # not carry a traceback that may quote the payload.
        logger.error("external ingress failed", extra={"extra_data": {
            "event": "external_signal_error",
            "body_fingerprint": fingerprint[:16]}}, exc_info=False)
        return _safe_error(503, "ingest_unavailable")

    logger.info("external signal delivery", extra={"extra_data": ei.audit_log_fields(
        result, source=result.get("source") or "unknown",
        fingerprint=fingerprint, payload_bytes=len(raw_body))})

    if result["status"] == ei.DELIVERY_REJECTED:
        # 422: we authenticated the caller and understood the request; the
        # CONTENT is wrong. A 400 would suggest the transport was at fault and
        # send a user hunting in the wrong place.
        return _safe_error(422, result["reason"], result.get("detail"))

    return {
        "status": result["status"],
        "signal_id": result.get("signal_id"),
        "symbol": result.get("symbol"),
        "symbol_scope": result.get("symbol_scope"),
        "effective_at": result.get("effective_at"),
        "contract_version": es.TRADINGVIEW_CONTRACT_VERSION,
    }


SOURCES_SQL = """
SELECT source, display_name, transports, supports_realtime, supports_historical,
       supports_symbol_scan, supports_signal_events, emits_signals,
       requires_paid_plan, status, notes
FROM public.external_signal_sources
ORDER BY status <> 'live', source
"""


@router.get("/external/sources")
async def external_sources(db: asyncpg.Connection = Depends(get_db)):
    """The source registry: what exists, what can reach us, and what cannot.

    Public and read-only. It contains no signal data, no credential and no
    endpoint — only capability facts an operator needs in order to answer
    "why is this source silent?" without opening a database session.
    """
    rows = await db.fetch(SOURCES_SQL)
    return {
        "contract_version": es.EXTERNAL_INTELLIGENCE_CONTRACT_VERSION,
        "sources": [es.build_source_entry(dict(r)) for r in rows],
    }


@router.get("/external/health")
async def external_ingress_health(db: asyncpg.Connection = Depends(get_db)):
    """Is the ingress able to accept a delivery right now?

    Reports readiness — database reachable, credential configured — and the
    per-source last-delivery clock. It deliberately exposes NO signal content:
    a health endpoint that leaked what an indicator said about a symbol would
    be an unauthenticated read of the very data the Product API gates.
    """
    now = datetime.now(timezone.utc)
    token_configured = bool(
        (getattr(settings, "EXTERNAL_INGEST_TOKEN", "") or "").strip())

    database_ready = True
    sources: List[Dict[str, Any]] = []
    try:
        rows = await db.fetch(
            """
            SELECT r.source, r.status AS registry_status,
                   c.status AS delivery_status, c.last_success_at,
                   c.events_upserted
            FROM public.external_signal_sources r
            LEFT JOIN public.catalyst_source_state c
                   ON c.source = $1 || r.source
                  AND c.scope = $2
            WHERE r.status IN ('live', 'requires_manual_setup')
            ORDER BY r.source
            """,
            es.SOURCE_STATE_PREFIX, SCOPE_PRODUCT,
        )
        for row in rows:
            last = row["last_success_at"]
            sources.append({
                "source": row["source"],
                "registry_status": row["registry_status"],
                "last_delivery_at": last.isoformat() if last else None,
                "total_signals": row["events_upserted"] or 0,
                # An external source that has never delivered is NOT an error:
                # nobody may have configured the alert, and an indicator that
                # has not fired is behaving correctly.
                "ever_delivered": bool(last),
            })
    except Exception:
        logger.warning("external ingress health: database unavailable",
                       exc_info=False)
        database_ready = False

    ready = database_ready and token_configured
    payload = {
        "status": "ready" if ready else "not_ready",
        "generated_at": now.isoformat(),
        "database_ready": database_ready,
        # Whether a credential EXISTS — never the credential, and never a hash
        # or prefix of it.
        "ingress_token_configured": token_configured,
        "contract_version": es.TRADINGVIEW_CONTRACT_VERSION,
        "max_payload_bytes": int(
            getattr(settings, "EXTERNAL_INGEST_MAX_PAYLOAD_BYTES", 8192)),
        "sources": sources,
    }
    return payload if ready else JSONResponse(status_code=503, content=payload)


__all__ = ["router", "INGRESS_TOKEN_HEADER"]
