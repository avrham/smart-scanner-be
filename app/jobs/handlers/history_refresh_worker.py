"""Durable history-incremental-refresh worker handler (Root Cause A).

Runs the provider-backed single-symbol incremental refresh by calling the
EXISTING ``history_incremental_refresh_execute_service`` directly against the DB
— no HTTP self-call, no ``fly ssh``, no operator WORKER_TOKEN. This handler runs
ONLY on the dedicated history-refresh worker app, which is the sole automated
component (besides the history-warmup HTTP app) that carries the Massive
credential; the pipeline driver / prospective / outcome apps never hold it.

The child computes the CURRENT server state (target session, latest local daily
+ 4H) so the service never rejects on a stale client view; a genuine mid-flight
state change surfaces as a 409 → retryable defer. A per-symbol provider failure
maps to its service-classified error class (rate-limit → retryable, auth →
operator, bad payload → terminal). A crash after the bars are persisted
reconciles via ``probe_fn`` — which recognizes that the symbol is now current.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import asyncpg

from app.jobs import contracts as C
from app.jobs import history_refresh as HR


def run_history_incremental_refresh_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """CHILD PROCESS entrypoint (picklable, module-level). Owns its own event
    loop + DB pool; never raises across the process boundary."""
    return asyncio.run(_child_main(payload))


async def _child_main(payload: Dict[str, Any]) -> Dict[str, Any]:
    from app.deps import init_db_pool, close_db_pool
    await init_db_pool()
    try:
        return await execute_history_refresh_symbol(payload)
    finally:
        try:
            await close_db_pool()
        except Exception:
            pass


def _current_state_body(symbol: str, contract: str, *, target, latest, latest_4h_end):
    """Build the service request body from CURRENT server state (never a stale
    client view). latest/latest_4h are server-observed, so the service's
    staleness gates pass by construction."""
    from zoneinfo import ZoneInfo
    latest_4h_session = (latest_4h_end.astimezone(ZoneInfo("America/New_York")).date()
                         if latest_4h_end else None)
    return {
        "contract_version": contract,
        "symbol": symbol,
        "target_completed_session": target.isoformat(),
        "latest_local_session": latest.isoformat() if latest else None,
        "latest_local_4h_session": latest_4h_session.isoformat() if latest_4h_session else None,
    }


async def execute_history_refresh_symbol(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Async core (a DB pool must already be initialised). Refreshes ONE symbol
    via the existing service and returns a bounded queue result dict."""
    from datetime import datetime, timezone
    from fastapi import HTTPException
    from app.routers.admin import history_incremental_refresh_execute_service
    from app.prospective_session import resolve_latest_completed_session
    from app.workers.persistence import get_db_connection, release_db_connection

    symbol = payload.get("symbol")
    if not symbol:
        return {"ok": False, "error_class": C.ERR_TERMINAL, "safe_error_code": "missing_symbol"}
    contract = payload.get("contract_version") or HR.HISTORY_REFRESH_CONTRACT_VERSION_V2
    now = datetime.now(timezone.utc)

    conn = await get_db_connection()
    try:
        target = resolve_latest_completed_session(now)
        latest = await conn.fetchval(
            "SELECT MAX(trading_date) FROM daily_bars WHERE symbol=$1", symbol)
        latest_4h_end = await conn.fetchval(
            "SELECT MAX(bar_end) FROM market_bars_4h WHERE symbol=$1 AND is_completed", symbol)
        body = _current_state_body(symbol, contract, target=target, latest=latest,
                                   latest_4h_end=latest_4h_end)
        try:
            result = await history_incremental_refresh_execute_service(conn, body=body, now=now)
        except HTTPException as e:
            # 409 = transient (execution lock / provider cooldown / a racing state
            # change) → defer; other 4xx = bad config/contract → terminal.
            cls = C.ERR_RETRYABLE if e.status_code == 409 else C.ERR_TERMINAL
            return {"ok": False, "error_class": cls,
                    "safe_error_code": "history_refresh_http_%s" % e.status_code}
        except asyncpg.PostgresError as e:
            return {"ok": False, "error_class": C.ERR_RETRYABLE,
                    "safe_error_code": "database_error", "message": type(e).__name__[:120]}
    finally:
        await release_db_connection(conn)

    status = result.get("status")
    if status in ("already_applied", "no-op", "executed"):
        return {"ok": True, "result": {
            "symbol": symbol, "status": status, "run_id": result.get("run_id"),
            "provider_request_count": int(result.get("provider_request_count") or 0)}}
    if status == "failed":
        err = result.get("error") or {}
        klass = err.get("class")
        error_class = klass if klass in C.ERROR_CLASSES else C.ERR_RETRYABLE
        return {"ok": False, "error_class": error_class,
                "safe_error_code": str(err.get("code") or "history_refresh_failed")}
    # Unknown / unexpected status — treat as retryable rather than silently succeed.
    return {"ok": False, "error_class": C.ERR_RETRYABLE,
            "safe_error_code": "history_refresh_unexpected_status"}


async def probe_history_refresh_durable_output(
        conn: asyncpg.Connection, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Crash-after-persist reconcile: the durable OUTPUT of a symbol refresh is
    "the symbol is current". If, at probe time, this symbol has no missing daily
    sessions through the resolved target AND its 4H is not refresh-needed, the
    work is effectively complete → reconcile succeeded. Otherwise None (retry).
    Robust to the fact that a successful run advances MAX(trading_date), so an
    identity recomputed from the new latest would not match the completed run."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    from app.prospective_session import resolve_latest_completed_session
    from app.history_warmup_execute import (
        missing_trading_sessions, classify_incremental_4h_state,
        STATE_INCREMENTAL_REFRESH_NEEDED)
    symbol = payload.get("symbol")
    if not symbol:
        return None
    now = datetime.now(timezone.utc)
    try:
        target = resolve_latest_completed_session(now)
        latest = await conn.fetchval(
            "SELECT MAX(trading_date) FROM daily_bars WHERE symbol=$1", symbol)
        latest_4h_end = await conn.fetchval(
            "SELECT MAX(bar_end) FROM market_bars_4h WHERE symbol=$1 AND is_completed", symbol)
    except asyncpg.PostgresError:
        return None
    if missing_trading_sessions(latest, target):
        return None
    latest_4h_session = (latest_4h_end.astimezone(ZoneInfo("America/New_York")).date()
                         if latest_4h_end else None)
    if classify_incremental_4h_state(latest_4h_session, target) == STATE_INCREMENTAL_REFRESH_NEEDED:
        return None
    return {"symbol": symbol, "status": "reconciled_current", "reconciled": True}


__all__ = [
    "run_history_incremental_refresh_task",
    "execute_history_refresh_symbol",
    "probe_history_refresh_durable_output",
]
