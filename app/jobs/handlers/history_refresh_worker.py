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
import time
from typing import Any, Dict, Optional

import asyncpg

from app.jobs import contracts as C
from app.jobs import history_refresh as HR

# The service's KNOWN transient 409 reasons: a shared history-warmup execution
# cooldown / active-execution window / advisory lock. These are NOT failures —
# the symbol will succeed once the shared window clears. They are absorbed by a
# BOUNDED in-task wait (below), so they never consume the queue-level attempt
# budget (which stays reserved for genuine provider errors). Any OTHER 409
# (scheduler_enabled, stale_*_session, etc.) keeps its fail-closed classification.
_COOLDOWN_409_REASONS = frozenset({
    "provider_cooldown_active",              # maintenance_cooldown.COOLDOWN_BLOCKING_REASON
    "history_warmup_execution_in_progress",  # another execution holds the lease
    "history_warmup_execution_locked",       # advisory lock held by a concurrent execution
})

# Indirections so tests can drive a deterministic virtual clock (no real sleep,
# no wall-clock dependence) while production uses the monotonic clock + asyncio.
_monotonic = time.monotonic


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _cooldown_wait_hint(exc) -> Optional[int]:
    """For a KNOWN transient cooldown/lock 409, return the server-indicated
    seconds to wait (Retry-After header, else cooldown_remaining_seconds, else 0
    meaning "no hint → use the poll default"). Returns None when the exception is
    NOT one of the recognized transient reasons (caller must fail-close)."""
    if getattr(exc, "status_code", None) != 409:
        return None
    detail = exc.detail if isinstance(getattr(exc, "detail", None), dict) else {}
    if detail.get("error") not in _COOLDOWN_409_REASONS:
        return None
    headers = getattr(exc, "headers", None) or {}
    for key in ("Retry-After", "retry-after"):
        if headers.get(key) is not None:
            try:
                return max(0, int(headers[key]))
            except (TypeError, ValueError):
                break
    crs = detail.get("cooldown_remaining_seconds")
    if crs is not None:
        try:
            return max(0, int(crs))
        except (TypeError, ValueError):
            pass
    return 0  # recognized transient reason, but no numeric hint → poll


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


def _map_service_result(result: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """Map a successful (non-exception) service result to a bounded queue result.
    Provider FAILURE runs keep their existing classification (rate-limit/transient
    → retryable, auth/operator → operator, bad payload → terminal)."""
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
    return {"ok": False, "error_class": C.ERR_RETRYABLE,
            "safe_error_code": "history_refresh_unexpected_status"}


async def execute_history_refresh_symbol(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Async core (a DB pool must already be initialised). Refreshes ONE symbol
    via the existing service and returns a bounded queue result dict.

    A KNOWN transient history-warmup cooldown / execution-lock 409 is absorbed by
    a BOUNDED in-task wait WITHIN THIS SAME queue claim: sleep the server-indicated
    time (+ a small margin), recompute CURRENT server state, and re-invoke the
    service — never busy-spinning, never calling the provider while waiting, and
    never consuming a queue-level attempt for the shared cooldown. The parent
    worker keeps renewing the task lease throughout the child's sleep. Only when
    the bounded wait is exhausted does it defer to a queue retry. Every OTHER 409
    (and non-409 4xx) keeps its fail-closed classification."""
    from datetime import datetime, timezone
    from fastapi import HTTPException
    from app.config import settings
    from app.routers.admin import history_incremental_refresh_execute_service
    from app.prospective_session import resolve_latest_completed_session
    from app.workers.persistence import get_db_connection, release_db_connection

    symbol = payload.get("symbol")
    if not symbol:
        return {"ok": False, "error_class": C.ERR_TERMINAL, "safe_error_code": "missing_symbol"}
    contract = payload.get("contract_version") or HR.HISTORY_REFRESH_CONTRACT_VERSION_V2
    max_wait = int(getattr(settings, "HISTORY_REFRESH_TASK_MAX_WAIT_SECONDS", 1800))
    margin = int(getattr(settings, "HISTORY_REFRESH_TASK_COOLDOWN_MARGIN_SECONDS", 3))
    poll = max(1, int(getattr(settings, "HISTORY_REFRESH_TASK_POLL_SECONDS", 5)))
    deadline = _monotonic() + max_wait

    while True:
        now = datetime.now(timezone.utc)
        result = None
        wait_hint = None
        conn = await get_db_connection()
        try:
            # recompute CURRENT server state every attempt so the service's own
            # staleness gates pass by construction (never a stale client view).
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
                wait_hint = _cooldown_wait_hint(e)
                if wait_hint is None:
                    # NOT a recognized transient cooldown/lock reason → fail-closed:
                    # 409 (racing state change) defers to queue retry; other 4xx terminal.
                    cls = C.ERR_RETRYABLE if e.status_code == 409 else C.ERR_TERMINAL
                    return {"ok": False, "error_class": cls,
                            "safe_error_code": "history_refresh_http_%s" % e.status_code}
            except asyncpg.PostgresError as e:
                return {"ok": False, "error_class": C.ERR_RETRYABLE,
                        "safe_error_code": "database_error", "message": type(e).__name__[:120]}
        finally:
            await release_db_connection(conn)

        if result is not None:
            return _map_service_result(result, symbol)

        # Recognized transient cooldown/lock → bounded in-task wait, then retry.
        remaining = deadline - _monotonic()
        if remaining <= 0:
            # bounded: still cooling down after the ceiling → benign queue retry.
            return {"ok": False, "error_class": C.ERR_RETRYABLE,
                    "safe_error_code": "history_refresh_cooldown_wait_exhausted"}
        sleep_for = min(max(1, (wait_hint + margin) if wait_hint else poll), remaining)
        await _sleep(sleep_for)  # NO provider call while waiting; lease renewed by parent


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
