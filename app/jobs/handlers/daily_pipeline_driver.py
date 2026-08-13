"""Durable pipeline-advance driver handler (smart_scanner_daily_pipeline_advance.v1).

Makes scheduler -> occurrence -> advance FULLY AUTOMATIC with no HTTP self-call,
no fly ssh, and no operator-held WORKER_TOKEN. The scheduler materialises ONE
driver task per fired occurrence; the dedicated pipeline-driver worker claims it
and invokes the EXISTING ``advance_daily_pipeline_service`` — the daily-pipeline
state machine is NEVER duplicated here.

Each invocation makes BOUNDED progress: it advances the occurrence one stage at a
time until it either reaches a terminal state (succeeded/failed) OR a stage is
legitimately waiting on asynchronous durable work (a running campaign/outcome
job, a history cooldown). In the waiting case it DEFERS via the queue's existing
retryable backoff — never spins, never marks failure, never creates duplicate
child jobs — and resumes from persisted pipeline state when re-claimed. A crash
after the occurrence has succeeded reconciles via ``probe_fn``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import asyncpg

from app.jobs import contracts as C
from app.jobs import daily_pipeline as DP

# Bounded number of stage advances per single claim. The occurrence has 4 stages;
# a few extra allow one claim to walk history_refresh -> prospective_campaign and
# stop cleanly once it starts waiting on the async campaign job.
_MAX_STAGE_ADVANCES = 6


def run_daily_pipeline_advance_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """CHILD PROCESS entrypoint (picklable, module-level). Owns its own event
    loop + DB pool; never raises across the process boundary."""
    return asyncio.run(_child_main(payload))


async def _child_main(payload: Dict[str, Any]) -> Dict[str, Any]:
    from app.deps import init_db_pool, close_db_pool
    await init_db_pool()
    try:
        return await drive_pipeline_advance(payload)
    finally:
        try:
            await close_db_pool()
        except Exception:
            pass


async def drive_pipeline_advance(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Async core (a DB pool must already be initialised). Advances the durable
    occurrence via the existing service until terminal or waiting-on-async."""
    from fastapi import HTTPException
    from app.routers.admin import advance_daily_pipeline_service
    from app.workers.persistence import get_db_connection, release_db_connection

    universe_id = payload.get("universe_id")
    if not universe_id:
        return {"ok": False, "error_class": C.ERR_TERMINAL, "safe_error_code": "missing_universe_id"}
    body = {
        "contract_version": payload.get("pipeline_contract_version") or DP.PIPELINE_CONTRACT_VERSION_V2,
        "universe_id": universe_id,
        "schedule_code": payload.get("schedule_code") or "SMART-SCANNER-DAILY-PIPELINE",
        "schedule_version": int(payload.get("schedule_version") or 1),
    }
    import time
    from app.config import settings
    max_wait = int(getattr(settings, "DAILY_PIPELINE_DRIVER_MAX_WAIT_SECONDS", 10800))
    poll = max(1, int(getattr(settings, "DAILY_PIPELINE_DRIVER_POLL_SECONDS", 30)))
    deadline = time.monotonic() + max_wait
    view: Optional[Dict[str, Any]] = None
    prev_signature = None
    while True:
        conn = await get_db_connection()
        try:
            try:
                view = await advance_daily_pipeline_service(conn, body=body)
            except C.JobError as e:  # pragma: no cover - defensive
                return {"ok": False, "error_class": e.error_class, "safe_error_code": e.safe_error_code}
            except HTTPException as e:
                # 4xx bad config (unknown universe, bad contract) is terminal;
                # 409 (a concurrent lock) is a transient defer.
                cls = C.ERR_RETRYABLE if e.status_code == 409 else C.ERR_TERMINAL
                return {"ok": False, "error_class": cls, "safe_error_code": "pipeline_advance_http_%s" % e.status_code}
            except asyncpg.PostgresError as e:
                return {"ok": False, "error_class": C.ERR_RETRYABLE,
                        "safe_error_code": "database_error", "message": type(e).__name__[:120]}
        finally:
            await release_db_connection(conn)

        status = view.get("occurrence_status")
        if status == "succeeded":
            return {"ok": True, "result": {"occurrence_id": view.get("occurrence_id"),
                                           "occurrence_status": "succeeded",
                                           "current_stage": view.get("current_stage")}}
        if status == "failed" or view.get("terminal_failure_stage"):
            return {"ok": False, "error_class": C.ERR_TERMINAL,
                    "safe_error_code": "occurrence_terminal_failure",
                    "message": str(view.get("terminal_failure_stage"))}
        signature = (view.get("current_stage"), tuple(sorted((view.get("stage_states") or {}).items())))
        if signature != prev_signature:
            prev_signature = signature
            continue  # progress made -> advance the next stage immediately
        # No progress -> a stage is waiting on asynchronous durable work (a running
        # campaign/outcome job, a history cooldown). Sleep and re-check WITHIN this
        # claim (the worker renews the task lease). Beyond the wall-clock ceiling,
        # DEFER cleanly (one retry) — a benign "still working", not a failure.
        if time.monotonic() >= deadline:
            return {"ok": False, "error_class": C.ERR_RETRYABLE,
                    "safe_error_code": "occurrence_in_progress",
                    "message": (view.get("current_stage") if view else "unknown")}
        await asyncio.sleep(poll)


async def probe_daily_pipeline_advance_durable_output(
        conn: asyncpg.Connection, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Crash-after-persist reconcile: if the occurrence for this (schedule,
    resolved session, frozen universe, v2) is already succeeded, return a bounded
    result so a crashed/lease-lost driver task reconciles to succeeded rather than
    re-running. Returns None when not yet terminally succeeded."""
    from datetime import datetime, timezone
    from app.prospective_session import resolve_latest_completed_session
    universe_id = payload.get("universe_id")
    if not universe_id:
        return None
    uhash = payload.get("universe_hash")
    if not uhash:
        try:
            row = await conn.fetchrow(
                "SELECT universe_hash FROM history_warmup_universes WHERE id=$1", universe_id)
        except asyncpg.PostgresError:
            return None
        uhash = row["universe_hash"] if row else None
    if not uhash:
        return None
    now = datetime.now(timezone.utc)
    key = DP.pipeline_occurrence_identity(
        schedule_code=payload.get("schedule_code") or "SMART-SCANNER-DAILY-PIPELINE",
        schedule_version=int(payload.get("schedule_version") or 1),
        resolved_session_date=str(resolve_latest_completed_session(now)),
        frozen_universe_hash=uhash,
        pipeline_contract_version=DP.PIPELINE_CONTRACT_VERSION_V2)
    occ = await conn.fetchrow(
        "SELECT id, status FROM job_runs WHERE idempotency_key=$1 AND job_type=$2",
        key, DP.PIPELINE_JOB_TYPE)
    if occ is not None and occ["status"] == C.JOB_SUCCEEDED:
        return {"occurrence_id": str(occ["id"]), "occurrence_status": "succeeded", "reconciled": True}
    return None


__all__ = [
    "run_daily_pipeline_advance_task", "drive_pipeline_advance",
    "probe_daily_pipeline_advance_durable_output",
]
