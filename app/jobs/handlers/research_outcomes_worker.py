"""CHILD-PROCESS handler for `smart_scanner_research_outcome_maturation.v1`.

Thin by design, exactly like `research_lifecycle_worker`. It owns an event loop
and a DB pool, calls `app.research_outcomes.run_outcome_maturation` — the SAME
function the operator CLI calls, with the same arguments — and maps the summary
to a bounded queue result. There is no measurement logic here, because a
handler that reimplemented any of it would mean the manual backfill and the
scheduled run were two different programs.

WHAT COMES BACK
---------------
Counts and a small sample: how many observations were planned, how many were
due, how many measured, how many are still waiting and why. No bar series, no
symbol dumps beyond ten sampled rows, no DSN, no credential. The full detail is
already in `research_scan_outcomes`, where it can be queried.

FAILURE CLASSIFICATION, AND WHY ALMOST NOTHING IS TERMINAL
-----------------------------------------------------------
Missing bars are NOT a failure. They are the expected steady state of this
system: a research symbol's forward bars arrive only when the bounded freshness
top-up reaches it, so on any given night most eligible horizons legitimately
have nothing to measure against. That is recorded per observation as
`waiting_for_data` and the RUN succeeds — a run that measured nothing because
nothing was measurable did its job perfectly.

What is left is transport: a dropped connection, a worker killed mid-pass. Those
are retryable, three attempts, short backoff. There is no `blocked_*` state and
no self-requested prerequisite, because this run asks nothing of anybody — it
reads bars that either exist or do not.

An invalid payload (no run key) is terminal: retrying a task that cannot say
what it is idempotent on would create a second run history of the same pass.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

import asyncpg

from app.jobs import contracts as C
from app.jobs import research_outcomes as RO

logger = logging.getLogger(__name__)


def run_research_outcomes_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """CHILD PROCESS entrypoint (picklable, module-level). Owns its own event
    loop + DB pool; never raises across the process boundary."""
    return asyncio.run(_child_main(payload))


async def _child_main(payload: Dict[str, Any]) -> Dict[str, Any]:
    from app.deps import close_db_pool, init_db_pool
    pool = await init_db_pool()
    try:
        async with pool.acquire() as conn:
            return await execute_research_outcomes(conn, payload)
    finally:
        try:
            await close_db_pool()
        except Exception:                               # noqa: BLE001
            pass


async def execute_research_outcomes(conn: asyncpg.Connection,
                                    payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run one maturation pass and shape the queue result. Never raises."""
    import app.research_outcomes as svc

    run_key = str(payload.get("run_key") or "").strip()
    if not run_key:
        return {"ok": False, "error": {
            "class": C.ERR_TERMINAL, "code": "missing_run_key",
            "message": "an outcome task must carry the run key it is "
                       "idempotent on"}}

    try:
        summary = await svc.run_outcome_maturation(
            conn, run_key=run_key,
            mode=str(payload.get("mode") or svc.MODE_SCHEDULED),
            scan_limit=int(payload.get("scan_limit", svc.DEFAULT_SCAN_LIMIT)),
            observation_limit=int(payload.get("observation_limit",
                                              svc.DEFAULT_OBSERVATION_LIMIT)),
            include_settled=bool(payload.get("include_settled", False)))
    except Exception as exc:                            # noqa: BLE001
        logger.exception("research outcome task failed")
        return {"ok": False, "error": {
            "class": C.ERR_RETRYABLE,
            "code": "research_outcome_maturation_failed",
            "message": type(exc).__name__}}

    return {"ok": True, "result": _bounded_result(summary)}


def _bounded_result(summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "run_key": summary.get("run_key"),
        "run_id": summary.get("run_id"),
        "status": summary.get("status"),
        "mode": summary.get("mode"),
        "as_of_session": summary.get("as_of_session"),
        "scans_considered": summary.get("scans_considered"),
        "observations_planned": summary.get("observations_planned"),
        "observations_due": summary.get("observations_due"),
        "measured": summary.get("measured"),
        "waiting_for_data": summary.get("waiting_for_data"),
        "not_yet_eligible": summary.get("not_yet_eligible"),
        "failed_terminal": summary.get("failed_terminal"),
        "revisions_detected": summary.get("revisions_detected"),
        "waiting_reasons": summary.get("waiting_reasons"),
        "measured_sample": summary.get("measured_sample"),
        "truncated_by_limit": summary.get("truncated_by_limit"),
    }


#: Run statuses that are NOT durable output, and therefore must never let the
#: probe reconcile a task to `succeeded`:
#:
#:   running   still executing — nothing finished
#:   failed    a recorded failure is not a success
#:
#: `dry_run` is absent because a dry run is never dispatched through the queue.
#: The lesson is T16's, restated: a probe that mistakes an unfinished run for a
#: finished one does not mislabel something, it destroys the continuation.
_NOT_DURABLE_OUTPUT = frozenset({"running", "failed"})


async def probe_research_outcomes_durable_output(
        conn: asyncpg.Connection,
        payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Crash-after-persist reconcile.

    `run_outcome_maturation` writes its run row in a `finally`, so a worker
    that died after the pass but before finalising the task has already left
    the evidence. If a COMPLETED run exists for this key, return it instead of
    re-running.

    Re-running would in fact be harmless here — every measured outcome is
    frozen by a database trigger, so a repeat pass cannot produce a second,
    different answer. The probe exists anyway, for the reason the lifecycle's
    does: the run ROW is the audit, and a second pass that re-opened it would
    overwrite a completed run's counters with a second pass's (mostly zero)
    ones, turning a correct record of what happened into a misleading one.
    """
    run_key = str(payload.get("run_key") or "").strip()
    if not run_key:
        return None
    try:
        row = await conn.fetchrow(
            "SELECT id, status, mode, as_of_session, scans_considered,"
            " observations_planned, observations_due, measured,"
            " waiting_for_data, not_yet_eligible, failed_terminal,"
            " revisions_detected, truncated_by_limit, duration_seconds "
            "FROM public.research_outcome_runs WHERE run_key = $1", run_key)
    except asyncpg.PostgresError:
        return None
    if row is None or row["status"] in _NOT_DURABLE_OUTPUT:
        return None
    return {"ok": True, "result": {
        "run_key": run_key, "run_id": str(row["id"]),
        "status": row["status"], "mode": row["mode"],
        "as_of_session": (row["as_of_session"].isoformat()
                          if row["as_of_session"] else None),
        "scans_considered": row["scans_considered"],
        "observations_planned": row["observations_planned"],
        "observations_due": row["observations_due"],
        "measured": row["measured"],
        "waiting_for_data": row["waiting_for_data"],
        "not_yet_eligible": row["not_yet_eligible"],
        "failed_terminal": row["failed_terminal"],
        "revisions_detected": row["revisions_detected"],
        "truncated_by_limit": row["truncated_by_limit"],
        "duration_seconds": (float(row["duration_seconds"])
                             if row["duration_seconds"] is not None else None),
        "reconciled_from_durable_output": True}}


__all__ = ["run_research_outcomes_task", "execute_research_outcomes",
           "_NOT_DURABLE_OUTPUT", "probe_research_outcomes_durable_output"]
