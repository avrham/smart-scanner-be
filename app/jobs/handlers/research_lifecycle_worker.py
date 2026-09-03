"""CHILD-PROCESS handler for `smart_scanner_research_lifecycle_run.v1`.

Thin by design. It owns an event loop and a DB pool, calls
`app.research_lifecycle.run_lifecycle` — the SAME function the operator CLI
calls, with the same arguments — and maps the summary to a bounded queue
result. There is no lifecycle logic here, because a handler that reimplemented
any of it would mean the manual proof and the scheduled run were two different
programs.

WHAT COMES BACK
---------------
A small result: status, the funnel headline, the provider cost in both
directions, and whether the funnel conserved. No provider payloads, no symbol
lists beyond the candidates, no DSN, no credential. The full detail is already
in `research_lifecycle_runs` where it can be queried.

FAILURE CLASSIFICATION
----------------------
  * `blocked_canonical_config_unavailable` is NOT a failure. It is a gate
    doing its job, and a run that correctly declined to work must not page
    anyone. It returns ok=True with the status named.

  * `blocked_stale_core_history` IS NOT A FAILURE EITHER — but it is also not
    an ENDING, and treating it as one is what broke the unattended path (T11).
    The lifecycle fires at 08:00 ET, finds the core bars not yet current for
    the session, ENQUEUES THE REFRESH ITSELF, and used to return ok=True and
    stop. The refresh then finished 24-45 minutes later and nothing ever came
    back: on 2026-09-02 the scheduled occurrence for 2026-09-01 terminated in
    0.2 seconds and the session's only automated research opportunity was
    gone, with every piece of infrastructure healthy.
    So a stale-core block that successfully requested its own prerequisites is
    now reported as RETRYABLE. The durable queue already knows how to express
    that: the task goes back to `retryable` with `available_at = NOW() +
    backoff`, and the worker re-claims it later. Same task, same task_key,
    same payload, same run_key, same run row, same pinned session — a deferral,
    not a second run.
  * a funnel that does not conserve IS a failure, and a terminal one: retrying
    an accounting bug produces the same accounting bug.
  * anything else is retryable once (RESEARCH_LIFECYCLE_MAX_ATTEMPTS = 2).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

import asyncpg

from app.jobs import contracts as C
from app.jobs import research_lifecycle as RL

logger = logging.getLogger(__name__)


def run_research_lifecycle_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """CHILD PROCESS entrypoint (picklable, module-level). Owns its own event
    loop + DB pool; never raises across the process boundary."""
    return asyncio.run(_child_main(payload))


async def _child_main(payload: Dict[str, Any]) -> Dict[str, Any]:
    from app.deps import close_db_pool, init_db_pool
    pool = await init_db_pool()
    try:
        async with pool.acquire() as conn:
            return await execute_research_lifecycle(conn, payload)
    finally:
        try:
            await close_db_pool()
        except Exception:                               # noqa: BLE001
            pass


async def execute_research_lifecycle(conn: asyncpg.Connection,
                                     payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run one lifecycle and shape the queue result. Never raises."""
    import app.research_funnel as rf
    import app.research_lifecycle as svc

    run_key = str(payload.get("run_key") or "").strip()
    if not run_key:
        return {"ok": False, "error": {
            "class": C.ERR_TERMINAL, "code": "missing_run_key",
            "message": "a lifecycle task must carry the run key it is idempotent on"}}

    try:
        summary = await svc.run_lifecycle(
            conn, run_key=run_key,
            admit_limit=int(payload.get("admit_limit", RL.DEFAULT_ADMIT_LIMIT)),
            warm_limit=int(payload.get("warm_limit", RL.DEFAULT_WARM_LIMIT)),
            provider_budget=int(payload.get("provider_budget",
                                            RL.DEFAULT_PROVIDER_BUDGET)),
            discovery_days=int(payload.get("discovery_days",
                                           RL.DEFAULT_DISCOVERY_DAYS)),
            refresh_discovery=bool(payload.get("refresh_discovery", True)),
            enrich=bool(payload.get("enrich", True)))
    except rf.FunnelConservationError as exc:
        # Terminal: an accounting bug does not fix itself on a second attempt,
        # and re-running would spend provider requests to reproduce it.
        return {"ok": False, "error": {
            "class": C.ERR_TERMINAL,
            "code": "funnel_does_not_conserve",
            "message": str(exc)[:400]}}
    except Exception as exc:                            # noqa: BLE001
        logger.exception("research lifecycle task failed")
        return {"ok": False, "error": {
            "class": C.ERR_RETRYABLE,
            "code": "research_lifecycle_failed",
            "message": type(exc).__name__}}

    # Waiting for prerequisites this run itself requested. Handing the queue a
    # retryable error is what buys the deferral; `_bounded_result` still rides
    # along so the deferred attempt is legible in the job event.
    status = summary.get("status")
    if status == svc.STATUS_BLOCKED_STALE and _refresh_was_requested(summary):
        return {"ok": False, "error": {
            "class": C.ERR_RETRYABLE,
            "code": "awaiting_core_history_refresh",
            "message": "core history refresh requested; re-entering after backoff"},
            "result": _bounded_result(summary)}

    return {"ok": True, "result": _bounded_result(summary)}


def _refresh_was_requested(summary: Dict[str, Any]) -> bool:
    """Did this attempt actually get its prerequisites moving?

    Only then is deferral honest. If the refresh could NOT be requested — the
    universe hash is missing, the enqueue raised — then re-entering would wait
    for something nobody started, and the run should stop and be seen.
    """
    requested = ((summary.get("core_refresh_request") or {}).get("requested")
                 or [])
    return any(r.get("status") in ("queued", "already_queued", "already_applied")
               for r in requested)


def _bounded_result(summary: Dict[str, Any]) -> Dict[str, Any]:
    funnel = summary.get("funnel") or {}
    provider = funnel.get("provider") or {}
    enrichment = summary.get("enrichment") or {}
    return {
        "run_key": summary.get("run_key"),
        "run_id": summary.get("run_id"),
        "status": summary.get("status"),
        "target_completed_session": summary.get("target_completed_session"),
        "blocked_detail": summary.get("blocked_detail"),
        "core_history_fresh": (summary.get("core_freshness") or {}).get("fresh"),
        "canonical_config_hash":
            (summary.get("canonical_config") or {}).get("config_hash"),
        "selected_for_research": funnel.get("selected_for_research"),
        "admission": funnel.get("admission"),
        "scanned": funnel.get("scanned"),
        "research_candidates": funnel.get("research_candidates"),
        "provider_requests_used": provider.get(
            "calls_used", summary.get("provider_requests_used")),
        "provider_requests_avoided": provider.get(
            "calls_avoided",
            summary.get("provider_requests_avoided_by_admission")),
        "enrichment_symbols": enrichment.get("enriched"),
        "enrichment_provider_requests": enrichment.get("provider_requests"),
        "funnel_conserved": bool((funnel.get("conservation") or {}).get("ok", True)),
        "duration_seconds": summary.get("duration_seconds"),
    }


#: Run statuses that are NOT durable output, and therefore must never let the
#: probe reconcile a task to `succeeded`:
#:
#:   running                     - still executing, nothing finished
#:   failed                      - a recorded failure is not a success
#:   blocked_stale_core_history  - deferred, waiting on prerequisites it asked
#:                                 for, and expecting to be re-entered (T16)
_NOT_DURABLE_OUTPUT = frozenset({
    "running",
    "failed",
    "blocked_stale_core_history",
})


async def probe_research_lifecycle_durable_output(
        conn: asyncpg.Connection,
        payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Crash-after-persist reconcile.

    The lifecycle writes its run row in a `finally`, so a worker that died
    after the work but before finalising the task has already left the
    evidence. If a COMPLETE run exists for this key, return it instead of
    re-running — which would spend the provider budget a second time for a run
    that already happened.

    "COMPLETE", NOT "A ROW EXISTS" (T16)
    ------------------------------------
    This probe is consulted from two places, and both of them will reconcile
    the task to SUCCEEDED on any non-None answer: `_reconcile_one` when a lease
    expires, and `_finalize_failure` before it applies retry backoff. So a
    probe that mistakes a deferred run for a finished one does not merely
    mislabel something — it destroys the continuation.

    That is exactly what happened to the scheduled occurrence for session
    2026-09-02 (run 508f1a5d, task 13149cc8) on 2026-09-03. The lifecycle did
    everything right: pinned the session, checked freshness before any
    research work, found the core bars stale, enqueued both refresh jobs, spent
    zero provider requests, and returned ERR_RETRYABLE to ask for a deferral.
    This probe then saw a `blocked_stale_core_history` row, called it durable
    output, and the task was reconciled to `succeeded` 1.6 seconds in with
    three of its four attempts unused. Both refreshes completed on time; nobody
    ever came back; the session was lost to automation.

    A blocked-on-stale-core run is a run that has NOT done its work and has
    asked to be re-entered. It is `running` in every sense that matters here,
    so it is treated the same way: return None and let the queue apply the
    backoff it was configured with. The queue — not this function — owns the
    attempt budget, so an exhausted continuation still settles terminally
    through the normal path.

    `blocked_canonical_config_unavailable` is deliberately NOT in this list.
    That gate reports a configuration problem that will not resolve on its own,
    the handler still returns ok=True for it, and it should stay durable.
    """
    run_key = str(payload.get("run_key") or "").strip()
    if not run_key:
        return None
    try:
        row = await conn.fetchrow(
            "SELECT id, status, symbols_selected, admission_passed,"
            " admission_rejected, research_scanned, research_candidates,"
            " provider_calls_used, provider_calls_avoided, funnel_conserved,"
            " duration_seconds, target_session "
            "FROM public.research_lifecycle_runs WHERE run_key = $1", run_key)
    except asyncpg.PostgresError:
        return None
    if row is None or row["status"] in _NOT_DURABLE_OUTPUT:
        # Not finished work: no output to reconcile to. Let the queue decide
        # whether an attempt remains and, if so, when it comes back.
        return None
    return {"ok": True, "result": {
        "run_key": run_key, "run_id": str(row["id"]),
        "status": row["status"],
        "target_completed_session": (row["target_session"].isoformat()
                                     if row["target_session"] else None),
        "selected_for_research": row["symbols_selected"],
        "admission": {"passed": row["admission_passed"],
                      "rejected": row["admission_rejected"]},
        "scanned": row["research_scanned"],
        "research_candidates": row["research_candidates"],
        "provider_requests_used": row["provider_calls_used"],
        "provider_requests_avoided": row["provider_calls_avoided"],
        "funnel_conserved": row["funnel_conserved"],
        "duration_seconds": (float(row["duration_seconds"])
                             if row["duration_seconds"] is not None else None),
        "reconciled_from_durable_output": True}}


__all__ = ["run_research_lifecycle_task", "execute_research_lifecycle",
           "_NOT_DURABLE_OUTPUT",
           "probe_research_lifecycle_durable_output"]
