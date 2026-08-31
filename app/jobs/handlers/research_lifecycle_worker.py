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
  * `blocked_stale_core_history` / `blocked_canonical_config_unavailable` are
    NOT failures. They are the gates doing their job, and a run that correctly
    declined to work must not burn a retry attempt or page anyone. They return
    ok=True with the status named.
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

    return {"ok": True, "result": _bounded_result(summary)}


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


async def probe_research_lifecycle_durable_output(
        conn: asyncpg.Connection,
        payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Crash-after-persist reconcile.

    The lifecycle writes its run row in a `finally`, so a worker that died
    after the work but before finalising the task has already left the
    evidence. If a terminal run exists for this key, return it instead of
    re-running — which would spend the provider budget a second time for a run
    that already happened.
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
    if row is None or row["status"] == "running":
        return None
    if row["status"] == "failed":
        # A recorded failure is not a reconcilable success; let the queue
        # decide whether an attempt remains.
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
           "probe_research_lifecycle_durable_output"]
