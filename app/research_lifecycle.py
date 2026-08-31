"""ONE bounded research lifecycle, in the order that makes sense.

        LATEST COMPLETED SESSION
                 |
        [gate] canonical + reference bars fresh?
                 |  no -> enqueue the EXISTING bounded refresh job, report
                 |        blocked, and stop. Never scan on stale benchmarks.
        refresh discovery
                 |
        admit / update research symbols
                 |
        cheap admission gates             <- ZERO provider requests
                 |
        bounded history warmup            <- the ONLY history-provider stage
                 |
        research readiness
                 |
        research scan                     <- local bars only
                 |
        candidate classification
                 |
        funnel accounting + conservation  <- must add up, or the run fails
                 |
        lazy catalyst enrichment          <- survivors only, research cohort
                 |
        one persisted run

WHY THIS LIVES IN `app/` AND NOT IN `ops/`
------------------------------------------
Because the durable task handler and the operator CLI must be the SAME code
path. "Run one lifecycle manually through the exact same dispatcher used by
scheduling" is not satisfiable if the manual path is a script and the scheduled
path is a handler that reimplements it — the two drift, and the thing that was
proven by hand is not the thing that runs at 18:30. So this module takes an
open connection and returns a summary, and both callers are three lines long.

WHY ONE COMMAND AND NOT SIX
---------------------------
The stages are not independent. Scanning against stale benchmark bars produces
a relative-strength reading that is quietly wrong; warming before admission
spends the requests admission exists to save; enriching before classification
spends them on symbols that were about to be rejected. Ordering IS the feature.

THE FRESHNESS GATE IS A GATE
----------------------------
If the frozen 25 and the 10 reference symbols are not current through the
latest completed session, the lifecycle enqueues the existing bounded refresh
job — the same durable job the daily pipeline uses, executed by the dedicated
history-refresh worker that already holds the provider credential — and then
reports `blocked_stale_core_history` and STOPS. It does not restore freshness
itself: the research role deliberately cannot write a frozen-universe bar, and
a second refresh implementation is exactly what this project keeps not building.

BUDGET
------
Every provider request is counted and the ceiling is enforced, not hoped for.
Warmup and enrichment have SEPARATE budgets, so buying context for today's
survivor cannot cost tomorrow's symbol its history. A run may stop cleanly at
exhaustion and resume on the next run; nothing retries in a storm.

ISOLATION
---------
A research symbol never becomes a frozen-universe member, an experiment pair,
a canonical outcome, a scanner row, ENTER-eligible or attention-ranked. This
module reads the frozen universe (to know which discovered symbols to ignore)
and writes only research relations and its own audit.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import app.research_admission as ra
import app.research_enrichment as re_
import app.research_funnel as rf
import app.research_ingest as ri
import app.research_runs as rr
import app.research_scan as rs
import app.research_universe as ru
from app.prospective_session import resolve_latest_completed_session

logger = logging.getLogger(__name__)

LIFECYCLE_CONTRACT_VERSION = "smart_scanner_research_lifecycle.v2"

STATUS_COMPLETED = rr.RUN_STATUS_COMPLETED
STATUS_BLOCKED_STALE = rr.RUN_STATUS_BLOCKED_STALE
STATUS_BLOCKED_CONFIG = rr.RUN_STATUS_BLOCKED_CONFIG
STATUS_DRY_RUN = rr.RUN_STATUS_DRY_RUN

#: The two universes whose bars every research scan depends on: the frozen 25
#: (the canonical freshness signal) and the reference market (SPY and the
#: sector ETFs, which the benchmark-relative context reads directly).
CORE_UNIVERSES = ("WYCKOFF-HISTORY-WARMUP-QUALIFICATION",
                  "SMART-SCANNER-REFERENCE-MARKET-V1")

#: Provider requests the movers refresh costs (one per entitled list).
DISCOVERY_REQUEST_COST = 3

CORE_FRESHNESS_SQL = """
SELECT u.universe_code, u.id AS universe_id, u.universe_hash,
       count(*)::int AS symbols,
       count(*) FILTER (
         WHERE (SELECT max(b.trading_date) FROM public.daily_bars b
                WHERE b.symbol = s.symbol) >= $2)::int AS current_symbols,
       min((SELECT max(b.trading_date) FROM public.daily_bars b
            WHERE b.symbol = s.symbol)) AS oldest_latest_bar,
       array_agg(s.symbol ORDER BY s.symbol)                  AS symbols_all,
       array_remove(array_agg(CASE WHEN (
         SELECT max(b.trading_date) FROM public.daily_bars b
         WHERE b.symbol = s.symbol) >= $2 THEN NULL ELSE s.symbol END),
         NULL)                                                AS stale_symbols
FROM public.history_warmup_universe_symbols s
JOIN public.history_warmup_universes u ON u.id = s.universe_id
WHERE u.universe_code = ANY($1::text[])
GROUP BY u.universe_code, u.id, u.universe_hash
"""


async def check_core_freshness(conn, *, target: date) -> Dict[str, Any]:
    """Are the canonical bars current through the latest completed session?

    Reported per universe rather than as one boolean: "the reference market is
    two sessions behind" and "one of the 25 is behind" are different operator
    problems and the summary must not blur them.
    """
    rows = [dict(r) for r in await conn.fetch(
        CORE_FRESHNESS_SQL, list(CORE_UNIVERSES), target)]
    universes = {
        r["universe_code"]: {
            "symbols": r["symbols"], "current": r["current_symbols"],
            "stale": r["symbols"] - r["current_symbols"],
            "oldest_latest_bar": r["oldest_latest_bar"],
            "universe_id": str(r["universe_id"]),
            "universe_hash": r["universe_hash"],
            "stale_symbols": list(r["stale_symbols"] or []),
            "all_symbols": list(r["symbols_all"] or [])}
        for r in rows}
    stale = sum(u["stale"] for u in universes.values())
    return {"target_completed_session": target, "universes": universes,
            "stale_symbols": stale, "fresh": stale == 0 and bool(universes)}


async def request_core_refresh(conn, freshness: Dict[str, Any], *,
                               target: date) -> Dict[str, Any]:
    """Ask the EXISTING durable refresh job to bring the core bars current.

    Deliberately an ENQUEUE and not an execution. The research identity cannot
    write a frozen-universe bar and should not be able to: canonical price data
    belongs to the component that already holds the provider credential and the
    warmup lock. So this creates (or recognises — it is idempotent) exactly the
    same `history_incremental_refresh` job the daily pipeline creates, and the
    dedicated history-refresh worker executes it under its own role.

    Never raises. A refresh that cannot be requested is reported as such; the
    run is blocked either way, and blocking is already the correct outcome.
    """
    from app.jobs import history_refresh as hr
    requested: List[Dict[str, Any]] = []
    for code, info in (freshness.get("universes") or {}).items():
        if not info.get("stale") or not info.get("universe_hash"):
            continue
        try:
            result = await hr.enqueue_history_incremental_refresh(
                conn, universe_id=info["universe_id"],
                universe_hash=info["universe_hash"],
                symbols=list(info["all_symbols"]),
                resolved_session_date=target.isoformat(),
                requested_by="research_lifecycle")
            requested.append({"universe_code": code,
                              "status": result.get("status"),
                              "job_id": result.get("job_id")})
        except Exception as exc:                       # noqa: BLE001
            logger.warning("core refresh enqueue failed for %s", code,
                           exc_info=True)
            requested.append({"universe_code": code, "status": "not_requested",
                              "reason": type(exc).__name__})
    return {"requested": requested,
            "detail": ("the refresh runs on the dedicated history-refresh "
                       "worker, which holds the provider credential; this "
                       "lifecycle only asks for it")}


async def resolve_canonical_config(conn) -> Dict[str, Any]:
    """The canonical arm, from the SAME resolution the research scan will use.

    Fails closed. Admission reading one configuration while the scan reads
    another is the drift the previous milestone removed, and the identity is
    now recorded per run (`config_hash`) so a future divergence is visible in
    the audit rather than discovered by comparing two reports.
    """
    from app.workers.patterns.config import ConfigUnavailable, bound_config_connection
    from app.workers.shadow.experiments import get_experiment
    from app.workers.shadow.runner import _resolve_arm

    experiment = get_experiment(rs.RESEARCH_EXPERIMENT_CODE)
    try:
        with bound_config_connection(conn, require_db=True):
            arm = await _resolve_arm(
                experiment.candidate_pattern_code,
                experiment.candidate_arm_code,
                config_overrides=experiment.candidate_config_overrides)
    except ConfigUnavailable as exc:
        return {"ok": False, "reason": str(exc), "min_price": None,
                "config_hash": None}
    except Exception as exc:                           # noqa: BLE001
        return {"ok": False, "reason": type(exc).__name__, "min_price": None,
                "config_hash": None}
    return {"ok": True, "min_price": ra.resolve_min_price(arm["config"]),
            "config_hash": arm.get("config_hash"),
            "pattern_code": experiment.candidate_pattern_code,
            "experiment_code": rs.RESEARCH_EXPERIMENT_CODE}


def _warm_detail(warm: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    """Per-symbol warmup facts, for the run's child rows."""
    detail: Dict[str, Dict[str, int]] = {}
    for entry in warm.get("warmed", []):
        detail[entry["symbol"]] = {
            "warmed": True,
            "provider_calls": int(entry.get("provider_requests") or 0),
            "bars_inserted": int(entry.get("inserted") or 0)}
    for entry in warm.get("failed", []):
        detail[entry["symbol"]] = {
            "warmed": False,
            "provider_calls": int(entry.get("provider_requests") or 0),
            "bars_inserted": 0}
    return detail


async def run_lifecycle(conn, *,
                        run_key: str,
                        admit_limit: int = ru.MAX_NEW_RESEARCH_SYMBOLS_PER_RUN,
                        warm_limit: int = ru.MAX_WARMUP_SYMBOLS_PER_RUN,
                        provider_budget: int = ru.MAX_PROVIDER_REQUESTS_PER_RUN,
                        discovery_days: int = 14,
                        dry_run: bool = False,
                        refresh_discovery: bool = True,
                        enrich: bool = True,
                        request_refresh_when_stale: bool = True,
                        now: Optional[datetime] = None) -> Dict[str, Any]:
    """One lifecycle. Idempotent by `run_key`; never leaves a run unrecorded.

    The audit row is opened BEFORE any work and closed in a `finally`, so a
    crash leaves a `running` row that says something was attempted rather than
    no evidence at all.
    """
    started = time.monotonic()
    moment = now or datetime.now(timezone.utc)
    target = resolve_latest_completed_session(moment)

    summary: Dict[str, Any] = {
        "contract_version": LIFECYCLE_CONTRACT_VERSION,
        "run_key": run_key,
        "started_at": moment.isoformat(),
        "target_completed_session": target.isoformat(),
        "status": STATUS_DRY_RUN if dry_run else STATUS_COMPLETED,
        "provider_budget": int(provider_budget),
        "provider_requests_used": 0,
        "provider_requests_avoided_by_admission": 0,
    }

    run = await rr.start_run(conn, run_key=run_key, target_session=target,
                             now=moment)
    summary["run_id"] = run["id"]
    warm_detail: Dict[str, Dict[str, int]] = {}
    funnel: Dict[str, Any] = {}

    try:
        # ---- 1. freshness gate (P6) ---------------------------------------- #
        freshness = await check_core_freshness(conn, target=target)
        summary["core_freshness"] = {
            "fresh": freshness["fresh"],
            "stale_symbols": freshness["stale_symbols"],
            "universes": {k: {"symbols": v["symbols"], "current": v["current"],
                              "stale": v["stale"],
                              "oldest_latest_bar": v["oldest_latest_bar"],
                              "stale_symbol_list": v["stale_symbols"][:25]}
                          for k, v in freshness["universes"].items()}}
        if not freshness["fresh"]:
            summary["status"] = STATUS_BLOCKED_STALE
            summary["blocked_detail"] = (
                "canonical daily bars are behind the latest completed session. "
                "A research scan against stale benchmark bars produces a "
                "relative-strength reading that looks like evidence and is not.")
            if request_refresh_when_stale and not dry_run:
                summary["core_refresh_request"] = await request_core_refresh(
                    conn, freshness, target=target)
            # No provider figure is claimed: a blocked run attempted nothing,
            # and reporting "0 avoided" would read as a measurement.
            funnel = await rf.load_funnel(conn)
            summary["funnel"] = funnel
            return summary

        # ---- 2. canonical configuration, fail closed ----------------------- #
        config = await resolve_canonical_config(conn)
        summary["canonical_config"] = {
            "resolved": config["ok"], "min_price": config.get("min_price"),
            "config_hash": config.get("config_hash"),
            "pattern_code": config.get("pattern_code"),
            "experiment_code": config.get("experiment_code")}
        if not config["ok"] or config["min_price"] is None:
            summary["status"] = STATUS_BLOCKED_CONFIG
            summary["blocked_detail"] = (
                config.get("reason") or "min_price unusable")
            funnel = await rf.load_funnel(conn)
            summary["funnel"] = funnel
            return summary

        # ---- 3. discovery refresh (provider-touching, bounded) ------------- #
        if refresh_discovery and not dry_run:
            summary["discovery_refresh"] = await _refresh_discovery(conn)
            summary["provider_requests_used"] += \
                summary["discovery_refresh"].get("provider_requests", 0)

        # ---- 4. admit / update research symbols ---------------------------- #
        since = moment.date() - timedelta(days=discovery_days)
        admitted = await ri.admit_from_discovery(
            conn, since=since, limit=admit_limit, now=moment)
        summary["admission_pool"] = {
            "considered": admitted["considered"],
            "admitted": admitted["admitted"],
            "refreshed": admitted["refreshed"]}

        # ---- 5. cheap admission gates — ZERO provider requests ------------- #
        gates = await ri.evaluate_admissions(
            conn, min_price=config["min_price"], now=moment)
        summary["admission"] = {
            "evaluated": gates["evaluated"], "states": gates["states"],
            "min_price": gates["min_price"],
            "decisions_on_restricted_source":
                gates["decisions_on_restricted_source"],
            "rejected": gates["rejected_before_history"]}
        summary["provider_requests_avoided_by_admission"] = \
            gates["provider_requests_avoided"]

        if dry_run:
            funnel = await rf.load_funnel(
                conn, provider_calls_used=summary["provider_requests_used"],
                provider_calls_avoided=gates["provider_requests_avoided"])
            summary["funnel"] = funnel
            return summary

        # ---- 6. bounded warmup — the only history-provider stage (P7) ------ #
        remaining = max(0, int(provider_budget)
                        - summary["provider_requests_used"])
        warm = await _run_warmup(conn, limit=warm_limit,
                                 max_requests=remaining)
        summary["warmup"] = {
            # The list for a human, and the COUNT for the audit column. The
            # audit must never have to infer a number from a field shaped for
            # reading.
            "selected": warm["selected"],
            "warmups_attempted": len(warm["selected"]),
            "warmed": len(warm["warmed"]),
            "failed": len(warm["failed"]),
            "budget": remaining,
            "budget_exhausted": warm["budget_exhausted"],
            "locked": warm["locked"],
            "bars_inserted": sum(w.get("inserted", 0) for w in warm["warmed"]),
            "detail": [{"symbol": w["symbol"], "before": w["bars_before"],
                        "after": w["bars_after"]} for w in warm["warmed"]],
            "failures": [{"symbol": f["symbol"], "code": f["error_code"]}
                         for f in warm["failed"]]}
        summary["provider_requests_used"] += warm["provider_requests"]
        warm_detail = _warm_detail(warm)

        # ---- 7/8. readiness + scan (local bars only) ----------------------- #
        states = await ri.refresh_states(conn, now=moment)
        summary["readiness"] = states["states"]
        scans = await rs.run_research_scans(
            conn, session=target, limit=warm_limit, now=moment)
        summary["research_scan"] = {
            "session": scans["session"],
            "scanned": len(scans["scanned"]), "failed": len(scans["failed"]),
            "results": scans["scanned"], "failures": scans["failed"]}

        # ---- 9. candidate classification ----------------------------------- #
        candidates = await rs.reclassify_candidates(conn, now=moment)
        summary["candidates"] = candidates["states"]

        # ---- 10. funnel accounting, and it must add up (P0) ---------------- #
        funnel = await rf.load_funnel(
            conn, provider_calls_used=summary["provider_requests_used"],
            provider_calls_avoided=summary["provider_requests_avoided_by_admission"])
        summary["funnel"] = funnel

        # ---- 11. lazy enrichment — survivors only, research cohort (P3) ---- #
        if enrich:
            summary["enrichment"] = await _enrich(conn, now=moment)
        else:
            summary["enrichment"] = {"enriched": 0, "provider_requests": 0,
                                     "sources": {},
                                     "reason": "disabled_by_caller"}
        return summary

    except Exception as exc:                            # noqa: BLE001
        logger.exception("research lifecycle run failed")
        summary["status"] = rr.RUN_STATUS_FAILED
        summary["failure_summary"] = type(exc).__name__
        raise
    finally:
        summary["duration_seconds"] = round(time.monotonic() - started, 1)
        try:
            await rr.finish_run(conn, run["id"], summary=summary,
                                funnel=funnel or summary.get("funnel"),
                                warm_detail=warm_detail, now=moment)
        except Exception as exc:                        # noqa: BLE001
            # Persisting the audit must not be able to fail the run — the work
            # is already done and the provider requests are already spent. But
            # it must not leave the row at `running` either: a run whose audit
            # could not be written is a FAILED audit, and saying so is the only
            # way anyone finds out. (This is exactly what went wrong once: the
            # task reported success while its run row sat at `running`.)
            logger.exception("could not persist research lifecycle run")
            try:
                await rr.fail_run(conn, run["id"],
                                  reason=f"audit_write_failed:{type(exc).__name__}",
                                  now=moment)
            except Exception:                           # noqa: BLE001
                logger.exception("could not even record the audit failure")
        # The conservation check runs AFTER the row is safely on disk. A funnel
        # that does not add up must be loud, but it must not also be the reason
        # its own evidence was never written.
        if funnel:
            rf.assert_conservation(funnel)


# --------------------------------------------------------------------------- #
# stages that need optional credentials — isolated so a missing one is an
# ordinary "unavailable" rather than an exception that ends the run
# --------------------------------------------------------------------------- #

async def _refresh_discovery(conn) -> Dict[str, Any]:
    import app.external_discovery as ed
    from app.config import settings
    try:
        client = ed.FmpDiscoveryClient(settings.FMP_API_KEY)
    except ed.DiscoverySourceUnavailable:
        client = None
    universe = await ri.frozen_universe_symbols(conn, (ri.SCANNER_UNIVERSE_CODE,))
    discovery = await ed.refresh_discovery_candidates(
        conn, client, universe=universe)
    out = {k: discovery.get(k) for k in
           ("status", "reference_session_date", "inserted", "updated",
            "distinct_symbols")}
    out["provider_requests"] = DISCOVERY_REQUEST_COST if client else 0
    return out


async def _run_warmup(conn, *, limit: int, max_requests: int) -> Dict[str, Any]:
    from app.config import settings
    from app.providers import get_market_data_provider
    provider = (get_market_data_provider()
                if settings.MASSIVE_API_KEY else None)
    return await ri.run_warmup(conn, provider, limit=limit,
                               max_requests=max_requests)


async def _enrich(conn, *, now: datetime) -> Dict[str, Any]:
    from app.config import settings
    return await re_.enrich_research_candidates(
        conn, now=now,
        massive_api_key=(settings.MASSIVE_API_KEY or ""),
        sec_user_agent=(getattr(settings, "SEC_USER_AGENT", "") or ""))


__all__ = [
    "LIFECYCLE_CONTRACT_VERSION", "CORE_UNIVERSES", "DISCOVERY_REQUEST_COST",
    "STATUS_COMPLETED", "STATUS_BLOCKED_STALE", "STATUS_BLOCKED_CONFIG",
    "STATUS_DRY_RUN", "check_core_freshness", "request_core_refresh",
    "resolve_canonical_config", "run_lifecycle",
]
