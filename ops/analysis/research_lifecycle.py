"""ONE bounded research lifecycle for staging, in the order that makes sense.

    python -m ops.analysis.research_lifecycle --run [--admit-limit 20]
    python -m ops.analysis.research_lifecycle --run --dry-run
    python -m ops.analysis.research_lifecycle --summary

        LATEST COMPLETED SESSION
                 |
        [gate] canonical bars fresh?      <- blocks if not, never proceeds stale
                 |
        refresh discovery
                 |
        admit / update research symbols
                 |
        cheap admission gates             <- ZERO provider requests
                 |
        bounded eligible cohort
                 |
        history warmup                    <- the ONLY provider-touching stage
                 |
        research readiness
                 |
        research scan                     <- local bars only
                 |
        candidate classification
                 |
        one summary

WHY ONE COMMAND AND NOT SIX CRON JOBS
-------------------------------------
The stages are not independent: scanning against stale benchmark bars produces
a relative-strength reading that is quietly wrong, and warming before admission
spends the requests admission exists to save. Ordering IS the feature, so it
lives in one place that can be read top to bottom.

It invents no scheduler. It is a bounded, idempotent, lease-protected run that
an operator or a future dispatcher invokes; the warmup stage takes the same
machine-wide advisory lock the frozen universe's warmup takes, so the two can
never reach the provider at once.

THE FRESHNESS GATE IS A GATE
----------------------------
If the frozen 25 and the 10 reference symbols are not current through the
latest completed session, the lifecycle reports `blocked_stale_core_history`
and stops. It does not "continue with what it has": a research scan whose
benchmark series ends three sessions early yields a relative-strength category
that looks like evidence and is not.

BUDGET
------
Every provider request is counted, the ceiling is enforced rather than hoped
for, and the summary reports both what was spent and what admission avoided.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import app.research_admission as ra
import app.research_ingest as ri
import app.research_scan as rs
import app.research_universe as ru
from app.prospective_session import resolve_latest_completed_session
from ops.analysis.intel_connection import intel_connection

LIFECYCLE_CONTRACT_VERSION = "smart_scanner_research_lifecycle.v1"

STATUS_COMPLETED = "completed"
STATUS_BLOCKED_STALE = "blocked_stale_core_history"
STATUS_BLOCKED_CONFIG = "blocked_canonical_config_unavailable"
STATUS_DRY_RUN = "dry_run"

#: The two universes whose bars every research scan depends on: the frozen 25
#: (nothing directly, but they are the canonical freshness signal for the
#: pipeline) and the reference market (SPY and the sector ETFs, which the
#: benchmark-relative context reads directly).
CORE_UNIVERSES = ("WYCKOFF-HISTORY-WARMUP-QUALIFICATION",
                  "SMART-SCANNER-REFERENCE-MARKET-V1")

CORE_FRESHNESS_SQL = """
SELECT u.universe_code,
       count(*)::int AS symbols,
       count(*) FILTER (
         WHERE (SELECT max(b.trading_date) FROM public.daily_bars b
                WHERE b.symbol = s.symbol) >= $2)::int AS current_symbols,
       min((SELECT max(b.trading_date) FROM public.daily_bars b
            WHERE b.symbol = s.symbol)) AS oldest_latest_bar
FROM public.history_warmup_universe_symbols s
JOIN public.history_warmup_universes u ON u.id = s.universe_id
WHERE u.universe_code = ANY($1::text[])
GROUP BY u.universe_code
"""


async def check_core_freshness(conn, *, target: date) -> Dict[str, Any]:
    """Are the canonical bars current through the latest completed session?

    Reports per universe rather than as one boolean: "the reference market is
    two sessions behind" and "one of the 25 is behind" are different operator
    problems and the summary should not blur them.
    """
    rows = [dict(r) for r in await conn.fetch(
        CORE_FRESHNESS_SQL, list(CORE_UNIVERSES), target)]
    universes = {
        r["universe_code"]: {
            "symbols": r["symbols"], "current": r["current_symbols"],
            "stale": r["symbols"] - r["current_symbols"],
            "oldest_latest_bar": r["oldest_latest_bar"]}
        for r in rows}
    stale = sum(u["stale"] for u in universes.values())
    return {"target_completed_session": target, "universes": universes,
            "stale_symbols": stale, "fresh": stale == 0 and bool(universes)}


async def resolve_canonical_min_price(conn) -> Dict[str, Any]:
    """The minimum, from the SAME resolution the research scan will use.

    Fails closed. Admission reading one configuration while the scan reads
    another is the drift this whole milestone exists to remove, so if the
    canonical config cannot be resolved the lifecycle does not run at all.
    """
    from app.workers.patterns.config import (ConfigUnavailable,
                                             resolve_pattern_config)
    from app.workers.shadow.experiments import get_experiment
    from app.workers.strategies.registry import get_strategy

    experiment = get_experiment(rs.RESEARCH_EXPERIMENT_CODE)
    strategy = get_strategy(experiment.candidate_pattern_code)
    try:
        config = await resolve_pattern_config(
            experiment.candidate_pattern_code, strategy.default_config(),
            conn=conn, require_db=True)
    except ConfigUnavailable as exc:
        return {"ok": False, "reason": str(exc), "min_price": None}
    return {"ok": True, "min_price": ra.resolve_min_price(config),
            "pattern_code": experiment.candidate_pattern_code}


async def run_lifecycle(*, admit_limit: int, warm_limit: int,
                        discovery_days: int, dry_run: bool,
                        refresh_discovery: bool,
                        now: Optional[datetime] = None) -> Dict[str, Any]:
    started = time.monotonic()
    moment = now or datetime.now(timezone.utc)
    target = resolve_latest_completed_session(moment)

    summary: Dict[str, Any] = {
        "contract_version": LIFECYCLE_CONTRACT_VERSION,
        "started_at": moment.isoformat(),
        "target_completed_session": target.isoformat(),
        "status": STATUS_DRY_RUN if dry_run else STATUS_COMPLETED,
        "provider_requests_used": 0,
        "provider_requests_avoided_by_admission": 0,
    }

    async with intel_connection() as conn:
        # ---- 1. freshness gate ------------------------------------------- #
        freshness = await check_core_freshness(conn, target=target)
        summary["core_freshness"] = {
            "fresh": freshness["fresh"],
            "stale_symbols": freshness["stale_symbols"],
            "universes": {k: {kk: (vv.isoformat() if isinstance(vv, date) else vv)
                              for kk, vv in v.items()}
                          for k, v in freshness["universes"].items()}}
        if not freshness["fresh"]:
            summary["status"] = STATUS_BLOCKED_STALE
            summary["blocked_detail"] = (
                "canonical daily bars are behind the latest completed session; "
                "run ops.analysis.refresh_daily_history --refresh first. A "
                "research scan against stale benchmark bars produces a "
                "relative-strength reading that looks like evidence and is not.")
            summary["duration_seconds"] = round(time.monotonic() - started, 1)
            return summary

        # ---- 2. canonical configuration, fail closed --------------------- #
        config = await resolve_canonical_min_price(conn)
        summary["canonical_config"] = {
            "resolved": config["ok"], "min_price": config.get("min_price"),
            "pattern_code": config.get("pattern_code")}
        if not config["ok"] or config["min_price"] is None:
            summary["status"] = STATUS_BLOCKED_CONFIG
            summary["blocked_detail"] = config.get("reason") or "min_price unusable"
            summary["duration_seconds"] = round(time.monotonic() - started, 1)
            return summary

        # ---- 3. discovery refresh (optional, provider-touching) ---------- #
        if refresh_discovery and not dry_run:
            import app.external_discovery as ed
            from app.config import settings
            try:
                client = ed.FmpDiscoveryClient(settings.FMP_API_KEY)
            except ed.DiscoverySourceUnavailable:
                client = None
            universe = await ri.frozen_universe_symbols(
                conn, (ri.SCANNER_UNIVERSE_CODE,))
            discovery = await ed.refresh_discovery_candidates(
                conn, client, universe=universe)
            summary["discovery_refresh"] = {
                k: discovery.get(k) for k in
                ("status", "reference_session_date", "inserted", "updated",
                 "distinct_symbols")}
            summary["provider_requests_used"] += 3 if client else 0

        # ---- 4. admit / update research symbols -------------------------- #
        since = moment.date() - timedelta(days=discovery_days)
        admitted = await ri.admit_from_discovery(
            conn, since=since, limit=admit_limit, now=moment)
        summary["admission_pool"] = {
            "considered": admitted["considered"],
            "admitted": admitted["admitted"],
            "refreshed": admitted["refreshed"]}

        # ---- 5. cheap admission gates — ZERO provider requests ----------- #
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
            summary["duration_seconds"] = round(time.monotonic() - started, 1)
            return summary

        # ---- 6. bounded warmup — the only provider-touching stage -------- #
        from app.config import settings
        from app.providers import get_market_data_provider
        provider = (get_market_data_provider()
                    if settings.MASSIVE_API_KEY else None)
        warm = await ri.run_warmup(conn, provider, limit=warm_limit)
        summary["warmup"] = {
            "selected": warm["selected"], "warmed": len(warm["warmed"]),
            "failed": len(warm["failed"]),
            "budget_exhausted": warm["budget_exhausted"],
            "locked": warm["locked"],
            "bars_inserted": sum(w.get("inserted", 0) for w in warm["warmed"]),
            "detail": [{"symbol": w["symbol"], "before": w["bars_before"],
                        "after": w["bars_after"]} for w in warm["warmed"]],
            "failures": [{"symbol": f["symbol"], "code": f["error_code"]}
                         for f in warm["failed"]]}
        summary["provider_requests_used"] += warm["provider_requests"]

        # ---- 7/8. readiness + scan (local bars only) --------------------- #
        states = await ri.refresh_states(conn, now=moment)
        summary["readiness"] = states["states"]
        scans = await rs.run_research_scans(
            conn, session=target, limit=warm_limit, now=moment)
        summary["research_scan"] = {
            "session": scans["session"],
            "scanned": len(scans["scanned"]), "failed": len(scans["failed"]),
            "results": scans["scanned"], "failures": scans["failed"]}

        # ---- 9. candidate classification --------------------------------- #
        candidates = await rs.reclassify_candidates(conn, now=moment)
        summary["candidates"] = candidates["states"]

        # ---- 10. LAZY enrichment — survivors only ------------------------ #
        summary["enrichment"] = await enrich_candidates(conn, now=moment)

    summary["duration_seconds"] = round(time.monotonic() - started, 1)
    return summary


# --------------------------------------------------------------------------- #
# lazy catalyst enrichment — survivors only, and currently DEFERRED
#
# The stage exists, is bounded, and runs only on symbols that survived the
# research screen. What it does NOT do is fetch, and the reason is a real
# architectural boundary rather than caution.
#
# WHAT WAS TRIED, AND WHAT IT REVEALED
# ------------------------------------
# The catalyst refreshers all take an arbitrary symbol list, so on the surface
# nothing assumes the frozen 25. The first live run reached this stage with one
# survivor (ONDS), called `refresh_sec_filings`, and was refused by row-level
# security. That refusal is CORRECT, and it is the finding:
#
#   `refresh_sec_filings` writes `catalyst_source_state` under `sec_edgar` —
#   a SHARED, per-source freshness row that the Product API reads to decide
#   whether the SEC dimension is trustworthy FOR THE FROZEN 25. A research run
#   over one symbol writing "sec_edgar: ok, symbols_covered=1" would tell the
#   product its SEC coverage was fresh when it had been refreshed for a symbol
#   the product cannot even see.
#
# So the infrastructure does assume one cohort — not in its symbol list, but in
# its freshness accounting, which is the part that matters. Widening the
# research role's RLS to let it write that row would trade a real product
# guarantee for a convenience.
#
# WHAT IT WOULD TAKE
# ------------------
# A per-cohort source-state identity (e.g. `sec_edgar:research`) so research
# and product freshness are separate facts. That is a schema and contract
# change to the catalyst domain, is not what this milestone is for, and is
# worth doing only once there are enough survivors to enrich — today there is
# one.
#
# The other sources are deferred on cost, not on this: earnings and news reach
# the market-data provider whose request budget the history warmup needs, and
# analyst grades are FMP internal-only and entitled per symbol.
# --------------------------------------------------------------------------- #

#: Hard cap for when this is switched on. Ten survivors is already more than a
#: person reviews in a sitting.
MAX_ENRICHED_SYMBOLS_PER_RUN = 10

ENRICHMENT_DEFERRED_REASON = "shared_source_state_assumes_frozen_universe"


async def enrich_candidates(conn, *, now: Optional[datetime] = None,
                            limit: int = MAX_ENRICHED_SYMBOLS_PER_RUN,
                            ) -> Dict[str, Any]:
    """Name the survivors that WOULD be enriched, and why nothing is fetched.

    Deliberately makes no request. Reporting the eligible set keeps the stage
    honest and measurable — when the per-cohort source-state identity exists,
    this is where it plugs in.
    """
    rows = await conn.fetch(
        "SELECT symbol FROM public.research_symbols "
        "WHERE candidate_state = 'research_candidate' "
        "ORDER BY latest_reference_session DESC, symbol LIMIT $1", limit)
    symbols = [r["symbol"] for r in rows]
    return {
        "eligible_symbols": symbols,
        "enriched": 0,
        "provider_requests": 0,
        "sources": {
            "sec_filings": {
                "status": "deferred",
                "reason": ENRICHMENT_DEFERRED_REASON,
                "detail": ("refresh_sec_filings writes the SHARED `sec_edgar` "
                           "freshness row the Product API reads for the frozen "
                           "25; a research run must not claim that coverage"),
            },
            "earnings": {"status": "deferred",
                         "reason": "shares the provider request budget the "
                                   "history warmup needs"},
            "company_news": {"status": "deferred",
                             "reason": "shares the provider request budget"},
            "analyst_grades": {"status": "deferred",
                               "reason": "FMP internal-only and entitled per "
                                         "symbol"},
        },
    }


FUNNEL_SQL = """
SELECT count(*)::int AS discovered,
       count(*) FILTER (WHERE admission_state = 'eligible_for_history')::int AS admission_passed,
       count(*) FILTER (WHERE admission_state = 'rejected_before_history')::int AS admission_rejected,
       count(*) FILTER (WHERE admission_state = 'insufficient_admission_data')::int AS admission_unknown,
       count(*) FILTER (WHERE state = 'research_ready' OR state = 'research_scanned')::int AS history_ready,
       count(*) FILTER (WHERE state = 'research_scanned')::int AS research_scanned,
       count(*) FILTER (WHERE candidate_state = 'research_candidate')::int AS research_candidate,
       count(*) FILTER (WHERE candidate_state = 'scanned_not_candidate')::int AS scanned_not_candidate,
       count(*) FILTER (WHERE state = 'unavailable')::int AS unavailable,
       count(*) FILTER (WHERE state = 'failed')::int AS failed,
       sum(warmup_provider_requests)::int AS provider_requests_lifetime
FROM public.research_symbols
"""

DROPOFF_SQL = """
SELECT coalesce(admission_reason, '-') AS admission_reason,
       coalesce(candidate_reason, '-') AS candidate_reason,
       coalesce(warmup_last_error_code, '-') AS warmup_error,
       count(*)::int AS symbols
FROM public.research_symbols
GROUP BY 1,2,3 ORDER BY 4 DESC
"""


async def funnel() -> Dict[str, Any]:
    async with intel_connection() as conn:
        row = dict(await conn.fetchrow(FUNNEL_SQL))
        drops = [dict(r) for r in await conn.fetch(DROPOFF_SQL)]
    return {"funnel": row, "dropoff": drops}


def _print_funnel(payload: Dict[str, Any]) -> None:
    f = payload["funnel"]
    print("\nRESEARCH FUNNEL\n")
    steps = [("DISCOVERED (admitted to research)", f["discovered"]),
             ("  ADMISSION PASSED", f["admission_passed"]),
             ("  admission unknown (proceeds)", f["admission_unknown"]),
             ("  admission REJECTED (no provider call)", f["admission_rejected"]),
             ("HISTORY READY", f["history_ready"]),
             ("RESEARCH SCANNED", f["research_scanned"]),
             ("  RESEARCH CANDIDATE", f["research_candidate"]),
             ("  scanned, not a candidate", f["scanned_not_candidate"]),
             ("unavailable", f["unavailable"]),
             ("failed", f["failed"])]
    for label, value in steps:
        print(f"  {label:<42} {value:>5}")
    print(f"\n  lifetime provider requests on research warmup: "
          f"{f['provider_requests_lifetime'] or 0}")
    print("\n  DROP-OFF BY REASON\n")
    print(f"  {'ADMISSION':<30} {'CANDIDATE':<24} {'WARMUP ERROR':<32} COUNT")
    for row in payload["dropoff"]:
        print(f"  {row['admission_reason']:<30} {row['candidate_reason']:<24} "
              f"{row['warmup_error']:<32} {row['symbols']:>5}")
    print("\n  Reason codes, never a score. `admission REJECTED` is the count "
          "of provider requests this gate did not have to spend.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="stop after admission; touch no provider")
    parser.add_argument("--refresh-discovery", action="store_true",
                        help="also refresh the movers feeds (3 provider calls)")
    parser.add_argument("--admit-limit", type=int,
                        default=ru.MAX_NEW_RESEARCH_SYMBOLS_PER_RUN)
    parser.add_argument("--warm-limit", type=int,
                        default=ru.MAX_WARMUP_SYMBOLS_PER_RUN)
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    if not (args.run or args.summary):
        parser.error("choose --run or --summary")

    if args.run:
        print(json.dumps(asyncio.run(run_lifecycle(
            admit_limit=args.admit_limit, warm_limit=args.warm_limit,
            discovery_days=args.days, dry_run=args.dry_run,
            refresh_discovery=args.refresh_discovery)),
            indent=2, default=str))
    if args.summary:
        _print_funnel(asyncio.run(funnel()))


if __name__ == "__main__":
    main()
