"""The dynamic-discovery research pipeline, end to end and bounded.

    python -m ops.analysis.research_pipeline --admit
    python -m ops.analysis.research_pipeline --warm       [--limit 5]
    python -m ops.analysis.research_pipeline --scan
    python -m ops.analysis.research_pipeline --candidates
    python -m ops.analysis.research_pipeline --report

    DISCOVERED -> HISTORY_REQUIRED -> HISTORY_WARMING -> RESEARCH_READY
              -> RESEARCH_SCANNED     (and UNAVAILABLE / FAILED alongside)

WHAT PROBLEM THIS CLOSES
------------------------
Wave 2 could see 67 symbols the market noticed and the scanner could not, and
could do nothing with any of them: no local history, no analysis. This gets a
bounded few of them to the point where the SAME strategy that reads the frozen
25 can read them too — and stops there, deliberately.

WHAT IT IS NOT
--------------
Not universe expansion. Nothing here writes a universe, creates an experiment
pair, produces a canonical outcome, assigns an attention tier, or makes
anything ENTER-eligible. Those are guarantees held by table shape, by database
constraint and by the ingestion role's privileges — not by this docstring.

LICENCE
-------
Every symbol here arrived through an FMP discovery, whose plan is
internal_research_only. The research tables are not granted to the Product
API's role and nothing on this path reaches the product. See
`app/source_licensing.py`.

COST
----
Massive Basic is five requests a minute; this repository paces warmup at one
symbol per 75 seconds behind a machine-wide advisory lock. Five symbols is
therefore about six minutes and about five provider calls, and the run counts
and prints both rather than estimating them.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import app.research_ingest as ri
import app.research_scan as rs
import app.research_universe as ru
from app.prospective_session import resolve_latest_completed_session
from ops.analysis.intel_connection import intel_connection

#: How far back a discovery still counts for admission. Two weeks: long enough
#: that a symbol noticed on a quiet Friday is still admissible on Monday, short
#: enough that the pool is what the market is doing now.
DEFAULT_DISCOVERY_LOOKBACK_DAYS = 14


async def run_admit(days: int, limit: int) -> Dict[str, Any]:
    async with intel_connection() as conn:
        since = datetime.now(timezone.utc).date() - timedelta(days=days)
        summary = await ri.admit_from_discovery(conn, since=since, limit=limit)
        summary["states"] = (await ri.refresh_states(conn))["states"]
        return summary


async def run_warm(limit: int) -> Dict[str, Any]:
    from app.config import settings
    from app.providers import get_market_data_provider
    async with intel_connection() as conn:
        await ri.refresh_states(conn)
        provider = get_market_data_provider() if settings.MASSIVE_API_KEY else None
        summary = await ri.run_warmup(conn, provider, limit=limit)
        summary["states"] = (await ri.refresh_states(conn))["states"]
        return summary


async def run_scan(limit: int, session: Optional[date]) -> Dict[str, Any]:
    async with intel_connection() as conn:
        await ri.refresh_states(conn)
        target = session or resolve_latest_completed_session(
            datetime.now(timezone.utc))
        return await rs.run_research_scans(conn, session=target, limit=limit)


CANDIDATE_SQL = """
SELECT r.symbol, r.state, r.discovery_reasons, r.discovery_observation_count,
       r.latest_reference_session, r.first_actionable_session,
       r.history_daily_bars, r.research_scanned_at,
       s.verdict, s.structure_state, s.setup_state, s.reason_code,
       s.benchmark_relative, s.sector_state, s.scan_session, s.bars_evaluated,
       s.rejection_reason
FROM public.research_symbols r
LEFT JOIN LATERAL (
    SELECT * FROM public.research_scan_results x
    WHERE x.symbol = r.symbol ORDER BY x.scan_session DESC LIMIT 1
) s ON true
ORDER BY r.latest_reference_session DESC, r.symbol
"""


async def run_candidates() -> Dict[str, Any]:
    async with intel_connection() as conn:
        rows = [dict(r) for r in await conn.fetch(CANDIDATE_SQL)]
        latest = await conn.fetchval(
            "SELECT max(reference_session_date) FROM public.external_discovery_candidates")
    out = []
    for row in rows:
        reasons = ru.research_candidate_reasons(
            row, latest_reference_session=latest)
        out.append({**row, "candidate_reasons": reasons,
                    "is_candidate": ru.is_research_candidate(
                        row, latest_reference_session=latest)})
    return {"latest_reference_session": latest, "rows": out}


def _print_report(payload: Dict[str, Any]) -> None:
    rows = payload["rows"]
    print(f"\nResearch domain — {len(rows)} symbols "
          f"(latest market session seen: {payload['latest_reference_session']})\n")
    print(f"  {'SYMBOL':<8} {'STATE':<18} {'BARS':>6} {'VERDICT':<9} "
          f"{'BENCH':<15} {'SECTOR':<18} REASONS")
    for row in rows:
        print(f"  {row['symbol']:<8} {row['state']:<18} "
              f"{row['history_daily_bars']:>6} "
              f"{str(row['verdict'] or '-'):<9} "
              f"{str(row['benchmark_relative'] or '-'):<15} "
              f"{str(row['sector_state'] or '-'):<18} "
              f"{','.join(row['candidate_reasons']) or '-'}")
    candidates = [r for r in rows if r["is_candidate"]]
    print(f"\n  {len(candidates)} worth a human looking at.")
    print("  Reason codes, not a score, and not a recommendation: each one "
          "names something that was OBSERVED, and none of them claims one "
          "caused another.")
    print("  Ordering when history is scarce is lexicographic and explainable:")
    for dim, why in ru.PRIORITY_DIMENSIONS:
        print(f"    - {dim:<26} {why}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admit", action="store_true")
    parser.add_argument("--warm", action="store_true")
    parser.add_argument("--scan", action="store_true")
    parser.add_argument("--candidates", action="store_true")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--days", type=int, default=DEFAULT_DISCOVERY_LOOKBACK_DAYS)
    parser.add_argument("--limit", type=int,
                        default=ru.MAX_NEW_RESEARCH_SYMBOLS_PER_RUN)
    parser.add_argument("--session", type=date.fromisoformat, default=None)
    args = parser.parse_args()
    if not any((args.admit, args.warm, args.scan, args.candidates, args.report)):
        parser.error("choose --admit, --warm, --scan, --candidates or --report")

    if args.admit:
        print(json.dumps(asyncio.run(run_admit(args.days, args.limit)),
                         indent=2, default=str))
    if args.warm:
        print(json.dumps(asyncio.run(run_warm(args.limit)), indent=2, default=str))
    if args.scan:
        print(json.dumps(asyncio.run(run_scan(args.limit, args.session)),
                         indent=2, default=str))
    if args.candidates or args.report:
        _print_report(asyncio.run(run_candidates()))


if __name__ == "__main__":
    main()
