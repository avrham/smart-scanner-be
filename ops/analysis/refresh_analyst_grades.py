"""Refresh analyst grade CHANGE events for the frozen universe.

    python -m ops.analysis.refresh_analyst_grades --refresh
    python -m ops.analysis.refresh_analyst_grades --refresh --lookback-days 3650
    python -m ops.analysis.refresh_analyst_grades --report --days 30

WHAT QUESTION THIS ANSWERS
--------------------------
"Who changed their mind about one of our 25, and when." The provider returns a
symbol's COMPLETE grade history on every call — back to 2012 for a mega-cap —
so `--lookback-days` bounds what is STORED, not what is fetched: run it wide
once to build the descriptive history, narrow every day after.

LICENCE — WHY THIS SCRIPT STOPS AT THE DATABASE
-----------------------------------------------
FMP's individual plans are personal and non-commercial and forbid integrating
the data into tools accessible by third parties. Nothing this script writes is
exposed through the Product API or the UI; the Product API's database role is
not granted `analyst_grade_events` at all, so the boundary is enforced by the
database rather than by anybody remembering it.

THE EXPERIMENT BOUNDARY
-----------------------
It reads the frozen universe to know WHICH symbols to ask about and to mark
which side of the line each row fell on. It never writes to a universe, never
enqueues a scan, and never touches a scanner relation — its database role holds
no privilege on any of them.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Set

import app.analyst_events as ae
import app.external_discovery as ed
from app.config import settings
from ops.analysis.intel_connection import intel_connection

UNIVERSE_SQL = """
SELECT s.symbol
FROM public.history_warmup_universe_symbols s
JOIN public.history_warmup_universes u ON u.id = s.universe_id
WHERE u.universe_code = $1
ORDER BY s.ordinal
"""

SCANNER_UNIVERSE_CODE = "WYCKOFF-HISTORY-WARMUP-QUALIFICATION"


async def _universe(conn) -> list:
    rows = await conn.fetch(UNIVERSE_SQL, SCANNER_UNIVERSE_CODE)
    return [str(r["symbol"]).upper() for r in rows]


async def run_refresh(lookback_days: int) -> dict:
    async with intel_connection() as conn:
        symbols = await _universe(conn)
        try:
            client = ed.FmpStableClient(settings.FMP_API_KEY)
        except ed.DiscoverySourceUnavailable:
            # No credential is the ordinary case, not an error.
            client = None
        return await ae.refresh_analyst_grades(
            conn, client, symbols=symbols, universe=set(symbols),
            lookback_days=lookback_days)


async def run_report(days: int) -> tuple:
    async with intel_connection() as conn:
        since = datetime.now(timezone.utc).date() - timedelta(days=days)
        return (await ae.change_counts(conn, since=since),
                await ae.recent_changes(conn, since=since,
                                        directional_only=True, limit=40))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--lookback-days", type=int,
                        default=ae.DEFAULT_LOOKBACK_DAYS,
                        help=f"store events newer than this "
                             f"(default {ae.DEFAULT_LOOKBACK_DAYS})")
    parser.add_argument("--days", type=int, default=30,
                        help="report lookback in calendar days (default 30)")
    args = parser.parse_args()
    if not args.refresh and not args.report:
        parser.error("choose --refresh, --report, or both")

    if args.refresh:
        summary = asyncio.run(run_refresh(args.lookback_days))
        # Per-symbol detail is verbose and rarely what you want on screen; the
        # failures are what matter, and they are named.
        compact = {k: v for k, v in summary.items() if k != "symbols"}
        compact["symbols_with_writes"] = sum(
            1 for v in summary.get("symbols", {}).values()
            if isinstance(v, dict) and v.get("inserted"))
        print(json.dumps(compact, indent=2, default=str))

    if args.report:
        counts, recent = asyncio.run(run_report(args.days))
        print(f"\nAnalyst actions on the frozen universe, last {args.days} days:\n")
        print(f"  {'SYMBOL':<8} {'UP':>4} {'DOWN':>5} {'MAINT':>6} {'TOTAL':>6}  LAST")
        for row in counts:
            print(f"  {row['symbol']:<8} {row['upgrades']:>4} "
                  f"{row['downgrades']:>5} {row['maintains']:>6} "
                  f"{row['total']:>6}  {row['last_event']}")
        if recent:
            print("\nDirectional changes (upgrade/downgrade only):\n")
            for row in recent:
                print(f"  {row['event_date']}  {row['symbol']:<8} "
                      f"{row['action_normalized']:<10} "
                      f"{(row['previous_grade'] or '-'):<16} -> "
                      f"{(row['new_grade'] or '-'):<16} {row['grading_company']}")
        print("\nCounts, not a score. Nothing here reaches the Product API, "
              "the UI, or any scanner path.")


if __name__ == "__main__":
    main()
