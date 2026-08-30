"""Refresh the market calendar from the two entitled first-party publishers.

    python -m ops.analysis.refresh_macro_calendar --refresh
    python -m ops.analysis.refresh_macro_calendar --report --days 30

WHAT QUESTION THIS ANSWERS
--------------------------
"Is a market-wide scheduled event close?" The scanner reads 25 charts and can
say a great deal about each of them; it cannot say that the Fed meets on
Wednesday, because that fact is not in any bar. This buys that back from the
agencies that publish it, at one HTTP request each per day.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It never says an event is risky, never assigns it a number, never changes a
verdict, an attention tier, an ordering or a market-regime classification. The
proximity vocabulary is computed from a calendar and stops there.

SOURCES AND LICENCE
-------------------
federalreserve.gov and bea.gov. Works of the U.S. Government are not subject to
copyright protection in the United States (17 U.S.C. 105), which is why — alone
among the external sources in this repository — this one is displayable in the
product.

Connects as `smart_scanner_market_intel` when MARKET_INTEL_DATABASE_URL is set,
otherwise via the ordinary selector. No credential is needed by either source.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, timedelta, timezone

import app.macro_calendar as mc
import app.macro_ingest as mi
from ops.analysis.intel_connection import intel_connection

REPORT_SQL = """
SELECT source, event_type, title, scheduled_date, scheduled_start_date,
       scheduled_time_local, source_listing, has_press_conference,
       has_projections, first_observed_at, observed_at
FROM public.macro_events
WHERE scheduled_date BETWEEN $1 AND $2
ORDER BY scheduled_date, event_type
"""


async def run_refresh() -> dict:
    async with intel_connection() as conn:
        return await mi.refresh_macro_calendar(conn, mi.MacroCalendarClient())


async def run_report(days: int) -> list:
    async with intel_connection() as conn:
        today = datetime.now(timezone.utc).date()
        rows = await conn.fetch(REPORT_SQL,
                                today - timedelta(days=mc.WINDOW_BACK_DAYS),
                                today + timedelta(days=days))
        return [dict(r) for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true",
                        help="fetch both calendars and upsert them")
    parser.add_argument("--report", action="store_true",
                        help="print the stored calendar around today")
    parser.add_argument("--days", type=int, default=30,
                        help="report look-ahead in calendar days (default 30)")
    args = parser.parse_args()
    if not args.refresh and not args.report:
        parser.error("choose --refresh, --report, or both")

    if args.refresh:
        print(json.dumps(asyncio.run(run_refresh()), indent=2, default=str))

    if args.report:
        rows = asyncio.run(run_report(args.days))
        if not rows:
            print("\nNo macro events stored for this window.")
            return
        today = datetime.now(timezone.utc).date()
        print(f"\nScheduled market-wide events (as of {today.isoformat()}):\n")
        print(f"  {'DATE':<12} {'TYPE':<20} {'IN':>5} {'PROXIMITY':<20} "
              f"{'SRC':<16} TITLE")
        for row in rows:
            days_until = (row["scheduled_date"] - today).days
            proximity = mc.classify_proximity(days_until)
            listing = "" if row["source_listing"] == "listed" else " [withdrawn]"
            print(f"  {row['scheduled_date'].isoformat():<12} "
                  f"{row['event_type']:<20} {days_until:>5} {proximity:<20} "
                  f"{row['source']:<16} {row['title'][:56]}{listing}")
        print("\nProximity is a calendar fact. It is not a risk level, and "
              "nothing in the scanner reads it.")


if __name__ == "__main__":
    main()
