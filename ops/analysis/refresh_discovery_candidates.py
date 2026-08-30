"""Bounded FMP market-movers refresh + the research read on top of it.

    python -m ops.analysis.refresh_discovery_candidates --refresh
    python -m ops.analysis.refresh_discovery_candidates --report --days 10
    python -m ops.analysis.refresh_discovery_candidates --cross-reference
    python -m ops.analysis.refresh_discovery_candidates \
        --backfill-reference-sessions --dry-run

WHAT QUESTION THIS ANSWERS
--------------------------
The scanner holds a frozen 25-symbol universe, which is what makes the
prospective experiment interpretable. The price of that choice is a blind
spot: it cannot see what the wider market is watching, and it can never
surface a symbol it does not already hold. This script buys that back cheaply
— without touching the experiment.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It never adds a symbol to any universe, never enqueues a scan, never writes to
a scanner relation and never reaches the Product API. `--report` is reading
material for a human deciding what to investigate next, not an input to
anything automated. The moment a discovered symbol could enter the frozen
universe by itself, the experiment stops being a prospective experiment.

LICENCE
-------
FMP's individual plans are personal and non-commercial, and forbid integrating
the data into tools accessible by third parties. Ingesting for our own
research is a different act from publishing, so this path stops at the
database: nothing it writes is exposed through the Product API or the UI.

Connects with the same DSN selection every other component uses; the FMP key
comes from configuration and is never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Set

import app.external_discovery as ed
from app.config import settings
from ops.analysis.intel_connection import intel_connection

UNIVERSE_SQL = """
SELECT s.symbol
FROM public.history_warmup_universe_symbols s
JOIN public.history_warmup_universes u ON u.id = s.universe_id
WHERE u.universe_code = $1
"""

SCANNER_UNIVERSE_CODE = "WYCKOFF-HISTORY-WARMUP-QUALIFICATION"


async def _universe(conn) -> Optional[Set[str]]:
    """The frozen universe, or None if it cannot be read.

    None rather than an empty set on failure: an empty set would silently mark
    every discovered symbol as outside the universe, which is the same value
    the honest answer produces and would therefore hide the failure.
    """
    try:
        rows = await conn.fetch(UNIVERSE_SQL, SCANNER_UNIVERSE_CODE)
        return {str(r["symbol"]).upper() for r in rows} or None
    except Exception:
        return None


async def run_refresh(limit: int) -> dict:
    async with intel_connection() as conn:
        universe = await _universe(conn)
        try:
            client = ed.FmpDiscoveryClient(settings.FMP_API_KEY)
        except ed.DiscoverySourceUnavailable:
            # No credential is the ordinary case, not an error. The refresh
            # records `unavailable` and returns a summary either way.
            client = None
        return await ed.refresh_discovery_candidates(
            conn, client, universe=universe, limit=limit)


async def run_report(days: int, limit: int) -> list:
    async with intel_connection() as conn:
        since = (datetime.now(timezone.utc).date() - timedelta(days=days))
        return await ed.symbols_worth_investigating(
            conn, since=since, limit=limit)


async def run_cross_reference(session: Optional[date]) -> dict:
    async with intel_connection() as conn:
        return await ed.cross_reference_universe(
            conn, reference_session_date=session)


async def run_backfill(dry_run: bool) -> dict:
    """Migration 025's backfill, using the ONE canonical trading calendar.

    Deliberately not SQL inside the migration: the inference needs weekends,
    the federal market holidays, Good Friday and the observed-day rules, and
    that calendar exists once in this repository. A second copy in SQL could
    drift, and a drifting calendar is exactly how a row gets labelled with a
    session that never happened.
    """
    async with intel_connection() as conn:
        return await ed.backfill_reference_sessions(conn, dry_run=dry_run)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true",
                        help="fetch the entitled movers feeds and upsert them")
    parser.add_argument("--report", action="store_true",
                        help="symbols outside the frozen universe, by persistence")
    parser.add_argument("--cross-reference", action="store_true",
                        help="one market session, sorted against what we hold")
    parser.add_argument("--session", type=date.fromisoformat, default=None,
                        help="ISO REFERENCE session date for --cross-reference "
                             "(default: the latest market session stored)")
    parser.add_argument("--backfill-reference-sessions", action="store_true",
                        help="migration 025: fill reference_session_date from "
                             "observed_at and the trading calendar")
    parser.add_argument("--dry-run", action="store_true",
                        help="with --backfill-reference-sessions: print the "
                             "proposed mapping and write nothing")
    parser.add_argument("--days", type=int, default=10,
                        help="report lookback in calendar days (default 10)")
    parser.add_argument("--limit", type=int, default=ed.DEFAULT_LIST_LIMIT,
                        help=f"ranks to keep per list (default {ed.DEFAULT_LIST_LIMIT})")
    args = parser.parse_args()

    if not (args.refresh or args.report or args.cross_reference
            or args.backfill_reference_sessions):
        parser.error("choose --refresh, --report, --cross-reference, "
                     "--backfill-reference-sessions, or several")

    if args.backfill_reference_sessions:
        summary = asyncio.run(run_backfill(args.dry_run))
        print("\nMigration 025 backfill "
              f"({'DRY RUN — nothing written' if summary['dry_run'] else 'APPLIED'}):\n")
        print(f"  rows needing a reference session : {summary['candidates']}")
        print(f"  rows with an UNSAFE inference    : {summary['unsafe']}")
        print(f"  rows updated                     : {summary['updated']}")
        if summary["distinct_mappings"]:
            print("\n  Proposed mapping — one line per distinct fetch:\n")
            print(f"    {'OBSERVED_AT (UTC)':<34} {'REFERENCE (describes)':<22} "
                  f"ACTIONABLE (session_date)")
            for observed, reference, actionable in summary["distinct_mappings"]:
                print(f"    {observed:<34} {reference:<22} {actionable}")
        if summary["unsafe"]:
            print("\n  REFUSED: an inference landed after its own actionable "
                  "session. Nothing was written — a partial backfill is worse "
                  "than none.")
        print("\n  Inference is from observed_at + the US equity trading "
              "calendar. It is NOT a provider timestamp: FMP's movers feeds "
              "carry none, which is why every row also stores the basis "
              "'inferred_from_observation_time'.")

    if args.refresh:
        summary = asyncio.run(run_refresh(args.limit))
        print(json.dumps(summary, indent=2, default=str))

    if args.report:
        rows = asyncio.run(run_report(args.days, 25))
        if not rows:
            print("\nNo discovery candidates outside the frozen universe yet.")
            return
        print(f"\nSymbols the market noticed that we never scan "
              f"(last {args.days} days, by persistence):\n")
        print(f"  {'SYMBOL':<10} {'SESSIONS':>8} {'HITS':>5} {'BEST':>5}  LISTS")
        for row in rows:
            lists = ",".join(row["lists"])
            print(f"  {row['symbol']:<10} {row['sessions_seen']:>8} "
                  f"{row['appearances']:>5} {row['best_rank']:>5}  {lists}")
        print("\nSessions counted are MARKET sessions the symbol appeared in, "
              "not sessions it became actionable in.")
        print("\nPersistence first, not the size of any single move: a stock "
              "that gapped once is noise, one in the cohort four sessions "
              "running is a question. Nothing here enters the frozen universe.")

    if args.cross_reference:
        report = asyncio.run(run_cross_reference(args.session))
        if report["reference_session_date"] is None:
            print("\nNo discovery candidates stored yet.")
            return
        print(f"\nDiscovery cross-reference — market session "
              f"{report['reference_session_date']} "
              f"(first actionable {report['first_actionable_session']}) — "
              f"{report['discovered']} symbols:\n")
        print(f"  inside the frozen 25 .............. "
              f"{len(report['inside_universe'])}")
        print(f"  OUTSIDE the frozen 25 ............. "
              f"{len(report['outside_universe'])}")
        print(f"  in more than one category ......... "
              f"{len(report['multi_category'])}")
        print(f"  with >= {report['min_local_bars']} local daily bars ...... "
              f"{len(report['with_local_history'])}")
        print(f"  with insufficient local history ... "
              f"{len(report['insufficient_local_history'])}")

        def _table(title, rows):
            if not rows:
                return
            print(f"\n  {title}")
            print(f"    {'SYMBOL':<10} {'RANK':>5} {'BARS':>6}  REASONS")
            for row in rows[:25]:
                reasons = ",".join(row.get("reasons") or [])
                print(f"    {row['symbol']:<10} "
                      f"{(row.get('best_rank') or 0):>5} "
                      f"{row.get('local_bars', 0):>6}  {reasons}")

        _table("Inside the frozen universe:", report["inside_universe"])
        _table("Outside the frozen universe:", report["outside_universe"])
        _table("Noticed by more than one list:", report["multi_category"])
        print("\nThis is the blind spot, stated: every symbol in the OUTSIDE "
              "list is one the scanner structurally cannot see. None of them "
              "is added to any universe by this or any other script.")
        print("The market session above is the tape these numbers came from; "
              "the actionable session is the earliest anybody could have done "
              "something about it. They are different dates on purpose.")


if __name__ == "__main__":
    main()
