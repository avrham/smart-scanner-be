"""Descriptive forward behaviour around Wave 2 external events.

    python -m ops.analysis.wave2_descriptive --analyst --days 3650
    python -m ops.analysis.wave2_descriptive --macro
    python -m ops.analysis.wave2_descriptive --discovery

WHAT THIS IS
------------
A description of what price did after events we recorded, at the horizons the
rest of this repository already uses (1D / 3D / 5D / 10D / 20D). It is reading
material for a human deciding what is worth investigating.

WHAT THIS IS NOT
----------------
It is not a backtest, not a signal, not evidence of an edge, and not an input
to anything. Specifically, and on purpose:

  * NO thresholds are tuned. Nothing here searches for a cut-off that makes a
    number look better, because a cut-off chosen after seeing the outcome is
    not a finding.
  * NO alpha claim. A mean forward return over a few dozen events, with no
    control for market direction, sector, or the fact that a downgrade tends to
    arrive after a fall, measures very little. The cohort sizes are printed
    beside every figure precisely so that is obvious.
  * NO strategy mutation. No verdict, threshold, universe or pattern flag is
    read or written by this script.

POINT IN TIME — THREE CONCEPTS, NAMED, NEVER COLLAPSED
-------------------------------------------------------
    EVIDENCE PROVENANCE   `observed_at`, plus `reference_session_date` for
                          discovery: when we looked, and which market session
                          the numbers we saw came from. This is what the row
                          IS. It is never an anchor.

    ACTIONABILITY         `session_date`: the first session anybody could have
                          acted on the observation. Forward-rolling — a Sunday
                          fetch is actionable on Monday, a grade dated Tuesday
                          is actionable Wednesday.

    OUTCOME ANCHOR        `session_date`, and only once that session has
                          actually closed.

The anchor is the ACTIONABLE session, never the reference session, and that
choice costs us a day on purpose. Anchoring a weekend discovery snapshot on the
Friday it describes would measure a move we only learned about on Sunday: that
is lookahead, and it is the single most tempting mistake this file could make.
Anchoring on Monday forfeits Friday's move and can only ever understate.

`forward_returns` refuses to measure at all unless the anchor session is
present in `daily_bars`, and a session that has not closed has no bar — so an
uncompleted session cannot become a t0 by construction. `_completed_anchor`
states the same rule explicitly rather than relying on that side effect, so
the guarantee survives someone changing how bars are loaded.

COVERAGE IS THE HONEST HALF OF THE ANSWER
-----------------------------------------
`daily_bars` holds the frozen 25 plus the reference market, and nothing else.
So most discovered symbols have NO local history and cannot be measured at all
— which is itself the finding the discovery path exists to surface. Every
section prints how many events it could measure and how many it could not.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import app.analyst_events as ae
import app.macro_calendar as mc
from app.prospective_session import resolve_latest_completed_session
from app.reference_market import PRIMARY_BENCHMARK
from ops.analysis.intel_connection import intel_connection

#: The horizons every outcome layer in this repository already uses. Fixed
#: here, not configurable, so nobody can quietly search over them.
HORIZONS = (1, 3, 5, 10, 20)

BARS_SQL = """
SELECT symbol, trading_date, close
FROM public.daily_bars
WHERE symbol = ANY($1::text[])
ORDER BY symbol, trading_date
"""

ANALYST_COHORT_SQL = """
SELECT symbol, session_date, action_normalized
FROM public.analyst_grade_events
WHERE session_date >= $1
  AND action_normalized = ANY($2::text[])
ORDER BY session_date
"""

MACRO_COHORT_SQL = """
SELECT event_type, scheduled_date
FROM public.macro_events
WHERE scheduled_date >= $1
  AND scheduled_date <= $2
  AND source_listing = 'listed'
ORDER BY scheduled_date
"""

#: Grouped by BOTH dates. The reference session is what the appearance was;
#: the actionable session is what we may measure from. A cohort keyed on one
#: of them alone would silently answer the other question.
DISCOVERY_COHORT_SQL = """
SELECT symbol, session_date, reference_session_date,
       array_agg(DISTINCT list_kind ORDER BY list_kind) AS reasons
FROM public.external_discovery_candidates
WHERE session_date >= $1
GROUP BY symbol, session_date, reference_session_date
ORDER BY reference_session_date, session_date
"""


async def _load_series(conn, symbols: Sequence[str],
                       ) -> Dict[str, List[Tuple[date, float]]]:
    """Every symbol's close series, once. Never one query per event."""
    if not symbols:
        return {}
    series: Dict[str, List[Tuple[date, float]]] = {}
    for row in await conn.fetch(BARS_SQL, list(dict.fromkeys(symbols))):
        series.setdefault(row["symbol"], []).append(
            (row["trading_date"], float(row["close"])))
    return series


def _completed_anchor(anchor: Optional[date], *,
                      latest_completed: date) -> Optional[date]:
    """The anchor, or None if that session has not closed yet.

    Stated as its own rule rather than left to `forward_returns` finding no
    bar. Both guards agree today; this one keeps agreeing if the bar loader
    ever changes, and it is the guarantee somebody will want to read.
    """
    if anchor is None or anchor > latest_completed:
        return None
    return anchor


def forward_returns(series: List[Tuple[date, float]], anchor: date,
                    ) -> Optional[Dict[int, Optional[float]]]:
    """Percentage change from the anchor session's close, by session offset.

    Returns None when the anchor session itself is not in the series: without a
    t0 close there is nothing to measure from, and substituting a nearby close
    would be inventing the very number the study is about.
    """
    index = {d: i for i, (d, _) in enumerate(series)}
    start = index.get(anchor)
    if start is None:
        return None
    base = series[start][1]
    if not base:
        return None
    out: Dict[int, Optional[float]] = {}
    for horizon in HORIZONS:
        target = start + horizon
        out[horizon] = (round((series[target][1] - base) / base * 100.0, 3)
                        if target < len(series) else None)
    return out


def summarise(cohort: List[Dict[int, Optional[float]]]) -> Dict[str, Any]:
    """Count, mean and median per horizon. Nothing that could be traded.

    Median as well as mean deliberately: with cohorts this small a single gap
    moves the mean enough to invent a story, and printing both makes that
    visible instead of persuasive.
    """
    out: Dict[str, Any] = {}
    for horizon in HORIZONS:
        values = sorted(v for row in cohort
                        if (v := row.get(horizon)) is not None)
        if not values:
            out[f"{horizon}D"] = {"n": 0, "mean": None, "median": None}
            continue
        middle = len(values) // 2
        median = (values[middle] if len(values) % 2
                  else (values[middle - 1] + values[middle]) / 2)
        out[f"{horizon}D"] = {
            "n": len(values),
            "mean": round(sum(values) / len(values), 3),
            "median": round(median, 3),
        }
    return out


def _print_table(title: str, groups: Dict[str, Dict[str, Any]],
                 measured: int, unmeasured: int) -> None:
    print(f"\n{title}")
    print(f"  measured events: {measured}   unmeasurable "
          f"(no local history at t0): {unmeasured}")
    if not groups:
        print("  (no measurable events)")
        return
    header = "  ".join(f"{h}D".rjust(16) for h in HORIZONS)
    print(f"  {'COHORT':<26} {header}")
    for name, stats in groups.items():
        cells = []
        for horizon in HORIZONS:
            cell = stats[f"{horizon}D"]
            cells.append((f"n={cell['n']} {cell['mean']:+.2f}%"
                          if cell["mean"] is not None
                          else f"n=0 —").rjust(16))
        print(f"  {name:<26} {'  '.join(cells)}")
    print("  (mean shown; medians in --json. Cohort sizes are the point: a "
          "figure over a handful of events describes those events and nothing "
          "more.)")


async def run_analyst(days: int) -> None:
    latest_completed = resolve_latest_completed_session(
        datetime.now(timezone.utc))
    async with intel_connection() as conn:
        since = datetime.now(timezone.utc).date() - timedelta(days=days)
        rows = [dict(r) for r in await conn.fetch(
            ANALYST_COHORT_SQL, since, list(ae.DIRECTIONAL_ACTIONS))]
        series = await _load_series(conn, [r["symbol"] for r in rows])

    groups: Dict[str, List[Dict[int, Optional[float]]]] = {}
    measured = unmeasured = 0
    for row in rows:
        anchor = _completed_anchor(row["session_date"],
                                   latest_completed=latest_completed)
        returns = (forward_returns(series.get(row["symbol"], []), anchor)
                   if anchor else None)
        if returns is None:
            unmeasured += 1
            continue
        measured += 1
        groups.setdefault(row["action_normalized"], []).append(returns)
    _print_table(
        f"Analyst upgrades/downgrades on the frozen universe "
        f"(t0 = first session AFTER the grade date, last {days} days)",
        {k: summarise(v) for k, v in sorted(groups.items())},
        measured, unmeasured)


async def run_macro(days: int) -> None:
    latest_completed = resolve_latest_completed_session(
        datetime.now(timezone.utc))
    async with intel_connection() as conn:
        today = datetime.now(timezone.utc).date()
        rows = [dict(r) for r in await conn.fetch(
            MACRO_COHORT_SQL, today - timedelta(days=days), today)]
        series = await _load_series(conn, [PRIMARY_BENCHMARK])

    bars = series.get(PRIMARY_BENCHMARK, [])
    groups: Dict[str, List[Dict[int, Optional[float]]]] = {}
    measured = unmeasured = 0
    for row in rows:
        anchor = _completed_anchor(row["scheduled_date"],
                                   latest_completed=latest_completed)
        returns = forward_returns(bars, anchor) if anchor else None
        if returns is None:
            # A scheduled date that is not a trading day, or one before our
            # benchmark history begins. Counted, never nudged to a neighbour.
            unmeasured += 1
            continue
        measured += 1
        label = mc.EVENT_LABELS.get(row["event_type"], row["event_type"])
        groups.setdefault(label, []).append(returns)
    _print_table(
        f"{PRIMARY_BENCHMARK} after scheduled macro events "
        f"(t0 = the event's own session close, last {days} days)",
        {k: summarise(v) for k, v in sorted(groups.items())},
        measured, unmeasured)
    print("  Broad-market description only. It says nothing about direction "
          "before the next such event, and no scanner path reads it.")


async def run_discovery(days: int) -> None:
    latest_completed = resolve_latest_completed_session(
        datetime.now(timezone.utc))
    async with intel_connection() as conn:
        since = datetime.now(timezone.utc).date() - timedelta(days=days)
        rows = [dict(r) for r in await conn.fetch(DISCOVERY_COHORT_SQL, since)]
        series = await _load_series(conn, [r["symbol"] for r in rows])

    groups: Dict[str, List[Dict[int, Optional[float]]]] = {}
    measured = unmeasured = 0
    for row in rows:
        # ACTIONABLE session, never the reference session it describes.
        anchor = _completed_anchor(row["session_date"],
                                   latest_completed=latest_completed)
        returns = (forward_returns(series.get(row["symbol"], []), anchor)
                   if anchor else None)
        if returns is None:
            unmeasured += 1
            continue
        measured += 1
        for reason in row["reasons"]:
            groups.setdefault(reason, []).append(returns)
    _print_table(
        f"Discovered movers, forward behaviour (last {days} days)",
        {k: summarise(v) for k, v in sorted(groups.items())},
        measured, unmeasured)
    print("  Anchored on the ACTIONABLE session, not the market session the "
          "snapshot describes — anchoring on the latter would measure a move "
          "we only learned about afterwards.")
    print("  The unmeasurable count is the real finding here: `daily_bars` "
          "holds the frozen 25 and the reference market, so a symbol the "
          "market noticed and we do not hold cannot be studied at all.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analyst", action="store_true")
    parser.add_argument("--macro", action="store_true")
    parser.add_argument("--discovery", action="store_true")
    parser.add_argument("--days", type=int, default=3650,
                        help="lookback in calendar days (default 3650)")
    args = parser.parse_args()
    if not (args.analyst or args.macro or args.discovery):
        parser.error("choose --analyst, --macro, --discovery, or several")
    if args.analyst:
        asyncio.run(run_analyst(args.days))
    if args.macro:
        asyncio.run(run_macro(args.days))
    if args.discovery:
        asyncio.run(run_discovery(args.days))


if __name__ == "__main__":
    main()
