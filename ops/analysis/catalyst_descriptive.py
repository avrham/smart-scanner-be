#!/usr/bin/env python3
"""Descriptive-only look at catalysts against the stored scan history.

This answers ONE question and refuses to answer any other: on the sessions we
actually scanned, how often did a symbol sit near a corporate event, and what
did those symbols do afterwards?

It is deliberately not a study.
  * Nothing here is fitted. The proximity windows come from `app/catalyst.py`
    and were fixed before any of this was measured.
  * Nothing here feeds back into the product. No threshold, weight or ranking
    input is produced, and none should be derived from this output.
  * Every number is printed with its own n. A median over four observations is
    reported as a median over four observations, not as a finding.

`observed_at` on every stored event is the ingestion time, so FORWARD-looking
events cannot be evaluated historically at all — that is exactly what the
point-in-time guard in `select_relevant_event` prevents. Filing dates are past
facts on the sessions we scan, so they are the only thing this can describe.

Usage:
    DATABASE_URL=... .venv/bin/python ops/analysis/catalyst_descriptive.py
"""

from __future__ import annotations

import asyncio
import os
import statistics
import sys
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional

import asyncpg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import app.catalyst as cat  # noqa: E402

# One row per evaluated symbol-session, with whatever forward return we have.
HISTORY_SQL = """
SELECT r.snapshot_session_date::text AS session,
       p.symbol,
       e.verdict         AS candidate_verdict,
       o.ret_5d,
       o.ret_10d,
       o.ret_20d,
       o.max_adverse_excursion
FROM prospective_campaign_registrations r
JOIN strategy_shadow_run_pairs rp ON rp.run_id = r.campaign_run_id
JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
LEFT JOIN strategy_shadow_evaluations e
       ON e.pair_id = p.id AND e.arm_code = 'candidate_wyckoff_v2'
LEFT JOIN strategy_shadow_pair_outcomes o ON o.pair_id = p.id
WHERE r.status = 'completed'
ORDER BY r.snapshot_session_date, p.symbol
"""

EVENTS_SQL = """
SELECT symbol, event_type, event_date, session_timing, certainty,
       fiscal_period, fiscal_year, source, source_reference, observed_at
FROM symbol_catalyst_events
"""


def median(values: List[float]) -> Optional[float]:
    clean = [v for v in values if v is not None]
    return statistics.median(clean) if clean else None


def fmt(value: Optional[float]) -> str:
    return "—" if value is None else f"{value:+.2f}%"


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print("DATABASE_URL is required.", file=sys.stderr)
        return 2

    conn = await asyncpg.connect(dsn)
    try:
        rows = [dict(r) for r in await conn.fetch(HISTORY_SQL)]
        events = [dict(r) for r in await conn.fetch(EVENTS_SQL)]
    finally:
        await conn.close()

    by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in events:
        by_symbol[e["symbol"]].append(e)

    # The freshness object the product would use; here every source is readable
    # because we are reading the stored table directly.
    ok = {"status": cat.STATUS_AVAILABLE, "reason": None, "last_refresh_at": None,
          "last_success_at": None, "age_hours": 0.0, "detail": None}

    sessions = sorted({r["session"] for r in rows})
    print(f"Sessions scanned: {len(sessions)}  ({sessions[0]} .. {sessions[-1]})")
    print(f"Symbol-sessions:  {len(rows)}")
    print(f"Catalyst events:  {len(events)} over {len(by_symbol)} symbols")
    print()

    # ---- 1. what the point-in-time guard actually does -------------------- #
    withheld = 0
    for row in rows:
        session = date.fromisoformat(row["session"])
        ctx = cat.build_catalyst_context(
            by_symbol.get(row["symbol"]) or [], as_of_session=session,
            earnings_freshness=ok, filings_freshness=ok)
        row["catalyst"] = ctx
        if ctx["earnings"]["reason"] == cat.REASON_NO_POINT_IN_TIME:
            withheld += 1

    print("1. POINT-IN-TIME")
    print(f"   symbol-sessions where a future event was withheld as hindsight: "
          f"{withheld}/{len(rows)}")
    print("   (every stored event was observed at ingestion time, so nothing")
    print("    forward-looking is knowable for a past session — by design)")
    print()

    # ---- 2. how often a scan sat near a filing ---------------------------- #
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[row["catalyst"]["last_financial_report"]["proximity"]].append(row)

    print("2. FILING PROXIMITY AT SCAN TIME  (descriptive; windows fixed a priori)")
    for proximity in cat.PROXIMITIES:
        group = buckets.get(proximity, [])
        if not group:
            continue
        print(f"   {proximity:<12} n={len(group):>3}"
              f"   sessions={len({g['session'] for g in group})}")
    print()

    # ---- 3. forward returns by proximity ---------------------------------- #
    print("3. FORWARD RETURNS BY FILING PROXIMITY")
    print("   Reported ONLY to show how thin this is. n is the number of")
    print("   symbol-sessions with a stored outcome, which is far below what")
    print("   any claim would need.")
    print(f"   {'proximity':<12} {'n':>4}  {'med 5d':>8} {'med 10d':>8} {'med 20d':>8}")
    for proximity in cat.PROXIMITIES:
        group = [g for g in buckets.get(proximity, []) if g["ret_5d"] is not None]
        if not group:
            continue
        print(f"   {proximity:<12} {len(group):>4}  "
              f"{fmt(median([g['ret_5d'] for g in group])):>8} "
              f"{fmt(median([g['ret_10d'] for g in group])):>8} "
              f"{fmt(median([g['ret_20d'] for g in group])):>8}")
    print()

    # ---- 4. does proximity coincide with attention? ----------------------- #
    print("4. FILING PROXIMITY vs CANDIDATE VERDICT")
    verdicts = sorted({(r["candidate_verdict"] or "none") for r in rows})
    header = "   " + f"{'proximity':<12}" + "".join(f"{v:>10}" for v in verdicts)
    print(header)
    for proximity in cat.PROXIMITIES:
        group = buckets.get(proximity, [])
        if not group:
            continue
        counts = defaultdict(int)
        for g in group:
            counts[g["candidate_verdict"] or "none"] += 1
        print("   " + f"{proximity:<12}" + "".join(f"{counts[v]:>10}" for v in verdicts))
    print()

    print("CONCLUSION")
    print("  This output is descriptive and is not evidence of anything. The")
    print("  sample is too small for a claim in either direction, and no part of")
    print("  it may be promoted into ranking, attention or a verdict. Catalyst")
    print("  data stays CONTEXT: the product says a setup exists AND an event is")
    print("  nearby, never that the setup is better or worse for it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
