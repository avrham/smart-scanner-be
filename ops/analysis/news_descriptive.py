#!/usr/bin/env python3
"""Descriptive-only look at company news against the stored scan history.

This answers ONE question and refuses to answer any other: on the sessions we
actually scanned, how often did a symbol carry a company-specific headline, and
what did those symbols do afterwards?

It is deliberately not a study.
  * Nothing here is fitted. The proximity windows, the scope thresholds and the
    relevance rule all come from `app/news.py` and were fixed before any of this
    was measured.
  * Nothing here feeds back into the product. No threshold, weight or ranking
    input is produced, and none should be derived from this output.
  * Every number is printed with its own n. A median over four observations is
    reported as a median over four observations, not as a finding.
  * No causal language. "Symbols with a headline moved X" is a description of a
    sample, not a claim that the headline moved them.

Point-in-time is handled by the same code the product uses: an article is only
counted for a session if it was published before that session's close.

Usage:
    DATABASE_URL=... .venv/bin/python ops/analysis/news_descriptive.py
"""

from __future__ import annotations

import asyncio
import os
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date
from typing import Any, Dict, List, Optional

import asyncpg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import app.news as nw  # noqa: E402

# One row per evaluated symbol-session, with whatever forward return we have.
HISTORY_SQL = """
SELECT r.snapshot_session_date::text AS session,
       p.symbol,
       e.verdict  AS candidate_verdict,
       c.verdict  AS control_verdict,
       o.ret_5d,
       o.ret_10d,
       o.ret_20d,
       o.max_adverse_excursion
FROM prospective_campaign_registrations r
JOIN strategy_shadow_run_pairs rp ON rp.run_id = r.campaign_run_id
JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
LEFT JOIN strategy_shadow_evaluations e
       ON e.pair_id = p.id AND e.arm_code = 'candidate_wyckoff_v2'
LEFT JOIN strategy_shadow_evaluations c
       ON c.pair_id = p.id AND c.arm_code <> 'candidate_wyckoff_v2'
LEFT JOIN strategy_shadow_pair_outcomes o ON o.pair_id = p.id
WHERE r.status = 'completed'
ORDER BY r.snapshot_session_date, p.symbol
"""

NEWS_SQL = """
SELECT s.symbol, s.relevance, a.published_at, a.title, a.title_normalized,
       a.publisher, a.article_url, a.category, a.category_source,
       a.scope, a.ticker_breadth
FROM company_news_symbols s
JOIN company_news_articles a ON a.id = s.article_id
ORDER BY s.symbol, a.published_at DESC
"""

OK = {"status": nw.STATUS_AVAILABLE, "reason": None, "last_refresh_at": None,
      "last_success_at": None, "age_hours": 0.0, "detail": None}


def median(values: List[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    return statistics.median(clean) if clean else None


def fmt(value: Optional[float]) -> str:
    return "—" if value is None else f"{value:+.2f}%"


def describe(label: str, rows: List[Dict[str, Any]]) -> None:
    """One cohort, with its n stated before any number derived from it."""
    n = len(rows)
    print(f"  {label:<34} n={n:<5}", end="")
    if not n:
        print()
        return
    print(f"  5d {fmt(median([r['ret_5d'] for r in rows])):>8}"
          f"  10d {fmt(median([r['ret_10d'] for r in rows])):>8}"
          f"  20d {fmt(median([r['ret_20d'] for r in rows])):>8}"
          f"  MAE {fmt(median([r['max_adverse_excursion'] for r in rows])):>8}")


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print("DATABASE_URL is required.", file=sys.stderr)
        return 2

    conn = await asyncpg.connect(dsn)
    try:
        rows = [dict(r) for r in await conn.fetch(HISTORY_SQL)]
        articles = [dict(r) for r in await conn.fetch(NEWS_SQL)]
    finally:
        await conn.close()

    by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for a in articles:
        by_symbol[a["symbol"]].append(a)

    sessions = sorted({r["session"] for r in rows})
    if not sessions:
        print("No completed campaigns to describe.")
        return 0

    print(f"Sessions scanned: {len(sessions)}  ({sessions[0]} .. {sessions[-1]})")
    print(f"Symbol-sessions:  {len(rows)}")
    print(f"News articles:    {len(articles)} links over {len(by_symbol)} symbols")
    print()

    # Attach the product's own news context to every symbol-session.
    for row in rows:
        ctx = nw.build_news_context(
            by_symbol.get(row["symbol"]) or [], symbol=row["symbol"],
            as_of_session=date.fromisoformat(row["session"]), freshness=OK)
        row["_news"] = ctx
        row["_notable"] = ctx["notable_count"] > 0
        row["_in_window"] = ctx["in_window_count"] > 0
        row["_category"] = ctx["top_category"]

    print("== COVERAGE ==")
    print(f"  symbol-sessions with ANY in-window article : "
          f"{sum(1 for r in rows if r['_in_window'])}/{len(rows)}")
    print(f"  symbol-sessions with a NOTABLE headline    : "
          f"{sum(1 for r in rows if r['_notable'])}/{len(rows)}")
    print()

    print("== BY SESSION (how many symbols carried a notable headline) ==")
    for session in sessions:
        same = [r for r in rows if r["session"] == session]
        print(f"  {session}  notable {sum(1 for r in same if r['_notable']):>3}"
              f" / {len(same):>3} symbols")
    print()

    print("== FORWARD MOVES BY NEWS COHORT (descriptive; medians, no claim) ==")
    describe("all symbol-sessions", rows)
    describe("with a notable headline", [r for r in rows if r["_notable"]])
    describe("without a notable headline", [r for r in rows if not r["_notable"]])
    print()

    print("== ATTENTION-ADJACENT COHORTS ==")
    watch = [r for r in rows if r["candidate_verdict"] == "WATCH"]
    describe("WATCH, all", watch)
    describe("WATCH + notable headline", [r for r in watch if r["_notable"]])
    describe("WATCH, no notable headline", [r for r in watch if not r["_notable"]])
    print()

    disagree = [r for r in rows
                if r["candidate_verdict"] and r["control_verdict"]
                and r["candidate_verdict"] != r["control_verdict"]]
    describe("arms disagree, all", disagree)
    describe("arms disagree + notable headline", [r for r in disagree if r["_notable"]])
    describe("arms disagree, no headline", [r for r in disagree if not r["_notable"]])
    print()

    print("== CATEGORY MIX OF NOTABLE HEADLINES ==")
    mix = Counter(r["_category"] for r in rows if r["_notable"])
    for category, count in mix.most_common():
        print(f"  {category:<26} {count}")
    print()

    print("NOTE: these are descriptions of small samples, not findings. Nothing")
    print("here establishes that a headline caused a move, and nothing here may")
    print("be used to tune a window, a threshold, the attention model or the")
    print("strategy. The windows were fixed before this was ever run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
