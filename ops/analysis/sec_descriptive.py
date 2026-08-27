#!/usr/bin/env python3
"""Descriptive-only look at SEC 8-K events against the stored scan history.

This answers TWO questions and refuses to answer any other:

  1. on the sessions we actually scanned, how often had a symbol formally
     disclosed a material corporate event?
  2. how does that overlap with what News V1 called notable?

The second question is the point of the milestone. News V1 measured 61 of 100
symbol-sessions carrying a "notable" headline — too broad to mean "a material
corporate catalyst". If SEC sharpens that, it will show here as a much smaller
set with a large slice of news-notable sessions that have no filing behind them.

It is deliberately not a study.
  * Nothing is fitted. The proximity windows, the supporting-item rule and the
    item taxonomy all come from `app/sec_events.py` and were fixed before this
    was ever run.
  * Nothing feeds back into the product. No threshold, weight or ranking input
    is produced, and none may be derived from this output.
  * Every number is printed with its own n.
  * No causal language. "Symbols with a filing moved X" describes a sample; it
    does not claim the filing moved them.

Point-in-time is handled by the same code the product uses: a filing counts for
a session only if EDGAR accepted it before that session's close.

Usage:
    DATABASE_URL=... .venv/bin/python ops/analysis/sec_descriptive.py
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
import app.sec_events as se  # noqa: E402

HISTORY_SQL = """
SELECT r.snapshot_session_date::text AS session,
       p.symbol,
       e.verdict  AS candidate_verdict,
       c.verdict  AS control_verdict,
       o.ret_5d, o.ret_10d, o.ret_20d, o.max_adverse_excursion
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

SEC_SQL = """
SELECT s.symbol, f.accession_number, f.cik, f.form, f.accepted_at,
       f.filing_date, f.period_of_report, f.item_codes, f.event_types,
       f.taxonomy_version, f.is_primary_event, f.amends_accession_number,
       f.filing_url
FROM sec_filing_symbols s
JOIN sec_filings f ON f.id = s.filing_id
ORDER BY s.symbol, f.accepted_at DESC
"""

NEWS_SQL = """
SELECT s.symbol, s.relevance, a.published_at, a.title, a.title_normalized,
       a.publisher, a.article_url, a.category, a.category_source,
       a.scope, a.ticker_breadth
FROM company_news_symbols s
JOIN company_news_articles a ON a.id = s.article_id
ORDER BY s.symbol, a.published_at DESC
"""

OK_SEC = {"status": se.STATUS_AVAILABLE, "reason": None, "last_refresh_at": None,
          "last_success_at": None, "age_hours": 0.0, "detail": None}
OK_NEWS = {"status": nw.STATUS_AVAILABLE, "reason": None, "last_refresh_at": None,
           "last_success_at": None, "age_hours": 0.0, "detail": None}


def median(values: List[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    return statistics.median(clean) if clean else None


def fmt(value: Optional[float]) -> str:
    return "—" if value is None else f"{value:+.2f}%"


def describe(label: str, rows: List[Dict[str, Any]]) -> None:
    """One cohort, with its n stated before any number derived from it."""
    n = len(rows)
    print(f"  {label:<38} n={n:<5}", end="")
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
        filings = [dict(r) for r in await conn.fetch(SEC_SQL)]
        articles = [dict(r) for r in await conn.fetch(NEWS_SQL)]
    finally:
        await conn.close()

    sec_by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for f in filings:
        sec_by_symbol[f["symbol"]].append(f)
    news_by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for a in articles:
        news_by_symbol[a["symbol"]].append(a)

    sessions = sorted({r["session"] for r in rows})
    if not sessions:
        print("No completed campaigns to describe.")
        return 0

    print(f"Sessions scanned: {len(sessions)}  ({sessions[0]} .. {sessions[-1]})")
    print(f"Symbol-sessions:  {len(rows)}")
    print(f"SEC filings:      {len(filings)} links over {len(sec_by_symbol)} symbols")
    print(f"News articles:    {len(articles)} links over {len(news_by_symbol)} symbols")
    print()

    # Attach BOTH product contexts to every symbol-session, point-in-time.
    for row in rows:
        session = date.fromisoformat(row["session"])
        sec = se.build_sec_context(sec_by_symbol.get(row["symbol"]) or [],
                                   as_of_session=session, freshness=OK_SEC)
        news = nw.build_news_context(news_by_symbol.get(row["symbol"]) or [],
                                     symbol=row["symbol"], as_of_session=session,
                                     freshness=OK_NEWS)
        row["_sec"] = sec
        row["_sec_in_window"] = sec["in_window_count"] > 0
        row["_sec_notable"] = sec["notable_count"] > 0
        row["_sec_type"] = sec["top_event_type"]
        row["_news_notable"] = news["notable_count"] > 0

    sec_notable = [r for r in rows if r["_sec_notable"]]
    news_notable = [r for r in rows if r["_news_notable"]]

    print("== COVERAGE ==")
    print(f"  symbol-sessions with ANY in-window filing   : "
          f"{sum(1 for r in rows if r['_sec_in_window'])}/{len(rows)}")
    print(f"  symbol-sessions with a RECENT PRIMARY event : "
          f"{len(sec_notable)}/{len(rows)}")
    print(f"  symbol-sessions with NOTABLE NEWS (V1)      : "
          f"{len(news_notable)}/{len(rows)}")
    print()

    print("== BY SESSION ==")
    for session in sessions:
        same = [r for r in rows if r["session"] == session]
        print(f"  {session}  sec {sum(1 for r in same if r['_sec_notable']):>3}"
              f" / {len(same):>3}   news {sum(1 for r in same if r['_news_notable']):>3}"
              f" / {len(same):>3}")
    print()

    print("== EVENT TYPE MIX (recent primary events) ==")
    mix = Counter(r["_sec_type"] for r in sec_notable if r["_sec_type"])
    for event_type, count in mix.most_common():
        print(f"  {event_type:<34} {count}")
    print()

    print("== ITEM CODE MIX (all in-window filings) ==")
    codes = Counter()
    for row in rows:
        for item in row["_sec"]["items"]:
            for code in item["item_codes"]:
                codes[code] += 1
    for code, count in sorted(codes.items()):
        print(f"  {code:<8} {count}")
    print()

    print("== SEC vs NEWS OVERLAP (the point of this milestone) ==")
    both = [r for r in rows if r["_sec_notable"] and r["_news_notable"]]
    sec_only = [r for r in rows if r["_sec_notable"] and not r["_news_notable"]]
    news_only = [r for r in rows if r["_news_notable"] and not r["_sec_notable"]]
    neither = [r for r in rows if not r["_sec_notable"] and not r["_news_notable"]]
    print(f"  both SEC event and notable news : {len(both)}")
    print(f"  SEC event, no notable news      : {len(sec_only)}")
    print(f"  notable news, no SEC event      : {len(news_only)}")
    print(f"  neither                         : {len(neither)}")
    if news_notable:
        share = 100.0 * len(both) / len(news_notable)
        print(f"\n  Of the {len(news_notable)} news-notable symbol-sessions, "
              f"{len(both)} ({share:.0f}%) had a formal SEC event behind them.")
        print("  The remainder is what News V1 could not distinguish: coverage")
        print("  about a company, with no corporate event on file. This is a")
        print("  DESCRIPTION of overlap, not a claim that those articles were")
        print("  worthless — many real events are not 8-K events.")
    print()

    print("== FORWARD MOVES BY COHORT (descriptive; medians, no claim) ==")
    describe("all symbol-sessions", rows)
    describe("SEC event", sec_notable)
    describe("no SEC event", [r for r in rows if not r["_sec_notable"]])
    describe("SEC event + notable news", both)
    describe("SEC event, no notable news", sec_only)
    describe("notable news, no SEC event", news_only)
    describe("neither", neither)
    print()

    print("== ATTENTION-ADJACENT COHORTS ==")
    watch = [r for r in rows if r["candidate_verdict"] == "WATCH"]
    describe("WATCH, all", watch)
    describe("WATCH + SEC event", [r for r in watch if r["_sec_notable"]])
    describe("WATCH, no SEC event", [r for r in watch if not r["_sec_notable"]])
    disagree = [r for r in rows
                if r["candidate_verdict"] and r["control_verdict"]
                and r["candidate_verdict"] != r["control_verdict"]]
    describe("arms disagree, all", disagree)
    describe("arms disagree + SEC event", [r for r in disagree if r["_sec_notable"]])
    print()

    print("NOTE: these are descriptions of small samples, not findings. Nothing")
    print("here establishes that a filing caused a move, and nothing here may be")
    print("used to tune a window, a threshold, the attention model or the")
    print("strategy. Every window was fixed before this was run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
