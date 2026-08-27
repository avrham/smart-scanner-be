#!/usr/bin/env python3
"""Read-only descriptive audit of the market-context layer.

Computes the shipped context metrics (app/market_context.py) AS OF each
completed campaign session, then cross-tabs them against attention tiers and —
where outcomes have matured — subsequent market paths.

SEMANTICS, do not violate:
  * Relative strength here is measured against the SCANNER UNIVERSE MEDIAN.
    The store holds no index or sector series, so this is NOT market-relative
    performance and must never be reported as such.
  * Breadth is `scanner_universe` breadth over 25 large caps, NOT market breadth.
  * Outcomes are SHARED pair-level market paths, never an arm's realized return.
  * Sample sizes are tiny. This describes; it does not infer, and nothing here
    may be used to tune a threshold or a strategy parameter.

Usage:
    DATABASE_URL=... .venv/bin/python ops/analysis/scanner_market_context.py [--json OUT]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Sequence

import asyncpg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import app.market_context as mc  # noqa: E402
import app.scanner_view as sv  # noqa: E402
from app.prospective_campaign import (  # noqa: E402
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
    candidate_signal_fields,
)

HORIZONS = ("ret_1d", "ret_3d", "ret_5d", "ret_10d")

EVAL_SQL = """
SELECT r.telemetry->>'as_of_date' AS session,
       p.symbol,
       x.verdict AS cand_verdict, x.score AS cand_score,
       x.details_snapshot AS cand_details,
       c.verdict AS ctrl_verdict,
       o.ret_1d, o.ret_3d, o.ret_5d, o.ret_10d,
       o.max_favorable_excursion AS mfe, o.max_adverse_excursion AS mae
FROM strategy_shadow_run_pairs rp
JOIN strategy_shadow_runs  r ON r.id = rp.run_id
JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
LEFT JOIN strategy_shadow_evaluations x ON x.pair_id = p.id AND x.arm_code = $1
LEFT JOIN strategy_shadow_evaluations c ON c.pair_id = p.id AND c.arm_code = $2
LEFT JOIN strategy_shadow_pair_outcomes o ON o.pair_id = p.id
WHERE r.telemetry->'campaign' IS NOT NULL
ORDER BY r.telemetry->>'as_of_date', p.symbol
"""

BARS_SQL = """
SELECT symbol, trading_date, close, volume
FROM daily_bars
WHERE trading_date <= $1::text::date
ORDER BY symbol, trading_date
"""


def _json(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            return None
    return v


async def load(dsn: str) -> Dict[str, Any]:
    conn = await asyncpg.connect(dsn)
    try:
        evals = await conn.fetch(EVAL_SQL, CANDIDATE_ARM_CODE, CONTROL_ARM_CODE)
        sessions = sorted({r["session"] for r in evals if r["session"]})
        bars_by_session: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for session in sessions:
            rows = await conn.fetch(BARS_SQL, session)
            by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for b in rows:
                by_symbol[b["symbol"]].append(
                    {"close": float(b["close"]), "volume": float(b["volume"] or 0.0)}
                )
            bars_by_session[session] = dict(by_symbol)
    finally:
        await conn.close()

    out = []
    for r in evals:
        d = dict(r)
        details = _json(d["cand_details"]) or {}
        sig = candidate_signal_fields(details) if details else {}
        d["attention"] = sv.classify_attention(
            has_candidate_result=d["cand_verdict"] is not None,
            candidate_verdict=d["cand_verdict"],
            setup_state=sig.get("setup_state"),
            readiness_status=sig.get("readiness_status"),
            control_verdict=d["ctrl_verdict"],
        )
        d["cross_arm"] = sv.classify_cross_arm(
            candidate_verdict=d["cand_verdict"], control_verdict=d["ctrl_verdict"]
        )
        out.append(d)
    return {"evals": out, "bars": bars_by_session, "sessions": sessions}


def line(title: str, ch: str = "-") -> None:
    print(f"\n{title}\n{ch * len(title)}")


def summarize(rows: Sequence[Dict[str, Any]], horizon: str) -> Optional[Dict[str, Any]]:
    vals = [r[horizon] for r in rows if r.get(horizon) is not None]
    if not vals:
        return None
    return {
        "n": len(vals),
        "median": statistics.median(vals),
        "share_positive": sum(1 for v in vals if v > 0) / len(vals),
    }


def group_report(title: str, rows: List[Dict[str, Any]], key) -> None:
    print(f"\n{title}\n{'-' * len(title)}")
    groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[key(r)].append(r)
    print(f"  {'group':<34} {'horizon':<9} {'n':>4} {'median%':>9} {'>0':>7}")
    for g in sorted(groups, key=str):
        for h in HORIZONS:
            s = summarize(groups[g], h)
            if s:
                print(f"  {str(g):<34} {h:<9} {s['n']:>4} {s['median']:>8.2f}% "
                      f"{100 * s['share_positive']:>6.0f}%")
        mfe = [r["mfe"] for r in groups[g] if r.get("mfe") is not None]
        mae = [r["mae"] for r in groups[g] if r.get("mae") is not None]
        if mfe and mae:
            print(f"  {str(g):<34} {'MFE/MAE':<9} {len(mfe):>4} "
                  f"{statistics.median(mfe):>8.2f}% {statistics.median(mae):>8.2f}%")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="write the full report to this path")
    args = ap.parse_args()
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print("DATABASE_URL is required (read-only DSN); never printed.", file=sys.stderr)
        return 2

    data = await load(dsn)
    evals, bars, sessions = data["evals"], data["bars"], data["sessions"]

    print("=" * 78)
    print("MARKET CONTEXT — DESCRIPTIVE AUDIT (no inference, no tuning)")
    print("=" * 78)
    print(f"campaigns: {', '.join(sessions)}   evaluations: {len(evals)}")
    print(f"relative strength comparator: {mc.COMPARATOR_UNIVERSE_MEDIAN} "
          f"(NOT a market index — none is stored)")

    # attach context as of each session
    for r in evals:
        by_symbol = bars.get(r["session"]) or {}
        r["context"] = sv.build_row_context(r["symbol"], by_symbol)

    line("UNIVERSE BREADTH PER CAMPAIGN (scanner universe, NOT the market)", "=")
    breadth_report: Dict[str, Any] = {}
    for session in sessions:
        b = mc.build_universe_breadth(bars.get(session) or {})
        breadth_report[session] = b
        p5 = b.get("positive_5d") or {}
        p20 = b.get("positive_20d") or {}
        at = b.get("above_trend") or {}
        print(f"  {session}  symbols={b['symbol_count']:>3}  "
              f"positive 5D={p5.get('pct')}%  positive 20D={p20.get('pct')}%  "
              f"above {at.get('window_days')}D trend={at.get('pct')}%")

    line("CONTEXT DISTRIBUTION PER CAMPAIGN", "=")
    dist_report: Dict[str, Any] = {}
    for session in sessions:
        rs = Counter(r["context"]["relative_strength"] for r in evals if r["session"] == session)
        vol = Counter(r["context"]["volume"] for r in evals if r["session"] == session)
        dist_report[session] = {"relative_strength": dict(rs), "volume": dict(vol)}
        print(f"  {session}  RS {dict(rs)}")
        print(f"  {'':<12} VOL {dict(vol)}")

    line("ATTENTION TIER x RELATIVE STRENGTH (counts)", "=")
    cross = Counter(
        (r["attention"], r["context"]["relative_strength"]) for r in evals
    )
    print(f"  {'attention':<16} {'rel strength':<18} {'n':>4}")
    for (tier, rs_cat), n in sorted(cross.items(), key=lambda kv: str(kv[0])):
        print(f"  {tier:<16} {str(rs_cat):<18} {n:>4}")

    line("ATTENTION TIER x VOLUME (counts)", "=")
    crossv = Counter((r["attention"], r["context"]["volume"]) for r in evals)
    print(f"  {'attention':<16} {'volume':<18} {'n':>4}")
    for (tier, v), n in sorted(crossv.items(), key=lambda kv: str(kv[0])):
        print(f"  {tier:<16} {str(v):<18} {n:>4}")

    matured = [r for r in evals if r.get("ret_5d") is not None]
    print(f"\n\nOUTCOME SECTION — matured pairs only: n={len(matured)} "
          f"of {len(evals)} (the newest campaign has no outcomes yet).")
    print("Outcomes are SHARED pair-level market paths. Sample sizes are far too")
    print("small for inference; no threshold was fitted to any of this.")

    group_report("BY RELATIVE STRENGTH", matured,
                 lambda r: r["context"]["relative_strength"] or "unavailable")
    group_report("BY VOLUME CONTEXT", matured,
                 lambda r: r["context"]["volume"] or "unavailable")
    group_report("BY ATTENTION TIER", matured, lambda r: r["attention"])
    group_report("HIGH/DEVELOPING x RELATIVE STRENGTH",
                 [r for r in matured if r["attention"] in ("high_attention", "developing")],
                 lambda r: f"{r['attention']}/{r['context']['relative_strength']}")
    group_report("CROSS-ARM x RELATIVE STRENGTH",
                 [r for r in matured if r["cross_arm"] in ("baseline_only", "candidate_only")],
                 lambda r: f"{r['cross_arm']}/{r['context']['relative_strength']}")

    print("\n" + "=" * 78)
    print("Read as: which context dimensions are worth SHOWING, not which make")
    print("money. Benchmark and sector context are unavailable by design here.")
    print("=" * 78)

    if args.json:
        with open(args.json, "w") as fh:
            json.dump({
                "sessions": sessions,
                "breadth": breadth_report,
                "context_distribution": dist_report,
                "matured_pairs": len(matured),
            }, fh, indent=1, default=str)
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
