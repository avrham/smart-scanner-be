#!/usr/bin/env python3
"""Read-only DESCRIPTIVE audit of scanner states against subsequent market paths.

IMPORTANT SEMANTICS — do not violate:
  * An outcome row is a SHARED, PAIR-LEVEL market path
    (reference_price_role = 'paired_decision_observation'). It is NOT a realized
    return for the candidate arm or for the control arm. Both arms observed the
    same market; only their CLASSIFICATION of it differs.
  * The sample is tiny (see the n= column printed on every row). This script
    reports description only. It computes no p-values, fits nothing, and must
    never be used to tune strategy thresholds.

Usage:
    DATABASE_URL=... .venv/bin/python ops/analysis/scanner_outcome_association.py
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence

import asyncpg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from app.prospective_campaign import (  # noqa: E402
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
    candidate_signal_fields,
)

HORIZONS = ("ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d")

# ret_* / MFE / MAE are persisted ALREADY IN PERCENT (ret_5d = 25.28 -> +25.28%),
# so they are printed verbatim and never rescaled.

SQL = """
SELECT
    r.telemetry->>'as_of_date' AS session,
    p.symbol,
    x.verdict AS cand_verdict,
    x.score   AS cand_score,
    x.details_snapshot AS cand_details,
    c.verdict AS ctrl_verdict,
    o.outcome_status, o.ret_1d, o.ret_3d, o.ret_5d, o.ret_10d, o.ret_20d,
    o.max_favorable_excursion AS mfe, o.max_adverse_excursion AS mae
FROM strategy_shadow_run_pairs rp
JOIN strategy_shadow_runs  r ON r.id = rp.run_id
JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
JOIN strategy_shadow_pair_outcomes o ON o.pair_id = p.id
LEFT JOIN strategy_shadow_evaluations x ON x.pair_id = p.id AND x.arm_code = $1
LEFT JOIN strategy_shadow_evaluations c ON c.pair_id = p.id AND c.arm_code = $2
WHERE r.telemetry->'campaign' IS NOT NULL
ORDER BY r.telemetry->>'as_of_date', p.symbol
"""


def _json(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            return None
    return v


async def load(dsn: str) -> List[Dict[str, Any]]:
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(SQL, CANDIDATE_ARM_CODE, CONTROL_ARM_CODE)
    finally:
        await conn.close()
    out = []
    for r in rows:
        d = dict(r)
        details = _json(d["cand_details"]) or {}
        sig = candidate_signal_fields(details) if details else {}
        d["setup_state"] = sig.get("setup_state")
        d["waiting_reasons"] = sig.get("waiting_reasons") or []
        d["structure_state"] = (details.get("structure") or {}).get("state")
        d["reason_code"] = (details.get("policy") or {}).get("reason_code")
        out.append(d)
    return out


def summarize(rows: Sequence[Dict[str, Any]], horizon: str) -> Optional[Dict[str, Any]]:
    vals = [r[horizon] for r in rows if r[horizon] is not None]
    if not vals:
        return None
    return {
        "n": len(vals),
        "median": statistics.median(vals),
        "mean": statistics.fmean(vals),
        "share_positive": sum(1 for v in vals if v > 0) / len(vals),
    }


def group_report(title: str, rows: List[Dict[str, Any]], key) -> None:
    print(f"\n{title}\n{'-' * len(title)}")
    groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[key(r)].append(r)

    header = f"  {'group':<34} {'horizon':<9} {'n':>4} {'median%':>9} {'mean%':>9} {'>0':>7}"
    print(header)
    for g in sorted(groups, key=lambda k: (str(k))):
        for h in HORIZONS:
            s = summarize(groups[g], h)
            if not s:
                continue
            print(f"  {str(g):<34} {h:<9} {s['n']:>4} "
                  f"{s['median']:>8.2f}% {s['mean']:>8.2f}% "
                  f"{100 * s['share_positive']:>6.0f}%")
        # excursions are a better small-sample descriptor than point returns
        mfe = [r["mfe"] for r in groups[g] if r["mfe"] is not None]
        mae = [r["mae"] for r in groups[g] if r["mae"] is not None]
        if mfe and mae:
            print(f"  {str(g):<34} {'MFE/MAE':<9} {len(mfe):>4} "
                  f"{statistics.median(mfe):>8.2f}% "
                  f"{statistics.median(mae):>8.2f}%")


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print("DATABASE_URL is required (read-only DSN); it is never printed.",
              file=sys.stderr)
        return 2
    rows = await load(dsn)

    print("=" * 78)
    print("DESCRIPTIVE OUTCOME AUDIT — NOT a performance claim, NOT significance")
    print("=" * 78)
    print(f"pairs with an outcome row: {len(rows)}")
    by_session: Dict[str, int] = defaultdict(int)
    for r in rows:
        by_session[r["session"]] += 1
    for s in sorted(by_session):
        mature20 = sum(1 for r in rows if r["session"] == s and r["ret_20d"] is not None)
        print(f"  {s}: n={by_session[s]}  (20D mature: {mature20})")
    print("\nOutcomes are SHARED pair-level market paths, identical for both arms.")
    print("They describe what the market did after the session — never an arm's")
    print("realized return. Sample sizes below are far too small for inference.")

    group_report("BY CANDIDATE VERDICT", rows, lambda r: r["cand_verdict"] or "none")
    group_report("BY SETUP STATE", rows, lambda r: r["setup_state"] or "none")
    group_report("BY STRUCTURE STATE", rows, lambda r: r["structure_state"] or "none")
    group_report("BY POLICY REASON CODE", rows, lambda r: r["reason_code"] or "none")
    group_report("BY CANDIDATE/CONTROL PAIR", rows,
                 lambda r: f"{r['cand_verdict']}/{r['ctrl_verdict']}")
    group_report("BY CANDIDATE SCORE PRESENCE", rows,
                 lambda r: "scored" if r["cand_score"] is not None else "unscored")

    scored = [r for r in rows if r["cand_score"] is not None]
    if len(scored) >= 6:
        med = statistics.median([r["cand_score"] for r in scored])
        group_report(f"BY SCORE ABOVE/BELOW MEDIAN ({med:.4f}), scored rows only",
                     scored,
                     lambda r: "score>=median" if r["cand_score"] >= med else "score<median")

    print("\n" + "=" * 78)
    print("Read this as: which scanner categories are worth SHOWING differently,")
    print("not as which categories make money. No parameter was tuned to it.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
