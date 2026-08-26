#!/usr/bin/env python3
"""Read-only forensic analysis of Smart Scanner signal quality.

Answers one product question from the canonical database, not from opinion:
does the scanner's output actually DIFFERENTIATE symbols enough to tell a user
what deserves attention first?

Strictly read-only: SELECTs only, no DDL, no writes, no parameter tuning. It
reuses `app.prospective_campaign.candidate_signal_fields` — the same extraction
the Product API surfaces — so what it measures is exactly what the user sees.

Usage:
    DATABASE_URL=postgresql://user:pass@host:port/db \\
        .venv/bin/python ops/analysis/scanner_signal_forensics.py [--json OUT]

The DSN is read from the environment and never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

import asyncpg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from app.prospective_campaign import (  # noqa: E402
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
    candidate_signal_fields,
)
import app.scanner_view as sv  # noqa: E402

# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #

ROWS_SQL = """
SELECT
    r.telemetry->>'as_of_date'              AS session,
    r.id                                    AS run_id,
    p.symbol                                AS symbol,
    x.verdict                               AS cand_verdict,
    x.score                                 AS cand_score,
    x.reason                                AS cand_reason,
    x.rejection_reason                      AS cand_rejection,
    x.details_snapshot                      AS cand_details,
    c.verdict                               AS ctrl_verdict,
    c.score                                 AS ctrl_score,
    c.reason                                AS ctrl_reason
FROM strategy_shadow_run_pairs rp
JOIN strategy_shadow_runs  r ON r.id = rp.run_id
JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
LEFT JOIN strategy_shadow_evaluations x
       ON x.pair_id = p.id AND x.arm_code = $1
LEFT JOIN strategy_shadow_evaluations c
       ON c.pair_id = p.id AND c.arm_code = $2
WHERE r.telemetry->'campaign' IS NOT NULL
ORDER BY r.telemetry->>'as_of_date', p.symbol
"""


def _json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return None
    return value


async def load(dsn: str) -> List[Dict[str, Any]]:
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(ROWS_SQL, CANDIDATE_ARM_CODE, CONTROL_ARM_CODE)
    finally:
        await conn.close()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        details = _json(d["cand_details"]) or {}
        d["cand_details"] = details
        # exactly what the Product API exposes as `evidence`
        d["signal"] = candidate_signal_fields(details) if details else None
        d["structure_state"] = (details.get("structure") or {}).get("state")
        d["structure_class"] = (details.get("structure") or {}).get("classification")
        d["selected_phase"] = details.get("selected_phase")
        d["phase_state"] = details.get("phase_state")
        d["policy_reason_code"] = (details.get("policy") or {}).get("reason_code")
        d["score_version"] = details.get("score_version")
        d["ranking"] = details.get("ranking")
        out.append(d)
    return out


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def dist(values) -> Dict[str, int]:
    return dict(sorted(Counter(values).items(), key=lambda kv: (-kv[1], str(kv[0]))))


def line(title: str, ch: str = "-") -> None:
    print(f"\n{title}\n{ch * len(title)}")


def fmt_counter(c: Dict[Any, int], total: Optional[int] = None) -> str:
    if not c:
        return "    (none)"
    parts = []
    for k, v in c.items():
        pct = f" ({100 * v / total:.0f}%)" if total else ""
        parts.append(f"    {str(k):<44} {v:>4}{pct}")
    return "\n".join(parts)


# --------------------------------------------------------------------------- #
# Per-campaign analysis
# --------------------------------------------------------------------------- #

def per_campaign(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_session: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_session[r["session"]].append(r)

    report: Dict[str, Any] = {}
    for session in sorted(by_session):
        rs = by_session[session]
        n = len(rs)
        sig = [r["signal"] for r in rs if r["signal"]]

        scores = [r["cand_score"] for r in rs if r["cand_score"] is not None]
        waiting = Counter()
        for s in sig:
            for w in s["waiting_reasons"]:
                waiting[w] += 1

        # How many symbols can the user actually TELL APART from the output the
        # Product API exposes on the list screen?
        list_fingerprint = Counter(
            (
                r["cand_verdict"],
                None if r["cand_score"] is None else round(r["cand_score"], 4),
                (r["signal"] or {}).get("setup_present"),
                (r["signal"] or {}).get("trigger_confirmed"),
                r["ctrl_verdict"],
            )
            for r in rs
        )
        # ...and from the full evidence on the detail screen
        detail_fingerprint = Counter(
            (
                r["cand_verdict"],
                (r["signal"] or {}).get("setup_state"),
                r["structure_state"],
                r["selected_phase"],
                (r["signal"] or {}).get("trigger_state"),
                tuple((r["signal"] or {}).get("waiting_reasons") or ()),
                (r["signal"] or {}).get("readiness_status"),
            )
            for r in rs
        )

        entry = {
            "n": n,
            "candidate_verdicts": dist(r["cand_verdict"] for r in rs),
            "control_verdicts": dist(r["ctrl_verdict"] for r in rs),
            "candidate_scored": len(scores),
            "candidate_score_values": dist(round(s, 4) for s in scores),
            "control_scored": sum(1 for r in rs if r["ctrl_score"] is not None),
            "control_score_nonzero": sum(
                1 for r in rs if (r["ctrl_score"] or 0) > 0
            ),
            "setup_state": dist((r["signal"] or {}).get("setup_state") for r in rs),
            "setup_present": dist((r["signal"] or {}).get("setup_present") for r in rs),
            "structure_state": dist(r["structure_state"] for r in rs),
            "selected_phase": dist(r["selected_phase"] for r in rs),
            "trigger_state": dist((r["signal"] or {}).get("trigger_state") for r in rs),
            "trigger_confirmed": dist(
                (r["signal"] or {}).get("trigger_confirmed") for r in rs
            ),
            "readiness_status": dist(
                (r["signal"] or {}).get("readiness_status") for r in rs
            ),
            "reason_code": dist(r["policy_reason_code"] for r in rs),
            "waiting_reasons": dict(sorted(waiting.items(), key=lambda kv: -kv[1])),
            "enter_eligible_without_rollout_gate": dist(
                (r["signal"] or {}).get("enter_eligible_without_rollout_gate")
                for r in rs
            ),
            "agreement_matrix": dist(
                f"{r['cand_verdict']} / {r['ctrl_verdict']}" for r in rs
            ),
            "distinct_list_fingerprints": len(list_fingerprint),
            "largest_list_bucket": max(list_fingerprint.values()),
            "distinct_detail_fingerprints": len(detail_fingerprint),
            "largest_detail_bucket": max(detail_fingerprint.values()),
        }
        report[session] = entry

        line(f"CAMPAIGN {session}   (n={n})", "=")
        print("  candidate verdicts:\n" + fmt_counter(entry["candidate_verdicts"], n))
        print("  control verdicts:\n" + fmt_counter(entry["control_verdicts"], n))
        print(f"  candidate scored: {entry['candidate_scored']}/{n}"
              f"   distinct score values: {len(entry['candidate_score_values'])}")
        if entry["candidate_score_values"]:
            print("  candidate score values:\n"
                  + fmt_counter(entry["candidate_score_values"]))
        print(f"  control scored: {entry['control_scored']}/{n}"
              f"   non-zero: {entry['control_score_nonzero']}")
        print("  setup_state:\n" + fmt_counter(entry["setup_state"], n))
        print("  setup_present (as the API exposes it):\n"
              + fmt_counter(entry["setup_present"], n))
        print("  structure.state:\n" + fmt_counter(entry["structure_state"], n))
        print("  selected_phase:\n" + fmt_counter(entry["selected_phase"], n))
        print("  trigger_state:\n" + fmt_counter(entry["trigger_state"], n))
        print("  readiness_status:\n" + fmt_counter(entry["readiness_status"], n))
        print("  policy reason_code:\n" + fmt_counter(entry["reason_code"], n))
        print("  waiting_reasons (symbol count):\n"
              + fmt_counter(entry["waiting_reasons"], n))
        print("  candidate/control verdict pairs:\n"
              + fmt_counter(entry["agreement_matrix"], n))
        print(f"  DISTINGUISHABILITY  list screen : "
              f"{entry['distinct_list_fingerprints']} distinct of {n} "
              f"(largest identical bucket = {entry['largest_list_bucket']})")
        print(f"                      detail screen: "
              f"{entry['distinct_detail_fingerprints']} distinct of {n} "
              f"(largest identical bucket = {entry['largest_detail_bucket']})")
    return report


# --------------------------------------------------------------------------- #
# Cross-campaign per-symbol analysis
# --------------------------------------------------------------------------- #

def cross_campaign(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_symbol[r["symbol"]].append(r)

    line("CROSS-CAMPAIGN PER-SYMBOL EVOLUTION", "=")
    sessions = sorted({r["session"] for r in rows})
    print(f"  sessions: {', '.join(sessions)}")
    print(f"\n  {'SYM':<6} {'verdicts':<26} {'scores':<30} "
          f"{'setup_state':<30} {'ctrl':<22} stable")

    out: Dict[str, Any] = {}
    never_changed = 0
    persistent_disagreement = 0
    for symbol in sorted(by_symbol):
        rs = sorted(by_symbol[symbol], key=lambda r: r["session"])
        verdicts = [r["cand_verdict"] for r in rs]
        scores = ["-" if r["cand_score"] is None else f"{r['cand_score']:.2f}" for r in rs]
        setups = [str((r["signal"] or {}).get("setup_state")) for r in rs]
        ctrls = [r["ctrl_verdict"] for r in rs]
        disagreements = [v != c for v, c in zip(verdicts, ctrls)]
        stable = len(set(verdicts)) == 1
        if stable:
            never_changed += 1
        if all(disagreements):
            persistent_disagreement += 1
        out[symbol] = {
            "verdicts": verdicts, "scores": scores,
            "setup_states": setups, "control_verdicts": ctrls,
            "verdict_stable": stable,
            "disagreement_every_session": all(disagreements),
        }
        print(f"  {symbol:<6} {','.join(v or '-' for v in verdicts):<26} "
              f"{','.join(scores):<30} {','.join(setups):<30} "
              f"{','.join(c or '-' for c in ctrls):<22} {'yes' if stable else 'NO'}")

    print(f"\n  symbols whose candidate verdict NEVER changed across 4 campaigns: "
          f"{never_changed}/{len(by_symbol)}")
    print(f"  symbols where candidate/control disagreed in EVERY campaign:        "
          f"{persistent_disagreement}/{len(by_symbol)}")
    return {
        "per_symbol": out,
        "verdict_never_changed": never_changed,
        "persistent_disagreement": persistent_disagreement,
        "symbols": len(by_symbol),
    }


# --------------------------------------------------------------------------- #
# Score semantics
# --------------------------------------------------------------------------- #

def score_semantics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    line("CANDIDATE SCORE SEMANTICS", "=")
    scored = [r for r in rows if r["cand_score"] is not None]
    unscored = [r for r in rows if r["cand_score"] is None]
    print(f"  evaluations with a candidate score : {len(scored)}/{len(rows)}")
    print(f"  evaluations WITHOUT a score        : {len(unscored)}/{len(rows)}")

    by_verdict_scored = Counter(r["cand_verdict"] for r in scored)
    by_verdict_unscored = Counter(r["cand_verdict"] for r in unscored)
    print("  scored rows by verdict:\n" + fmt_counter(dict(by_verdict_scored)))
    print("  UNSCORED rows by verdict:\n" + fmt_counter(dict(by_verdict_unscored)))

    vals = sorted(round(r["cand_score"], 6) for r in scored)
    print(f"  distinct score values: {len(set(vals))} across {len(vals)} scored rows")
    if vals:
        print(f"  min={vals[0]:.4f}  max={vals[-1]:.4f}")
    print("  score_version:\n" + fmt_counter(dist(r["score_version"] for r in rows)))

    # Does score separate WATCH from AVOID among rows that HAVE a score?
    watch = sorted(r["cand_score"] for r in scored if r["cand_verdict"] == "WATCH")
    avoid = sorted(r["cand_score"] for r in scored if r["cand_verdict"] == "AVOID")
    print(f"\n  scored WATCH n={len(watch)} "
          f"range=[{watch[0]:.4f}, {watch[-1]:.4f}]" if watch else "\n  scored WATCH n=0")
    print(f"  scored AVOID n={len(avoid)} "
          f"range=[{avoid[0]:.4f}, {avoid[-1]:.4f}]" if avoid else "  scored AVOID n=0")
    overlap = bool(watch and avoid and not (min(watch) > max(avoid) or min(avoid) > max(watch)))
    print(f"  WATCH/AVOID score ranges overlap: {overlap}")

    return {
        "scored": len(scored), "unscored": len(unscored),
        "distinct_values": len(set(vals)),
        "watch_scored": len(watch), "avoid_scored": len(avoid),
        "ranges_overlap": overlap,
    }


# --------------------------------------------------------------------------- #
# Agreement semantics
# --------------------------------------------------------------------------- #

def agreement_semantics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    line("CANDIDATE/CONTROL AGREEMENT SEMANTICS", "=")
    cand_vocab = dist(r["cand_verdict"] for r in rows)
    ctrl_vocab = dist(r["ctrl_verdict"] for r in rows)
    print("  candidate verdict vocabulary:\n" + fmt_counter(cand_vocab))
    print("  control verdict vocabulary:\n" + fmt_counter(ctrl_vocab))
    shared = set(cand_vocab) & set(ctrl_vocab)
    print(f"  verdict values BOTH arms can emit: {sorted(shared)}")
    agree = [r for r in rows if r["cand_verdict"] == r["ctrl_verdict"]]
    print(f"  agreement rows: {len(agree)}/{len(rows)}")
    print("  what agreement actually means:\n"
          + fmt_counter(dist(r["cand_verdict"] for r in agree)))
    return {
        "candidate_vocabulary": sorted(cand_vocab),
        "control_vocabulary": sorted(ctrl_vocab),
        "shared_vocabulary": sorted(shared),
        "agree_rows": len(agree),
        "total": len(rows),
    }


def attention_model(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """What the shipped attention tiering does to the REAL campaigns.

    This is the justification check for app/scanner_view.py::classify_attention:
    a tiering that leaves every symbol in one bucket would be no better than the
    verdict it replaced.
    """
    line("ATTENTION MODEL APPLIED TO REAL CAMPAIGNS", "=")
    by_session: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_session[r["session"]].append(r)

    out: Dict[str, Any] = {}
    for session in sorted(by_session):
        rs = by_session[session]
        tiers = Counter()
        for r in rs:
            sig = r["signal"] or {}
            tiers[sv.classify_attention(
                has_candidate_result=r["cand_verdict"] is not None,
                candidate_verdict=r["cand_verdict"],
                setup_state=sig.get("setup_state"),
                readiness_status=sig.get("readiness_status"),
                control_verdict=r["ctrl_verdict"],
            )] += 1
        out[session] = dict(tiers)
        occupied = sum(1 for t in sv.ATTENTION_TIERS if tiers.get(t))
        print(f"\n  {session}  (n={len(rs)}, tiers occupied: {occupied}/"
              f"{len(sv.ATTENTION_TIERS)})")
        for tier in sv.ATTENTION_TIERS:
            n = tiers.get(tier, 0)
            bar = "#" * n
            print(f"    {tier:<16} {n:>3}  {bar}")

    print("\n  Compare with the field the list used to lead on:")
    print("    setup_present == True for "
          f"{sum(1 for r in rows if (r['signal'] or {}).get('setup_present'))}"
          f"/{len(rows)} evaluations (differentiates nothing)")
    return out


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="write the full report to this path")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print("DATABASE_URL is required (read-only DSN); it is never printed.",
              file=sys.stderr)
        return 2

    rows = await load(dsn)
    print(f"loaded {len(rows)} pair rows across "
          f"{len({r['session'] for r in rows})} campaigns")

    report = {
        "pair_rows": len(rows),
        "per_campaign": per_campaign(rows),
        "cross_campaign": cross_campaign(rows),
        "attention_model": attention_model(rows),
        "score_semantics": score_semantics(rows),
        "agreement_semantics": agreement_semantics(rows),
    }
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(report, fh, indent=1, default=str)
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
