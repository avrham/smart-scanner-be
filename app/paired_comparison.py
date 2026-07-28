"""PURE read-only builders for the paired candidate-vs-control analytical surface.

v2 (`shadow_paired_comparison.v2` / `shadow_paired_metrics.v2`) corrects two
semantic hazards found in v1:

  1. SIGNAL SEMANTICS. v1 collapsed WATCH/ENTER/pre-rollout into a single broad
     `actionable`. WATCH is ambiguous (valid-setup-waiting vs
     trigger-confirmed-but-rollout-blocked). v2 exposes SEPARATE candidate
     populations — setup / trigger-confirmed / pre-rollout-entry-eligible /
     rollout-blocked-entry / final-ENTER / WATCH — plus a per-row
     `watch_classification`, and names a versioned primary signal
     (`candidate_signal_definition = pre_rollout_enter_eligible.v1`). It does NOT
     emit a bare `actionable = WATCH or ENTER` field.

  2. OUTCOME SEMANTICS. One matured outcome per pair is a SHARED MARKET PATH
     (Concept A) — identical for both arms. v1 could be read as implying a
     candidate-minus-control strategy return; that difference is definitionally
     ~0 and is NOT a strategy P&L. v2 removes the paired-difference framing and
     instead reports SELECTION-CONDITIONED MARKET-PATH distributions
     (candidate-selected / control-selected / both / candidate-only /
     control-only / neither / unconditional) — labelled selection-quality
     analyses, not arm returns. A true arm-conditioned outcome (Concept B)
     requires arm-specific entry semantics that are NOT currently persisted (see
     the runbook + the proposed `strategy_shadow_arm_outcomes` design).

Pure: no DB, provider or token. The endpoint fetches rows via the frozen readers
(`fetch_evidence_records` per arm, `fetch_pair_outcomes` for the shared path) and
hands them here. Decision detail uses the production classifiers in
`app.workers.shadow.strategy_metrics` (read-only import) so semantics never drift.
"""

from __future__ import annotations

import math
import random
from typing import Any, Dict, List, Optional, Tuple

from app.workers.shadow.strategy_metrics import (
    TRIGGER_CLASS_CONFIRMED,
    classify_trigger_state,
    is_pre_rollout_enter_candidate,
    is_rollout_blocked,
)

PAIRED_COMPARISON_CONTRACT_VERSION = "shadow_paired_comparison.v2"
PAIRED_METRICS_CONTRACT_VERSION = "shadow_paired_metrics.v2"

# The recommended primary candidate signal for the next shadow experiment: the
# strategy's "would-enter" decision measured WHILE allow_enter stays false.
CANDIDATE_SIGNAL_DEFINITION = "pre_rollout_enter_eligible.v1"

HORIZONS: Tuple[str, ...] = ("1d", "3d", "5d", "10d", "20d")
MIN_INFERENTIAL_SAMPLE = 30
HORIZON_FAMILY_SIZE = len(HORIZONS)

CANDIDATE_STRATEGY = "wyckoff_mtf_v2"
CONTROL_STRATEGY = "sma150_bounce"
# sma150_bounce emits only ENTER/AVOID; wyckoff can emit ENTER/WATCH/AVOID.
ACTIONABLE_VERDICTS = frozenset({"ENTER", "WATCH"})


# --------------------------------------------------------------------------- #
# Candidate decision-detail extraction (via production classifiers).
# --------------------------------------------------------------------------- #
def _setup_present(ev: Optional[Dict[str, Any]]) -> Optional[bool]:
    if not ev:
        return None
    policy = ev.get("policy") or {}
    if not isinstance(policy, dict) or "setup_state" not in policy:
        return None
    return policy.get("setup_state") == "valid"


def _trigger_confirmed(ev: Optional[Dict[str, Any]]) -> Optional[bool]:
    if not ev:
        return None
    if ev.get("four_hour_trigger") is None and not ev.get("policy"):
        return None
    return classify_trigger_state(ev) == TRIGGER_CLASS_CONFIRMED


def _four_hour_state(ev: Optional[Dict[str, Any]]) -> Optional[str]:
    if not ev:
        return None
    meta = ev.get("four_hour_frame_meta")
    return meta.get("state") if isinstance(meta, dict) else None


def _watch_classification(ev: Optional[Dict[str, Any]]) -> Optional[str]:
    """Decompose a WATCH final verdict into its material sub-state (never treat
    all WATCH as equivalent). Determinable purely from persisted fields."""
    if not ev or ev.get("verdict") != "WATCH":
        return None
    if is_rollout_blocked(ev) is True:
        # confirmed/eligible ENTER setup blocked only by allow_enter=false
        return "trigger_confirmed_rollout_blocked"
    if _trigger_confirmed(ev) is True:
        return "trigger_confirmed_other"
    if _setup_present(ev) is True:
        return "valid_setup_trigger_unconfirmed"
    return "watch_other"


# --------------------------------------------------------------------------- #
# Reconciliation + paired join (symmetric; no silent drops).
# --------------------------------------------------------------------------- #
def reconcile_pairs(
    candidate_records: List[Dict[str, Any]],
    control_records: List[Dict[str, Any]],
    outcome_items: List[Dict[str, Any]],
    *,
    excluded_manual_pair_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    excluded = set(excluded_manual_pair_ids or [])

    def _by_pair(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        for r in records:
            out.setdefault(str(r["pair_id"]), []).append(r)
        return out

    cand = _by_pair(candidate_records)
    ctrl = _by_pair(control_records)
    outcomes = {str(o["pair"]["pair_id"]): o for o in outcome_items}

    all_pairs = set(cand) | set(ctrl)
    dup_candidate = sorted(p for p, rs in cand.items() if len(rs) > 1)
    dup_control = sorted(p for p, rs in ctrl.items() if len(rs) > 1)
    missing_candidate = sorted(p for p in all_pairs if p not in cand)
    missing_control = sorted(p for p in all_pairs if p not in ctrl)
    with_both = sorted(p for p in all_pairs if p in cand and p in ctrl)
    missing_outcome = sorted(
        p for p in with_both if (cand[p][0].get("outcome_status") != "complete"))

    return {
        "raw_pair_rows": len(all_pairs),
        "raw_candidate_evaluation_rows": len(candidate_records),
        "raw_control_evaluation_rows": len(control_records),
        "unique_candidate_pairs": len(cand),
        "unique_control_pairs": len(ctrl),
        "valid_paired_rows": len([p for p in with_both if p not in dup_candidate
                                  and p not in dup_control and p not in excluded]),
        "missing_candidate_rows": len(missing_candidate),
        "missing_control_rows": len(missing_control),
        "duplicate_candidate_rows": len(dup_candidate),
        "duplicate_control_rows": len(dup_control),
        "missing_outcome_rows": len(missing_outcome),
        "excluded_manual_rows": len([p for p in all_pairs if p in excluded]),
        "samples": {
            "duplicate_candidate": dup_candidate[:20],
            "duplicate_control": dup_control[:20],
            "missing_candidate": missing_candidate[:20],
            "missing_control": missing_control[:20],
            "missing_outcome": missing_outcome[:20],
        },
        "_index": {"cand": cand, "ctrl": ctrl, "outcomes": outcomes,
                   "with_both": with_both, "excluded": excluded,
                   "dup_candidate": set(dup_candidate),
                   "dup_control": set(dup_control)},
    }


def _num(v: Any) -> Optional[float]:
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


def build_pair_row(pair_id: str, recon_index: Dict[str, Any]) -> Dict[str, Any]:
    cand_recs = recon_index["cand"].get(pair_id, [])
    ctrl_recs = recon_index["ctrl"].get(pair_id, [])
    cand = cand_recs[0] if cand_recs else None
    ctrl = ctrl_recs[0] if ctrl_recs else None
    oc = recon_index["outcomes"].get(pair_id)
    base = cand or ctrl or {}
    cand_verdict = cand.get("verdict") if cand else None
    ctrl_verdict = ctrl.get("verdict") if ctrl else None

    oc_outcome = (oc or {}).get("outcome") or {}
    returns = oc_outcome.get("returns")
    outcome_block: Dict[str, Any] = {
        "concept": "shared_market_path_outcome",
        "shared_across_arms": True,
        "status": oc_outcome.get("outcome_status") if oc else (cand or {}).get("outcome_status"),
        "market_path_returns": {h: (returns or {}).get(h) for h in HORIZONS}
        if returns is not None else {h: None for h in HORIZONS},
        "benchmark_returns": oc_outcome.get("benchmark_returns") if oc else None,
        "relative_returns": (oc or {}).get("relative_returns") if oc else None,
        "max_favorable_excursion": oc_outcome.get("max_favorable_excursion") if oc else None,
        "max_adverse_excursion": oc_outcome.get("max_adverse_excursion") if oc else None,
        # Arm-specific entry/stop/target are NOT persisted → null, never fabricated.
        "arm_conditioned_available": False,
        "stop_price": None,
        "target_price": None,
    }

    pre_rollout = is_pre_rollout_enter_candidate(cand or {})
    return {
        "pair_id": pair_id,
        "pair_fingerprint": base.get("pair_fingerprint"),
        "symbol": base.get("symbol"),
        "snapshot_date": str(base.get("snapshot_date")) if base.get("snapshot_date") else None,
        "campaign_ids": base.get("campaign_ids") or [],
        "candidate": {
            "strategy_code": (cand or {}).get("strategy_code"),
            "strategy_version": (cand or {}).get("strategy_version"),
            "arm_code": (cand or {}).get("arm_code"),
            "verdict": cand_verdict,
            "readiness_status": (cand or {}).get("readiness_status"),
            "setup_present": _setup_present(cand),
            "trigger_confirmed": _trigger_confirmed(cand),
            "pre_rollout_enter_eligible": pre_rollout,
            "rollout_blocked": is_rollout_blocked(cand or {}),
            "final_enter": cand_verdict == "ENTER",
            "watch": cand_verdict == "WATCH",
            "watch_classification": _watch_classification(cand),
            "score": (cand or {}).get("score"),
            "four_hour_frame_state": _four_hour_state(cand),
            # primary signal for the prospective experiment (NOT a broad actionable)
            "primary_signal": pre_rollout is True,
            "primary_signal_definition": CANDIDATE_SIGNAL_DEFINITION,
        },
        "control": {
            "strategy_code": (ctrl or {}).get("strategy_code"),
            "strategy_version": (ctrl or {}).get("strategy_version"),
            "arm_code": (ctrl or {}).get("arm_code"),
            "verdict": ctrl_verdict,
            "score": (ctrl or {}).get("score"),
            "signal": ctrl_verdict in ACTIONABLE_VERDICTS,  # sma150 => ENTER
        },
        "outcome": outcome_block,
        "structure": {
            "has_candidate": cand is not None,
            "has_control": ctrl is not None,
            "duplicate_candidate": pair_id in recon_index["dup_candidate"],
            "duplicate_control": pair_id in recon_index["dup_control"],
            "has_outcome": oc is not None,
            "excluded_manual": pair_id in recon_index["excluded"],
        },
    }


def _valid(row: Dict[str, Any]) -> bool:
    st = row["structure"]
    return (st["has_candidate"] and st["has_control"] and not st["duplicate_candidate"]
            and not st["duplicate_control"] and not st["excluded_manual"])


# Explicit, separated candidate populations (Part 3) — never a broad "actionable".
def _in_population(row: Dict[str, Any], population: str) -> bool:
    if not _valid(row):
        return False
    c, k = row["candidate"], row["control"]
    if population == "all_valid_pairs":
        return True
    if population == "candidate_ready_population":
        return c["readiness_status"] == "ready"
    if population == "candidate_setup_population":
        return c["setup_present"] is True
    if population == "candidate_trigger_population":
        return c["trigger_confirmed"] is True
    if population == "candidate_pre_rollout_entry_population":
        return c["pre_rollout_enter_eligible"] is True
    if population == "candidate_rollout_blocked_entry_population":
        return (c["pre_rollout_enter_eligible"] is True and c["verdict"] != "ENTER"
                and c["rollout_blocked"] is True)
    if population == "candidate_final_enter_population":
        return c["verdict"] == "ENTER"
    if population == "candidate_watch_population":
        return c["verdict"] == "WATCH"
    if population == "control_signal_population":
        return c is not None and k["signal"] is True
    return False


POPULATIONS = (
    "all_valid_pairs", "candidate_ready_population", "candidate_setup_population",
    "candidate_trigger_population", "candidate_pre_rollout_entry_population",
    "candidate_rollout_blocked_entry_population", "candidate_final_enter_population",
    "candidate_watch_population", "control_signal_population",
)

# Selection populations for market-path (selection-quality) analysis. Candidate
# selection uses the versioned PRIMARY signal (pre-rollout entry eligibility).
def _candidate_selected(row: Dict[str, Any]) -> bool:
    return _valid(row) and row["candidate"]["primary_signal"] is True


def _control_selected(row: Dict[str, Any]) -> bool:
    return _valid(row) and row["control"]["signal"] is True


SELECTION_POPULATIONS = (
    "candidate_selected", "control_selected", "both_selected",
    "candidate_only", "control_only", "neither_selected", "unconditional",
)


def _in_selection(row: Dict[str, Any], sel: str) -> bool:
    if not _valid(row):
        return False
    cs, ks = _candidate_selected(row), _control_selected(row)
    return {
        "candidate_selected": cs, "control_selected": ks,
        "both_selected": cs and ks, "candidate_only": cs and not ks,
        "control_only": ks and not cs, "neither_selected": not cs and not ks,
        "unconditional": True,
    }[sel]


def build_paired_comparison(
    reconciliation: Dict[str, Any], *, experiment_code: str, campaign_scope: str,
    horizon: Optional[str] = None, decision_population: Optional[str] = None,
    cursor: int = 0, limit: int = 100,
) -> Dict[str, Any]:
    idx = reconciliation["_index"]
    all_pairs = sorted(set(idx["cand"]) | set(idx["ctrl"]))
    rows = [build_pair_row(p, idx) for p in all_pairs]
    if decision_population:
        rows = [r for r in rows if _in_population(r, decision_population)]
    total = len(rows)
    limit = max(1, min(int(limit), 500))
    cursor = max(0, int(cursor))
    page = rows[cursor:cursor + limit]
    return {
        "contract_version": PAIRED_COMPARISON_CONTRACT_VERSION,
        "experiment_code": experiment_code,
        "cohort_scope": campaign_scope,
        "candidate_strategy": CANDIDATE_STRATEGY,
        "control_strategy": CONTROL_STRATEGY,
        "candidate_signal_definition": CANDIDATE_SIGNAL_DEFINITION,
        "horizon_filter": horizon,
        "decision_population_filter": decision_population,
        "available_decision_populations": list(POPULATIONS),
        "reconciliation": {k: v for k, v in reconciliation.items() if k != "_index"},
        "total_rows": total,
        "cursor": cursor,
        "limit": limit,
        "next_cursor": cursor + limit if cursor + limit < total else None,
        "rows": page,
        "semantics": {
            "outcome": ("outcome is a SHARED_MARKET_PATH (Concept A): one matured "
                        "outcome per pair, identical for both arms. Arm-conditioned "
                        "(entry-specific) outcomes are NOT available (no persisted "
                        "arm entry price/timestamp)."),
            "candidate_signal": ("WATCH is decomposed via watch_classification; the "
                                 "primary signal is pre_rollout_enter_eligible, NOT a "
                                 "broad actionable=WATCH-or-ENTER."),
            "null": ("null return / benchmark / stop / target = not present in the "
                     "stored outcome; never coerced to zero."),
        },
    }


# --------------------------------------------------------------------------- #
# Statistics (used only where a real non-degenerate sample exists).
# --------------------------------------------------------------------------- #
def _mean(xs: List[float]) -> Optional[float]:
    return sum(xs) / len(xs) if xs else None


def _median(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs); n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _norm_two_sided_p(z: float) -> float:
    return max(0.0, min(1.0, 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2.0))))))


def sign_test(diffs: List[float]) -> Optional[Dict[str, Any]]:
    nz = [d for d in diffs if d != 0]; n = len(nz)
    if n < MIN_INFERENTIAL_SAMPLE:
        return None
    k = sum(1 for d in nz if d > 0)
    def _cdf(x: int) -> float:
        return sum(math.comb(n, i) for i in range(0, x + 1)) / (2.0 ** n)
    p = min(1.0, 2.0 * min(_cdf(k), 1.0 - _cdf(k - 1)))
    return {"n_nonzero": n, "positives": k, "negatives": n - k, "p_value": p,
            "test": "sign_test_exact_binomial"}


def wilcoxon_signed_rank(diffs: List[float]) -> Optional[Dict[str, Any]]:
    nz = [d for d in diffs if d != 0]; n = len(nz)
    if n < MIN_INFERENTIAL_SAMPLE:
        return None
    order = sorted(range(n), key=lambda i: abs(nz[i]))
    ranks = [0.0] * n; i = 0
    while i < n:
        j = i
        while j + 1 < n and abs(nz[order[j + 1]]) == abs(nz[order[i]]):
            j += 1
        avg = (i + 1 + j + 1) / 2.0
        for t in range(i, j + 1):
            ranks[order[t]] = avg
        i = j + 1
    w_plus = sum(ranks[i] for i in range(n) if nz[i] > 0)
    mean_w = n * (n + 1) / 4.0
    sd_w = math.sqrt(n * (n + 1) * (2 * n + 1) / 24.0)
    z = (w_plus - mean_w) / sd_w if sd_w else 0.0
    return {"n_nonzero": n, "w_plus": w_plus, "z": z,
            "p_value": _norm_two_sided_p(z), "test": "wilcoxon_normal_approx"}


def paired_t_test(diffs: List[float]) -> Optional[Dict[str, Any]]:
    n = len(diffs)
    if n < MIN_INFERENTIAL_SAMPLE:
        return None
    m = _mean(diffs) or 0.0
    var = sum((d - m) ** 2 for d in diffs) / (n - 1) if n > 1 else 0.0
    se = math.sqrt(var / n) if n else 0.0
    t = m / se if se else 0.0
    return {"n": n, "mean_diff": m, "std_error": se, "t_stat": t, "df": n - 1,
            "p_value_normal_approx": _norm_two_sided_p(t), "test": "paired_t_normal_approx"}


def bootstrap_ci(diffs: List[float], *, iters: int = 2000, alpha: float = 0.05,
                 seed: int = 0) -> Optional[Dict[str, Any]]:
    n = len(diffs)
    if n < MIN_INFERENTIAL_SAMPLE:
        return None
    rng = random.Random(seed)
    means = [sum(diffs[rng.randrange(n)] for _ in range(n)) / n for _ in range(iters)]
    means.sort()
    return {"mean": _mean(diffs), "ci_lower": means[int((alpha / 2) * iters)],
            "ci_upper": means[min(iters - 1, int((1 - alpha / 2) * iters))],
            "alpha": alpha, "iters": iters, "seed": seed, "method": "percentile"}


def _market_path_dist(rows: List[Dict[str, Any]], horizon: str) -> Dict[str, Any]:
    vals = [_num(r["outcome"]["market_path_returns"].get(horizon)) for r in rows]
    present = [v for v in vals if v is not None]
    return {
        "n": len(rows),
        "n_with_return": len(present),
        "missing": len(vals) - len(present),
        "mean_market_path_return": _mean(present),
        "median_market_path_return": _median(present),
        "positive_return_rate": (sum(1 for v in present if v > 0) / len(present))
        if present else None,
    }


def build_paired_metrics(
    reconciliation: Dict[str, Any], *, experiment_code: str, campaign_scope: str,
) -> Dict[str, Any]:
    idx = reconciliation["_index"]
    all_pairs = sorted(set(idx["cand"]) | set(idx["ctrl"]))
    rows = [build_pair_row(p, idx) for p in all_pairs]

    population_counts = {pop: sum(1 for r in rows if _in_population(r, pop))
                         for pop in POPULATIONS}
    population_counts["pairs_excluded_data_quality"] = sum(
        1 for r in rows if r["structure"]["duplicate_candidate"]
        or r["structure"]["duplicate_control"])
    population_counts["pairs_excluded_missing_history"] = sum(
        1 for r in rows if r["candidate"]["readiness_status"] not in (None, "ready"))
    population_counts["pairs_missing_outcome"] = sum(
        1 for r in rows if not r["structure"]["has_outcome"])

    # WATCH decomposition (never a single signal)
    watch_rows = [r for r in rows if r["candidate"]["verdict"] == "WATCH"]
    watch_breakdown: Dict[str, int] = {}
    for r in watch_rows:
        wc = r["candidate"]["watch_classification"] or "watch_other"
        watch_breakdown[wc] = watch_breakdown.get(wc, 0) + 1

    selection_counts = {sel: sum(1 for r in rows if _in_selection(r, sel))
                        for sel in SELECTION_POPULATIONS}
    selection_metrics: Dict[str, Any] = {}
    for sel in SELECTION_POPULATIONS:
        sel_rows = [r for r in rows if _in_selection(r, sel)]
        selection_metrics[sel] = {
            "pair_count": len(sel_rows),
            "horizons": {h: _market_path_dist(sel_rows, h) for h in HORIZONS},
        }

    return {
        "contract_version": PAIRED_METRICS_CONTRACT_VERSION,
        "experiment_code": experiment_code,
        "cohort_scope": campaign_scope,
        "candidate_strategy": CANDIDATE_STRATEGY,
        "control_strategy": CONTROL_STRATEGY,
        "candidate_signal_definition": CANDIDATE_SIGNAL_DEFINITION,
        "min_inferential_sample": MIN_INFERENTIAL_SAMPLE,
        "horizons": list(HORIZONS),
        "population_counts": population_counts,
        "candidate_watch_breakdown": watch_breakdown,
        "reconciliation": {k: v for k, v in reconciliation.items() if k != "_index"},
        "selection_conditioned_market_path_metrics": {
            "note": ("SELECTION-QUALITY analysis over the SHARED market-path "
                     "outcome (Concept A). NOT strategy P&L or arm return. For "
                     "both_selected the forward returns are IDENTICAL for both "
                     "arms by construction — a zero cross-arm difference is NOT "
                     "evidence of equivalence. A true arm-conditioned comparison "
                     "requires arm-specific entry semantics (Concept B), which are "
                     "not persisted (see strategy_shadow_arm_outcomes design)."),
            "selection_population_counts": selection_counts,
            "populations": selection_metrics,
        },
        "prohibited": {
            "candidate_return_minus_control_return": (
                "not emitted: both draw from the same shared pair outcome row"),
            "portfolio_or_pnl_metrics": (
                "not emitted: no entry price/timestamp/sizing/costs defined"),
        },
    }


__all__ = [
    "PAIRED_COMPARISON_CONTRACT_VERSION", "PAIRED_METRICS_CONTRACT_VERSION",
    "CANDIDATE_SIGNAL_DEFINITION", "HORIZONS", "MIN_INFERENTIAL_SAMPLE",
    "POPULATIONS", "SELECTION_POPULATIONS", "CANDIDATE_STRATEGY", "CONTROL_STRATEGY",
    "reconcile_pairs", "build_pair_row", "build_paired_comparison",
    "build_paired_metrics", "sign_test", "wilcoxon_signed_rank", "paired_t_test",
    "bootstrap_ci",
]
