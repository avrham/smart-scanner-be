"""PURE read-only builders for the paired candidate-vs-control analytical surface.

This module never touches the database, a provider or a token. The audit
endpoint fetches rows through the EXISTING frozen readers
(`fetch_pair_outcomes` → paired verdicts + ret_1d..20d/MFE/MAE/benchmark, and
`fetch_strategy_shadow_evaluations` → per-arm decision detail) and hands them to
the pure functions here, which:

  * join candidate + control + outcome per pair (symmetric, no silent drops);
  * reconcile arm/outcome structure (duplicate / missing arm, missing outcome);
  * expose a bounded, secret-free pair-level dataset (`shadow_paired_comparison.v1`);
  * compute symmetric per-population, per-horizon aggregates and — only above a
    documented minimum sample — paired inferential statistics
    (`shadow_paired_metrics.v1`).

Decision detail is derived using the SAME production classifiers from
`app.workers.shadow.strategy_metrics` (read-only import) so semantics never drift
from the aggregation the closeout uses.
"""

from __future__ import annotations

import math
import random
from typing import Any, Dict, List, Optional, Tuple

# Read-only reuse of the production decision classifiers (never re-derived here).
from app.workers.shadow.strategy_metrics import (
    TRIGGER_CLASS_CONFIRMED,
    classify_trigger_state,
    is_pre_rollout_enter_candidate,
    is_rollout_blocked,
)

PAIRED_COMPARISON_CONTRACT_VERSION = "shadow_paired_comparison.v1"
PAIRED_METRICS_CONTRACT_VERSION = "shadow_paired_metrics.v1"

# Horizons (calendar-day labels matching the stored outcome return keys).
HORIZONS: Tuple[str, ...] = ("1d", "3d", "5d", "10d", "20d")

# Inferential statistics are SUPPRESSED below this paired sample size — a small
# sample cannot support a defensible significance claim. Documented + tested.
MIN_INFERENTIAL_SAMPLE = 30
# Number of horizons an inferential claim spans → Bonferroni family size.
HORIZON_FAMILY_SIZE = len(HORIZONS)

CANDIDATE_STRATEGY = "wyckoff_mtf_v2"
CONTROL_STRATEGY = "sma150_bounce"
ACTIONABLE_VERDICTS = frozenset({"ENTER", "WATCH"})


# --------------------------------------------------------------------------- #
# Candidate/control decision-detail extraction (via production classifiers).
# --------------------------------------------------------------------------- #
def _setup_present(evaluation: Optional[Dict[str, Any]]) -> Optional[bool]:
    if not evaluation:
        return None
    policy = evaluation.get("policy") or {}
    if not isinstance(policy, dict) or "setup_state" not in policy:
        return None
    return policy.get("setup_state") == "valid"


def _trigger_confirmed(evaluation: Optional[Dict[str, Any]]) -> Optional[bool]:
    if not evaluation:
        return None
    if evaluation.get("four_hour_trigger") is None and not evaluation.get("policy"):
        return None
    return classify_trigger_state(evaluation) == TRIGGER_CLASS_CONFIRMED


def _four_hour_state(evaluation: Optional[Dict[str, Any]]) -> Optional[str]:
    if not evaluation:
        return None
    meta = evaluation.get("four_hour_frame_meta")
    if isinstance(meta, dict):
        return meta.get("state")
    return None


def _candidate_actionable(evaluation: Optional[Dict[str, Any]], verdict: Optional[str]) -> bool:
    """Candidate is actionable if its FINAL verdict is ENTER/WATCH OR it was
    enter-eligible before the rollout gate (the pre-rollout signal). This is the
    key correction: actionability is NOT `verdict==ENTER` while allow_enter=false."""
    if verdict in ACTIONABLE_VERDICTS:
        return True
    return is_pre_rollout_enter_candidate(evaluation or {}) is True


# --------------------------------------------------------------------------- #
# Reconciliation + paired join (Part 6 — symmetric, no silent drops).
# --------------------------------------------------------------------------- #
def reconcile_pairs(
    candidate_records: List[Dict[str, Any]],
    control_records: List[Dict[str, Any]],
    outcome_items: List[Dict[str, Any]],
    *,
    excluded_manual_pair_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Group candidate + control evaluation rows and outcome rows by pair_id and
    report the full structural reconciliation. Every anomaly is COUNTED and
    sampled — never silently dropped."""
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
        p for p in with_both
        if (cand[p][0].get("outcome_status") != "complete")
    )

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
    """Assemble one safe, secret-free paired row (`shadow_paired_comparison.v1`)."""
    cand_recs = recon_index["cand"].get(pair_id, [])
    ctrl_recs = recon_index["ctrl"].get(pair_id, [])
    cand = cand_recs[0] if cand_recs else None
    ctrl = ctrl_recs[0] if ctrl_recs else None
    oc = recon_index["outcomes"].get(pair_id)

    base = cand or ctrl or {}
    cand_verdict = cand.get("verdict") if cand else None

    # outcome returns (null-preserving — missing is null, never zero). The
    # frozen reader nests returns under oc["outcome"] and relative_returns at the
    # top level of the outcome item.
    oc_outcome = (oc or {}).get("outcome") or {}
    returns = oc_outcome.get("returns")
    outcome_block: Dict[str, Any] = {
        "status": oc_outcome.get("outcome_status") if oc else (cand or {}).get("outcome_status"),
        "returns": {h: (returns or {}).get(h) for h in HORIZONS} if returns is not None
        else {h: None for h in HORIZONS},
        "benchmark_returns": oc_outcome.get("benchmark_returns") if oc else None,
        "relative_returns": (oc or {}).get("relative_returns") if oc else None,
        "max_favorable_excursion": oc_outcome.get("max_favorable_excursion") if oc else None,
        "max_adverse_excursion": oc_outcome.get("max_adverse_excursion") if oc else None,
        # stop/target only surfaced when GENUINELY present (the stored outcome
        # does not currently persist them → null, never coerced to zero).
        "stop_price": oc_outcome.get("stop_price") if oc else None,
        "target_price": oc_outcome.get("target_price") if oc else None,
    }

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
            "pre_rollout_enter_eligible": is_pre_rollout_enter_candidate(cand or {}),
            "rollout_blocked": is_rollout_blocked(cand or {}),
            "score": (cand or {}).get("score"),
            "four_hour_frame_state": _four_hour_state(cand),
            "actionable": _candidate_actionable(cand, cand_verdict),
        },
        "control": {
            "strategy_code": (ctrl or {}).get("strategy_code"),
            "strategy_version": (ctrl or {}).get("strategy_version"),
            "arm_code": (ctrl or {}).get("arm_code"),
            "verdict": (ctrl or {}).get("verdict"),
            "score": (ctrl or {}).get("score"),
            "actionable": ((ctrl or {}).get("verdict") in ACTIONABLE_VERDICTS),
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


def build_paired_comparison(
    reconciliation: Dict[str, Any],
    *,
    experiment_code: str,
    campaign_scope: str,
    horizon: Optional[str] = None,
    decision_population: Optional[str] = None,
    cursor: int = 0,
    limit: int = 100,
) -> Dict[str, Any]:
    """`shadow_paired_comparison.v1` — bounded, cursor-paginated pair-level rows."""
    idx = reconciliation["_index"]
    all_pairs = sorted(set(idx["cand"]) | set(idx["ctrl"]))
    rows = [build_pair_row(p, idx) for p in all_pairs]

    if decision_population:
        rows = [r for r in rows if _in_population(r, decision_population)]

    total = len(rows)
    limit = max(1, min(int(limit), 500))
    cursor = max(0, int(cursor))
    page = rows[cursor:cursor + limit]
    next_cursor = cursor + limit if cursor + limit < total else None

    return {
        "contract_version": PAIRED_COMPARISON_CONTRACT_VERSION,
        "experiment_code": experiment_code,
        "cohort_scope": campaign_scope,
        "candidate_strategy": CANDIDATE_STRATEGY,
        "control_strategy": CONTROL_STRATEGY,
        "horizon_filter": horizon,
        "decision_population_filter": decision_population,
        "reconciliation": {k: v for k, v in reconciliation.items() if k != "_index"},
        "total_rows": total,
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "rows": page,
        "null_semantics": (
            "A null return / benchmark / stop / target means the value is not "
            "present in the stored outcome; it is NEVER coerced to zero. A null "
            "candidate detail (readiness/setup/trigger/score) means that arm did "
            "not record it. Absent arm → structure flags expose it explicitly."),
    }


# --------------------------------------------------------------------------- #
# Populations (Part 9) — counted before any performance is reported.
# --------------------------------------------------------------------------- #
def _in_population(row: Dict[str, Any], population: str) -> bool:
    c = row["candidate"]
    k = row["control"]
    st = row["structure"]
    valid = st["has_candidate"] and st["has_control"] and not st["duplicate_candidate"] \
        and not st["duplicate_control"] and not st["excluded_manual"]
    if population == "A_full":
        return valid and st["has_outcome"]
    if population == "B_candidate_ready":
        return valid and c["readiness_status"] == "ready"
    if population == "C_candidate_trigger_confirmed":
        return valid and c["trigger_confirmed"] is True
    if population == "D_candidate_actionable":
        return valid and c["actionable"] is True
    if population == "E_control_actionable":
        return valid and k["actionable"] is True
    if population == "F_both_actionable":
        return valid and c["actionable"] is True and k["actionable"] is True
    if population == "candidate_setup_present":
        return valid and c["setup_present"] is True
    return valid


POPULATIONS = (
    "A_full", "B_candidate_ready", "candidate_setup_present",
    "C_candidate_trigger_confirmed", "D_candidate_actionable",
    "E_control_actionable", "F_both_actionable",
)


# --------------------------------------------------------------------------- #
# Pure statistics (paired) — gated by MIN_INFERENTIAL_SAMPLE, effect-size first.
# --------------------------------------------------------------------------- #
def _mean(xs: List[float]) -> Optional[float]:
    return sum(xs) / len(xs) if xs else None


def _median(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _norm_two_sided_p(z: float) -> float:
    # two-sided p from standard normal via erf
    return max(0.0, min(1.0, 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2.0))))))


def sign_test(diffs: List[float]) -> Optional[Dict[str, Any]]:
    nz = [d for d in diffs if d != 0]
    n = len(nz)
    if n < MIN_INFERENTIAL_SAMPLE:
        return None
    k = sum(1 for d in nz if d > 0)
    # exact two-sided binomial p at prob 0.5
    def _cdf(x: int) -> float:
        return sum(math.comb(n, i) for i in range(0, x + 1)) / (2.0 ** n)
    p = min(1.0, 2.0 * min(_cdf(k), 1.0 - _cdf(k - 1))) if n else 1.0
    return {"n_nonzero": n, "positives": k, "negatives": n - k, "p_value": p,
            "test": "sign_test_exact_binomial"}


def wilcoxon_signed_rank(diffs: List[float]) -> Optional[Dict[str, Any]]:
    nz = [d for d in diffs if d != 0]
    n = len(nz)
    if n < MIN_INFERENTIAL_SAMPLE:
        return None
    order = sorted(range(n), key=lambda i: abs(nz[i]))
    ranks = [0.0] * n
    i = 0
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
    # normal-approx two-sided p (n>=30 → acceptable; labelled approximate)
    return {"n": n, "mean_diff": m, "std_error": se, "t_stat": t, "df": n - 1,
            "p_value_normal_approx": _norm_two_sided_p(t),
            "test": "paired_t_normal_approx"}


def bootstrap_ci(diffs: List[float], *, iters: int = 2000, alpha: float = 0.05,
                 seed: int = 0) -> Optional[Dict[str, Any]]:
    n = len(diffs)
    if n < MIN_INFERENTIAL_SAMPLE:
        return None
    rng = random.Random(seed)  # deterministic for fixed input+seed
    means = []
    for _ in range(iters):
        means.append(sum(diffs[rng.randrange(n)] for _ in range(n)) / n)
    means.sort()
    lo = means[int((alpha / 2) * iters)]
    hi = means[min(iters - 1, int((1 - alpha / 2) * iters))]
    return {"mean": _mean(diffs), "ci_lower": lo, "ci_upper": hi,
            "alpha": alpha, "iters": iters, "seed": seed, "method": "percentile"}


def _horizon_stats(pair_returns: List[Tuple[Optional[float], Optional[float]]]) -> Dict[str, Any]:
    """pair_returns: list of (candidate_ret, control_ret) for one horizon.
    Effect sizes and denominators always; inferential stats only when both
    values present for >= MIN_INFERENTIAL_SAMPLE pairs."""
    cand = [c for c, _ in pair_returns if c is not None]
    ctrl = [k for _, k in pair_returns if k is not None]
    paired = [(c, k) for c, k in pair_returns if c is not None and k is not None]
    diffs = [c - k for c, k in paired]
    n_paired = len(diffs)
    result: Dict[str, Any] = {
        "candidate_n": len(cand),
        "control_n": len(ctrl),
        "paired_n": n_paired,
        "candidate_missing": sum(1 for c, _ in pair_returns if c is None),
        "control_missing": sum(1 for _, k in pair_returns if k is None),
        "candidate_mean_return": _mean(cand),
        "candidate_median_return": _median(cand),
        "control_mean_return": _mean(ctrl),
        "control_median_return": _median(ctrl),
        "candidate_positive_return_rate": (sum(1 for c in cand if c > 0) / len(cand)) if cand else None,
        "control_positive_return_rate": (sum(1 for k in ctrl if k > 0) / len(ctrl)) if ctrl else None,
        "mean_paired_difference": _mean(diffs),
        "median_paired_difference": _median(diffs),
        "positive_paired_difference_rate": (sum(1 for d in diffs if d > 0) / n_paired) if n_paired else None,
        "inferential": None,
        "inferential_suppressed_reason": None,
    }
    if n_paired < MIN_INFERENTIAL_SAMPLE:
        result["inferential_suppressed_reason"] = (
            f"paired_n={n_paired} < MIN_INFERENTIAL_SAMPLE={MIN_INFERENTIAL_SAMPLE}")
    else:
        result["inferential"] = {
            "sign_test": sign_test(diffs),
            "wilcoxon_signed_rank": wilcoxon_signed_rank(diffs),
            "paired_t_test": paired_t_test(diffs),
            "bootstrap_ci_mean_diff": bootstrap_ci(diffs),
            "bonferroni_family_size": HORIZON_FAMILY_SIZE,
            "note": ("p-values are two-sided; apply the Bonferroni family size "
                     "for multi-horizon inference. Significance is NOT strategy "
                     "validation — report effect size and denominators."),
        }
    return result


def build_paired_metrics(
    reconciliation: Dict[str, Any],
    *,
    experiment_code: str,
    campaign_scope: str,
) -> Dict[str, Any]:
    """`shadow_paired_metrics.v1` — symmetric candidate/control population counts
    and per-horizon effect sizes + gated inferential statistics."""
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

    per_population: Dict[str, Any] = {}
    for pop in POPULATIONS:
        pop_rows = [r for r in rows if _in_population(r, pop)]
        horizons: Dict[str, Any] = {}
        for h in HORIZONS:
            # One matured outcome per pair is shared by both arms, so the
            # candidate and control forward returns are the SAME pair path here
            # (raw paired difference is definitionally ~0 without an
            # entry-conditioned model — see interpretation_guard). We still
            # report per-arm distributions over each population.
            pr = [(_num(r["outcome"]["returns"].get(h)),
                   _num(r["outcome"]["returns"].get(h)))
                  for r in pop_rows]
            # candidate & control share the SAME per-pair outcome path (one
            # matured outcome per pair); a paired difference is only meaningful
            # under an entry-conditioned model — see runbook. Here candidate and
            # control "returns" are the shared pair forward return, so the
            # paired difference is definitionally 0 unless entry-conditioning is
            # applied. We still expose per-arm actionable-conditioned returns.
            horizons[h] = _horizon_stats(pr)
        per_population[pop] = {"pair_count": len(pop_rows), "horizons": horizons}

    return {
        "contract_version": PAIRED_METRICS_CONTRACT_VERSION,
        "experiment_code": experiment_code,
        "cohort_scope": campaign_scope,
        "candidate_strategy": CANDIDATE_STRATEGY,
        "control_strategy": CONTROL_STRATEGY,
        "min_inferential_sample": MIN_INFERENTIAL_SAMPLE,
        "horizons": list(HORIZONS),
        "population_counts": population_counts,
        "reconciliation": {k: v for k, v in reconciliation.items() if k != "_index"},
        "per_population": per_population,
        "interpretation_guard": (
            "Outcomes are a single per-pair forward market path shared by both "
            "arms; a raw candidate-minus-control return difference is only "
            "economically meaningful under an entry-conditioned model that the "
            "current experiment does NOT define. Treat these as descriptive "
            "signal-level statistics, not a backtest or portfolio result."),
    }


__all__ = [
    "PAIRED_COMPARISON_CONTRACT_VERSION",
    "PAIRED_METRICS_CONTRACT_VERSION",
    "HORIZONS",
    "MIN_INFERENTIAL_SAMPLE",
    "POPULATIONS",
    "CANDIDATE_STRATEGY",
    "CONTROL_STRATEGY",
    "reconcile_pairs",
    "build_pair_row",
    "build_paired_comparison",
    "build_paired_metrics",
    "sign_test",
    "wilcoxon_signed_rank",
    "paired_t_test",
    "bootstrap_ci",
]
