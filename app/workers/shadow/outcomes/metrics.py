"""PURE neutral resolution metrics for shadow pair outcomes (8.1B2).

Contract version: shadow_pair_resolution_metrics.v1.

This module deliberately does NOT reuse aggregate_outcomes: that aggregator
carries signal/trade terminology (win_rate, simulated R, stop/target
baselines) that is invalid for pair market-path outcomes.

Hard rules:
  * rows are NEVER pooled across the mandatory grouping identity (experiment,
    both arm strategy/policy/config identities, both verdicts, disagreement
    category, calculation/coverage/forward-frame versions, forward provider);
  * the canonical neutral term is positive_return_rate — win_rate is never
    emitted;
  * no better/winner/improvement/regression/pass/fail/promote/disable labels
    and no parameter recommendations — grouped neutral evidence only;
  * neutral bands are METRICS-LAYER constants (not strategy thresholds); a
    band change requires a new metrics contract version;
  * action-favorability is derived only for ACTION-DIVERGENT categories
    (exactly one arm chose ENTER), with arm-neutral enter_arm language — v2
    is never assumed to be the entering arm;
  * WATCH/AVOID disagreements are policy_state_disagreement and agreements
    report market outcomes only: action_resolvable=false, price movement is
    context, no arm winner is ever derived.

Input rows are the joined records produced by
persistence.fetch_pair_outcomes (pair + both frozen evaluations + shared
outcome). All functions are pure (no I/O).
"""

from statistics import mean, median
from typing import Any, Dict, List, Optional, Tuple

from app.workers.outcomes.calculator import HOLDING_WINDOWS, window_label
from app.workers.shadow.outcomes.constants import (
    ACTION_DIVERGENT_CATEGORIES,
    METRICS_CONTRACT_VERSION,
    NEUTRAL_BANDS_PCT,
    POLICY_STATE_CATEGORIES,
    STATUS_COMPLETE,
    STATUS_ERROR,
)


GROUPING_IDENTITY_FIELDS = (
    "experiment_code",
    "experiment_version",
    "control_strategy_code",
    "control_strategy_version",
    "control_decision_policy_version",
    "control_config_hash",
    "control_verdict",
    "candidate_strategy_code",
    "candidate_strategy_version",
    "candidate_decision_policy_version",
    "candidate_config_hash",
    "candidate_verdict",
    "disagreement_category",
    "calculation_version",
    "outcome_coverage_version",
    "forward_frame_version",
    "forward_provider",
)


def _mean_or_none(values: List[float]) -> Optional[float]:
    return mean(values) if values else None


def _median_or_none(values: List[float]) -> Optional[float]:
    return median(values) if values else None


def grouping_identity(row: Dict[str, Any]) -> Dict[str, Any]:
    """The mandatory grouping identity for one joined outcome row."""
    pair = row.get("pair") or {}
    control = row.get("control") or {}
    candidate = row.get("candidate") or {}
    outcome = row.get("outcome") or {}
    return {
        "experiment_code": pair.get("experiment_code"),
        "experiment_version": pair.get("experiment_version"),
        "control_strategy_code": control.get("strategy_code"),
        "control_strategy_version": control.get("strategy_version"),
        "control_decision_policy_version": control.get("decision_policy_version"),
        "control_config_hash": control.get("config_hash"),
        "control_verdict": control.get("verdict"),
        "candidate_strategy_code": candidate.get("strategy_code"),
        "candidate_strategy_version": candidate.get("strategy_version"),
        "candidate_decision_policy_version": candidate.get(
            "decision_policy_version"
        ),
        "candidate_config_hash": candidate.get("config_hash"),
        "candidate_verdict": candidate.get("verdict"),
        "disagreement_category": row.get("disagreement_category"),
        "calculation_version": outcome.get("calculation_version"),
        "outcome_coverage_version": outcome.get("outcome_coverage_version"),
        "forward_frame_version": outcome.get("forward_frame_version"),
        "forward_provider": outcome.get("forward_provider"),
    }


def derive_enter_arm(
    control_verdict: Optional[str],
    candidate_verdict: Optional[str],
) -> Optional[str]:
    """'control' or 'candidate' for action-divergent pairs, else None.

    Arm-neutral: the entering arm is derived from the frozen verdicts, never
    assumed. If both or neither entered, there is no single entering arm.
    """
    c = (control_verdict or "").upper() == "ENTER"
    x = (candidate_verdict or "").upper() == "ENTER"
    if c and not x:
        return "control"
    if x and not c:
        return "candidate"
    return None


def classify_horizon_return(
    ret: Optional[float],
    window: int,
    *,
    bands: Dict[int, float] = NEUTRAL_BANDS_PCT,
) -> str:
    """Neutral-band classification for one pair at one horizon.

    Bands are inclusive on the neutral side: |return| <= band is
    flat_or_neutral; only a move STRICTLY beyond the band is favorable to
    either action.
    """
    if ret is None:
        return "incomplete"
    band = bands[window]
    if ret > band:
        return "enter_action_favorable"
    if ret < -band:
        return "non_enter_action_favorable"
    return "flat_or_neutral"


def _pair_return(row: Dict[str, Any], window: int) -> Optional[float]:
    outcome = row.get("outcome") or {}
    returns = outcome.get("returns") or {}
    return returns.get(window_label(window))


def _relative_return(
    row: Dict[str, Any], window: int, benchmark: str
) -> Optional[float]:
    pair_ret = _pair_return(row, window)
    outcome = row.get("outcome") or {}
    bench = (outcome.get("benchmark_returns") or {}).get(benchmark) or {}
    bench_ret = bench.get(window_label(window))
    if pair_ret is None or bench_ret is None:
        return None
    return pair_ret - bench_ret


def _horizon_stats(rows: List[Dict[str, Any]], window: int) -> Dict[str, Any]:
    """Neutral per-horizon statistics for one identity group."""
    returns = [
        r for r in (_pair_return(row, window) for row in rows) if r is not None
    ]
    spy_rel = [
        r for r in (_relative_return(row, window, "SPY") for row in rows)
        if r is not None
    ]
    qqq_rel = [
        r for r in (_relative_return(row, window, "QQQ") for row in rows)
        if r is not None
    ]
    n = len(returns)
    positive = sum(1 for r in returns if r > 0)
    negative = sum(1 for r in returns if r < 0)
    return {
        "window": window_label(window),
        "sample_count": n,
        "mean_return": _mean_or_none(returns),
        "median_return": _median_or_none(returns),
        "positive_return_rate": (positive / n) if n else None,
        "negative_return_rate": (negative / n) if n else None,
        "mean_spy_relative_return": _mean_or_none(spy_rel),
        "median_spy_relative_return": _median_or_none(spy_rel),
        "spy_relative_sample_count": len(spy_rel),
        "mean_qqq_relative_return": _mean_or_none(qqq_rel),
        "median_qqq_relative_return": _median_or_none(qqq_rel),
        "qqq_relative_sample_count": len(qqq_rel),
    }


def _resolution_for_window(
    rows: List[Dict[str, Any]], window: int
) -> Dict[str, Any]:
    """Per-horizon neutral-band resolution for one ACTION-DIVERGENT group.

    missed_upside / avoided_downside use the NON-entering arm's perspective:
    the return the arm that did not act would have observed. All labels stay
    arm-neutral (favorable to the ENTER action vs. favorable to not
    entering) — never a winner claim.
    """
    classifications = [
        classify_horizon_return(_pair_return(row, window), window)
        for row in rows
    ]
    incomplete = sum(1 for c in classifications if c == "incomplete")
    classified = len(classifications) - incomplete
    enter_fav = sum(1 for c in classifications if c == "enter_action_favorable")
    non_enter_fav = sum(
        1 for c in classifications if c == "non_enter_action_favorable"
    )
    flat = sum(1 for c in classifications if c == "flat_or_neutral")

    upside_returns = [
        r for r, c in (
            (_pair_return(row, window), cls)
            for row, cls in zip(rows, classifications)
        )
        if c == "enter_action_favorable" and r is not None
    ]
    downside_returns = [
        r for r, c in (
            (_pair_return(row, window), cls)
            for row, cls in zip(rows, classifications)
        )
        if c == "non_enter_action_favorable" and r is not None
    ]

    return {
        "window": window_label(window),
        "neutral_band_pct": NEUTRAL_BANDS_PCT[window],
        "classified_count": classified,
        "incomplete_count": incomplete,
        "enter_action_favorable_count": enter_fav,
        "non_enter_action_favorable_count": non_enter_fav,
        "flat_or_neutral_count": flat,
        "enter_action_favorable_rate": (
            enter_fav / classified if classified else None
        ),
        "non_enter_action_favorable_rate": (
            non_enter_fav / classified if classified else None
        ),
        "flat_or_neutral_rate": (flat / classified if classified else None),
        # Non-entering-arm perspective (arm-neutral supporting metrics).
        "missed_upside_rate": (enter_fav / classified if classified else None),
        "mean_missed_upside_pct": _mean_or_none(upside_returns),
        "avoided_downside_rate": (
            non_enter_fav / classified if classified else None
        ),
        "mean_avoided_downside_pct": _mean_or_none(downside_returns),
    }


def _group_metrics(
    identity: Dict[str, Any], rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    statuses = [
        (row.get("outcome") or {}).get("outcome_status") for row in rows
    ]
    completed = sum(1 for s in statuses if s == STATUS_COMPLETE)
    errors = sum(1 for s in statuses if s == STATUS_ERROR)
    incomplete = len(rows) - completed - errors

    mfes = [
        v for v in (
            (row.get("outcome") or {}).get("max_favorable_excursion")
            for row in rows
        )
        if v is not None
    ]
    maes = [
        v for v in (
            (row.get("outcome") or {}).get("max_adverse_excursion")
            for row in rows
        )
        if v is not None
    ]

    category = identity.get("disagreement_category")
    control_verdict = identity.get("control_verdict")
    candidate_verdict = identity.get("candidate_verdict")
    # Verdict-derived classification (Phase 9D): identical to the historical
    # label-based check for sma150 categories (every ACTION_DIVERGENT /
    # POLICY_STATE label is itself derived from the frozen verdicts) and
    # correct for any declared experiment's neutral labels.
    action_divergent = (
        category in ACTION_DIVERGENT_CATEGORIES
        or derive_enter_arm(control_verdict, candidate_verdict) is not None
    )

    result: Dict[str, Any] = {
        **identity,
        "metrics_contract_version": METRICS_CONTRACT_VERSION,
        "pair_count": len(rows),
        "completed_count": completed,
        "incomplete_count": incomplete,
        "error_count": errors,
        "mean_mfe": _mean_or_none(mfes),
        "median_mfe": _median_or_none(mfes),
        "mean_mae": _mean_or_none(maes),
        "median_mae": _median_or_none(maes),
        "by_window": [_horizon_stats(rows, w) for w in HOLDING_WINDOWS],
    }

    if action_divergent:
        result["action_resolvable"] = True
        result["enter_arm"] = derive_enter_arm(
            control_verdict, candidate_verdict
        )
        result["resolution_by_window"] = [
            _resolution_for_window(rows, w) for w in HOLDING_WINDOWS
        ]
    elif (
        category in POLICY_STATE_CATEGORIES
        or (
            control_verdict is not None
            and candidate_verdict is not None
            and control_verdict != candidate_verdict
        )
    ):
        # Neither arm acted: price movement is context only; no arm winner
        # may be derived from a WATCH-vs-AVOID state disagreement.
        result["action_resolvable"] = False
        result["classification"] = "policy_state_disagreement"
    else:
        # Agreement categories: market outcomes only.
        result["action_resolvable"] = False
        result["classification"] = "agreement"

    return result


def aggregate_pair_outcome_metrics(
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Group joined outcome rows by the FULL mandatory identity and compute
    neutral grouped evidence for each group. Rows are never pooled across
    any identity field. Deterministic group ordering (identity key)."""
    groups: Dict[Tuple, Tuple[Dict[str, Any], List[Dict[str, Any]]]] = {}
    for row in rows:
        identity = grouping_identity(row)
        key = tuple(identity[f] for f in GROUPING_IDENTITY_FIELDS)
        if key not in groups:
            groups[key] = (identity, [])
        groups[key][1].append(row)

    return [
        _group_metrics(identity, group_rows)
        for key, (identity, group_rows) in sorted(
            groups.items(), key=lambda item: tuple(str(k) for k in item[0])
        )
    ]
