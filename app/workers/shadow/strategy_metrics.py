"""PURE strategy-filtered shadow evaluation metrics (Phase 9D5).

Contract version: strategy_shadow_metrics.v1.

Aggregates the bounded per-evaluation records produced by
persistence.fetch_strategy_shadow_evaluations for ONE strategy code into
neutral grouped evidence. This module deliberately measures DECISION STATES,
not returns — market-path return statistics stay on the existing
shadow_pair_resolution_metrics.v1 contract (outcomes.metrics), which is never
duplicated here.

Hard rules:
  * rows are never pooled across (experiment_code, experiment_version,
    strategy_version, decision_policy_version, config_hash);
  * every state stays separate and explicit — insufficient data, rejected
    setup, rollout-blocked, valid non-ENTER decision, ENTER candidate blocked
    by rollout, missing outcome row — none is ever collapsed into another,
    and a missing outcome is never represented as a zero return;
  * rollout-blocked and pre-rollout-ENTER states are read from the frozen
    policy record persisted with the evaluation
    (enter_eligible_without_rollout_gate / allow_enter) — never re-derived
    from live configuration;
  * a strategy without a persisted policy/readiness record (e.g. the sma150
    baseline arm) reports those states as unknown counts, never as zeros
    pretending to be measurements;
  * no better/winner/improvement/regression labels and no parameter
    recommendations — grouped neutral evidence only.

All functions are pure (no I/O).
"""

from statistics import mean, median
from typing import Any, Dict, List, Optional, Tuple


STRATEGY_METRICS_CONTRACT_VERSION = "strategy_shadow_metrics.v1"

VALID_DECISIONS = ("ENTER", "WATCH", "AVOID")

GROUP_IDENTITY_FIELDS = (
    "experiment_code",
    "experiment_version",
    "strategy_code",
    "strategy_version",
    "decision_policy_version",
    "config_hash",
)


def _policy(record: Dict[str, Any]) -> Dict[str, Any]:
    policy = record.get("policy")
    return policy if isinstance(policy, dict) else {}


def is_rollout_blocked(record: Dict[str, Any]) -> Optional[bool]:
    """True when the frozen policy proves an otherwise-valid ENTER setup was
    blocked only by the rollout gate; None when the strategy persists no
    policy record (unknown, never assumed False)."""
    policy = _policy(record)
    eligible = policy.get("enter_eligible_without_rollout_gate")
    allow_enter = policy.get("allow_enter")
    if not isinstance(eligible, bool) or not isinstance(allow_enter, bool):
        return None
    return eligible and not allow_enter


def is_pre_rollout_enter_candidate(record: Dict[str, Any]) -> Optional[bool]:
    """True when the frozen policy proves the setup passed every gate except
    (possibly) the rollout gate; None when no policy record exists."""
    policy = _policy(record)
    eligible = policy.get("enter_eligible_without_rollout_gate")
    if not isinstance(eligible, bool):
        return None
    return eligible


def _mean_or_none(values: List[float]) -> Optional[float]:
    return mean(values) if values else None


def _median_or_none(values: List[float]) -> Optional[float]:
    return median(values) if values else None


def _distribution(values: List[Optional[str]]) -> Dict[str, int]:
    """Deterministic sorted count map over non-None string values."""
    counts: Dict[str, int] = {}
    for value in values:
        if value is None:
            continue
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _group_identity(record: Dict[str, Any]) -> Dict[str, Any]:
    return {f: record.get(f) for f in GROUP_IDENTITY_FIELDS}


def _group_metrics(
    identity: Dict[str, Any], records: List[Dict[str, Any]]
) -> Dict[str, Any]:
    verdicts = [r.get("verdict") for r in records]
    decision_counts = _distribution(verdicts)
    valid_decisions = sum(
        1 for v in verdicts if v in VALID_DECISIONS
    )

    readiness_statuses = [r.get("readiness_status") for r in records]
    readiness_known = [s for s in readiness_statuses if s is not None]
    insufficient_data = sum(1 for s in readiness_known if s != "ready")

    rollout_flags = [is_rollout_blocked(r) for r in records]
    rollout_blocked = sum(1 for f in rollout_flags if f is True)
    rollout_state_unknown = sum(1 for f in rollout_flags if f is None)

    pre_rollout_flags = [is_pre_rollout_enter_candidate(r) for r in records]
    pre_rollout_enter = sum(1 for f in pre_rollout_flags if f is True)

    rejected_setups = sum(
        1 for r in records
        if r.get("verdict") == "AVOID" and r.get("rejection_reason")
    )
    valid_non_enter = sum(
        1 for r in records
        if r.get("verdict") in ("WATCH", "AVOID")
    )

    with_outcome = sum(1 for r in records if r.get("has_outcome"))
    outcome_statuses = _distribution(
        [r.get("outcome_status") for r in records if r.get("has_outcome")]
    )

    scores = [
        float(r["score"]) for r in records
        if isinstance(r.get("score"), (int, float))
        and not isinstance(r.get("score"), bool)
    ]

    evidence_categories: List[Optional[str]] = []
    for r in records:
        cats = r.get("evidence_categories")
        if isinstance(cats, list):
            evidence_categories.extend(
                c for c in cats if isinstance(c, str)
            )

    waiting_reasons: List[Optional[str]] = []
    for r in records:
        reasons = _policy(r).get("waiting_reasons")
        if isinstance(reasons, list):
            waiting_reasons.extend(x for x in reasons if isinstance(x, str))

    evaluated = len(records)
    return {
        **identity,
        "metrics_contract_version": STRATEGY_METRICS_CONTRACT_VERSION,
        "evaluated_count": evaluated,
        "decision_counts": decision_counts,
        "valid_decision_count": valid_decisions,
        "valid_non_enter_decision_count": valid_non_enter,
        # Data-sufficiency states (unknown stays unknown, never zero).
        "insufficient_data_count": insufficient_data,
        "readiness_known_count": len(readiness_known),
        "readiness_unknown_count": evaluated - len(readiness_known),
        "readiness_status_distribution": _distribution(readiness_statuses),
        # Setup rejection states.
        "rejected_setup_count": rejected_setups,
        "failure_reason_distribution": _distribution(
            [r.get("rejection_reason") for r in records]
        ),
        # Rollout states (frozen policy record; unknown stays unknown).
        "rollout_blocked_count": rollout_blocked,
        "pre_rollout_enter_candidate_count": pre_rollout_enter,
        "rollout_state_unknown_count": rollout_state_unknown,
        "waiting_reason_distribution": _distribution(waiting_reasons),
        # Outcome coverage (a MISSING outcome row is counted, never zeroed).
        "outcome_eligible_count": evaluated,
        "with_outcome_count": with_outcome,
        "missing_outcome_count": evaluated - with_outcome,
        "outcome_coverage": (with_outcome / evaluated) if evaluated else None,
        "outcome_status_distribution": outcome_statuses,
        # Neutral score evidence (sample count always adjacent).
        "score_sample_count": len(scores),
        "mean_score": _mean_or_none(scores),
        "median_score": _median_or_none(scores),
        # Evidence-category distribution (evidence.v1 item categories).
        "evidence_category_distribution": _distribution(evidence_categories),
    }


def aggregate_strategy_shadow_metrics(
    records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Group per-evaluation records by the full strategy identity and compute
    neutral grouped evidence. Deterministic group ordering (identity key)."""
    groups: Dict[Tuple, Tuple[Dict[str, Any], List[Dict[str, Any]]]] = {}
    for record in records:
        identity = _group_identity(record)
        key = tuple(identity[f] for f in GROUP_IDENTITY_FIELDS)
        if key not in groups:
            groups[key] = (identity, [])
        groups[key][1].append(record)

    return {
        "metrics_contract_version": STRATEGY_METRICS_CONTRACT_VERSION,
        "evaluated_count": len(records),
        "groups": [
            _group_metrics(identity, group_records)
            for key, (identity, group_records) in sorted(
                groups.items(), key=lambda item: tuple(str(k) for k in item[0])
            )
        ],
    }
