"""Advisory rollout-readiness policy — wyckoff_v2_rollout_readiness.v1 (9F5).

STRICTLY ADVISORY AND READ-ONLY. This module can only ever RETURN one of
four advisory states; it has no write path, imports no persistence writer,
and never touches patterns.is_enabled, allow_enter or enable_4h_trigger.
The advisory vocabulary deliberately contains no 'enabled' state.

Design (approach 3 from the milestone): conservative versioned DEFAULT
thresholds, plus BOUNDED typed operator overrides. Every threshold, every
observed value and every pass/fail condition is returned verbatim — nothing
is hidden inside the decision.

Deterministic status precedence (evaluated in order; first match wins):

  1. not_ready          — any BLOCKING data-quality issue, or the evidence
                          volume is below the hard floor (a fraction of the
                          minimums), or there is no evidence at all;
  2. continue_shadow    — any volume/coverage/failure-rate condition unmet:
                          the pipeline is healthy but the cohort is simply
                          not big or mature enough yet;
  3. review_required    — volume and coverage are sufficient but the
                          evidence needs human judgment: consistency
                          warnings (mixed versions/config), insufficient
                          performance samples, or non-positive
                          benchmark/control-relative evidence. FINANCIAL
                          PERFORMANCE CAN NEVER SKIP COVERAGE: performance
                          conditions are only consulted after every
                          coverage condition passed;
  4. eligible_for_controlled_read_only_rollout — everything above passed.

All functions are pure (no I/O).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.workers.shadow.evidence_cohorts import build_cohorts
from app.workers.shadow.outcome_evidence import build_outcome_evidence


READINESS_POLICY_VERSION = "wyckoff_v2_rollout_readiness.v1"

STATUS_NOT_READY = "not_ready"
STATUS_CONTINUE_SHADOW = "continue_shadow"
STATUS_REVIEW_REQUIRED = "review_required"
STATUS_ELIGIBLE = "eligible_for_controlled_read_only_rollout"
ADVISORY_STATUSES = (
    STATUS_NOT_READY,
    STATUS_CONTINUE_SHADOW,
    STATUS_REVIEW_REQUIRED,
    STATUS_ELIGIBLE,
)

# Conservative versioned defaults. Changing ANY value requires a new policy
# version. Every threshold is echoed in the response.
DEFAULT_THRESHOLDS: Dict[str, Any] = {
    "min_evaluated": 200,
    "min_unique_symbols": 50,
    "min_unique_sessions": 10,
    "min_trigger_confirmed": 20,
    "min_matured_outcomes": 100,
    "min_outcome_coverage": 0.80,
    "max_provider_failure_rate": 0.05,
    "max_frame_rejection_rate": 0.05,
    "max_readiness_unknown_rate": 0.05,
    "min_performance_sample": 30,
    "min_divergent_resolved": 20,
    "target_horizon": "20D",
    "require_single_strategy_version": True,
    "require_single_policy_version": True,
    "require_single_config_hash": True,
}

# Bounded operator override ranges — an override outside these bounds is a
# typed error, never silently clamped.
_OVERRIDE_BOUNDS: Dict[str, Tuple[float, float]] = {
    "min_evaluated": (25, 10_000),
    "min_unique_symbols": (10, 5_000),
    "min_unique_sessions": (2, 250),
    "min_trigger_confirmed": (5, 1_000),
    "min_matured_outcomes": (20, 10_000),
    "min_outcome_coverage": (0.5, 1.0),
    "max_provider_failure_rate": (0.0, 0.25),
    "max_frame_rejection_rate": (0.0, 0.25),
    "max_readiness_unknown_rate": (0.0, 0.25),
    "min_performance_sample": (10, 5_000),
    "min_divergent_resolved": (5, 5_000),
}

_VALID_HORIZONS = ("1D", "3D", "5D", "10D", "20D")

# Hard floor: below this fraction of the volume minimums the evidence is
# not merely thin — it cannot support any discussion yet.
_HARD_FLOOR_FRACTION = 0.25


class ReadinessOverrideError(ValueError):
    """Invalid operator threshold override (unknown key / out of bounds)."""


def resolve_thresholds(
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Defaults overlaid with BOUNDED typed operator overrides."""
    thresholds = dict(DEFAULT_THRESHOLDS)
    for key, value in (overrides or {}).items():
        if value is None:
            continue
        if key == "target_horizon":
            if value not in _VALID_HORIZONS:
                raise ReadinessOverrideError(
                    f"target_horizon must be one of {list(_VALID_HORIZONS)}"
                )
            thresholds[key] = value
            continue
        if key not in _OVERRIDE_BOUNDS:
            raise ReadinessOverrideError(
                f"unknown or non-overridable threshold {key!r}"
            )
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            raise ReadinessOverrideError(f"{key} must be numeric")
        low, high = _OVERRIDE_BOUNDS[key]
        if numeric < low or numeric > high:
            raise ReadinessOverrideError(
                f"{key} must be between {low} and {high}"
            )
        default = DEFAULT_THRESHOLDS[key]
        thresholds[key] = int(numeric) if isinstance(default, int) else numeric
    return thresholds


def _rate(numerator: int, denominator: int) -> Optional[float]:
    return (numerator / denominator) if denominator else None


def _observe(
    records: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
    quality_audit: Dict[str, Any],
    thresholds: Dict[str, Any],
) -> Dict[str, Any]:
    """Every observed value the policy consults, computed once, verbatim."""
    cohorts = build_cohorts(records)["cohorts"]
    evaluated = cohorts["evaluated"]
    n = evaluated["record_count"]

    horizon = thresholds["target_horizon"]
    outcome_evidence = build_outcome_evidence(rows=outcome_rows)
    perf_sample = 0
    spy_rel_median: Optional[float] = None
    qqq_rel_median: Optional[float] = None
    divergent_resolved = 0
    candidate_favorable_rate: Optional[float] = None
    version_groups = len(outcome_evidence["groups"])
    if version_groups == 1:
        group = outcome_evidence["groups"][0]
        window = {w["window"]: w for w in group["by_window"]}[horizon]
        perf_sample = window["candidate"]["return_count"]
        spy_rel_median = window["benchmarks"]["SPY"]["relative_median_return"]
        qqq_rel_median = window["benchmarks"]["QQQ"]["relative_median_return"]
        divergent_resolved = window["paired_action_divergence"][
            "resolved_count"
        ]
        candidate_favorable_rate = window["paired_action_divergence"][
            "candidate_favorable_rate"
        ]

    return {
        "evaluated_count": n,
        "unique_symbol_count": evaluated["unique_symbol_count"],
        "unique_session_count": evaluated["unique_session_count"],
        "unique_campaign_count": evaluated["unique_campaign_count"],
        "trigger_confirmed_count": cohorts["trigger_confirmed"][
            "record_count"
        ],
        "matured_outcome_count": cohorts["matured_outcome"]["record_count"],
        "outcome_coverage": evaluated["outcome_coverage"],
        "provider_failure_rate": _rate(
            cohorts["provider_failure_4h"]["record_count"], n
        ),
        "frame_rejection_rate": _rate(
            cohorts["frame_rejected_4h"]["record_count"], n
        ),
        "readiness_unknown_rate": _rate(
            cohorts["daily_readiness_unknown"]["record_count"], n
        ),
        "strategy_version_count": len(
            evaluated["strategy_version_distribution"]
        ),
        "policy_version_count": len(
            evaluated["decision_policy_version_distribution"]
        ),
        "config_hash_count": len(evaluated["config_hash_distribution"]),
        "blocking_quality_issues": quality_audit["blocking_count"],
        "quality_warning_count": quality_audit["warning_count"],
        "outcome_group_count": version_groups,
        "target_horizon": horizon,
        "performance_sample_count": perf_sample,
        "spy_relative_median_return": spy_rel_median,
        "qqq_relative_median_return": qqq_rel_median,
        "divergent_resolved_count": divergent_resolved,
        "candidate_favorable_rate": candidate_favorable_rate,
        "first_session": evaluated["first_session"],
        "last_session": evaluated["last_session"],
    }


def evaluate_rollout_readiness(
    records: List[Dict[str, Any]],
    *,
    outcome_rows: Optional[List[Dict[str, Any]]] = None,
    quality_audit: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
    filters: Optional[Dict[str, Any]] = None,
    evidence_timestamp: Optional[str] = None,
) -> Dict[str, Any]:
    """Evaluate the ADVISORY readiness policy over frozen evidence.

    Returns the full transparent decision record. NEVER mutates anything
    and never returns an 'enabled' state.
    """
    resolved = thresholds or resolve_thresholds()
    observed = _observe(
        records, outcome_rows or [], quality_audit, resolved
    )

    passed: List[str] = []
    failed: List[str] = []
    blocking: List[str] = []
    warnings: List[str] = []

    # ---- 1. blocking gates ------------------------------------------------ #
    if observed["blocking_quality_issues"] > 0:
        blocking.append("blocking_quality_issues_present")
    else:
        passed.append("no_blocking_quality_issues")
    hard_floor = max(
        1, int(resolved["min_evaluated"] * _HARD_FLOOR_FRACTION)
    )
    if observed["evaluated_count"] < hard_floor:
        blocking.append("evidence_below_hard_floor")
    else:
        passed.append("evidence_above_hard_floor")

    # ---- 2. volume / coverage gates ---------------------------------------- #
    volume_checks = (
        ("min_evaluated", observed["evaluated_count"]
         >= resolved["min_evaluated"]),
        ("min_unique_symbols", observed["unique_symbol_count"]
         >= resolved["min_unique_symbols"]),
        ("min_unique_sessions", observed["unique_session_count"]
         >= resolved["min_unique_sessions"]),
        ("min_trigger_confirmed", observed["trigger_confirmed_count"]
         >= resolved["min_trigger_confirmed"]),
        ("min_matured_outcomes", observed["matured_outcome_count"]
         >= resolved["min_matured_outcomes"]),
        ("min_outcome_coverage",
         observed["outcome_coverage"] is not None
         and observed["outcome_coverage"] >= resolved["min_outcome_coverage"]),
        ("max_provider_failure_rate",
         (observed["provider_failure_rate"] or 0.0)
         <= resolved["max_provider_failure_rate"]),
        ("max_frame_rejection_rate",
         (observed["frame_rejection_rate"] or 0.0)
         <= resolved["max_frame_rejection_rate"]),
        ("max_readiness_unknown_rate",
         (observed["readiness_unknown_rate"] or 0.0)
         <= resolved["max_readiness_unknown_rate"]),
    )
    for name, ok in volume_checks:
        (passed if ok else failed).append(name)

    # ---- 3. consistency + performance (review conditions) ------------------ #
    review: List[str] = []
    consistency_checks = (
        ("require_single_strategy_version",
         resolved["require_single_strategy_version"],
         observed["strategy_version_count"] <= 1),
        ("require_single_policy_version",
         resolved["require_single_policy_version"],
         observed["policy_version_count"] <= 1),
        ("require_single_config_hash",
         resolved["require_single_config_hash"],
         observed["config_hash_count"] <= 1),
    )
    for name, required, ok in consistency_checks:
        if not required:
            continue
        if ok:
            passed.append(name)
        else:
            review.append(name)
            warnings.append(f"{name}_violated")

    perf_checks = (
        ("min_performance_sample",
         observed["performance_sample_count"]
         >= resolved["min_performance_sample"]),
        ("min_divergent_resolved",
         observed["divergent_resolved_count"]
         >= resolved["min_divergent_resolved"]),
        ("spy_relative_median_non_negative",
         observed["spy_relative_median_return"] is not None
         and observed["spy_relative_median_return"] >= 0.0),
        ("candidate_favorable_rate_at_least_half",
         observed["candidate_favorable_rate"] is not None
         and observed["candidate_favorable_rate"] >= 0.5),
    )
    for name, ok in perf_checks:
        (passed if ok else review).append(name)

    # ---- deterministic precedence ------------------------------------------ #
    if blocking:
        status = STATUS_NOT_READY
    elif failed:
        status = STATUS_CONTINUE_SHADOW
    elif review:
        status = STATUS_REVIEW_REQUIRED
    else:
        status = STATUS_ELIGIBLE

    return {
        "policy_version": READINESS_POLICY_VERSION,
        "advisory_status": status,
        "advisory_status_vocabulary": list(ADVISORY_STATUSES),
        "thresholds": dict(resolved),
        "observed": observed,
        "passed_conditions": sorted(passed),
        "failed_conditions": sorted(failed),
        "review_conditions": sorted(review),
        "blocking_reasons": sorted(blocking),
        "warnings": sorted(warnings),
        "evidence_timestamp": evidence_timestamp,
        "data_range": {
            "first_session": observed["first_session"],
            "last_session": observed["last_session"],
        },
        "filters": filters,
        # Explicit, load-bearing statement: this policy is advisory only.
        "rollout_mutation_performed": False,
        "rollout_defaults": {
            "patterns.is_enabled": False,
            "allow_enter": False,
            "enable_4h_trigger": False,
            "min_price": 5.0,
        },
    }
