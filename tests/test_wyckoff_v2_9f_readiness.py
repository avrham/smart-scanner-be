"""Phase 9F5: advisory rollout-readiness policy."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from app.workers.shadow.quality_audit import build_quality_audit
from app.workers.shadow.rollout_readiness import (
    ADVISORY_STATUSES,
    DEFAULT_THRESHOLDS,
    READINESS_POLICY_VERSION,
    ReadinessOverrideError,
    STATUS_CONTINUE_SHADOW,
    STATUS_ELIGIBLE,
    STATUS_NOT_READY,
    STATUS_REVIEW_REQUIRED,
    evaluate_rollout_readiness,
    resolve_thresholds,
)
from app.workers.strategies.wyckoff_v2.constants import (
    default_config as v2_default_config,
)

from test_wyckoff_v2_9f_cohorts import evidence_record, trigger_record
from test_wyckoff_v2_9f_outcome_evidence import outcome_row


LOW_BOUND_OVERRIDES = {
    "min_evaluated": 25,
    "min_unique_symbols": 10,
    "min_unique_sessions": 2,
    "min_trigger_confirmed": 5,
    "min_matured_outcomes": 20,
    "min_outcome_coverage": 0.5,
    "min_performance_sample": 10,
    "min_divergent_resolved": 5,
    "target_horizon": "1D",
}


def _volume_records(n: int = 25) -> List[Dict[str, Any]]:
    records = []
    for i in range(n):
        records.append(evidence_record(
            symbol=f"S{i:03d}",
            snapshot="2026-07-16" if i % 2 == 0 else "2026-07-17",
            trigger=(
                trigger_record("confirmed", price=50.0) if i < 6
                else trigger_record("missing")
            ),
            eligible=(i < 6),
            has_outcome=(i < 20),
            outcome_status="complete" if i < 20 else None,
            campaign_ids=["camp-1"],
        ))
    return records


def _favorable_outcome_rows(n: int = 10) -> List[Dict[str, Any]]:
    return [
        outcome_row(
            candidate_verdict="ENTER", control_verdict="AVOID",
            ret_1d=2.0, spy_rel_1d=1.0, qqq_rel_1d=0.5,
            status="complete",
        )
        for _ in range(n)
    ]


def _evaluate(records, rows, overrides=LOW_BOUND_OVERRIDES):
    audit = build_quality_audit(records, outcome_rows=rows)
    return evaluate_rollout_readiness(
        records,
        outcome_rows=rows,
        quality_audit=audit,
        thresholds=resolve_thresholds(overrides),
    )


class TestThresholdResolution:
    def test_defaults_are_versioned_and_conservative(self):
        thresholds = resolve_thresholds()
        assert thresholds == DEFAULT_THRESHOLDS
        assert thresholds["min_evaluated"] == 200
        assert thresholds["min_outcome_coverage"] == 0.80

    def test_unknown_override_rejects(self):
        with pytest.raises(ReadinessOverrideError):
            resolve_thresholds({"min_profit": 1})
        with pytest.raises(ReadinessOverrideError):
            resolve_thresholds({"require_single_strategy_version": False})

    def test_out_of_bounds_override_rejects(self):
        with pytest.raises(ReadinessOverrideError):
            resolve_thresholds({"min_evaluated": 1})
        with pytest.raises(ReadinessOverrideError):
            resolve_thresholds({"max_provider_failure_rate": 0.9})
        with pytest.raises(ReadinessOverrideError):
            resolve_thresholds({"min_evaluated": "many"})
        with pytest.raises(ReadinessOverrideError):
            resolve_thresholds({"target_horizon": "40D"})

    def test_bounded_override_applies(self):
        thresholds = resolve_thresholds({"min_evaluated": 25})
        assert thresholds["min_evaluated"] == 25
        assert thresholds["min_unique_symbols"] == 50   # untouched default


class TestAdvisoryStatuses:
    def test_vocabulary_never_contains_enabled(self):
        assert "enabled" not in ADVISORY_STATUSES
        assert set(ADVISORY_STATUSES) == {
            STATUS_NOT_READY, STATUS_CONTINUE_SHADOW,
            STATUS_REVIEW_REQUIRED, STATUS_ELIGIBLE,
        }

    def test_empty_evidence_is_not_ready(self):
        result = _evaluate([], [])
        assert result["policy_version"] == READINESS_POLICY_VERSION
        assert result["advisory_status"] == STATUS_NOT_READY
        assert "evidence_below_hard_floor" in result["blocking_reasons"]

    def test_thin_but_healthy_evidence_continues_shadow(self):
        records = _volume_records(10)   # above 25*0.25 floor, below minimums
        result = _evaluate(records, [])
        assert result["advisory_status"] == STATUS_CONTINUE_SHADOW
        assert "min_evaluated" in result["failed_conditions"]
        assert result["blocking_reasons"] == []

    def test_blocking_quality_issue_forces_not_ready(self):
        records = _volume_records(25)
        # One confirmed trigger with NO price is a blocking honesty issue.
        records[0]["four_hour_trigger"] = trigger_record(
            "confirmed", price=None
        )
        result = _evaluate(records, _favorable_outcome_rows())
        assert result["advisory_status"] == STATUS_NOT_READY
        assert "blocking_quality_issues_present" in result["blocking_reasons"]

    def test_performance_cannot_override_missing_coverage(self):
        # Outstanding returns but only 12 records: coverage gates fail and
        # the status stays continue_shadow regardless of performance.
        records = _volume_records(12)
        result = _evaluate(records, _favorable_outcome_rows(50))
        assert result["advisory_status"] == STATUS_CONTINUE_SHADOW
        assert "min_evaluated" in result["failed_conditions"]

    def test_mixed_versions_force_review(self):
        records = _volume_records(25)
        records[3]["strategy_version"] = "wyckoff_mtf.v2-exp"
        result = _evaluate(records, _favorable_outcome_rows())
        assert result["advisory_status"] == STATUS_REVIEW_REQUIRED
        assert "require_single_strategy_version" in result[
            "review_conditions"
        ]
        assert "require_single_strategy_version_violated" in result[
            "warnings"
        ]

    def test_unfavorable_performance_forces_review_not_block(self):
        records = _volume_records(25)
        rows = [
            outcome_row(
                candidate_verdict="ENTER", control_verdict="AVOID",
                ret_1d=-2.0, spy_rel_1d=-1.0, status="complete",
            )
            for _ in range(10)
        ]
        result = _evaluate(records, rows)
        assert result["advisory_status"] == STATUS_REVIEW_REQUIRED
        assert "spy_relative_median_non_negative" in result[
            "review_conditions"
        ]
        assert "candidate_favorable_rate_at_least_half" in result[
            "review_conditions"
        ]

    def test_full_evidence_is_eligible(self):
        result = _evaluate(_volume_records(25), _favorable_outcome_rows())
        assert result["advisory_status"] == STATUS_ELIGIBLE
        assert result["failed_conditions"] == []
        assert result["blocking_reasons"] == []
        assert result["review_conditions"] == []
        assert "min_trigger_confirmed" in result["passed_conditions"]

    def test_deterministic(self):
        records = _volume_records(25)
        rows = _favorable_outcome_rows()
        assert _evaluate(records, rows) == _evaluate(
            list(records), list(rows)
        )


class TestAdvisorySafety:
    def test_response_carries_full_transparency(self):
        result = _evaluate(_volume_records(25), _favorable_outcome_rows())
        assert result["thresholds"]["min_evaluated"] == 25
        assert result["observed"]["evaluated_count"] == 25
        assert result["observed"]["trigger_confirmed_count"] == 6
        assert result["observed"]["outcome_coverage"] == pytest.approx(0.8)
        assert result["data_range"] == {
            "first_session": "2026-07-16", "last_session": "2026-07-17",
        }
        assert result["rollout_mutation_performed"] is False
        assert result["rollout_defaults"] == {
            "patterns.is_enabled": False,
            "allow_enter": False,
            "enable_4h_trigger": False,
            "min_price": 5.0,
        }

    def test_policy_never_mutates_configuration(self):
        before = v2_default_config()
        _evaluate(_volume_records(25), _favorable_outcome_rows())
        after = v2_default_config()
        assert before == after
        assert after["allow_enter"] is False
        assert after["enable_4h_trigger"] is False

    def test_policy_module_has_no_write_paths(self):
        import inspect

        from app.workers.shadow import rollout_readiness

        source = inspect.getsource(rollout_readiness)
        for forbidden in ("INSERT", "UPDATE ", "save_signal",
                          "persist_", "upsert", "execute("):
            assert forbidden not in source, forbidden
