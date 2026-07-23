"""Phase 9D5: strategy shadow decision metrics + generalized comparison
metrics (existing shadow_pair_resolution_metrics.v1 definitions preserved)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from app.workers.shadow.outcomes.metrics import (
    aggregate_pair_outcome_metrics,
    derive_enter_arm,
)
from app.workers.shadow.strategy_metrics import (
    STRATEGY_METRICS_CONTRACT_VERSION,
    aggregate_strategy_shadow_metrics,
    is_pre_rollout_enter_candidate,
    is_rollout_blocked,
)


def _record(
    *,
    verdict: str = "WATCH",
    strategy_version: str = "wyckoff_mtf.v2",
    decision_policy_version: str = "wyckoff_mtf.policy.v1",
    config_hash: str = "cfg-1",
    readiness_status: Optional[str] = "ready",
    rejection_reason: Optional[str] = None,
    eligible: Optional[bool] = False,
    allow_enter: Optional[bool] = False,
    waiting: Optional[List[str]] = None,
    has_outcome: bool = False,
    outcome_status: Optional[str] = None,
    score: Optional[float] = None,
    evidence_categories: Optional[List[str]] = None,
    experiment_code: str = "wyckoff_v2_vs_baseline",
) -> Dict[str, Any]:
    policy: Optional[Dict[str, Any]] = None
    if eligible is not None and allow_enter is not None:
        policy = {
            "enter_eligible_without_rollout_gate": eligible,
            "allow_enter": allow_enter,
            "waiting_reasons": waiting or [],
        }
    return {
        "strategy_code": "wyckoff_mtf_v2",
        "strategy_version": strategy_version,
        "decision_policy_version": decision_policy_version,
        "config_hash": config_hash,
        "experiment_code": experiment_code,
        "experiment_version": "wyckoff_v2_shadow.v1",
        "verdict": verdict,
        "score": score,
        "rejection_reason": rejection_reason,
        "policy": policy,
        "readiness_status": readiness_status,
        "evidence_categories": evidence_categories or [],
        "has_outcome": has_outcome,
        "outcome_status": outcome_status,
    }


class TestRolloutStateHelpers:
    def test_blocked_requires_frozen_policy_proof(self):
        assert is_rollout_blocked(
            _record(eligible=True, allow_enter=False)
        ) is True
        assert is_rollout_blocked(
            _record(eligible=False, allow_enter=False)
        ) is False
        assert is_rollout_blocked(
            _record(eligible=True, allow_enter=True)
        ) is False

    def test_missing_policy_is_unknown_never_false_positive(self):
        record = _record()
        record["policy"] = None
        assert is_rollout_blocked(record) is None
        assert is_pre_rollout_enter_candidate(record) is None


class TestStrategyShadowMetrics:
    def test_empty_dataset_is_safe(self):
        metrics = aggregate_strategy_shadow_metrics([])
        assert metrics["metrics_contract_version"] == (
            STRATEGY_METRICS_CONTRACT_VERSION
        )
        assert metrics["evaluated_count"] == 0
        assert metrics["groups"] == []

    def test_states_stay_separate(self):
        records = [
            # Valid WATCH setup, blocked only by rollout.
            _record(
                verdict="WATCH", eligible=True, allow_enter=False,
                waiting=["enter_disabled_shadow_only"],
                score=0.7,
            ),
            # Insufficient history (explicit readiness, not zero-scored).
            _record(
                verdict="AVOID", readiness_status="insufficient_history",
                rejection_reason="insufficient_history",
            ),
            # Rejected setup on ready data.
            _record(
                verdict="AVOID", rejection_reason="no_valid_selected_range",
                evidence_categories=["range", "readiness"],
            ),
            # Valid WATCH without rollout eligibility, with outcome.
            _record(
                verdict="WATCH", eligible=False,
                has_outcome=True, outcome_status="partial", score=0.4,
            ),
        ]
        metrics = aggregate_strategy_shadow_metrics(records)
        assert metrics["evaluated_count"] == 4
        assert len(metrics["groups"]) == 1
        group = metrics["groups"][0]

        assert group["decision_counts"] == {"AVOID": 2, "WATCH": 2}
        assert group["valid_decision_count"] == 4
        assert group["insufficient_data_count"] == 1
        assert group["rejected_setup_count"] == 2
        assert group["rollout_blocked_count"] == 1
        assert group["pre_rollout_enter_candidate_count"] == 1
        assert group["waiting_reason_distribution"] == {
            "enter_disabled_shadow_only": 1
        }
        assert group["failure_reason_distribution"] == {
            "insufficient_history": 1,
            "no_valid_selected_range": 1,
        }
        # Outcome coverage: missing outcomes stay MISSING, never zeros.
        assert group["outcome_eligible_count"] == 4
        assert group["with_outcome_count"] == 1
        assert group["missing_outcome_count"] == 3
        assert group["outcome_coverage"] == pytest.approx(0.25)
        assert group["outcome_status_distribution"] == {"partial": 1}
        # Scores: sample count adjacent, missing scores excluded not zeroed.
        assert group["score_sample_count"] == 2
        assert group["mean_score"] == pytest.approx(0.55)
        assert group["evidence_category_distribution"] == {
            "range": 1, "readiness": 1,
        }

    def test_unknown_readiness_and_rollout_stay_unknown(self):
        record = _record(readiness_status=None)
        record["policy"] = None
        metrics = aggregate_strategy_shadow_metrics([record])
        group = metrics["groups"][0]
        assert group["insufficient_data_count"] == 0
        assert group["readiness_unknown_count"] == 1
        assert group["rollout_state_unknown_count"] == 1
        assert group["rollout_blocked_count"] == 0

    def test_grouping_by_version_and_policy(self):
        records = [
            _record(strategy_version="wyckoff_mtf.v2"),
            _record(strategy_version="wyckoff_mtf.v2"),
            _record(strategy_version="wyckoff_mtf.v2-exp"),
            _record(decision_policy_version="wyckoff_mtf.policy.v2-exp"),
            _record(config_hash="cfg-2"),
        ]
        metrics = aggregate_strategy_shadow_metrics(records)
        keys = {
            (
                g["strategy_version"],
                g["decision_policy_version"],
                g["config_hash"],
            )
            for g in metrics["groups"]
        }
        assert len(metrics["groups"]) == 4
        assert ("wyckoff_mtf.v2", "wyckoff_mtf.policy.v1", "cfg-1") in keys
        counts = {
            (
                g["strategy_version"],
                g["decision_policy_version"],
                g["config_hash"],
            ): g["evaluated_count"]
            for g in metrics["groups"]
        }
        assert counts[
            ("wyckoff_mtf.v2", "wyckoff_mtf.policy.v1", "cfg-1")
        ] == 2


def _outcome_row(
    *,
    control_verdict: str,
    candidate_verdict: str,
    category: str,
    ret_1d: Optional[float] = None,
    status: str = "partial",
    benchmark: Optional[Dict[str, Dict[str, float]]] = None,
) -> Dict[str, Any]:
    return {
        "pair": {
            "experiment_code": "wyckoff_v2_vs_baseline",
            "experiment_version": "wyckoff_v2_shadow.v1",
        },
        "control": {
            "arm_code": "control_baseline",
            "strategy_code": "sma150_bounce",
            "strategy_version": "sma150.v2",
            "decision_policy_version": "strategy_decision.v1",
            "config_hash": "c",
            "verdict": control_verdict,
        },
        "candidate": {
            "arm_code": "candidate_wyckoff_v2",
            "strategy_code": "wyckoff_mtf_v2",
            "strategy_version": "wyckoff_mtf.v2",
            "decision_policy_version": "wyckoff_mtf.policy.v1",
            "config_hash": "x",
            "verdict": candidate_verdict,
        },
        "disagreement_category": category,
        "outcome": {
            "calculation_version": "outcome.v1",
            "outcome_coverage_version": "shadow_pair_outcomes.v1",
            "forward_frame_version": "shadow_forward_bars.v1",
            "forward_provider": "massive",
            "outcome_status": status,
            "returns": {
                "1D": ret_1d, "3D": None, "5D": None,
                "10D": None, "20D": None,
            },
            "benchmark_returns": benchmark or {},
            "max_favorable_excursion": None,
            "max_adverse_excursion": None,
        },
    }


class TestGeneralizedComparisonMetrics:
    def test_generic_action_divergent_category_resolves(self):
        rows = [
            _outcome_row(
                control_verdict="AVOID", candidate_verdict="ENTER",
                category="control_avoid_candidate_enter",
                ret_1d=2.0,
            ),
        ]
        groups = aggregate_pair_outcome_metrics(rows)
        assert len(groups) == 1
        group = groups[0]
        assert group["action_resolvable"] is True
        assert group["enter_arm"] == "candidate"
        assert group["disagreement_category"] == (
            "control_avoid_candidate_enter"
        )

    def test_generic_policy_state_disagreement(self):
        rows = [
            _outcome_row(
                control_verdict="WATCH", candidate_verdict="AVOID",
                category="control_watch_candidate_avoid",
            ),
        ]
        group = aggregate_pair_outcome_metrics(rows)[0]
        assert group["action_resolvable"] is False
        assert group["classification"] == "policy_state_disagreement"

    def test_agreement_stays_agreement(self):
        rows = [
            _outcome_row(
                control_verdict="AVOID", candidate_verdict="AVOID",
                category="same_avoid",
            ),
        ]
        group = aggregate_pair_outcome_metrics(rows)[0]
        assert group["action_resolvable"] is False
        assert group["classification"] == "agreement"

    def test_legacy_labels_unchanged(self):
        rows = [
            _outcome_row(
                control_verdict="ENTER", candidate_verdict="AVOID",
                category="v2_enter_v3_avoid", ret_1d=1.5,
            ),
        ]
        group = aggregate_pair_outcome_metrics(rows)[0]
        assert group["action_resolvable"] is True
        assert group["enter_arm"] == "control"

    def test_missing_outcome_returns_never_zeroed(self):
        rows = [
            _outcome_row(
                control_verdict="AVOID", candidate_verdict="ENTER",
                category="control_avoid_candidate_enter",
                ret_1d=None, status="pending_forward_bars",
            ),
        ]
        group = aggregate_pair_outcome_metrics(rows)[0]
        by_window = {w["window"]: w for w in group["by_window"]}
        assert by_window["1D"]["sample_count"] == 0
        assert by_window["1D"]["mean_return"] is None
        assert by_window["1D"]["positive_return_rate"] is None
        resolution = {
            r["window"]: r for r in group["resolution_by_window"]
        }
        assert resolution["1D"]["incomplete_count"] == 1
        assert resolution["1D"]["classified_count"] == 0

    def test_baseline_relative_returns_use_existing_definition(self):
        rows = [
            _outcome_row(
                control_verdict="AVOID", candidate_verdict="ENTER",
                category="control_avoid_candidate_enter",
                ret_1d=2.0,
                benchmark={"SPY": {"1D": 0.5}, "QQQ": {"1D": 1.0}},
            ),
        ]
        group = aggregate_pair_outcome_metrics(rows)[0]
        by_window = {w["window"]: w for w in group["by_window"]}
        assert by_window["1D"]["mean_spy_relative_return"] == pytest.approx(1.5)
        assert by_window["1D"]["mean_qqq_relative_return"] == pytest.approx(1.0)

    def test_rows_never_pooled_across_experiments(self):
        wyckoff = _outcome_row(
            control_verdict="AVOID", candidate_verdict="WATCH",
            category="control_avoid_candidate_watch",
        )
        legacy = _outcome_row(
            control_verdict="AVOID", candidate_verdict="WATCH",
            category="v2_avoid_v3_watch",
        )
        legacy["pair"]["experiment_code"] = "sma150_v2_vs_v3"
        legacy["pair"]["experiment_version"] = "sma150_shadow.v1"
        legacy["control"]["arm_code"] = "control_v2"
        legacy["candidate"]["arm_code"] = "candidate_v3"
        legacy["candidate"]["strategy_code"] = "sma150_bounce_v3"
        groups = aggregate_pair_outcome_metrics([wyckoff, legacy])
        assert len(groups) == 2

    def test_derive_enter_arm_is_verdict_based(self):
        assert derive_enter_arm("ENTER", "WATCH") == "control"
        assert derive_enter_arm("AVOID", "ENTER") == "candidate"
        assert derive_enter_arm("ENTER", "ENTER") is None
        assert derive_enter_arm("WATCH", "AVOID") is None
