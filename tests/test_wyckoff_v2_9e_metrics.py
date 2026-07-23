"""Phase 9E5: trigger + readiness metrics (strategy_shadow_metrics.v2)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from app.workers.shadow.strategy_metrics import (
    STRATEGY_METRICS_CONTRACT_VERSION,
    TRIGGER_CLASS_ABSENT,
    TRIGGER_CLASS_CONFIRMED,
    TRIGGER_CLASS_INSUFFICIENT,
    TRIGGER_CLASS_NOT_EVALUATED,
    TRIGGER_CLASS_WAITING,
    aggregate_strategy_shadow_metrics,
    classify_trigger_state,
)


def _trigger(state: str, *, reasons=(), trigger_price=None) -> Dict[str, Any]:
    return {
        "state": state,
        "reason_codes": list(reasons),
        "trigger_price": trigger_price,
    }


def _record(
    *,
    verdict: str = "WATCH",
    readiness_status: Optional[str] = "ready",
    trigger: Optional[Dict[str, Any]] = None,
    frame_meta: Optional[Dict[str, Any]] = None,
    policy: Optional[Dict[str, Any]] = "default",
    has_outcome: bool = False,
    outcome_status: Optional[str] = None,
    rejection_reason: Optional[str] = None,
    strategy_version: str = "wyckoff_mtf.v2",
    daily_contract: str = "daily_ohlcv_snapshot.v1",
) -> Dict[str, Any]:
    if policy == "default":
        policy = {
            "enter_eligible_without_rollout_gate": False,
            "allow_enter": False,
            "waiting_reasons": [],
            "setup_state": "valid",
        }
    return {
        "strategy_code": "wyckoff_mtf_v2",
        "strategy_version": strategy_version,
        "decision_policy_version": "wyckoff_mtf.policy.v1",
        "config_hash": "cfg",
        "experiment_code": "wyckoff_v2_vs_baseline",
        "experiment_version": "wyckoff_v2_shadow.v2",
        "daily_frame_contract_version": daily_contract,
        "verdict": verdict,
        "score": None,
        "rejection_reason": rejection_reason,
        "policy": policy,
        "readiness_status": readiness_status,
        "four_hour_trigger": trigger,
        "four_hour_frame_meta": frame_meta,
        "evidence_categories": [],
        "has_outcome": has_outcome,
        "outcome_status": outcome_status,
    }


BUILT = {"contract_version": "four_hour_frame.v1", "state": "built",
         "frame_hash": "4h"}


class TestTriggerClassification:
    def test_confirmed_waiting_contradicted(self):
        assert classify_trigger_state(
            _record(trigger=_trigger("confirmed", trigger_price=55.0))
        ) == TRIGGER_CLASS_CONFIRMED
        assert classify_trigger_state(
            _record(trigger=_trigger("missing"))
        ) == TRIGGER_CLASS_WAITING
        assert classify_trigger_state(
            _record(trigger=_trigger("contradicted"))
        ) == "contradicted"

    def test_absent_vs_insufficient_are_separate(self):
        absent = classify_trigger_state(_record(
            trigger=_trigger("unknown", reasons=("four_hour_data_missing",))
        ))
        insufficient = classify_trigger_state(_record(
            trigger=_trigger("unknown", reasons=("insufficient_4h_history",))
        ))
        stale = classify_trigger_state(_record(
            trigger=_trigger("unknown", reasons=("four_hour_trigger_stale",))
        ))
        assert absent == TRIGGER_CLASS_ABSENT
        assert insufficient == TRIGGER_CLASS_INSUFFICIENT
        assert stale == TRIGGER_CLASS_INSUFFICIENT
        assert absent != insufficient

    def test_not_evaluated_when_no_trigger_record(self):
        assert classify_trigger_state(_record(trigger=None)) == (
            TRIGGER_CLASS_NOT_EVALUATED
        )

    def test_disabled_is_its_own_state(self):
        assert classify_trigger_state(_record(
            trigger=_trigger("unknown", reasons=("four_hour_trigger_disabled",))
        )) == "disabled"


class TestTriggerMetrics:
    def test_all_states_stay_separate(self):
        records = [
            # Confirmed real trigger (rollout-blocked WATCH).
            _record(
                trigger=_trigger("confirmed", trigger_price=55.0),
                frame_meta=BUILT,
                policy={
                    "enter_eligible_without_rollout_gate": True,
                    "allow_enter": False,
                    "waiting_reasons": ["enter_disabled_shadow_only"],
                    "setup_state": "valid",
                },
            ),
            # Waiting (setup present, no breakout yet).
            _record(trigger=_trigger("missing"), frame_meta=BUILT),
            # Insufficient 4H history.
            _record(
                trigger=_trigger(
                    "unknown", reasons=("insufficient_4h_history",)
                ),
                frame_meta=BUILT,
            ),
            # 4H frame could not be fetched -> trigger absent.
            _record(
                trigger=_trigger(
                    "unknown", reasons=("four_hour_data_missing",)
                ),
                frame_meta={
                    "contract_version": "four_hour_frame.v1",
                    "state": "fetch_error", "frame_hash": None,
                },
            ),
            # Setup never reached trigger analysis.
            _record(
                verdict="AVOID", trigger=None, frame_meta=BUILT,
                rejection_reason="no_valid_selected_range",
                policy={
                    "enter_eligible_without_rollout_gate": False,
                    "allow_enter": False,
                    "waiting_reasons": [],
                    "setup_state": "invalid",
                },
                readiness_status="insufficient_history",
            ),
        ]
        metrics = aggregate_strategy_shadow_metrics(records)
        assert metrics["metrics_contract_version"] == (
            STRATEGY_METRICS_CONTRACT_VERSION
        )
        assert len(metrics["groups"]) == 1
        g = metrics["groups"][0]

        assert g["evaluated_count"] == 5
        assert g["trigger_confirmed_count"] == 1
        assert g["trigger_waiting_count"] == 1
        assert g["four_hour_insufficient_count"] == 1
        assert g["trigger_absent_count"] == 1
        assert g["trigger_not_evaluated_count"] == 1
        assert g["real_trigger_price_count"] == 1
        assert g["four_hour_ready_count"] == 2      # confirmed + waiting
        # Frame states separate from trigger states.
        assert g["four_hour_frames_built_count"] == 4
        assert g["four_hour_fetch_error_count"] == 1
        # Daily readiness separate from 4H readiness.
        assert g["daily_ready_count"] == 4
        assert g["daily_insufficient_count"] == 1
        # Rollout blocking separate from strategy rejection.
        assert g["rollout_blocked_count"] == 1
        assert g["rejected_setup_count"] == 1
        assert g["setup_present_count"] == 4
        # Missing outcomes stay missing (never zero returns).
        assert g["missing_outcome_count"] == 5
        assert g["matured_outcome_count"] == 0

    def test_real_trigger_price_requires_a_price(self):
        # A confirmed state without a price must not count a real price.
        records = [
            _record(trigger=_trigger("confirmed", trigger_price=None)),
            _record(trigger=_trigger("confirmed", trigger_price=51.25)),
        ]
        g = aggregate_strategy_shadow_metrics(records)["groups"][0]
        assert g["trigger_confirmed_count"] == 2
        assert g["real_trigger_price_count"] == 1

    def test_matured_vs_missing_outcomes(self):
        records = [
            _record(has_outcome=True, outcome_status="complete"),
            _record(has_outcome=True, outcome_status="partial"),
            _record(has_outcome=False),
        ]
        g = aggregate_strategy_shadow_metrics(records)["groups"][0]
        assert g["matured_outcome_count"] == 1
        assert g["with_outcome_count"] == 2
        assert g["missing_outcome_count"] == 1
        assert g["outcome_coverage"] == pytest.approx(2 / 3)

    def test_grouping_by_frame_contract_versions(self):
        records = [
            _record(frame_meta=BUILT),
            _record(frame_meta=BUILT),
            _record(frame_meta={
                "contract_version": "four_hour_frame.v2-exp",
                "state": "built", "frame_hash": "x",
            }),
            _record(frame_meta=None),   # daily-only row
            _record(daily_contract="daily_ohlcv_snapshot.v2-exp",
                    frame_meta=BUILT),
        ]
        metrics = aggregate_strategy_shadow_metrics(records)
        keys = {
            (
                g["daily_frame_contract_version"],
                g["four_hour_frame_contract_version"],
            )
            for g in metrics["groups"]
        }
        assert keys == {
            ("daily_ohlcv_snapshot.v1", "four_hour_frame.v1"),
            ("daily_ohlcv_snapshot.v1", "four_hour_frame.v2-exp"),
            ("daily_ohlcv_snapshot.v1", None),
            ("daily_ohlcv_snapshot.v2-exp", "four_hour_frame.v1"),
        }

    def test_empty_dataset_is_safe(self):
        metrics = aggregate_strategy_shadow_metrics([])
        assert metrics["evaluated_count"] == 0
        assert metrics["groups"] == []

    def test_v1_fields_keep_exact_semantics(self):
        """Every 9D field survives with identical meaning."""
        records = [
            _record(
                verdict="WATCH",
                policy={
                    "enter_eligible_without_rollout_gate": True,
                    "allow_enter": False,
                    "waiting_reasons": ["enter_disabled_shadow_only"],
                    "setup_state": "valid",
                },
            ),
        ]
        g = aggregate_strategy_shadow_metrics(records)["groups"][0]
        assert g["decision_counts"] == {"WATCH": 1}
        assert g["valid_decision_count"] == 1
        assert g["rollout_blocked_count"] == 1
        assert g["pre_rollout_enter_candidate_count"] == 1
        assert g["waiting_reason_distribution"] == {
            "enter_disabled_shadow_only": 1
        }
        assert g["outcome_eligible_count"] == 1
        assert g["outcome_coverage"] == 0.0
