"""Phase 9F1/9F2: evidence review filters and explicit cohort analysis."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import pytest

from app.workers.shadow.evidence_cohorts import (
    COHORT_CONTRACT_VERSION,
    COHORT_PREDICATES,
    build_cohorts,
    cohort_members,
)
from app.workers.shadow.evidence_review import (
    EVIDENCE_REVIEW_CONTRACT_VERSION,
    EvidenceFilterError,
    apply_derived_filters,
    filters_for_response,
    normalize_evidence_filters,
    outcome_maturity,
    readiness_state,
)


def evidence_record(
    *,
    symbol: str = "AAAX",
    snapshot: str = "2026-07-17",
    verdict: str = "WATCH",
    readiness_status: Optional[str] = "ready",
    trigger: Optional[Dict[str, Any]] = None,
    frame_state: Optional[str] = "built",
    setup_state: str = "valid",
    eligible: bool = False,
    allow_enter: bool = False,
    rejection_reason: Optional[str] = None,
    has_outcome: bool = False,
    outcome_status: Optional[str] = None,
    strategy_version: str = "wyckoff_mtf.v2",
    policy_version: str = "wyckoff_mtf.policy.v1",
    config_hash: str = "cfg-1",
    campaign_ids: Optional[List[str]] = None,
    created: str = "2026-07-18T00:00:00+00:00",
) -> Dict[str, Any]:
    """Synthetic frozen evaluation record in the persistence read shape."""
    return {
        "evaluation_id": f"eval-{symbol}-{snapshot}",
        "pair_id": f"pair-{symbol}-{snapshot}",
        "arm_code": "candidate_wyckoff_v2",
        "strategy_code": "wyckoff_mtf_v2",
        "strategy_version": strategy_version,
        "decision_policy_version": policy_version,
        "config_hash": config_hash,
        "experiment_code": "wyckoff_v2_vs_baseline",
        "experiment_version": "wyckoff_v2_shadow.v2",
        "verdict": verdict,
        "score": None,
        "reason": None,
        "rejection_reason": rejection_reason,
        "policy": {
            "setup_state": setup_state,
            "enter_eligible_without_rollout_gate": eligible,
            "allow_enter": allow_enter,
            "waiting_reasons": (
                ["enter_disabled_shadow_only"] if eligible and not allow_enter
                else []
            ),
        },
        "readiness_status": readiness_status,
        "four_hour_trigger": trigger,
        "four_hour_frame_meta": (
            None if frame_state is None else {
                "contract_version": "four_hour_frame.v1",
                "state": frame_state,
                "frame_hash": "4h" if frame_state == "built" else None,
            }
        ),
        "evidence_categories": [],
        "symbol": symbol,
        "snapshot_date": date.fromisoformat(snapshot),
        "daily_frame_contract_version": "daily_ohlcv_snapshot.v1",
        "provider": "massive",
        "has_outcome": has_outcome,
        "outcome_status": outcome_status,
        "campaign_ids": campaign_ids or [],
        "created_at": datetime.fromisoformat(created),
    }


def trigger_record(state: str, *, reasons=(), price=None) -> Dict[str, Any]:
    return {
        "state": state,
        "reason_codes": list(reasons),
        "trigger_price": price,
    }


def _sample_records() -> List[Dict[str, Any]]:
    return [
        evidence_record(
            symbol="AAAX", snapshot="2026-07-16",
            trigger=trigger_record("confirmed", price=55.0),
            eligible=True,
            has_outcome=True, outcome_status="complete",
            campaign_ids=["camp-1"],
        ),
        evidence_record(
            symbol="BBBX", snapshot="2026-07-16",
            trigger=trigger_record("missing"),
            has_outcome=True, outcome_status="partial",
            campaign_ids=["camp-1"],
        ),
        evidence_record(
            symbol="CCCX", snapshot="2026-07-17",
            trigger=trigger_record(
                "unknown", reasons=("insufficient_4h_history",)
            ),
            campaign_ids=["camp-2"],
        ),
        evidence_record(
            symbol="DDDX", snapshot="2026-07-17",
            trigger=trigger_record(
                "unknown", reasons=("four_hour_data_missing",)
            ),
            frame_state="fetch_error",
        ),
        evidence_record(
            symbol="EEEX", snapshot="2026-07-17", verdict="AVOID",
            readiness_status="insufficient_history",
            trigger=None, setup_state="unknown",
            rejection_reason="insufficient_history",
        ),
        evidence_record(
            symbol="FFFX", snapshot="2026-07-17", verdict="AVOID",
            trigger=None, setup_state="invalid",
            rejection_reason="no_valid_selected_range",
            frame_state="frame_rejected",
        ),
    ]


class TestFilterNormalization:
    def test_defaults_and_contract(self):
        filters = normalize_evidence_filters()
        assert filters["contract_version"] == EVIDENCE_REVIEW_CONTRACT_VERSION
        assert filters["strategy_code"] == "wyckoff_mtf_v2"
        assert filters["limit"] == 1000

    def test_unknown_vocabularies_reject(self):
        with pytest.raises(EvidenceFilterError):
            normalize_evidence_filters(trigger_state="bogus")
        with pytest.raises(EvidenceFilterError):
            normalize_evidence_filters(readiness="bogus")
        with pytest.raises(EvidenceFilterError):
            normalize_evidence_filters(outcome_maturity_filter="bogus")

    def test_limit_bounds(self):
        with pytest.raises(EvidenceFilterError):
            normalize_evidence_filters(limit=0)
        with pytest.raises(EvidenceFilterError):
            normalize_evidence_filters(limit=2001)

    def test_bad_date_rejects(self):
        with pytest.raises(EvidenceFilterError):
            normalize_evidence_filters(min_snapshot_date="July 1")

    def test_response_echo_is_json_safe(self):
        filters = normalize_evidence_filters(
            symbol="aaax", min_snapshot_date="2026-07-01",
        )
        echoed = filters_for_response(filters)
        assert echoed["symbol"] == "AAAX"
        assert echoed["min_snapshot_date"] == "2026-07-01"


class TestDerivedFilters:
    def test_trigger_state_filter(self):
        records = _sample_records()
        filters = normalize_evidence_filters(trigger_state="confirmed")
        result = apply_derived_filters(records, filters)
        assert [r["symbol"] for r in result] == ["AAAX"]

    def test_readiness_filter(self):
        records = _sample_records()
        filters = normalize_evidence_filters(readiness="not_ready")
        result = apply_derived_filters(records, filters)
        assert [r["symbol"] for r in result] == ["EEEX"]

    def test_rollout_blocked_filter(self):
        records = _sample_records()
        filters = normalize_evidence_filters(rollout_blocked=True)
        result = apply_derived_filters(records, filters)
        assert [r["symbol"] for r in result] == ["AAAX"]

    def test_outcome_maturity_filter(self):
        records = _sample_records()
        matured = apply_derived_filters(
            records, normalize_evidence_filters(outcome_maturity_filter="matured")
        )
        pending = apply_derived_filters(
            records, normalize_evidence_filters(outcome_maturity_filter="pending")
        )
        missing = apply_derived_filters(
            records, normalize_evidence_filters(outcome_maturity_filter="missing")
        )
        assert [r["symbol"] for r in matured] == ["AAAX"]
        assert [r["symbol"] for r in pending] == ["BBBX"]
        assert len(missing) == 4

    def test_maturity_semantics(self):
        assert outcome_maturity({"has_outcome": False}) == "missing"
        assert outcome_maturity(
            {"has_outcome": True, "outcome_status": "partial"}
        ) == "pending"
        # The honest error state is NOT matured.
        assert outcome_maturity(
            {"has_outcome": True, "outcome_status": "error"}
        ) == "pending"
        assert outcome_maturity(
            {"has_outcome": True, "outcome_status": "complete"}
        ) == "matured"

    def test_readiness_semantics(self):
        assert readiness_state({"readiness_status": "ready"}) == "ready"
        assert readiness_state(
            {"readiness_status": "missing_volume"}
        ) == "not_ready"
        assert readiness_state({"readiness_status": None}) == "unknown"


class TestCohorts:
    def test_contract_and_universe(self):
        result = build_cohorts(_sample_records())
        assert result["contract_version"] == COHORT_CONTRACT_VERSION
        assert result["cohorts_overlap"] is True
        assert result["evaluated_count"] == 6
        assert result["cohorts"]["evaluated"]["record_count"] == 6

    def test_explicit_membership_rules(self):
        records = _sample_records()
        cohorts = build_cohorts(records)["cohorts"]
        assert cohorts["trigger_confirmed"]["record_count"] == 1
        assert cohorts["trigger_waiting"]["record_count"] == 1
        assert cohorts["four_hour_insufficient"]["record_count"] == 1
        assert cohorts["trigger_absent"]["record_count"] == 1
        assert cohorts["trigger_not_evaluated"]["record_count"] == 2
        assert cohorts["daily_ready"]["record_count"] == 5
        assert cohorts["daily_insufficient"]["record_count"] == 1
        assert cohorts["setup_present"]["record_count"] == 4
        assert cohorts["setup_rejected"]["record_count"] == 2
        assert cohorts["rollout_blocked"]["record_count"] == 1
        assert cohorts["matured_outcome"]["record_count"] == 1
        assert cohorts["pending_outcome"]["record_count"] == 1
        assert cohorts["missing_outcome"]["record_count"] == 4
        assert cohorts["provider_failure_4h"]["record_count"] == 1
        assert cohorts["frame_rejected_4h"]["record_count"] == 1

    def test_overlapping_cohorts_are_not_partitioned(self):
        records = _sample_records()
        confirmed = cohort_members(records, "trigger_confirmed")
        blocked = cohort_members(records, "rollout_blocked")
        evaluated = cohort_members(records, "evaluated")
        # The confirmed record is ALSO rollout-blocked and evaluated.
        assert confirmed[0]["symbol"] == blocked[0]["symbol"] == "AAAX"
        assert len(evaluated) == 6
        total = sum(
            build_cohorts(records)["cohorts"][name]["record_count"]
            for name in COHORT_PREDICATES
        )
        assert total > len(records)   # overlap is real, never hidden

    def test_unique_counts_and_timestamps(self):
        records = _sample_records()
        summary = build_cohorts(records)["cohorts"]["evaluated"]
        assert summary["unique_symbol_count"] == 6
        assert summary["unique_session_count"] == 2
        assert summary["unique_campaign_count"] == 2
        assert summary["first_session"] == "2026-07-16"
        assert summary["last_session"] == "2026-07-17"
        assert summary["outcome_coverage"] == pytest.approx(2 / 6)

    def test_version_distributions(self):
        records = _sample_records() + [
            evidence_record(
                symbol="GGGX", snapshot="2026-07-17",
                strategy_version="wyckoff_mtf.v2-exp",
                config_hash="cfg-2",
            ),
        ]
        summary = build_cohorts(records)["cohorts"]["evaluated"]
        assert summary["strategy_version_distribution"] == {
            "wyckoff_mtf.v2": 6, "wyckoff_mtf.v2-exp": 1,
        }
        assert summary["config_hash_distribution"] == {
            "cfg-1": 6, "cfg-2": 1,
        }
        assert summary["four_hour_frame_contract_distribution"] == {
            "four_hour_frame.v1": 7,
        }

    def test_reason_distribution_merges_rejections_and_trigger_reasons(self):
        summary = build_cohorts(_sample_records())["cohorts"]["evaluated"]
        reasons = summary["reason_distribution"]
        assert reasons["insufficient_history"] == 1
        assert reasons["no_valid_selected_range"] == 1
        assert reasons["insufficient_4h_history"] == 1
        assert reasons["four_hour_data_missing"] == 1

    def test_empty_data_is_safe(self):
        result = build_cohorts([])
        assert result["evaluated_count"] == 0
        for summary in result["cohorts"].values():
            assert summary["record_count"] == 0
            assert summary["outcome_coverage"] is None
            assert summary["first_session"] is None

    def test_deterministic_output(self):
        records = _sample_records()
        assert build_cohorts(records) == build_cohorts(list(records))
