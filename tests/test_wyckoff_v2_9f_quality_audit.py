"""Phase 9F4: evidence-quality audit (shadow_evidence_quality.v1)."""

from __future__ import annotations

import copy
from typing import Any, Dict, List

import pytest

from app.workers.shadow.quality_audit import (
    QUALITY_AUDIT_CONTRACT_VERSION,
    SEVERITIES,
    build_quality_audit,
)

from test_wyckoff_v2_9f_cohorts import evidence_record, trigger_record
from test_wyckoff_v2_9f_outcome_evidence import outcome_row


def _issue_map(audit: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {issue["code"]: issue for issue in audit["issues"]}


class TestAuditContract:
    def test_contract_and_vocabulary(self):
        audit = build_quality_audit([])
        assert audit["contract_version"] == QUALITY_AUDIT_CONTRACT_VERSION
        assert audit["severity_vocabulary"] == list(SEVERITIES)
        assert audit["issue_count"] == 0
        assert audit["blocking_count"] == 0

    def test_no_record_mutation(self):
        records = [
            evidence_record(
                trigger=trigger_record("confirmed", price=None),
                frame_state="fetch_error",
            ),
        ]
        frozen = copy.deepcopy(records)
        build_quality_audit(records)
        assert records == frozen


class TestConfigurationIssues:
    def test_missing_db_pattern_row_is_blocking(self):
        audit = build_quality_audit(
            [], strategy_discovery={
                "db_configured": False,
                "config_status": "missing_pattern_row",
            },
        )
        issue = _issue_map(audit)["missing_db_pattern_row"]
        assert issue["severity"] == "blocking"
        assert audit["blocking_count"] == 1

    def test_configured_row_raises_no_issue(self):
        audit = build_quality_audit(
            [], strategy_discovery={
                "db_configured": True, "config_status": "configured",
            },
        )
        assert "missing_db_pattern_row" not in _issue_map(audit)


class TestAcquisitionIssues:
    def test_unsupported_provider_detected(self):
        audit = build_quality_audit([
            evidence_record(frame_state="unsupported_provider"),
        ])
        issue = _issue_map(audit)["unsupported_provider_4h"]
        assert issue["severity"] == "warning"
        assert issue["affected_record_count"] == 1

    def test_frame_rejection_and_duplicate_bars_detected(self):
        record = evidence_record(frame_state="frame_rejected")
        record["four_hour_frame_meta"]["reason_code"] = "duplicate_bar_start"
        audit = build_quality_audit([record])
        issues = _issue_map(audit)
        assert issues["four_hour_frame_rejected"]["affected_record_count"] == 1
        assert issues["four_hour_frame_rejected"]["detail"]["reasons"] == [
            "duplicate_bar_start"
        ]
        assert issues["duplicate_4h_bars_rejected"]["affected_record_count"] == 1

    def test_insufficient_and_stale_are_separate(self):
        audit = build_quality_audit([
            evidence_record(trigger=trigger_record(
                "unknown", reasons=("insufficient_4h_history",)
            )),
            evidence_record(trigger=trigger_record(
                "unknown", reasons=("four_hour_trigger_stale",)
            )),
            evidence_record(readiness_status="insufficient_history"),
        ])
        issues = _issue_map(audit)
        assert issues["insufficient_four_hour_history"][
            "affected_record_count"] == 1
        assert issues["stale_four_hour_data"]["affected_record_count"] == 1
        assert issues["insufficient_daily_history"][
            "affected_record_count"] == 1


class TestTriggerHonesty:
    def test_confirmed_trigger_missing_price_is_blocking(self):
        audit = build_quality_audit([
            evidence_record(trigger=trigger_record("confirmed", price=None)),
        ])
        issue = _issue_map(audit)["confirmed_trigger_missing_price"]
        assert issue["severity"] == "blocking"
        assert audit["blocking_count"] == 1

    def test_confirmed_with_real_price_is_clean(self):
        audit = build_quality_audit([
            evidence_record(trigger=trigger_record("confirmed", price=55.0)),
        ])
        assert "confirmed_trigger_missing_price" not in _issue_map(audit)

    def test_missing_trigger_evidence_on_valid_setup(self):
        audit = build_quality_audit([
            evidence_record(trigger=None, frame_state="built",
                            setup_state="valid"),
        ])
        assert _issue_map(audit)["missing_trigger_evidence"][
            "affected_record_count"] == 1


class TestCoverageAndConsistency:
    def test_outcome_coverage_gaps(self):
        audit = build_quality_audit([
            evidence_record(has_outcome=False),
            evidence_record(has_outcome=True, outcome_status="partial"),
        ])
        issues = _issue_map(audit)
        assert issues["missing_outcomes"]["affected_record_count"] == 1
        assert issues["missing_outcomes"]["severity"] == "warning"
        assert issues["pending_outcomes"]["severity"] == "informational"

    def test_mixed_version_detection(self):
        audit = build_quality_audit([
            evidence_record(strategy_version="wyckoff_mtf.v2"),
            evidence_record(strategy_version="wyckoff_mtf.v2-exp",
                            config_hash="cfg-2"),
        ])
        issues = _issue_map(audit)
        assert issues["mixed_strategy_versions"]["detail"]["values"] == [
            "wyckoff_mtf.v2", "wyckoff_mtf.v2-exp",
        ]
        assert "mixed_config_hashes" in issues
        assert "mixed_policy_versions" not in issues

    def test_provider_mismatch_and_benchmark_missing_from_outcome_rows(self):
        mismatch = outcome_row()
        mismatch["outcome"]["error_code"] = "provider_mismatch"
        no_benchmark = outcome_row(status="partial", benchmark_returns=None)
        audit = build_quality_audit(
            [], outcome_rows=[mismatch, no_benchmark],
        )
        issues = _issue_map(audit)
        assert issues["provider_mismatch_outcomes"][
            "affected_record_count"] == 1
        assert issues["benchmark_snapshot_missing"][
            "affected_record_count"] == 1


class TestCampaignIssues:
    def test_partial_failures_and_symbol_gaps(self):
        runs = [
            {
                "run_id": "run-1", "status": "failed",
                "requested_symbols": ["A", "B"], "pair_count": 0,
                "rejected_symbols": {},
            },
            {
                "run_id": "run-2", "status": "completed",
                "requested_symbols": ["C", "D", "E"], "pair_count": 1,
                "rejected_symbols": {"fetch_error": ["D"]},
            },
            {
                "run_id": "run-3", "status": "completed",
                "requested_symbols": ["F"], "pair_count": 1,
                "rejected_symbols": {},
            },
        ]
        audit = build_quality_audit([], campaign_runs=runs)
        issues = _issue_map(audit)
        assert issues["campaign_partial_failures"]["detail"]["run_ids"] == [
            "run-1"
        ]
        # run-2: 1 pair + 1 rejected < 3 requested -> gap; run-3 clean.
        assert issues["campaign_symbol_count_gaps"]["detail"]["run_ids"] == [
            "run-2"
        ]

    def test_clean_campaigns_raise_no_issue(self):
        runs = [{
            "run_id": "run-1", "status": "completed",
            "requested_symbols": ["A"], "pair_count": 1,
            "rejected_symbols": {},
        }]
        audit = build_quality_audit([], campaign_runs=runs)
        assert "campaign_partial_failures" not in _issue_map(audit)
        assert "campaign_symbol_count_gaps" not in _issue_map(audit)
