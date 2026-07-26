"""Single prospective-campaign post-run audit (shadow_campaign_audit.v1).

Proves the verdict invariants: exact-membership validation (persisted set,
then explicit list, then a weak count), duplicate/side-effect/allow_enter
invalidation, resumable→incomplete (not invalid), and that ZERO confirmed
triggers is a valid campaign.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

from app.workers.shadow.campaign_audit import (
    CAMPAIGN_AUDIT_CONTRACT_VERSION,
    MEMBERSHIP_SOURCE_COUNT_ONLY,
    MEMBERSHIP_SOURCE_EXPLICIT,
    MEMBERSHIP_SOURCE_NONE,
    MEMBERSHIP_SOURCE_PERSISTED,
    VERDICT_INCOMPLETE,
    VERDICT_INVALID,
    VERDICT_MEMBERSHIP_UNVERIFIABLE,
    VERDICT_VALID,
    build_campaign_audit,
)

from test_wyckoff_v2_9f_cohorts import evidence_record, trigger_record


def _run(
    *,
    requested: List[str],
    status: str = "completed",
    rejected: Optional[Dict[str, List[str]]] = None,
    error_code: Optional[str] = None,
    campaign_id: str = "camp-1",
    as_of: str = "2026-07-24",
) -> Dict[str, Any]:
    return {
        "run_id": "run-" + status,
        "experiment_code": "wyckoff_v2_vs_baseline",
        "experiment_version": "wyckoff_v2_shadow.v2",
        "status": status,
        "requested_symbols": requested,
        "rejected_symbols": rejected or {},
        "error_code": error_code,
        "pair_count": len(requested),
        "campaign": {
            "campaign_id": campaign_id,
            "as_of_date": as_of,
            "chunk_index": 0,
            "chunk_count": 1,
            "requested_count": len(requested),
        },
    }


def _records(symbols, **kwargs):
    return [
        evidence_record(symbol=s, snapshot="2026-07-24", **kwargs)
        for s in symbols
    ]


class TestContractAndValidCampaign:
    def test_contract_version(self):
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]), [_run(requested=["AAAX", "BBBX"])]
        )
        assert audit["audit_contract_version"] == CAMPAIGN_AUDIT_CONTRACT_VERSION

    def test_persisted_membership_valid(self):
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]), [_run(requested=["AAAX", "BBBX"])]
        )
        assert audit["verdict"] == VERDICT_VALID
        assert audit["membership_source"] == MEMBERSHIP_SOURCE_PERSISTED
        assert audit["evaluation_count"] == 2
        assert audit["unique_symbol_count"] == 2
        assert audit["missing_symbols"] == []
        assert audit["verdict_reasons"] == []

    def test_zero_confirmed_triggers_is_valid(self):
        # No trigger records at all → zero confirmed triggers.
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]), [_run(requested=["AAAX", "BBBX"])]
        )
        assert audit["trigger_confirmed_count"] == 0
        assert audit["verdict"] == VERDICT_VALID

    def test_confirmed_triggers_still_valid(self):
        records = [
            evidence_record(
                symbol="AAAX", snapshot="2026-07-24",
                trigger=trigger_record("confirmed", price=55.0),
            ),
            evidence_record(symbol="BBBX", snapshot="2026-07-24"),
        ]
        audit = build_campaign_audit(records, [_run(requested=["AAAX", "BBBX"])])
        assert audit["trigger_confirmed_count"] == 1
        assert audit["verdict"] == VERDICT_VALID

    def test_no_input_mutation(self):
        records = _records(["AAAX", "BBBX"])
        runs = [_run(requested=["AAAX", "BBBX"])]
        frozen_r, frozen_runs = copy.deepcopy(records), copy.deepcopy(runs)
        build_campaign_audit(records, runs)
        assert records == frozen_r
        assert runs == frozen_runs


class TestMembershipPrecedence:
    def test_explicit_used_when_no_persisted(self):
        runs = [_run(requested=[])]  # no persisted symbols
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]), runs,
            expected_symbols=["bbbx", "aaax"],
        )
        assert audit["membership_source"] == MEMBERSHIP_SOURCE_EXPLICIT
        assert audit["verdict"] == VERDICT_VALID

    def test_count_only_is_unverifiable(self):
        runs = [{"run_id": "r", "status": "completed",
                 "requested_symbols": [], "rejected_symbols": {},
                 "campaign": {"campaign_id": "camp-1"}}]
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]), runs, expected_count=2,
        )
        assert audit["membership_source"] == MEMBERSHIP_SOURCE_COUNT_ONLY
        assert audit["verdict"] == VERDICT_MEMBERSHIP_UNVERIFIABLE

    def test_count_match_never_proves_membership(self):
        # Two arbitrary symbols, count matches 2, but membership is unverifiable
        # so the campaign does NOT pass as valid.
        runs = [{"run_id": "r", "status": "completed",
                 "requested_symbols": [], "rejected_symbols": {},
                 "campaign": {"campaign_id": "camp-1"}}]
        audit = build_campaign_audit(
            _records(["ZZZX", "YYYX"]), runs, expected_count=2,
        )
        assert audit["verdict"] != VERDICT_VALID

    def test_no_expected_set_is_unverifiable(self):
        runs = [{"run_id": "r", "status": "completed",
                 "requested_symbols": [], "rejected_symbols": {},
                 "campaign": {"campaign_id": "camp-1"}}]
        audit = build_campaign_audit(_records(["AAAX"]), runs)
        assert audit["membership_source"] == MEMBERSHIP_SOURCE_NONE
        assert audit["verdict"] == VERDICT_MEMBERSHIP_UNVERIFIABLE

    def test_explicit_mismatch_with_persisted_is_invalid(self):
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]),
            [_run(requested=["AAAX", "BBBX"])],
            expected_symbols=["AAAX", "CCCX"],  # frozen file disagrees
        )
        assert audit["explicit_vs_persisted_mismatch"] is True
        assert audit["verdict"] == VERDICT_INVALID
        assert "membership_hash_mismatch" in audit["verdict_reasons"]


class TestInvalidation:
    def test_duplicate_evaluations_invalid(self):
        records = _records(["AAAX", "AAAX", "BBBX"])
        audit = build_campaign_audit(records, [_run(requested=["AAAX", "BBBX"])])
        assert audit["verdict"] == VERDICT_INVALID
        assert "duplicate_evaluations" in audit["verdict_reasons"]
        assert audit["duplicate_evaluations"] == {"AAAX": 2}

    def test_missing_expected_symbol_invalid(self):
        audit = build_campaign_audit(
            _records(["AAAX"]), [_run(requested=["AAAX", "BBBX"])]
        )
        assert audit["verdict"] == VERDICT_INVALID
        assert audit["missing_symbols"] == ["BBBX"]
        assert "missing_expected_symbols" in audit["verdict_reasons"]

    def test_allow_enter_true_invalid(self):
        records = [
            evidence_record(symbol="AAAX", snapshot="2026-07-24",
                            eligible=True, allow_enter=True),
            evidence_record(symbol="BBBX", snapshot="2026-07-24"),
        ]
        audit = build_campaign_audit(records, [_run(requested=["AAAX", "BBBX"])])
        assert audit["allow_enter_true_count"] == 1
        assert audit["verdict"] == VERDICT_INVALID
        assert "allow_enter_true" in audit["verdict_reasons"]

    def test_watches_created_invalid(self):
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]), [_run(requested=["AAAX", "BBBX"])],
            watches_created=1,
        )
        assert audit["verdict"] == VERDICT_INVALID
        assert "watches_created" in audit["verdict_reasons"]

    def test_decision_cards_created_invalid(self):
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]), [_run(requested=["AAAX", "BBBX"])],
            decision_cards_created=2,
        )
        assert audit["verdict"] == VERDICT_INVALID
        assert "decision_cards_created" in audit["verdict_reasons"]

    def test_systemic_pair_failure_invalid(self):
        runs = [_run(requested=["AAAX", "BBBX"], status="completed",
                     rejected={"pair_error": ["AAAX", "BBBX"]})]
        audit = build_campaign_audit(_records([]), runs)
        assert audit["pair_error_count"] == 2
        assert audit["verdict"] == VERDICT_INVALID
        assert "systemic_pair_failure" in audit["verdict_reasons"]


class TestIncompleteVsInvalid:
    def test_resumable_run_is_incomplete_not_invalid(self):
        # A failed chunk with no systemic pair/provider failure is resumable.
        runs = [_run(requested=["AAAX", "BBBX"], status="run_failed",
                     error_code="chunk_TimeoutError")]
        audit = build_campaign_audit(
            _records(["AAAX", "BBBX"]), runs,
        )
        assert audit["terminal_success"] is False
        assert audit["verdict"] == VERDICT_INCOMPLETE
        assert "non_terminal_campaign_status" in audit["verdict_reasons"]

    def test_completed_with_failures_but_pair_error_is_invalid(self):
        # A pair_error is systemic → invalid wins over incomplete.
        runs = [
            _run(requested=["AAAX"], status="completed"),
            _run(requested=["BBBX"], status="run_failed",
                 rejected={"pair_error": ["BBBX"]}),
        ]
        audit = build_campaign_audit(_records(["AAAX"]), runs)
        assert audit["verdict"] == VERDICT_INVALID


class TestProviderFailurePolicy:
    def test_isolated_4h_fetch_error_is_still_valid(self):
        # A handful of typed 4H fetch_errors on otherwise healthy evaluations
        # is an isolated, bounded failure — surfaced but NOT invalidating.
        records = [
            evidence_record(symbol="AAAX", snapshot="2026-07-24",
                            frame_state="fetch_error"),
            evidence_record(symbol="BBBX", snapshot="2026-07-24"),
        ]
        audit = build_campaign_audit(records, [_run(requested=["AAAX", "BBBX"])])
        assert audit["provider_failure_count"] == 1
        assert audit["systemic_provider_failure"] is False
        assert audit["verdict"] == VERDICT_VALID

    def test_all_unsupported_provider_is_systemic_invalid(self):
        records = [
            evidence_record(symbol="AAAX", snapshot="2026-07-24",
                            frame_state="unsupported_provider"),
            evidence_record(symbol="BBBX", snapshot="2026-07-24",
                            frame_state="unsupported_provider"),
        ]
        audit = build_campaign_audit(records, [_run(requested=["AAAX", "BBBX"])])
        assert audit["systemic_provider_failure"] is True
        assert audit["verdict"] == VERDICT_INVALID
        assert "systemic_provider_failure" in audit["verdict_reasons"]


class TestVerdictStability:
    def test_repeated_audit_is_deterministic(self):
        records = _records(["AAAX", "BBBX"])
        runs = [_run(requested=["AAAX", "BBBX"])]
        a1 = build_campaign_audit(records, runs)
        a2 = build_campaign_audit(records, runs)
        assert a1 == a2

    def test_every_non_valid_verdict_has_reasons(self):
        # invalid, incomplete and membership_unverifiable all carry reasons.
        invalid = build_campaign_audit(
            _records(["AAAX", "AAAX"]), [_run(requested=["AAAX"])]
        )
        incomplete = build_campaign_audit(
            _records(["AAAX"]),
            [_run(requested=["AAAX"], status="run_failed")],
        )
        unverifiable = build_campaign_audit(
            _records(["AAAX"]),
            [{"run_id": "r", "status": "completed", "requested_symbols": [],
              "rejected_symbols": {}, "campaign": {"campaign_id": "c"}}],
        )
        assert invalid["verdict"] == VERDICT_INVALID and invalid["verdict_reasons"]
        assert incomplete["verdict"] == VERDICT_INCOMPLETE and (
            incomplete["verdict_reasons"]
        )
        assert unverifiable["verdict"] == VERDICT_MEMBERSHIP_UNVERIFIABLE and (
            unverifiable["verdict_reasons"]
        )
