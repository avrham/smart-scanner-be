"""Phase 9F3: outcome and benchmark evidence (shadow_outcome_evidence.v1)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from app.workers.shadow.outcome_evidence import (
    OUTCOME_EVIDENCE_CONTRACT_VERSION,
    build_outcome_evidence,
)


def outcome_row(
    *,
    control_verdict: str = "AVOID",
    candidate_verdict: str = "WATCH",
    ret_1d: Optional[float] = None,
    ret_5d: Optional[float] = None,
    spy_rel_1d: Optional[float] = None,
    qqq_rel_1d: Optional[float] = None,
    status: str = "partial",
    candidate_version: str = "wyckoff_mtf.v2",
    config_hash: str = "cfg-1",
    benchmark_returns: Optional[Dict[str, Any]] = "default",
) -> Dict[str, Any]:
    if benchmark_returns == "default":
        benchmark_returns = {"SPY": {"1D": 0.1}, "QQQ": {"1D": 0.2}}
    return {
        "pair": {
            "experiment_code": "wyckoff_v2_vs_baseline",
            "experiment_version": "wyckoff_v2_shadow.v2",
            "symbol": "AAAX",
            "snapshot_date": "2026-07-16",
        },
        "control": {
            "strategy_code": "sma150_bounce",
            "strategy_version": "sma150.v2",
            "decision_policy_version": "strategy_decision.v1",
            "config_hash": "ctl",
            "verdict": control_verdict,
        },
        "candidate": {
            "strategy_code": "wyckoff_mtf_v2",
            "strategy_version": candidate_version,
            "decision_policy_version": "wyckoff_mtf.policy.v1",
            "config_hash": config_hash,
            "verdict": candidate_verdict,
        },
        "outcome": {
            "outcome_status": status,
            "error_code": None,
            "returns": {
                "1D": ret_1d, "3D": None, "5D": ret_5d,
                "10D": None, "20D": None,
            },
            "benchmark_returns": benchmark_returns,
        },
        "relative_returns": {
            "SPY": {"1D": spy_rel_1d, "3D": None, "5D": None,
                    "10D": None, "20D": None},
            "QQQ": {"1D": qqq_rel_1d, "3D": None, "5D": None,
                    "10D": None, "20D": None},
        },
    }


def _window(group: Dict[str, Any], label: str) -> Dict[str, Any]:
    return {w["window"]: w for w in group["by_window"]}[label]


class TestGroupingAndContract:
    def test_contract_and_missing_outcomes_reported(self):
        evidence = build_outcome_evidence([], missing_outcome_count=7)
        assert evidence["contract_version"] == (
            OUTCOME_EVIDENCE_CONTRACT_VERSION
        )
        assert evidence["total_outcome_rows"] == 0
        assert evidence["missing_outcome_count"] == 7
        assert evidence["groups"] == []

    def test_groups_never_pool_versions(self):
        rows = [
            outcome_row(candidate_version="wyckoff_mtf.v2"),
            outcome_row(candidate_version="wyckoff_mtf.v2-exp"),
            outcome_row(config_hash="cfg-2"),
        ]
        evidence = build_outcome_evidence(rows)
        assert len(evidence["groups"]) == 3

    def test_maturity_counts(self):
        rows = [
            outcome_row(status="complete"),
            outcome_row(status="partial"),
            outcome_row(status="pending_forward_bars"),
            outcome_row(status="error"),
        ]
        group = build_outcome_evidence(rows)["groups"][0]
        assert group["pair_count"] == 4
        assert group["matured_outcome_count"] == 1
        assert group["pending_outcome_count"] == 2
        assert group["error_outcome_count"] == 1


class TestHorizonSeparation:
    def test_horizons_never_combined(self):
        rows = [outcome_row(ret_1d=2.0, ret_5d=-3.0)]
        group = build_outcome_evidence(rows)["groups"][0]
        w1 = _window(group, "1D")
        w5 = _window(group, "5D")
        assert w1["candidate"]["mean_return"] == pytest.approx(2.0)
        assert w5["candidate"]["mean_return"] == pytest.approx(-3.0)
        w3 = _window(group, "3D")
        assert w3["candidate"]["return_count"] == 0
        assert w3["candidate"]["mean_return"] is None

    def test_missing_return_is_excluded_with_count_never_zero(self):
        rows = [
            outcome_row(ret_1d=4.0),
            outcome_row(ret_1d=None),
        ]
        w1 = _window(build_outcome_evidence(rows)["groups"][0], "1D")
        assert w1["candidate"]["actionable_pair_count"] == 2
        assert w1["candidate"]["return_count"] == 1
        assert w1["candidate"]["mean_return"] == pytest.approx(4.0)


class TestDecisionCohorts:
    def test_candidate_and_control_cohorts_are_verdict_based(self):
        rows = [
            outcome_row(candidate_verdict="WATCH", control_verdict="AVOID",
                        ret_1d=1.0),
            outcome_row(candidate_verdict="AVOID", control_verdict="ENTER",
                        ret_1d=-2.0),
        ]
        w1 = _window(build_outcome_evidence(rows)["groups"][0], "1D")
        assert w1["candidate"]["actionable_pair_count"] == 1
        assert w1["candidate"]["mean_return"] == pytest.approx(1.0)
        assert w1["control"]["actionable_pair_count"] == 1
        assert w1["control"]["mean_return"] == pytest.approx(-2.0)

    def test_positive_return_rate(self):
        rows = [
            outcome_row(ret_1d=1.0),
            outcome_row(ret_1d=-1.0),
            outcome_row(ret_1d=3.0),
        ]
        w1 = _window(build_outcome_evidence(rows)["groups"][0], "1D")
        assert w1["candidate"]["positive_return_count"] == 2
        assert w1["candidate"]["positive_return_rate"] == pytest.approx(2 / 3)


class TestBenchmarks:
    def test_spy_and_qqq_stay_separate(self):
        rows = [
            outcome_row(ret_1d=2.0, spy_rel_1d=1.5, qqq_rel_1d=-0.5),
            outcome_row(ret_1d=1.0, spy_rel_1d=0.5, qqq_rel_1d=0.5),
        ]
        w1 = _window(build_outcome_evidence(rows)["groups"][0], "1D")
        spy = w1["benchmarks"]["SPY"]
        qqq = w1["benchmarks"]["QQQ"]
        assert spy["relative_mean_return"] == pytest.approx(1.0)
        assert spy["candidate_better_count"] == 2
        assert qqq["relative_mean_return"] == pytest.approx(0.0)
        assert qqq["candidate_better_count"] == 1
        assert qqq["candidate_better_rate"] == pytest.approx(0.5)

    def test_benchmark_relatives_only_over_candidate_actionable_pairs(self):
        rows = [
            outcome_row(candidate_verdict="AVOID", spy_rel_1d=9.9),
        ]
        w1 = _window(build_outcome_evidence(rows)["groups"][0], "1D")
        assert w1["benchmarks"]["SPY"]["relative_return_count"] == 0


class TestPairedActionDivergence:
    def test_paired_resolution_uses_existing_neutral_bands(self):
        rows = [
            # Candidate ENTER, control AVOID, +2% > 0.5% band -> candidate.
            outcome_row(candidate_verdict="ENTER", control_verdict="AVOID",
                        ret_1d=2.0),
            # Control ENTER, candidate AVOID, +2% -> control favorable.
            outcome_row(candidate_verdict="AVOID", control_verdict="ENTER",
                        ret_1d=2.0),
            # Candidate ENTER, -2% -> control favorable.
            outcome_row(candidate_verdict="ENTER", control_verdict="AVOID",
                        ret_1d=-2.0),
            # Within the 1D neutral band -> flat.
            outcome_row(candidate_verdict="ENTER", control_verdict="AVOID",
                        ret_1d=0.2),
            # No return yet -> incomplete, never resolved.
            outcome_row(candidate_verdict="ENTER", control_verdict="AVOID",
                        ret_1d=None),
        ]
        w1 = _window(build_outcome_evidence(rows)["groups"][0], "1D")
        paired = w1["paired_action_divergence"]
        assert paired["divergent_pair_count"] == 5
        assert paired["candidate_favorable_count"] == 1
        assert paired["control_favorable_count"] == 2
        assert paired["flat_or_neutral_count"] == 1
        assert paired["incomplete_count"] == 1
        assert paired["resolved_count"] == 4
        assert paired["candidate_favorable_rate"] == pytest.approx(0.25)

    def test_non_divergent_pairs_never_enter_paired_metrics(self):
        rows = [
            # Both actionable but neither/both ENTER -> not divergent.
            outcome_row(candidate_verdict="WATCH", control_verdict="WATCH",
                        ret_1d=5.0),
            outcome_row(candidate_verdict="ENTER", control_verdict="ENTER",
                        ret_1d=5.0),
        ]
        w1 = _window(build_outcome_evidence(rows)["groups"][0], "1D")
        assert w1["paired_action_divergence"]["divergent_pair_count"] == 0
        assert w1["paired_action_divergence"]["candidate_favorable_rate"] is None

    def test_deterministic(self):
        rows = [
            outcome_row(ret_1d=1.0),
            outcome_row(candidate_version="wyckoff_mtf.v2-exp", ret_1d=2.0),
        ]
        assert build_outcome_evidence(rows) == build_outcome_evidence(
            list(rows)
        )
