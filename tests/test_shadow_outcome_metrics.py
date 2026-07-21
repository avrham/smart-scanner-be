"""Phase 8.1B2: neutral resolution metrics — no superiority language."""

import json

import pytest

from app.workers.shadow.outcomes.constants import (
    METRICS_CONTRACT_VERSION,
    NEUTRAL_BANDS_PCT,
)
from app.workers.shadow.outcomes.metrics import (
    GROUPING_IDENTITY_FIELDS,
    aggregate_pair_outcome_metrics,
    classify_horizon_return,
    derive_enter_arm,
    grouping_identity,
)


def _row(
    *,
    control_verdict="ENTER",
    candidate_verdict="WATCH",
    category="v2_enter_v3_watch",
    control_config_hash="cfg-v2-a",
    candidate_config_hash="cfg-v3-a",
    control_strategy_version="sma150.v2",
    candidate_strategy_version="sma150.v3",
    control_policy="sma150_bounce.policy.legacy",
    candidate_policy="sma150_bounce.policy.v1",
    forward_provider="massive",
    ret_1d=2.0,
    ret_3d=None,
    status="partial",
    mfe=3.0,
    mae=-1.0,
    spy_1d=0.5,
    qqq_1d=0.3,
):
    return {
        "pair": {
            "experiment_code": "sma150_v2_vs_v3",
            "experiment_version": "sma150_shadow.v1",
            "symbol": "DHR",
        },
        "control": {
            "strategy_code": "sma150",
            "strategy_version": control_strategy_version,
            "decision_policy_version": control_policy,
            "config_hash": control_config_hash,
            "verdict": control_verdict,
        },
        "candidate": {
            "strategy_code": "sma150",
            "strategy_version": candidate_strategy_version,
            "decision_policy_version": candidate_policy,
            "config_hash": candidate_config_hash,
            "verdict": candidate_verdict,
        },
        "disagreement_category": category,
        "outcome": {
            "calculation_version": "outcome.v1",
            "outcome_coverage_version": "shadow_pair_outcomes.v1",
            "forward_frame_version": "shadow_forward_bars.v1",
            "forward_provider": forward_provider,
            "outcome_status": status,
            "returns": {
                "1D": ret_1d, "3D": ret_3d, "5D": None, "10D": None, "20D": None
            },
            "max_favorable_excursion": mfe,
            "max_adverse_excursion": mae,
            "benchmark_returns": {
                "SPY": {
                    "1D": spy_1d, "3D": None, "5D": None, "10D": None, "20D": None
                },
                "QQQ": {
                    "1D": qqq_1d, "3D": None, "5D": None, "10D": None, "20D": None
                },
            },
        },
    }


class TestGroupingIdentity:
    def test_mandatory_fields_present(self):
        identity = grouping_identity(_row())
        for field in GROUPING_IDENTITY_FIELDS:
            assert field in identity

    def test_config_hashes_never_pool(self):
        a = _row(control_config_hash="cfg-a")
        b = _row(control_config_hash="cfg-b")
        groups = aggregate_pair_outcome_metrics([a, b])
        assert len(groups) == 2

    def test_policy_versions_never_pool(self):
        a = _row(candidate_policy="policy.a")
        b = _row(candidate_policy="policy.b")
        assert len(aggregate_pair_outcome_metrics([a, b])) == 2

    def test_strategy_versions_never_pool(self):
        a = _row(candidate_strategy_version="sma150.v3")
        b = _row(candidate_strategy_version="sma150.v3b")
        assert len(aggregate_pair_outcome_metrics([a, b])) == 2

    def test_provider_identities_never_pool(self):
        a = _row(forward_provider="massive")
        b = _row(forward_provider="fmp")
        assert len(aggregate_pair_outcome_metrics([a, b])) == 2


class TestNeutralRates:
    def test_positive_and_negative_return_rate(self):
        rows = [
            _row(ret_1d=2.0),
            _row(ret_1d=-1.0),
            _row(ret_1d=0.0),
        ]
        group = aggregate_pair_outcome_metrics(rows)[0]
        w1 = next(w for w in group["by_window"] if w["window"] == "1D")
        assert w1["sample_count"] == 3
        assert w1["positive_return_rate"] == pytest.approx(1 / 3)
        assert w1["negative_return_rate"] == pytest.approx(1 / 3)
        assert "win_rate" not in w1
        assert "win_rate" not in group

    def test_no_superiority_labels_in_payload(self):
        payload = json.dumps(aggregate_pair_outcome_metrics([_row()]))
        lowered = payload.lower()
        for forbidden in (
            "winner", "better", "improvement", "regression",
            "promote", "disable", "pass", "fail", "win_rate",
        ):
            # "pass" alone is too broad — check whole-word-ish via quotes.
            if forbidden in ("pass", "fail"):
                assert f'"{forbidden}"' not in lowered
            else:
                assert forbidden not in lowered


class TestActionResolution:
    def test_enter_arm_derived_from_control(self):
        assert derive_enter_arm("ENTER", "WATCH") == "control"
        assert derive_enter_arm("ENTER", "AVOID") == "control"

    def test_enter_arm_derived_from_candidate(self):
        assert derive_enter_arm("WATCH", "ENTER") == "candidate"
        assert derive_enter_arm("AVOID", "ENTER") == "candidate"

    def test_no_enter_arm_when_both_or_neither(self):
        assert derive_enter_arm("ENTER", "ENTER") is None
        assert derive_enter_arm("WATCH", "AVOID") is None

    @pytest.mark.parametrize("ret,window,expected", [
        (None, 1, "incomplete"),
        (0.5, 1, "flat_or_neutral"),   # exactly on band
        (0.51, 1, "enter_action_favorable"),
        (-0.5, 1, "flat_or_neutral"),
        (-0.51, 1, "non_enter_action_favorable"),
        (1.0, 5, "flat_or_neutral"),
        (1.01, 5, "enter_action_favorable"),
        (-1.01, 5, "non_enter_action_favorable"),
    ])
    def test_neutral_band_boundaries(self, ret, window, expected):
        assert classify_horizon_return(ret, window) == expected
        assert NEUTRAL_BANDS_PCT[1] == 0.5
        assert NEUTRAL_BANDS_PCT[5] == 1.0

    def test_action_divergent_group_is_resolvable(self):
        group = aggregate_pair_outcome_metrics([_row()])[0]
        assert group["action_resolvable"] is True
        assert group["enter_arm"] == "control"
        assert group["metrics_contract_version"] == METRICS_CONTRACT_VERSION
        res = next(r for r in group["resolution_by_window"] if r["window"] == "1D")
        assert res["enter_action_favorable_rate"] == 1.0
        assert "missed_upside_rate" in res
        assert "avoided_downside_rate" in res

    def test_watch_avoid_is_non_action_resolvable(self):
        row = _row(
            control_verdict="WATCH",
            candidate_verdict="AVOID",
            category="v2_watch_v3_avoid",
        )
        group = aggregate_pair_outcome_metrics([row])[0]
        assert group["action_resolvable"] is False
        assert group["classification"] == "policy_state_disagreement"
        assert "resolution_by_window" not in group
        assert "enter_arm" not in group

    def test_agreements_are_non_action_resolvable(self):
        row = _row(
            control_verdict="AVOID",
            candidate_verdict="AVOID",
            category="same_avoid",
        )
        group = aggregate_pair_outcome_metrics([row])[0]
        assert group["action_resolvable"] is False
        assert group["classification"] == "agreement"

    def test_spy_and_qqq_relative_metrics(self):
        group = aggregate_pair_outcome_metrics([_row(ret_1d=2.0, spy_1d=0.5, qqq_1d=0.3)])[0]
        w1 = next(w for w in group["by_window"] if w["window"] == "1D")
        assert w1["mean_spy_relative_return"] == pytest.approx(1.5)
        assert w1["mean_qqq_relative_return"] == pytest.approx(1.7)
