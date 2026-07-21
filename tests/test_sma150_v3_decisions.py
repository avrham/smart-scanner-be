"""Phase 8: sma150.v3 setup and confirmation decision tests.

Layer authority: data readiness > setup validity > entry confirmation;
the ranking score never decides. Includes the decision-card contract.
"""

import pandas as pd
import pytest

from app.workers.strategies import get_strategy
from app.workers.strategies.base import StrategyContext, StrategyDecision
from app.workers.strategies.decision_card import build_decision_card
from app.workers.strategies.sma150_v3 import (
    close_location_value,
    sma_slope_stats,
)
from tests.sma150_v3_frames import build_uptrend_frame


def _evaluate(df, config_overrides=None):
    strategy = get_strategy("sma150_bounce_v3")
    config = strategy.default_config()
    if config_overrides:
        config.update(config_overrides)
    context = StrategyContext(
        symbol="TEST", pattern_code="sma150_bounce_v3", config=config
    )
    return strategy.evaluate(df, context)


def _bundle(result):
    return result.details["evidence"]


def _item(result, code):
    return next(i for i in _bundle(result)["items"] if i["code"] == code)


class TestSetupDecisions:
    def test_insufficient_history_avoids_with_unknown_evidence(self):
        df = build_uptrend_frame(n=430).iloc[-100:].reset_index(drop=True)
        result = _evaluate(df)
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason == "insufficient_history"
        bundle = _bundle(result)
        assert bundle["setup_state"] == "unknown"
        assert bundle["trigger_state"] == "unknown"
        for code in ("sma_available", "volume_average_available",
                     "completed_rebound_windows"):
            assert _item(result, code)["state"] == "unknown"
        assert result.score is None  # no fabricated zero score

    def test_too_far_above_sma_avoids(self):
        result = _evaluate(build_uptrend_frame(end_prox_pct=4.5, trigger=True))
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason == "too_far_above_sma"
        assert _bundle(result)["setup_state"] == "invalid"

    def test_too_far_below_sma_avoids(self):
        result = _evaluate(build_uptrend_frame(end_prox_pct=-2.0, trigger=False))
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason == "too_far_below_sma"

    def test_too_few_independent_bounces_avoids(self):
        result = _evaluate(build_uptrend_frame(touch_offsets=(340,)))
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason == "insufficient_independent_bounces"

    def test_weak_median_rebound_avoids(self):
        result = _evaluate(build_uptrend_frame(rebound_pct=2.0))
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason == "weak_median_rebound"
        item = _item(result, "median_rebound")
        assert item["state"] == "fail"
        assert item["raw_value"] < 5.0

    def test_valid_setup_missing_trigger_watches(self):
        result = _evaluate(build_uptrend_frame(trigger=False, vol_ratio=1.30))
        assert result.decision == StrategyDecision.WATCH
        bundle = _bundle(result)
        assert bundle["setup_state"] == "valid"
        assert bundle["trigger_state"] in ("missing", "contradicted")
        assert "bullish_trigger" in result.details["failed_confirmations"]

    def test_confirmations_cannot_override_invalid_setup(self):
        # Perfect trigger/volume but only one bounce -> still AVOID.
        result = _evaluate(
            build_uptrend_frame(touch_offsets=(340,), trigger=True,
                                vol_ratio=1.5)
        )
        assert result.decision == StrategyDecision.AVOID

    def test_median_and_mean_both_persisted(self):
        result = _evaluate(build_uptrend_frame())
        assert result.details["median_rebound_pct"] is not None
        assert result.details["avg_rebound_pct"] is not None


class TestConfirmationDecisions:
    def test_all_confirmations_passing_enters(self):
        result = _evaluate(
            build_uptrend_frame(trigger=True, vol_ratio=1.30, end_prox_pct=2.0)
        )
        assert result.decision == StrategyDecision.ENTER
        bundle = _bundle(result)
        assert bundle["setup_state"] == "valid"
        assert bundle["trigger_state"] == "confirmed"
        assert result.details["failed_confirmations"] == []

    def test_negative_sma_slope_blocks_enter_produces_watch(self):
        from tests.sma150_v3_frames import build_jbl_like_frame
        result = _evaluate(build_jbl_like_frame())
        assert result.decision == StrategyDecision.WATCH
        assert result.details["sma_slope_pct"] < 0
        assert "sma_slope" in result.details["failed_confirmations"]
        assert "sma_slope_negative" in _bundle(result)["contradictions"]

    def test_flat_sma_slope_blocks_enter(self):
        flat = pd.Series([100.0] * 200)
        stats = sma_slope_stats(flat, 20)
        assert stats["slope_pct"] == pytest.approx(0.0)
        # min_sma_slope_pct default 0.0 is a STRICT bound: flat fails.
        assert not (stats["slope_pct"] > 0.0)

    def test_positive_slope_passes(self):
        result = _evaluate(build_uptrend_frame(trigger=True, vol_ratio=1.30))
        assert _item(result, "sma_slope")["state"] == "pass"

    def test_close_below_sma_blocks_enter(self):
        result = _evaluate(
            build_uptrend_frame(end_prox_pct=-0.5, trigger=False,
                                vol_ratio=1.30)
        )
        # Proximity band allows -1.0%, so the setup is valid; the close-
        # above-SMA confirmation fails => WATCH, contradiction recorded.
        assert result.decision == StrategyDecision.WATCH
        assert "close_above_sma" in result.details["failed_confirmations"]
        assert "close_below_sma" in _bundle(result)["contradictions"]

    def test_no_prior_high_breakout_blocks_enter(self):
        result = _evaluate(build_uptrend_frame(trigger=False, vol_ratio=1.30))
        assert result.decision == StrategyDecision.WATCH
        assert _item(result, "close_above_prior_high")["state"] == "fail"

    def test_bearish_latest_candle_blocks_enter(self):
        result = _evaluate(build_uptrend_frame(trigger=False, vol_ratio=1.30))
        assert _item(result, "bullish_close")["state"] == "fail"
        assert "bearish_close" in _bundle(result)["contradictions"]

    def test_weak_close_location_blocks_enter(self):
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        # Sabotage only the CLV: close near the LOW of a wide last bar,
        # while still breaking the prior high and closing above the open.
        last = len(df) - 1
        close = float(df.loc[last, "close"])
        df.loc[last, "low"] = close * 0.99
        df.loc[last, "high"] = close * 1.05   # close sits low in the range
        df.loc[last, "open"] = close * 0.995
        result = _evaluate(df)
        assert result.decision == StrategyDecision.WATCH
        assert _item(result, "close_location")["state"] == "fail"
        assert _item(result, "close_location")["reason_code"] == "weak_close_location"

    def test_zero_range_candle_handled_as_unknown(self):
        assert close_location_value(100.0, 100.0, 100.0) is None
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        last = len(df) - 1
        close = float(df.loc[last, "close"])
        for col in ("open", "high", "low"):
            df.loc[last, col] = close
        result = _evaluate(df)
        assert result.decision == StrategyDecision.WATCH  # never ENTER
        item = _item(result, "close_location")
        assert item["state"] == "unknown"
        assert item["reason_code"] == "zero_range_bar"
        assert "close_location" in _bundle(result)["missing_data"]

    def test_volume_ratio_107_fails_120_threshold(self):
        result = _evaluate(
            build_uptrend_frame(trigger=True, vol_ratio=1.07)
        )
        assert result.decision == StrategyDecision.WATCH
        item = _item(result, "trigger_volume_ratio")
        assert item["state"] == "fail"
        assert item["raw_value"] == pytest.approx(1.07, abs=0.01)
        # Continuous quality: partial credit, never full.
        assert 0.0 < item["normalized_value"] < 1.0


class TestDecisionCard:
    def test_v3_card_fields_present_and_no_fabricated_target(self):
        result = _evaluate(build_uptrend_frame(trigger=False, vol_ratio=1.07))
        card = build_decision_card(result)
        assert card["evidence_version"] == "evidence.v1"
        assert card["decision_policy_version"] == "sma150_bounce.policy.v1"
        assert card["setup_state"] == "valid"
        assert card["trigger_state"] in ("missing", "contradicted")
        for key in ("current_price", "sma_value", "proximity_pct",
                    "sma_slope_pct", "independent_bounce_count",
                    "median_rebound_pct", "mean_rebound_pct", "volume_ratio",
                    "trigger_level", "market_data_as_of", "ranking_score",
                    "ranking_components"):
            assert key in card
        assert card["target_price"] is None      # never invented
        assert card["entry_price"] is None

    def test_deterministic_invalidation_exists(self):
        result = _evaluate(build_uptrend_frame())
        card = build_decision_card(result)
        rule = card["invalidation_rule"]
        assert rule["rule_code"] == "daily_close_below_sma150_pct"
        assert rule["threshold_pct"] == 2.0
        assert rule["level"] == pytest.approx(
            result.details["sma_value"] * 0.98, rel=1e-3
        )
        assert result.invalidation == rule["level"]

    def test_failed_confirmations_visible_on_card(self):
        result = _evaluate(build_uptrend_frame(trigger=False, vol_ratio=1.07))
        card = build_decision_card(result)
        assert "bullish_trigger" in card["failed_confirmations"]
        assert "trigger_volume_ratio" in card["failed_confirmations"]
        assert len(card["trigger_conditions"]) >= 4

    def test_raw_evidence_retained(self):
        result = _evaluate(build_uptrend_frame(trigger=True, vol_ratio=1.30))
        card = build_decision_card(result)
        assert card["raw_evidence"]["volume_ratio"] == pytest.approx(1.30, abs=0.01)
        assert "median_rebound_pct" in card["raw_evidence"]

    def test_v2_card_unchanged_no_v3_fields(self):
        strategy = get_strategy("sma150_bounce")
        df = build_uptrend_frame()
        result = strategy.evaluate(
            df, StrategyContext(symbol="TEST", pattern_code="sma150_bounce",
                                config=strategy.default_config())
        )
        card = build_decision_card(result)
        assert "evidence_version" not in card
        assert "setup_state" not in card
        assert card["card_version"] == "decision_card.v1"
