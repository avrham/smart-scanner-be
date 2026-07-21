"""Phase 8: sma150.v3 ranking score.

The score orders candidates; it never authorizes ENTER, never hides a failed
gate, and is never labelled a probability.
"""

import json

import pytest

from app.workers.strategies import get_strategy
from app.workers.strategies.base import StrategyContext, StrategyDecision
from app.workers.strategies.sma150_v3 import (
    DEFAULT_CONFIG,
    RANKING_COMPONENTS,
    RANKING_VERSION,
    _proximity_quality,
    _trend_quality,
    _trigger_quality,
    bounce_recency_quality,
)
from tests.sma150_v3_frames import build_jbl_like_frame, build_uptrend_frame

CFG = {
    "max_close_above_sma_pct": 3.0,
    "max_close_below_sma_pct": 1.0,
    "trend_quality_full_scale_slope_pct": 2.0,
}


def _evaluate(df):
    strategy = get_strategy("sma150_bounce_v3")
    return strategy.evaluate(
        df, StrategyContext(symbol="TEST", pattern_code="sma150_bounce_v3",
                            config=strategy.default_config())
    )


class TestNormalizationFormulas:
    def test_proximity_quality_bounds_and_shape(self):
        assert _proximity_quality(0.0, CFG) == 1.0
        assert _proximity_quality(3.0, CFG) == 0.0
        assert _proximity_quality(1.5, CFG) == pytest.approx(0.5)
        assert _proximity_quality(-1.0, CFG) == 0.0
        assert _proximity_quality(-0.5, CFG) == pytest.approx(0.5)
        assert _proximity_quality(10.0, CFG) == 0.0   # clamped
        assert _proximity_quality(-10.0, CFG) == 0.0

    def test_trend_quality_bounds(self):
        assert _trend_quality(-1.0, CFG) == 0.0
        assert _trend_quality(0.0, CFG) == 0.0
        assert _trend_quality(1.0, CFG) == pytest.approx(0.5)
        assert _trend_quality(2.0, CFG) == 1.0
        assert _trend_quality(50.0, CFG) == 1.0       # clamped

    def test_trigger_quality_continuous_and_unknown(self):
        assert _trigger_quality(None, True, 0.9, 0.65) is None
        assert _trigger_quality(True, True, 0.65, 0.65) == 1.0
        partial = _trigger_quality(False, True, 0.325, 0.65)
        assert 0.0 < partial < 1.0
        # Zero-range bar contributes 0, never a fabricated pass.
        assert _trigger_quality(True, True, None, 0.65) == pytest.approx(2 / 3)

    def test_recency_quality_decay(self):
        assert bounce_recency_quality(0, 126) == 1.0
        assert bounce_recency_quality(126, 126) == pytest.approx(0.5)
        assert bounce_recency_quality(252, 126) == pytest.approx(0.25)
        assert bounce_recency_quality(None, 126) is None


class TestScoreProperties:
    def test_all_components_within_bounds(self):
        for df in (build_uptrend_frame(trigger=True, vol_ratio=1.30),
                   build_uptrend_frame(trigger=False, vol_ratio=1.07),
                   build_jbl_like_frame()):
            result = _evaluate(df)
            components = result.details["ranking"]["components"]
            assert set(components) == set(RANKING_COMPONENTS)
            for name, value in components.items():
                if value is not None:
                    assert 0.0 <= value <= 1.0, name

    def test_score_is_continuous_mean_of_components(self):
        result = _evaluate(build_uptrend_frame(trigger=False, vol_ratio=1.07))
        components = result.details["ranking"]["components"]
        expected = sum(components[n] for n in RANKING_COMPONENTS) / len(
            RANKING_COMPONENTS
        )
        assert result.details["ranking"]["score"] == pytest.approx(
            expected, abs=1e-6
        )
        assert 0.0 < result.details["ranking"]["score"] < 1.0

    def test_score_does_not_authorize_enter(self):
        # High score with a missing trigger remains WATCH.
        result = _evaluate(build_uptrend_frame(trigger=False, vol_ratio=1.30))
        assert result.decision == StrategyDecision.WATCH
        assert result.details["ranking"]["score"] > 0.5

    def test_high_score_cannot_override_negative_trend(self):
        result = _evaluate(build_jbl_like_frame())
        assert result.decision == StrategyDecision.WATCH
        assert "sma_slope" in result.details["failed_confirmations"]

    def test_score_null_when_not_computable(self):
        small = build_uptrend_frame().iloc[-100:].reset_index(drop=True)
        result = _evaluate(small)
        assert result.score is None
        assert result.details["ranking"]["score"] is None
        # Missing components stay visible as None, not fabricated zeros.
        assert all(
            v is None
            for v in result.details["ranking"]["components"].values()
        )

    def test_v2_score_threshold_not_reused(self):
        # v3 config has no score_threshold key; the v2 gate is not part of v3.
        assert "score_threshold" not in DEFAULT_CONFIG
        strategy = get_strategy("sma150_bounce_v3")
        assert "score_threshold" not in strategy.default_config()

    def test_score_never_labelled_probability(self):
        result = _evaluate(build_uptrend_frame())
        text = json.dumps(result.details).lower()
        assert "probability" not in text
        assert result.details["ranking"]["ranking_version"] == RANKING_VERSION
