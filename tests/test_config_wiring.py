"""Changing config must change evaluation behavior (B1)."""

from app.workers.patterns.sma150 import evaluate_sma150_bounce, DEFAULT_CONFIG
from tests.helpers import make_ohlcv


def test_config_changes_verdict():
    df = make_ohlcv(320, price=100.0)

    # Strict default config: a flat series has 0 bounces => AVOID.
    strict = evaluate_sma150_bounce("TEST", df, DEFAULT_CONFIG.copy())

    # Lenient config: accept zero bounces and any score => ENTER for the same
    # data. This proves the evaluator honors the injected config.
    lenient = DEFAULT_CONFIG.copy()
    lenient["min_bounces"] = 0
    lenient["score_threshold"] = 0.0
    loose = evaluate_sma150_bounce("TEST", df, lenient)

    assert strict["verdict"] == "AVOID"
    assert loose["verdict"] == "ENTER"
    assert strict["verdict"] != loose["verdict"]


def test_thresholds_used_reflects_injected_config():
    df = make_ohlcv(320, price=100.0)
    cfg = DEFAULT_CONFIG.copy()
    cfg["touch_tolerance_pct"] = 1.23
    cfg["score_threshold"] = 0.77
    result = evaluate_sma150_bounce("TEST", df, cfg)
    thresholds = result["details"]["thresholds_used"]
    assert thresholds["touch_tolerance_pct"] == 1.23
    assert thresholds["score_threshold"] == 0.77
