"""Phase 4 strategy interface + registry + sma150 adapter tests.

Pure/deterministic. No live FMP or Supabase. The adapter-equivalence tests call
the existing `evaluate_sma150_bounce` directly and via the adapter on the SAME
DataFrame/config and assert the outputs are identical, proving the wrapper does
not change behavior.
"""

import numpy as np
import pandas as pd
import pytest

from app.workers.patterns.sma150 import (
    DEFAULT_CONFIG,
    SCORE_VERSION,
    evaluate_sma150_bounce,
)
from app.workers.strategies import (
    UnknownStrategyError,
    get_strategy,
    list_strategies,
)
from app.workers.strategies.base import (
    StrategyContext,
    StrategyDecision,
    StrategyResult,
    StrategySide,
)
from app.workers.strategies.sma150_adapter import Sma150BounceStrategy


def _make_df(n: int = 360, base: float = 50.0) -> pd.DataFrame:
    """Deterministic OHLCV frame long enough to reach the full scoring path."""
    dates = pd.date_range("2022-01-01", periods=n, freq="D")
    close = base + np.sin(np.arange(n) / 5.0) * 3.0 + np.arange(n) * 0.01
    return pd.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": np.full(n, 1_000_000.0),
        }
    )


def _ctx(symbol="AAA", config=None):
    return StrategyContext(
        symbol=symbol, pattern_code="sma150_bounce", config=config, scanner_mode="funnel"
    )


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

def test_registry_returns_sma150_strategy():
    strat = get_strategy("sma150_bounce")
    assert isinstance(strat, Sma150BounceStrategy)
    assert strat.pattern_code == "sma150_bounce"
    assert strat.required_timeframes == ["1d"]
    assert strat.version == SCORE_VERSION
    assert "sma150_bounce" in list_strategies()


def test_unknown_strategy_raises_clean_error():
    with pytest.raises(UnknownStrategyError) as exc:
        get_strategy("does_not_exist")
    # Clear, actionable message that names the unknown code.
    assert "does_not_exist" in str(exc.value)
    # Subclass of KeyError so existing except-KeyError call sites still catch it.
    assert isinstance(exc.value, KeyError)


# --------------------------------------------------------------------------- #
# Adapter equivalence (does NOT change sma150 behavior)
# --------------------------------------------------------------------------- #

def test_adapter_matches_legacy_evaluator_full_path():
    df = _make_df()
    config = DEFAULT_CONFIG.copy()

    raw = evaluate_sma150_bounce("AAA", df, config)
    res = get_strategy("sma150_bounce").evaluate(df, _ctx("AAA", config))

    assert isinstance(res, StrategyResult)
    assert res.decision.value == raw["verdict"]
    assert res.score == raw["score"]
    assert res.reason == raw["reason"]
    # details persisted verbatim (byte-identical payload for UI + outcomes).
    assert res.details == raw["details"]
    assert res.score_components == raw["details"].get("score_components", {})


def test_adapter_defaults_to_default_config_when_none():
    df = _make_df()
    raw = evaluate_sma150_bounce("AAA", df, None)
    res = get_strategy("sma150_bounce").evaluate(df, _ctx("AAA", config=None))
    assert res.decision.value == raw["verdict"]
    assert res.score == raw["score"]
    assert res.details == raw["details"]


def test_adapter_passes_config_through():
    """A config override must flow through the adapter into evaluation."""
    df = _make_df()
    config = DEFAULT_CONFIG.copy()
    config["min_price"] = 1e9  # force a price_below_min rejection

    res = get_strategy("sma150_bounce").evaluate(df, _ctx("AAA", config))
    assert res.decision == StrategyDecision.AVOID
    assert res.rejection_reason == "price_below_min"


def test_score_components_remain_raw():
    df = _make_df()
    res = get_strategy("sma150_bounce").evaluate(df, _ctx("AAA", DEFAULT_CONFIG.copy()))
    sc = res.score_components
    # Raw measured values only; never a weighted/derived "score" field (B2).
    assert "proximity_to_sma150_pct" in sc
    assert "score" not in sc
    assert "weighted_score" not in sc


# --------------------------------------------------------------------------- #
# Nothing invented for sma150
# --------------------------------------------------------------------------- #

def test_adapter_does_not_invent_side_stop_target():
    df = _make_df()
    res = get_strategy("sma150_bounce").evaluate(df, _ctx("AAA", DEFAULT_CONFIG.copy()))

    # Long-only rebound semantics documented at the interface level...
    assert res.side == StrategySide.LONG
    # ...but no stop/target/invalidation/entry are fabricated.
    assert res.entry_price is None
    assert res.stop_price is None
    assert res.target_price is None
    assert res.invalidation is None
    # And the persisted details are NOT mutated with side/stop/target.
    assert "side" not in res.details
    assert "stop_price" not in res.details
    assert "target_price" not in res.details


def test_result_verdict_property_maps_decision():
    r = StrategyResult(
        decision=StrategyDecision.ENTER, symbol="X", pattern_code="sma150_bounce"
    )
    assert r.verdict == "ENTER"
    assert r.is_actionable() is True
    r2 = StrategyResult(
        decision=StrategyDecision.AVOID, symbol="X", pattern_code="sma150_bounce"
    )
    assert r2.verdict == "AVOID"
    assert r2.is_actionable() is False


# --------------------------------------------------------------------------- #
# Legacy path still exists
# --------------------------------------------------------------------------- #

def test_legacy_scan_path_still_exists():
    import app.workers.scan_runner as scan_runner

    # Legacy scanner is preserved and still calls sma150 directly (unchanged).
    assert hasattr(scan_runner, "run_scan_batch")
    assert scan_runner.evaluate_sma150_bounce is evaluate_sma150_bounce
