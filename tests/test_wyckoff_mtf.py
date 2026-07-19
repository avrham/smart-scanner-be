"""Phase 5 — deterministic Wyckoff MTF tests.

No live FMP or Supabase. Pure rule functions are tested directly; the strategy
and funnel integration use synthesized OHLCV frames.
"""

import asyncio

import numpy as np
import pandas as pd
import pytest

import app.workers.scanner.funnel as funnel
from app.workers.strategies import get_strategy, list_strategies
from app.workers.strategies.base import StrategyContext, StrategyDecision, StrategySide
from app.workers.strategies.wyckoff import DEFAULT_CONFIG, events, structure
from app.workers.strategies.wyckoff.strategy import WyckoffMTFStrategy
from app.workers.timeframes import (
    normalize_ohlcv,
    resample_to_monthly,
    resample_to_weekly,
)


# --------------------------------------------------------------------------- #
# Builders
# --------------------------------------------------------------------------- #

def _ohlcv(dates, closes, volumes=None, spread=0.5):
    closes = np.asarray(closes, dtype=float)
    n = len(closes)
    vol = np.full(n, 1_000_000.0) if volumes is None else np.asarray(volumes, dtype=float)
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "open": closes,
            "high": closes + spread,
            "low": closes - spread,
            "close": closes,
            "volume": vol,
        }
    )


def _monthly_frame(closes):
    dates = pd.date_range("2020-01-31", periods=len(closes), freq="ME")
    return _ohlcv(dates, closes, spread=2.0)


def _weekly_frame(closes):
    dates = pd.date_range("2022-01-07", periods=len(closes), freq="W-FRI")
    return _ohlcv(dates, closes, spread=1.0)


def _range_then_current(current_close, current_high, current_low, current_vol):
    """20 slow-oscillating range bars + one custom current bar (21 total)."""
    k = np.arange(20)
    band = 105.0 + 5.0 * np.sin(2 * np.pi * k / 10.0)  # 100..110, small daily change
    dates = list(pd.date_range("2023-01-02", periods=21, freq="B"))
    df = _ohlcv(dates[:20], band, spread=0.5)
    cur = pd.DataFrame(
        {
            "date": [pd.to_datetime(dates[20])],
            "open": [current_close],
            "high": [current_high],
            "low": [current_low],
            "close": [current_close],
            "volume": [current_vol],
        }
    )
    return pd.concat([df, cur], ignore_index=True)


_DAILY_SETUP_CONFIG = {
    "daily_range_lookback": 20,
    "atr_window": 5,
    "min_range_atr_multiple": 2.0,
    "pierce_atr_multiple": 0.1,
    "volume_sma_window": 5,
    "min_breakout_volume_ratio": 1.5,
}


def _wyckoff_daily(n=560):
    """A long uptrend that ends in a low-ATR consolidation + a volume breakout.

    Yields monthly LONG bias, weekly markup alignment, and a daily SOS setup.
    """
    dates = pd.date_range("2021-01-01", periods=n, freq="B")
    closes = np.empty(n, dtype=float)
    vols = np.full(n, 1_000_000.0)

    trend_end = n - 61
    for i in range(trend_end):
        closes[i] = 50.0 + i * 0.3
    # 60-bar consolidation band around 200 (slow oscillation -> low ATR).
    for j, i in enumerate(range(trend_end, n - 1)):
        closes[i] = 200.0 + 6.0 * np.sin(2 * np.pi * j / 30.0)
    # Breakout bar: closes well above the range high, with a volume surge.
    closes[n - 1] = 215.0
    vols[n - 1] = 3_000_000.0

    df = _ohlcv(dates, closes, volumes=vols, spread=0.5)
    df.loc[n - 1, "high"] = 215.5
    df.loc[n - 1, "low"] = 210.0
    return df


def _four_hour_long_trigger(lookback=10, n=20):
    dates = pd.date_range("2023-06-01", periods=n, freq="4h")
    closes = np.array([100.0 + i for i in range(n)], dtype=float)
    return _ohlcv(dates, closes, spread=0.5)


def _fmp_payload(df):
    """Convert an oldest-first OHLCV frame to FMP newest-first payload."""
    rows = []
    for _, r in df.iloc[::-1].iterrows():
        rows.append(
            {
                "date": pd.to_datetime(r["date"]).strftime("%Y-%m-%d"),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
            }
        )
    return {"historical": rows}


def _ctx(symbol="AAA", config=None, data_meta=None):
    return StrategyContext(
        symbol=symbol,
        pattern_code="wyckoff_mtf",
        config=config,
        scanner_mode="funnel",
        data_meta=data_meta,
    )


# --------------------------------------------------------------------------- #
# Timeframe utilities
# --------------------------------------------------------------------------- #

def test_resample_daily_to_weekly_preserves_ohlcv():
    # Two trading weeks (Mon-Fri).
    dates = list(pd.bdate_range("2023-01-02", "2023-01-13"))  # 10 business days
    closes = [10, 11, 12, 13, 14, 20, 19, 22, 18, 21]
    vols = [100, 100, 100, 100, 100, 200, 200, 200, 200, 200]
    daily = _ohlcv(dates, closes, volumes=vols, spread=1.0)

    weekly = resample_to_weekly(daily)
    assert len(weekly) == 2
    w1 = weekly.iloc[0]
    # open=first, close=last, high=max, low=min, volume=sum over week 1.
    assert w1["open"] == 10
    assert w1["close"] == 14
    assert w1["high"] == 14 + 1.0
    assert w1["low"] == 10 - 1.0
    assert w1["volume"] == 500


def test_resample_daily_to_monthly_counts_months():
    dates = pd.bdate_range("2023-01-02", "2023-03-31")
    closes = np.arange(len(dates), dtype=float) + 50.0
    monthly = resample_to_monthly(_ohlcv(dates, closes))
    assert len(monthly) == 3  # Jan, Feb, Mar
    assert monthly.iloc[0]["close"] < monthly.iloc[-1]["close"]


def test_normalize_handles_empty():
    assert normalize_ohlcv(None).empty
    assert normalize_ohlcv(pd.DataFrame()).empty


# --------------------------------------------------------------------------- #
# Monthly bias
# --------------------------------------------------------------------------- #

def test_monthly_bias_long():
    bias, comp = structure.monthly_bias(_monthly_frame(np.arange(30) + 100.0), DEFAULT_CONFIG)
    assert bias == structure.LONG
    assert comp["monthly_sma_slope_pct"] > 0
    assert comp["monthly_bias_quality"] > 0


def test_monthly_bias_short():
    bias, _ = structure.monthly_bias(_monthly_frame(200.0 - np.arange(30)), DEFAULT_CONFIG)
    assert bias == structure.SHORT


def test_monthly_bias_neutral_when_flat():
    bias, _ = structure.monthly_bias(_monthly_frame(np.full(30, 100.0)), DEFAULT_CONFIG)
    assert bias == structure.NEUTRAL


def test_monthly_bias_neutral_when_insufficient():
    bias, comp = structure.monthly_bias(_monthly_frame(np.arange(10) + 100.0), DEFAULT_CONFIG)
    assert bias == structure.NEUTRAL
    assert comp.get("monthly_insufficient") is True


# --------------------------------------------------------------------------- #
# Weekly alignment
# --------------------------------------------------------------------------- #

def test_weekly_alignment_long_pass():
    aligned, phase, comp = structure.weekly_alignment(
        _weekly_frame(np.arange(40) + 100.0), structure.LONG, DEFAULT_CONFIG
    )
    assert aligned is True
    assert phase == structure.PHASE_MARKUP
    assert comp["weekly_alignment_quality"] > 0


def test_weekly_alignment_fails_when_trend_disagrees():
    # Rising weekly trend but monthly bias SHORT -> not aligned.
    aligned, _, _ = structure.weekly_alignment(
        _weekly_frame(np.arange(40) + 100.0), structure.SHORT, DEFAULT_CONFIG
    )
    assert aligned is False


# --------------------------------------------------------------------------- #
# Daily setup detection
# --------------------------------------------------------------------------- #

def test_daily_spring_detected():
    df = _range_then_current(current_close=102.0, current_high=102.5, current_low=98.0, current_vol=1_000_000.0)
    out = events.detect_daily_setup(df, "LONG", _DAILY_SETUP_CONFIG)
    assert out["setup_type"] == events.SETUP_SPRING
    assert out["daily_setup_quality"] == 1.0


def test_daily_sos_detected_with_volume():
    df = _range_then_current(current_close=113.0, current_high=113.5, current_low=110.0, current_vol=3_000_000.0)
    out = events.detect_daily_setup(df, "LONG", _DAILY_SETUP_CONFIG)
    assert out["setup_type"] == events.SETUP_SOS


def test_daily_utad_detected():
    df = _range_then_current(current_close=108.0, current_high=113.0, current_low=107.5, current_vol=1_000_000.0)
    out = events.detect_daily_setup(df, "SHORT", _DAILY_SETUP_CONFIG)
    assert out["setup_type"] == events.SETUP_UTAD


def test_daily_sow_detected_with_volume():
    df = _range_then_current(current_close=97.0, current_high=100.0, current_low=96.5, current_vol=3_000_000.0)
    out = events.detect_daily_setup(df, "SHORT", _DAILY_SETUP_CONFIG)
    assert out["setup_type"] == events.SETUP_SOW


def test_daily_setup_none_when_inside_range():
    df = _range_then_current(current_close=105.0, current_high=105.5, current_low=104.5, current_vol=1_000_000.0)
    out = events.detect_daily_setup(df, "LONG", _DAILY_SETUP_CONFIG)
    assert out["setup_type"] == events.SETUP_NONE


def test_long_side_never_returns_bearish_setup():
    # A close below the range on the LONG side must NOT become sow/breakdown.
    df = _range_then_current(current_close=97.0, current_high=100.0, current_low=96.5, current_vol=3_000_000.0)
    out = events.detect_daily_setup(df, "LONG", _DAILY_SETUP_CONFIG)
    assert out["setup_type"] == events.SETUP_NONE


# --------------------------------------------------------------------------- #
# 4H trigger
# --------------------------------------------------------------------------- #

def test_four_hour_trigger_long():
    out = events.four_hour_trigger(_four_hour_long_trigger(), "LONG", DEFAULT_CONFIG)
    assert out is not None
    assert out["triggered"] is True
    assert out["entry_price"] is not None
    assert out["stop_price"] is not None


def test_four_hour_trigger_none_when_missing():
    assert events.four_hour_trigger(None, "LONG", DEFAULT_CONFIG) is None


# --------------------------------------------------------------------------- #
# Strategy-level decisions
# --------------------------------------------------------------------------- #

def test_strategy_watch_when_no_4h():
    res = get_strategy("wyckoff_mtf").evaluate(_wyckoff_daily(), _ctx())
    assert res.decision == StrategyDecision.WATCH
    assert res.side == StrategySide.LONG
    assert res.setup_type == events.SETUP_SOS
    # No trigger -> nothing invented.
    assert res.entry_price is None
    assert res.stop_price is None
    # score is a decomposed structure score in 0..1 (no fake 0-100).
    assert 0.0 <= res.score <= 1.0
    assert res.details["side"] == "LONG"


def test_strategy_enter_when_4h_trigger_and_enabled():
    cfg = get_strategy("wyckoff_mtf").default_config()
    cfg["enable_4h_trigger"] = True
    res = get_strategy("wyckoff_mtf").evaluate(
        _wyckoff_daily(), _ctx(config=cfg, data_meta={"df_4h": _four_hour_long_trigger()})
    )
    assert res.decision == StrategyDecision.ENTER
    assert res.side == StrategySide.LONG
    assert res.entry_price is not None
    assert res.stop_price is not None
    assert res.details["entry_price"] == res.entry_price


def test_strategy_rejects_flat_as_monthly_neutral():
    dates = pd.date_range("2021-01-01", periods=560, freq="B")
    flat = _ohlcv(dates, np.full(560, 100.0))
    res = get_strategy("wyckoff_mtf").evaluate(flat, _ctx())
    assert res.decision == StrategyDecision.REJECT
    assert res.rejection_reason == "monthly_neutral"


def test_strategy_rejects_insufficient_daily():
    dates = pd.date_range("2023-01-01", periods=100, freq="B")
    res = get_strategy("wyckoff_mtf").evaluate(_ohlcv(dates, np.arange(100) + 50.0), _ctx())
    assert res.decision == StrategyDecision.REJECT
    assert res.rejection_reason == "insufficient_daily_data"


def test_strategy_score_components_are_raw():
    res = get_strategy("wyckoff_mtf").evaluate(_wyckoff_daily(), _ctx())
    sc = res.score_components
    # Raw measured values are present; no opaque fabricated confidence field.
    for key in ("monthly_close_vs_sma_pct", "weekly_sma_slope_pct", "daily_range_high", "daily_volume_ratio"):
        assert key in sc
    assert "confidence" not in sc


# --------------------------------------------------------------------------- #
# Registry + funnel integration
# --------------------------------------------------------------------------- #

def test_registry_includes_wyckoff():
    assert "wyckoff_mtf" in list_strategies()
    assert isinstance(get_strategy("wyckoff_mtf"), WyckoffMTFStrategy)


class _FakeFMP:
    def __init__(self, payloads):
        self._payloads = payloads
        self.requested = None
        self.timeseries = None

    async def batch_historical_data(self, symbols, timeseries=350):
        self.requested = list(symbols)
        self.timeseries = timeseries
        return {s: self._payloads.get(s, {"historical": []}) for s in symbols}


def _async(value):
    async def _f(*a, **k):
        return value
    return _f


def test_funnel_evaluates_wyckoff_via_registry_watch(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()
    monkeypatch.setattr(funnel, "resolve_pattern_config", _async(cfg))
    monkeypatch.setattr(
        funnel, "get_universe_tickers",
        _async([
            {"symbol": "AAA", "market_cap": 5e9, "last_volume": 1e6},
            {"symbol": "BBB", "market_cap": 4e9, "last_volume": 1e6},
        ]),
    )
    monkeypatch.setattr(funnel, "log_pattern_run", _async("run-id"))
    monkeypatch.setattr(funnel, "was_seen_today", _async(False))
    monkeypatch.setattr(funnel, "mark_seen_today", _async(None))

    saved = []

    async def fake_save(**kwargs):
        saved.append(kwargs["symbol"])
        return "sig-id"

    monkeypatch.setattr(funnel, "save_signal", fake_save)

    payload = _fmp_payload(_wyckoff_daily())
    fake_fmp = _FakeFMP({"AAA": payload, "BBB": payload})

    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=fake_fmp, pattern_code="wyckoff_mtf", dry_run=False)
    )

    sc = summary["stage_counts"]
    # History fetch depth was sized to wyckoff's deep-history requirement.
    assert fake_fmp.timeseries == max(350, WyckoffMTFStrategy.min_daily_bars + 60)
    assert set(fake_fmp.requested) == {"AAA", "BBB"}
    # No 4H available in the funnel -> WATCH, never ENTER. Phase 5.2: WATCH
    # candidates are persisted (with decision cards) by default.
    assert sc["stage_3_evaluated"] == 2
    assert sc["watch_count"] == 2
    assert sc["enter_count"] == 0
    assert sc["watch_saved_count"] == 2
    assert sorted(saved) == ["AAA", "BBB"]


def test_funnel_dry_run_wyckoff_makes_no_fmp_calls(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()
    monkeypatch.setattr(funnel, "resolve_pattern_config", _async(cfg))
    monkeypatch.setattr(
        funnel, "get_universe_tickers",
        _async([{"symbol": "AAA", "market_cap": 5e9, "last_volume": 1e6}]),
    )
    monkeypatch.setattr(funnel, "log_pattern_run", _async("run-id"))

    class _RaisingFMP:
        async def batch_historical_data(self, *a, **k):
            raise AssertionError("FMP must not be called in dry_run")

    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=_RaisingFMP(), pattern_code="wyckoff_mtf", dry_run=True)
    )
    assert summary["dry_run"] is True
    assert summary["stage_counts"]["stage_3_evaluated"] == 0
