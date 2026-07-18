"""build_outcome_from_frames: end-to-end pure record building from DataFrames.

Covers the calculated path, benchmark alignment, missing entry date, and
no-future-bars (insufficient data) handling.
"""

from datetime import date, datetime

import pandas as pd
import pytest

from app.workers.outcomes.service import build_outcome_from_frames


def _frame(start: str, closes, base_high=1.0, base_low=1.0):
    n = len(closes)
    dates = pd.date_range(start, periods=n, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": [c + base_high for c in closes],
            "low": [c - base_low for c in closes],
            "close": closes,
            "volume": [1_000_000] * n,
        }
    )


def _signal(snapshot: date, details=None):
    return {
        "signal_id": "sig-1",
        "symbol": "TEST",
        "pattern_code": "sma150_bounce",
        "snapshot_date": snapshot,
        "created_at": datetime(2023, 1, 5, 12, 0, 0),
        "details": details or {},
    }


def test_calculated_outcome_with_benchmarks():
    closes = [100, 100, 100, 100, 100, 101, 102, 103, 104, 105, 106, 107]
    symbol_df = _frame("2023-01-01", closes)
    spy_closes = [200, 200, 200, 200, 200, 202, 204, 206, 208, 210, 212, 214]
    spy_df = _frame("2023-01-01", spy_closes)

    signal = _signal(date(2023, 1, 5))  # index 4, entry close = 100
    rec = build_outcome_from_frames(signal, symbol_df, {"SPY": spy_df, "QQQ": None})

    assert rec["outcome_status"] == "calculated"
    assert rec["entry_price"] == pytest.approx(100.0)
    assert rec["side"] == "LONG"
    assert rec["ret_by_window"][1] == pytest.approx(1.0)
    assert rec["ret_by_window"][3] == pytest.approx(3.0)
    assert rec["ret_by_window"][5] == pytest.approx(5.0)
    assert rec["ret_by_window"][10] is None  # only 7 forward bars
    # same-ticker buy&hold mirrors LONG returns here
    assert rec["same_ticker_buy_hold"]["1D"] == pytest.approx(1.0)
    # benchmark aligned by date
    assert rec["benchmark_returns"]["SPY"]["1D"] == pytest.approx(1.0)
    assert rec["benchmark_returns"]["QQQ"]["1D"] is None
    # MFE uses highs (close+1): best is 107+1 => +8%
    assert rec["max_favorable_excursion"] == pytest.approx(8.0)
    # no stop/target for sma150 => nullable derived fields
    assert rec["hit_stop"] is None
    assert rec["hit_target"] is None
    assert rec["simulated_r"] is None


def test_missing_snapshot_date_is_insufficient():
    symbol_df = _frame("2023-01-01", [100] * 12)
    signal = _signal(date(2020, 1, 1))  # not present in the frame
    rec = build_outcome_from_frames(signal, symbol_df, {})
    assert rec["outcome_status"] == "insufficient_data"
    assert rec["entry_price"] is None


def test_no_future_bars_is_insufficient():
    closes = [100, 101, 102, 103, 104]
    symbol_df = _frame("2023-01-01", closes)
    signal = _signal(date(2023, 1, 5))  # last bar => no forward bars
    rec = build_outcome_from_frames(signal, symbol_df, {})
    assert rec["outcome_status"] == "insufficient_data"
    assert rec["entry_price"] == pytest.approx(104.0)


def test_none_symbol_frame_is_insufficient():
    signal = _signal(date(2023, 1, 5))
    rec = build_outcome_from_frames(signal, None, {})
    assert rec["outcome_status"] == "insufficient_data"
    assert rec["entry_price"] is None


def test_short_signal_uses_side_from_details():
    closes = [100, 100, 100, 100, 100, 99, 98, 97, 96, 95, 94, 93]
    symbol_df = _frame("2023-01-01", closes)
    signal = _signal(date(2023, 1, 5), details={"side": "SHORT"})
    rec = build_outcome_from_frames(signal, symbol_df, {})
    assert rec["side"] == "SHORT"
    # price fell 1% after 1 day => SHORT gains +1%
    assert rec["ret_by_window"][1] == pytest.approx(1.0)
