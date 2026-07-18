"""Aggregation metrics: sample size, win rate, profit factor, baseline deltas."""

import pytest

from app.workers.outcomes.metrics import (
    aggregate_outcomes,
    group_and_aggregate,
)


def _rec(ret5, same5, spy5, r, mfe, mae, side="LONG", pattern="sma150_bounce"):
    return {
        "side": side,
        "pattern_code": pattern,
        "symbol": "TEST",
        "ret_by_window": {1: None, 3: None, 5: ret5, 10: None, 20: None},
        "same_ticker_buy_hold": {"5D": same5},
        "benchmark_returns": {"SPY": {"5D": spy5}, "QQQ": {"5D": spy5}},
        "simulated_r": r,
        "mfe": mfe,
        "mae": mae,
        "outcome_status": "calculated",
    }


def test_aggregate_basic_stats():
    recs = [
        _rec(4.0, 1.0, 2.0, 2.0, 5.0, -2.0),
        _rec(-2.0, 0.5, 1.0, -1.0, 1.0, -3.0),
        _rec(6.0, 3.0, 2.0, 3.0, 8.0, -1.0),
    ]
    m = aggregate_outcomes(recs, window=5)

    assert m["window"] == "5D"
    assert m["sample_size"] == 3
    assert m["win_rate"] == pytest.approx(2 / 3)
    assert m["avg_return"] == pytest.approx(8 / 3)
    assert m["median_return"] == pytest.approx(4.0)
    assert m["avg_r"] == pytest.approx(4 / 3)
    # gains 4+6=10, losses -2 => 10/2 = 5
    assert m["profit_factor"] == pytest.approx(5.0)
    assert m["avg_mfe"] == pytest.approx(14 / 3)
    assert m["avg_mae"] == pytest.approx(-2.0)
    # (4-2)+(-2-1)+(6-2) = 3 ; /3 = 1.0
    assert m["baseline_delta_vs_spy"] == pytest.approx(1.0)
    # (4-1)+(-2-0.5)+(6-3) = 3.5 ; /3
    assert m["baseline_delta_vs_same_ticker"] == pytest.approx(3.5 / 3)


def test_empty_sample_is_honest():
    m = aggregate_outcomes([], window=5)
    assert m["sample_size"] == 0
    assert m["win_rate"] is None
    assert m["avg_return"] is None
    assert m["profit_factor"] is None


def test_profit_factor_none_without_losses():
    recs = [_rec(4.0, 1.0, 2.0, 2.0, 5.0, -2.0), _rec(6.0, 3.0, 2.0, 3.0, 8.0, -1.0)]
    m = aggregate_outcomes(recs, window=5)
    assert m["profit_factor"] is None  # no losing trades => undefined


def test_window_filter_excludes_none_returns():
    recs = [_rec(4.0, 1.0, 2.0, 2.0, 5.0, -2.0)]
    # window 10 has no data in these records
    m = aggregate_outcomes(recs, window=10)
    assert m["sample_size"] == 0


def test_group_and_aggregate_by_side():
    recs = [
        _rec(4.0, 1.0, 2.0, 2.0, 5.0, -2.0, side="LONG"),
        _rec(6.0, 3.0, 2.0, 3.0, 8.0, -1.0, side="LONG"),
        _rec(-2.0, 0.5, 1.0, -1.0, 1.0, -3.0, side="SHORT"),
    ]
    groups = group_and_aggregate(recs, ["side"], window=5)
    by_side = {g["side"]: g for g in groups}
    assert by_side["LONG"]["sample_size"] == 2
    assert by_side["SHORT"]["sample_size"] == 1
    # largest sample first
    assert groups[0]["side"] == "LONG"
