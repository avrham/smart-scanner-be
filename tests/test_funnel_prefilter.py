"""Stage 2 cheap prefilter classification."""

import pandas as pd

from app.workers.scanner.funnel import cheap_prefilter


def _valid_df(n=210, price=50.0):
    dates = pd.date_range("2022-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "open": [price] * n,
            "high": [price] * n,
            "low": [price] * n,
            "close": [price] * n,
            "volume": [1_000_000] * n,
        }
    )


def test_none_or_empty_is_no_data():
    assert cheap_prefilter(None, min_price=5.0) == "no_data"
    assert cheap_prefilter(pd.DataFrame(), min_price=5.0) == "no_data"


def test_missing_columns():
    df = pd.DataFrame({"date": pd.date_range("2022-01-01", periods=210), "close": [1] * 210})
    assert cheap_prefilter(df, min_price=5.0) == "missing_columns"


def test_insufficient_history():
    df = _valid_df(n=50)
    assert cheap_prefilter(df, min_price=5.0) == "insufficient_history"


def test_invalid_ohlcv():
    df = _valid_df()
    df.loc[df.index[-1], "high"] = 0  # breaks high>=low / positivity checks
    assert cheap_prefilter(df, min_price=5.0) == "invalid_ohlcv"


def test_price_below_min():
    df = _valid_df(price=3.0)
    assert cheap_prefilter(df, min_price=5.0) == "price_below_min"


def test_passes():
    df = _valid_df(price=50.0)
    assert cheap_prefilter(df, min_price=5.0) is None
