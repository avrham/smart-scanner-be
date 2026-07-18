"""Test helpers: build synthetic OHLCV DataFrames."""

from typing import List, Optional

import pandas as pd


def make_df_with_sma(
    closes: List[float],
    highs: Optional[List[float]] = None,
    sma_value: float = 100.0,
) -> pd.DataFrame:
    """Build a dataframe with an explicit constant SMA column.

    Used to test bounce detection where we want precise control over the
    distance between close and the SMA.
    """
    n = len(closes)
    highs = highs if highs is not None else list(closes)
    dates = pd.date_range("2023-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "close": closes,
            "high": highs,
            "low": [c * 0.99 for c in closes],
            "volume": [1_000_000] * n,
            "sma_150": [sma_value] * n,
        }
    )


def make_ohlcv(n: int, price: float = 100.0, volume: float = 1_000_000) -> pd.DataFrame:
    """Build a flat-price OHLCV dataframe with n bars (oldest first).

    Enough bars so that after a 150-bar SMA warmup there are still >=150
    non-NaN rows (evaluate requires len(df_clean) >= sma_window).
    """
    dates = pd.date_range("2022-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "open": [price] * n,
            "high": [price] * n,
            "low": [price] * n,
            "close": [price] * n,
            "volume": [volume] * n,
        }
    )
