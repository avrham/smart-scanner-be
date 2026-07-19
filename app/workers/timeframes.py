"""Timeframe utilities (Phase 5).

Pure, deterministic helpers to normalize a daily OHLCV frame and resample it to
weekly / monthly while preserving OHLCV semantics:

    open   = first
    high   = max
    low    = min
    close  = last
    volume = sum

These functions NEVER fetch data. They operate on an in-memory daily DataFrame
(oldest-first, as produced by `indicators.to_dataframe`). Insufficient/empty
input yields an empty frame with the canonical columns (never an exception, so a
strategy can classify it as insufficient-data safely).
"""

from typing import Optional

import pandas as pd


OHLCV_COLS = ["date", "open", "high", "low", "close", "volume"]

# pandas 2.x resample rules (month-end / week ending Friday).
_MONTHLY_RULE = "ME"
_WEEKLY_RULE = "W-FRI"

_AGG = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
}


def _empty_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(columns=OHLCV_COLS)


def has_required_columns(df: Optional[pd.DataFrame]) -> bool:
    """True if df is a non-empty frame containing all OHLCV columns."""
    if df is None or len(df) == 0:
        return False
    return all(col in df.columns for col in OHLCV_COLS)


def normalize_ohlcv(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Return a clean, oldest-first OHLCV frame or an empty canonical frame.

    - validates required columns
    - parses `date` to datetime
    - coerces numerics and drops rows with NaN in critical columns
    - sorts ascending by date and resets the index
    """
    if not has_required_columns(df):
        return _empty_ohlcv()

    out = df[OHLCV_COLS].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    for col in ("open", "high", "low", "close", "volume"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=OHLCV_COLS)
    out = out.sort_values("date").reset_index(drop=True)
    return out


def _resample(df: Optional[pd.DataFrame], rule: str) -> pd.DataFrame:
    """Resample a daily OHLCV frame to `rule`, preserving OHLCV semantics."""
    norm = normalize_ohlcv(df)
    if norm.empty:
        return _empty_ohlcv()

    indexed = norm.set_index("date")
    agg = indexed.resample(rule).agg(_AGG)
    # Drop periods with no trading (all-NaN price rows from the resample).
    agg = agg.dropna(subset=["open", "high", "low", "close"])
    agg = agg.reset_index()
    return agg[OHLCV_COLS]


def resample_to_weekly(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Daily -> weekly (week ending Friday)."""
    return _resample(df, _WEEKLY_RULE)


def resample_to_monthly(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Daily -> monthly (calendar month end)."""
    return _resample(df, _MONTHLY_RULE)
