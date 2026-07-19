"""Deterministic Wyckoff-style daily setup detection + optional 4H trigger.

Pure functions only. Every rule is a measurable condition on OHLCV/ATR/volume.
Subjective concepts (e.g. nuanced LPS/LPSY/effort-vs-result reading) are
intentionally NOT implemented in v1 and are documented as future work.
"""

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


# Daily setup type constants.
SETUP_SPRING = "spring"
SETUP_UTAD = "utad"
SETUP_SOS = "sos"
SETUP_SOW = "sow"
SETUP_RANGE_BREAKOUT = "range_breakout"
SETUP_RANGE_BREAKDOWN = "range_breakdown"
SETUP_NONE = "none"

# Setup -> daily_setup_quality (raw, transparent mapping; not fitted).
_SETUP_QUALITY = {
    SETUP_SPRING: 1.0,
    SETUP_UTAD: 1.0,
    SETUP_SOS: 0.9,
    SETUP_SOW: 0.9,
    SETUP_RANGE_BREAKOUT: 0.6,
    SETUP_RANGE_BREAKDOWN: 0.6,
    SETUP_NONE: 0.0,
}

# Bullish setups are only valid for LONG; bearish only for SHORT.
_BULLISH = {SETUP_SPRING, SETUP_SOS, SETUP_RANGE_BREAKOUT}
_BEARISH = {SETUP_UTAD, SETUP_SOW, SETUP_RANGE_BREAKDOWN}


def _atr(df: pd.DataFrame, window: int) -> float:
    """Last ATR value (index-preserving); NaN if insufficient bars."""
    if len(df) < window + 1:
        return float("nan")
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return float(tr.rolling(window=window).mean().iloc[-1])


def setup_quality(setup_type: str) -> float:
    return _SETUP_QUALITY.get(setup_type, 0.0)


def detect_daily_setup(
    daily_df: pd.DataFrame, side: str, config: Dict[str, Any]
) -> Dict[str, Any]:
    """Detect a deterministic daily Wyckoff-style setup for the given side.

    Returns a dict with `setup_type` (matching `side` direction or 'none') plus
    raw measured components. The range is measured over `daily_range_lookback`
    bars EXCLUDING the current bar; the current bar is the trigger candidate.
    """
    lookback = int(config["daily_range_lookback"])
    atr_window = int(config["atr_window"])
    min_range_mult = float(config["min_range_atr_multiple"])
    pierce_mult = float(config["pierce_atr_multiple"])
    vol_window = int(config["volume_sma_window"])
    min_vol_ratio = float(config["min_breakout_volume_ratio"])

    components: Dict[str, Any] = {"setup_type": SETUP_NONE, "daily_setup_quality": 0.0}

    needed = lookback + 1
    if len(daily_df) < max(needed, atr_window + 1, vol_window + 1):
        components["daily_insufficient"] = True
        return components

    df = daily_df.reset_index(drop=True)
    window = df.iloc[-(lookback + 1):-1]  # range excludes current bar
    cur = df.iloc[-1]

    range_high = float(window["high"].max())
    range_low = float(window["low"].min())
    range_height = range_high - range_low
    atr_val = _atr(df, atr_window)

    vol_sma = float(df["volume"].rolling(window=vol_window).mean().iloc[-1])
    cur_vol = float(cur["volume"])
    vol_ratio = (cur_vol / vol_sma) if vol_sma and vol_sma == vol_sma else float("nan")

    cur_close = float(cur["close"])
    cur_high = float(cur["high"])
    cur_low = float(cur["low"])
    pierce = pierce_mult * atr_val if atr_val == atr_val else 0.0

    range_atr_multiple = (range_height / atr_val) if atr_val and atr_val == atr_val else float("nan")

    components.update(
        {
            "daily_range_high": round(range_high, 4),
            "daily_range_low": round(range_low, 4),
            "daily_range_atr_multiple": round(range_atr_multiple, 4) if range_atr_multiple == range_atr_multiple else None,
            "daily_atr": round(atr_val, 4) if atr_val == atr_val else None,
            "daily_volume_ratio": round(vol_ratio, 4) if vol_ratio == vol_ratio else None,
        }
    )

    # The range must be meaningful relative to ATR, otherwise it is just noise.
    range_ok = (
        atr_val == atr_val and atr_val > 0 and range_atr_multiple == range_atr_multiple
        and range_atr_multiple >= min_range_mult
    )
    if not range_ok:
        components["daily_range_rejected"] = True
        return components

    vol_ok = vol_ratio == vol_ratio and vol_ratio >= min_vol_ratio

    setup = SETUP_NONE
    if side == "LONG":
        if cur_low < (range_low - pierce) and cur_close > range_low:
            setup = SETUP_SPRING
        elif cur_close > range_high and vol_ok:
            setup = SETUP_SOS
        elif cur_close > range_high:
            setup = SETUP_RANGE_BREAKOUT
    elif side == "SHORT":
        if cur_high > (range_high + pierce) and cur_close < range_high:
            setup = SETUP_UTAD
        elif cur_close < range_low and vol_ok:
            setup = SETUP_SOW
        elif cur_close < range_low:
            setup = SETUP_RANGE_BREAKDOWN

    # Guard: never return a setup that contradicts the intended side.
    if side == "LONG" and setup not in _BULLISH:
        setup = SETUP_NONE
    if side == "SHORT" and setup not in _BEARISH:
        setup = SETUP_NONE

    components["setup_type"] = setup
    components["daily_setup_quality"] = setup_quality(setup)
    return components


def four_hour_trigger(
    df_4h: Optional[pd.DataFrame], side: str, config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Optional 4H entry trigger. Returns None if data is missing/insufficient.

    LONG  : last 4H close breaks above the prior local high.
    SHORT : last 4H close breaks below the prior local low.
    Sets entry_price (trigger close), stop_price / invalidation (recent local
    swing). target_price stays None in v1 (no deterministic target).
    """
    lookback = int(config.get("trigger_lookback_4h", 10))
    if df_4h is None or len(df_4h) < lookback + 1:
        return None

    df = df_4h.reset_index(drop=True)
    window = df.iloc[-(lookback + 1):-1]
    cur = df.iloc[-1]
    cur_close = float(cur["close"])

    local_high = float(window["high"].max())
    local_low = float(window["low"].min())

    triggered = False
    entry_price = stop_price = invalidation = None
    if side == "LONG" and cur_close > local_high:
        triggered = True
        entry_price = cur_close
        stop_price = local_low
        invalidation = local_low
    elif side == "SHORT" and cur_close < local_low:
        triggered = True
        entry_price = cur_close
        stop_price = local_high
        invalidation = local_high

    return {
        "triggered": triggered,
        "entry_price": entry_price,
        "stop_price": stop_price,
        "invalidation": invalidation,
        "target_price": None,
        "trigger_quality": 1.0 if triggered else 0.0,
        "local_high": round(local_high, 4),
        "local_low": round(local_low, 4),
    }
