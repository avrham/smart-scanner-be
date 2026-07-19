"""Deterministic higher-timeframe structure analysis for Wyckoff MTF v1.

Pure functions only (no I/O). Every decision is a measurable rule on OHLCV
values; there is NO subjective chart interpretation here.
"""

from typing import Any, Dict, Tuple

import pandas as pd


# Heuristic normalizers used ONLY to turn a raw measured slope (in %) into a
# 0..1 "quality" component. They are transparent scaling constants, NOT fitted
# parameters and NOT a confidence score. The raw values are always persisted.
MONTHLY_SLOPE_REF_PCT = 2.0
WEEKLY_SLOPE_REF_PCT = 1.5

# Bias / phase string constants.
LONG = "LONG"
SHORT = "SHORT"
NEUTRAL = "NEUTRAL"

PHASE_ACCUMULATION = "accumulation"
PHASE_MARKUP = "markup"
PHASE_DISTRIBUTION = "distribution"
PHASE_MARKDOWN = "markdown"
PHASE_UNKNOWN = "unknown"


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _sma_slope_pct(close: pd.Series, window: int, lookback: int) -> Tuple[float, float, float]:
    """Return (last_close, last_sma, slope_pct) for an SMA over `window`.

    slope_pct is the percent change of the SMA over `lookback` bars. Requires
    window + lookback bars; otherwise returns NaNs.
    """
    if len(close) < window + lookback:
        return float("nan"), float("nan"), float("nan")
    sma = close.rolling(window=window).mean()
    last_sma = float(sma.iloc[-1])
    prev_sma = float(sma.iloc[-1 - lookback])
    if not (last_sma == last_sma) or not (prev_sma == prev_sma) or prev_sma == 0:
        return float(close.iloc[-1]), last_sma, float("nan")
    slope_pct = (last_sma - prev_sma) / abs(prev_sma) * 100.0
    return float(close.iloc[-1]), last_sma, slope_pct


def _strictly_decreasing(values) -> bool:
    return len(values) >= 3 and values[-1] < values[-2] < values[-3]


def _strictly_increasing(values) -> bool:
    return len(values) >= 3 and values[-1] > values[-2] > values[-3]


def monthly_bias(monthly_df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Deterministic monthly macro bias: LONG / SHORT / NEUTRAL.

    LONG  : close > SMA and SMA slope > 0 and not making 3 lower lows.
    SHORT : close < SMA and SMA slope < 0 and not making 3 higher highs.
    else  : NEUTRAL.
    """
    window = int(config["monthly_sma_window"])
    min_bars = int(config["monthly_min_bars"])
    lookback = int(config["monthly_slope_lookback"])

    bars = len(monthly_df)
    components: Dict[str, Any] = {"monthly_bars": bars}

    if bars < max(min_bars, window + lookback):
        components["monthly_insufficient"] = True
        return NEUTRAL, components

    close = monthly_df["close"].reset_index(drop=True)
    last_close, last_sma, slope_pct = _sma_slope_pct(close, window, lookback)
    lows = monthly_df["low"].to_numpy()
    highs = monthly_df["high"].to_numpy()
    lower_lows = _strictly_decreasing(lows)
    higher_highs = _strictly_increasing(highs)

    close_vs_sma_pct = (last_close - last_sma) / last_sma * 100.0 if last_sma else float("nan")

    components.update(
        {
            "monthly_close_vs_sma_pct": round(close_vs_sma_pct, 4),
            "monthly_sma_slope_pct": round(slope_pct, 4) if slope_pct == slope_pct else None,
            "monthly_lower_lows": bool(lower_lows),
            "monthly_higher_highs": bool(higher_highs),
        }
    )

    bias = NEUTRAL
    if slope_pct == slope_pct:  # not NaN
        if last_close > last_sma and slope_pct > 0 and not lower_lows:
            bias = LONG
        elif last_close < last_sma and slope_pct < 0 and not higher_highs:
            bias = SHORT

    components["monthly_bias"] = bias
    components["monthly_bias_quality"] = (
        _clip01(abs(slope_pct) / MONTHLY_SLOPE_REF_PCT) if (bias != NEUTRAL and slope_pct == slope_pct) else 0.0
    )
    return bias, components


def classify_weekly_phase(above_sma: bool, slope_up: bool) -> str:
    """Deterministic v1 phase mapping from price side + SMA slope sign."""
    if above_sma and slope_up:
        return PHASE_MARKUP
    if (not above_sma) and (not slope_up):
        return PHASE_MARKDOWN
    if above_sma and (not slope_up):
        return PHASE_DISTRIBUTION
    if (not above_sma) and slope_up:
        return PHASE_ACCUMULATION
    return PHASE_UNKNOWN


def weekly_alignment(
    weekly_df: pd.DataFrame, bias: str, config: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Deterministic weekly alignment with the monthly bias.

    LONG  aligned : monthly LONG + weekly SMA slope up + phase in {accumulation, markup}.
    SHORT aligned : monthly SHORT + weekly SMA slope down + phase in {distribution, markdown}.
    """
    window = int(config["weekly_sma_window"])
    min_bars = int(config["weekly_min_bars"])
    lookback = int(config["weekly_slope_lookback"])

    bars = len(weekly_df)
    components: Dict[str, Any] = {"weekly_bars": bars}

    if bars < max(min_bars, window + lookback):
        components["weekly_insufficient"] = True
        return False, PHASE_UNKNOWN, components

    close = weekly_df["close"].reset_index(drop=True)
    last_close, last_sma, slope_pct = _sma_slope_pct(close, window, lookback)
    if slope_pct != slope_pct:  # NaN
        components["weekly_insufficient"] = True
        return False, PHASE_UNKNOWN, components

    above = last_close >= last_sma
    slope_up = slope_pct > 0
    phase = classify_weekly_phase(above, slope_up)
    close_vs_sma_pct = (last_close - last_sma) / last_sma * 100.0 if last_sma else float("nan")

    aligned = False
    if bias == LONG and slope_up and phase in (PHASE_ACCUMULATION, PHASE_MARKUP):
        aligned = True
    elif bias == SHORT and (slope_pct < 0) and phase in (PHASE_DISTRIBUTION, PHASE_MARKDOWN):
        aligned = True

    components.update(
        {
            "weekly_close_vs_sma_pct": round(close_vs_sma_pct, 4),
            "weekly_sma_slope_pct": round(slope_pct, 4),
            "weekly_phase": phase,
            "weekly_aligned": aligned,
            "weekly_alignment_quality": _clip01(abs(slope_pct) / WEEKLY_SLOPE_REF_PCT) if aligned else 0.0,
        }
    )
    return aligned, phase, components
