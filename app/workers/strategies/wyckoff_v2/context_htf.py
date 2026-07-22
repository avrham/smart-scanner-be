"""Higher-timeframe context for wyckoff_mtf.v2 (Phase 9B).

Uses completed monthly/weekly periods only. Bias, slope, trend quality and
window structure are descriptive evidence — they never authorize a structure
classification or strategy verdict.

Pure functions only — no I/O.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple

import math
import pandas as pd

from app.workers.strategies.wyckoff_v2.constants import (
    HTF_CONTEXT_VERSION,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import (
    CompletedAggregationResult,
    HTFContextResult,
)


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return pd.Timestamp(value).date()


def _clip01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _sma(closes: pd.Series, window: int) -> Optional[float]:
    if len(closes) < window:
        return None
    vals = closes.iloc[-window:].astype(float)
    if vals.isna().any():
        return None
    return float(vals.mean())


def _slope_pct(closes: pd.Series, lookback: int) -> Optional[float]:
    if lookback <= 0 or len(closes) < lookback + 1:
        return None
    end = float(closes.iloc[-1])
    start = float(closes.iloc[-(lookback + 1)])
    if not math.isfinite(end) or not math.isfinite(start) or start == 0.0:
        return None
    return 100.0 * (end - start) / start


def _bias(
    close: Optional[float],
    sma: Optional[float],
    slope_pct: Optional[float],
) -> str:
    if close is None or sma is None or slope_pct is None:
        return "unknown"
    if close > sma and slope_pct > 0:
        return "up"
    if close < sma and slope_pct < 0:
        return "down"
    return "neutral"


def _window_structure(
    frame: pd.DataFrame,
    n_periods: int,
    tolerance_pct: float,
) -> Tuple[str, Dict[str, Optional[float]]]:
    empty_raw: Dict[str, Optional[float]] = {
        "recent_high": None,
        "recent_low": None,
        "prior_high": None,
        "prior_low": None,
    }
    if frame is None or len(frame) < 2 * n_periods:
        return "unknown", empty_raw

    recent = frame.iloc[-n_periods:]
    prior = frame.iloc[-(2 * n_periods) : -n_periods]
    recent_high = float(recent["high"].max())
    recent_low = float(recent["low"].min())
    prior_high = float(prior["high"].max())
    prior_low = float(prior["low"].min())
    raw = {
        "recent_high": recent_high,
        "recent_low": recent_low,
        "prior_high": prior_high,
        "prior_low": prior_low,
    }

    tol_high = abs(prior_high) * (tolerance_pct / 100.0)
    tol_low = abs(prior_low) * (tolerance_pct / 100.0)

    hh = recent_high > prior_high + tol_high
    hl = recent_low > prior_low + tol_low
    lh = recent_high < prior_high - tol_high
    ll = recent_low < prior_low - tol_low

    if hh and hl:
        return "higher_high_higher_low", raw
    if lh and ll:
        return "lower_high_lower_low", raw
    return "mixed", raw


def _timeframe_context(
    frame: pd.DataFrame,
    *,
    sma_window: int,
    slope_lookback: int,
    slope_reference_pct: float,
    structure_window: int,
    tolerance_pct: float,
) -> Dict[str, Any]:
    missing: list[str] = []
    if frame is None or len(frame) == 0:
        return {
            "bias": "unknown",
            "sma": None,
            "slope_pct": None,
            "trend_quality": None,
            "window_structure": "unknown",
            "window_raw": {
                "recent_high": None,
                "recent_low": None,
                "prior_high": None,
                "prior_low": None,
            },
            "missing": ("insufficient_periods",),
        }

    closes = frame["close"]
    sma = _sma(closes, sma_window)
    slope = _slope_pct(closes, slope_lookback)
    close = float(closes.iloc[-1]) if len(closes) else None
    if sma is None:
        missing.append("insufficient_sma_window")
    if slope is None:
        missing.append("insufficient_slope_lookback")

    bias = _bias(close, sma, slope)
    if slope is None or slope_reference_pct <= 0:
        trend_quality = None
        if slope_reference_pct <= 0:
            missing.append("invalid_slope_reference")
    else:
        trend_quality = _clip01(abs(slope) / slope_reference_pct)

    structure, window_raw = _window_structure(
        frame, structure_window, tolerance_pct
    )
    if structure == "unknown":
        missing.append("insufficient_structure_periods")

    return {
        "bias": bias,
        "sma": sma,
        "slope_pct": slope,
        "trend_quality": trend_quality,
        "window_structure": structure,
        "window_raw": window_raw,
        "missing": tuple(missing),
    }


def _alignment(monthly_bias: str, weekly_bias: str) -> Tuple[str, Tuple[str, ...]]:
    if monthly_bias == "unknown" or weekly_bias == "unknown":
        return "unknown", ("htf_bias_unknown",)
    if monthly_bias == "up" and weekly_bias == "up":
        return "aligned_up", ()
    if monthly_bias == "down" and weekly_bias == "down":
        return "aligned_down", ()
    if {monthly_bias, weekly_bias} == {"up", "down"}:
        return "contradiction", ("monthly_weekly_bias_contradiction",)
    return "mixed", ()


def measure_htf_context(
    aggregation: CompletedAggregationResult,
    *,
    as_of_date: Any,
    config: Optional[Dict[str, Any]] = None,
) -> HTFContextResult:
    """Compute HTF context from completed monthly/weekly frames.

    Uses only periods already present on the aggregation result. Does not
    recompute aggregation and never inspects daily bars after as_of.
    """
    cfg = resolve_config(config)
    pinned = _as_date(as_of_date).isoformat()

    monthly = aggregation.monthly_frame
    weekly = aggregation.weekly_frame

    # Guard: never use HTF periods ending after the pinned as_of.
    if monthly is not None and len(monthly) > 0:
        monthly = monthly[
            monthly["date"].map(lambda d: _as_date(d) <= _as_date(as_of_date))
        ].copy()
    if weekly is not None and len(weekly) > 0:
        weekly = weekly[
            weekly["date"].map(lambda d: _as_date(d) <= _as_date(as_of_date))
        ].copy()

    m = _timeframe_context(
        monthly,
        sma_window=int(cfg["monthly_sma_window"]),
        slope_lookback=int(cfg["monthly_slope_lookback"]),
        slope_reference_pct=float(cfg["monthly_slope_reference_pct"]),
        structure_window=int(cfg["monthly_structure_window_periods"]),
        tolerance_pct=float(cfg["htf_structure_tolerance_pct"]),
    )
    w = _timeframe_context(
        weekly,
        sma_window=int(cfg["weekly_sma_window"]),
        slope_lookback=int(cfg["weekly_slope_lookback"]),
        slope_reference_pct=float(cfg["weekly_slope_reference_pct"]),
        structure_window=int(cfg["weekly_structure_window_periods"]),
        tolerance_pct=float(cfg["htf_structure_tolerance_pct"]),
    )

    alignment, contradiction_codes = _alignment(m["bias"], w["bias"])
    missing = tuple(
        [f"monthly_{x}" for x in m["missing"]]
        + [f"weekly_{x}" for x in w["missing"]]
    )

    config_used = {
        k: cfg[k]
        for k in (
            "monthly_sma_window",
            "monthly_slope_lookback",
            "monthly_slope_reference_pct",
            "monthly_structure_window_periods",
            "weekly_sma_window",
            "weekly_slope_lookback",
            "weekly_slope_reference_pct",
            "weekly_structure_window_periods",
            "htf_structure_tolerance_pct",
        )
    }

    return HTFContextResult(
        htf_context_version=HTF_CONTEXT_VERSION,
        as_of_date=pinned,
        monthly_bias=m["bias"],
        monthly_sma=m["sma"],
        monthly_slope_pct=m["slope_pct"],
        monthly_trend_quality=m["trend_quality"],
        monthly_window_structure=m["window_structure"],
        monthly_window_raw=dict(m["window_raw"]),
        weekly_bias=w["bias"],
        weekly_sma=w["sma"],
        weekly_slope_pct=w["slope_pct"],
        weekly_trend_quality=w["trend_quality"],
        weekly_window_structure=w["window_structure"],
        weekly_window_raw=dict(w["window_raw"]),
        htf_alignment=alignment,
        contradiction_codes=contradiction_codes,
        missing_data=missing,
        config_used=config_used,
    )
