"""Causal effort-vs-result measurements for wyckoff_mtf.v2 (Phase 9B).

Pure per-bar measurements. Never inspects rows after the measured index.
Missing volume stays missing — never coerced to zero or forward-filled.

These states are descriptive evidence only; they are not bullish/bearish
decisions and never authorize ENTER.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import math
import pandas as pd

from app.workers.strategies.wyckoff_v2.constants import (
    EFFORT_RESULT_VERSION,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import EffortResultMeasurement


class EffortResultError(ValueError):
    """Deterministic rejection of invalid effort-result inputs."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return pd.Timestamp(value).date()


def _finite_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    f = float(value)
    if not math.isfinite(f):
        raise EffortResultError("non_finite_input", f"non-finite value: {value!r}")
    return f


def _true_range(high: float, low: float, previous_close: Optional[float]) -> float:
    spread = high - low
    if previous_close is None:
        return spread
    return max(
        spread,
        abs(high - previous_close),
        abs(low - previous_close),
    )


def _truncate_to_as_of(daily: pd.DataFrame, as_of_date: Any) -> pd.DataFrame:
    pinned = _as_date(as_of_date)
    dates = daily["date"].map(_as_date)
    return daily.loc[dates <= pinned].copy().reset_index(drop=True)


def measure_effort_result_at_index(
    daily: pd.DataFrame,
    index: int,
    *,
    as_of_date: Any,
    config: Optional[Dict[str, Any]] = None,
    timeframe: str = "1d",
) -> EffortResultMeasurement:
    """Measure effort-vs-result at ``index`` using only bars ``<= index``.

    The frame is first truncated to ``as_of_date``. Bars after the measured
    index are never inspected.
    """
    cfg = resolve_config(config)
    frame = _truncate_to_as_of(daily, as_of_date)
    if index < 0 or index >= len(frame):
        raise EffortResultError(
            "index_out_of_range",
            f"index {index} outside truncated frame length {len(frame)}",
        )

    atr_window = int(cfg["event_atr_window"])
    vol_window = int(cfg["event_volume_baseline_window"])
    min_vol_bars = int(cfg["event_min_volume_baseline_bars"])
    effort_high = float(cfg["effort_high_volume_ratio"])
    effort_low = float(cfg["effort_low_volume_ratio"])
    result_high = float(cfg["result_high_atr_ratio"])
    result_low = float(cfg["result_low_atr_ratio"])

    row = frame.iloc[index]
    high = _finite_or_none(row["high"])
    low = _finite_or_none(row["low"])
    close = _finite_or_none(row["close"])
    volume = _finite_or_none(row["volume"])
    if high is None or low is None or close is None:
        raise EffortResultError("missing_ohlc", "measured bar missing OHLC")

    missing: List[str] = []
    previous_close: Optional[float] = None
    if index > 0:
        previous_close = _finite_or_none(frame.iloc[index - 1]["close"])
        if previous_close is None:
            missing.append("missing_previous_close")
    else:
        missing.append("no_previous_close")

    # Causal ATR: TR values ending at measured bar (inclusive).
    atr: Optional[float] = None
    if index + 1 < atr_window:
        missing.append("insufficient_atr_history")
    else:
        trs: List[float] = []
        start = index - atr_window + 1
        for i in range(start, index + 1):
            h = _finite_or_none(frame.iloc[i]["high"])
            l = _finite_or_none(frame.iloc[i]["low"])
            if h is None or l is None:
                trs = []
                break
            prev_c: Optional[float] = None
            if i > 0:
                prev_c = _finite_or_none(frame.iloc[i - 1]["close"])
            trs.append(_true_range(h, l, prev_c))
        if len(trs) == atr_window:
            atr = float(sum(trs) / atr_window)
        else:
            missing.append("insufficient_atr_history")

    price_spread = high - low
    spread_atr_ratio: Optional[float] = None
    if atr is not None and atr > 0:
        spread_atr_ratio = price_spread / atr
    elif atr is None:
        pass
    else:
        missing.append("zero_atr")

    close_location_value: Optional[float] = None
    if high > low:
        close_location_value = (close - low) / (high - low)
    else:
        missing.append("zero_range_bar")

    directional_result_pct: Optional[float] = None
    directional_result_atr_ratio: Optional[float] = None
    if previous_close is not None and previous_close != 0.0:
        directional_result_pct = 100.0 * (close - previous_close) / previous_close
        if atr is not None and atr > 0:
            directional_result_atr_ratio = abs(close - previous_close) / atr

    # Relative volume: baseline excludes measured bar.
    volume_baseline_mean: Optional[float] = None
    usable_vols: List[float] = []
    if index > 0:
        start = max(0, index - vol_window)
        for i in range(start, index):  # excludes measured bar
            v = _finite_or_none(frame.iloc[i]["volume"])
            if v is not None and v > 0:
                usable_vols.append(v)
    volume_baseline_usable_bars = len(usable_vols)
    if volume_baseline_usable_bars >= min_vol_bars:
        volume_baseline_mean = float(sum(usable_vols) / volume_baseline_usable_bars)
    else:
        missing.append("insufficient_volume_baseline")

    relative_volume: Optional[float] = None
    if volume is None:
        missing.append("missing_measured_volume")
    elif volume <= 0:
        missing.append("non_positive_measured_volume")
    elif volume_baseline_mean is None or volume_baseline_mean <= 0:
        missing.append("missing_volume_baseline")
    else:
        relative_volume = volume / volume_baseline_mean

    # Effort / result states
    if relative_volume is None:
        effort_state = "unknown"
    elif relative_volume >= effort_high:
        effort_state = "high"
    elif relative_volume <= effort_low:
        effort_state = "low"
    else:
        effort_state = "normal"

    if directional_result_atr_ratio is None:
        result_state = "unknown"
    elif directional_result_atr_ratio >= result_high:
        result_state = "high"
    elif directional_result_atr_ratio <= result_low:
        result_state = "low"
    else:
        result_state = "normal"

    if effort_state == "unknown" or result_state == "unknown":
        effort_result_state = "unknown"
    elif effort_state == "high" and result_state == "high":
        effort_result_state = "agreement_high"
    elif effort_state == "low" and result_state == "low":
        effort_result_state = "agreement_low"
    elif effort_state == "high" and result_state == "low":
        effort_result_state = "high_effort_low_result"
    elif effort_state == "low" and result_state == "high":
        effort_result_state = "low_effort_high_result"
    else:
        effort_result_state = "mixed"

    raw_components = {
        "true_range": _true_range(high, low, previous_close),
        "atr_window": float(atr_window),
        "volume_baseline_window": float(vol_window),
        "effort_high_volume_ratio": effort_high,
        "effort_low_volume_ratio": effort_low,
        "result_high_atr_ratio": result_high,
        "result_low_atr_ratio": result_low,
    }

    return EffortResultMeasurement(
        effort_result_version=EFFORT_RESULT_VERSION,
        date=_as_date(row["date"]).isoformat(),
        index=int(index),
        timeframe=timeframe,
        atr=atr,
        price_spread=float(price_spread),
        spread_atr_ratio=spread_atr_ratio,
        close_location_value=close_location_value,
        previous_close=previous_close,
        directional_result_pct=directional_result_pct,
        directional_result_atr_ratio=directional_result_atr_ratio,
        volume=volume,
        relative_volume=relative_volume,
        volume_baseline_mean=volume_baseline_mean,
        volume_baseline_usable_bars=int(volume_baseline_usable_bars),
        effort_state=effort_state,
        result_state=result_state,
        effort_result_state=effort_result_state,
        missing_data=tuple(missing),
        raw_components=raw_components,
    )


def measure_effort_results(
    daily: pd.DataFrame,
    *,
    as_of_date: Any,
    config: Optional[Dict[str, Any]] = None,
    indices: Optional[Sequence[int]] = None,
    timeframe: str = "1d",
) -> Tuple[EffortResultMeasurement, ...]:
    """Measure effort-result for selected (or all) indices at pinned as_of."""
    frame = _truncate_to_as_of(daily, as_of_date)
    if indices is None:
        target = range(len(frame))
    else:
        target = indices
    return tuple(
        measure_effort_result_at_index(
            frame,
            i,
            as_of_date=as_of_date,
            config=config,
            timeframe=timeframe,
        )
        for i in target
    )
