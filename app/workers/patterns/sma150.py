"""
SMA-150 Bounce Pattern Detection Algorithm
Identifies stocks that respect their 150-day moving average and show bounce potential.

Phase 1 (Evidence Engine) notes:
- Thresholds are conservative-by-default and fully config-driven (B12). A DB
  `pattern_configs` entry overrides any default via resolve_pattern_config().
- score_components hold RAW MEASURED values only, never score*weight (B2).
- Historical bounce counting is deduplicated: a contiguous run of in-band days
  is a single touch event, not one bounce per day.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List

from app.workers.indicators import sma, add_basic_indicators, validate_dataframe


logger = logging.getLogger(__name__)


# Version stamp persisted alongside score_components so downstream analysis
# knows which measurement/formula produced them.
SCORE_VERSION = "sma150.v2"


# Conservative Phase 1 defaults. These are intentionally STRICTER than the
# previous permissive values (which produced noisy signals). They are the
# fallback when no DB config exists; DB `pattern_configs` overrides them.
DEFAULT_CONFIG: Dict[str, Any] = {
    "sma_window": 150,
    "touch_tolerance_pct": 3.0,        # within 3% of SMA counts as a touch
    "lookback_days_for_history": 365,
    "min_bounces": 2,                  # need >=2 DISTINCT historical bounces
    "min_avg_rebound_pct": 5.0,        # meaningful rebounds only
    "rebound_window_days": 10,
    "min_volume_sma_ratio": 1.0,       # volume at/above its 20d average
    "min_price": 5.0,                  # avoid low-priced names
    "score_threshold": 0.5,            # composite score gate for ENTER
    "min_liquidity_filters": {
        "min_market_cap": 200000000,
        "min_daily_volume": 200000,
    },
}


def find_historical_bounces(
    df: pd.DataFrame,
    sma_col: str,
    tolerance_pct: float,
    rebound_window: int,
    min_rebound_pct: float,
) -> List[Dict[str, Any]]:
    """Find DISTINCT historical bounce events near the SMA.

    A "touch" is a contiguous run of days where price is within `tolerance_pct`
    of the SMA. Consecutive in-band days collapse into ONE event (dedup). The
    touch bar is the minimum-distance bar inside the run. It qualifies as a
    bounce when the maximum high in the following `rebound_window` days is at
    least `min_rebound_pct` above the touch price.
    """
    bounces: List[Dict[str, Any]] = []

    if df.empty or sma_col not in df.columns:
        return bounces

    df = df.reset_index(drop=True)
    sma_values = df[sma_col]
    distance_pct = (np.abs(df["close"] - sma_values) / sma_values * 100)
    in_band = (distance_pct <= tolerance_pct).to_numpy()

    n = len(df)
    i = 0
    while i < n:
        if not in_band[i]:
            i += 1
            continue

        # Contiguous in-band run => a single touch event.
        run_start = i
        while i < n and in_band[i]:
            i += 1
        run_end = i - 1  # inclusive

        run_distance = distance_pct.iloc[run_start:run_end + 1]
        touch_idx = int(run_distance.idxmin())
        touch_price = float(df.iloc[touch_idx]["close"])

        # Rebound is measured strictly AFTER the touch bar.
        window = df.iloc[touch_idx + 1:touch_idx + 1 + rebound_window]
        if window.empty:
            continue

        max_high = float(window["high"].max())
        max_gain_pct = (max_high - touch_price) / touch_price * 100

        if max_gain_pct >= min_rebound_pct:
            bounces.append({
                "touch_index": touch_idx,
                "date": df.iloc[touch_idx]["date"],
                "touch_price": touch_price,
                "sma_value": float(df.iloc[touch_idx][sma_col]),
                "distance_pct": float(run_distance.min()),
                "max_gain_pct": max_gain_pct,
                "run_length": run_end - run_start + 1,
            })

    return bounces


def _trend_context(closes: pd.Series, window: int = 20) -> str:
    """Classify recent trend as up/down/flat from the slope of recent closes."""
    recent = closes.dropna().tail(window)
    if len(recent) < 2:
        return "flat"
    slope = np.polyfit(range(len(recent)), recent.to_numpy(), 1)[0]
    if slope > 0:
        return "up"
    if slope < 0:
        return "down"
    return "flat"


def calculate_score(
    distance_pct: float,
    tolerance_pct: float,
    bounces: List[Dict[str, Any]],
    min_bounces: int,
    avg_rebound_pct: float,
    min_avg_rebound_pct: float,
    volume_ratio: float,
    min_volume_ratio: float,
) -> float:
    """Composite 0..1 score. Weights live here ONLY; the persisted
    score_components store the raw measured inputs, never these weighted terms.
    """
    proximity_score = max(0.0, 1 - (distance_pct / tolerance_pct)) if tolerance_pct else 0.0
    bounce_score = min(1.0, len(bounces) / max(min_bounces, 1))
    rebound_score = (
        min(1.0, max(0.0, avg_rebound_pct) / min_avg_rebound_pct)
        if min_avg_rebound_pct else 0.0
    )
    volume_score = 1.0 if volume_ratio >= min_volume_ratio else 0.0

    return (
        0.35 * proximity_score
        + 0.30 * bounce_score
        + 0.25 * rebound_score
        + 0.10 * volume_score
    )


def _avoid(symbol: str, reason: str, extra: Dict[str, Any] = None) -> Dict[str, Any]:
    details = {
        "symbol": symbol,
        "snapshot_date": str(datetime.now().date()),
        "score_version": SCORE_VERSION,
        "rejection_reason": reason,
    }
    if extra:
        details.update(extra)
    return {"verdict": "AVOID", "score": 0.0, "reason": reason, "details": details}


def evaluate_sma150_bounce(
    symbol: str,
    df: pd.DataFrame,
    config: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Evaluate the SMA-150 bounce pattern for a symbol.

    Args:
        symbol: ticker
        df: OHLCV dataframe (oldest first)
        config: resolved config dict; falls back to DEFAULT_CONFIG when None.

    Returns dict with keys: verdict ('ENTER'|'AVOID'), score, reason, details.
    `details.score_components` contains RAW measured values only.
    """
    if config is None:
        config = DEFAULT_CONFIG.copy()

    sma_window = int(config["sma_window"])
    tolerance_pct = float(config["touch_tolerance_pct"])
    min_bounces = int(config["min_bounces"])
    min_avg_rebound_pct = float(config["min_avg_rebound_pct"])
    rebound_window_days = int(config["rebound_window_days"])
    min_volume_ratio = float(config["min_volume_sma_ratio"])
    min_price = float(config.get("min_price", 0.0))
    score_threshold = float(config["score_threshold"])
    lookback_days = int(config["lookback_days_for_history"])

    thresholds_used = {
        "sma_window": sma_window,
        "touch_tolerance_pct": tolerance_pct,
        "min_bounces": min_bounces,
        "min_avg_rebound_pct": min_avg_rebound_pct,
        "rebound_window_days": rebound_window_days,
        "min_volume_sma_ratio": min_volume_ratio,
        "min_price": min_price,
        "score_threshold": score_threshold,
    }

    if not validate_dataframe(df, min_bars=sma_window + 50):
        return _avoid(
            symbol,
            "insufficient_data",
            {
                "bars_available": len(df),
                "bars_required": sma_window + 50,
                "thresholds_used": thresholds_used,
            },
        )

    try:
        df_ind = add_basic_indicators(df)
        sma_col = f"sma_{sma_window}"
        if sma_col not in df_ind.columns:
            df_ind[sma_col] = sma(df_ind["close"], sma_window)

        df_clean = df_ind.dropna(subset=[sma_col]).copy()
        if len(df_clean) < sma_window:
            return _avoid(
                symbol,
                "insufficient_data_after_sma",
                {"thresholds_used": thresholds_used},
            )

        # Historical window excludes the current (most recent) bar.
        history_start_idx = max(0, len(df_clean) - 1 - lookback_days)
        historical_df = df_clean.iloc[history_start_idx:-1]

        bounces = find_historical_bounces(
            historical_df,
            sma_col,
            tolerance_pct,
            rebound_window_days,
            min_avg_rebound_pct,
        )

        current = df_clean.iloc[-1]
        current_price = float(current["close"])
        sma_value = float(current[sma_col])
        distance_pct = abs(current_price - sma_value) / sma_value * 100
        price_vs_sma_pct = (current_price - sma_value) / sma_value * 100
        volume_ratio = float(current.get("vol_ratio", 0.0) or 0.0)
        avg_rebound = float(np.mean([b["max_gain_pct"] for b in bounces])) if bounces else 0.0
        bounce_count = len(bounces)
        trend_context = _trend_context(df_clean["close"])

        # RAW measured values only. No score*weight derivatives here (B2).
        score_components = {
            "proximity_to_sma150_pct": round(distance_pct, 4),
            "price_vs_sma150_pct": round(price_vs_sma_pct, 4),
            "bounce_count_deduped": float(bounce_count),
            "avg_rebound_pct": round(avg_rebound, 4),
            "volume_ratio": round(volume_ratio, 4),
        }

        near_sma = distance_pct <= tolerance_pct
        score = calculate_score(
            distance_pct,
            tolerance_pct,
            bounces,
            min_bounces,
            avg_rebound,
            min_avg_rebound_pct,
            volume_ratio,
            min_volume_ratio,
        ) if near_sma else 0.0

        current_date = current["date"]
        snapshot_date = (
            current_date.date()
            if isinstance(current_date, pd.Timestamp)
            else pd.to_datetime(current_date).date()
        )

        # Determine verdict + explicit rejection reason.
        rejection_reason = None
        if current_price < min_price:
            rejection_reason = "price_below_min"
        elif not near_sma:
            rejection_reason = "not_near_sma"
        elif bounce_count < min_bounces:
            rejection_reason = "insufficient_bounces"
        elif score < score_threshold:
            rejection_reason = "score_below_threshold"

        verdict = "ENTER" if rejection_reason is None else "AVOID"

        reason = (
            f"proximity {distance_pct:.1f}%, bounces {bounce_count}, "
            f"avg_rebound {avg_rebound:.1f}%, vol_ratio {volume_ratio:.2f}"
        )
        if rejection_reason:
            reason = f"{rejection_reason}: {reason}"

        details = {
            "symbol": symbol,
            "snapshot_date": str(snapshot_date),
            "score_version": SCORE_VERSION,
            "trend_context": trend_context,
            "thresholds_used": thresholds_used,
            "rejection_reason": rejection_reason,
            "score_components": score_components,
            # Back-compat keys consumed by the existing UI drawer.
            "proximity_pct": round(distance_pct, 4),
            "bounce_count": bounce_count,
            "avg_rebound_pct": round(avg_rebound, 4),
            "vol_ratio": round(volume_ratio, 4),
            "current_price": current_price,
            "sma_value": sma_value,
            "bounces_detail": bounces[-5:] if len(bounces) > 5 else bounces,
        }

        return {
            "verdict": verdict,
            "score": round(float(score), 3),
            "reason": reason,
            "details": details,
        }

    except Exception as e:
        logger.error(f"Error evaluating SMA-150 bounce for {symbol}: {e}")
        return _avoid(symbol, f"analysis_error: {str(e)}", {"error": str(e)})
