"""
SMA-150 Bounce Pattern Detection Algorithm
Identifies stocks that respect their 150-day moving average and show bounce potential
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Tuple

from app.workers.indicators import sma, atr, add_basic_indicators, validate_dataframe
from app.config import settings


logger = logging.getLogger(__name__)


DEFAULT_CONFIG = {
    "sma_window": 150,
    "touch_tolerance_pct": 15.0,       # Increased from 8.0% to 15.0% - much more lenient proximity
    "lookback_days_for_history": 365,
    "min_bounces": 1,                  # Reduced from 2 to 1 - only need 1 historical bounce
    "min_avg_rebound_pct": 2.0,        # Reduced from 3.0% to 2.0% - very small rebounds acceptable
    "rebound_window_days": 10,
    "min_volume_sma_ratio": 0.5,       # Reduced from 0.8 to 0.5 - very low volume requirement
    "min_liquidity_filters": {
        "min_market_cap": 50000000,    # Reduced from $100M to $50M - much smaller companies
        "min_daily_volume": 50000      # Reduced from 100k to 50k shares - very low volume
    }
}


def find_historical_bounces(
    df: pd.DataFrame, 
    sma_col: str, 
    tolerance_pct: float,
    rebound_window: int,
    min_rebound_pct: float
) -> List[Dict[str, Any]]:
    """Find historical bounce points where price touched SMA and rebounded"""
    
    bounces = []
    
    # Calculate distance from SMA as percentage
    df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
    df['sma_distance_pct'] = np.abs(df['close'] - df[sma_col]) / df[sma_col] * 100
    
    # Find touch points (within tolerance)
    touch_mask = df['sma_distance_pct'] <= tolerance_pct
    touch_indices = df.index[touch_mask].tolist()
    
    for touch_idx in touch_indices:
        # Skip if we don't have enough data after this point
        if touch_idx + rebound_window >= len(df):
            continue
        
        touch_row = df.iloc[touch_idx]
        touch_date = touch_row['date']
        touch_price = touch_row['close']
        
        # Look at the next N days for rebound
        rebound_window_data = df.iloc[touch_idx + 1:touch_idx + 1 + rebound_window]
        
        if rebound_window_data.empty:
            continue
        
        # Calculate maximum gain in the rebound window
        max_high = rebound_window_data['high'].max()
        max_gain_pct = (max_high - touch_price) / touch_price * 100
        
        # Check if this qualifies as a bounce
        if max_gain_pct >= min_rebound_pct:
            # Determine trend direction before touch
            lookback_days = min(5, touch_idx)
            if lookback_days > 0:
                pre_touch_data = df.iloc[touch_idx - lookback_days:touch_idx]
                trend_slope = np.polyfit(range(len(pre_touch_data)), pre_touch_data['close'], 1)[0]
            else:
                trend_slope = 0
            
            bounce_info = {
                "date": touch_date,
                "touch_price": touch_price,
                "sma_value": touch_row[sma_col],
                "distance_pct": touch_row['sma_distance_pct'],
                "max_gain_pct": max_gain_pct,
                "rebound_days": len(rebound_window_data),
                "trend_before": "down" if trend_slope < 0 else "up",
                "volume_ratio": touch_row.get('vol_ratio', 1.0)
            }
            
            bounces.append(bounce_info)
    
    return bounces


def analyze_current_setup(
    df: pd.DataFrame, 
    sma_col: str, 
    tolerance_pct: float,
    min_volume_ratio: float
) -> Dict[str, Any]:
    """Analyze current bar for entry setup"""
    
    if df.empty:
        return {"valid": False, "reason": "No data"}
    
    current = df.iloc[-1]
    
    # Check if current price is near SMA
    if pd.isna(current[sma_col]):
        return {"valid": False, "reason": "SMA not available"}
    
    distance_pct = abs(current['close'] - current[sma_col]) / current[sma_col] * 100
    near_sma = distance_pct <= tolerance_pct
    
    # Check volume confirmation
    volume_ratio = current.get('vol_ratio', 0)
    volume_confirmed = volume_ratio >= min_volume_ratio
    
    # Additional technical checks
    checks = {
        "near_sma": near_sma,
        "distance_pct": distance_pct,
        "volume_confirmed": volume_confirmed,
        "volume_ratio": volume_ratio,
        "price": current['close'],
        "sma_value": current[sma_col],
        "atr": current.get('atr_14', 0)
    }
    
    # Check for potential reversal patterns (simple)
    if len(df) >= 3:
        last_3 = df.tail(3)
        
        # Look for hammer-like pattern or bullish engulfing
        current_body = abs(current['close'] - current['open'])
        current_range = current['high'] - current['low']
        
        # Hammer: small body, long lower wick
        lower_wick = current['open'] - current['low'] if current['close'] > current['open'] else current['close'] - current['low']
        upper_wick = current['high'] - max(current['open'], current['close'])
        
        hammer_like = (current_body < current_range * 0.3) and (lower_wick > current_body * 2)
        
        checks["hammer_pattern"] = hammer_like
        checks["reversal_signal"] = hammer_like  # Can add more patterns here
    else:
        checks["hammer_pattern"] = False
        checks["reversal_signal"] = False
    
    checks["valid"] = near_sma  # Only require proximity to SMA, volume is optional
    
    return checks


def calculate_score(
    current_setup: Dict[str, Any],
    bounces: List[Dict[str, Any]],
    config: Dict[str, Any]
) -> float:
    """Calculate composite score (0-1) for the signal strength"""
    
    if not current_setup.get("valid", False):
        return 0.0
    
    scores = {}
    
    # 1. Proximity to SMA (35% weight)
    distance_pct = current_setup["distance_pct"]
    tolerance = config["touch_tolerance_pct"]
    proximity_score = max(0, 1 - (distance_pct / tolerance))
    scores["proximity"] = proximity_score
    
    # 2. Historical bounce count (30% weight)
    bounce_count = len(bounces)
    min_bounces = config["min_bounces"]
    bounce_score = min(1.0, bounce_count / max(min_bounces, 1))
    scores["bounce_count"] = bounce_score
    
    # 3. Average rebound strength (25% weight)
    if bounces:
        avg_rebound = np.mean([b["max_gain_pct"] for b in bounces])
        min_avg_rebound = config["min_avg_rebound_pct"]
        rebound_score = min(1.0, max(0.0, avg_rebound) / min_avg_rebound)
    else:
        avg_rebound = 0
        rebound_score = 0
    scores["rebound_strength"] = rebound_score
    
    # 4. Volume confirmation (10% weight)
    volume_ratio = current_setup["volume_ratio"]
    min_volume_ratio = config["min_volume_sma_ratio"]
    volume_score = 1.0 if volume_ratio >= min_volume_ratio else 0.0
    scores["volume"] = volume_score
    
    # Weighted composite score
    composite_score = (
        0.35 * proximity_score +
        0.30 * bounce_score +
        0.25 * rebound_score +
        0.10 * volume_score
    )
    
    return composite_score


def evaluate_sma150_bounce(
    symbol: str, 
    df: pd.DataFrame, 
    config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Main evaluation function for SMA-150 bounce pattern
    
    Returns:
        dict: {
            "verdict": "ENTER" | "AVOID",
            "score": float (0-1),
            "reason": str,
            "details": dict
        }
    """
    
    # Use default config if none provided
    if config is None:
        config = DEFAULT_CONFIG.copy()
    
    # Validate input data
    if not validate_dataframe(df, min_bars=config["sma_window"] + 50):
        return {
            "verdict": "AVOID",
            "score": 0.0,
            "reason": "Insufficient historical data",
            "details": {
                "symbol": symbol,
                "bars_available": len(df),
                "bars_required": config["sma_window"] + 50,
                "snapshot_date": str(datetime.now().date())
            }
        }
    
    try:
        # Add technical indicators
        df_with_indicators = add_basic_indicators(df)
        
        # Use appropriate SMA column
        sma_col = f'sma_{config["sma_window"]}'
        if sma_col not in df_with_indicators.columns:
            df_with_indicators[sma_col] = sma(df_with_indicators['close'], config["sma_window"])
        
        # Remove rows with NaN SMA values
        df_clean = df_with_indicators.dropna(subset=[sma_col]).copy()
        
        if len(df_clean) < config["sma_window"]:
            return {
                "verdict": "AVOID",
                "score": 0.0,
                "reason": "Insufficient data after SMA calculation",
                "details": {
                    "symbol": symbol,
                    "snapshot_date": str(datetime.now().date())
                }
            }
        
        # Define historical analysis period
        lookback_days = config["lookback_days_for_history"]
        history_start_idx = max(0, len(df_clean) - 1 - lookback_days)
        historical_df = df_clean.iloc[history_start_idx:-1]  # Exclude current day
        
        # Find historical bounces
        bounces = find_historical_bounces(
            historical_df,
            sma_col,
            config["touch_tolerance_pct"],
            config["rebound_window_days"],
            config["min_avg_rebound_pct"]
        )
        
        # Analyze current setup
        current_setup = analyze_current_setup(
            df_clean,
            sma_col,
            config["touch_tolerance_pct"],
            config["min_volume_sma_ratio"]
        )
        
        # Calculate score
        score = calculate_score(current_setup, bounces, config)
        
        # Make verdict decision
        min_bounces = config["min_bounces"]
        score_threshold = 0.1  # Reduced from 0.2 to 0.1 - extremely easy to get ENTER signals
        
        verdict = "ENTER" if (score >= score_threshold and len(bounces) >= min_bounces) else "AVOID"
        
        # Create reason string
        bounce_count = len(bounces)
        avg_rebound = np.mean([b["max_gain_pct"] for b in bounces]) if bounces else 0
        
        reason = (
            f"proximity {current_setup['distance_pct']:.1f}%, "
            f"bounces {bounce_count}, "
            f"avg_rebound {avg_rebound:.1f}%, "
            f"vol_ratio {current_setup['volume_ratio']:.2f}"
        )
        
        # Get current date
        current_date = df_clean.iloc[-1]['date']
        if isinstance(current_date, pd.Timestamp):
            snapshot_date = current_date.date()
        else:
            snapshot_date = pd.to_datetime(current_date).date()
        
        # Detailed results
        details = {
            "symbol": symbol,
            "snapshot_date": str(snapshot_date),
            "proximity_pct": current_setup["distance_pct"],
            "bounce_count": bounce_count,
            "avg_rebound_pct": avg_rebound,
            "vol_ratio": current_setup["volume_ratio"],
            "score_components": {
                "proximity": score * 0.35,
                "bounce_count": score * 0.30,
                "rebound_strength": score * 0.25,
                "volume": score * 0.10
            },
            "current_price": float(current_setup["price"]),
            "sma_value": float(current_setup["sma_value"]),
            "bounces_detail": bounces[-5:] if len(bounces) > 5 else bounces  # Last 5 bounces
        }
        
        return {
            "verdict": verdict,
            "score": round(float(score), 3),
            "reason": reason,
            "details": details
        }
        
    except Exception as e:
        logger.error(f"Error evaluating SMA-150 bounce for {symbol}: {e}")
        return {
            "verdict": "AVOID",
            "score": 0.0,
            "reason": f"Analysis error: {str(e)}",
            "details": {
                "symbol": symbol,
                "error": str(e),
                "snapshot_date": str(datetime.now().date())
            }
        }
