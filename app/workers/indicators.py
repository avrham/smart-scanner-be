"""
Technical indicators for pattern detection
Optimized implementations using pandas and numpy
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List


def sma(series: pd.Series, window: int) -> pd.Series:
    """Simple Moving Average"""
    return series.rolling(window=window).mean()


def ema(series: pd.Series, window: int) -> pd.Series:
    """Exponential Moving Average"""
    return series.ewm(span=window).mean()


def atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """Average True Range"""
    high = df['high']
    low = df['low']
    close = df['close']
    
    # True Range calculation
    tr1 = high - low
    tr2 = np.abs(high - close.shift(1))
    tr3 = np.abs(low - close.shift(1))
    
    true_range = np.maximum(tr1, np.maximum(tr2, tr3))
    
    # Average True Range
    return pd.Series(true_range).rolling(window=window).mean()


def rsi(series: pd.Series, window: int = 14) -> pd.Series:
    """Relative Strength Index"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=window).mean()
    avg_loss = loss.rolling(window=window).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def bollinger_bands(series: pd.Series, window: int = 20, std_dev: float = 2) -> Dict[str, pd.Series]:
    """Bollinger Bands"""
    middle = sma(series, window)
    std = series.rolling(window=window).std()
    
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    
    return {
        'upper': upper,
        'middle': middle,
        'lower': lower
    }


def stochastic(df: pd.DataFrame, k_window: int = 14, d_window: int = 3) -> Dict[str, pd.Series]:
    """Stochastic Oscillator"""
    high = df['high']
    low = df['low']
    close = df['close']
    
    lowest_low = low.rolling(window=k_window).min()
    highest_high = high.rolling(window=k_window).max()
    
    k_percent = 100 * ((close - lowest_low) / (highest_high - lowest_low))
    d_percent = k_percent.rolling(window=d_window).mean()
    
    return {
        'k': k_percent,
        'd': d_percent
    }


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, pd.Series]:
    """MACD (Moving Average Convergence Divergence)"""
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line
    
    return {
        'macd': macd_line,
        'signal': signal_line,
        'histogram': histogram
    }


def to_dataframe(fmp_data: Dict[str, Any]) -> pd.DataFrame:
    """Convert FMP historical data to pandas DataFrame"""
    if not fmp_data.get("historical"):
        return pd.DataFrame()
    
    # Extract historical data
    historical = fmp_data["historical"]
    
    # Convert to DataFrame
    df = pd.DataFrame(historical)
    
    # Ensure we have required columns
    required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Convert date column
    df['date'] = pd.to_datetime(df['date'])
    
    # Convert price/volume columns to numeric
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Sort by date (oldest first)
    df = df.sort_values('date').reset_index(drop=True)
    
    # Remove any rows with NaN values in critical columns
    df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
    
    return df


def validate_dataframe(df: pd.DataFrame, min_bars: int = 200) -> bool:
    """Validate DataFrame has sufficient data for analysis"""
    if df.empty:
        return False
    
    if len(df) < min_bars:
        return False
    
    # Check for required columns
    required = ['date', 'open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required):
        return False
    
    # Check for reasonable price data
    if (df[['open', 'high', 'low', 'close']] <= 0).any().any():
        return False
    
    # Check price relationships
    if (df['high'] < df['low']).any():
        return False
    
    if (df['high'] < df[['open', 'close']].max(axis=1)).any():
        return False
    
    if (df['low'] > df[['open', 'close']].min(axis=1)).any():
        return False
    
    return True


def add_basic_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add commonly used indicators to DataFrame"""
    df = df.copy()
    
    # Moving averages
    df['sma_20'] = sma(df['close'], 20)
    df['sma_50'] = sma(df['close'], 50)
    df['sma_150'] = sma(df['close'], 150)
    df['sma_200'] = sma(df['close'], 200)
    
    # Volume indicators
    df['vol_sma_20'] = sma(df['volume'], 20)
    df['vol_ratio'] = df['volume'] / df['vol_sma_20']
    
    # Volatility
    df['atr_14'] = atr(df, 14)
    
    # Price position relative to moving averages
    df['price_vs_sma150'] = (df['close'] - df['sma_150']) / df['sma_150']
    df['price_vs_sma200'] = (df['close'] - df['sma_200']) / df['sma_200']
    
    return df
