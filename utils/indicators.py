import pandas as pd
import numpy as np

def calculate_rsi(series, period=14):
    """
    Calculate Relative Strength Index (RSI).
    """
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    # Fill NaN with 50 (neutral) to avoid breaking early logic
    return rsi.fillna(50)

def calculate_bollinger_bands(series, window=20, num_std=2):
    """
    Calculate Bollinger Bands.
    Returns: (Upper Band, Middle Band, Lower Band)
    """
    rolling_mean = series.rolling(window=window).mean()
    rolling_std = series.rolling(window=window).std()
    
    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)
    
    return upper_band, rolling_mean, lower_band

def calculate_support_resistance(series, window=20):
    """
    Calculate dynamic Support and Resistance based on rolling min/max.
    """
    resistance = series.rolling(window=window).max()
    support = series.rolling(window=window).min()
    return support, resistance

def calculate_sma(series, window=50):
    """Calculate Simple Moving Average."""
    return series.rolling(window=window).mean()

def calculate_macd(series, fast=12, slow=26, signal=9):
    """
    Calculate MACD.
    Returns: (macd_line, signal_line, histogram)
    """
    exp1 = series.ewm(span=fast, adjust=False).mean()
    exp2 = series.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    return macd, signal_line, histogram

def calculate_atr(high, low, close, window=14):
    """Calculate Average True Range."""
    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close = (low - close.shift()).abs()
    
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    
    atr = true_range.rolling(window=window).mean()
    return atr
