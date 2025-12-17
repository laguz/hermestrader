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
