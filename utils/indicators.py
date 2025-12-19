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

from sklearn.cluster import KMeans
from scipy.signal import argrelextrema

def find_key_levels(close_series, volume_series=None, window=10, n_clusters=6):
    """
    Find key Support and Resistance levels using K-Means Clustering on Pivots.
    
    Args:
        close_series (pd.Series): Closing prices.
        volume_series (pd.Series): Volume data (optional).
        window (int): Window for pivot detection.
        n_clusters (int): Number of price clusters to identify.
        
    Returns:
        List[dict]: [{'price': float, 'type': 'support'|'resistance', 'strength': float}]
    """
    if close_series.empty:
        return []
        
    prices = close_series.values
    n = len(prices)
    
    # 1. Find Pivots (Local Minima and Maxima)
    # iloc indices
    max_idx = argrelextrema(prices, np.greater, order=window)[0]
    min_idx = argrelextrema(prices, np.less, order=window)[0]
    
    pivots = []
    
    for idx in max_idx:
        pivots.append({
            'index': idx,
            'price': prices[idx],
            'type': 'resistance',
            'volume': volume_series.iloc[idx] if volume_series is not None else 1
        })
        
    for idx in min_idx:
        pivots.append({
            'index': idx,
            'price': prices[idx],
            'type': 'support',
            'volume': volume_series.iloc[idx] if volume_series is not None else 1
        })
        
    if not pivots:
        return []
        
    # 2. Prepare Data for Clustering
    # We cluster on Price primarily.
    pivot_data = pd.DataFrame(pivots)
    X = pivot_data[['price']].values
    
    # Adaptive K: If we don't have enough pivots, reduce K
    k = min(n_clusters, len(pivots))
    if k < 1: 
        return []
        
    kmeans = KMeans(n_clusters=k, n_init=10, random_state=42)
    kmeans.fit(X)
    
    pivot_data['cluster'] = kmeans.labels_
    
    # 3. Analyze Clusters
    key_levels = []
    current_price = prices[-1]
    
    for cluster_id in range(k):
        cluster_points = pivot_data[pivot_data['cluster'] == cluster_id]
        if cluster_points.empty:
            continue
            
        # Weighted Average Level based on Volume and Recency
        # Recency: Higher weight for higher index
        # Weight = Volume * (Index / N)^2  (Exponential decay impact)
        
        # Normalize index to 0-1
        cluster_points = cluster_points.copy()
        cluster_points['recency'] = cluster_points['index'] / n
        cluster_points['weight'] = cluster_points['volume'] * (cluster_points['recency'] ** 2)
        
        # If weights are zero (e.g. index 0), handle
        total_weight = cluster_points['weight'].sum()
        if total_weight == 0:
            avg_price = cluster_points['price'].mean()
        else:
            avg_price = (cluster_points['price'] * cluster_points['weight']).sum() / total_weight
            
        # Strength = Sum of weights (Volume + Recency support validity)
        # Normalize strength for UI (1-10 scale approximation?)
        # Let's just use raw score relative to others, or count
        count = len(cluster_points)
        strength = count # Simple touch count for now, robustness
        
        # Determine Major Type (Support or Resistance relative to CURRENT PRICE)
        # Traditionally, below = Support, above = Resistance
        level_type = 'support' if avg_price < current_price else 'resistance'
        
        key_levels.append({
            'price': float(avg_price),
            'type': level_type,
            'strength': int(strength),
            'touches': int(count)
        })
        
    # Sort by price
    key_levels.sort(key=lambda x: x['price'])
    
    return key_levels
