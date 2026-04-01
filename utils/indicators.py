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

def calculate_ema(series, span=50):
    """Calculate Exponential Moving Average."""
    return series.ewm(span=span, adjust=False).mean()

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

def find_key_levels(close_series, volume_series=None, high_series=None, low_series=None, window=5, n_clusters=6):
    """
    Find key Support and Resistance levels using K-Means Clustering on Pivots.
    
    Args:
        close_series (pd.Series): Closing prices.
        volume_series (pd.Series): Volume data (optional).
        high_series (pd.Series): High prices (optional, for period max).
        low_series (pd.Series): Low prices (optional, for period min).
        window (int): Window for pivot detection.
        n_clusters (int): Number of price clusters to identify.
        n_clusters (int): Number of price clusters to identify.
        
    Returns:
        List[dict]: [{'price': float, 'type': 'support'|'resistance', 'strength': float}]
    """
    if close_series.empty:
        print(f"DEBUG: find_key_levels called with EMPTY series")
        return []
    
    print(f"DEBUG: find_key_levels called with {len(close_series)} points")
        
    prices = close_series.values
    n = len(prices)
    
    # 1. Find Pivots (Local Minima and Maxima)
    # iloc indices
    max_idx = argrelextrema(prices, np.greater, order=window)[0]
    min_idx = argrelextrema(prices, np.less, order=window)[0]
    
    # print(f"DEBUG: KeyLevels - Prices Len: {len(prices)}, MaxIdx: {len(max_idx)}, MinIdx: {len(min_idx)}")
    print(f"DEBUG: KeyLevels - Prices Len: {len(prices)}, MaxIdx: {len(max_idx)}, MinIdx: {len(min_idx)}")
    
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
        
    
    # Ensure we have at least one Support and one Resistance
    has_support = any(p['type'] == 'support' for p in pivots)
    has_resistance = any(p['type'] == 'resistance' for p in pivots)
    
    avg_vol = volume_series.mean() if volume_series is not None else 1
    
    if not has_support:
        min_p_idx = np.argmin(prices)
        pivots.append({
            'index': min_p_idx,
            'price': prices[min_p_idx],
            'type': 'support',
            'volume': avg_vol
        })
        
    if not has_resistance:
        max_p_idx = np.argmax(prices)
        pivots.append({
            'index': max_p_idx,
            'price': prices[max_p_idx],
            'type': 'resistance',
            'volume': avg_vol
        })
        
        
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
    
    # 4. Add Period Min/Max as specific Key Levels
    # Use provided high/low or fallback to close
    series_min = low_series.min() if low_series is not None else close_series.min()
    series_max = high_series.max() if high_series is not None else close_series.max()
    
    # Add Min as Support
    key_levels.append({
        'price': float(series_min),
        'type': 'support',
        'strength': 3, # High strength for period extreme
        'touches': 1
    })
    
    # Add Max as Resistance
    key_levels.append({
        'price': float(series_max),
        'type': 'resistance',
        'strength': 3, # High strength for period extreme
        'touches': 1
    })
    
    # Re-sort to include new levels
    key_levels.sort(key=lambda x: x['price'])
    
    return key_levels

from scipy.stats import norm

def calculate_historical_volatility(close_series, window=252):
    """
    Calculate Annualized Historical Volatility.
    """
    log_returns = np.log(close_series / close_series.shift(1))
    volatility = log_returns.rolling(window=window).std() * np.sqrt(252)
    return volatility.iloc[-1]

def calculate_prob_it_expires_otm(current_price, strike_price, volatility, days_to_expiry=30, risk_free_rate=0.04, credit=0.0):
    """
    Calculate the Probability of Profit (Probability it expires OTM).
    Uses the d2 formula from Black-Scholes for a more professional estimate.
    
    Args:
        current_price (float): Current underlying price.
        strike_price (float): Strike price of the option.
        volatility (float): Annualized Implied (or Historical) Volatility (e.g., 0.25 for 25%).
        days_to_expiry (int): Days until expiration.
        risk_free_rate (float): Annual risk-free rate (default 4%).
        credit (float): Credit received (shifts break-even).
        
    Returns:
        float: Probability (0 to 1).
    """
    if volatility <= 0 or days_to_expiry <= 0:
        return 0.5
        
    # Adjustment for Bull Put Credit Spread: Break-even = Strike - Credit
    # For Bear Call: Break-even = Strike + Credit
    if strike_price < current_price: # Put
        break_even = strike_price - credit
    else: # Call
        break_even = strike_price + credit

    t = days_to_expiry / 365.0
    
    # Professional d2 formula:
    # d2 = [ln(S/K) + (r - q - 0.5*sigma^2)T] / (sigma * sqrt(T))
    # We ignore dividend yield (q) for simplicity here.
    
    num = np.log(current_price / break_even) + (risk_free_rate - 0.5 * volatility**2) * t
    denom = volatility * np.sqrt(t)
    d2 = num / denom
    
    if strike_price < current_price:
        # Put: We want Price > Break-even at expiry.
        # N(d2) is the probability that Price > K.
        prob_otm = norm.cdf(d2)
    else:
        # Call: We want Price < Break-even at expiry.
        # N(-d2) is the probability that Price < K.
        prob_otm = norm.cdf(-d2)
        
    return prob_otm

def calculate_prob_of_touch(current_price, strike_price, volatility, days_to_expiry=30, risk_free_rate=0.04):
    """
    Calculate the Probability of the stock price 'touching' the strike price 
    at any point before expiration.
    Approximation: POT = 2 * P(ITM) = 2 * (1 - POP)
    """
    prob_otm = calculate_prob_it_expires_otm(current_price, strike_price, volatility, days_to_expiry, risk_free_rate, credit=0)
    prob_itm = 1 - prob_otm
    pot = min(1.0, 2.0 * prob_itm)
    return pot

def calculate_option_price(current_price, strike_price, time_to_expiry_years, volatility, risk_free_rate=0.04, option_type='call'):
    """
    Calculate Option Price using Black-Scholes Formula.
    """
    if time_to_expiry_years <= 0:
        # Intrinsic Value
        if option_type == 'call':
            return max(0, current_price - strike_price)
        else:
            return max(0, strike_price - current_price)
            
    d1 = (np.log(current_price / strike_price) + (risk_free_rate + 0.5 * volatility ** 2) * time_to_expiry_years) / (volatility * np.sqrt(time_to_expiry_years))
    d2 = d1 - volatility * np.sqrt(time_to_expiry_years)
    
    if option_type == 'call':
        price = (current_price * norm.cdf(d1)) - (strike_price * np.exp(-risk_free_rate * time_to_expiry_years) * norm.cdf(d2))
    else:
        price = (strike_price * np.exp(-risk_free_rate * time_to_expiry_years) * norm.cdf(-d2)) - (current_price * norm.cdf(-d1))
        
    return price

def calculate_adx(high, low, close, period=14):
    """
    Calculate Average Directional Index (ADX).
    """
    plus_dm = high.diff()
    minus_dm = low.diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    
    tr1 = pd.DataFrame(high - low)
    tr2 = pd.DataFrame(abs(high - close.shift(1)))
    tr3 = pd.DataFrame(abs(low - close.shift(1)))
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis=1, join='outer').max(axis=1)
    
    atr = tr.rolling(period).mean()
    
    plus_di = 100 * (plus_dm.ewm(alpha=1/period).mean() / atr)
    minus_di = 100 * (abs(minus_dm).ewm(alpha=1/period).mean() / atr)
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = ((dx.shift(1) * (period - 1)) + dx) / period
    adx_smooth = adx.ewm(alpha=1/period).mean()
    return adx_smooth

def calculate_hv_rank(close_series, window=30, lookback=252):
    """
    Calculate the Percentile Rank of current Historical Volatility 
    compared to the last 'lookback' days.
    """
    log_returns = np.log(close_series / close_series.shift(1))
    # Rolling annualized volatility
    rolling_vol = log_returns.rolling(window=window).std() * np.sqrt(252)
    
    # Get the last 'lookback' days of volatility
    history = rolling_vol.tail(lookback).dropna()
    
    if history.empty:
        return 50 # Default middle
        
    current_vol = rolling_vol.iloc[-1]
    
    # Calculate percentile rank (0-100)
    from scipy import stats
    return stats.percentileofscore(history, current_vol)

def calculate_obv(close, volume):
    """
    Calculate On-Balance Volume (OBV).
    """
    # OBV = Previous OBV + Volume if Close > Previous Close
    # OBV = Previous OBV - Volume if Close < Previous Close
    # OBV = Previous OBV if Close = Previous Close
    
    close_diff = close.diff()
    direction = pd.Series(0, index=close.index)
    direction[close_diff > 0] = 1
    direction[close_diff < 0] = -1
    
    obv = (volume * direction).cumsum()
    return obv

def calculate_vwap(high, low, close, volume, window=14):
    """
    Calculate Rolling Volume Weighted Average Price (VWAP).
    """
    typical_price = (high + low + close) / 3
    
    # Cumulative sum of Typical Price * Volume
    tp_v = typical_price * volume
    
    # Rolling sum
    rolling_tp_v = tp_v.rolling(window=window).sum()
    rolling_vol = volume.rolling(window=window).sum()
    
    return rolling_tp_v / rolling_vol

