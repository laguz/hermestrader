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

def find_key_levels(series, window=5, threshold=0.03):
    """
    Find key Support and Resistance levels based on local minima and maxima.
    Returns: [{'price': float, 'type': 'support'|'resistance', 'strength': int}]
    """
    levels = []
    
    # Simple local extrema detection using rolling window
    # We check if a point is the min/max of its neighborhood (+/- window)
    
    # Convert series to list for easier indexing if needed, or iterate
    prices = series.values
    n = len(prices)
    
    raw_levels = []
    
    for i in range(window, n - window):
        segment = prices[i-window : i+window+1]
        center_val = prices[i]
        
        # Local Max (Resistance)
        if center_val == max(segment):
             raw_levels.append({'price': center_val, 'type': 'resistance'})
             
        # Local Min (Support)
        elif center_val == min(segment):
             raw_levels.append({'price': center_val, 'type': 'support'})
             
    # Cluster levels
    # We will sort by price and group those within 'threshold' percent
    if not raw_levels:
        return []
        
    raw_levels.sort(key=lambda x: x['price'])
    
    grouped_levels = []
    current_group = [raw_levels[0]]
    
    for i in range(1, len(raw_levels)):
        lvl = raw_levels[i]
        last_lvl_avg = np.mean([x['price'] for x in current_group])
        
        # If within threshold% of the group average
        if abs(lvl['price'] - last_lvl_avg) / last_lvl_avg <= threshold:
            current_group.append(lvl)
        else:
            # Process current group
            avg_price = np.mean([x['price'] for x in current_group])
            # Determine type by majority vote or just count
            res_count = sum(1 for x in current_group if x['type'] == 'resistance')
            sup_count = sum(1 for x in current_group if x['type'] == 'support')
            
            l_type = 'resistance' if res_count > sup_count else 'support'
            if res_count == sup_count: l_type = 'pivot' # Mixed interest
            
            grouped_levels.append({
                'price': float(avg_price),
                'type': l_type,
                'strength': len(current_group)
            })
            current_group = [lvl]
            
    # Process last group
    if current_group:
        avg_price = np.mean([x['price'] for x in current_group])
        res_count = sum(1 for x in current_group if x['type'] == 'resistance')
        sup_count = sum(1 for x in current_group if x['type'] == 'support')
        l_type = 'resistance' if res_count > sup_count else 'support'
        grouped_levels.append({
            'price': float(avg_price),
            'type': l_type,
            'strength': len(current_group)
        })
        
    # Sort by strength descending, then filter? Or return sorted by price?
    # Usually users want to find nearest levels. Sorting by price makes sense for display.
    # But usually we want "Key" levels implies Strong levels.
    # Let's return sorted by price, the UI can filter or highlight.
    
    grouped_levels.sort(key=lambda x: x['price'])
    
    return grouped_levels
