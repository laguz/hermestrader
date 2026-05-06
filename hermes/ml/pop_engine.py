import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
from scipy.stats import norm
from sklearn.cluster import KMeans
from typing import Dict, List, Any

# Institutional Default Weights
# Format: [beta_0 (Intercept), beta_1 (Delta), beta_2 (XGB), beta_3 (Vol), beta_4 (Protection)]
DEFAULT_REGIME_WEIGHTS = {
    '3M': [0.0, 1.0, 0.6, 0.3, 0.4],
    '6M': [0.0, 1.0, 0.6, 0.3, 0.4],
    '1Y': [0.0, 1.0, 0.6, 0.3, 0.4],
}

def find_key_levels(close_series: pd.Series, volume_series: pd.Series, window: int = 5, n_clusters: int = 6) -> List[Dict[str, Any]]:
    """
    Finds key S/R levels using K-Means Clustering on Pivots.
    """
    prices = close_series.values
    volumes = volume_series.values
    n = len(prices)
    if n == 0:
        return []
    
    current_price = prices[-1]
    
    # 1. Find Pivots (Local Minima and Maxima)
    max_idx = argrelextrema(prices, np.greater, order=window)[0]
    min_idx = argrelextrema(prices, np.less, order=window)[0]
    all_pivots_idx = np.sort(np.concatenate((max_idx, min_idx)))
    
    if len(all_pivots_idx) == 0:
        return []

    # Build Pivot DataFrame
    pivot_data = pd.DataFrame({
        'index': all_pivots_idx,
        'price': prices[all_pivots_idx],
        'volume': volumes[all_pivots_idx]
    })

    # 2. Prepare Data for Clustering
    X = pivot_data[['price']].values
    k = min(n_clusters, len(pivot_data))
    
    if k == 0: return []
    
    kmeans = KMeans(n_clusters=k, n_init=10, random_state=42)
    pivot_data['cluster'] = kmeans.fit_predict(X)
    
    key_levels = []
    
    # 3. Analyze Clusters and calculate Weighted Average Level
    for cluster_id in range(k):
        cluster_points = pivot_data[pivot_data['cluster'] == cluster_id].copy()
        
        # Weight = Volume * (Recency^2) -> Higher weight to recent, high-volume pivots
        # Note: avoid DivisionByZero by ensuring n > 0
        cluster_points['weight'] = cluster_points['volume'] * ((cluster_points['index'] / max(n, 1)) ** 2)
        total_weight = cluster_points['weight'].sum()
        
        if total_weight == 0: continue
            
        avg_price = (cluster_points['price'] * cluster_points['weight']).sum() / total_weight
        
        # Determine Type
        level_type = 'support' if avg_price < current_price else 'resistance'
        
        key_levels.append({
            'price': float(avg_price),
            'type': level_type,
            'strength': len(cluster_points) # Number of "touches"
        })
        
    return key_levels

def calculate_strike_protection(key_levels: List[Dict[str, Any]], current_price: float, short_strike: float, spread_type: str) -> float:
    """
    Calculates a numerical score representing how well a short strike is protected by S/R clusters.
    spread_type: 'put_credit' or 'call_credit'
    """
    protection_score = 0.0
    
    for level in key_levels:
        # For a Put Credit Spread, we want Support levels ABOVE our short strike and BELOW current price
        if spread_type == 'put_credit' and level['type'] == 'support':
            if short_strike < level['price'] < current_price:
                # Add score based on strength. Close levels offer better protection.
                distance = current_price - level['price']
                protection_score += level['strength'] * (1 / max(distance, 0.1)) 
                
        # For a Call Credit Spread, we want Resistance levels BELOW our short strike and ABOVE current price
        elif spread_type == 'call_credit' and level['type'] == 'resistance':
            if current_price < level['price'] < short_strike:
                distance = level['price'] - current_price
                protection_score += level['strength'] * (1 / max(distance, 0.1))

    # Normalize to a baseline of 1.0 (to fit smoothly into the log-odds equation)
    # 1.0 = No protection, >1.0 = Strong protection
    return 1.0 + (protection_score * 0.1)

def calculate_log_odds(probability: float) -> float:
    p = np.clip(probability, 0.01, 0.99)
    return float(np.log(p / (1 - p)))

def predict_single_pop(delta: float, current_vol: float, avg_vol: float, xgb_prob: float, protection_score: float, weights: List[float], side: str = 'put') -> float:
    p_base = 1.0 - abs(delta)
    rv = current_vol / (avg_vol + 1e-5) 
    
    l_base = calculate_log_odds(p_base)
    l_xgb = calculate_log_odds(xgb_prob)
    
    if side == 'call':
        l_xgb = -l_xgb
    
    # Unpack the 5 weights (Intercept, Delta, XGB, Vol, Protection)
    beta_0, beta_1, beta_2, beta_3, beta_4 = weights
    
    # Calculate the new score
    score = beta_0 + (beta_1 * l_base) + (beta_2 * l_xgb) + (beta_3 * rv) + (beta_4 * protection_score)
    
    return float(1 / (1 + np.exp(-score)))

def generate_regime_pops(delta: float, current_vol: float, vol_sma_21: float, protection_score: float, xgb_preds: Dict[str, float], regime_weights: Dict[str, List[float]] = DEFAULT_REGIME_WEIGHTS, side: str = 'put') -> Dict[str, float]:
    timeframes = ['3M', '6M', '1Y']
    results = {}
    for tf in timeframes:
        pop = predict_single_pop(
            delta, 
            current_vol, 
            vol_sma_21, 
            xgb_preds.get(tf, 0.5), # Default to neutral probability if not provided
            protection_score, 
            regime_weights.get(tf, DEFAULT_REGIME_WEIGHTS['3M']),
            side=side
        )
        results[tf] = pop
    return results

def augment_levels_with_pop(analysis: Dict[str, Any], xgb_pred: Dict[str, Any], period: str = "6m") -> Dict[str, Any]:
    """
    Injects estimated POP calculations into key levels data.
    Standardizes the logic used by the analytics dashboard and strategies.
    """
    current_price = float(analysis.get("current_price", 0))
    current_vol = float(analysis.get("current_vol", 0.30))
    avg_vol = float(analysis.get("avg_vol", 0.25))
    key_levels = analysis.get("key_levels", [])
    
    # Clean NaN values
    if np.isnan(current_vol) or current_vol <= 0: current_vol = 0.30
    if np.isnan(avg_vol) or avg_vol <= 0: avg_vol = 0.25
    if np.isnan(current_price) or current_price <= 0: return analysis

    pred_ret = float(xgb_pred.get("predicted_return", 0.0))
    if np.isnan(pred_ret): pred_ret = 0.0
    
    # Convert predicted return to a rough probability (0.5 + return * 5, clipped to 0.01-0.99)
    xgb_prob = max(0.01, min(0.99, 0.5 + (pred_ret * 5)))
    
    # Strategy Alignment: 3M horizon = 7 DTE (CS7), 6M/1Y = 45 DTE (CS75)
    target_dte = 7 if period.lower() == "3m" else 45
    t_years = target_dte / 365
    sigma = max(0.05, current_vol)
    
    # Weight Alignment: 3M, 6M, or 1Y
    weight_key = period.upper()
    weights = DEFAULT_REGIME_WEIGHTS.get(weight_key, [0.0, 1.0, 0.6, 0.3, 0.4])

    for level in key_levels:
        strike = float(level.get("price", 0))
        if strike <= 0 or np.isnan(strike): continue
            
        # 1. Estimate Delta / Baseline POP (Probability OTM)
        try:
            z = np.log(strike / current_price) / (sigma * np.sqrt(t_years))
            p_base = float(norm.cdf(abs(z)))
            if np.isnan(p_base): p_base = 0.84
            delta_est = 1.0 - p_base
        except Exception:
            p_base = 0.84
            delta_est = 0.16
            
        # 2. Calculate Protection Score
        side = 'put' if level.get("type") == "support" else 'call'
        spread_type = f"{side}_credit"
        
        prot_score = calculate_strike_protection(
            key_levels, current_price, strike, spread_type
        )
        if np.isnan(prot_score): prot_score = 1.0
        
        # 3. Predict POP using Regime Weights
        # Pass the side explicitly to ensure XGB probability is correctly signed
        pop = predict_single_pop(delta_est, current_vol, avg_vol, xgb_prob, prot_score, weights, side=side)
        
        if np.isnan(pop): pop = p_base
        
        level["pop"] = float(pop)
        level["p_base"] = float(p_base)
        
    return analysis
