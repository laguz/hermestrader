import numpy as np
from scipy.stats import norm

# Standard Institutional Weights
weights = [0.0, 1.0, 0.6, 0.3, 0.4]

def calculate_log_odds(probability: float) -> float:
    p = np.clip(probability, 0.01, 0.99)
    return float(np.log(p / (1 - p)))

def predict_single_pop(delta: float, current_vol: float, avg_vol: float, xgb_prob: float, protection_score: float, weights: list) -> float:
    p_base = 1.0 - abs(delta)
    rv = current_vol / (avg_vol + 1e-5) 
    l_base = calculate_log_odds(p_base)
    l_xgb = calculate_log_odds(xgb_prob)
    beta_0, beta_1, beta_2, beta_3, beta_4 = weights
    score = beta_0 + (beta_1 * l_base) + (beta_2 * l_xgb) + (beta_3 * rv) + (beta_4 * protection_score)
    return float(1 / (1 + np.exp(-score)))

def calculate_strike_protection(key_levels: list, current_price: float, short_strike: float, side: str) -> float:
    protection_score = 0.0
    for level in key_levels:
        if side == 'put' and level['type'] == 'support':
            if short_strike < level['price'] < current_price:
                distance = current_price - level['price']
                protection_score += level['strength'] * (1 / max(distance, 0.1)) 
        elif side == 'call' and level['type'] == 'resistance':
            if current_price < level['price'] < short_strike:
                distance = level['price'] - current_price
                protection_score += level['strength'] * (1 / max(distance, 0.1))
    return 1.0 + (protection_score * 0.1)

# TEST DATA
current_price = 276.83
vol = 0.25 # Assumption
avg_vol = 0.22 # Assumption
t_years = 45 / 365
key_levels = [
    {'price': 273.52, 'type': 'support', 'strength': 5},
    {'price': 266.51, 'type': 'support', 'strength': 2},
    {'price': 255.78, 'type': 'support', 'strength': 1},
    {'price': 247.40, 'type': 'support', 'strength': 3},
    {'price': 279.83, 'type': 'resistance', 'strength': 2},
    {'price': 286.19, 'type': 'resistance', 'strength': 1}
]

print(f"--- AAPL CS75 Simulation (Spot: ${current_price}) ---")

# We simulate strikes for a Put Credit Spread
strikes_to_test = [275, 272.5, 270, 267.5, 265, 262.5, 260]

for s in strikes_to_test:
    z = np.log(s / current_price) / (vol * np.sqrt(t_years))
    delta = norm.cdf(z)
    prot = calculate_strike_protection(key_levels, current_price, s, 'put')
    pop = predict_single_pop(delta, vol, avg_vol, 0.5, prot, weights)
    print(f"Strike: ${s} | Delta: {delta:.2f} | Protection: {prot:.2f} | POP: {pop*100:.1f}%")
