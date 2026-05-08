import numpy as np
from scipy.stats import norm

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
    print(f"DEBUG: score={score}, l_base={l_base}, l_xgb={l_xgb}, rv={rv}, prot={protection_score}")
    return float(1 / (1 + np.exp(-score)))

# Test case: AAPL at 180, strike at 170 (support)
current_price = 180.0
strike = 170.0
sigma = 0.25
t_years = 45 / 365
z = np.log(strike / current_price) / (sigma * np.sqrt(t_years))
delta = norm.cdf(z) 
print(f"DEBUG: z={z}, delta={delta}")

weights = [0.0, 1.0, 0.6, 0.3, 0.4]
pop = predict_single_pop(delta, 0.25, 0.20, 0.5, 1.0, weights)
print(f"POP: {pop}")
