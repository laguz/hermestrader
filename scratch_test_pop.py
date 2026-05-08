import numpy as np
import pandas as pd
from hermes.ml.pop_engine import find_key_levels, calculate_strike_protection, generate_regime_pops

def test_pop_engine():
    print("Testing POP Engine...")
    
    # Generate some synthetic price data with clear S/R levels
    np.random.seed(42)
    # Create a trend that bounces between 100 and 120
    prices = []
    vols = []
    base_price = 110
    for i in range(100):
        # Sine wave + noise to create local minima/maxima
        p = base_price + 10 * np.sin(i * 0.5) + np.random.normal(0, 1)
        prices.append(p)
        vols.append(np.random.randint(100, 1000))
        
    close_series = pd.Series(prices)
    volume_series = pd.Series(vols)
    current_price = prices[-1]
    
    print(f"Current Price: {current_price:.2f}")
    
    # 1. Find Key Levels
    key_levels = find_key_levels(close_series, volume_series)
    print("\nKey Levels Found:")
    for level in key_levels:
        print(f"  - Price: {level['price']:.2f}, Type: {level['type']}, Strength: {level['strength']}")
        
    # 2. Calculate Strike Protection
    # Assume a put credit spread, strike 95 (well below the 100 support)
    short_strike_put = 95.0
    prot_score_put = calculate_strike_protection(key_levels, current_price, short_strike_put, 'put_credit')
    print(f"\nProtection Score for Put Strike {short_strike_put}: {prot_score_put:.2f}")
    
    # Assume a call credit spread, strike 125 (well above the 120 resistance)
    short_strike_call = 125.0
    prot_score_call = calculate_strike_protection(key_levels, current_price, short_strike_call, 'call_credit')
    print(f"Protection Score for Call Strike {short_strike_call}: {prot_score_call:.2f}")
    
    # 3. Generate Regime POPs
    delta = 0.16
    current_vol = 0.25
    avg_vol = 0.20
    xgb_preds = {'3M': 0.65, '6M': 0.60, '1Y': 0.55}
    
    pops = generate_regime_pops(delta, current_vol, avg_vol, prot_score_put, xgb_preds)
    print("\nGenerated POPs for Put Credit Spread:")
    for tf, pop in pops.items():
        print(f"  {tf}: {pop:.2%}")

if __name__ == "__main__":
    test_pop_engine()
