import sys
import os

# Mock the environment
os.environ["HERMES_DSN"] = "sqlite:///:memory:"

# Add project root to path
sys.path.append(os.getcwd())

from hermes.ml.pop_engine import augment_levels_with_pop

def test_pop_logic():
    # 1. Bullish Prediction
    xgb_pred_bullish = {"predicted_return": 0.05} # +5%
    
    analysis = {
        "symbol": "TEST",
        "current_price": 100.0,
        "current_vol": 0.20,
        "avg_vol": 0.20,
        "key_levels": [
            {"price": 90.0, "type": "support", "strength": 5},
            {"price": 110.0, "type": "resistance", "strength": 5}
        ]
    }
    
    print("Testing with Bullish Prediction (+5%)...")
    result_bullish = augment_levels_with_pop(analysis.copy(), xgb_pred_bullish, period="6m")
    
    pop_support = result_bullish["key_levels"][0]["pop"]
    pop_resistance = result_bullish["key_levels"][1]["pop"]
    
    print(f"Support (Put) POP: {pop_support:.1%}")
    print(f"Resistance (Call) POP: {pop_resistance:.1%}")
    
    # 2. Bearish Prediction
    xgb_pred_bearish = {"predicted_return": -0.05} # -5%
    print("\nTesting with Bearish Prediction (-5%)...")
    result_bearish = augment_levels_with_pop(analysis.copy(), xgb_pred_bearish, period="6m")
    
    pop_support_bear = result_bearish["key_levels"][0]["pop"]
    pop_resistance_bear = result_bearish["key_levels"][1]["pop"]
    
    print(f"Support (Put) POP: {pop_support_bear:.1%}")
    print(f"Resistance (Call) POP: {pop_resistance_bear:.1%}")
    
    # Assertions
    # In bullish case, Put POP should be HIGHER than in bearish case
    assert pop_support > pop_support_bear, "Bullish prediction should increase Put POP"
    # In bullish case, Call POP should be LOWER than in bearish case
    assert pop_resistance < pop_resistance_bear, "Bullish prediction should decrease Call POP"
    
    print("\n✅ Verification SUCCESS: Side-aware POP logic is working correctly.")

if __name__ == "__main__":
    try:
        test_pop_logic()
    except Exception as e:
        print(f"\n❌ Verification FAILED: {e}")
        sys.exit(1)
