
import pandas as pd
import numpy as np
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.indicators import find_key_levels

def run_verification():
    print("Verifying Support/Resistance Algorithm...")
    
    # Generate mock data: Sine wave with some noise to simulate price
    t = np.linspace(0, 100, 200)
    prices = 100 + 10 * np.sin(t/5) + np.random.normal(0, 1, 200)
    volumes = np.random.randint(1000, 50000, 200)
    
    close_series = pd.Series(prices)
    volume_series = pd.Series(volumes)
    
    print(f"Mock Data Created. Length: {len(close_series)}")
    
    try:
        levels = find_key_levels(close_series, volume_series, window=10, n_clusters=5)
        
        print(f"Algorithm returned {len(levels)} levels.")
        
        for i, lvl in enumerate(levels):
            print(f"Level {i+1}: Price={lvl['price']:.2f}, Type={lvl['type']}, Strength={lvl['strength']}, Touches={lvl['touches']}")
            
        if len(levels) > 0:
            print("\nSUCCESS: Levels detected.")
        else:
            print("\nWARNING: No levels detected (might be due to random data matching).")
            
    except Exception as e:
        print(f"\nERROR: Algorithm failed with exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_verification()
