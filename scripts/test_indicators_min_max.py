import unittest
import pandas as pd
import numpy as np

# Import the function to test
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.indicators import find_key_levels

class TestIndicatorsMinMax(unittest.TestCase):
    def test_find_key_levels_with_min_max(self):
        # Create synthetic data
        # periods=10
        # Close: flat 100
        # High: max 110 at index 5
        # Low: min 90 at index 2
        
        close = pd.Series([100] * 10)
        high = pd.Series([100] * 10)
        low = pd.Series([100] * 10)
        volume = pd.Series([1000] * 10)
        
        high[5] = 110
        low[2] = 90
        
        # Call function
        levels = find_key_levels(close, volume, high_series=high, low_series=low, window=5)
        
        print("Found Levels:", levels)
        
        # Verify Min Support
        # Logic says: series_min = low.min() -> 90.0
        # type='support', strength=3
        
        min_support = next((l for l in levels if l['price'] == 90.0), None)
        self.assertIsNotNone(min_support, "Min support (90.0) not found")
        self.assertEqual(min_support['type'], 'support')
        self.assertEqual(min_support['strength'], 3)
        
        # Verify Max Resistance
        # Logic says: series_max = high.max() -> 110.0
        # type='resistance', strength=3
        
        max_resistance = next((l for l in levels if l['price'] == 110.0), None)
        self.assertIsNotNone(max_resistance, "Max resistance (110.0) not found")
        self.assertEqual(max_resistance['type'], 'resistance')
        self.assertEqual(max_resistance['strength'], 3)

    def test_find_key_levels_fallback(self):
        # Test without high/low series (should fallback to close)
        close = pd.Series([100, 95, 105, 100])
        
        levels = find_key_levels(close)
        
        # Min=95, Max=105
        # Note: Clustering might find 95 as a level with lower strength.
        # We want to ensure our explicit High Strength (3) level exists.
        
        min_levels = [l for l in levels if l['price'] == 95.0]
        strong_min = next((l for l in min_levels if l['strength'] == 3), None)
        self.assertIsNotNone(strong_min, "Fallback Min (95.0) with strength 3 not found")
        
        max_levels = [l for l in levels if l['price'] == 105.0]
        strong_max = next((l for l in max_levels if l['strength'] == 3), None)
        self.assertIsNotNone(strong_max, "Fallback Max (105.0) with strength 3 not found")

if __name__ == '__main__':
    unittest.main()
