
import sys
import os
import unittest
from unittest.mock import MagicMock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.analysis_service import AnalysisService

class TestEntryPoints(unittest.TestCase):
    def setUp(self):
        self.mock_tradier = MagicMock()
        self.mock_ml_service = MagicMock()
        self.service = AnalysisService(self.mock_tradier, self.mock_ml_service)

    def test_entry_point_rounding(self):
        # 1. Mock Data Setup
        # Create data that will result in specific key levels
        # We need enough data for find_key_levels to work (window=5)
        # We'll construct a Price series with local minima/maxima at known values
        
        dates = pd.date_range(end=datetime.now(), periods=100)
        
        # Scenario 1: Prices > 100 -> Round to nearest 5
        # Scenario 2: Prices < 100 -> Round to nearest 1
        
        # Let's just mock find_key_levels in utils.indicators if it's easier, 
        # but integration testing is better.
        # However, to control find_key_levels output via raw price data is reliable but tedious.
        # Let's instead test the _logic_ we are going to add or just mock the dependencies of the service.
        
        # Actually, since I'm modifying analyze_symbol, I can't easily mock internal variables.
        # But I can rely on the fact that analyze_symbol calls find_key_levels.
        # Let's mock find_key_levels to return specific levels and check if the service rounds them correctly.
        
        pass

    def test_rounding_logic_direct(self):
        # Since I can't easily injection-mock the internal key_levels without patching,
        # I will test the method behavior assuming I feed it data that *would* generate those levels?
        # No, that's too indirect.
        
        # I'll just patch 'utils.indicators.find_key_levels' to return exact levels I want to test.
        # Then I verify the 'entry_points' in the result.
        
        with unittest.mock.patch('utils.indicators.find_key_levels') as mock_find:
            # high price > 100, low price < 100
            mock_find.return_value = [
                {'price': 153.20, 'type': 'resistance'}, # Should round to 155
                {'price': 148.80, 'type': 'support'},    # Should round to 150
                {'price': 102.40, 'type': 'support'},    # Should round to 100
                {'price': 98.70, 'type': 'resistance'},  # Should round to 99
                {'price': 45.20, 'type': 'support'}      # Should round to 45
            ]
            
            # Mock get_historical_pricing to return dummy successful data so the function proceeds
            self.mock_tradier.get_historical_pricing.return_value = [
                {'date': '2023-01-01', 'close': 100, 'high': 105, 'low': 95, 'volume': 1000},
                {'date': '2023-01-02', 'close': 101, 'high': 106, 'low': 96, 'volume': 1100}
            ]
            
            # Mock ML service responses to avoid errors
            self.mock_ml_service.predict_next_day.return_value = {'predicted_price': 100, 'confidence': 0.05}
            self.mock_ml_service.score_put_entry.return_value = (5, 0.5, [])
            self.mock_ml_service.score_call_entry.return_value = (5, 0.5, [])

            # Run analysis
            result = self.service.analyze_symbol('TSLA')
            
            self.assertIn('put_entry_points', result)
            self.assertIn('call_entry_points', result)
            
            put_entry_points = result['put_entry_points']
            call_entry_points = result['call_entry_points']
            
            # Expected:
            # Puts (Support): 150, 100, 45 (rounded from 148.8, 102.4, 45.2)
            # Calls (Resistance): 155, 99 (rounded from 153.2, 98.7)
            
            expected_puts = [150, 100, 45]
            expected_calls = [155, 99]
            
            print("Original Levels:", [x['price'] for x in mock_find.return_value])
            print("Put Entry Points:", put_entry_points)
            print("Call Entry Points:", call_entry_points)
            
            actual_puts = [ep['price'] for ep in put_entry_points]
            actual_calls = [ep['price'] for ep in call_entry_points]
            
            for p in expected_puts:
                self.assertIn(p, actual_puts)
                
            for p in expected_calls:
                self.assertIn(p, actual_calls)

if __name__ == '__main__':
    unittest.main()
