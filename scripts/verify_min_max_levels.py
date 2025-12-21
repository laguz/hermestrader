import sys
import os
import unittest
from unittest.mock import MagicMock
import pandas as pd
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.analysis_service import AnalysisService

class TestMinMaxLevels(unittest.TestCase):
    def setUp(self):
        self.mock_tradier = MagicMock()
        self.mock_ml_service = MagicMock()
        self.service = AnalysisService(self.mock_tradier, self.mock_ml_service)

    def test_min_max_levels_inclusion(self):
        # Create mock data with clear min/max
        dates = pd.date_range(end=datetime.now(), periods=10)
        
        # Min is 90 (Day 0), Max is 110 (Day 5)
        # The service calculates period_min from 'low' and period_max from 'high'
        data = []
        for i in range(10):
            row = {
                'date': dates[i].strftime('%Y-%m-%d'),
                'close': 100,
                'high': 100,
                'low': 100,
                'volume': 1000
            }
            if i == 0:
                row['low'] = 90
            if i == 5:
                row['high'] = 110
            data.append(row)
            
        self.mock_tradier.get_historical_pricing.return_value = data
        
        # Mock ML service
        self.mock_ml_service.predict_next_day.return_value = {}
        
        # Patch find_key_levels to return empty so we only see our added levels
        with unittest.mock.patch('utils.indicators.find_key_levels') as mock_find:
            mock_find.return_value = []
            
            result = self.service.analyze_symbol('TEST')
            
            key_levels = result['key_levels']
            print("Key Levels:", key_levels)
            
            # Check for Min Support
            min_support = next((kl for kl in key_levels if kl['price'] == 90.0 and kl['type'] == 'support'), None)
            self.assertIsNotNone(min_support, "Min support level not found")
            self.assertEqual(min_support['strength'], 3)
            
            # Check for Max Resistance
            max_resistance = next((kl for kl in key_levels if kl['price'] == 110.0 and kl['type'] == 'resistance'), None)
            self.assertIsNotNone(max_resistance, "Max resistance level not found")
            self.assertEqual(max_resistance['strength'], 3)
            
            # Also verify they made it into entry points (rounded)
            # 90 is < 100, so round(90) = 90
            # 110 is > 100, so 5 * round(110/5) = 110
            
            puts = [p['price'] for p in result['put_entry_points']]
            calls = [c['price'] for c in result['call_entry_points']]
            
            self.assertIn(90, puts)
            self.assertIn(110, calls)

if __name__ == '__main__':
    unittest.main()
