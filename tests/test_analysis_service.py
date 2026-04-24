import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from services.analysis_service import AnalysisService

class TestAnalysisService(unittest.TestCase):
    def setUp(self):
        self.tradier_service = MagicMock()
        self.ml_service = MagicMock()
        self.db = MagicMock()
        self.analysis_service = AnalysisService(self.tradier_service, self.ml_service, self.db)

    def test_analyze_symbol_no_data(self):
        self.tradier_service.get_historical_pricing.return_value = []
        result = self.analysis_service.analyze_symbol('AAPL')
        self.assertEqual(result, {"error": "No data found for symbol"})

    @patch('services.analysis_service.datetime')
    def test_analyze_symbol_success(self, mock_datetime):
        # Mock current datetime for consistent testing
        mock_now = datetime(2023, 10, 1)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime

        # Generate enough dummy historical data to pass SMA 200 requirement (>= 300 days)
        quotes = []
        current_date = mock_now - timedelta(days=300)
        for i in range(300):
            quotes.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'open': 140.0 + i * 0.1,
                'high': 145.0 + i * 0.1,
                'low': 135.0 + i * 0.1,
                'close': 142.0 + i * 0.1,
                'volume': 1000000.0
            })
            current_date += timedelta(days=1)

        self.tradier_service.get_historical_pricing.return_value = quotes

        # Mock ML prediction
        self.ml_service.predict_next_day.return_value = {'predicted_price': 180.0}

        # Mock IV Fallback
        self.tradier_service.get_option_expirations.return_value = ['2023-10-31', '2023-11-15']
        self.tradier_service.get_option_chains.return_value = [
            {'strike': 142.0 + 29.9, 'greeks': {'mid_iv': 0.25}}  # near ATM
        ]

        result = self.analysis_service.analyze_symbol('AAPL')

        # Verify db logic
        self.db.entries.update_one.assert_called_once()
        call_args = self.db.entries.update_one.call_args[0]
        self.assertEqual(call_args[0], {'symbol': 'AAPL'})

        # Verify return values
        self.assertEqual(result['symbol'], 'AAPL')
        self.assertEqual(result['prediction']['price'], 180.0)
        self.assertIn('put_entry_points', result)
        self.assertIn('call_entry_points', result)
        self.assertIn('sell_put_entry', result)
        self.assertIn('sell_call_entry', result)
        self.assertIn('indicators', result)

if __name__ == '__main__':
    unittest.main()
