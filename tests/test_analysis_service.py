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


    @patch('services.analysis_service.datetime')
    def test_analyze_symbol_short_history_and_low_price(self, mock_datetime):
        mock_now = datetime(2023, 10, 1)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime

        # Generate short history (20 days)
        quotes = []
        current_date = mock_now - timedelta(days=20)
        for i in range(20):
            quotes.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'open': 48.0 + i * 0.1,
                'high': 52.0 + i * 0.1,
                'low': 45.0 + i * 0.1,
                'close': 50.0 + i * 0.1, # Price < 100 to hit rounding logic
                'volume': 1000000.0
            })
            current_date += timedelta(days=1)

        self.tradier_service.get_historical_pricing.return_value = quotes

        # Ensure IV returns valid to test pop/pot on low prices, or we can let it fail/pass
        self.tradier_service.get_option_expirations.return_value = ['2023-10-31']
        self.tradier_service.get_option_chains.return_value = [
            {'strike': 50.0, 'greeks': {'mid_iv': 0.30}}
        ]

        # Mock ML prediction
        self.ml_service.predict_next_day.return_value = {'predicted_price': 55.0}

        result = self.analysis_service.analyze_symbol('AAPL', period='3m') # request 3m but only have 20 days

        # Verify return values
        self.assertEqual(result['symbol'], 'AAPL')
        self.assertIn('put_entry_points', result)
        # Verify it handled short history without failing



    @patch('services.analysis_service.datetime')
    def test_analyze_symbol_exceptions(self, mock_datetime):
        mock_now = datetime(2023, 10, 1)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime

        # Generate standard data
        quotes = []
        current_date = mock_now - timedelta(days=50)
        for i in range(50):
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

        # Exceptions
        self.tradier_service.get_option_expirations.side_effect = Exception("Tradier IV Error")
        self.ml_service.predict_next_day.side_effect = Exception("ML Error")
        self.db.entries.update_one.side_effect = Exception("DB Error")

        result = self.analysis_service.analyze_symbol('AAPL')

        # Verify it didn't crash and handled the exceptions
        self.assertEqual(result['symbol'], 'AAPL')
        # ML error means prediction logic won't be set
        self.assertIsNone(result['prediction']['price'])
        self.assertEqual(result['prediction']['change_pct'], 0)



    @patch('services.analysis_service.datetime')
    def test_analyze_symbol_bearish_indicators(self, mock_datetime):
        mock_now = datetime(2023, 10, 1)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime

        # Generate a strong bearish setup
        quotes = []
        current_date = mock_now - timedelta(days=300)
        # We need an uptrend that is overbought and near resistance
        # latest close = 160. latest sma_200 = 150. latest rsi > 70. macd bearish.
        for i in range(300):
            # Slow uptrend
            c = 140.0 + i * 0.05

            # The last few days we make it overbought then suddenly drop (bearish macd cross)
            if i > 295:
                c = 160.0 + (300-i)*2 # 160, 162, 164, ...
            if i == 299:
                c = 160.0 # Sudden drop to trigger bearish MACD

            quotes.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'open': c - 2.0,
                'high': c + 2.0,
                'low': c - 2.0,
                'close': c,
                'volume': 1000000.0
            })
            current_date += timedelta(days=1)

        # Manually force some values by mocking the indicators, but we don't have patch on them.
        # So we just provide data. We'll patch calculate_* if needed, but easier to use mock data or patch the indicators module.

        self.tradier_service.get_historical_pricing.return_value = quotes
        self.tradier_service.get_option_expirations.return_value = []

        self.ml_service.predict_next_day.return_value = {'predicted_price': 150.0} # AI predicts down

        # We will mock the indicators to perfectly hit the bearish branches
        with patch('services.analysis_service.calculate_rsi') as mock_rsi, \
             patch('services.analysis_service.calculate_support_resistance') as mock_sr, \
             patch('services.analysis_service.calculate_macd') as mock_macd, \
             patch('services.analysis_service.calculate_bollinger_bands') as mock_bb, \
             patch('services.analysis_service.calculate_sma') as mock_sma, \
             patch('services.analysis_service.calculate_adx') as mock_adx, \
             patch('services.analysis_service.calculate_hv_rank') as mock_hv:

            # length of dataframe is 300
            # mock arrays of size 300
            mock_rsi_data = [50]*299 + [75] # latest RSI > 70
            mock_rsi.return_value = mock_rsi_data
            mock_sr.return_value = ([100]*300, [161]*300)

            # MACD bearish: latest macd < latest signal. momentum weakening: latest hist < prev hist
            macd = [0]*298 + [2, 1]
            signal = [0]*298 + [1, 2]
            hist = [0]*298 + [1, -1]
            mock_macd.return_value = (macd, signal, hist)

            # BB overbought: latest close >= latest bb_upper * 0.99
            # latest close is 160. So bb_upper should be around 160.
            bb_upper = [160]*300
            bb_mid = [150]*300
            bb_lower = [140]*300
            mock_bb.return_value = (bb_upper, bb_mid, bb_lower)

            # SMA 200 > latest close (Bearish trend) - close is 160, sma is 165
            mock_sma.side_effect = lambda x, window: [165]*300 if window == 200 else [1000000]*300 # sma 200 and volume sma

            # ADX > 25 (Strong trend)
            mock_adx.return_value = [30]*300

            # HV Rank > 80
            mock_hv.return_value = 85

            result = self.analysis_service.analyze_symbol('AAPL')

            self.assertGreater(result['sell_call_entry']['score'], 5)

    @patch('services.analysis_service.datetime')
    def test_analyze_symbol_bullish_indicators(self, mock_datetime):
        mock_now = datetime(2023, 10, 1)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime

        # Generate a strong bullish setup
        quotes = []
        current_date = mock_now - timedelta(days=300)
        for i in range(300):
            quotes.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 100.0,
                'volume': 1000000.0
            })
            current_date += timedelta(days=1)

        self.tradier_service.get_historical_pricing.return_value = quotes
        self.tradier_service.get_option_expirations.return_value = []

        self.ml_service.predict_next_day.return_value = {'predicted_price': 110.0} # AI predicts up

        # We will mock the indicators to perfectly hit the bullish branches
        with patch('services.analysis_service.calculate_rsi') as mock_rsi, \
             patch('services.analysis_service.calculate_support_resistance') as mock_sr, \
             patch('services.analysis_service.calculate_macd') as mock_macd, \
             patch('services.analysis_service.calculate_bollinger_bands') as mock_bb, \
             patch('services.analysis_service.calculate_sma') as mock_sma, \
             patch('services.analysis_service.calculate_adx') as mock_adx, \
             patch('services.analysis_service.calculate_hv_rank') as mock_hv:

            # RSI oversold < 30
            mock_rsi.return_value = [50]*299 + [25]
            mock_sr.return_value = ([99]*300, [150]*300) # near support (100 close, 99 sup -> dist < 0.015)

            # MACD bullish: latest macd > latest signal.
            macd = [0]*298 + [1, 2]
            signal = [0]*298 + [2, 1]
            hist = [0]*298 + [-1, 1]
            mock_macd.return_value = (macd, signal, hist)

            # BB oversold: latest close <= latest bb_lower * 1.01
            # latest close is 100. So bb_lower should be 100
            bb_upper = [120]*300
            bb_mid = [110]*300
            bb_lower = [100]*300
            mock_bb.return_value = (bb_upper, bb_mid, bb_lower)

            # SMA 200 < latest close (Bullish trend) - close is 100, sma is 90
            mock_sma.side_effect = lambda x, window: [90]*300 if window == 200 else [1000000]*300

            # ADX < 20 (Weak trend)
            mock_adx.return_value = [15]*300

            # HV Rank 50 < x < 80
            mock_hv.return_value = 60

            result = self.analysis_service.analyze_symbol('AAPL')

            self.assertGreater(result['sell_put_entry']['score'], 5)



    @patch('services.analysis_service.datetime')
    def test_analyze_symbol_medium_indicators(self, mock_datetime):
        mock_now = datetime(2023, 10, 1)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime

        # Generate a medium setup
        quotes = []
        current_date = mock_now - timedelta(days=300)
        for i in range(300):
            quotes.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 100.0,
                'volume': 1000000.0
            })
            current_date += timedelta(days=1)

        self.tradier_service.get_historical_pricing.return_value = quotes
        self.tradier_service.get_option_expirations.return_value = []

        self.ml_service.predict_next_day.return_value = {'predicted_price': 100.2}

        # We will mock the indicators to hit the "elif" branches
        with patch('services.analysis_service.calculate_rsi') as mock_rsi, \
             patch('services.analysis_service.calculate_support_resistance') as mock_sr, \
             patch('services.analysis_service.calculate_macd') as mock_macd, \
             patch('services.analysis_service.calculate_bollinger_bands') as mock_bb, \
             patch('services.analysis_service.calculate_sma') as mock_sma, \
             patch('services.analysis_service.calculate_adx') as mock_adx, \
             patch('services.analysis_service.calculate_hv_rank') as mock_hv:

            # RSI neutral/low and neutral/high
            # To hit both, we'd need multiple tests or to just provide values that hit the "elif"
            # For this test, we will target the "elif" for RSI < 45 and RSI > 55
            # We'll set latest RSI to 40. Then another time we'd set to 60.
            # We'll just run it with RSI 40 first, then we can check coverage.
            mock_rsi.return_value = [50]*299 + [40]
            mock_sr.return_value = ([90]*300, [103]*300)

            # MACD momentum improving (latest macd <= latest signal, but latest hist > prev hist)
            macd = [0]*298 + [1, 1]
            signal = [0]*298 + [2, 2]
            hist = [0]*298 + [-1, 0]
            mock_macd.return_value = (macd, signal, hist)

            bb_upper = [120]*300
            bb_mid = [110]*300
            bb_lower = [80]*300 # Price not near BBs
            mock_bb.return_value = (bb_upper, bb_mid, bb_lower)

            # SMA 200
            mock_sma.side_effect = lambda x, window: [90]*300 if window == 200 else [1000000]*300

            # ADX < 20 (Weak trend)
            mock_adx.return_value = [15]*300

            # HV Rank < 20
            mock_hv.return_value = 10

            result = self.analysis_service.analyze_symbol('AAPL')

            # Run again for sc_score elifs
            mock_rsi.return_value = [50]*299 + [60]
            macd = [0]*298 + [2, 2]
            signal = [0]*298 + [1, 1]
            hist = [0]*298 + [1, 0] # momentum weakening
            mock_macd.return_value = (macd, signal, hist)

            self.ml_service.predict_next_day.return_value = {'predicted_price': 99.8} # slight down

            result2 = self.analysis_service.analyze_symbol('AAPL')


if __name__ == '__main__':
    unittest.main()
