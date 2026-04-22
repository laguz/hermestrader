import unittest
import pandas as pd
import numpy as np
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.indicators import calculate_rsi

class TestIndicatorsRSI(unittest.TestCase):
    def test_calculate_rsi_normal(self):
        # A normal series of prices
        prices = pd.Series([
            44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42,
            45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28, 46.00
        ])

        # Calculate RSI with a period of 14
        rsi = calculate_rsi(prices, period=14)

        # Period is 14.
        self.assertEqual(len(rsi), 16)

        # The first 13 values (index 0 to 12) don't have enough data for a 14-period rolling mean
        # (they have fewer than 14 data points of diff), so they will be NaN, which our function fills with 50.
        self.assertEqual(rsi.iloc[0], 50.0)
        self.assertEqual(rsi.iloc[12], 50.0)

        # Values after index 12 should not be 50.0 (unless perfectly flat)
        self.assertNotEqual(rsi.iloc[13], 50.0)
        self.assertNotEqual(rsi.iloc[14], 50.0)

        # Let's just do a sanity check on bounds
        self.assertTrue(0 <= rsi.iloc[14] <= 100)
        self.assertTrue(0 <= rsi.iloc[15] <= 100)

    def test_calculate_rsi_uptrend(self):
        # Steady uptrend
        prices = pd.Series(np.arange(10, 40, dtype=float))
        rsi = calculate_rsi(prices, period=14)

        # After the initial period, RSI should be 100 since there are no losses
        # If losses are 0, rs is inf, rsi is 100
        self.assertEqual(rsi.iloc[14], 100.0)
        self.assertEqual(rsi.iloc[-1], 100.0)

    def test_calculate_rsi_downtrend(self):
        # Steady downtrend
        prices = pd.Series(np.arange(40, 10, -1, dtype=float))
        rsi = calculate_rsi(prices, period=14)

        # After the initial period, RSI should be 0 since there are no gains
        # If gains are 0, rs is 0, rsi is 0
        self.assertEqual(rsi.iloc[14], 0.0)
        self.assertEqual(rsi.iloc[-1], 0.0)

    def test_calculate_rsi_flat(self):
        # Constant prices
        prices = pd.Series([100.0] * 20)
        rsi = calculate_rsi(prices, period=14)

        # All gains and losses are 0.
        # This results in 0/0 which is NaN in pandas/numpy, and the function uses fillna(50)
        self.assertEqual(rsi.iloc[14], 50.0)
        self.assertEqual(rsi.iloc[-1], 50.0)

    def test_calculate_rsi_short_series(self):
        # Series length is less than the period
        prices = pd.Series([10, 12, 15, 14, 16])
        rsi = calculate_rsi(prices, period=14)

        # Since length < period, the rolling window never gets enough data
        # so all results are NaN, filled with 50.0
        self.assertEqual(len(rsi), 5)
        for val in rsi:
            self.assertEqual(val, 50.0)

if __name__ == '__main__':
    unittest.main()
