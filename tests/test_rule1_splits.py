import unittest
import pandas as pd
import sys
import os
import logging

# Add project root to path if needed (though pytest usually handles this)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logic.calculator import calculate_metrics

class TestRule1Splits(unittest.TestCase):
    def setUp(self):
        # Configure logging to capture output during tests if needed
        logging.basicConfig(level=logging.INFO)

        self.base_data = {
            'FilingDate': ['2020-01-01', '2021-01-01', '2023-01-01'],
            'EPS': [10.0, 12.0, 7.0],
            'Shares': [1000, 1000, 2000],
            'NetIncome': [10000, 12000, 14000],
            'Equity': [50000, 60000, 70000],
            'Revenue': [100000, 120000, 140000],
            'Cash': [5000, 6000, 7000],
            'LongTermDebt': [2000, 2000, 2000]
        }
        self.df = pd.DataFrame(self.base_data)
        self.df['FilingDate'] = pd.to_datetime(self.df['FilingDate'])
        # Set index similar to app logic (Year)
        self.df = self.df.set_index(pd.Index([2019, 2020, 2022], name='Year'))

    def test_normal_split_2_to_1(self):
        # Split 2:1 on 2022-01-01
        splits = pd.Series({'2022-01-01': 2.0})
        
        metrics = calculate_metrics(self.df, splits)
        result_df = metrics.get('Financials')
        
        # 2019 (Index 0) - Before split
        # EPS 10.0 -> 5.0
        self.assertAlmostEqual(result_df['EPS'].iloc[0], 5.0)
        # Shares 1000 -> 2000
        self.assertAlmostEqual(result_df['Shares'].iloc[0], 2000)
        
        # 2020 (Index 1) - Before split
        # EPS 12.0 -> 6.0
        self.assertAlmostEqual(result_df['EPS'].iloc[1], 6.0)
        
        # 2022 (Index 2) - After split
        # EPS 7.0 -> 7.0 (No Change)
        self.assertAlmostEqual(result_df['EPS'].iloc[2], 7.0)

    def test_reverse_split_1_to_10(self):
        # Reverse Split 1:10 (Ratio 0.1) on 2022-01-01
        # EPS should increase (fewer shares)
        splits = pd.Series({'2022-01-01': 0.1})
        
        metrics = calculate_metrics(self.df, splits)
        result_df = metrics.get('Financials')
        
        # 2019 EPS 10.0 -> 100.0
        self.assertAlmostEqual(result_df['EPS'].iloc[0], 100.0)
        # Shares 1000 -> 100
        self.assertAlmostEqual(result_df['Shares'].iloc[0], 100)
        
        # 2022 Unchanged
        self.assertAlmostEqual(result_df['EPS'].iloc[2], 7.0)

    def test_no_splits(self):
        splits = None
        metrics = calculate_metrics(self.df, splits)
        result_df = metrics.get('Financials')
        
        # Original values maintained
        self.assertAlmostEqual(result_df['EPS'].iloc[0], 10.0)
        self.assertAlmostEqual(result_df['Shares'].iloc[0], 1000)

    def test_multiple_splits(self):
        # 2:1 on 2020-06-01 (Affects 2019 only)
        # 2:1 on 2022-01-01 (Affects 2019 and 2020)
        
        # Data Dates: 2020-01-01, 2021-01-01, 2023-01-01
        splits = pd.Series({
            '2020-06-01': 2.0,
            '2022-01-01': 2.0
        })
        
        metrics = calculate_metrics(self.df, splits)
        result_df = metrics.get('Financials')
        
        # 2019 (2020-01-01 filing) -> Affected by BOTH
        # EPS 10.0 / 2 / 2 = 2.5
        self.assertAlmostEqual(result_df['EPS'].iloc[0], 2.5)
        
        # 2020 (2021-01-01 filing) -> Affected by ONLY 2022 split
        # EPS 12.0 / 2 = 6.0
        self.assertAlmostEqual(result_df['EPS'].iloc[1], 6.0)

    def test_analyze_stock_uses_adjusted_eps(self):
        # 2:1 Split. 
        # We need enough data for 5y growth (at least 6 points)
        
        data = {
            'FilingDate': pd.date_range(start='2015-01-01', periods=6, freq='YE'), # 2015-2020
            'EPS': [1.0, 1.2, 1.4, 1.6, 1.8, 2.0], # Current (2020) is 2.0
            'Shares': [1000] * 6,
            'NetIncome': [1000] * 6,
            'Equity': [1000, 1200, 1400, 1600, 1800, 2000], # Consistent BVPS growth
            'Revenue': [1000] * 6,
            'Cash': [100] * 6,
            'LongTermDebt': [0] * 6,
            'OCF': [100] * 6
        }
        df = pd.DataFrame(data)
        
        # Split 2:1 on 2021-01-01 (After all data)
        # However, to test historical adjustment affecting current EPS...
        # If Split is in 2021, and Data ends 2020.
        # ALL data is "Historical" relative to split?
        # Yes. 2020 EPS (2.0) should be adjusted to 1.0.
        
        from logic.calculator import analyze_stock
        
        splits = pd.Series({'2021-01-01': 2.0})
        
        metrics, valuation = analyze_stock("TEST", df, splits)
        
        if 'Error' in valuation:
             self.fail(f"Analysis failed with error: {valuation['Error']}")
        
        current_eps_used = valuation.get('Current_EPS')
        
        # Original Last EPS = 2.0
        # Adjusted Last EPS = 1.0
        # Method should use 1.0
        
        self.assertAlmostEqual(current_eps_used, 1.0)

if __name__ == '__main__':
    unittest.main()
