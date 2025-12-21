
import sys
import os
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

# Mock Indicator functions before importing service
with patch('utils.indicators.calculate_rsi') as mock_rsi, \
     patch('utils.indicators.calculate_bollinger_bands') as mock_bb, \
     patch('utils.indicators.calculate_support_resistance') as mock_sr, \
     patch('utils.indicators.find_key_levels') as mock_kl:

    # Import Service
    from services.backtest_service import BacktestService

    def run_test():
        print("--- Verifying Backtest Logic (Expiration) ---")
        
        mock_tradier = MagicMock()
        service = BacktestService(mock_tradier)
        
        # Test Expiration
        # Day 90: Entry.
        # Day 120: Should Expire (30 days later).
        
        dates = pd.date_range(start="2023-01-01", periods=150)
        data = []
        for d in dates:
            data.append({
                'date': d.strftime('%Y-%m-%d'),
                'open': 100, 'high': 100, 'low': 100, 'close': 100, 'volume': 1000
            })
            
        mock_tradier.get_historical_pricing.return_value = data
        
        rsi_values = [50.0] * 150
        rsi_values[90] = 30.0 # Entry
        
        mock_rsi.return_value = pd.Series(rsi_values)
        mock_bb.return_value = (pd.Series([0]*150), pd.Series([0]*150), pd.Series([0]*150))
        mock_sr.return_value = (pd.Series([0]*150), pd.Series([0]*150))
        mock_kl.return_value = []
        
        result = service.run_backtest("TEST", "credit_spread", "2023-04-01", "2023-05-20")
        trades = result.get('trades', [])
        
        for t in trades:
            print(f"Trade: {t}")
            
        close_trade = next((t for t in trades if "CLOSE_EXPIRED" in t['action']), None)
        
        if close_trade:
            print("SUCCESS: Trade Closed due to Expiration.")
        else:
            print("FAILURE: Trade did NOT close (Infinite Hold).")

    run_test()
