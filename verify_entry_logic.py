
import sys
import os
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime

sys.path.append(os.getcwd())

with patch('utils.indicators.calculate_rsi') as mock_rsi, \
     patch('utils.indicators.calculate_bollinger_bands') as mock_bb, \
     patch('utils.indicators.calculate_support_resistance') as mock_sr, \
     patch('utils.indicators.find_key_levels') as mock_kl, \
     patch('utils.indicators.calculate_prob_it_expires_otm') as mock_pop:

    from services.backtest_service import BacktestService

    def run_test():
        print("--- Verifying Entry Logic (POP & S/R) ---")
        mock_tradier = MagicMock()
        service = BacktestService(mock_tradier)
        
        # Base Data
        dates = pd.date_range(start="2023-01-01", periods=100)
        data = []
        for d in dates:
             data.append({'date': d.strftime('%Y-%m-%d'), 'close': 100.0, 'high': 105, 'low': 95, 'volume': 1000})
        
        mock_tradier.get_historical_pricing.return_value = data
        mock_rsi.return_value = pd.Series([50.0]*100) # Neutral RSI. Should NOT trigger old logic (<40), but SHOULD trigger New Logic.
        mock_bb.return_value = (pd.Series([0]*100), pd.Series([0]*100), pd.Series([0]*100))
        mock_sr.return_value = (pd.Series([0]*100), pd.Series([0]*100))
        
        # --- TEST 1: S/R Entry (POP Good) ---
        # Mock Key Levels: Support at 95.
        mock_kl.return_value = [{'price': 95.0, 'type': 'support', 'strength': 5}]
        
        # Mock POP to be 60% (Valid: 55-70)
        # Note: calculate_prob_it_expires_otm returns float 0-1
        mock_pop.return_value = 0.60
        
        print("\nTest 1: S/R Entry with POP 60%")
        result = service.run_backtest("TEST_SR", "credit_spread", "2023-04-01", "2023-04-05")
        
        trades = result.get('trades', [])
        open_trade = next((t for t in trades if "OPEN" in t['action']), None)
        
        if open_trade and "Algo S/R" in open_trade['action']:
             print(f"SUCCESS: Opened S/R Trade: {open_trade['action']}")
        else:
             print(f"FAILURE: Did not open S/R trade. Trades: {trades}")

        # --- TEST 2: Fallback Entry (No S/R) ---
        mock_kl.return_value = [] # No Levels
        
        print("\nTest 2: Fallback Entry (No S/R)")
        result_fb = service.run_backtest("TEST_FB", "credit_spread", "2023-04-01", "2023-04-05")
        
        trades_fb = result_fb.get('trades', [])
        open_trade_fb = next((t for t in trades_fb if "OPEN" in t['action']), None)
        
        if open_trade_fb and "Algo Delta" in open_trade_fb['action']:
             print(f"SUCCESS: Opened Fallback Trade: {open_trade_fb['action']}")
        else:
             print(f"FAILURE: Did not open Fallback trade. Trades: {trades}")

    run_test()
