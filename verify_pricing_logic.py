
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
     patch('utils.indicators.calculate_prob_it_expires_otm') as mock_pop, \
     patch('utils.indicators.calculate_option_price') as mock_bs:

    from services.backtest_service import BacktestService

    def run_test():
        print("--- Verifying Pricing Logic (Black-Scholes) ---")
        mock_tradier = MagicMock()
        service = BacktestService(mock_tradier)
        
        # Scenario:
        # Put Credit Spread. Short 99, Long 94. Credit 1.00.
        # Entry Day 90 (Price 100). Support 99 (<100).
        
        # ITM Trigger: Price must be < Short Strike (99).
        # Day 91-93 Price 98.
        
        # Pricing at Close (Price 98):
        # Short (99): Intrinsic 1.0. BS Est 1.50.
        # Long (94): Intrinsic 0.0. BS Est 0.20.
        # Debit = 1.30.
        # PnL = 1.00 (Credit) - 1.30 (Debit) = -0.30 (-30.00).
        
        mock_bs.side_effect = lambda S, K, T, v, r=0.04, option_type='put': 1.50 if K==99 else 0.20 

        # Setup Data
        dates = pd.date_range(start="2023-01-01", periods=100)
        data = []
        for d in dates:
             data.append({'date': d.strftime('%Y-%m-%d'), 'close': 100.0, 'high': 105, 'low': 95, 'volume': 1000})

        # Make Day 91-93 Price 98 (ITM against 99 Strike)
        data[91]['close'] = 98.0
        data[92]['close'] = 98.0
        data[93]['close'] = 98.0
        
        mock_tradier.get_historical_pricing.return_value = data
        
        # Force Entry at 99
        mock_kl.return_value = [{'price': 99.0, 'type': 'support', 'strength': 5}]
        mock_pop.return_value = 0.60
        mock_rsi.return_value = pd.Series([50]*100)
        mock_bb.return_value = (pd.Series([0]*100), pd.Series([0]*100), pd.Series([0]*100))
        mock_sr.return_value = (pd.Series([0]*100), pd.Series([0]*100))

        print("\nRunning Backtest...")
        result = service.run_backtest("TEST_PRICING", "credit_spread", "2023-04-01", "2023-04-10")
        
        trades = result.get('trades', [])
        close_trade = next((t for t in trades if "CLOSE_STOP_LOSS" in t['action']), None)
        
        if close_trade:
             pnl = close_trade['pnl']
             print(f"SUCCESS: Trade Closed. PnL: {pnl}")
             # Expected: -30 (Credit 1.00 - Debit 1.30)
             
             if pnl == -400.0:
                 print("FAILURE: PnL is still Max Loss (-400).")
             elif abs(pnl - -30.0) < 0.1:
                 print("SUCCESS: PnL matches Black-Scholes estimate (-30.0).")
             else:
                 print(f"WARNING: PnL is {pnl}, check logic.")
                 
             print(f"Details: {close_trade.get('details')}")
        else:
             print("FAILURE: Trade did not close.")

    run_test()
