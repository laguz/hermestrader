
import sys
import os
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.getcwd())

# Mock Indicators
with patch('utils.indicators.calculate_rsi') as mock_rsi, \
     patch('utils.indicators.calculate_bollinger_bands') as mock_bb, \
     patch('utils.indicators.calculate_support_resistance') as mock_sr, \
     patch('utils.indicators.find_key_levels') as mock_kl:

    from services.backtest_service import BacktestService

    def run_debug():
        print("--- Running Debug Backtest ---")
        mock_tradier = MagicMock()
        service = BacktestService(mock_tradier)
        
        # Scenario: 
        # Entry Day 90: Price 100. RSI 30 (Trigger Put Credit).
        # Algo sets strike (e.g. 95).
        # Day 91: Price 94 (ITM 1).
        # Day 92: Price 93 (ITM 2).
        # Day 93: Price 92 (Execution).
        
        dates = pd.date_range(start="2023-01-01", periods=100)
        data = []
        for d in dates:
            data.append({
                'date': d.strftime('%Y-%m-%d'),
                'open': 100, 'high': 105, 'low': 95, 'close': 100, 'volume': 1000
            })
            
        # Mock Logic to force specific strike? 
        # Strike = Price - Dist. Dist depends on vol.
        # Let's just update prices to definitely be ITM.
        
        # Day 91-93 -> Drop to 50. Definitely ITM.
        data[91]['close'] = 50.0
        data[92]['close'] = 50.0
        data[93]['close'] = 50.0
        
        # RSI Mock
        rsi_vals = [50.0] * 100
        rsi_vals[90] = 30.0
        mock_rsi.return_value = pd.Series(rsi_vals)
        
        mock_tradier.get_historical_pricing.return_value = data
        
        # Other mocks
        mock_bb.return_value = (pd.Series([0]*100), pd.Series([0]*100), pd.Series([0]*100))
        mock_sr.return_value = (pd.Series([0]*100), pd.Series([0]*100))
        mock_kl.return_value = []
        
        # Run
        result = service.run_backtest("DEBUG_SYM", "credit_spread", "2023-04-01", "2023-04-10")
        
        print("\n--- Trades ---")
        for t in result.get('trades', []):
            print(t)

    run_debug()
