import sys
import os
sys.path.append(os.getcwd())

from unittest.mock import MagicMock
from bot.strategies.credit_spreads import CreditSpreadStrategy

def test_limits_enforcement():
    # 1. Mock DB Config
    mock_db = MagicMock()
    mock_db.__getitem__.return_value.find_one.return_value = {
        'settings': {
            'max_total_credit_spreads': 10,
            'max_credit_spreads_per_symbol': 5
        }
    }
    
    # 2. Mock Tradier
    mock_tradier = MagicMock()
    
    # CASE A: Per-Symbol Limit (Target 5, have 5)
    print("\n--- TEST CASE A: Per-Symbol Limit (Limit 5) ---")
    positions_a = []
    # Create 5 existing spreads for AAPL
    for i in range(5):
        positions_a.append({
            'symbol': f'AAPL250101P00150{i}',
            'underlying': 'AAPL',
            'option_type': 'put',
            'quantity': -1
        })
    mock_tradier.get_positions.return_value = positions_a
    
    strategy = CreditSpreadStrategy(mock_tradier, mock_db, dry_run=True)
    # Redirect print/log to capture output? Or just rely on stdout
    
    # We expect 'AAPL' to be SKIPPED
    print("Executing for ['AAPL']...")
    strategy.execute(['AAPL'])
    
    # CASE B: Global Limit (Target 10, have 10 mixed)
    print("\n--- TEST CASE B: Global Limit (Limit 10) ---")
    positions_b = []
    # 5 AAPL + 5 TSLA = 10 Total
    for i in range(5):
        positions_b.append({'symbol': f'AAPL250101P{i}', 'underlying': 'AAPL', 'option_type': 'put', 'quantity': -1})
        positions_b.append({'symbol': f'TSLA250101P{i}', 'underlying': 'TSLA', 'option_type': 'put', 'quantity': -1})
        
    mock_tradier.get_positions.return_value = positions_b
    
    # We expect 'MSFT' (new symbol) to be SKIPPED due to Global Limit
    print("Executing for ['MSFT']...")
    strategy.execute(['MSFT'])

if __name__ == "__main__":
    test_limits_enforcement()
