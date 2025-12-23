import sys
import os
sys.path.append(os.getcwd())

from unittest.mock import MagicMock
from bot.strategies.credit_spreads import CreditSpreadStrategy

def test_limits_missing_option_type():
    # 1. Mock DB
    mock_db = MagicMock()
    mock_db.__getitem__.return_value.find_one.return_value = {
        'settings': {
            'max_total_credit_spreads': 10,
            'max_credit_spreads_per_symbol': 5
        }
    }
    
    # 2. Mock Tradier
    mock_tradier = MagicMock()
    
    # CASE D: Missing 'option_type' (Real world scenario)
    print("\n--- TEST CASE D: Missing 'option_type' (Regex Parsing) ---")
    positions_d = []
    # 5 positions for NVDA
    for i in range(5):
        positions_d.append({
            'symbol': f'NVDA250117P001{i}0000', # P = Put
            # 'option_type': 'put', # MISSING!
            'quantity': -1
        })
        # Add a Long leg that should be ignored
        positions_d.append({
            'symbol': f'NVDA250117P001{i}0005',
            # 'option_type': 'put', # MISSING!
            'quantity': 1
        })
        
    mock_tradier.get_positions.return_value = positions_d
    mock_tradier.get_orders.return_value = []
    
    strategy = CreditSpreadStrategy(mock_tradier, mock_db, dry_run=True)
    
    # We expect 'NVDA' to be SKIPPED (5 short puts found via regex)
    print("Executing for ['NVDA']...")
    strategy.execute(['NVDA'])

    # Test Case E: 4 Shorts (Should NOT skip)
    print("\n--- TEST CASE E: 4 Shorts (Limit 5) ---")
    mock_tradier.get_positions.return_value = positions_d[:8] # 4 pairs = 8 legs
    strategy.execute(['NVDA'])

if __name__ == "__main__":
    test_limits_missing_option_type()
