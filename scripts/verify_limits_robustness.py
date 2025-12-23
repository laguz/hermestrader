import sys
import os
sys.path.append(os.getcwd())

from unittest.mock import MagicMock
from bot.strategies.credit_spreads import CreditSpreadStrategy

def test_limits_robustness():
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
    
    # CASE C: Missing Underlying (Simulate Tradier quirk)
    print("\n--- TEST CASE C: Missing 'underlying' Field (Robust Parsing) ---")
    positions_c = []
    # 5 positions for NVDA, but 'underlying' is MISSING.
    # Symbol format: NVDA250117P...
    for i in range(5):
        positions_c.append({
            'symbol': f'NVDA250117P001{i}0000',
            # 'underlying': 'NVDA', # MISSING!
            'option_type': 'put',
            'quantity': -1
        })
        
    mock_tradier.get_positions.return_value = positions_c
    
    strategy = CreditSpreadStrategy(mock_tradier, mock_db, dry_run=True)
    
    # We expect 'NVDA' to be SKIPPED because our regex should detect it's NVDA.
    print("Executing for ['NVDA']...")
    strategy.execute(['NVDA'])

if __name__ == "__main__":
    test_limits_robustness()
