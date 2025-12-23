import sys
import os
sys.path.append(os.getcwd())

from unittest.mock import MagicMock
from bot.strategies.credit_spreads import CreditSpreadStrategy

def test_expiry_limits():
    # 1. Mock DB
    mock_db = MagicMock()
    mock_db.__getitem__.return_value.find_one.return_value = {
        'settings': {
            'max_total_credit_spreads': 20, 
            'max_credit_spreads_per_symbol': 20 
        }
    }
    
    # 2. Mock Tradier
    mock_tradier = MagicMock()
    
    # CASE H: 5 Lots in Jan 9 (Should Block Jan 9). 0 Lots in Jan 16 (Should Allow).
    print("\n--- TEST CASE H: 5 Lots in Jan 9, 0 in Jan 16 ---")
    positions_h = [{
        'symbol': 'TSLA260109P00400000', # Jan 9th
        'quantity': -5.0, 
        'option_type': 'put',
        'underlying': 'TSLA'
    }]
    mock_tradier.get_positions.return_value = positions_h
    mock_tradier.get_orders.return_value = []
    
    # Mock Option Chain for Jan 9 (To be blocked)
    mock_tradier.get_option_chains.side_effect = lambda sym, exp: [
        {'symbol': f'TSLA{exp.replace("-","")[2:]}P...', 'strike': 400, 'option_type': 'put', 'greeks': {'delta': -0.35}} # Mock chain item
    ]
    
    strategy = CreditSpreadStrategy(mock_tradier, mock_db, dry_run=True)
    
    # We want to test _check_expiry_constraints directly first
    full_weeks = strategy._check_expiry_constraints('TSLA', is_put=True)
    print(f"Full Weeks (Put): {full_weeks}")
    assert '2026-01-09' in full_weeks, "Jan 9 should be full"
    assert '2026-01-16' not in full_weeks, "Jan 16 should be empty"
    
    print("Direct check passed.")

if __name__ == "__main__":
    test_expiry_limits()
