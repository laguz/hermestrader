import sys
import os
sys.path.append(os.getcwd())

from unittest.mock import MagicMock
from bot.strategies.credit_spreads import CreditSpreadStrategy

def test_contract_limits():
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
    
    # CASE F: 1 Row with Quote -14 (Should be 14 spreads)
    print("\n--- TEST CASE F: 1 Position Row with Qty -14 (Should count as 14, Limit 5) ---")
    positions_f = [{
        'symbol': 'TSLA250109C00500000',
        'quantity': -14.0, # This is 14 lots!
        # Missing option_type to test regex too
    }]
    mock_tradier.get_positions.return_value = positions_f
    mock_tradier.get_orders.return_value = []
    
    strategy = CreditSpreadStrategy(mock_tradier, mock_db, dry_run=True)
    
    print(f"Executing for ['TSLA']...")
    strategy.execute(['TSLA'])
    
    # CASE G: Pending Order with Qty 5
    print("\n--- TEST CASE G: Pending Order with Qty 5 (Limit 5) ---")
    mock_tradier.get_positions.return_value = []
    orders_g = [{
        'id': 999,
        'symbol': 'TSLA',
        'class': 'multileg',
        'status': 'open',
        'quantity': 5.0 # 5 Lots
    }]
    mock_tradier.get_orders.return_value = orders_g
    
    print(f"Executing for ['TSLA']...")
    strategy.execute(['TSLA'])

if __name__ == "__main__":
    test_contract_limits()
