import sys
import os
sys.path.append(os.getcwd())

from unittest.mock import MagicMock
from bot.strategies.credit_spreads import CreditSpreadStrategy

def test_pending_orders_limit():
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
    
    # Setup: 4 Existing Positions for NVDA + 1 Pending Order for NVDA
    # Total Risk Units = 5. Should SKIP new orders.
    
    # Positions
    positions = []
    for i in range(4):
        positions.append({
            'symbol': f'NVDA250101P001{i}0000',
            'underlying': 'NVDA',
            'option_type': 'put',
            'quantity': -1
        })
    mock_tradier.get_positions.return_value = positions
    
    # Orders
    orders = []
    orders.append({
        'id': 12345,
        'symbol': 'NVDA', # Multileg usually has underlying symbol here
        'class': 'multileg', # This counts as a spread
        'status': 'open',
        'legs': [{'option_symbol': '...', 'side': 'sell_to_open'}, {'option_symbol': '...', 'side': 'buy_to_open'}]
    })
    mock_tradier.get_orders.return_value = orders
    
    strategy = CreditSpreadStrategy(mock_tradier, mock_db, dry_run=True)
    
    print("\nExecuting for ['NVDA'] (Expect Skip due to 4 Pos + 1 Order = 5)...")
    strategy.execute(['NVDA'])

    # Test Case B: 3 Positions + 1 Order = 4. Should NOT Skip.
    print("\nTest Case B: 3 Pos + 1 Order = 4 (Below Limit). Expect Analysis...")
    positions_b = positions[:3] # 3
    mock_tradier.get_positions.return_value = positions_b
    strategy.execute(['NVDA'])

if __name__ == "__main__":
    test_pending_orders_limit()
