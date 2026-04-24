import os
import sys
import unittest
from unittest.mock import MagicMock

# Ensure project root is in path
sys.path.insert(0, os.path.abspath('.'))

from bot.strategies.wheel import WheelStrategy

class TestWheelTag(unittest.TestCase):
    def setUp(self):
        self.mock_tradier = MagicMock()
        self.mock_db = MagicMock()
        self.strategy = WheelStrategy(self.mock_tradier, self.mock_db)
        
        # Override _log to avoid DB calls in test
        self.strategy._log = MagicMock()
        # Override _close_trade
        self.strategy._close_trade = MagicMock()

    def test_take_profit_tag(self):
        # Setup mock position that is profitable
        # entry_price = 1.00, ask_price = 0.10 -> 90% profit
        positions = [
            {
                'symbol': 'AAPL260320P00150000',
                'quantity': -1,
                'cost_basis': -100.0, # 100 / (1 * 100) = 1.00 entry
                'option_type': 'put'
            }
        ]
        
        # Mock Tradier responses
        self.mock_tradier.get_orders.return_value = []
        self.mock_tradier.get_quote.side_effect = lambda sym: {
            'AAPL260320P00150000': {'ask': 0.10, 'last': 0.10, 'close': 0.10},
            'AAPL': {'last': 160.00}
        }.get(sym)
        self.mock_tradier.account_id = "TEST_ACC"
        
        # Mock place_order to capture arguments
        self.mock_tradier.place_order.return_value = {'id': 'order_123'}

        # Execute management (where TP logic lives)
        self.strategy._manage_positions(positions, watchlist=['AAPL'])

        # Verify place_order was called with the correct tag
        self.mock_tradier.place_order.assert_called_once()
        args, kwargs = self.mock_tradier.place_order.call_args
        
        self.assertEqual(kwargs.get('tag'), "WHEELTP")
        print(f"Verified tag: {kwargs.get('tag')}")

if __name__ == '__main__':
    unittest.main()
