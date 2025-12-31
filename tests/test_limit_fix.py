
import unittest
from unittest.mock import MagicMock, patch
from bot.strategies.credit_spreads import CreditSpreadStrategy
from datetime import datetime

class TestLimitFix(unittest.TestCase):
    def setUp(self):
        self.tradier = MagicMock()
        self.db = MagicMock()
        self.strategy = CreditSpreadStrategy(self.tradier, self.db, dry_run=True)

    @patch('bot.strategies.credit_spreads.datetime')
    def test_check_expiry_constraints_pending_multileg(self, mock_datetime):
        # Mock current time
        mock_datetime.now.return_value = datetime(2025, 12, 30)
        mock_datetime.strptime.side_effect = datetime.strptime

        # 1. Setup Mock Positions (3 existing lots for 2026-01-16)
        self.tradier.get_positions.return_value = [
            {'symbol': 'IWM260116P00200000', 'quantity': -3, 'option_type': 'put'}
        ]

        # 2. Setup Mock Orders (2 pending lots for 2026-01-16 using 'leg' field)
        # Tradier multileg order structure
        self.tradier.get_orders.return_value = [
            {
                'status': 'open',
                'class': 'multileg',
                'symbol': 'IWM',
                'quantity': 2,
                'leg': [
                    {'option_symbol': 'IWM260116P00200000', 'side': 'sell_to_open', 'quantity': 1},
                    {'option_symbol': 'IWM260116P00205000', 'side': 'buy_to_open', 'quantity': 1}
                ]
            }
        ]

        # 3. Test: Limit is 5. We have 3 positions + 2 pending = 5.
        # Should return ['2026-01-16'] as full.
        exclusions = self.strategy._check_expiry_constraints('IWM', is_put=True, max_lots=5)
        
        self.assertIn('2026-01-16', exclusions)
        print(f"Verified: Expiry 2026-01-16 excluded with 3 positions and 2 pending multileg orders.")

    @patch('bot.strategies.credit_spreads.datetime')
    def test_check_expiry_constraints_pending_legs_fallback(self, mock_datetime):
        # Mock current time
        mock_datetime.now.return_value = datetime(2025, 12, 30)
        mock_datetime.strptime.side_effect = datetime.strptime

        # 1. Setup Mock Positions (0 existing)
        self.tradier.get_positions.return_value = []

        # 2. Setup Mock Orders (5 pending lots for 2026-01-23 using 'legs' field as fallback)
        self.tradier.get_orders.return_value = [
            {
                'status': 'pending',
                'class': 'multileg',
                'symbol': 'IWM',
                'quantity': 5,
                'legs': [
                    {'option_symbol': 'IWM260123P00210000', 'side': 'sell_to_open', 'quantity': 1},
                    {'option_symbol': 'IWM260123P00215000', 'side': 'buy_to_open', 'quantity': 1}
                ]
            }
        ]

        # 3. Test: Limit is 5. We have 5 pending.
        exclusions = self.strategy._check_expiry_constraints('IWM', is_put=True, max_lots=5)
        
        self.assertIn('2026-01-23', exclusions)
        print(f"Verified: Expiry 2026-01-23 excluded with 5 pending orders (using 'legs' field).")

if __name__ == '__main__':
    unittest.main()
