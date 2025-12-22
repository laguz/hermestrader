import sys
import os
import unittest
from unittest.mock import MagicMock

# Adjust path
sys.path.append(os.getcwd())

from bot.money_manager import MoneyManager
from bot.strategies.wheel import WheelStrategy
from bot.strategies.credit_spreads import CreditSpreadStrategy

class TestMoneyManager(unittest.TestCase):
    def setUp(self):
        self.mock_tradier = MagicMock()
        self.mock_db = MagicMock()
        self.mock_wheel = MagicMock()
        self.mock_spread = MagicMock()
        
        self.mm = MoneyManager(self.mock_tradier, self.mock_db, self.mock_wheel, self.mock_spread)

    def test_inventory_accounting_wheel(self):
        """Test strict paired unit counting for Wheel."""
        # Case 1: 3 Puts, 2 Calls => Count should be 2
        # Need strikes for sort (even if wheel logic doesn't use spreads, get_inventory sorts everything)
        self.mock_tradier.get_positions.return_value = [
            {'symbol': 'XYZ', 'underlying': 'XYZ', 'quantity': -1, 'option_type': 'put', 'strike': 100},
            {'symbol': 'XYZ', 'underlying': 'XYZ', 'quantity': -1, 'option_type': 'put', 'strike': 100},
            {'symbol': 'XYZ', 'underlying': 'XYZ', 'quantity': -1, 'option_type': 'put', 'strike': 100},
            {'symbol': 'XYZ', 'underlying': 'XYZ', 'quantity': -1, 'option_type': 'call', 'strike': 110},
            {'symbol': 'XYZ', 'underlying': 'XYZ', 'quantity': -1, 'option_type': 'call', 'strike': 110}
        ]
        
        inv = self.mm.get_inventory('XYZ')
        print(f"Inventory Test 1 (3P/2C): {inv}")
        self.assertEqual(inv['wheel_count'], 2)

    def test_inventory_accounting_spread(self):
        """Test strict paired unit counting for Spreads."""
        # Case: 5 Call Spreads, 3 Put Spreads => Count should be 3
        # Call Spread: Short Call < Long Call? No, Bear Call = Sell Low (Short), Buy High (Long).
        # Put Spread: Bull Put = Sell High (Short), Buy Low (Long).
        
        self.mock_tradier.get_positions.return_value = [
             # 5 Call Spreads (Bear Call)
             {'symbol': 'XYZ', 'underlying': 'XYZ', 'strike': 100, 'quantity': -5, 'option_type': 'call'}, # Short
             {'symbol': 'XYZ', 'underlying': 'XYZ', 'strike': 105, 'quantity': 5, 'option_type': 'call'},  # Long (Protection)
             
             # 3 Put Spreads (Bull Put)
             {'symbol': 'XYZ', 'underlying': 'XYZ', 'strike': 90, 'quantity': -3, 'option_type': 'put'},   # Short
             {'symbol': 'XYZ', 'underlying': 'XYZ', 'strike': 85, 'quantity': 3, 'option_type': 'put'},    # Long (Protection)
        ]
        
        inv = self.mm.get_inventory('XYZ')
        print(f"Inventory Test 2 (5CS/3PS): {inv}")
        self.assertEqual(inv['spread_count'], 3)
        self.assertEqual(inv['details']['call_spreads'], 5)
        self.assertEqual(inv['details']['put_spreads'], 3)

    def test_wheel_ladder_cash_only(self):
        """Test Wheel Ladder with Cash Only -> Puts."""
        self.mock_tradier.get_positions.return_value = [] # Empty inventory
        
        # Determine Resources
        # Cash = True (default mock checks > 1000)
        self.mock_tradier.get_account_balances.return_value = {'option_buying_power': 5000}
        
        # Shares = False
        # (Positions empty implies 0 shares)
        
        target_qty = 2
        self.mm.process_symbol('XYZ', target_qty, 0)
        
        # Should call execute_single_leg('put') 2 times
        # Price ladder: 0.30, 0.40
        self.assertEqual(self.mock_wheel.execute_single_leg.call_count, 2)
        
        # Verify calls
        calls = self.mock_wheel.execute_single_leg.call_args_list
        # Call 1: Put, 0.30
        self.assertEqual(calls[0][0][1], 'put')
        self.assertEqual(calls[0][1]['min_credit'], 0.30)
        
        # Call 2: Put, 0.40
        self.assertEqual(calls[1][0][1], 'put')
        self.assertEqual(calls[1][1]['min_credit'], 0.40)
        print("Wheel Ladder (Cash Only) Verified.")

    def test_wheel_ladder_both_resources(self):
        """Test Wheel Ladder with Cash AND Shares -> Both."""
        # Inventory: 0 Units (Need 1)
        # But we HAVE shares.
        self.mock_tradier.get_positions.return_value = [
            {'symbol': 'XYZ', 'quantity': 100} # 100 Shares
        ]
        self.mock_tradier.get_account_balances.return_value = {'option_buying_power': 5000}
        
        self.mm.process_symbol('XYZ', 1, 0)
        
        # Should fire BOTH Put and Call for the 1 unit needed
        self.assertEqual(self.mock_wheel.execute_single_leg.call_count, 2)
        
        args = [c[0][1] for c in self.mock_wheel.execute_single_leg.call_args_list]
        self.assertIn('put', args)
        self.assertIn('call', args)
        print("Wheel Ladder (Both Resources) Verified.")

    def test_spread_ladder(self):
        """Test Spread Ladder -> Always Both."""
        self.mock_tradier.get_positions.return_value = []
        
        # Need 2 Spread Units
        self.mm.process_symbol('XYZ', 0, 2)
        
        # Should fire 2 rounds of BOTH (Total 4 calls)
        # Steps: 0.80, 0.90
        self.assertEqual(self.mock_spread.execute_spread.call_count, 4)
        
        print("Spread Ladder Verified.")

if __name__ == '__main__':
    unittest.main()
