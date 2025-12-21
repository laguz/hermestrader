import unittest
from unittest.mock import MagicMock, patch
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.strategies.credit_spreads import CreditSpreadStrategy

class TestClosingLogic(unittest.TestCase):
    def setUp(self):
        self.mock_tradier = MagicMock()
        self.mock_db = MagicMock()
        # Mock Collection
        self.mock_collection = MagicMock()
        self.mock_db.__getitem__.return_value = self.mock_collection
        
        self.strategy = CreditSpreadStrategy(self.mock_tradier, self.mock_db, dry_run=True)

    @patch('bot.strategies.credit_spreads.datetime')
    def test_itm_tracking_and_close(self, mock_datetime):
        # 1. Setup Mock Trade in DB
        trade_doc = {
            '_id': 'mock_id',
            'symbol': 'TEST',
            'status': 'OPEN',
            'short_leg': 'TEST230120P00100000', # Strike 100 Put
            'long_leg': 'TEST230120P00095000',
            'days_itm': 0,
            'last_check_date': '2023-01-01',
            'close_on_next_day': False
        }
        self.mock_collection.find.return_value = [trade_doc]
        
        # 2. Setup Tradier Positions (Trade exists)
        self.mock_tradier.get_positions.return_value = [
            {'symbol': 'TEST230120P00100000', 'quantity': -1}, # Short
            {'symbol': 'TEST230120P00095000', 'quantity': 1}   # Long
        ]
        
        # 3. Setup Quote (ITM)
        # Put Strike 100. Price 90 -> ITM.
        self.mock_tradier.get_quote.return_value = {'last': 90.0}
        
        # --- Run Day 1 (ITM) ---
        # Mock Time: 3:30 PM (15:30) on 2023-01-02
        mock_datetime.now.return_value = datetime(2023, 1, 2, 15, 30)
        
        self.strategy.manage_positions()
        
        # Verify Update: days_itm -> 1
        self.mock_collection.update_one.assert_called()
        args, kwargs = self.mock_collection.update_one.call_args
        update_doc = args[1]['$set']
        self.assertEqual(update_doc['days_itm'], 1)
        self.assertEqual(update_doc['last_check_date'], '2023-01-02')
        # Check if set, or just not True
        self.assertFalse(update_doc.get('close_on_next_day', False))
        
        # --- Run Day 2 (ITM Again) ---
        # Update mock state to reflect db update
        trade_doc['days_itm'] = 1
        trade_doc['last_check_date'] = '2023-01-02'
        
        # Mock Time: 3:30 PM on 2023-01-03
        mock_datetime.now.return_value = datetime(2023, 1, 3, 15, 30)
        
        self.strategy.manage_positions()
        
        # Verify Update: days_itm -> 2, close_on_next_day -> True
        args, kwargs = self.mock_collection.update_one.call_args
        update_doc = args[1]['$set']
        self.assertEqual(update_doc['days_itm'], 2)
        self.assertTrue(update_doc['close_on_next_day'])
        
        print("Day 2 Check Passed: Flagged for close.")
        
        # --- Run Day 3 (Execution) ---
        trade_doc['days_itm'] = 2
        trade_doc['last_check_date'] = '2023-01-03'
        trade_doc['close_on_next_day'] = True
        
        mock_datetime.now.return_value = datetime(2023, 1, 4, 15, 30)
        
        self.strategy.manage_positions()
        
        # Verify Close Execution
        # Should call update_one with "CLOSED_STOP_LOSS" (Dry Run Logic)
        args, kwargs = self.mock_collection.update_one.call_args
        update_doc = args[1]['$set']
        
        # Note: Depending on logic flow, it might update 'last_check_date' OR 'status'.
        # If it executes close, it updates status.
        if 'status' in update_doc:
            self.assertEqual(update_doc['status'], 'CLOSED_STOP_LOSS')
            print("Day 3 Check Passed: Executed Close.")
        else:
            self.fail(f"Did not close position on Day 3. Updated: {update_doc}")

if __name__ == '__main__':
    unittest.main()
