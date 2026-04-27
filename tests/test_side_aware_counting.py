
import sys
import os
from unittest.mock import MagicMock
import unittest

# Add current directory to path
sys.path.append(os.getcwd())

from bot.strategies.base_strategy import AbstractStrategy

class MockStrategy(AbstractStrategy):
    def __init__(self, tradier, db, strategy_id):
        self.tradier = tradier
        self.db = db
        self.strategy_id = strategy_id
        self.trade_manager = None
    
    def execute(self, watchlist, config=None):
        pass

class TestSideAwareCounting(unittest.TestCase):
    def setUp(self):
        self.tradier = MagicMock()
        self.db = MagicMock()
        self.strategy = MockStrategy(self.tradier, self.db, strategy_id="TEST_STRAT")
        
    def test_count_by_side(self):
        # Mock open trades: 3 Puts and 2 Calls
        self.strategy.get_open_trades = MagicMock(return_value=[
            {
                "symbol": "RIOT",
                "legs_info": [{"option_symbol": "RIOT260515P00010000", "side": "sell_to_open", "quantity": 3}]
            },
            {
                "symbol": "RIOT",
                "legs_info": [{"option_symbol": "RIOT260515C00020000", "side": "sell_to_open", "quantity": 2}]
            },
            {
                "symbol": "SPY",
                "legs_info": [{"option_symbol": "SPY260515P00400000", "side": "sell_to_open", "quantity": 1}]
            }
        ])
        
        # Test RIOT Put count
        self.assertEqual(self.strategy._count_existing_on_symbol("RIOT", side="put"), 3)
        
        # Test RIOT Call count
        self.assertEqual(self.strategy._count_existing_on_symbol("RIOT", side="call"), 2)
        
        # Test RIOT total count (no side)
        self.assertEqual(self.strategy._count_existing_on_symbol("RIOT"), 5)
        
        # Test non-existent side
        self.assertEqual(self.strategy._count_existing_on_symbol("RIOT", side="none"), 0)

if __name__ == "__main__":
    unittest.main()
