
import sys
import os
from unittest.mock import MagicMock
import unittest

# Add current directory to path
sys.path.append(os.getcwd())

from bot.trade_manager import TradeManager, TradeAction

class TestTradeManagerFix(unittest.TestCase):
    def setUp(self):
        self.tradier = MagicMock()
        self.tradier.account_id = "test_account"
        self.db = MagicMock()
        self.tm = TradeManager(self.tradier, self.db)

    def test_single_leg_option_limit(self):
        action = TradeAction(
            strategy_id="WHEEL",
            symbol="RIOT",
            order_class="option",
            legs=[{"option_symbol": "RIOT260515P00010000", "side": "sell_to_open", "quantity": 1}],
            price=1.50,
            side="sell_to_open",
            quantity=1
        )
        self.tm.execute_strategy_order(action)
        args, kwargs = self.tradier.place_order.call_args
        self.assertEqual(kwargs.get('order_type'), 'limit')
        self.assertEqual(kwargs.get('option_symbol'), "RIOT260515P00010000")

    def test_single_leg_option_market(self):
        action = TradeAction(
            strategy_id="WHEEL",
            symbol="RIOT",
            order_class="option",
            legs=[{"option_symbol": "RIOT260515P00010000", "side": "sell_to_open", "quantity": 1}],
            price=None,
            side="sell_to_open",
            quantity=1
        )
        self.tm.execute_strategy_order(action)
        args, kwargs = self.tradier.place_order.call_args
        self.assertEqual(kwargs.get('order_type'), 'market')

    def test_multileg_credit(self):
        action = TradeAction(
            strategy_id="CREDIT_SPREAD",
            symbol="SPY",
            order_class="multileg",
            legs=[
                {"option_symbol": "SPY260515P00400000", "side": "sell_to_open", "quantity": 1},
                {"option_symbol": "SPY260515P00395000", "side": "buy_to_open", "quantity": 1}
            ],
            price=0.50,
            side="sell",
            quantity=1
        )
        self.tm.execute_strategy_order(action)
        args, kwargs = self.tradier.place_order.call_args
        self.assertEqual(kwargs.get('order_type'), 'credit')
        self.assertIsNone(kwargs.get('option_symbol'))

    def test_multileg_debit(self):
        action = TradeAction(
            strategy_id="CREDIT_SPREAD",
            symbol="SPY",
            order_class="multileg",
            legs=[
                {"option_symbol": "SPY260515P00400000", "side": "buy_to_close", "quantity": 1},
                {"option_symbol": "SPY260515P00395000", "side": "sell_to_close", "quantity": 1}
            ],
            price=0.10,
            side="buy",
            quantity=1
        )
        self.tm.execute_strategy_order(action)
        args, kwargs = self.tradier.place_order.call_args
        self.assertEqual(kwargs.get('order_type'), 'debit')

    def test_equity_limit(self):
        action = TradeAction(
            strategy_id="WHEEL",
            symbol="RIOT",
            order_class="equity",
            legs=[],
            price=10.00,
            side="buy",
            quantity=100
        )
        self.tm.execute_strategy_order(action)
        args, kwargs = self.tradier.place_order.call_args
        self.assertEqual(kwargs.get('order_type'), 'limit')
        self.assertIsNone(kwargs.get('option_symbol'))

if __name__ == "__main__":
    unittest.main()
