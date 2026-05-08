
import sys
from unittest.mock import MagicMock

# Mock out heavy dependencies that might be missing
mock_modules = [
    'numpy', 'pandas', 'xgboost', 'sqlalchemy', 'sqlalchemy.orm',
    'sqlalchemy.dialects.postgresql', 'sklearn', 'sklearn.cluster',
    'mcp', 'mcp.server.fastmcp', 'tenacity', 'cryptography', 'cryptography.fernet',
    'scipy', 'scipy.signal', 'scipy.stats'
]
for mod in mock_modules:
    sys.modules[mod] = MagicMock()

import unittest
from datetime import date, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

# Now we can import from hermes
from hermes.service1_agent.strategies.wheel import WheelStrategy
from hermes.service1_agent.core import TradeAction, MoneyManager, IronCondorBuilder

class MockBroker:
    def __init__(self):
        self.get_quote_call_count = 0
        self.quoted_symbols = []
        self.current_date = None

    def get_quote(self, symbols: str) -> List[Dict[str, Any]]:
        self.get_quote_call_count += 1
        self.quoted_symbols.append(symbols)
        return [{"symbol": s.strip(), "last": 100.0} for s in symbols.split(",")]

    def roll_to_next_month(self, symbol):
        return symbol + "_NEXT"

class MockDB:
    def __init__(self):
        self.trades = []
        self.logs = []
    def open_trades(self, strategy_id):
        return self.trades
    def write_log(self, strategy_id, msg):
        self.logs.append(msg)
    def count_open_contracts(self, *args): return 0
    def count_pending_orders(self, *args): return 0

class TestWheelStrategyOptimized(unittest.TestCase):
    def test_manage_positions_batches_quotes(self):
        broker = MockBroker()
        db = MockDB()
        strategy_id = "WHEEL"

        # Setup 3 trades
        today = date.today()
        expiry = today + timedelta(days=5)
        expiry_str = expiry.strftime("%y%m%d")

        trades = []
        for sym in ["AAPL", "MSFT", "GOOG"]:
            occ = f"{sym}{expiry_str}P00090000"
            trades.append({
                "id": sym,
                "strategy_id": strategy_id,
                "symbol": sym,
                "short_leg": occ,
                "short_strike": 110.0, # ITM
                "lots": 1
            })
        db.trades = trades

        mm = MoneyManager(broker, db, {})
        strategy = WheelStrategy(broker, db, mm, IronCondorBuilder(mm), {}, False)
        strategy.today = lambda: today

        actions = strategy.manage_positions()

        self.assertEqual(len(actions), 3)
        self.assertEqual(broker.get_quote_call_count, 1)
        # Check that all symbols were in the single call (order might vary due to set)
        quoted_syms = set(broker.quoted_symbols[0].split(","))
        self.assertEqual(quoted_syms, {"AAPL", "MSFT", "GOOG"})

    def test_manage_positions_no_trades(self):
        broker = MockBroker()
        db = MockDB()
        db.trades = []
        mm = MoneyManager(broker, db, {})
        strategy = WheelStrategy(broker, db, mm, IronCondorBuilder(mm), {}, False)

        actions = strategy.manage_positions()
        self.assertEqual(len(actions), 0)
        self.assertEqual(broker.get_quote_call_count, 0)

if __name__ == "__main__":
    unittest.main()
