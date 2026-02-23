import os, sys
sys.path.insert(0, os.path.abspath('.'))
from bot.strategies.wheel import WheelStrategy

class MockTradier:
    account_id = "test"
    def get_positions(self): return []
    def get_orders(self): return []
    def get_option_expirations(self, symbol): return ["2026-03-27", "2026-04-03", "2026-04-10"]
    def get_option_chains(self, symbol, exp):
        return [
            {"symbol": "RIOT260403P00013000", "option_type": "put", "strike": 13.0, "bid": 0.26, "ask": 0.28, "greeks": {"delta": -0.15}}
        ]
    def get_quote(self, symbol):
        return {'ask': 0.28, 'last': 0.27}

class MockDB: pass

analysis = {
    'current_price': 15.68,
    'put_entry_points': [{'price': 13, 'type': 'support', 'strength': 4, 'pop': 81.0, 'pot': 56.3}]
}

ws = WheelStrategy(MockTradier(), MockDB(), dry_run=True)
ws._log = lambda msg: print(msg)
ws._record_trade = lambda *args: print("TRADE RECORDED:", args)
ws._entry_sell_put("RIOT", 15.68, analysis, max_lots=200)
