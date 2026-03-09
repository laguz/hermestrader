import sys
import os
from datetime import datetime
from bot.strategies.wheel import WheelStrategy

class MockTradier:
    def __init__(self):
        self.account_id = "test_acct"
        self.current_date = datetime(2026, 3, 9, 10, 0, 0)
        
    def get_positions(self):
        return [
            {
                "symbol": "RIOT260313P00016000",
                "quantity": -3.0,
                "cost_basis": 150.0  # $0.50 entry price per contract
            }
        ]
        
    def get_orders(self):
        return []
        
    def get_quote(self, symbol):
        if symbol == 'RIOT':
            return {'last': 15.0}  # ITM
        if symbol == 'RIOT260313P00016000':
            return {'ask': 1.10} # 1.10 current ask (no profit)
        return {'last': 0, 'ask': 0}

    def get_option_chains(self, symbol, expiry):
        if expiry == '2026-03-13':
            return [{'symbol': 'RIOT260313P00016000', 'strike': 16.0, 'option_type': 'put', 'ask': 1.15}]
        elif expiry == '2026-04-24': # 46 DTE
            return [{'symbol': 'RIOT260424P00015000', 'strike': 15.0, 'option_type': 'put', 'bid': 1.50, 'greeks': {'delta': -0.15}}]
        return []

    def get_option_expirations(self, symbol):
        # Return 2026-04-24 which is 46 days away from Mar 9
        return ['2026-03-13', '2026-04-24']
        
    def place_order(self, **kwargs):
        print(f"MOCK ORDER PLACED: {kwargs}")
        return {'id': 'test_order_123', 'status': 'ok'}

class MockDB:
    def __getitem__(self, item):
        class MockCol:
            def find_one(self, *args, **kwargs): return None
            def update_one(self, *args, **kwargs): pass
            def insert_one(self, *args, **kwargs): pass
        return MockCol()

class MockAnalysis:
    def analyze_symbol(self, *args, **kwargs): return {}

tradier = MockTradier()
db = MockDB()

class DebugWheel(WheelStrategy):
    def _log(self, message):
        print(f"[WHEEL LOG] {message}")
        
    def _is_bp_sufficient(self, requirement):
        return True

wheel = DebugWheel(tradier, db, dry_run=False, analysis_service=MockAnalysis())

positions = tradier.get_positions()
print("Starting _manage_positions...")
wheel._manage_positions(positions, watchlist=['RIOT'], config={'max_wheel_contracts_per_symbol': 20})
print("Done.")

