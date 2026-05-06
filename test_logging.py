
import sys
import os
from unittest.mock import MagicMock
from datetime import date

# Add the workspace to sys.path
sys.path.append(os.getcwd())

from hermes.service1_agent.strategies import CreditSpreads75
from hermes.service1_agent.core import MoneyManager, IronCondorBuilder

def test_cs75_logging():
    # Mock dependencies
    broker = MagicMock()
    db = MagicMock()
    mm = MagicMock()
    ic = MagicMock()
    config = {"cs75_width": 5.0}
    
    strategy = CreditSpreads75(broker, db, mm, ic, config)
    
    # Mock data
    symbol = "AAPL"
    expiry = "2026-06-20"
    side = "put"
    lots = 1
    width = 5.0
    min_credit = 1.25
    current_price = 180.0
    
    analysis = {
        "current_price": current_price,
        "current_vol": 0.20,
        "avg_vol": 0.20,
        "key_levels": []
    }
    
    # Mock chain with strikes that will have POP < 75%
    # (High delta puts)
    broker.get_option_chains.return_value = [
        {"strike": "178", "option_type": "put", "symbol": "AAPL260620P00178000", "greeks": {"delta": -0.45}},
        {"strike": "175", "option_type": "put", "symbol": "AAPL260620P00175000", "greeks": {"delta": -0.35}},
    ]
    
    # Mock DB prediction
    db.latest_prediction.return_value = {"predicted_return": -0.05} # Bearish
    
    print("--- Running test_cs75_logging ---")
    strategy._build_spread_action(
        symbol=symbol, expiry=expiry, side=side, lots=lots, width=width,
        min_credit=min_credit, analysis=analysis, current_price=current_price
    )
    
    print("\nExecution Logs:")
    for log in strategy.execution_logs:
        print(log)

if __name__ == "__main__":
    test_cs75_logging()
