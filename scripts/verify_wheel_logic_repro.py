import sys
import os
import logging
from datetime import datetime, date, timedelta
from unittest.mock import MagicMock

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from bot.strategies.wheel import WheelStrategy

def verify_wheel_logic():
    print("--- Verifying Wheel Strategy Put Logic ---")
    
    # 1. Setup Mocks
    mock_tradier = MagicMock()
    mock_db = MagicMock()
    
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=True)
    
    # Mock Constants if needed (already set in __init__)
    # strategy.TARGET_DTE = 42 (6 weeks)
    
    # 2. Define Scenario Data
    symbol = "TEST"
    current_price = 14.50
    
    # Analysis Mock: Support at 13, POP 60% (Valid)
    # Also add a "trap" support: Strike 12 (lower), Strike 14 (closer but maybe different POP)
    analysis_data = {
        'current_price': current_price,
        'put_entry_points': [
            {'price': 12, 'pop': 65, 'type': 'support'}, # Valid but lower
            {'price': 13, 'pop': 60, 'type': 'support'}, # Target (Closest valid)
            {'price': 14.2, 'pop': 40, 'type': 'support'} # Invalid POP (too low)
        ],
        'call_entry_points': [] 
    }
    
    # 3. Mock Expirations
    # Target date: Dec 21 + 42 days = Feb 1, 2026.
    # We want '2026-01-30' to be chosen.
    # Let's provide a list of expirations around that time.
    mock_tradier.get_option_expirations.return_value = [
        '2026-01-16',
        '2026-01-23',
        '2026-01-30', # Target (Friday)
        '2026-02-06',
        '2026-02-13'
    ]
    
    # 4. Mock Option Chain (Needed for _execute_order call usually, but _execute_order fetches it too)
    # Actually _entry_sell_put calls _execute_order which fetches chain.
    # But _entry_sell_put also calls _find_delta_strike ONLY if Technical fails.
    # Since we expect Technical to succeed, we might not need chain for selection.
    # However, _execute_order fetches chain to find option symbol.
    
    mock_chain = [
        {'strike': 13.0, 'option_type': 'put', 'symbol': 'TEST260130P00013000', 'bid': 0.50, 'ask': 0.60},
        {'strike': 12.0, 'option_type': 'put', 'symbol': 'TEST260130P00012000', 'bid': 0.30, 'ask': 0.40}
    ]
    mock_tradier.get_option_chains.return_value = mock_chain
    
    # 5. Execute Logic
    print(f"Current Price: {current_price}")
    print(f"Supports: {analysis_data['put_entry_points']}")
    
    strategy._entry_sell_put(symbol, current_price, analysis_data)
    
    # 6. Verify Results
    print("\n--- Execution Logs ---")
    for log in strategy.execution_logs:
        print(log)
        
    print("\n--- Order Verification ---")
    # Check if _execute_order was called (or since we didn't mock it, check if tradier.place_order was called via dry run recording)
    # In dry_run=True, _execute_order calls _record_trade instead of place_order.
    
    # But we can inspect the logs to see what was "Found".
    
    # Logic check:
    # 1. Expiry Selection
    # Today: 2025-12-21. +6 weeks = 2026-02-01.
    # Closest expiry to Feb 1 provided is Jan 30 (2 days diff) vs Feb 6 (5 days diff).
    # Expected: 2026-01-30.
    
    # 2. Strike Selection
    # Supports: 12, 13, 14.2
    # 14.2: < 14.50 (True), POP 40 (False - Min 55). REJECT.
    # 13: < 14.50 (True), POP 60 (True). VALID.
    # 12: < 14.50 (True), POP 65 (True). VALID.
    # Sort Descending: 13, 12.
    # First Match: 13.
    
    # Expected Result: 13 Strike, Jan 30 Expiry.
    
    has_success_log = any("Found Technical Entry: Strike 13" in log for log in strategy.execution_logs)
    if has_success_log:
        print("✅ SUCCESS: Logic correctly identified Strike 13.")
    else:
        print("❌ FAILURE: Logic did not identify Strike 13.")

if __name__ == "__main__":
    verify_wheel_logic()
