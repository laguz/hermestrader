
import pandas as pd
from datetime import datetime
import sys
import os

# Mock the BacktestService logic in a simplified function to test the core logic change
def simulate_logic(price_sequence):
    """
    Simulate the credit spread logic.
    price_sequence: List of dicts [{'date': ..., 'close': ...}]
    """
    active_position = {
        'type': 'put_credit_spread',
        'short_strike': 100, # Put Credit Spread, Short Strike 100
        'width': 5,
        'credit': 1.0,
        'days_held': 0,
        'days_itm': 0,
        'close_on_next_day': False # New Flag
    }
    
    events = []
    
    print(f"Starting Simulation. Position: Short Put 100. Width 5.")
    
    for day_idx, data in enumerate(price_sequence):
        price = data['close']
        date_str = data['date']
        
        # --- LOGIC START ---
        if not active_position:
            break
            
        active_position['days_held'] += 1
        
        # Check ITM
        # Put Credit Spread: ITM if Price < Short Strike
        is_itm = False
        if price < active_position['short_strike']:
            is_itm = True
            
        if is_itm:
            active_position['days_itm'] += 1
        else:
            # Logic: If OTM, reset days_itm? 
            # CreditSpreads.py: "Trade {symbol} back OTM. Resetting counter."
            active_position['days_itm'] = 0
            active_position['close_on_next_day'] = False # Reset schedule if OTM
            
        # Check Close Schedule (Day 3 check effectively)
        # In CreditSpreads.py, it checks `close_on_next_day` at START of processing.
        # Here we are processing "End of Day" or "During Day".
        # If the flag was set YESTERDAY, we close TODAY.
        
        if active_position.get('close_on_next_day'):
             events.append(f"{date_str}: Price {price}. CLOSING (Scheduled from prev day). Days ITM: {active_position['days_itm']}")
             active_position = None
             continue

        # Check for ITM duration to SET schedule
        if active_position['days_itm'] >= 2:
            # CreditSpreads.py: "Trade {symbol} ITM for 2 days. Scheduled for close next session."
            active_position['close_on_next_day'] = True
            events.append(f"{date_str}: Price {price}. ITM Day {active_position['days_itm']}. Scheduling Close for Next Day.")
        else:
            events.append(f"{date_str}: Price {price}. ITM Day {active_position['days_itm']}. Holding.")

    return events

# Test Case
# Day 1: ITM (1)
# Day 2: ITM (2) -> Schedule Close
# Day 3: Execute Close

prices = [
    {'date': 'Day 1', 'close': 95}, # ITM
    {'date': 'Day 2', 'close': 95}, # ITM (2nd consecutive)
    {'date': 'Day 3', 'close': 95}, # Execution Day
    {'date': 'Day 4', 'close': 95},
]

print("--- Simulation ---")
results = simulate_logic(prices)
for r in results:
    print(r)
