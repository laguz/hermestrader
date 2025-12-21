
import sys
import os
sys.path.append(os.getcwd())

from services.container import Container
from dotenv import load_dotenv
import logging

load_dotenv()

def debug_wheel_state():
    print("--- Debugging Wheel Strategy State ---")
    
    tradier = Container.get_tradier_service()
    
    # 1. Fetch Positions
    try:
        positions = tradier.get_positions()
        print(f"Total Positions: {len(positions)}")
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return

    # Watchlist
    watchlist = ['SPY', 'IWM', 'QQQ', 'DIA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
    
    for symbol in watchlist:
        print(f"\nAnalyzing {symbol}:")
        
        # Filter positions
        symbol_positions = [p for p in positions if p.get('symbol') == symbol or p.get('underlying') == symbol]
        
        shares_held = sum(int(p['quantity']) for p in symbol_positions if p['symbol'] == symbol)
        options_held = [p for p in symbol_positions if p['symbol'] != symbol]
        
        short_puts = [o for o in options_held if o['option_type'] == 'put' and o['quantity'] < 0]
        short_calls = [o for o in options_held if o['option_type'] == 'call' and o['quantity'] < 0]
        
        print(f"  • Shares Held: {shares_held}")
        print(f"  • Short Puts: {len(short_puts)}")
        print(f"  • Short Calls: {len(short_calls)}")
        
        # Logic Re-trace
        decision = "UNKNOWN"
        
        if short_puts:
            decision = "MONITOR_PUT (Active Short Put)"
        elif shares_held >= 100:
            open_call_contracts = abs(sum(o['quantity'] for o in short_calls))
            free_shares = shares_held - (open_call_contracts * 100)
            if free_shares >= 100:
                decision = "SELL_CALL (Has Unencumbered Shares)"
            else:
                decision = "MONITOR_SHARES (All shares covered)"
        elif not short_puts and not short_calls:
            decision = "SELL_PUT (Clean State)"
            
        print(f"  -> Decision: {decision}")

if __name__ == "__main__":
    debug_wheel_state()
