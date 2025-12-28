
import sys
import os
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.backtest_service import BacktestService
from services.container import Container
from dotenv import load_dotenv

def main():
    load_dotenv()
    # Setup
    print("Initializing Backtest Service...")
    # We pass None or a dummy because BacktestService ignores it in favor of internal mocks
    backtester = BacktestService(None)

    # Date Range: July 10 2024 to Aug 20 2024 (Market Correction)
    # SPY Should drop, triggering assignment of Puts.
    start_date = "2024-07-10"
    end_date = "2024-08-20"
    
    symbol = "SPY"
    
    print(f"Running Wheel Strategy Backtest on {symbol} from {start_date} to {end_date}...")
    
    try:
        result = backtester.run_backtest(symbol, 'wheel', start_date, end_date)
        
        if "error" in result:
            print(f"Backtest Failed: {result['error']}")
            return

        print("\n--- Backtest Results ---")
        print(f"Trade Count: {result['metrics']['trade_count']}")
        print(f"Total Return: {result['metrics']['total_return']}")
        print(f"Final Value: {result['metrics']['final_value']}")
        
        print("\n--- Trade Log ---")
        assigned_count = 0
        call_sold_count = 0
        
        for trade in result['trades']:
            date = trade['date']
            action = trade['action']
            pnl = trade.get('pnl', 0)
            debit = trade.get('debit', '')
            credit = trade.get('credit', '')
            
            amt = f"-${debit:.2f}" if debit else (f"+${credit:.2f}" if credit else f"${pnl:.2f}")
            print(f"[{date}] {action} | {amt}")
            
            if "ASSIGNED" in action:
                assigned_count += 1
            if "OPEN" in action and "Call" in action: # Naive check, might need better parsing
                # OPEN SPY...C...
                 if "C00" in action: call_sold_count += 1
                 
        print("\n--- Verification ---")
        if assigned_count > 0:
            print("✅ SUCCESS: Stock Assignment triggered.")
        else:
            print("⚠️ WARNING: No Stock Assignment triggered (Market might not have dropped enough for 30 delta).")
            
        # Check if we sold a call after assignment
        # Logic: If we own stock, Wheel should sell call.
        if call_sold_count > 0:
             print("✅ SUCCESS: Covered Call sold.")
        else:
             if assigned_count > 0:
                 print("⚠️ WARNING: Assignment occurred but no Call sold? (Maybe price kept dropping?)")
             else:
                 print("ℹ️ Info: No Call sold (expected no assignment).")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
