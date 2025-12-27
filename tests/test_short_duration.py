import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.tradier_service import TradierService
from services.backtest_service import BacktestService
from dotenv import load_dotenv
from datetime import datetime, timedelta

def main():
    load_dotenv()
    tradier = TradierService()
    backtester = BacktestService(tradier)
    
    # 30 days only - likely not enough for indicators
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    print(f"Running Short Duration Backtest on SPY from {start_date} to {end_date}...")
    result = backtester.run_backtest('SPY', 'credit_spread', start_date, end_date)
    
    if "error" in result:
        print(f"❌ Error: {result['error']}")
        return

    print("\n--- Backtest Results ---")
    print(f"Trade Count: {result['metrics']['trade_count']}")
    
    if not result['trades']:
        print("⚠️ No trades generated (Expected failure due to warmup period).")
        # Check if indicators are NaN
        # We can't easily check internal DF state here without modifying service, 
        # but 0 trades confirms the symptom.

if __name__ == "__main__":
    main()
