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
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    
    print(f"Running LONG CALL Backtest on SPY from {start_date} to {end_date}...")
    result = backtester.run_backtest('SPY', 'long_call', start_date, end_date)
    
    if "error" in result:
        print(f"❌ Error: {result['error']}")
        return

    print("\n--- Backtest Results ---")
    print(f"Total Return: {result['metrics']['total_return']}")
    print(f"Trade Count: {result['metrics']['trade_count']}")
    
    if result['trades']:
        print("✅ Trades generated.")
        print(f"Sample: {result['trades'][0]}")
    else:
        print("⚠️ No trades generated.")

if __name__ == "__main__":
    main()
