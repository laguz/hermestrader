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
    
    # Run backtest for last 6 months to ensure enough data for indicators and trades
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    
    print(f"Running Credit Spread Backtest on SPY from {start_date} to {end_date}...")
    result = backtester.run_backtest('SPY', 'credit_spread', start_date, end_date)
    
    if "error" in result:
        print(f"❌ Error: {result['error']}")
        return

    print("\n--- Backtest Results ---")
    print(f"Total Return: {result['metrics']['total_return']}")
    print(f"Final Value: {result['metrics']['final_value']}")
    print(f"Trade Count: {result['metrics']['trade_count']}")
    
    if result['trades']:
        print("\nLast 5 Trades:")
        for trade in result['trades'][-5:]:
            print(trade)
    else:
        print("\n⚠️ No trades generated. This might be due to strict entry conditions or market conditions.")

if __name__ == "__main__":
    main()
