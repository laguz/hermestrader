
import os
from services.container import Container
from services.backtest_service import BacktestService
from services.tradier_service import TradierService

def test_rulebase_backtest():
    # Setup Container with real tradier for history fetching
    # Note: TRADIER_ACCESS_TOKEN must be in env
    tradier = TradierService()
    backtester = BacktestService(tradier)
    
    symbol = "TSLA"
    strategy = "credit_spread_rulebase"
    start_date = "2025-10-01"
    end_date = "2025-12-31"
    
    print(f"Running backtest for {symbol} using {strategy}...")
    result = backtester.run_backtest(symbol, strategy, start_date, end_date)
    
    if "error" in result:
        print(f"Backtest Error: {result['error']}")
    else:
        print("Backtest Results:")
        print(f"Final Value: {result['metrics']['final_value']}")
        print(f"Total Return: {result['metrics']['total_return']}")
        print(f"Trade Count: {result['metrics']['trade_count']}")
        
        # Look for specific rule-based trades
        for trade in result['trades'][:10]:
            print(f"Date: {trade['date']} | Action: {trade['action']} | Credit/Debit: {trade.get('credit') or trade.get('debit')}")

if __name__ == "__main__":
    test_rulebase_backtest()
