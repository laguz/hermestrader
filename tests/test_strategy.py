from services.tradier_service import TradierService
from services.backtest_service import BacktestService
from dotenv import load_dotenv
from datetime import datetime, timedelta

def test_credit_spread_backtest():
    load_dotenv()
    tradier = TradierService()
    backtester = BacktestService(tradier)
    
    # Run backtest for last 6 months
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    
    print(f"Running Credit Spread Backtest on SPY from {start_date} to {end_date}...")
    result = backtester.run_backtest('SPY', 'credit_spread', start_date, end_date)
    
    assert "error" not in result, f"Backtest failed with error: {result.get('error')}"
    
    print("\n--- Backtest Results ---")
    print(f"Total Return: {result['metrics']['total_return']}")
    
    # Basic assertions to ensure it ran
    assert 'metrics' in result
    assert 'total_return' in result['metrics']
    assert 'trades' in result

if __name__ == "__main__":
    test_credit_spread_backtest()

