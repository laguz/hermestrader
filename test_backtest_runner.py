import logging
logging.basicConfig(level=logging.INFO)
from services.backtest_service import BacktestService
from services.tradier_service import TradierService

tradier = TradierService()
bt = BacktestService(tradier)

print("Running Backtest for SPY...")
result = bt.run_backtest(
    symbol="SPY",
    strategy_type="wheel",
    start_date="2023-01-01",
    end_date="2024-01-01"
)
print("\n--- RESULTS ---")
print(result.get('metrics', result.get('error', 'No metrics found.')))
