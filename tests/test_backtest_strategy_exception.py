from unittest.mock import MagicMock, patch
from services.backtest_service import BacktestService

class FaultyStrategy:
    def execute(self, symbols):
        raise Exception("Faulty strategy crashed!")

def test_backtest_strategy_exception():
    """Test that an exception inside the strategy during a backtest is gracefully caught."""
    mock_tradier_real = MagicMock()
    # Provide enough historical data to trigger strategy evaluation
    mock_tradier_real.get_historical_pricing.return_value = [
        {"date": "2024-01-01", "close": 100, "high": 105, "low": 95, "open": 98, "volume": 1000},
        {"date": "2024-01-02", "close": 102, "high": 106, "low": 98, "open": 100, "volume": 1200}
    ]

    service = BacktestService(mock_tradier_real)

    # Disable DB dependency so it uses mocked tradier data only
    with patch("services.container.Container.get_db", return_value=None):
        # Override _setup_strategy to return our failing strategy
        with patch.object(service, "_setup_strategy", return_value=FaultyStrategy()):
            result = service.run_backtest(
                symbol="SPY",
                strategy_type="wheel",
                start_date="2024-01-01",
                end_date="2024-01-02"
            )

            # Ensure backtest handles the crash rather than propagating the exception
            assert result is not None
            assert "error" in result
            assert "Backtest failed: Faulty strategy crashed!" in result["error"]
