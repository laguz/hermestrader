import pytest
from unittest.mock import MagicMock, patch
from services.backtest_service import BacktestService
from datetime import datetime

class DummyStrategy:
    def execute(self, *args, **kwargs):
        pass

def test_backtest_strategy_exception_handling():
    """Test that BacktestService handles strategy execution errors gracefully."""

    # Setup mocks
    mock_tradier_real = MagicMock()
    mock_tradier_real.get_historical_pricing.return_value = [
        {"date": "2024-01-01", "close": 100, "high": 105, "low": 95, "open": 98, "volume": 1000},
        {"date": "2024-01-02", "close": 102, "high": 106, "low": 98, "open": 100, "volume": 1200}
    ]

    service = BacktestService(mock_tradier_real)

    with patch("services.container.Container.get_db", return_value=None):
        with patch.object(service, "_process_open_orders", side_effect=Exception("Intentional exception during order processing")):
            # Mock _setup_strategy to return our DummyStrategy instead of looking it up in the registry
            with patch.object(service, "_setup_strategy", return_value=DummyStrategy()):
                # Run the backtest using any strategy type since it's mocked
                result = service.run_backtest(
                    symbol="SPY",
                    strategy_type="dummy_strategy",
                    start_date="2024-01-01",
                    end_date="2024-01-02"
                )

                # Verify the backtest completes without crashing and returns results
                assert result is not None
                assert "metrics" in result
                assert "error" not in result
