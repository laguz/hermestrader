import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Make sure the project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.ml_service import MLService
import pandas as pd
from datetime import datetime, timedelta

class MockTradier:
    pass

@pytest.fixture
def ml_service():
    with patch('services.container.Container.get_db') as mock_db:
        # Mocking db with basic collections
        mock_collection = MagicMock()
        mock_db.return_value = {'market_data': mock_collection}
        service = MLService(MockTradier())
        service.db = mock_db.return_value
        return service

def test_evaluate_model_unsupported_model_type(ml_service):
    # Setup mock data for evaluate_model
    # We need enough data to pass the early checks
    now = datetime.now()
    dates = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(100)]
    mock_data = [
        {"symbol": "AAPL", "date": d, "close": 150.0 + i, "volume": 1000 + i*10, "high": 155.0, "low": 145.0, "open": 149.0}
        for i, d in enumerate(dates)
    ]

    ml_service.db['market_data'].find.return_value.sort.return_value = mock_data

    with patch.object(ml_service, 'prepare_features') as mock_prep:
        # Need target column and features to be set properly
        def mock_prepare_features(df):
            df['target'] = df['close'].shift(-1)
            # Add some dummy features so dropna doesn't clear everything
            for f in ml_service.default_features:
                if f not in df.columns:
                    df[f] = 1.0
            return df

        mock_prep.side_effect = mock_prepare_features

        # Call evaluate_model with an unsupported model_type to trigger the exception and error handling
        result = ml_service.evaluate_model("AAPL", days=1, model_type="unsupported_model")

        # Verify the exception was caught and returned as an error dictionary
        assert isinstance(result, dict)
        assert "error" in result
        assert "Unknown model_type for evaluation: unsupported_model" in result["error"]
