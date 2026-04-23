import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from sklearn.preprocessing import MinMaxScaler
from services.ml_service import MLService

class MockTradier:
    pass

@pytest.fixture
def ml_service():
    with patch('services.container.Container.get_db') as mock_db:
        mock_db.return_value = None
        service = MLService(MockTradier())
        service.sequence_length = 5 # Set to a small number for testing
        return service

def test_prepare_lstm_data_fit_scaler_happy_path(ml_service):
    df = pd.DataFrame({
        'close': np.random.rand(15),
        'volume': np.random.rand(15),
        'target': np.random.rand(15)
    })
    features = ['close', 'volume']

    X, y, scaler = ml_service._prepare_lstm_data(df, features, fit_scaler=True)

    # 15 rows, sequence of 5 -> 15 - 5 = 10 samples
    assert X.shape == (10, 5, 2)
    assert y.shape == (10,)
    assert isinstance(scaler, MinMaxScaler)

def test_prepare_lstm_data_transform_scaler(ml_service):
    df_train = pd.DataFrame({
        'close': np.random.rand(10),
        'volume': np.random.rand(10),
        'target': np.random.rand(10)
    })
    features = ['close', 'volume']
    _, _, scaler = ml_service._prepare_lstm_data(df_train, features, fit_scaler=True)

    df_test = pd.DataFrame({
        'close': np.random.rand(12),
        'volume': np.random.rand(12),
        'target': np.random.rand(12)
    })

    X, y, returned_scaler = ml_service._prepare_lstm_data(df_test, features, fit_scaler=False, scaler=scaler)

    # 12 rows, sequence of 5 -> 12 - 5 = 7 samples
    assert X.shape == (7, 5, 2)
    assert y.shape == (7,)
    assert returned_scaler is scaler

def test_prepare_lstm_data_missing_scaler(ml_service):
    df = pd.DataFrame({
        'close': np.random.rand(10),
        'target': np.random.rand(10)
    })
    features = ['close']

    with pytest.raises(ValueError, match="Scaler required for transforming data"):
        ml_service._prepare_lstm_data(df, features, fit_scaler=False, scaler=None)
