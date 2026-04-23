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

def test_prepare_lstm_data_matrix_transformation(ml_service):
    """
    Test that the _prepare_lstm_data function successfully transforms
    a 2D input array (samples, features) into a 3D output array
    (samples - sequence_length, sequence_length, features) for LSTM ingestion.
    """
    df = pd.DataFrame({
        'close': [1, 2, 3, 4, 5, 6, 7, 8],
        'volume': [10, 20, 30, 40, 50, 60, 70, 80],
        'target': [0, 1, 0, 1, 0, 1, 0, 1]
    })
    features = ['close', 'volume']

    # ML service has sequence_length = 5
    X, y, scaler = ml_service._prepare_lstm_data(df, features, fit_scaler=True)

    # Input is 2D: 8 rows, 2 features
    # Output should be 3D: (8 - 5) = 3 samples, 5 timesteps, 2 features
    assert len(X.shape) == 3
    assert X.shape == (3, 5, 2)
    assert len(y.shape) == 1
    assert y.shape == (3,)
