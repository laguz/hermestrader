import unittest
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

def test_prepare_lstm_data_insufficient_data(ml_service):
    """
    Test that _prepare_lstm_data returns empty arrays if the input
    data has fewer rows than the sequence_length.
    """
    df = pd.DataFrame({
        'close': [1, 2, 3],
        'volume': [10, 20, 30],
        'target': [0, 1, 0]
    })
    features = ['close', 'volume']

    # ML service has sequence_length = 5
    X, y, scaler = ml_service._prepare_lstm_data(df, features, fit_scaler=True)

    assert X.size == 0
    assert y.size == 0

def test_build_lstm_model_happy_path(ml_service):
    """
    Test that the _build_lstm_model function returns a compiled Keras Sequential
    model with the expected configuration.
    """
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout, InputLayer

    input_shape = (5, 2)
    model = ml_service._build_lstm_model(input_shape)

    # Check that model is a Keras Sequential model
    assert isinstance(model, Sequential)

    # Check that model has the correct number of layers (LSTM, Dropout, LSTM, Dropout, Dense, Dense)
    # Plus InputLayer which is implicit or explicit depending on TF version.
    # The current ml_service uses `model.add(Input(shape=input_shape))` which adds an InputLayer in recent TF.
    # We will verify some specific layers are present
    assert len(model.layers) >= 6

    # The last layer should predict a single continuous value directly (1 unit, linear/no activation specifically set)
    assert isinstance(model.layers[-1], Dense)
    assert model.layers[-1].units == 1

    # It should have an optimizer and loss function since it's compiled
    assert model.optimizer is not None
    assert model.loss is not None
    assert model.loss == 'mean_squared_error'

from unittest.mock import MagicMock, patch
from exceptions import ExternalServiceError, ResourceNotFoundError, ValidationError

@patch('services.ml_service.load_model')
@patch('services.ml_service.joblib.load')
@patch('os.path.exists')
@patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='["close", "volume"]')
def test_predict_next_day_lstm_happy_path(mock_open, mock_exists, mock_joblib_load, mock_load_model, ml_service):
    """Test the happy path of predict_next_day for LSTM model."""
    # Setup mock data
    symbol = "AAPL"

    # Mock os.path.exists to return True for model, scaler, feature file
    mock_exists.return_value = True

    # Mock joblib.load for scaler
    mock_scaler = MagicMock()
    # Let's say shape is (sequence_length, len(features))
    mock_scaler.transform.return_value = np.zeros((ml_service.sequence_length, 2))

    mock_target_scaler = MagicMock()
    # inverse transform returns 2D array, we extract [0][0]
    mock_target_scaler.inverse_transform.return_value = np.array([[0.05]])

    # Joblib load will be called multiple times:
    # 1. scaler
    # 2. target_scaler (if exists, which we mocked True)
    mock_joblib_load.side_effect = [mock_scaler, mock_target_scaler]

    # Mock Keras model
    mock_model = MagicMock()
    mock_model.predict.return_value = np.array([[0.5]]) # Not actually used if target scaler exists
    mock_load_model.return_value = mock_model

    # Mock database
    ml_service.db = MagicMock()
    mock_market_data = MagicMock()
    mock_predictions = MagicMock()
    def mock_db_getitem(key):
        if key == 'market_data': return mock_market_data
        if key == 'predictions': return mock_predictions
        return MagicMock()
    ml_service.db.__getitem__.side_effect = mock_db_getitem

    # Mock DB cursor for market_data
    # We need sequence_length + enough data for prep_features if we don't mock it.
    # We will mock `prepare_features` to simplify.

    # Need to give some initial data to get past the first empty check
    mock_market_data.find.return_value.sort.return_value = [
        {"date": "2023-01-01", "close": 150.0, "volume": 1000},
        {"date": "2023-01-02", "close": 151.0, "volume": 1100}
    ]

    # Mock prepare_features
    # Our mocked sequence length is 5, so we need at least 5 rows
    df_prepared = pd.DataFrame({
        'date': pd.date_range(start='1/1/2023', periods=10),
        'close': np.linspace(100, 110, 10),
        'volume': np.linspace(1000, 1100, 10)
    })
    ml_service.prepare_features = MagicMock(return_value=df_prepared)

    # Mock DB cursor for predictions (bias correction)
    mock_limit = MagicMock()
    mock_limit.limit.return_value = [
        {"predicted_price": 105.0, "actual_close_price": 100.0},
        {"predicted_price": 106.0, "actual_close_price": 101.0}
    ]
    mock_predictions.find.return_value.sort.return_value = mock_limit

    # Mock find_one for actual_close_price
    mock_market_data.find_one.return_value = {"close": 115.0}

    # Execute
    result = ml_service.predict_next_day(symbol, model_type='lstm')

    # Asserts
    # raw_prediction = last_close * np.exp(pred_log_return)
    # pred_log_return is 0.05
    # last_close is 110.0 (last element of linspace 100..110)
    # 110.0 * np.exp(0.05) ~= 110.0 * 1.05127 ~= 115.64
    # bias is 5.0
    # prediction = 115.64 - 5.0 = 110.64

    assert result['symbol'] == "AAPL"
    assert result['model'] == "lstm"
    assert "predicted_price" in result
    assert result['last_close'] == 110.0
    assert result['used_features'] == ["close", "volume"]
    assert "change" in result
    assert "percent_change_str" in result

    # Check DB was called to save
    mock_predictions.update_one.assert_called_once()

def test_predict_next_day_lstm_no_data(ml_service):
    """Test predict_next_day when no recent data is found and backfill fails."""
    symbol = "AAPL"

    ml_service.db = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.sort.return_value = []
    ml_service.db['market_data'].find.return_value = mock_cursor

    # Mock backfill to fail
    ml_service.backfill_symbol = MagicMock(return_value=False)

    with pytest.raises(ExternalServiceError, match="No recent data found in DB and backfill failed"):
        ml_service.predict_next_day(symbol, model_type='lstm')

def test_predict_next_day_lstm_model_not_found(ml_service):
    """Test predict_next_day when model file does not exist."""
    symbol = "AAPL"

    ml_service.db = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.sort.return_value = [
        {"date": "2023-01-01", "close": 150.0}
    ]
    ml_service.db['market_data'].find.return_value = mock_cursor

    # Mock prepare_features
    df_prepared = pd.DataFrame({
        'date': pd.date_range(start='1/1/2023', periods=10),
        'close': np.linspace(100, 110, 10),
    })
    ml_service.prepare_features = MagicMock(return_value=df_prepared)

    with patch('os.path.exists', return_value=False):
        with pytest.raises(ResourceNotFoundError, match="LSTM model for AAPL not found"):
            ml_service.predict_next_day(symbol, model_type='lstm')

@patch('services.ml_service.load_model')
@patch('services.ml_service.joblib.load')
@patch('os.path.exists')
@patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='["close", "volume"]')
def test_predict_next_day_lstm_not_enough_data(mock_open, mock_exists, mock_joblib_load, mock_load_model, ml_service):
    """Test predict_next_day when there's not enough data for sequence length."""
    symbol = "AAPL"

    # Mock os.path.exists to return True
    mock_exists.return_value = True

    ml_service.db = MagicMock()
    mock_cursor = MagicMock()
    # Need some data to get past the initial check
    mock_cursor.sort.return_value = [
        {"date": "2023-01-01", "close": 150.0}
    ]
    ml_service.db['market_data'].find.return_value = mock_cursor

    # We set sequence_length to 5
    ml_service.sequence_length = 5

    # Provide only 3 rows
    df_prepared = pd.DataFrame({
        'date': pd.date_range(start='1/1/2023', periods=3),
        'close': [100.0, 101.0, 102.0],
        'volume': [1000, 1100, 1200]
    })
    ml_service.prepare_features = MagicMock(return_value=df_prepared)

    mock_joblib_load.return_value = MagicMock()
    mock_load_model.return_value = MagicMock()

    with pytest.raises(ValidationError, match="Not enough valid data for LSTM sequence"):
        ml_service.predict_next_day(symbol, model_type='lstm')
