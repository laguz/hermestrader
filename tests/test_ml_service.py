from datetime import datetime, timedelta
from exceptions import ValidationError
import numpy as np
import pytest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
from services.ml_service import MLService

class MockTradier:
    pass

@pytest.fixture
def ml_service():
    with patch('services.container.Container.get_db') as mock_db:
        mock_db.return_value = None
        service = MLService(MockTradier())
        return service

def test_select_top_features_happy_path(ml_service):
    """Test happy path with known correlations to target."""
    df = pd.DataFrame({
        'target': [1, 2, 3, 4, 5],
        'high_corr': [1.1, 1.9, 3.2, 4.1, 4.9],  # Highly positively correlated
        'high_neg_corr': [5.1, 4.0, 3.1, 1.9, 1.0],  # Highly negatively correlated
        'low_corr': [1, 5, 2, 4, 3],  # Low correlation
        'zero_corr': [1, 1, 1, 1, 1]   # Zero correlation
    })

    top_features = ml_service.select_top_features(df, target_col='target', n_top=2)

    # high_corr and high_neg_corr should have the highest absolute correlation
    assert len(top_features) == 2
    assert 'high_corr' in top_features
    assert 'high_neg_corr' in top_features

def test_select_top_features_excludes_columns(ml_service):
    """Test that specific columns are excluded from being selected as features."""
    df = pd.DataFrame({
        'target': [1, 2, 3, 4, 5],
        'date': [1, 2, 3, 4, 5],  # Should be excluded
        'symbol': [1, 2, 3, 4, 5],  # Should be excluded
        'target_return': [1, 2, 3, 4, 5],  # Should be excluded
        'log_return': [1, 2, 3, 4, 5],  # Should be excluded
        'valid_feature': [1, 2, 3, 4, 5]  # Highly correlated
    })

    top_features = ml_service.select_top_features(df, target_col='target', n_top=10)

    assert len(top_features) == 1
    assert 'valid_feature' in top_features
    assert 'date' not in top_features
    assert 'symbol' not in top_features
    assert 'target_return' not in top_features
    assert 'log_return' not in top_features

def test_select_top_features_missing_target_fallback(ml_service):
    """Test fallback to 'close' column if target_col is missing."""
    df = pd.DataFrame({
        'close': [1, 2, 3, 4, 5],
        'feat1': [1, 2, 3, 4, 5],  # Perfectly correlated with close
        'feat2': [5, 4, 3, 2, 1]   # Perfectly negatively correlated with close
    })

    # We pass target_col='missing', so it should fallback to 'close'.
    # Because 'close' is not in the exclude list, it correlates perfectly with itself.
    # We expect 'close', 'feat1', and 'feat2' to all be top features.
    top_features = ml_service.select_top_features(df, target_col='missing', n_top=3)

    assert len(top_features) == 3
    assert 'close' in top_features
    assert 'feat1' in top_features
    assert 'feat2' in top_features

def test_select_top_features_fewer_features_than_n_top(ml_service):
    """Test when n_top is greater than available features."""
    df = pd.DataFrame({
        'target': [1, 2, 3, 4, 5],
        'feat1': [1, 2, 3, 4, 5]
    })

    top_features = ml_service.select_top_features(df, target_col='target', n_top=10)

    assert len(top_features) == 1
    assert top_features == ['feat1']

def test_prepare_lstm_data_matrix_transformation(ml_service):
    """
    Test that the _prepare_lstm_data function successfully transforms
    a 2D input array (samples, features) into a 3D output array
    (samples - sequence_length, sequence_length, features) for LSTM ingestion.
    """

    ml_service.sequence_length = 5

    df = pd.DataFrame({
        'close': [1, 2, 3, 4, 5, 6, 7, 8],
        'volume': [10, 20, 30, 40, 50, 60, 70, 80],
        'target': [0, 1, 0, 1, 0, 1, 0, 1]
    })
    features = ['close', 'volume']

    X, y, returned_scaler = ml_service._prepare_lstm_data(df, features, fit_scaler=True)

    # Input is 2D: 8 rows, 2 features
    # Output should be 3D: (8 - 5) = 3 samples, 5 timesteps, 2 features
    assert len(X.shape) == 3
    assert X.shape == (3, 5, 2)
    assert len(y.shape) == 1
    assert y.shape == (3,)

def test_select_top_features_key_error(ml_service):
    """Test when the target column and fallback 'close' column are missing."""
    import pytest
    df = pd.DataFrame({
        'feat1': [1, 2, 3, 4, 5],
        'feat2': [5, 4, 3, 2, 1]
    })
    with pytest.raises(KeyError):
        ml_service.select_top_features(df, target_col='missing', n_top=3)

def test_select_top_features_constant_feature(ml_service):
    """Test that features with zero variance (which result in NaN correlation) are handled correctly, although currently pandas `.sort_values()` might place NaNs at the end, it's good to document behavior."""
    df = pd.DataFrame({
        'target': [1, 2, 3, 4, 5],
        'constant': [1, 1, 1, 1, 1], # Correlation will be NaN
        'good': [1, 2, 3, 4, 5]
    })
    top_features = ml_service.select_top_features(df, target_col='target', n_top=2)

    # good should be first, constant might still be selected if n_top is large enough
    assert top_features[0] == 'good'
    assert 'constant' in top_features

def test_select_top_features_filtering(ml_service):
    """
    Test filtering the top features when there are more available features than n_top.
    Verifies that features are sorted by absolute correlation and top n are selected.
    """
    import pandas as pd
    df = pd.DataFrame({
        'target': [1, 2, 3, 4, 5, 6],
        'feat_perfect_pos': [1, 2, 3, 4, 5, 6],    # 1.0
        'feat_perfect_neg': [6, 5, 4, 3, 2, 1],    # -1.0
        'feat_high_pos': [1, 2, 3, 4, 5, 5],       # ~0.98
        'feat_low': [1, 6, 2, 5, 3, 4],            # near zero
        'feat_noise': [1, 1, 6, 6, 1, 1]           # near zero
    })

    top_features = ml_service.select_top_features(df, target_col='target', n_top=3)

    assert len(top_features) == 3
    # The top 3 should be perfect positive, perfect negative, and high positive
    assert 'feat_perfect_pos' in top_features
    assert 'feat_perfect_neg' in top_features
    assert 'feat_high_pos' in top_features

    # We also check that they are ordered correctly
    # Since both perfect pos and perfect neg have absolute correlation of 1.0, their order may vary, but feat_high_pos should be 3rd.
    assert top_features[2] == 'feat_high_pos'

def test_prepare_features_happy_path(ml_service):
    """Test prepare_features with all expected columns."""
    import numpy as np

    # Needs at least 50 rows for sma_50
    np.random.seed(42)
    df = pd.DataFrame({
        'open': np.random.uniform(100, 150, 60),
        'high': np.random.uniform(100, 150, 60),
        'low': np.random.uniform(100, 150, 60),
        'close': np.random.uniform(100, 150, 60),
        'volume': np.random.randint(1000, 10000, 60)
    })

    # Ensure high >= low
    df['high'] = df[['high', 'low']].max(axis=1)
    df['low'] = df[['high', 'low']].min(axis=1)

    result_df = ml_service.prepare_features(df)

    expected_columns = [
        'rsi', 'upper_bb', 'mid_bb', 'lower_bb', 'macd', 'macd_signal', 'sma_50',
        'obv', 'vwap', 'atr', 'close_lag_1', 'close_lag_2', 'close_lag_3', 'close_lag_5',
        'daily_return', 'daily_return_lag_1'
    ]

    for col in expected_columns:
        assert col in result_df.columns

def test_prepare_features_missing_columns(ml_service):
    """Test prepare_features with missing high/low columns, ensuring atr and vwap fallback to 0.0."""
    import numpy as np

    df = pd.DataFrame({
        'close': np.random.uniform(100, 150, 60),
        'volume': np.random.randint(1000, 10000, 60)
    })

    result_df = ml_service.prepare_features(df)

    assert 'atr' in result_df.columns
    assert 'vwap' in result_df.columns

    # Check that atr and vwap are set to 0.0
    assert (result_df['atr'] == 0.0).all()
    assert (result_df['vwap'] == 0.0).all()


def test_prepare_features_empty_df(ml_service):
    """Test prepare_features with an empty DataFrame."""
    df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    result_df = ml_service.prepare_features(df)

    expected_columns = [
        'rsi', 'upper_bb', 'mid_bb', 'lower_bb', 'macd', 'macd_signal', 'sma_50',
        'obv', 'vwap', 'atr', 'close_lag_1', 'close_lag_2', 'close_lag_3', 'close_lag_5',
        'daily_return', 'daily_return_lag_1'
    ]

    assert len(result_df) == 0
    for col in expected_columns:
        assert col in result_df.columns

def test_prepare_features_single_row(ml_service):
    """Test prepare_features with a single-row DataFrame."""
    df = pd.DataFrame({
        'open': [100.0],
        'high': [105.0],
        'low': [95.0],
        'close': [102.0],
        'volume': [1000.0]
    })
    result_df = ml_service.prepare_features(df)

    expected_columns = [
        'rsi', 'upper_bb', 'mid_bb', 'lower_bb', 'macd', 'macd_signal', 'sma_50',
        'obv', 'vwap', 'atr', 'close_lag_1', 'close_lag_2', 'close_lag_3', 'close_lag_5',
        'daily_return', 'daily_return_lag_1'
    ]

    assert len(result_df) == 1
    for col in expected_columns:
        assert col in result_df.columns

def test_build_lstm_model(ml_service):
    """Test the structure of the built LSTM model."""
    from services.ml_service import HAS_TENSORFLOW
    if not HAS_TENSORFLOW:
        pytest.skip("TensorFlow not available")

    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout

    input_shape = (10, 5)
    model = ml_service._build_lstm_model(input_shape)

    assert isinstance(model, Sequential)

    # Check layer types (Tensorflow >= 2.11 treats Input as separate from Sequential.layers sometimes, but layers list contains others)
    # The expected structure: LSTM, Dropout, LSTM, Dropout, Dense, Dense
    layer_types = [type(layer) for layer in model.layers]

    assert layer_types.count(LSTM) == 2
    assert layer_types.count(Dropout) == 2
    assert layer_types.count(Dense) == 2

    # Also verify optimizer and loss exist and match roughly
    # the name might be 'adam' or Adam object
    assert model.optimizer is not None
    assert getattr(model.optimizer, 'name', '').lower() == 'adam' or 'adam' in str(type(model.optimizer)).lower()

    assert model.loss is not None
    assert 'mean_squared_error' in str(model.loss) or model.loss == 'mean_squared_error' or getattr(model.loss, 'name', '') == 'mean_squared_error'

@patch('services.ml_service.datetime')
def test_backfill_symbol_date_calculation(mock_datetime, ml_service):
    """Test backfill_symbol correctly calculates start and end dates."""
    fixed_date = datetime(2023, 10, 10)
    mock_datetime.now.return_value = fixed_date
    mock_datetime.strftime = datetime.strftime

    ml_service.db = MagicMock()
    mock_collection = MagicMock()
    ml_service.db.__getitem__.return_value = mock_collection
    mock_bulk_write_result = MagicMock()
    mock_bulk_write_result.upserted_count = 1
    mock_bulk_write_result.modified_count = 0
    mock_collection.bulk_write.return_value = mock_bulk_write_result

    mock_history = [
        {"date": "2023-10-09", "open": "100.0", "high": "105.0", "low": "99.0", "close": "104.0", "volume": "1000"}
    ]
    ml_service.tradier.get_historical_pricing = MagicMock(return_value=mock_history)

    assert ml_service.backfill_symbol("AAPL", years=2) is True

    expected_end_date = "2023-10-10"
    expected_start_date = "2021-10-10" # 2023-10-10 - 2 * 365 days = 2021-10-10

    ml_service.tradier.get_historical_pricing.assert_called_once_with("AAPL", expected_start_date, expected_end_date)


def test_backfill_symbol_invalid_symbol(ml_service):
    """Test backfill_symbol raises ValidationError for invalid symbols."""
    with pytest.raises(ValidationError):
        ml_service.backfill_symbol("123INVALID!")

def test_backfill_symbol_db_none(ml_service):
    """Test backfill_symbol returns False when db is None."""
    ml_service.db = None
    assert ml_service.backfill_symbol("AAPL") is False

def test_backfill_symbol_tradier_error(ml_service):
    """Test backfill_symbol returns False when tradier API raises Exception."""
    ml_service.db = MagicMock()
    ml_service.tradier.get_historical_pricing = MagicMock(side_effect=Exception("API Error"))
    assert ml_service.backfill_symbol("AAPL") is False
    ml_service.tradier.get_historical_pricing.assert_called_once()

def test_backfill_symbol_no_data(ml_service):
    """Test backfill_symbol returns False when tradier API returns empty data."""
    ml_service.db = MagicMock()
    ml_service.tradier.get_historical_pricing = MagicMock(return_value=[])
    assert ml_service.backfill_symbol("AAPL") is False
    ml_service.tradier.get_historical_pricing.assert_called_once()

def test_backfill_symbol_success(ml_service):
    """Test backfill_symbol successfully fetches data and writes to DB."""
    ml_service.db = MagicMock()
    mock_collection = MagicMock()
    ml_service.db.__getitem__.return_value = mock_collection
    mock_bulk_write_result = MagicMock()
    mock_bulk_write_result.upserted_count = 2
    mock_bulk_write_result.modified_count = 0
    mock_collection.bulk_write.return_value = mock_bulk_write_result

    mock_history = [
        {"date": "2023-01-01", "open": "100.0", "high": "105.0", "low": "99.0", "close": "104.0", "volume": "1000"},
        {"date": "2023-01-02", "open": "104.0", "high": "106.0", "low": "103.0", "close": "105.0", "volume": "2000"}
    ]
    ml_service.tradier.get_historical_pricing = MagicMock(return_value=mock_history)

    assert ml_service.backfill_symbol("AAPL", years=1) is True
    ml_service.tradier.get_historical_pricing.assert_called_once()
    mock_collection.bulk_write.assert_called_once()

    # Verify the contents of bulk_write call
    operations = mock_collection.bulk_write.call_args[0][0]
    assert len(operations) == 2

    # Check that update values are parsed correctly
    first_op = operations[0]
    doc = first_op._doc["$set"]
    assert doc["symbol"] == "AAPL"
    assert doc["date"] == "2023-01-01"
    assert doc["open"] == 100.0
    assert doc["volume"] == 1000.0



def test_train_model_happy_path_lstm(ml_service):
    # Setup dataframe with 300 rows to pass walk-forward validation min size
    np.random.seed(42)
    df = pd.DataFrame({
        'close': np.random.rand(300),
        'volume': np.random.rand(300),
        'log_return': np.random.rand(300),
        'target': np.random.rand(300)
    })

    mock_model = MagicMock()
    # mock_model() returns a mock tensor that has .numpy()
    mock_tensor = MagicMock()
    # Assuming sequence length 5, test size 20
    mock_tensor.numpy.return_value = np.zeros((20, 1))
    mock_model.return_value = mock_tensor

    with patch.object(ml_service, '_fetch_and_prepare_training_data') as mock_fetch, \
         patch.object(ml_service, 'select_top_features', return_value=['close', 'volume']) as mock_select_features, \
         patch.object(ml_service, '_get_feature_file_path', return_value='/tmp/mock_features.json'), \
         patch('builtins.open', mock_open()), \
         patch.object(ml_service, '_build_lstm_model', return_value=mock_model) as mock_build, \
         patch.object(ml_service, '_run_training_worker', return_value=0.015) as mock_worker:

        # By default express=False, will trigger perform_walk_forward_validation
        result = ml_service.train_model("AAPL", model_type='lstm', pre_prepared_df=df)

        assert result['status'] == 'trained'
        assert result['symbol'] == 'AAPL'
        assert result['type'] == 'lstm'
        assert result['mse'] == 0.015
        assert result['val_mse'] >= 0  # Should be computed
        assert mock_build.called
        assert mock_worker.called
        assert mock_model.fit.called


def test_train_model_express_true(ml_service):
    np.random.seed(42)
    df = pd.DataFrame({
        'close': np.random.rand(300),
        'volume': np.random.rand(300),
        'log_return': np.random.rand(300),
        'target': np.random.rand(300)
    })

    with patch.object(ml_service, '_fetch_and_prepare_training_data') as mock_fetch, \
         patch.object(ml_service, 'select_top_features', return_value=['close', 'volume']) as mock_select_features, \
         patch.object(ml_service, '_get_feature_file_path', return_value='/tmp/mock_features.json'), \
         patch('builtins.open', mock_open()), \
         patch.object(ml_service, '_build_lstm_model') as mock_build, \
         patch.object(ml_service, '_run_training_worker', return_value=0.015) as mock_worker:

        result = ml_service.train_model("AAPL", model_type='lstm', express=True, pre_prepared_df=df)

        assert result['status'] == 'trained'
        assert result['symbol'] == 'AAPL'
        assert result['type'] == 'lstm'
        assert result['mse'] == 0.015
        assert result['val_mse'] == 0  # Walk-forward validation skipped
        assert not mock_build.called
        assert mock_worker.called


def test_train_model_rl_skips_validation(ml_service):
    np.random.seed(42)
    df = pd.DataFrame({
        'close': np.random.rand(300),
        'volume': np.random.rand(300),
        'log_return': np.random.rand(300),
        'target': np.random.rand(300)
    })

    with patch.object(ml_service, '_fetch_and_prepare_training_data') as mock_fetch, \
         patch.object(ml_service, 'select_top_features', return_value=['close', 'volume']) as mock_select_features, \
         patch.object(ml_service, '_get_feature_file_path', return_value='/tmp/mock_features.json'), \
         patch('builtins.open', mock_open()), \
         patch.object(ml_service, '_build_lstm_model') as mock_build, \
         patch.object(ml_service, 'perform_walk_forward_validation') as mock_wfv, \
         patch.object(ml_service, '_run_training_worker', return_value=0.02) as mock_worker:

        result = ml_service.train_model("AAPL", model_type='rl', express=False, pre_prepared_df=df)

        assert result['status'] == 'trained'
        assert result['symbol'] == 'AAPL'
        assert result['type'] == 'rl'
        assert result['mse'] == 0.02
        assert result['val_mse'] == 0  # Walk-forward validation skipped
        assert not mock_wfv.called
        assert mock_worker.called


def test_train_model_not_enough_data(ml_service):
    # Setup dataframe with <100 rows
    df = pd.DataFrame({'close': range(50)})

    with pytest.raises(ValidationError, match="Not enough data for training after processing"):
        ml_service.train_model("AAPL", pre_prepared_df=df)
