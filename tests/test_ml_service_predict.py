import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from services.ml_service import MLService
from exceptions import ExternalServiceError, ResourceNotFoundError, ValidationError

class MockTradier:
    pass

@pytest.fixture
def ml_service():
    with patch('services.container.Container.get_db') as mock_db:
        # Initializing the db mock to something other than None so __init__ doesn't set self.db=None
        mock_db.return_value = MagicMock()
        service = MLService(MockTradier())
        service.sequence_length = 5
        return service

def test_predict_next_day_invalid_symbol(ml_service):
    with pytest.raises(ValidationError, match="Invalid symbol"):
        ml_service.predict_next_day("INVALID!@#")

def test_predict_next_day_db_unavailable(ml_service):
    ml_service.db = None
    with pytest.raises(ExternalServiceError, match="Database not available"):
        ml_service.predict_next_day("AAPL")

def test_predict_next_day_no_data(ml_service):
    # Mocking db calls to return empty data and backfill to fail
    mock_collection = MagicMock()
    mock_collection.find.return_value.sort.return_value = []
    ml_service.db.__getitem__.return_value = mock_collection

    with patch.object(ml_service, 'backfill_symbol', return_value=False):
        with pytest.raises(ExternalServiceError, match="No recent data found in DB and backfill failed"):
            ml_service.predict_next_day("AAPL")

def test_predict_next_day_lstm_happy_path(ml_service):
    # Setup mock DB data
    mock_collection = MagicMock()
    # Need enough data for lags, features (e.g. sma_50 -> need 50 rows, sequence length 5 -> 55 rows)
    # We will mock prepare_features to just return a dummy dataframe to bypass indicator complexity
    ml_service.db.__getitem__.return_value = mock_collection

    # Let's bypass the actual DB cursor and return a dummy list with 1 item so it passes `if not data`
    mock_collection.find.return_value.sort.return_value = [{"date": "2023-01-01", "close": 150.0}]

    dummy_df = pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
        'close': [150.0] * 10,
        'feat1': [1.0] * 10
    })

    with patch.object(ml_service, 'prepare_features', return_value=dummy_df), \
         patch('os.path.exists', return_value=True), \
         patch('builtins.open', new_callable=MagicMock) as mock_open, \
         patch('json.load', return_value=['feat1']), \
         patch('services.ml_service.load_model') as mock_load_model, \
         patch('services.ml_service.joblib.load') as mock_joblib_load, \
         patch('services.ml_service.CustomBusinessDay') as mock_cbd:

        # Mock load_model
        mock_model = MagicMock()
        # Predicting log return = 0.0 => price doesn't change
        mock_model.predict.return_value = np.array([[0.0]])
        mock_load_model.return_value = mock_model

        # Mock scaler (scaler transform)
        mock_scaler = MagicMock()
        mock_scaler.transform.return_value = np.array([[1.0]] * ml_service.sequence_length)

        # Mock target scaler
        mock_target_scaler = MagicMock()
        mock_target_scaler.inverse_transform.return_value = np.array([[0.0]]) # 0 log return

        # joblib.load is called for scaler and target_scaler
        def joblib_side_effect(path):
            if 'target_scaler' in path:
                return mock_target_scaler
            return mock_scaler

        mock_joblib_load.side_effect = joblib_side_effect

        # Mock CustomBusinessDay to just add 1 day
        mock_cbd.return_value = pd.Timedelta(days=1)

        result = ml_service.predict_next_day("AAPL", model_type="lstm")

        assert result['symbol'] == "AAPL"
        assert result['model'] == "lstm"
        assert result['predicted_price'] == 150.0 # 150 * exp(0)
        assert result['last_close'] == 150.0
        assert result['change'] == 0.0
        assert result['percent_change_str'] == "0.00%"
        assert 'prediction_date' in result
        assert result['used_features'] == ['feat1']

        # Ensure DB update was called for persistence
        ml_service.db['predictions'].update_one.assert_called_once()


def test_predict_next_day_rl_happy_path(ml_service):
    # Setup mock DB data
    mock_collection = MagicMock()
    ml_service.db.__getitem__.return_value = mock_collection
    mock_collection.find.return_value.sort.return_value = [{"date": "2023-01-01", "close": 150.0}]

    dummy_df = pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
        'close': [150.0] * 10,
        'feat1': [1.0] * 10
    })

    with patch.object(ml_service, 'prepare_features', return_value=dummy_df), \
         patch('os.path.exists', return_value=True), \
         patch('builtins.open', new_callable=MagicMock) as mock_open, \
         patch('json.load') as mock_json_load, \
         patch('services.ml_service.joblib.dump') as mock_joblib_dump, \
         patch('subprocess.run') as mock_subprocess_run, \
         patch('os.remove') as mock_os_remove, \
         patch('services.ml_service.CustomBusinessDay') as mock_cbd:

        # json.load is called twice: once for features (list), once for RL output (dict)
        def json_load_side_effect(fh):
            if hasattr(fh, 'name') and 'result' in getattr(fh, 'name', ''):
                return {"prediction": 155.0}
            elif isinstance(fh, MagicMock): # from mocked open
                # By looking at the code, it uses open for feature file, and later for result file
                # The first mock_json_load call is for feature file, returning list
                if mock_json_load.call_count == 1:
                    return ['feat1']
                else:
                    return {"prediction": 155.0}
            return ['feat1']

        mock_json_load.side_effect = json_load_side_effect

        mock_subprocess_result = MagicMock()
        mock_subprocess_result.returncode = 0
        mock_subprocess_run.return_value = mock_subprocess_result

        mock_cbd.return_value = pd.Timedelta(days=1)

        result = ml_service.predict_next_day("AAPL", model_type="rl")

        assert result['symbol'] == "AAPL"
        assert result['model'] == "rl"
        assert result['predicted_price'] == 155.0
        assert result['last_close'] == 150.0
        assert result['change'] == 5.0
        assert result['percent_change_str'] == "3.33%"
        assert 'prediction_date' in result
        assert result['used_features'] == ['feat1']

def test_predict_next_day_rl_model_not_found(ml_service):
    # Setup mock DB data
    mock_collection = MagicMock()
    ml_service.db.__getitem__.return_value = mock_collection
    mock_collection.find.return_value.sort.return_value = [{"date": "2023-01-01", "close": 150.0}]

    dummy_df = pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
        'close': [150.0] * 10,
        'feat1': [1.0] * 10
    })

    with patch.object(ml_service, 'prepare_features', return_value=dummy_df), \
         patch('os.path.exists', return_value=True), \
         patch('builtins.open', new_callable=MagicMock) as mock_open, \
         patch('json.load', return_value=['feat1']), \
         patch('services.ml_service.joblib.dump') as mock_joblib_dump, \
         patch('subprocess.run') as mock_subprocess_run, \
         patch('os.remove') as mock_os_remove:

        mock_subprocess_result = MagicMock()
        mock_subprocess_result.returncode = 1
        mock_subprocess_result.stderr = "Model file not found in path"
        mock_subprocess_run.return_value = mock_subprocess_result

        with pytest.raises(ResourceNotFoundError, match="RL model for AAPL not found."):
            ml_service.predict_next_day("AAPL", model_type="rl")

def test_predict_next_day_rl_subprocess_error(ml_service):
    # Setup mock DB data
    mock_collection = MagicMock()
    ml_service.db.__getitem__.return_value = mock_collection
    mock_collection.find.return_value.sort.return_value = [{"date": "2023-01-01", "close": 150.0}]

    dummy_df = pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
        'close': [150.0] * 10,
        'feat1': [1.0] * 10
    })

    with patch.object(ml_service, 'prepare_features', return_value=dummy_df), \
         patch('os.path.exists', return_value=True), \
         patch('builtins.open', new_callable=MagicMock) as mock_open, \
         patch('json.load', return_value=['feat1']), \
         patch('services.ml_service.joblib.dump') as mock_joblib_dump, \
         patch('subprocess.run') as mock_subprocess_run, \
         patch('os.remove') as mock_os_remove:

        mock_subprocess_result = MagicMock()
        mock_subprocess_result.returncode = 1
        mock_subprocess_result.stderr = "Some generic exception occurred"
        mock_subprocess_run.return_value = mock_subprocess_result

        # AppError is imported from exceptions but maybe we should use match
        from exceptions import AppError
        with pytest.raises(AppError, match="RL Prediction failed in subprocess"):
            ml_service.predict_next_day("AAPL", model_type="rl")

def test_predict_next_day_unknown_model_type(ml_service):
    # Setup mock DB data
    mock_collection = MagicMock()
    ml_service.db.__getitem__.return_value = mock_collection
    mock_collection.find.return_value.sort.return_value = [{"date": "2023-01-01", "close": 150.0}]

    dummy_df = pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
        'close': [150.0] * 10,
        'feat1': [1.0] * 10
    })

    with patch.object(ml_service, 'prepare_features', return_value=dummy_df), \
         patch('os.path.exists', return_value=True), \
         patch('builtins.open', new_callable=MagicMock) as mock_open, \
         patch('json.load', return_value=['feat1']):

        with pytest.raises(ValueError, match="Unknown model_type: unknown"):
            ml_service.predict_next_day("AAPL", model_type="unknown")
