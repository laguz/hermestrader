import pytest
from unittest.mock import patch, MagicMock
import sys

class MockTradier:
    pass

@pytest.fixture
def mock_sys_modules():
    mock_modules = {
        'numpy': MagicMock(),
        'pandas': MagicMock(),
        'pandas.tseries': MagicMock(),
        'pandas.tseries.holiday': MagicMock(),
        'pandas.tseries.offsets': MagicMock(),
        'sklearn': MagicMock(),
        'sklearn.preprocessing': MagicMock(),
        'sklearn.ensemble': MagicMock(),
        'sklearn.metrics': MagicMock(),
        'sklearn.model_selection': MagicMock(),
        'sklearn.svm': MagicMock(),
        'sklearn.cluster': MagicMock(),
        'scipy': MagicMock(),
        'scipy.stats': MagicMock(),
        'scipy.signal': MagicMock(),
        'gymnasium': MagicMock(),
        'joblib': MagicMock(),
        'pymongo': MagicMock(),
        'pymongo.errors': MagicMock(),
        'tensorflow': MagicMock(),
        'keras': MagicMock(),
        'keras.models': MagicMock(),
        'keras.layers': MagicMock(),
        'keras.callbacks': MagicMock(),
        'stable_baselines3': MagicMock(),
        'certifi': MagicMock(),
        'plotly': MagicMock(),
        'plotly.graph_objects': MagicMock(),
        'plotly.subplots': MagicMock(),
        'requests': MagicMock(),
        'Flask': MagicMock(),
        'werkzeug': MagicMock(),
        'werkzeug.security': MagicMock(),
        'bs4': MagicMock(),
    }

    with patch.dict('sys.modules', mock_modules):
        from services.ml_service import MLService
        yield MLService

@pytest.fixture
def ml_service(mock_sys_modules):
    MLService = mock_sys_modules
    with patch('services.container.Container.get_db') as mock_db:
        mock_db.return_value = None
        service = MLService(MockTradier())
        return service

def test_run_batch_predictions_error(ml_service):
    """
    Test run_batch_predictions with one success and one error.
    Verifies that the error is caught and included in the batch results.
    """
    symbols = ['AAPL', 'MSFT']

    def mock_predict_next_day(symbol, model_type='lstm'):
        if symbol == 'AAPL':
            return {'predicted_price': 150.0}
        elif symbol == 'MSFT':
            raise Exception("Prediction failed for MSFT")
        else:
            return {'predicted_price': 100.0}

    with patch.object(ml_service, 'predict_next_day', side_effect=mock_predict_next_day):
        with patch.object(ml_service, 'refresh_prediction_actuals') as mock_refresh:
            results = ml_service.run_batch_predictions(symbols)

            # The method processes each symbol against each model_type.
            # model_types = ['lstm', 'rl'] (2 types)
            # AAPL succeeds for both lstm and rl -> 2 successes
            # MSFT fails for both lstm and rl -> 2 errors
            assert results['success'] == 2
            assert results['errors'] == 2
            assert results['skipped'] == 0
            assert len(results['details']) == 2

            # Check if details list captured the errors
            assert any("MSFT" in d for d in results['details'])
            assert any("Prediction failed for MSFT" in d for d in results['details'])

def test_run_batch_predictions_empty_symbols(ml_service):
    """
    Test run_batch_predictions with an empty list of symbols.
    Verifies that it handles empty input gracefully and returns zero counts.
    """
    with patch.object(ml_service, 'refresh_prediction_actuals'):
        results = ml_service.run_batch_predictions([])

        assert results['success'] == 0
        assert results['errors'] == 0
        assert results['skipped'] == 0
        assert len(results['details']) == 0

def test_run_batch_predictions_resource_not_found(ml_service):
    """
    Test run_batch_predictions catching ResourceNotFoundError and incrementing skipped_count.
    """
    from exceptions import ResourceNotFoundError
    symbols = ['AAPL']

    def mock_predict_next_day(symbol, model_type='lstm'):
        raise ResourceNotFoundError("Model not found")

    with patch.object(ml_service, 'predict_next_day', side_effect=mock_predict_next_day):
        with patch.object(ml_service, 'refresh_prediction_actuals'):
            results = ml_service.run_batch_predictions(symbols)

            assert results['success'] == 0
            assert results['errors'] == 0
            # 1 symbol * 2 model types ('lstm', 'rl')
            assert results['skipped'] == 2
            assert len(results['details']) == 0

def test_run_batch_predictions_refresh_error(ml_service):
    """
    Test run_batch_predictions when refresh_prediction_actuals raises an error.
    It should catch the error and continue processing predictions.
    """
    symbols = ['AAPL']

    with patch.object(ml_service, 'refresh_prediction_actuals', side_effect=Exception("Refresh failed")):
        with patch.object(ml_service, 'predict_next_day', return_value={'predicted_price': 150.0}):
            results = ml_service.run_batch_predictions(symbols)

            # 1 symbol * 2 model types -> 2 successes despite refresh error
            assert results['success'] == 2
            assert results['errors'] == 0
            assert results['skipped'] == 0
            assert len(results['details']) == 0

def test_run_batch_training_success(ml_service):
    """
    Test run_batch_training with successful training for all symbols.
    """
    symbols = ['AAPL', 'MSFT']

    with patch.object(ml_service, 'train_model', return_value={'mse': 0.1}) as mock_train:
        results = ml_service.run_batch_training(symbols)

        assert results['success'] == 4  # 2 symbols * 2 model types ('lstm', 'rl')
        assert results['errors'] == 0
        assert len(results['details']) == 0

        # Verify train_model was called with correct arguments
        assert mock_train.call_count == 4
        mock_train.assert_any_call('AAPL', model_type='lstm', express=True)
        mock_train.assert_any_call('AAPL', model_type='rl', express=True)
        mock_train.assert_any_call('MSFT', model_type='lstm', express=True)
        mock_train.assert_any_call('MSFT', model_type='rl', express=True)

def test_run_batch_training_error(ml_service):
    """
    Test run_batch_training when train_model raises an exception.
    """
    symbols = ['AAPL', 'MSFT']

    def mock_train_model(symbol, model_type, express):
        if symbol == 'MSFT':
            raise Exception("Training failed for MSFT")
        return {'mse': 0.1}

    with patch.object(ml_service, 'train_model', side_effect=mock_train_model) as mock_train:
        results = ml_service.run_batch_training(symbols)

        assert results['success'] == 2  # AAPL succeeds for both
        assert results['errors'] == 2   # MSFT fails for both
        assert len(results['details']) == 2

        assert any("MSFT" in d for d in results['details'])
        assert any("Training failed for MSFT" in d for d in results['details'])

def test_run_batch_training_empty_symbols(ml_service):
    """
    Test run_batch_training with empty list of symbols.
    """
    with patch.object(ml_service, 'train_model') as mock_train:
        results = ml_service.run_batch_training([])

        assert results['success'] == 0
        assert results['errors'] == 0
        assert len(results['details']) == 0
        mock_train.assert_not_called()

def test_run_batch_training_express_false(ml_service):
    """
    Test run_batch_training with express=False.
    """
    symbols = ['AAPL']

    with patch.object(ml_service, 'train_model', return_value={'mse': 0.1}) as mock_train:
        results = ml_service.run_batch_training(symbols, express=False)

        assert results['success'] == 2  # 1 symbol * 2 model types
        assert results['errors'] == 0

        # Verify train_model was called with express=False
        assert mock_train.call_count == 2
        mock_train.assert_any_call('AAPL', model_type='lstm', express=False)
        mock_train.assert_any_call('AAPL', model_type='rl', express=False)
