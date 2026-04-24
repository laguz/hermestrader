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

def test_run_batch_training_success(ml_service):
    """
    Test run_batch_training with all successes.
    Verifies that train_model is called for each symbol and model type,
    and successes are correctly counted.
    """
    symbols = ['AAPL', 'MSFT']

    with patch.object(ml_service, 'train_model') as mock_train:
        mock_train.return_value = {'mse': 0.01}

        results = ml_service.run_batch_training(symbols, express=True)

        # 2 symbols * 2 model types ('lstm', 'rl') = 4 successes
        assert results['success'] == 4
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
    Test run_batch_training with some errors.
    Verifies that exceptions are caught, errors are counted,
    and details list captures the error messages.
    """
    symbols = ['AAPL', 'MSFT']

    def mock_train_model(symbol, model_type='lstm', express=True):
        if symbol == 'MSFT' and model_type == 'rl':
            raise Exception("Training failed for MSFT RL")
        return {'mse': 0.05}

    with patch.object(ml_service, 'train_model', side_effect=mock_train_model):
        results = ml_service.run_batch_training(symbols, express=False)

        # 2 symbols * 2 model types = 4 attempts
        # MSFT RL fails, others succeed
        assert results['success'] == 3
        assert results['errors'] == 1
        assert len(results['details']) == 1

        assert any("MSFT/rl" in d for d in results['details'])
        assert any("Training failed for MSFT RL" in d for d in results['details'])
