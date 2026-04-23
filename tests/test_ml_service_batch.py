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
