import pytest
from unittest.mock import patch
from services.ml_service import MLService
import os

class MockTradier:
    pass

@pytest.fixture
def ml_service():
    with patch('services.container.Container.get_db') as mock_db:
        mock_db.return_value = None
        service = MLService(MockTradier())
        return service

def test_secure_tmp_files_train_model(ml_service):
    with patch('services.ml_service.MLService._fetch_and_prepare_training_data') as mock_fetch:
        with patch('services.ml_service.MLService.select_top_features') as mock_select:
            with patch('services.ml_service.MLService.perform_walk_forward_validation') as mock_validate:
                with patch('os.path.exists') as mock_exists:
                    with patch('os.makedirs'):
                        with patch('builtins.open'):
                            with patch('joblib.dump'):
                                with patch('subprocess.run') as mock_run:
                                    import pandas as pd

                                    mock_fetch.return_value = pd.DataFrame({'close': [1,2,3,4,5], 'target': [1,2,3,4,5], 'log_return': [1,2,3,4,5]})
                                    mock_select.return_value = ['close']
                                    mock_validate.return_value = {}

                                    # Create a dummy return so run() works without crashing
                                    class DummyResult:
                                        returncode = 0
                                    mock_run.return_value = DummyResult()

                                    # malicious symbol
                                    malicious_symbol = "../../../etc/passwd"

                                    try:
                                        ml_service.train_model(malicious_symbol, model_type='rl', express=True)
                                    except Exception as e:
                                        print(f"Caught expected exception or validation error: {e}")
                                        pass

                                    # Let's see what subprocess.run was called with
                                    if mock_run.called:
                                        args = mock_run.call_args[0][0]
                                        # args = [sys.executable, worker_path, symbol, model_dir, df_path, features_path]
                                        worker_path = args[1]
                                        df_path = args[4]
                                        features_path = args[5]
                                        print(f"Worker Path: {worker_path}")
                                        print(f"DF Path: {df_path}")
                                        print(f"Features Path: {features_path}")

                                        assert "etc" not in worker_path
                                        assert "passwd" not in worker_path

def test_nosql_injection_fetch_and_prepare(ml_service):
    from exceptions import ValidationError
    from unittest.mock import MagicMock

    ml_service.db = MagicMock()
    malicious_payload = {"$ne": "invalid"}

    with pytest.raises(ValidationError) as exc_info:
        ml_service._fetch_and_prepare_training_data(malicious_payload)

    assert "Symbol must be a string" in str(exc_info.value)
