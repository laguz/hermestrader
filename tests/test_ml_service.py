import pytest
from unittest.mock import patch, MagicMock
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
    import numpy as np

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
