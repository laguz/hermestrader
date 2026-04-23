import pytest
from unittest.mock import patch
from services.ml_service import MLService
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dropout, Dense

class MockTradier:
    pass

@pytest.fixture
def ml_service():
    with patch('services.container.Container.get_db') as mock_db:
        mock_db.return_value = None
        service = MLService(MockTradier())
        return service

def test_build_lstm_model_happy_path(ml_service):
    """
    Verify that _build_lstm_model constructs the correct Keras LSTM architecture
    with the expected layers, shapes, and configurations.
    """
    input_shape = (10, 5)
    model = ml_service._build_lstm_model(input_shape)

    # Assert model is a valid Sequential model
    assert isinstance(model, Sequential)

    # Assert total number of layers is 6
    assert len(model.layers) == 6

    # 1. First LSTM Layer
    assert isinstance(model.layers[0], LSTM)
    assert model.layers[0].units == 64
    assert model.layers[0].return_sequences is True

    # 2. First Dropout Layer
    assert isinstance(model.layers[1], Dropout)
    assert model.layers[1].rate == 0.2

    # 3. Second LSTM Layer
    assert isinstance(model.layers[2], LSTM)
    assert model.layers[2].units == 64
    assert model.layers[2].return_sequences is False

    # 4. Second Dropout Layer
    assert isinstance(model.layers[3], Dropout)
    assert model.layers[3].rate == 0.2

    # 5. Dense Relu Layer
    assert isinstance(model.layers[4], Dense)
    assert model.layers[4].units == 32
    assert model.layers[4].activation.__name__ == 'relu'

    # 6. Dense Output Layer
    assert isinstance(model.layers[5], Dense)
    assert model.layers[5].units == 1

    # Assert compilation properties
    assert model.optimizer.name.lower() in ['adam', 'adamw']
    assert model.loss in ['mean_squared_error', 'mse']
