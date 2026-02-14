"""Shared pytest fixtures and test configuration."""
import os
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_db():
    """Create a mock MongoDB database."""
    db = MagicMock()
    db.bot_config = MagicMock()
    db.market_data = MagicMock()
    db.predictions = MagicMock()
    db.open_positions = MagicMock()
    db.auto_trades = MagicMock()
    db.users = MagicMock()
    return db


@pytest.fixture
def mock_tradier():
    """Create a mock TradierService."""
    tradier = MagicMock()
    tradier.get_quote.return_value = {
        'symbol': 'SPY',
        'last': 450.0,
        'close': 449.5,
        'high': 451.0,
        'low': 448.0,
        'volume': 1000000
    }
    tradier.get_account_balances.return_value = {
        'total_equity': 100000.0,
        'option_buying_power': 50000.0,
        'stock_buying_power': 100000.0,
        'cash': 50000.0
    }
    tradier.get_positions.return_value = []
    tradier.get_orders.return_value = []
    tradier.get_option_expirations.return_value = []
    tradier.get_option_chains.return_value = []
    tradier.check_connection.return_value = True
    return tradier


@pytest.fixture
def app():
    """Create a Flask test application."""
    os.environ.setdefault('FLASK_SECRET_KEY', 'test-secret-key')
    os.environ.setdefault('MONGODB_URI', 'mongodb://localhost:27017/test_db')

    with patch('services.container.Container.get_mongo_client', return_value=MagicMock()), \
         patch('services.container.Container.get_db', return_value=MagicMock()):
        from app import create_app
        app = create_app()
        app.config['TESTING'] = True
        yield app


@pytest.fixture
def client(app):
    """Create a Flask test client."""
    return app.test_client()
