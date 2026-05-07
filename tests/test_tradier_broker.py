
import sys
from unittest.mock import MagicMock, patch

# Save original modules
original_modules = {
    "numpy": sys.modules.get("numpy"),
    "pandas": sys.modules.get("pandas"),
    "requests": sys.modules.get("requests"),
    "tenacity": sys.modules.get("tenacity"),
    "hermes.ml.pop_engine": sys.modules.get("hermes.ml.pop_engine"),
}

# Mock all dependencies before importing TradierBroker
sys.modules["numpy"] = MagicMock()
sys.modules["pandas"] = MagicMock()
sys.modules["requests"] = MagicMock()
sys.modules["tenacity"] = MagicMock()
sys.modules["hermes.ml.pop_engine"] = MagicMock()

from hermes.broker.tradier import TradierBroker

def test_get_delta_broker_success():
    """Test TradierBroker.get_delta correctly extracts delta from quotes."""
    config = {
        "tradier_access_token": "mock_token",
        "tradier_account_id": "mock_account"
    }
    broker = TradierBroker(config)

    # Mock get_quote response
    mock_quotes = [
        {
            "symbol": "AAPL240621C00150000",
            "greeks": {
                "delta": 0.5234
            }
        }
    ]

    with patch.object(broker, "get_quote", return_value=mock_quotes):
        delta = broker.get_delta("AAPL240621C00150000")
        assert delta == 0.5234

def test_get_delta_broker_no_quotes():
    """Test TradierBroker.get_delta returns 0.0 when no quotes are found."""
    broker = TradierBroker({"tradier_access_token": "t", "tradier_account_id": "a"})

    with patch.object(broker, "get_quote", return_value=[]):
        delta = broker.get_delta("INVALID")
        assert delta == 0.0

def test_get_delta_broker_no_greeks():
    """Test TradierBroker.get_delta returns 0.0 when greeks are missing."""
    broker = TradierBroker({"tradier_access_token": "t", "tradier_account_id": "a"})
    mock_quotes = [{"symbol": "SYM"}] # No 'greeks' key

    with patch.object(broker, "get_quote", return_value=mock_quotes):
        delta = broker.get_delta("SYM")
        assert delta == 0.0

def test_get_delta_broker_none_delta():
    """Test TradierBroker.get_delta returns 0.0 when delta is None."""
    broker = TradierBroker({"tradier_access_token": "t", "tradier_account_id": "a"})
    mock_quotes = [{"symbol": "SYM", "greeks": {"delta": None}}]

    with patch.object(broker, "get_quote", return_value=mock_quotes):
        delta = broker.get_delta("SYM")
        assert delta == 0.0

# Cleanup at the end of module (not ideal for pytest, but better than nothing here)
for mod, val in original_modules.items():
    if val is None:
        sys.modules.pop(mod, None)
    else:
        sys.modules[mod] = val
