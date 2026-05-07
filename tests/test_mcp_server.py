import sys
from unittest.mock import MagicMock
import pytest

# Manually mock mcp and other missing dependencies before anything else
def tool_decorator(*args, **kwargs):
    def decorator(f):
        return f
    return decorator

mcp_instance = MagicMock()
mcp_instance.tool = tool_decorator

class FastMCP:
    def __new__(cls, *args, **kwargs):
        return mcp_instance

# Setup sys.modules mocks
missing_deps = [
    "mcp", "mcp.server", "mcp.server.fastmcp",
    "requests", "numpy", "pandas", "tenacity",
    "hermes.ml.pop_engine"
]
for dep in missing_deps:
    sys.modules[dep] = MagicMock()

sys.modules["mcp.server.fastmcp"].FastMCP = FastMCP

# Now we can import the server
from hermes.mcp import server

def test_get_orders_happy_path(monkeypatch):
    mock_broker = MagicMock()
    expected_orders = [{"id": "order_1", "status": "filled"}]
    mock_broker.get_orders.return_value = expected_orders

    # Mock the _broker() function
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.get_orders()

    assert result == expected_orders
    mock_broker.get_orders.assert_called_once()

def test_get_orders_empty(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.get_orders.return_value = []

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.get_orders()

    assert result == []
    mock_broker.get_orders.assert_called_once()

def test_get_orders_exception(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.get_orders.side_effect = Exception("Broker error")

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    with pytest.raises(Exception, match="Broker error"):
        server.get_orders()

def test_place_multileg_order_credit(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.place_order_from_action.return_value = {"status": "ok"}
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    symbol = "SPY"
    legs = [{"option_symbol": "SPY241220C00500000", "quantity": 1, "action": "sell_to_open"}]
    price = 1.5

    result = server.place_multileg_order(
        symbol=symbol,
        legs=legs,
        price=price,
        order_type="credit",
        duration="day",
        tag="test-tag"
    )

    assert result == {"status": "ok"}
    mock_broker.place_order_from_action.assert_called_once()
    args, _ = mock_broker.place_order_from_action.call_args
    action = args[0]

    assert action.strategy_id == "mcp"
    assert action.symbol == symbol
    assert action.order_class == "multileg"
    assert action.legs == legs
    assert action.price == price
    assert action.side == "sell"
    assert action.order_type == "credit"
    assert action.duration == "day"
    assert action.tag == "test-tag"

def test_place_multileg_order_debit(monkeypatch):
    mock_broker = MagicMock()
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    server.place_multileg_order(
        symbol="AAPL",
        legs=[],
        price=2.0,
        order_type="debit"
    )

    mock_broker.place_order_from_action.assert_called_once()
    args, _ = mock_broker.place_order_from_action.call_args
    action = args[0]
    assert action.side == "buy"
    assert action.order_type == "debit"

def test_place_single_option_order_buy(monkeypatch):
    mock_broker = MagicMock()
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    server.place_single_option_order(
        symbol="TSLA",
        option_symbol="TSLA241220C00200000",
        side="buy_to_open",
        quantity=2,
        price=5.0
    )

    mock_broker.place_order_from_action.assert_called_once()
    args, _ = mock_broker.place_order_from_action.call_args
    action = args[0]
    assert action.order_class == "option"
    assert action.side == "buy"
    assert action.quantity == 2
    assert action.legs == [{"option_symbol": "TSLA241220C00200000", "action": "buy_to_open", "quantity": 2}]

def test_place_single_option_order_sell(monkeypatch):
    mock_broker = MagicMock()
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    server.place_single_option_order(
        symbol="TSLA",
        option_symbol="TSLA241220P00190000",
        side="sell_to_open",
        quantity=3
    )

    mock_broker.place_order_from_action.assert_called_once()
    args, _ = mock_broker.place_order_from_action.call_args
    action = args[0]
    assert action.side == "sell"
    assert action.quantity == 3
    assert action.legs == [{"option_symbol": "TSLA241220P00190000", "action": "sell_to_open", "quantity": 3}]

def test_place_equity_order_buy(monkeypatch):
    mock_broker = MagicMock()
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    server.place_equity_order(
        symbol="NVDA",
        side="buy",
        quantity=100,
        order_type="limit",
        price=120.0
    )

    mock_broker.place_order_from_action.assert_called_once()
    args, _ = mock_broker.place_order_from_action.call_args
    action = args[0]
    assert action.order_class == "equity"
    assert action.side == "buy"
    assert action.quantity == 100
    assert action.order_type == "limit"
    assert action.price == 120.0
    assert action.legs == [{"side": "buy", "quantity": 100}]

def test_place_equity_order_sell_short(monkeypatch):
    mock_broker = MagicMock()
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    server.place_equity_order(
        symbol="NVDA",
        side="sell_short",
        quantity=50
    )

    mock_broker.place_order_from_action.assert_called_once()
    args, _ = mock_broker.place_order_from_action.call_args
    action = args[0]
    assert action.side == "sell_short"
    assert action.quantity == 50
    assert action.legs == [{"side": "sell_short", "quantity": 50}]
