from unittest.mock import AsyncMock, patch

from hermes.broker.tradier import TradierBroker


_CFG = {"tradier_access_token": "t", "tradier_account_id": "a"}


async def test_get_option_chains_parses_raw_tradier_legs():
    """Regression: the raw Tradier option dict carries symbol/strike/option_type/
    bid/ask/greeks, the same names the leg is built with explicitly. An
    unfiltered ``**o`` spread raised 'multiple values for keyword argument' and
    silently emptied every chain — so no strategy could ever build an entry."""
    broker = TradierBroker(_CFG)
    raw = {
        "options": {
            "option": [
                {
                    "symbol": "TSLA260731P00400000",
                    "strike": 400.0,
                    "option_type": "put",
                    "bid": 5.0,
                    "ask": 5.4,
                    "open_interest": 1234,
                    "greeks": {"delta": -0.31, "mid_iv": 0.55},
                }
            ]
        }
    }
    with patch.object(broker, "_get", new_callable=AsyncMock, return_value=raw):
        chain = await broker.get_option_chains("TSLA", "2026-07-31")
    assert len(chain) == 1
    leg = chain[0]
    assert leg["symbol"] == "TSLA260731P00400000"
    assert leg["strike"] == 400.0
    assert leg["delta"] == -0.31
    # Extra raw fields still round-trip through the spread.
    assert leg["open_interest"] == 1234
    await broker.close()


async def test_get_quote_parses_raw_tradier_quote():
    """Regression: same collision class as chains (symbol/bid/ask/volume)."""
    broker = TradierBroker(_CFG)
    raw = {
        "quotes": {
            "quote": {
                "symbol": "TSLA",
                "last": 408.98,
                "bid": 408.5,
                "ask": 409.5,
                "volume": 5000,
                "exch": "Q",
            }
        }
    }
    with patch.object(broker, "_get", new_callable=AsyncMock, return_value=raw):
        quotes = await broker.get_quote("TSLA")
    assert len(quotes) == 1
    assert quotes[0]["symbol"] == "TSLA"
    assert quotes[0]["price"] == 408.98
    assert quotes[0]["exch"] == "Q"
    await broker.close()


async def test_get_orders_parses_raw_tradier_order():
    """Regression: same collision class (symbol/status/quantity/price/side/tag)."""
    broker = TradierBroker(_CFG)
    raw = {
        "orders": {
            "order": {
                "id": 99,
                "symbol": "TSLA",
                "status": "open",
                "quantity": 2,
                "price": 1.25,
                "side": "sell_to_open",
                "tag": "HERMES-CS75",
                "class": "multileg",
                # Raw Tradier orders also carry 'id' and 'leg', whose names
                # collide with BrokerOrder's internally re-emitted aliases.
                "leg": [{"option_symbol": "TSLA260731P00400000"}],
            }
        }
    }
    with patch.object(broker, "_get", new_callable=AsyncMock, return_value=raw):
        orders = await broker.get_orders()
    assert len(orders) == 1
    assert orders[0]["order_id"] == "99"
    assert orders[0]["status"] == "open"
    assert orders[0]["tag"] == "HERMES-CS75"
    await broker.close()

async def test_get_delta_broker_success():
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

    with patch.object(broker, "get_quote", new_callable=AsyncMock) as mock_get_quote:
        mock_get_quote.return_value = mock_quotes
        delta = await broker.get_delta("AAPL240621C00150000")
        assert delta == 0.5234
    await broker.close()

async def test_get_delta_broker_no_quotes():
    """Test TradierBroker.get_delta returns 0.0 when no quotes are found."""
    broker = TradierBroker({"tradier_access_token": "t", "tradier_account_id": "a"})

    with patch.object(broker, "get_quote", new_callable=AsyncMock) as mock_get_quote:
        mock_get_quote.return_value = []
        delta = await broker.get_delta("INVALID")
        assert delta == 0.0
    await broker.close()

async def test_get_delta_broker_no_greeks():
    """Test TradierBroker.get_delta returns 0.0 when greeks are missing."""
    broker = TradierBroker({"tradier_access_token": "t", "tradier_account_id": "a"})
    mock_quotes = [{"symbol": "SYM"}] # No 'greeks' key

    with patch.object(broker, "get_quote", new_callable=AsyncMock) as mock_get_quote:
        mock_get_quote.return_value = mock_quotes
        delta = await broker.get_delta("SYM")
        assert delta == 0.0
    await broker.close()

async def test_get_delta_broker_none_delta():
    """Test TradierBroker.get_delta returns 0.0 when delta is None."""
    broker = TradierBroker({"tradier_access_token": "t", "tradier_account_id": "a"})
    mock_quotes = [{"symbol": "SYM", "greeks": {"delta": None}}]

    with patch.object(broker, "get_quote", new_callable=AsyncMock) as mock_get_quote:
        mock_get_quote.return_value = mock_quotes
        delta = await broker.get_delta("SYM")
        assert delta == 0.0
    await broker.close()
