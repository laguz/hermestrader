from __future__ import annotations

from unittest.mock import AsyncMock
import pytest
from datetime import datetime, timedelta

from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper


@pytest.fixture(autouse=True)
def clear_shared_cache():
    # Reset the shared cache singleton before/after each test
    AsyncBrokerWrapper.clear_cache()
    yield
    AsyncBrokerWrapper.clear_cache()


class DummyBroker:
    def __init__(self):
        self.current_date = None
        self.get_option_chains = AsyncMock()
        self.get_option_expirations = AsyncMock()
        self.analyze_symbol = AsyncMock()
        self.get_quote = AsyncMock()


@pytest.mark.anyio
async def test_option_chains_caching():
    dummy = DummyBroker()
    wrapper = AsyncBrokerWrapper(dummy)
    
    dummy.get_option_chains.return_value = [{"symbol": "AAPL_1", "strike": 150}]
    
    # First call - cache miss
    res1 = await wrapper.get_option_chains("AAPL", "2026-06-19")
    assert res1 == [{"symbol": "AAPL_1", "strike": 150}]
    dummy.get_option_chains.assert_called_once_with("AAPL", "2026-06-19")
    
    # Second call - cache hit
    dummy.get_option_chains.reset_mock()
    res2 = await wrapper.get_option_chains("AAPL", "2026-06-19")
    assert res2 == [{"symbol": "AAPL_1", "strike": 150}]
    dummy.get_option_chains.assert_not_called()


@pytest.mark.anyio
async def test_cache_ttl_and_simulated_time():
    dummy = DummyBroker()
    # Let's set a simulated date
    dummy.current_date = datetime(2026, 6, 7, 10, 0, 0)
    wrapper = AsyncBrokerWrapper(dummy)
    
    dummy.get_option_expirations.return_value = ["2026-06-19", "2026-06-26"]
    
    # Cache miss
    res1 = await wrapper.get_option_expirations("AAPL")
    assert res1 == ["2026-06-19", "2026-06-26"]
    dummy.get_option_expirations.assert_called_once_with("AAPL")
    
    # Cache hit
    dummy.get_option_expirations.reset_mock()
    res2 = await wrapper.get_option_expirations("AAPL")
    dummy.get_option_expirations.assert_not_called()
    
    # Advance simulated time past TTL (default is 120s, let's advance by 130s)
    dummy.current_date = dummy.current_date + timedelta(seconds=130)
    
    # Cache miss again due to expiry
    res3 = await wrapper.get_option_expirations("AAPL")
    assert res3 == ["2026-06-19", "2026-06-26"]
    dummy.get_option_expirations.assert_called_once_with("AAPL")


@pytest.mark.anyio
async def test_get_quote_batch_caching():
    dummy = DummyBroker()
    wrapper = AsyncBrokerWrapper(dummy)
    
    # Mock return list of dicts with symbols
    dummy.get_quote.side_effect = lambda syms: [
        {"symbol": s.strip(), "last": 100.0} for s in syms.split(",") if s.strip()
    ]
    
    # 1. Fetch batch
    res1 = await wrapper.get_quote("AAPL,MSFT")
    assert len(res1) == 2
    assert res1[0]["symbol"] == "AAPL"
    assert res1[1]["symbol"] == "MSFT"
    dummy.get_quote.assert_called_once_with("AAPL,MSFT")
    
    # 2. Fetch single AAPL - should hit cache
    dummy.get_quote.reset_mock()
    res2 = await wrapper.get_quote("AAPL")
    assert len(res2) == 1
    assert res2[0]["symbol"] == "AAPL"
    dummy.get_quote.assert_not_called()
    
    # 3. Fetch AAPL,TSLA - should batch only TSLA
    dummy.get_quote.reset_mock()
    res3 = await wrapper.get_quote("AAPL,TSLA")
    assert len(res3) == 2
    assert res3[0]["symbol"] == "AAPL"
    assert res3[1]["symbol"] == "TSLA"
    dummy.get_quote.assert_called_once_with("TSLA")


@pytest.mark.anyio
async def test_clear_cache():
    dummy = DummyBroker()
    wrapper = AsyncBrokerWrapper(dummy)
    
    dummy.analyze_symbol.return_value = {"symbol": "AAPL", "avg_vol": 0.25}
    
    await wrapper.analyze_symbol("AAPL", "6m")
    dummy.analyze_symbol.assert_called_once_with("AAPL", "6m")
    
    # Clear cache
    AsyncBrokerWrapper.clear_cache()
    
    # Should call broker again
    dummy.analyze_symbol.reset_mock()
    await wrapper.analyze_symbol("AAPL", "6m")
    dummy.analyze_symbol.assert_called_once_with("AAPL", "6m")
