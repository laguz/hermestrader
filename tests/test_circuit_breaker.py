from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest
import time

from hermes.broker.circuit_breaker import CircuitBreaker, CircuitBreakerError
from hermes.service1_agent.core import AsyncBrokerWrapper, TradeAction
from tests._stubs import StubBroker, StubDB


@pytest.mark.anyio
async def test_circuit_breaker_state_transitions():
    db = AsyncMock()
    cb = CircuitBreaker(failure_threshold=3, cooldown_s=0.2)

    assert cb.state == "CLOSED"
    assert cb.failure_count == 0

    # 1st failure
    await cb.record_failure(db, "Error 1")
    assert cb.state == "CLOSED"
    assert cb.failure_count == 1

    # 2nd failure
    await cb.record_failure(db, "Error 2")
    assert cb.state == "CLOSED"
    assert cb.failure_count == 2

    # 3rd failure - should trip to OPEN
    await cb.record_failure(db, "Error 3")
    assert cb.state == "OPEN"
    assert cb.failure_count == 3
    db.set_setting.assert_awaited_once_with("agent_paused", "true")
    db.write_log.assert_awaited_once()

    # Check state before cooldown
    assert cb.check_state() == "OPEN"

    # Wait for cooldown
    await asyncio.sleep(0.25)
    assert cb.check_state() == "HALF-OPEN"

    # In HALF-OPEN, success resets to CLOSED
    cb.record_success()
    assert cb.state == "CLOSED"
    assert cb.failure_count == 0


@pytest.mark.anyio
async def test_circuit_breaker_half_open_failure():
    db = AsyncMock()
    cb = CircuitBreaker(failure_threshold=2, cooldown_s=0.1)

    # Trip the circuit
    await cb.record_failure(db, "Err 1")
    await cb.record_failure(db, "Err 2")
    assert cb.state == "OPEN"

    # Cooldown
    await asyncio.sleep(0.15)
    assert cb.check_state() == "HALF-OPEN"

    # Failure in HALF-OPEN should trip immediately
    db.reset_mock()
    await cb.record_failure(db, "Err 3")
    assert cb.state == "OPEN"
    db.set_setting.assert_awaited_once_with("agent_paused", "true")


@pytest.mark.anyio
async def test_async_broker_wrapper_circuit_breaker_integration():
    # Setup fresh CB for the class to avoid pollution
    AsyncBrokerWrapper._shared_cb = CircuitBreaker(failure_threshold=3, cooldown_s=0.1)
    cb = AsyncBrokerWrapper._shared_cb

    db = AsyncMock()
    broker = StubBroker()
    
    # Wrap broker
    wrapper = AsyncBrokerWrapper(broker, db)
    
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[],
        price=1.5,
        side="sell",
        quantity=1,
        order_type="credit"
    )

    # 1. Successful order placement
    res = await wrapper.place_order_from_action(action)
    assert res["status"] == "ok"
    assert cb.state == "CLOSED"

    # 2. Mock order rejection response format
    rejected_res = {"status": "error", "errors": {"error": "Too many orders"}}
    
    # Override stub broker to return rejection
    broker.place_order_from_action = MagicMock(return_value=rejected_res)
    
    # 1st failure (rejection)
    res = await wrapper.place_order_from_action(action)
    assert res == rejected_res
    assert cb.state == "CLOSED"

    # 2nd failure (exception)
    broker.place_order_from_action = MagicMock(side_effect=ValueError("API connection lost"))
    with pytest.raises(ValueError):
        await wrapper.place_order_from_action(action)
    assert cb.state == "CLOSED"

    # 3rd failure (rejection by order status)
    status_rejected_res = {"status": "ok", "order": {"status": "rejected", "id": 123}}
    broker.place_order_from_action = MagicMock(return_value=status_rejected_res)
    
    with pytest.raises(CircuitBreakerError):
        await wrapper.place_order_from_action(action)
    
    assert cb.state == "OPEN"
    db.set_setting.assert_awaited_once_with("agent_paused", "true")

    # Subsequent orders should fail fast
    with pytest.raises(CircuitBreakerError):
        await wrapper.place_order_from_action(action)
