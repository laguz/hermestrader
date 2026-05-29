import asyncio
import pytest
from typing import Any, Dict, List, Optional

from hermes.service1_agent.core import TradeAction, CascadingEngine
from hermes.service1_agent.overseer import HermesOverseer
from hermes.events.bus import EventBus
from ._stubs import StubDB, StubBroker


class _FakeLLM:
    """Returns whatever ``reply`` is set to. Captures the last call args."""
    def __init__(self, reply="{\"verdict\": \"APPROVE\", \"rationale\": \"ok\"}"):
        self.reply = reply
        self.last_messages: Optional[List[Dict[str, Any]]] = None
        self.last_images: Optional[List[Any]] = None

    def chat(self, messages, images=None):
        self.last_messages = list(messages)
        self.last_images = list(images or [])
        return self.reply


def _action(symbol: str = "AAPL") -> TradeAction:
    return TradeAction(
        strategy_id="CS75", symbol=symbol, order_class="multileg",
        legs=[
            {"option_symbol": f"{symbol}250620P00090000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": f"{symbol}250620P00085000", "side": "buy_to_open",  "quantity": 1},
        ],
        price=1.50, side="sell", quantity=1, order_type="credit",
        tag="HERMES_CS75", strategy_params={"side_type": "put"},
        expiry="2025-06-20", width=5.0,
    )


@pytest.fixture(autouse=True)
def allow_offhours(monkeypatch):
    monkeypatch.setenv("HERMES_ALLOW_OFFHOURS_TRADES", "true")



@pytest.mark.asyncio
async def test_async_overseer_approval_flow():
    """Verify that an action goes through the EventBus, is approved by the AI Overseer,
    and then placed via the broker.
    """
    bus = EventBus()
    bus.start()

    db = StubDB()
    broker = StubBroker()
    
    # Enable advisory/enforcing through the engine
    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[],
        approval_mode=False,
        event_bus=bus
    )

    llm = _FakeLLM('{"verdict": "APPROVE", "rationale": "Strong support pattern"}')
    overseer = HermesOverseer(
        llm_client=llm,
        db=db,
        vision_enabled=False,
        autonomy="enforcing",
        event_bus=bus
    )
    await overseer.start()
    engine.overseer = overseer

    action = _action("AAPL")
    
    # Submitting should emit ReviewRequestEvent asynchronously
    await engine.submit([action])

    # Wait for the async processing to occur
    await asyncio.sleep(0.1)

    # Verify that the broker received the order since it was approved
    assert len(broker.placed) == 1
    assert broker.placed[0]["symbol"] == "AAPL"
    
    await overseer.stop()
    await bus.stop()


@pytest.mark.asyncio
async def test_async_overseer_veto_flow():
    """Verify that a VETO verdict stops the order placement and logs a veto."""
    bus = EventBus()
    bus.start()

    db = StubDB()
    broker = StubBroker()
    
    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[],
        approval_mode=False,
        event_bus=bus
    )

    llm = _FakeLLM('{"verdict": "VETO", "rationale": "High risk pattern detected"}')
    overseer = HermesOverseer(
        llm_client=llm,
        db=db,
        vision_enabled=False,
        autonomy="enforcing",
        event_bus=bus
    )
    await overseer.start()
    engine.overseer = overseer

    action = _action("MSFT")
    
    await engine.submit([action])
    await asyncio.sleep(0.1)

    # Broker should NOT have placed the order
    assert len(broker.placed) == 0
    
    # A veto log should be written in DB logs
    veto_logs = [log for log in db.logs if "[AI VETOED]" in log]
    assert len(veto_logs) == 1
    assert "High risk pattern detected" in veto_logs[0]

    await overseer.stop()
    await bus.stop()


@pytest.mark.asyncio
async def test_async_overseer_modify_flow():
    """Verify that a MODIFY verdict updates attributes and places the modified action."""
    bus = EventBus()
    bus.start()

    db = StubDB()
    broker = StubBroker()
    
    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[],
        approval_mode=False,
        event_bus=bus
    )

    llm = _FakeLLM('{"verdict": "MODIFY", "rationale": "Improve pricing fill probability", "modifications": {"price": 1.25}}')
    overseer = HermesOverseer(
        llm_client=llm,
        db=db,
        vision_enabled=False,
        autonomy="enforcing",
        event_bus=bus
    )
    await overseer.start()
    engine.overseer = overseer

    action = _action("GOOG")
    # Store initial reference to verify modifications
    initial_price = action.price
    assert initial_price == 1.50
    
    await engine.submit([action])
    await asyncio.sleep(0.1)

    # Broker should have placed the order with the modified price
    assert len(broker.placed) == 1
    assert action.price == 1.25
    assert action.ai_authored is True
    assert action.ai_rationale == "Improve pricing fill probability"

    await overseer.stop()
    await bus.stop()


@pytest.mark.asyncio
async def test_async_overseer_propose_flow():
    """Verify that a proposed trade from the overseer runs asynchronously without blocking."""
    bus = EventBus()
    bus.start()

    db = StubDB()
    broker = StubBroker()
    
    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[],
        approval_mode=False,
        event_bus=bus
    )

    payload = {
        "verdict": "OPEN",
        "action": {
            "strategy_id": "AI",
            "symbol": "AAPL",
            "order_class": "option",
            "legs": [{"option_symbol": "AAPL250620P00090000", "side": "sell_to_open", "quantity": 1}],
            "price": 0.5,
            "side": "sell",
        },
    }
    
    llm = _FakeLLM(reply=payload)
    overseer = HermesOverseer(
        llm_client=llm,
        db=db,
        vision_enabled=False,
        autonomy="autonomous",
        event_bus=bus
    )
    await overseer.start()
    engine.overseer = overseer

    # Trigger async propose in CascadingEngine
    await engine._async_propose(["AAPL"])
    await asyncio.sleep(0.1)

    # Verify that the AI-authored proposal was placed
    assert len(broker.placed) == 1
    assert broker.placed[0]["symbol"] == "AAPL"
    
    await overseer.stop()
    await bus.stop()
