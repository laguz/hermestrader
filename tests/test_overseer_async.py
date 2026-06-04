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


async def test_veto_suppresses_repeat_entry():
    """After a VETO, re-submitting the identical entry is suppressed without
    a second review round-trip (no brute-forcing the same action each tick).
    """
    bus = EventBus()
    bus.start()

    db = StubDB()
    broker = StubBroker()

    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[],
        approval_mode=False,
        event_bus=bus,
        config={"veto_suppression_s": 1800},
    )

    llm = _FakeLLM('{"verdict": "VETO", "rationale": "Repeated brute-force entry for RIOT"}')
    overseer = HermesOverseer(
        llm_client=llm, db=db, vision_enabled=False,
        autonomy="enforcing", event_bus=bus,
    )
    await overseer.start()
    engine.overseer = overseer

    # First submission: reviewed and vetoed → records a suppression.
    await engine.submit([_action("RIOT")])
    await asyncio.sleep(0.1)
    assert len(broker.placed) == 0
    assert len([l for l in db.logs if "[AI VETOED]" in l]) == 1

    # Second identical submission: suppressed before review — no new veto log,
    # and a [VETO-SUPPRESSED] entry instead.
    llm.last_messages = None
    await engine.submit([_action("RIOT")])
    await asyncio.sleep(0.1)
    assert len(broker.placed) == 0
    assert len([l for l in db.logs if "[AI VETOED]" in l]) == 1   # still just one
    assert len([l for l in db.logs if "[VETO-SUPPRESSED]" in l]) == 1
    assert llm.last_messages is None   # overseer was never consulted again

    await overseer.stop()
    await bus.stop()


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


async def test_no_event_bus_veto_blocks_order():
    """When event_bus is None, submit() must still honour an enforcing VETO.

    This is the synchronous review path: ``submit`` awaits
    ``overseer.review`` directly instead of going through the EventBus.
    Regression guard for the missing ``await`` that turned the verdict
    into an ignored coroutine and let vetoed orders reach the broker.
    """
    db = StubDB()
    broker = StubBroker()

    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[],
        approval_mode=False,
        event_bus=None,
    )

    llm = _FakeLLM('{"verdict": "VETO", "rationale": "High risk pattern detected"}')
    overseer = HermesOverseer(
        llm_client=llm, db=db, vision_enabled=False, autonomy="enforcing",
    )
    engine.overseer = overseer

    await engine.submit([_action("MSFT")])

    # VETO must block the order from ever reaching the broker.
    assert len(broker.placed) == 0
    assert len(db.pending_orders) == 0


async def test_no_event_bus_approve_places_order():
    """The synchronous review path still places an APPROVED order."""
    db = StubDB()
    broker = StubBroker()

    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[],
        approval_mode=False,
        event_bus=None,
    )

    llm = _FakeLLM('{"verdict": "APPROVE", "rationale": "Looks good"}')
    overseer = HermesOverseer(
        llm_client=llm, db=db, vision_enabled=False, autonomy="enforcing",
    )
    engine.overseer = overseer

    await engine.submit([_action("AAPL")])

    assert len(broker.placed) == 1
    assert broker.placed[0]["symbol"] == "AAPL"


async def test_no_event_bus_modify_places_modified_order():
    """The synchronous review path applies a MODIFY before placing the order."""
    db = StubDB()
    broker = StubBroker()

    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[],
        approval_mode=False,
        event_bus=None,
    )

    llm = _FakeLLM('{"verdict": "MODIFY", "rationale": "tighten fill", "modifications": {"price": 1.25}}')
    overseer = HermesOverseer(
        llm_client=llm, db=db, vision_enabled=False, autonomy="enforcing",
    )
    engine.overseer = overseer

    action = _action("GOOG")
    await engine.submit([action])

    assert len(broker.placed) == 1
    assert action.price == 1.25
    assert action.ai_authored is True


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

    # The proposal is a naked single-leg short put and this engine has no
    # MoneyManager wired — the AI-entry gate fails closed on both counts, so
    # nothing reaches the broker. (Before the gate existed, AI proposals
    # bypassed every risk filter and this asserted a fill.)
    assert len(broker.placed) == 0
    assert any("AI-GATE" in log for log in db.logs)

    await overseer.stop()
    await bus.stop()


async def test_async_overseer_close_flow():
    """An overseer-proposed close is priced from live quotes and routed to the
    broker as a management close — bypassing re-review of its own decision."""
    import json
    from ._stubs import make_trade

    bus = EventBus()
    bus.start()

    db = StubDB()
    broker = StubBroker()
    db.set_open_trades("CS75", [make_trade("CS75", "AAPL", trade_id=1, lots=1)])

    # Track the close routing to the real Trade row.
    closed: List[Any] = []
    orig_close = db.close_trade_from_action
    async def _track_close(action, response):
        closed.append(action)
        await orig_close(action, response)
    db.close_trade_from_action = _track_close

    engine = CascadingEngine(broker=broker, db=db, strategies=[],
                             approval_mode=False, event_bus=bus)

    reply = json.dumps({"closes": [{"trade_id": 1, "rationale": "lock profit"}]})
    overseer = HermesOverseer(llm_client=_FakeLLM(reply=reply), db=db,
                              vision_enabled=False, autonomy="autonomous",
                              event_bus=bus)
    await overseer.start()
    engine.overseer = overseer

    await engine._async_propose_closes()
    await asyncio.sleep(0.1)

    # The close reached the broker and was routed as a Trade-row close, not a
    # fresh entry — and it was never re-reviewed (no VETO log).
    assert len(broker.placed) == 1
    assert broker.placed[0]["symbol"] == "AAPL"
    assert len(closed) == 1
    assert closed[0].ai_authored is True
    assert closed[0].price is not None        # engine priced it from quotes
    assert not any("VETOED" in log for log in db.logs)

    await overseer.stop()
    await bus.stop()
