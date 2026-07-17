"""Tests for the C2 approval-execution lifecycle.

These guard the bug where the bot logged ``[C2 EXECUTED]`` and flipped
the approval row to ``EXECUTED`` even when:

* ``broker.dry_run=True`` — no broker call happened.
* The broker returned an ``errors`` response or a rejected order status.
* The broker raised an exception.

In every one of those cases the operator's C2 panel would say the trade
executed while the broker had nothing on its books.

The helper under test is ``hermes.service1_agent.main._execute_approved_action``.
"""
from __future__ import annotations

import dataclasses
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from hermes.service1_agent.core import TradeAction
from hermes.service1_agent.main import _execute_approved_action
from ._stubs import RepoNamespaceMixin


@pytest.fixture(autouse=True)
def mock_market_hours():
    with patch("hermes.market_hours.should_block_trades", return_value=(False, "regular session")), \
         patch("hermes.market_hours.should_block_new_entries", return_value=(False, "regular session")):
        yield


@pytest.fixture(autouse=True)
def reset_circuit_breaker():
    from hermes.service1_agent.core import AsyncBrokerWrapper
    from hermes.broker.circuit_breaker import CircuitBreaker
    AsyncBrokerWrapper._shared_cb = CircuitBreaker()


# ── Fakes ────────────────────────────────────────────────────────────────────
class _FakeBroker:
    """Captures place_order_from_action calls and replays a scripted response.

    Set ``raise_exc`` to an Exception instance to make the call raise instead
    of returning ``response``.
    """

    def __init__(self, *, response: Optional[Dict[str, Any]] = None,
                 raise_exc: Optional[Exception] = None,
                 dry_run: bool = False):
        self.response = response if response is not None else {
            "order": {"status": "ok", "id": "STUB-1"}
        }
        self.raise_exc = raise_exc
        self.dry_run = dry_run
        self.calls: List[TradeAction] = []

    def place_order_from_action(self, action: TradeAction) -> Dict[str, Any]:
        self.calls.append(action)
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.response


class _FakeDB(RepoNamespaceMixin):
    """Captures every DB write the helper performs so tests can assert order."""

    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        # Mirror of approval row state so tests can read the final status
        # without re-querying through a fake SQL layer.
        self.approval_status: Optional[str] = None
        self.approval_notes: Optional[str] = None

    async def record_pending_order(self, action: TradeAction) -> None:
        self.events.append({"op": "record_pending_order",
                            "symbol": action.symbol})

    async def record_order_response(self, action: TradeAction, resp: Dict[str, Any]) -> None:
        self.events.append({"op": "record_order_response",
                            "symbol": action.symbol, "resp": resp})

    async def close_trade_from_action(self, action: TradeAction, resp: Dict[str, Any]) -> None:
        self.events.append({"op": "close_trade_from_action",
                            "symbol": action.symbol, "resp": resp})

    async def mark_approval_executed(self, approval_id: int, success: bool = True,
                                notes: Optional[str] = None) -> None:
        self.events.append({"op": "mark_approval_executed",
                            "approval_id": approval_id,
                            "success": success, "notes": notes})
        self.approval_status = "EXECUTED" if success else "FAILED"
        self.approval_notes = notes

    async def write_log(self, strategy_id: str, message: str) -> None:
        self.events.append({"op": "write_log",
                            "strategy_id": strategy_id, "message": message})


# ── Helpers ──────────────────────────────────────────────────────────────────
def _make_action(symbol: str = "AAPL") -> TradeAction:
    return TradeAction(
        strategy_id="CS75",
        symbol=symbol,
        order_class="multileg",
        legs=[{"option_symbol": "AAPL250620P00150000",
               "side": "sell_to_open", "quantity": 1}],
        price=1.25, side="sell", quantity=1, order_type="credit",
        tag="HERMES_CS75",
        strategy_params={"side_type": "put"},
        expiry="2025-06-20",
    )


def _approval_item(action: TradeAction, approval_id: int = 1) -> Dict[str, Any]:
    return {"id": approval_id, "action_json": dataclasses.asdict(action),
            "strategy_id": action.strategy_id, "symbol": action.symbol}


def _logged(db: _FakeDB) -> List[str]:
    return [e["message"] for e in db.events if e["op"] == "write_log"]


# ── Happy path ───────────────────────────────────────────────────────────────
async def test_execute_marks_executed_on_clean_broker_response():
    """Happy path: broker accepts → approval row flips to EXECUTED and
    the operator feed gets [C2 EXECUTED]."""
    broker = _FakeBroker(response={"order": {"status": "ok", "id": "X-42"}})
    db = _FakeDB()
    action = _make_action()

    result = await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    assert result == "executed"
    assert len(broker.calls) == 1
    assert db.approval_status == "EXECUTED"
    assert any("[C2 EXECUTED]" in m for m in _logged(db))
    assert not any("[C2 PREVIEW]" in m for m in _logged(db))


# ── dry_run guard ────────────────────────────────────────────────────────────
async def test_dry_run_does_not_call_broker_and_does_not_mark_executed():
    """The original bug: dry_run=True skipped the broker call yet the row
    was marked EXECUTED. The fix must call no broker and write PREVIEW."""
    broker = _FakeBroker(dry_run=True)
    db = _FakeDB()
    action = _make_action()

    result = await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    assert result == "preview"
    assert broker.calls == []
    assert db.approval_status == "FAILED"
    assert any("[C2 PREVIEW]" in m for m in _logged(db))
    assert not any("[C2 EXECUTED]" in m for m in _logged(db))
    # Capacity must NOT be consumed by a pending row that will never settle.
    assert not any(e["op"] == "record_pending_order" for e in db.events)


# ── Rejection paths ──────────────────────────────────────────────────────────
async def test_broker_errors_response_is_marked_failed_not_executed():
    """Tradier returned an ``errors`` payload → must NOT mark EXECUTED."""
    broker = _FakeBroker(response={"errors": {"error": "insufficient buying power"}})
    db = _FakeDB()
    action = _make_action()

    result = await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    assert result == "rejected"
    assert len(broker.calls) == 1
    assert db.approval_status == "FAILED"
    assert any("[C2 REJECTED]" in m for m in _logged(db))
    assert not any("[C2 EXECUTED]" in m for m in _logged(db))


@pytest.mark.parametrize("status", ["rejected", "expired", "canceled",
                                     "cancelled", "error"])
async def test_broker_rejected_order_status_is_marked_failed(status):
    """Tradier returned a terminal-failure status on the order itself."""
    broker = _FakeBroker(response={"order": {"status": status, "id": "X-9"}})
    db = _FakeDB()
    action = _make_action()

    result = await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    assert result == "rejected"
    assert db.approval_status == "FAILED"
    assert not any("[C2 EXECUTED]" in m for m in _logged(db))


async def test_broker_raised_exception_is_marked_failed_with_error_note():
    """Broker connection blew up before returning a response."""
    broker = _FakeBroker(raise_exc=RuntimeError("tradier 503"))
    db = _FakeDB()
    action = _make_action()

    result = await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    assert result == "failed"
    assert db.approval_status == "FAILED"
    assert "tradier 503" in (db.approval_notes or "")
    # record_order_response was called with an errors payload so the
    # PendingOrder row is freed and capacity recovers.
    rr = [e for e in db.events if e["op"] == "record_order_response"]
    assert len(rr) == 1 and "errors" in rr[0]["resp"]
    assert not any("[C2 EXECUTED]" in m for m in _logged(db))


# ── Ordering invariant ───────────────────────────────────────────────────────
async def test_pending_order_is_recorded_before_broker_call():
    """record_pending_order must happen before place_order_from_action so
    capacity is reserved before the broker round-trip. Otherwise two
    concurrent ticks could double-submit."""
    broker = _FakeBroker()
    db = _FakeDB()
    action = _make_action()

    await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    ops = [e["op"] for e in db.events]
    pre_idx = ops.index("record_pending_order")
    resp_idx = ops.index("record_order_response")
    assert pre_idx < resp_idx


async def test_execute_approved_action_pure_close_routes_to_close_trade():
    """Verify that if the action being executed from C2 is a pure-close action,
    it calls close_trade_from_action instead of record_order_response."""
    broker = _FakeBroker()
    db = _FakeDB()
    
    # Pure-close action (all legs are *_to_close)
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL250620P00150000", "side": "buy_to_close", "quantity": 1},
            {"option_symbol": "AAPL250620P00145000", "side": "sell_to_close", "quantity": 1}
        ],
        price=1.25, side="buy", quantity=1, order_type="debit",
        tag="HERMES_CS75_CLOSE_TEST",
        strategy_params={"trade_id": 123},
        expiry="2025-06-20",
    )

    result = await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    assert result == "executed"
    assert len(broker.calls) == 1
    assert db.approval_status == "EXECUTED"
    
    # Assert close_trade_from_action was called instead of record_order_response
    ops = [e["op"] for e in db.events]
    assert "close_trade_from_action" in ops
    assert "record_order_response" not in ops


# ── Close-buffer gate routing ───────────────────────────────────────────────
def _close_action() -> TradeAction:
    return TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[
            {"option_symbol": "AAPL250620P00150000", "side": "buy_to_close", "quantity": 1},
            {"option_symbol": "AAPL250620P00145000", "side": "sell_to_close", "quantity": 1},
        ],
        price=1.25, side="buy", quantity=1, order_type="debit",
        tag="HERMES_CS75_CLOSE_TEST", strategy_params={"trade_id": 123},
        expiry="2025-06-20",
    )


async def test_approval_open_action_deferred_in_close_buffer():
    """An opening C2-approved action must be deferred (not sent to the
    broker) inside the pre-close entry buffer — same regression as the
    2026-07-16 HermesAlpha META incident, applied to the approval path."""
    broker = _FakeBroker()
    db = _FakeDB()
    action = _make_action()  # sell_to_open — an entry, not a close

    with patch("hermes.market_hours.should_block_trades", return_value=(False, "regular session")), \
         patch("hermes.market_hours.should_block_new_entries",
               return_value=(True, "closing soon (0.2m to close < 5m entry cutoff)")):
        result = await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    assert result == "deferred"
    assert broker.calls == []
    assert any("[C2 DEFERRED]" in m for m in _logged(db))


async def test_approval_close_action_ignores_close_buffer():
    """A pure-close C2-approved action must still execute inside the
    pre-close entry buffer — exits shouldn't be held back from the bell."""
    broker = _FakeBroker()
    db = _FakeDB()
    action = _close_action()

    with patch("hermes.market_hours.should_block_trades", return_value=(False, "regular session")), \
         patch("hermes.market_hours.should_block_new_entries",
               return_value=(True, "closing soon (0.2m to close < 5m entry cutoff)")):
        result = await _execute_approved_action(_approval_item(action), broker=broker, db=db)

    assert result == "executed"
    assert len(broker.calls) == 1

