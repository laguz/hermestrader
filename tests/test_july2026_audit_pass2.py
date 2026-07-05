"""Regression tests for the 2026-07-04 audit pass 2 (hand-verified against
source before fixing; each test was confirmed to fail when its fix was
reverted)."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from hermes.broker.tradier import TradierBroker
from hermes.ml.ledger import backfill_prediction_outcomes
from hermes.service1_agent._engine_reactive import ReactiveController

_CFG = {"tradier_access_token": "t", "tradier_account_id": "a"}


class DummyDB:
    def __init__(self):
        self.AsyncSession = MagicMock()


# --- Bug: backfill_prediction_outcomes falsy-zero on spot -------------------
async def test_ledger_backfill_preserves_zero_spot():
    """A genuinely recorded spot=0.0 was replaced by realized_close (`if
    row.spot else realized_close`), making outcome = 1.0 if realized_close >
    realized_close else 0.0 — always 0.0 regardless of real price movement."""
    db = DummyDB()
    row = MagicMock()
    row.horizon_dte = 1
    row.ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    row.symbol = "AAPL"
    row.realized_outcome = None
    row.spot = 0.0

    async def mock_execute(*args, **kwargs):
        res = MagicMock()
        res.scalars.return_value.all.return_value = [row]
        return res

    session = AsyncMock()
    db.AsyncSession.return_value = session
    session.__aenter__.return_value.execute = mock_execute

    idx = pd.date_range("2026-06-01", periods=5, freq="D", tz="UTC")
    df = pd.DataFrame({"close": [10.0, 11.0, 12.0, 13.0, 14.0]}, index=idx)
    db.daily_bars = AsyncMock(return_value=df)

    await backfill_prediction_outcomes(db, lookback_days=10)

    assert row.realized_outcome == 1.0


# --- Bug: unrecognized event_type silently resolved to None, no signal -----
async def test_process_event_unknown_type_logs_warning(caplog):
    ctx = MagicMock()
    ctx.event_bus = MagicMock()
    engine_mock = MagicMock()
    engine_mock.ctx = ctx
    controller = ReactiveController(engine_mock)

    with caplog.at_level(logging.WARNING):
        await controller._process_event("BOGUS_TYPE", {})

    assert any("Unrecognized event_type" in r.message for r in caplog.records)


# --- Bug: one strategy's watchlist-lookup failure aborted reactive entries -
# for every strategy, not just the failing one (gather without isolation) ---
async def test_process_reactive_entries_isolates_watchlist_failure():
    ok_strategy = MagicMock()
    ok_strategy.strategy_id = "CS75"
    ok_strategy.NAME = "CS75"
    ok_strategy.execute_entries = AsyncMock(return_value=[])

    bad_strategy = MagicMock()
    bad_strategy.strategy_id = "WHEEL"
    bad_strategy.NAME = "Wheel"
    bad_strategy.execute_entries = AsyncMock(return_value=[])

    def fake_emit(event):
        # Mirrors the real EventBus: resolving the command's future is what
        # lets `await cmd.future` return instead of hanging forever.
        fut = getattr(event, "future", None)
        if fut is not None and not fut.done():
            fut.set_result(None)

    ctx = MagicMock()
    ctx.strategies = [bad_strategy, ok_strategy]
    ctx.config = {}
    ctx.event_bus = MagicMock()
    ctx.event_bus.emit = MagicMock(side_effect=fake_emit)

    engine_mock = MagicMock()
    engine_mock.ctx = ctx

    async def watchlist_for(strategy_id, symbols):
        if strategy_id == "WHEEL":
            raise RuntimeError("db down")
        return symbols

    engine_mock._watchlist_for = AsyncMock(side_effect=watchlist_for)

    controller = ReactiveController(engine_mock)
    # Before the fix this raised RuntimeError out of asyncio.gather, aborting
    # entries for ok_strategy too, not just the strategy whose lookup failed.
    await controller.process_reactive_entries("AAPL")

    ok_strategy.execute_entries.assert_called_once_with(["AAPL"])


# --- Bug: cancel_order bypassed the shared retry policy + structured body --
# logging every other network call in this file uses -------------------------
async def test_cancel_order_retries_transient_failure_and_succeeds():
    broker = TradierBroker(_CFG)

    fail_response = MagicMock()
    fail_response.is_success = False
    fail_response.status_code = 503
    fail_response.reason_phrase = "Service Unavailable"
    fail_response.json.side_effect = ValueError()
    fail_response.text = "unavailable"
    fail_response.request = MagicMock()

    ok_response = MagicMock()
    ok_response.is_success = True
    ok_response.json.return_value = {"order": {"id": 1, "status": "ok"}}

    client = AsyncMock()
    client.delete = AsyncMock(side_effect=[fail_response, ok_response])

    with patch.object(broker, "_get_client", return_value=client), \
         patch("asyncio.sleep", new=AsyncMock()):
        result = await broker.cancel_order("123")

    assert result == {"order": {"id": 1, "status": "ok"}}
    assert client.delete.call_count == 2
    await broker.close()


async def test_cancel_order_logs_structured_body_on_failure(caplog):
    broker = TradierBroker(_CFG)

    fail_response = MagicMock()
    fail_response.is_success = False
    fail_response.status_code = 400
    fail_response.reason_phrase = "Bad Request"
    fail_response.json.return_value = {"fault": "already filled"}
    fail_response.request = MagicMock()

    client = AsyncMock()
    client.delete = AsyncMock(return_value=fail_response)

    with patch.object(broker, "_get_client", return_value=client), \
         patch("asyncio.sleep", new=AsyncMock()), \
         caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            await broker.cancel_order("123")

    assert any("already filled" in r.message for r in caplog.records)
    await broker.close()
