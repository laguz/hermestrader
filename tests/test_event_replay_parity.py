"""Replay-parity guardrail for the event-sourcing layer.

The invariant the whole migration rests on: the read models (trades,
pending_orders, system_settings) are a deterministic projection of the
append-only ``event_ledger``. This test asserts that two ways of building the
read models agree:

  (a) **live** — events recorded via ``EventStoreManager.record_event``, which
      appends to the ledger AND projects in the same transaction;
  (b) **replay** — the same ledger loaded and re-projected onto a clean DB.

If these ever diverge, the projection logic and the event log have fallen out
of sync — exactly the failure event sourcing must never have. This guardrail is
the safety net every later migration phase relies on.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select

from hermes.db.models import Trade, PendingOrder, SystemSetting, StrategyWatchlist
from hermes.db.events import (
    EventStoreManager,
    OrderSubmittedEvent,
    OrderFilledEvent,
    CloseSubmittedEvent,
    CloseFilledEvent,
    SystemSettingChangedEvent,
    WatchlistChangedEvent,
    ModeChangedEvent,
    StrategyToggledEvent,
    AutonomyChangedEvent,
    PauseChangedEvent,
)
from hermes.db.repositories.projections import ProjectionsRepository


def _sample_lifecycle():
    """A small end-to-end lifecycle: open a credit spread, then close it for a win."""
    now = datetime.utcnow().isoformat()
    return [
        OrderSubmittedEvent(id=1, strategy_id="CS75", symbol="AAPL", side="sell",
                            quantity=5, payload={"legs": []}, submitted_at=now),
        OrderFilledEvent(pending_order_id=1, trade_id=1, trade_fields={
            "strategy_id": "CS75", "symbol": "AAPL", "side_type": "put",
            "lots": 5, "entry_credit": 1.25, "opened_at": now,
        }),
        SystemSettingChangedEvent(key="agent_paused", value="true", updated_at=now),
        OrderSubmittedEvent(id=2, strategy_id="CS75", symbol="AAPL", side="buy",
                            quantity=5, payload={"legs": []}, submitted_at=now),
        CloseSubmittedEvent(pending_order_id=2, trade_id=1, exit_price=0.25,
                            close_reason="TP", close_tag="HERMES_CS75_CLOSE_TP"),
        CloseFilledEvent(trade_id=1, closed_at=now),
        WatchlistChangedEvent(strategy_id="CS75", symbols=["AAPL", "MSFT"], updated_at=now),
        ModeChangedEvent(mode="live", updated_at=now),
        StrategyToggledEvent(strategy_id="CS75", enabled=False, updated_at=now),
        AutonomyChangedEvent(autonomy="live", updated_at=now),
        PauseChangedEvent(paused=True, updated_at=now),
    ]


async def _snapshot(db):
    """A comparable snapshot of the read-model tables."""
    async with db.AsyncSession() as s:
        trades = (await s.execute(select(Trade).order_by(Trade.id))).scalars().all()
        orders = (await s.execute(select(PendingOrder).order_by(PendingOrder.id))).scalars().all()
        settings = (await s.execute(select(SystemSetting).order_by(SystemSetting.key))).scalars().all()
        watchlists = (await s.execute(select(StrategyWatchlist).order_by(StrategyWatchlist.strategy_id, StrategyWatchlist.symbol))).scalars().all()
    return (
        [(t.id, t.status, str(t.exit_price), t.close_reason, str(t.pnl)) for t in trades],
        [(o.id, o.status) for o in orders],
        [(s.key, s.value) for s in settings],
        [(w.strategy_id, w.symbol) for w in watchlists],
    )


async def test_live_projection_matches_ledger_replay(make_db):
    # (a) live: record_event appends to the ledger AND projects, atomically.
    live = make_db()
    # Postgres enforces the trades.strategy_id FK, so the strategies registry
    # must exist before the projection inserts trades (production seeds it at boot).
    await live.watchlist.ensure_strategies({"CS75": 1})
    async with live.AsyncSession() as s:
        for ev in _sample_lifecycle():
            await EventStoreManager.record_event(s, ev)
        await s.commit()

    # (b) replay: load the ledger and re-project onto a clean DB.
    async with live.AsyncSession() as s:
        events = await EventStoreManager.load_events(s)
    replay = make_db()
    await replay.watchlist.ensure_strategies({"CS75": 1})
    async with replay.AsyncSession() as s:
        for ev in events:
            await ProjectionsRepository.apply_event_projection(s, ev)
        await s.commit()

    live_state = await _snapshot(live)
    replay_state = await _snapshot(replay)
    assert live_state == replay_state, (
        "Read models built live (record_event) diverged from a clean replay of "
        "the same event_ledger — projection logic and the log are out of sync."
    )

    # Sanity: the lifecycle actually projected through to a closed, winning trade
    # and the setting change landed — so the parity above is meaningful, not two
    # empty databases matching.
    trades, orders, settings, watchlists = live_state
    assert trades == [(1, "CLOSED", "0.2500", "TP", "500.00")]
    assert orders == [(1, "SUBMITTED"), (2, "SUBMITTED")]
    assert ("agent_paused", "true") in settings
    assert ("hermes_mode", "live") in settings
    assert ("strategy_cs75_enabled", "false") in settings
    assert ("agent_autonomy", "live") in settings
    assert ("agent_paused", "true") in settings
    assert watchlists == [("CS75", "AAPL"), ("CS75", "MSFT")]


async def test_record_event_is_atomic_append_plus_projection(make_db):
    """One record_event call writes both the ledger row and its projection."""
    db = make_db()
    await db.watchlist.ensure_strategies({"CS75": 1})
    now = datetime.utcnow().isoformat()
    async with db.AsyncSession() as s:
        await EventStoreManager.record_event(
            s,
            OrderSubmittedEvent(id=1, strategy_id="CS75", symbol="MSFT",
                                side="sell", quantity=3, payload={"legs": []},
                                submitted_at=now),
        )
        await s.commit()

    async with db.AsyncSession() as s:
        ledger = await EventStoreManager.load_events(s)
        order = (await s.execute(select(PendingOrder).where(PendingOrder.id == 1))).scalars().first()
    assert len(ledger) == 1 and isinstance(ledger[0], OrderSubmittedEvent)
    assert order is not None and order.status == "PENDING"


async def test_read_models_recover_by_rebuilding_from_ledger(make_db):
    """Crash recovery: wipe the order/trade read models, replay the immutable
    event_ledger, and the exact prior state returns — state is a pure function
    of the log. This is the defining property of the event-sourced persistence."""
    from sqlalchemy import delete

    db = make_db()
    await db.watchlist.ensure_strategies({"CS75": 1})
    async with db.AsyncSession() as s:
        for ev in _sample_lifecycle():
            await EventStoreManager.record_event(s, ev)
        await s.commit()

    before = await _snapshot(db)
    assert before[0] and before[0][0][1] == "CLOSED"   # there is real trade state

    # Simulate read-model loss/corruption — the event_ledger is left intact.
    async with db.AsyncSession() as s:
        await s.execute(delete(Trade))
        await s.execute(delete(PendingOrder))
        await s.commit()
    wiped = await _snapshot(db)
    assert wiped[0] == [] and wiped[1] == [], "read models were not actually wiped"

    # Recover purely from the log.
    async with db.AsyncSession() as s:
        n = await ProjectionsRepository.rebuild(s)
        await s.commit()
    after = await _snapshot(db)

    assert n >= 6
    assert after[0] == before[0], "trades not fully recovered from the ledger"
    assert after[1] == before[1], "pending_orders not fully recovered from the ledger"
