"""Phase-3 exit-timing policy: offline learner + engine capture/advisory.

Unit-tests the pure batch value estimator (state bucketing, Q(close)/Q(hold),
recommendations, support gating) and the engine integration (shadow captures +
advises without acting; active closes confidently-flagged positions only under
enforcing autonomy; off is a no-op).
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from hermes.db.models import Trade
from hermes.ml.exit_policy import recommend, state_key, train_exit_policy
from hermes.service1_agent.core import CascadingEngine


# --------------------------------------------------------------------------- #
# pure learner
# --------------------------------------------------------------------------- #
def test_state_key_buckets_pnl_and_dte():
    assert state_key(0.10, 25) == "pnl:0–0.25|dte:21–30"
    assert state_key(-0.80, 3) == "pnl:<-0.5|dte:<7"
    assert state_key(0.90, 60) == "pnl:≥0.75|dte:≥45"


def _tick(tid, pnl, dte, action, ts):
    return {"trade_id": tid, "unrealized_pnl_pct": pnl, "dte": dte,
            "action": action, "ts": ts}


def test_policy_recommends_close_when_holding_underperforms():
    # 12 trajectories: held at (pnl 0.1, dte 25) then ended at -0.30.
    # Closing locks ~0.1; holding historically decayed to -0.30 → close.
    ticks = []
    for tid in range(12):
        ticks.append(_tick(tid, 0.10, 25, "hold", ts=1))
        ticks.append(_tick(tid, -0.30, 4, "close", ts=2))
    policy = train_exit_policy(ticks, min_support=10, margin=0.05)

    rec = recommend(policy, 0.10, 25)
    assert rec["action"] == "close"
    assert rec["confident"] is True
    assert rec["q_close"] > rec["q_hold"]
    assert rec["support"] == 12


def test_policy_holds_when_holding_outperforms():
    # Held at (pnl 0.1, dte 40) then improved to +0.60 → keep holding.
    ticks = []
    for tid in range(12):
        ticks.append(_tick(tid, 0.10, 40, "hold", ts=1))
        ticks.append(_tick(tid, 0.60, 38, "close", ts=2))
    policy = train_exit_policy(ticks, min_support=10, margin=0.05)
    assert recommend(policy, 0.10, 40)["action"] == "hold"


def test_low_support_never_recommends_close():
    ticks = []
    for tid in range(3):                                  # only 3 < min_support
        ticks.append(_tick(tid, 0.10, 25, "hold", ts=1))
        ticks.append(_tick(tid, -0.30, 4, "close", ts=2))
    policy = train_exit_policy(ticks, min_support=10, margin=0.05)
    assert recommend(policy, 0.10, 25)["action"] == "hold"


def test_unseen_state_holds_and_is_not_confident():
    policy = train_exit_policy([], min_support=10)
    rec = recommend(policy, 0.5, 30)
    assert rec["action"] == "hold"
    assert rec["confident"] is False
    assert rec["support"] == 0


# --------------------------------------------------------------------------- #
# engine integration
# --------------------------------------------------------------------------- #
# ``db`` fixture (fresh throwaway Timescale DB) is provided by tests/conftest.py.


class _StubOverseer:
    def __init__(self, autonomy):
        self.autonomy = autonomy


class _StubBroker:
    """Minimal broker: quotes for legs + an order sink for active closes."""

    def __init__(self, quotes):
        self._quotes = quotes
        self.placed = []
        self.dry_run = False

    async def get_quote(self, symbols):
        syms = [s.strip() for s in symbols.split(",") if s.strip()]
        return [self._quotes[s] for s in syms if s in self._quotes]

    async def get_positions(self):
        return []

    async def place_order_from_action(self, action):
        self.placed.append(action)
        return {"order": {"status": "filled", "id": "1"}}


SHORT = "TSLA260717P00440000"
LONG = "TSLA260717P00435000"
# Quotes give a spread mid debit of 0.90 → pnl% = (1.0 - 0.90)/1.0 = 0.10.
QUOTES = {
    SHORT: {"symbol": SHORT, "bid": 0.90, "ask": 1.00},   # mid 0.95
    LONG: {"symbol": LONG, "bid": 0.00, "ask": 0.10},     # mid 0.05
}


async def _seed_open_trade(db, *, dte=25):
    await db.watchlist.ensure_strategies({"CS75": 1})
    async with db.AsyncSession() as s:
        s.add(Trade(
            id=1, strategy_id="CS75", symbol="TSLA", side_type="put",
            short_leg=SHORT, long_leg=LONG, width=5.0, lots=1,
            entry_credit=1.00, status="OPEN",
            expiry=date.today() + timedelta(days=dte),
        ))
        await s.commit()


async def _seed_close_recommending_trajectories(db, n=12):
    """Completed trajectories where holding at (0.1, ~25) decayed to -0.30."""
    for tid in range(100, 100 + n):
        await db.trades.record_exit_tick(
            trade_id=tid, strategy_id="CS75", symbol="TSLA", dte=25,
            unrealized_pnl_pct=0.10, debit=0.90, entry_credit=1.0, action="hold")
        await db.trades.record_exit_tick(
            trade_id=tid, strategy_id="CS75", symbol="TSLA", dte=4,
            unrealized_pnl_pct=-0.30, debit=1.30, entry_credit=1.0,
            action="close", close_reason="SL")


@pytest.mark.asyncio
async def test_shadow_captures_and_advises_without_acting(db):
    await _seed_open_trade(db)
    await db.settings.set_setting("exit_policy_mode", "shadow")
    broker = _StubBroker(QUOTES)
    engine = CascadingEngine(broker=broker, db=db, strategies=[])

    await engine.ai._maybe_capture_and_advise_exits([])

    # A trajectory tick was recorded; advice audited; nothing closed.
    ticks = await db.trades.fetch_exit_ticks()
    assert len(ticks) == 1
    assert ticks[0]["action"] == "hold"
    assert abs(ticks[0]["unrealized_pnl_pct"] - 0.10) < 1e-6
    decisions = await db.decisions.recent_ai_decisions(strategy_id="EXITPOLICY")
    assert len(decisions) == 1
    assert decisions[0]["decision"]["mode"] == "shadow"
    assert decisions[0]["decision"]["acted"] == []
    assert broker.placed == []                            # no order submitted


@pytest.mark.asyncio
async def test_active_closes_confident_positions(db):
    # Spy on submit() to isolate the Phase-3 decision (decide + dispatch a
    # close) from the broker/order-recording path.
    await _seed_open_trade(db)
    await _seed_close_recommending_trajectories(db)       # policy learns 'close'
    await db.settings.set_setting("exit_policy_mode", "active")
    engine = CascadingEngine(
        broker=_StubBroker(QUOTES), db=db, strategies=[],
        overseer=_StubOverseer("enforcing"))

    submitted = []

    async def _spy(actions, action_type="entry"):
        submitted.append((list(actions), action_type))

    engine.submit = _spy

    await engine.ai._maybe_capture_and_advise_exits([])

    # A confident close was dispatched for the open trade.
    assert len(submitted) == 1
    actions, action_type = submitted[0]
    assert action_type == "management"
    assert actions[0].strategy_params["trade_id"] == 1
    assert actions[0].strategy_params["close_reason"] == "EXIT-POLICY"
    assert actions[0].ai_authored is True
    decisions = await db.decisions.recent_ai_decisions(strategy_id="EXITPOLICY")
    assert decisions[0]["decision"]["acted"] == [1]


@pytest.mark.asyncio
async def test_active_close_price_never_exceeds_spread_width(db):
    # A near-max-loss 5-wide: spread mid debit ~4.90, so debit*1.05 = 5.145
    # would bid ABOVE the width. The close limit must be capped at 5.00.
    deep_short = "TSLA260717P00500000"
    deep_long = "TSLA260717P00450000"
    quotes = {
        deep_short: {"symbol": deep_short, "bid": 4.90, "ask": 5.00},   # mid 4.95
        deep_long: {"symbol": deep_long, "bid": 0.00, "ask": 0.10},      # mid 0.05
    }                                                                     # debit 4.90
    await db.watchlist.ensure_strategies({"CS75": 1})
    async with db.AsyncSession() as s:
        s.add(Trade(
            id=1, strategy_id="CS75", symbol="TSLA", side_type="put",
            short_leg=deep_short, long_leg=deep_long, width=5.0, lots=1,
            entry_credit=1.00, status="OPEN",
            expiry=date.today() + timedelta(days=25)))
        await s.commit()
    # Trajectories that recommend closing at this deep-loss state.
    for tid in range(100, 112):
        await db.trades.record_exit_tick(
            trade_id=tid, strategy_id="CS75", symbol="TSLA", dte=25,
            unrealized_pnl_pct=-3.90, debit=4.90, entry_credit=1.0, action="hold")
        await db.trades.record_exit_tick(
            trade_id=tid, strategy_id="CS75", symbol="TSLA", dte=4,
            unrealized_pnl_pct=-4.50, debit=5.00, entry_credit=1.0,
            action="close", close_reason="SL")
    await db.settings.set_setting("exit_policy_mode", "active")
    engine = CascadingEngine(
        broker=_StubBroker(quotes), db=db, strategies=[],
        overseer=_StubOverseer("enforcing"))

    submitted = []

    async def _spy(actions, action_type="entry"):
        submitted.append((list(actions), action_type))

    engine.submit = _spy

    await engine.ai._maybe_capture_and_advise_exits([])

    assert len(submitted) == 1                              # confident close fired
    price = submitted[0][0][0].price
    assert price <= 5.00                                    # never above the width
    assert price == 5.00                                    # capped at it


@pytest.mark.asyncio
async def test_active_blocked_under_advisory_autonomy(db):
    await _seed_open_trade(db)
    await _seed_close_recommending_trajectories(db)
    await db.settings.set_setting("exit_policy_mode", "active")
    engine = CascadingEngine(
        broker=_StubBroker(QUOTES), db=db, strategies=[],
        overseer=_StubOverseer("advisory"))

    submitted = []

    async def _spy(actions, action_type="entry"):
        submitted.append((list(actions), action_type))

    engine.submit = _spy

    await engine.ai._maybe_capture_and_advise_exits([])

    # Advisory autonomy never closes, even with a confident recommendation.
    assert submitted == []


@pytest.mark.asyncio
async def test_off_mode_is_a_noop(db):
    await _seed_open_trade(db)
    broker = _StubBroker(QUOTES)
    engine = CascadingEngine(broker=broker, db=db, strategies=[])

    await engine.ai._maybe_capture_and_advise_exits([])

    assert await db.trades.fetch_exit_ticks() == []
    assert await db.decisions.recent_ai_decisions(strategy_id="EXITPOLICY") == []


@pytest.mark.asyncio
async def test_reactive_exit_on_market_data_event(db, monkeypatch):
    from hermes.events.bus import MarketDataEvent
    import hermes.market_hours
    monkeypatch.setattr(hermes.market_hours, "should_block_trades", lambda: (False, ""))

    await _seed_open_trade(db)
    await _seed_close_recommending_trajectories(db)
    await db.settings.set_setting("exit_policy_mode", "active")

    engine = CascadingEngine(
        broker=_StubBroker(QUOTES), db=db, strategies=[],
        overseer=_StubOverseer("enforcing"))

    submitted = []
    async def _spy(actions, action_type="entry"):
        submitted.append((list(actions), action_type))
    engine.submit = _spy

    event = MarketDataEvent(
        symbol="TSLA260717P00440000",
        price=0.95,
        volume=10,
        data={"bid": 0.90, "ask": 1.00}
    )

    engine._quote_cache["TSLA260717P00440000"] = {"bid": 0.90, "ask": 1.00}
    engine._quote_cache["TSLA260717P00435000"] = {"bid": 0.00, "ask": 0.10}

    await engine.handle_market_data(event)

    assert len(submitted) == 1
    actions, action_type = submitted[0]
    assert action_type == "management"
    assert actions[0].strategy_params["trade_id"] == 1
    assert actions[0].strategy_params["close_reason"] == "EXIT-POLICY-REACTIVE"
    assert actions[0].ai_authored is True
