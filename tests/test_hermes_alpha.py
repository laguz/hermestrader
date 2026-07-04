"""Unit tests for autonomous HermesAlpha (priority-5, LLM-originated spreads).

Covers the four things that make Alpha safe to run unattended:

- **Origination is autonomous-only.** ``execute_entries`` no-ops unless the
  overseer is in ``autonomous`` mode; an overseer intent is resolved + priced
  against the live chain (the LLM names a structure, the chain prices it).
- **Exits are broker-verified.** The close price comes from the live quote via
  ``compute_close_debit``; a stale/unverifiable quote forces a *hold*, and a
  deterministic backstop forces a time-exit the LLM cannot defer.
- **The no-human-in-the-loop bypass is gated, not strategy-scoped.** When
  ``alpha_autonomous_live`` is armed *and* autonomy is ``autonomous``, ANY
  strategy (HermesAlpha, CS75, …) skips the approval queue; the switch-OFF case
  still queues for every strategy.
- **The weekly kill switch** trips on loss-rate / capital-loss / CS75
  underperformance and disables Alpha.

All offline — stub broker / stub DB, fake overseer.
"""
from __future__ import annotations

from datetime import timedelta

import pytest

from hermes.service1_agent.core import IronCondorBuilder, MoneyManager, TradeAction, CascadingEngine
from hermes.service1_agent.control_state import ControlState
from hermes.service1_agent.strategies import HermesAlpha
from hermes.service1_agent import alpha_killswitch as ks

from ._stubs import StubBroker, StubDB, make_trade, _et_today


# ── fakes ────────────────────────────────────────────────────────────────────
class _FakeOverseer:
    def __init__(self, *, autonomy="autonomous", intent=None, exit_action="hold"):
        self.autonomy = autonomy
        self._intent = intent
        self._exit_action = exit_action
        self.proposed = []          # symbols passed to propose_intent
        self.exit_calls = []        # symbols passed to decide_exit

    async def propose_intent(self, symbol, context):
        self.proposed.append(symbol)
        return self._intent

    async def decide_exit(self, trade, context):
        self.exit_calls.append(trade["symbol"])
        return {"action": self._exit_action, "rationale": "test"}


def _intent(side="put"):
    return {"action": "trade", "side": side, "target_delta": 0.16,
            "dte_min": 30, "dte_max": 45, "width": 5, "rationale": "test setup"}


def _build_alpha(*, overseer, db=None, config=None, broker_kwargs=None):
    broker = StubBroker(**(broker_kwargs or {}))
    db = db or StubDB()
    cfg = {"hermesalpha_width": 5, "hermesalpha_min_credit_pct": 0.0,
           "hermesalpha_target_lots": 1, "hermesalpha_max_lots": 1}
    cfg.update(config or {})
    mm = MoneyManager(broker, db, cfg)
    s = HermesAlpha(broker=broker, db=db, money_manager=mm,
                    ic_builder=IronCondorBuilder(mm), config=cfg,
                    dry_run=False, overseer=overseer)
    return s, broker, db


# ── origination gating ───────────────────────────────────────────────────────
async def test_no_origination_without_overseer():
    s, _, _ = _build_alpha(overseer=None)
    s.overseer = None
    assert await s.execute_entries(["AAPL"]) == []


@pytest.mark.parametrize("autonomy", ["advisory", "enforcing"])
async def test_no_origination_unless_autonomous(autonomy):
    ov = _FakeOverseer(autonomy=autonomy, intent=_intent())
    s, _, _ = _build_alpha(overseer=ov)
    assert await s.execute_entries(["AAPL"]) == []
    assert ov.proposed == []          # overseer never consulted off-autonomous


async def test_autonomous_origination_prices_intent_against_chain():
    ov = _FakeOverseer(autonomy="autonomous", intent=_intent("put"))
    s, _, _ = _build_alpha(overseer=ov,
                           broker_kwargs={"expirations": [
                               (_et_today() + timedelta(days=d)).isoformat()
                               for d in (30, 40, 45)]})
    actions = await s.execute_entries(["AAPL"])
    assert len(actions) == 1
    a = actions[0]
    assert a.tag == "HERMES_HERMESALPHA"
    assert a.order_class == "multileg"
    assert a.order_type == "credit" and a.price > 0
    assert a.strategy_params.get("side_type") == "put"
    # Overseer-originated → flagged AI-authored so the engine won't re-review it.
    assert a.ai_authored is True


async def test_no_trade_intent_yields_no_action():
    ov = _FakeOverseer(autonomy="autonomous", intent=None)   # propose_intent declined
    s, _, _ = _build_alpha(overseer=ov)
    assert await s.execute_entries(["AAPL"]) == []
    assert ov.proposed == ["AAPL"]    # consulted, but declined


async def test_origination_scoped_to_supplied_watchlist():
    ov = _FakeOverseer(autonomy="autonomous", intent=_intent())
    s, _, _ = _build_alpha(overseer=ov)
    await s.execute_entries(["AAA", "BBB"])
    # Only the symbols handed in are scanned — Alpha never reaches outside its
    # own watchlist (the pipeline routes each strategy its own list).
    assert ov.proposed == ["AAA", "BBB"]


# ── broker-verified exits ────────────────────────────────────────────────────
def _seed_open(db, *, days_to_expiry=21):
    db.set_open_trades("HERMESALPHA", [
        make_trade("HERMESALPHA", "AAPL", entry_credit=1.50,
                   days_to_expiry=days_to_expiry)])


async def test_exit_submitted_when_live_quote_validates():
    ov = _FakeOverseer(autonomy="autonomous", exit_action="close")
    db = StubDB()
    _seed_open(db)
    s, broker, _ = _build_alpha(overseer=ov, db=db)
    # Default stub quotes (bid 99.95 / ask 100.05) make compute_close_debit a
    # small, valid debit → the LLM close is verified and submitted.
    actions = await s.manage_positions()
    assert any(a.tag.startswith("HERMES_HERMESALPHA_CLOSE") for a in actions)
    assert ov.exit_calls == ["AAPL"]


async def test_exit_dropped_when_quote_unverifiable():
    ov = _FakeOverseer(autonomy="autonomous", exit_action="close")
    db = StubDB()
    _seed_open(db)
    s, broker, _ = _build_alpha(overseer=ov, db=db)
    # Stale quote: bid=0 → compute_close_debit blocks. The LLM's close cannot be
    # price-verified, so we hold and never even consult the overseer for it.
    broker.get_quote = lambda symbols: [
        {"symbol": x.strip(), "bid": 0.0, "ask": 2.0} for x in symbols.split(",")]
    actions = await s.manage_positions()
    assert actions == []
    assert ov.exit_calls == []


async def test_exit_hold_keeps_position():
    ov = _FakeOverseer(autonomy="autonomous", exit_action="hold")
    db = StubDB()
    _seed_open(db)
    s, _, _ = _build_alpha(overseer=ov, db=db)
    assert await s.manage_positions() == []
    assert ov.exit_calls == ["AAPL"]


async def test_time_exit_backstop_overrides_llm_hold():
    ov = _FakeOverseer(autonomy="autonomous", exit_action="hold")
    db = StubDB()
    _seed_open(db, days_to_expiry=1)     # inside the time-exit floor (default 2)
    s, _, _ = _build_alpha(overseer=ov, db=db)
    actions = await s.manage_positions()
    assert any("CLOSE_TIME-EXIT" in a.tag for a in actions)
    # Deterministic backstop fires before (and instead of) consulting the LLM.
    assert ov.exit_calls == []


# ── scoped approval-queue bypass ─────────────────────────────────────────────
def _alpha_action():
    return TradeAction(
        strategy_id="HERMESALPHA", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL250620P00090000", "side": "sell_to_open", "quantity": 1},
              {"option_symbol": "AAPL250620P00085000", "side": "buy_to_open", "quantity": 1}],
        price=1.50, side="sell", quantity=1, order_type="credit",
        tag="HERMES_HERMESALPHA", strategy_params={"side_type": "put"},
        expiry="2025-06-20", width=5.0)


def _cs75_action():
    a = _alpha_action()
    a.strategy_id = "CS75"
    a.tag = "HERMES_CS75"
    return a


def _engine_with_state(*, autonomy, alpha_live):
    db = StubDB()
    broker = StubBroker()
    engine = CascadingEngine(broker=broker, db=db, strategies=[], approval_mode=True)
    cs = ControlState()
    cs.autonomy = autonomy
    cs.alpha_autonomous_live = alpha_live
    engine.control_state = cs
    return engine, db


async def test_alpha_autonomous_live_bypasses_approval_queue():
    engine, db = _engine_with_state(autonomy="autonomous", alpha_live=True)
    await engine._execute_or_queue(_alpha_action(), "entry")
    assert list(db.approvals) == []           # not queued for a human
    assert len(db.pending_orders) == 1        # routed straight to the broker path


async def test_alpha_still_queues_when_switch_off():
    engine, db = _engine_with_state(autonomy="autonomous", alpha_live=False)
    await engine._execute_or_queue(_alpha_action(), "entry")
    assert len(list(db.approvals)) == 1       # default-OFF switch keeps the human gate
    assert db.pending_orders == []


async def test_non_alpha_also_bypasses_when_autonomous_live():
    # Global auto-execute: the armed switch bypasses approval for every strategy,
    # not just HermesAlpha.
    engine, db = _engine_with_state(autonomy="autonomous", alpha_live=True)
    await engine._execute_or_queue(_cs75_action(), "entry")
    assert list(db.approvals) == []           # CS75 routed straight through
    assert len(db.pending_orders) == 1


async def test_non_alpha_still_queues_when_switch_off():
    engine, db = _engine_with_state(autonomy="autonomous", alpha_live=False)
    await engine._execute_or_queue(_cs75_action(), "entry")
    assert len(list(db.approvals)) == 1       # default-OFF switch keeps the human gate
    assert db.pending_orders == []


# ── kill switch: pure trip logic ─────────────────────────────────────────────
def test_killswitch_trips_on_loss_rate():
    reason = ks.evaluate({"closed": 20, "losers": 12, "realized_pnl": -50.0},
                         {"realized_pnl": 100.0}, 100_000)
    assert reason and "loss rate" in reason


def test_killswitch_loss_rate_needs_min_sample():
    # 3 losers out of 3 is a 100% loss rate but below the min sample (5):
    # the loss-rate rule must not trip on so few trades.
    reason = ks.evaluate({"closed": 3, "losers": 3, "realized_pnl": -30.0},
                         {"realized_pnl": -10.0}, 100_000, capital_loss_pct=0.99)
    # (capital test disabled via huge pct; underperf: -30 < -10 → still trips on (c))
    assert reason and "underperform" in reason


def test_killswitch_trips_on_capital_loss():
    reason = ks.evaluate({"closed": 3, "losers": 1, "realized_pnl": -2500.0},
                         {"realized_pnl": 0.0}, 100_000)
    assert reason and "equity" in reason


def test_killswitch_trips_on_cs75_underperformance():
    reason = ks.evaluate({"closed": 4, "losers": 1, "realized_pnl": -10.0},
                         {"realized_pnl": 50.0}, 100_000)
    assert reason and "underperform" in reason


def test_killswitch_no_trip_when_healthy():
    assert ks.evaluate({"closed": 20, "losers": 5, "realized_pnl": 500.0},
                       {"realized_pnl": 100.0}, 100_000) is None


def test_killswitch_capital_test_skipped_when_equity_unknown():
    # equity=None → can't evaluate the % test; loss-rate/underperf still apply.
    assert ks.evaluate({"closed": 2, "losers": 0, "realized_pnl": 200.0},
                       {"realized_pnl": 100.0}, None) is None


# ── kill switch: enforcement disables Alpha ──────────────────────────────────
class _StatsDB(StubDB):
    def __init__(self, stats):
        super().__init__()
        self._stats = stats

    async def strategy_window_stats(self, days: int = 7):
        return self._stats


class _EquityBroker:
    async def get_account_balances(self):
        return {"total_equity": 100_000.0}


async def test_enforce_disables_alpha_and_persists():
    db = _StatsDB({"HERMESALPHA": {"closed": 20, "losers": 13, "realized_pnl": -100.0},
                   "CS75": {"closed": 5, "losers": 1, "realized_pnl": 200.0}})
    cs = ControlState()
    tripped = await ks.enforce_alpha_killswitch(db, _EquityBroker(), cs, {})
    assert tripped is True
    assert cs.strategy_enabled["HERMESALPHA"] is False
    assert db.settings.get("strategy_hermesalpha_enabled") == "false"


async def test_enforce_noop_when_healthy():
    db = _StatsDB({"HERMESALPHA": {"closed": 20, "losers": 4, "realized_pnl": 500.0},
                   "CS75": {"realized_pnl": 100.0}})
    cs = ControlState()
    assert await ks.enforce_alpha_killswitch(db, _EquityBroker(), cs, {}) is False
    assert cs.strategy_enabled["HERMESALPHA"] is True


async def test_enforce_noop_when_already_disabled():
    db = _StatsDB({"HERMESALPHA": {"closed": 20, "losers": 20, "realized_pnl": -999.0},
                   "CS75": {"realized_pnl": 100.0}})
    cs = ControlState()
    cs.strategy_enabled["HERMESALPHA"] = False
    # Already off → no work, no redundant write.
    assert await ks.enforce_alpha_killswitch(db, _EquityBroker(), cs, {}) is False
    assert db.settings.get("strategy_hermesalpha_enabled") is None
