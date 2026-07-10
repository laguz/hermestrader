"""Regression tests for DS0 (priority-6, 0 DTE S/R-fade debit spreads).

Pins the rule set of docs/ds0_spec.md against stub broker / stub DB:

- Entries: trigger band on the touched level, the 3m POP ≥ 0.75 gate, the
  $0.10 debit cap with closest-to-the-money pair selection, the 14:00 ET
  cutoff, the one-shot-per-side-per-day gate, and the "both sides can arm on
  the same day" contract. Actions must be debit multileg day-limits tagged
  ``HERMES_DS0`` carrying an approval-TTL stamp.
- Empty own-watchlist means idle — DS0 must never trade the engine-wide
  default watchlist fallback (SPY/IWM there are perfectly valid 0DTE
  symbols the operator never armed).
- Management: the $0.40 TP close is placed exactly when the fill is visible
  (both legs held at the broker); the 3:00 PM sweep closes marks above entry
  cost and rides marks at/below it; a CLOSING trade's sweep close carries
  ``replace_broker_order_id`` for its resting TP; the 3:50 assignment guard
  fires on strike proximity regardless of mark and can be disarmed.
- Executor contracts DS0 introduced: ``valid_until`` expiry in
  ``_execute_approved_action`` (a stale approval must NOT reach the broker)
  and cancel-or-abort for ``replace_broker_order_id`` in
  ``_execute_or_queue`` (a failed cancel must NOT double-close).
- First debit-opening strategy: ``_trade_dict`` must expose ``entry_debit``
  (0.0 preserved, not coerced), and the POP engine must degrade to the
  linear ``1-|delta|`` path at ``dte=0`` rather than misbehaving.
"""
from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone

from hermes.service1_agent.core import (
    CascadingEngine, IronCondorBuilder, MoneyManager, TradeAction,
)
from hermes.service1_agent.strategies import DebitSpreads0DTE
from hermes.service1_agent.agent_approvals import _execute_approved_action

from ._stubs import StubBroker, StubDB, make_trade, _et_today

SYM = "QQQ"

# 2026-07-10 is an EDT date: ET = UTC-4.
_UTC = timezone.utc


def _utc_at_et(hour: int, minute: int = 0) -> datetime:
    """A UTC datetime whose ET wall-clock time is hour:minute today (EDT)."""
    d = _et_today()
    return datetime(d.year, d.month, d.day, hour + 4, minute, tzinfo=_UTC)


def _opt(expiry: str, opt_type: str, strike: float, bid: float, ask: float,
         delta: float) -> dict:
    yymmdd = datetime.strptime(expiry, "%Y-%m-%d").strftime("%y%m%d")
    pc = "P" if opt_type == "put" else "C"
    occ = f"{SYM}{yymmdd}{pc}{int(round(strike * 1000)):08d}"
    if opt_type == "put":
        delta = -abs(delta)
    return {"symbol": occ, "option_type": opt_type, "strike": float(strike),
            "bid": bid, "ask": ask, "greeks": {"delta": float(delta)}}


def _entry_chain(expiry: str, *, gate_call_delta=0.20, gate_put_delta=0.20):
    """Both sides priced so the closest affordable pair is deterministic.

    Puts (spot 100): 99/98 pair costs 0.14 (too rich), 98/97 costs 0.08.
    Calls: 101/102 costs 0.14, 102/103 costs 0.08. The gate strikes are the
    options nearest the levels (call 101 for resistance, put 99 for support).
    """
    return [
        _opt(expiry, "put", 99.0, 0.28, 0.32, gate_put_delta),
        _opt(expiry, "put", 98.0, 0.14, 0.18, 0.12),
        _opt(expiry, "put", 97.0, 0.06, 0.10, 0.08),
        _opt(expiry, "call", 101.0, 0.28, 0.32, gate_call_delta),
        _opt(expiry, "call", 102.0, 0.14, 0.18, 0.12),
        _opt(expiry, "call", 103.0, 0.06, 0.10, 0.08),
    ]


def _analysis(price: float, *, support=90.0, resistance=110.0):
    return {
        "symbol": SYM, "current_price": price,
        "current_vol": 0.20, "avg_vol": 0.20,
        "key_levels": [
            {"price": support, "type": "support", "strength": 5},
            {"price": resistance, "type": "resistance", "strength": 5},
        ],
        "samples": 100, "period": "3m",
    }


def _build_ds0(*, now_utc: datetime, expiry: str | None = None,
               analysis: dict | None = None, chain: list | None = None,
               db: StubDB | None = None, config: dict | None = None):
    expiry = expiry or _et_today().isoformat()
    broker = StubBroker(expirations=[expiry])
    broker.current_date = now_utc
    if analysis is not None:
        broker.analyze_symbol = lambda symbol, period="3m": dict(analysis)
    if chain is not None:
        broker.get_option_chains = lambda symbol, exp: list(chain)
    db = db or StubDB()
    db.set_watchlist("DS0", [SYM])
    cfg = {"ds0_target_lots": 1, "ds0_max_lots": 1}
    cfg.update(config or {})
    mm = MoneyManager(broker, db, cfg)
    s = DebitSpreads0DTE(broker=broker, db=db, money_manager=mm,
                         ic_builder=IronCondorBuilder(mm), config=cfg,
                         dry_run=False, overseer=None)
    return s, broker, db


# ── entries: triggers, gates and the order envelope ──────────────────────────
async def test_put_fade_on_resistance_touch():
    expiry = _et_today().isoformat()
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry,
        analysis=_analysis(100.0, resistance=100.2),
        chain=_entry_chain(expiry))
    actions = await s.execute_entries([SYM])
    assert len(actions) == 1
    a = actions[0]
    assert a.tag == "HERMES_DS0"
    assert a.order_class == "multileg" and a.order_type == "debit"
    assert a.side == "buy" and a.price == 0.10 and a.duration == "day"
    assert a.strategy_params["side_type"] == "put"
    assert a.expiry == expiry and a.dte == 0
    assert "valid_until" in a.strategy_params
    sides = {leg["side"] for leg in a.legs}
    assert sides == {"buy_to_open", "sell_to_open"}
    # Closest affordable pair: long 98 (bought), short 97 (sold).
    assert "P00098000" in a.strategy_params["long_leg"]
    assert "P00097000" in a.strategy_params["short_leg"]


async def test_call_fade_on_support_touch():
    expiry = _et_today().isoformat()
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry,
        analysis=_analysis(100.0, support=99.8),
        chain=_entry_chain(expiry))
    actions = await s.execute_entries([SYM])
    assert len(actions) == 1
    a = actions[0]
    assert a.strategy_params["side_type"] == "call"
    assert "C00102000" in a.strategy_params["long_leg"]
    assert "C00103000" in a.strategy_params["short_leg"]


async def test_both_sides_arm_on_the_same_day():
    expiry = _et_today().isoformat()
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry,
        analysis=_analysis(100.0, support=99.8, resistance=100.2),
        chain=_entry_chain(expiry))
    actions = await s.execute_entries([SYM])
    assert {a.strategy_params["side_type"] for a in actions} == {"put", "call"}


async def test_no_trigger_outside_band():
    expiry = _et_today().isoformat()
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry,
        analysis=_analysis(100.0),         # levels at 90 / 110 — 10% away
        chain=_entry_chain(expiry))
    assert await s.execute_entries([SYM]) == []


async def test_pop_gate_blocks_weak_level():
    expiry = _et_today().isoformat()
    # Gate delta 0.30 → linear POP 0.70 < the 0.75 floor.
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry,
        analysis=_analysis(100.0, resistance=100.2),
        chain=_entry_chain(expiry, gate_call_delta=0.30))
    assert await s.execute_entries([SYM]) == []


async def test_debit_cap_rejects_rich_chains():
    expiry = _et_today().isoformat()
    chain = [
        _opt(expiry, "put", 99.0, 0.58, 0.62, 0.20),
        _opt(expiry, "put", 98.0, 0.38, 0.42, 0.12),   # 99/98 = 0.20
        _opt(expiry, "put", 97.0, 0.24, 0.28, 0.08),   # 98/97 = 0.14
        _opt(expiry, "call", 101.0, 0.28, 0.32, 0.20),
    ]
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry,
        analysis=_analysis(100.0, resistance=100.2), chain=chain)
    assert await s.execute_entries([SYM]) == []


async def test_no_same_day_expiry_skips_symbol():
    later = (_et_today() + timedelta(days=7)).isoformat()
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=later,
        analysis=_analysis(100.0, resistance=100.2))
    assert await s.execute_entries([SYM]) == []


async def test_entry_cutoff_blocks_late_entries():
    expiry = _et_today().isoformat()
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(14, 30), expiry=expiry,   # 14:30 ET ≥ 14:00 cutoff
        analysis=_analysis(100.0, resistance=100.2),
        chain=_entry_chain(expiry))
    assert await s.execute_entries([SYM]) == []


async def test_one_shot_per_side_per_day():
    expiry = _et_today().isoformat()
    db = StubDB()
    # A CLOSED put trade today (a banked win) must still block the put side…
    closed = make_trade("DS0", SYM, side_type="put", short_strike=97.0,
                        long_strike=98.0, width=1.0, days_to_expiry=0)
    closed["status"] = "CLOSED"
    db.set_closed_trades("DS0", [closed])
    s, _, _ = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry, db=db,
        analysis=_analysis(100.0, support=99.8, resistance=100.2),
        chain=_entry_chain(expiry))
    actions = await s.execute_entries([SYM])
    # …while the untouched call side still arms.
    assert {a.strategy_params["side_type"] for a in actions} == {"call"}


async def test_empty_own_watchlist_means_idle():
    expiry = _et_today().isoformat()
    s, _, db = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry,
        analysis=_analysis(100.0, resistance=100.2),
        chain=_entry_chain(expiry))
    db.set_watchlist("DS0", [])
    # The engine's fallback would pass the global list here — DS0 must idle.
    assert await s.execute_entries([SYM, "SPY"]) == []


async def test_symbols_off_own_watchlist_are_ignored():
    expiry = _et_today().isoformat()
    s, broker, db = _build_ds0(
        now_utc=_utc_at_et(11, 0), expiry=expiry,
        analysis=_analysis(100.0, resistance=100.2),
        chain=_entry_chain(expiry))
    db.set_watchlist("DS0", ["IWM"])          # QQQ not armed
    assert await s.execute_entries([SYM]) == []


# ── management: TP on fill, sweep, guard ─────────────────────────────────────
def _ds0_trade(*, entry_debit=0.10, status="OPEN"):
    t = make_trade("DS0", SYM, side_type="put", short_strike=97.0,
                   long_strike=98.0, width=1.0, entry_credit=0.0,
                   days_to_expiry=0)
    t["entry_debit"] = entry_debit
    t["status"] = status
    return t


def _wire_quotes(broker, trade, *, long_q, short_q, spot=100.0):
    qmap = {trade["long_leg"]: long_q, trade["short_leg"]: short_q,
            SYM: {"last": spot}}

    def gq(symbols):
        out = []
        for s_ in symbols.split(","):
            s_ = s_.strip()
            if s_ in qmap:
                out.append({"symbol": s_, **qmap[s_]})
        return out
    broker.get_quote = gq


def _hold_positions(broker, trade):
    broker._positions = [{"symbol": trade["long_leg"], "quantity": 1},
                         {"symbol": trade["short_leg"], "quantity": -1}]


async def test_tp_close_placed_once_fill_is_visible():
    trade = _ds0_trade()
    db = StubDB()
    db.set_open_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(11, 0), db=db)
    _hold_positions(broker, trade)
    _wire_quotes(broker, trade, long_q={"bid": 0.10, "ask": 0.14},
                 short_q={"bid": 0.02, "ask": 0.04})
    actions = await s.manage_positions()
    assert len(actions) == 1
    a = actions[0]
    assert a.tag == "HERMES_DS0_CLOSE_TP"
    assert a.order_type == "credit" and a.side == "sell"
    assert a.price == 0.40
    sides = {leg["side"] for leg in a.legs}
    assert sides == {"sell_to_close", "buy_to_close"}
    assert a.strategy_params["trade_id"] == trade["id"]


async def test_no_tp_while_entry_still_resting():
    trade = _ds0_trade()
    db = StubDB()
    db.set_open_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(11, 0), db=db)
    # No broker positions → the entry day-limit hasn't filled.
    _wire_quotes(broker, trade, long_q={"bid": 0.10, "ask": 0.14},
                 short_q={"bid": 0.02, "ask": 0.04})
    assert await s.manage_positions() == []


async def test_sweep_banks_marks_above_entry_cost():
    trade = _ds0_trade(entry_debit=0.10)
    db = StubDB()
    db.set_open_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(15, 5), db=db)
    _hold_positions(broker, trade)
    # mid = 0.30 − 0.05 = 0.25 → between entry (0.10) and target (0.40).
    _wire_quotes(broker, trade, long_q={"bid": 0.28, "ask": 0.32},
                 short_q={"bid": 0.04, "ask": 0.06})
    actions = await s.manage_positions()
    assert len(actions) == 1
    a = actions[0]
    assert a.tag == "HERMES_DS0_CLOSE_SWEEP-3PM"
    assert a.price == 0.22                       # exec credit: 0.28 − 0.06


async def test_sweep_rides_marks_at_or_below_entry_cost():
    trade = _ds0_trade(entry_debit=0.10)
    db = StubDB()
    db.set_open_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(15, 5), db=db)
    _hold_positions(broker, trade)
    # mid = 0.11 − 0.03 = 0.08 ≤ entry cost → accepted loss, ride to expiry.
    _wire_quotes(broker, trade, long_q={"bid": 0.10, "ask": 0.12},
                 short_q={"bid": 0.02, "ask": 0.04})
    assert await s.manage_positions() == []


async def test_sweep_replaces_resting_tp_on_closing_trade():
    trade = _ds0_trade(entry_debit=0.10, status="CLOSING")
    db = StubDB()
    db.set_closing_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(15, 5), db=db)
    _hold_positions(broker, trade)
    broker._orders = [{
        "id": 42, "status": "open",
        "leg": [
            {"option_symbol": trade["long_leg"], "side": "sell_to_close"},
            {"option_symbol": trade["short_leg"], "side": "buy_to_close"},
        ],
    }]
    _wire_quotes(broker, trade, long_q={"bid": 0.28, "ask": 0.32},
                 short_q={"bid": 0.04, "ask": 0.06})
    actions = await s.manage_positions()
    assert len(actions) == 1
    assert actions[0].strategy_params["replace_broker_order_id"] == "42"


async def test_sweep_skips_closing_trade_when_no_resting_order_found():
    # The resting TP may have just filled — placing another close would
    # double-close, so DS0 must stand down for this pass.
    trade = _ds0_trade(entry_debit=0.10, status="CLOSING")
    db = StubDB()
    db.set_closing_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(15, 5), db=db)
    _hold_positions(broker, trade)
    _wire_quotes(broker, trade, long_q={"bid": 0.28, "ask": 0.32},
                 short_q={"bid": 0.04, "ask": 0.06})
    assert await s.manage_positions() == []


async def test_closing_trade_left_alone_before_sweep_time():
    trade = _ds0_trade(entry_debit=0.10, status="CLOSING")
    db = StubDB()
    db.set_closing_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(13, 0), db=db)
    _hold_positions(broker, trade)
    _wire_quotes(broker, trade, long_q={"bid": 0.28, "ask": 0.32},
                 short_q={"bid": 0.04, "ask": 0.06})
    assert await s.manage_positions() == []


async def test_assignment_guard_fires_on_strike_proximity():
    trade = _ds0_trade(entry_debit=0.10)
    db = StubDB()
    db.set_open_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(15, 55), db=db)
    _hold_positions(broker, trade)
    # Mark is at/below entry cost (sweep would ride) but spot sits at the
    # long strike → pin/assignment risk trumps the mark.
    _wire_quotes(broker, trade, long_q={"bid": 0.10, "ask": 0.12},
                 short_q={"bid": 0.02, "ask": 0.04}, spot=98.2)
    actions = await s.manage_positions()
    assert len(actions) == 1
    assert actions[0].tag == "HERMES_DS0_CLOSE_ASSIGN-GUARD"


async def test_assignment_guard_can_be_disarmed():
    trade = _ds0_trade(entry_debit=0.10)
    db = StubDB()
    db.settings["ds0_assignment_guard"] = "0"
    db.set_open_trades("DS0", [trade])
    s, broker, _ = _build_ds0(now_utc=_utc_at_et(15, 55), db=db)
    _hold_positions(broker, trade)
    _wire_quotes(broker, trade, long_q={"bid": 0.10, "ask": 0.12},
                 short_q={"bid": 0.02, "ask": 0.04}, spot=98.2)
    assert await s.manage_positions() == []


# ── executor contracts introduced for DS0 ────────────────────────────────────
def _entry_action(valid_until: str | None) -> TradeAction:
    sp = {"side_type": "put"}
    if valid_until is not None:
        sp["valid_until"] = valid_until
    return TradeAction(
        strategy_id="DS0", symbol=SYM, order_class="multileg",
        legs=[{"option_symbol": f"{SYM}260710P00098000", "side": "buy_to_open", "quantity": 1},
              {"option_symbol": f"{SYM}260710P00097000", "side": "sell_to_open", "quantity": 1}],
        price=0.10, side="buy", quantity=1, order_type="debit",
        tag="HERMES_DS0", strategy_params=sp,
        expiry=_et_today().isoformat(), width=1.0)


async def _seed_approval(db: StubDB, action: TradeAction) -> int:
    app_id = await db.queue_for_approval(dataclasses.asdict(action))
    for item in db.approvals:
        if item["id"] == app_id:
            item["status"] = "APPROVED"
            return app_id
    raise AssertionError("approval not seeded")


async def test_expired_approval_never_reaches_broker():
    db, broker = StubDB(), StubBroker()
    stale = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    app_id = await _seed_approval(db, _entry_action(stale))
    item = next(i for i in db.approvals if i["id"] == app_id)
    result = await _execute_approved_action(item, broker=broker, db=db)
    assert result == "expired"
    assert broker.placed == []
    assert item["status"] == "FAILED"
    assert "TTL" in (item["notes"] or "")


async def test_fresh_approval_executes(monkeypatch):
    import hermes.market_hours as mh
    monkeypatch.setattr(mh, "should_block_trades", lambda: (False, ""))
    db, broker = StubDB(), StubBroker()
    fresh = (datetime.now(timezone.utc) + timedelta(seconds=900)).isoformat()
    app_id = await _seed_approval(db, _entry_action(fresh))
    item = next(i for i in db.approvals if i["id"] == app_id)
    result = await _execute_approved_action(item, broker=broker, db=db)
    assert result == "executed"
    assert len(broker.placed) == 1


def _replace_close_action(oid: str) -> TradeAction:
    return TradeAction(
        strategy_id="DS0", symbol=SYM, order_class="multileg",
        legs=[{"option_symbol": f"{SYM}260710P00098000", "side": "sell_to_close", "quantity": 1},
              {"option_symbol": f"{SYM}260710P00097000", "side": "buy_to_close", "quantity": 1}],
        price=0.22, side="sell", quantity=1, order_type="credit",
        tag="HERMES_DS0_CLOSE_SWEEP-3PM",
        strategy_params={"trade_id": 1, "close_reason": "SWEEP-3PM",
                         "side_type": "put",
                         "replace_broker_order_id": oid})


async def test_replace_cancels_resting_order_then_places():
    db, broker = StubDB(), StubBroker()
    cancelled: list = []
    broker.cancel_order = lambda oid: cancelled.append(oid) or {"ok": True}
    engine = CascadingEngine(broker=broker, db=db, strategies=[],
                             approval_mode=False)
    await engine._execute_or_queue(_replace_close_action("42"), "management")
    assert cancelled == ["42"]
    assert len(broker.placed) == 1


async def test_replace_aborts_when_cancel_fails():
    db, broker = StubDB(), StubBroker()

    def _boom(oid):
        raise RuntimeError("order already filled")
    broker.cancel_order = _boom
    engine = CascadingEngine(broker=broker, db=db, strategies=[],
                             approval_mode=False)
    await engine._execute_or_queue(_replace_close_action("42"), "management")
    # Cancel-or-abort: the replacement must NOT go out (double-close risk).
    assert broker.placed == []


# ── strategy-set completeness ────────────────────────────────────────────────
def test_make_strategies_covers_every_registered_strategy():
    """The settings-changed rebuild and build() share make_strategies; this
    pins it against STRATEGY_PRIORITIES. The 2026-07-10 DS0 outage: the
    rebuild kept its own hardcoded five-strategy list, so the first settings
    event after startup (the ML loop fires one every ~10s) silently evicted
    DS0 from the engine before its first tick."""
    from hermes.common import STRATEGY_PRIORITIES
    from hermes.service1_agent.agent_construction import make_strategies

    broker, db = StubBroker(), StubDB()
    cfg: dict = {}
    mm = MoneyManager(broker, db, cfg)
    common = dict(broker=broker, db=db, money_manager=mm,
                  ic_builder=IronCondorBuilder(mm), config=cfg,
                  overseer=None, dry_run=False)
    strategies = make_strategies(common)
    assert {s.NAME for s in strategies} == set(STRATEGY_PRIORITIES)
    assert [s.PRIORITY for s in strategies] == sorted(
        STRATEGY_PRIORITIES[s.NAME] for s in strategies)


# ── first debit-opening strategy: persistence + POP engine pins ──────────────
def test_trade_dict_exposes_entry_debit_preserving_zero():
    from hermes.db.orm import Trade
    from hermes.db.repositories.trades import TradesRepository
    base = dict(id=1, strategy_id="DS0", symbol=SYM, side_type="put",
                short_leg="S", long_leg="L", lots=1, status="OPEN")
    d = TradesRepository._trade_dict(Trade(**base, entry_debit=0.10))
    assert d["entry_debit"] == 0.10
    d = TradesRepository._trade_dict(Trade(**base, entry_debit=0.0))
    assert d["entry_debit"] == 0.0          # falsy zero must survive
    d = TradesRepository._trade_dict(Trade(**base, entry_debit=None))
    assert d["entry_debit"] is None


def test_pop_engine_dte_zero_uses_linear_path():
    from hermes.ml.pop_engine import (
        FeatureVector, delta_implied_p_otm, set_pop_calibrator,
    )
    set_pop_calibrator(None)
    common = dict(delta=0.20, xgb_prob=0.5, current_vol=0.20, avg_vol=0.20,
                  protection_score=1.0, side="call", period="3M", symbol=SYM,
                  sigma=0.50)
    at_zero = delta_implied_p_otm(FeatureVector(**common, dte=0.0))
    no_dte = delta_implied_p_otm(FeatureVector(**common, dte=None))
    # dte=0 must fall back to the linear 1-|delta| form — no √t singularity.
    assert at_zero == no_dte == 0.80
