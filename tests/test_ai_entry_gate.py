"""Unit tests for the AI-entry gate and the goal-aware parameter tuner.

The gate (``hermes.service1_agent.entry_gate.gate_ai_action``) re-derives the
same mechanical filters the rule-based strategies enforce — POP, delta band,
minimum credit, DTE window, and MoneyManager capacity — against live market
data, so an overseer-proposed trade can only fill if it clears the same bar.
It fails closed: anything it cannot validate is rejected.

The tuner (``HermesOverseer.propose_parameter_adjustments``) lets the overseer
nudge a bounded allow-list of live settings toward the operator's goal; it can
neither invent settings nor push a value past its sanctioned range.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from hermes.service1_agent.core import AsyncBrokerWrapper, MoneyManager, TradeAction
from hermes.service1_agent.entry_gate import gate_ai_action
from hermes.service1_agent.overseer import HermesOverseer

from ._stubs import StubBroker, StubDB


def _occ(symbol: str, expiry: date, pc: str, strike: float) -> str:
    return f"{symbol}{expiry.strftime('%y%m%d')}{pc}{int(round(strike * 1000)):08d}"


def _expiry(days: int = 30) -> date:
    return date.today() + timedelta(days=days)


def _spread_action(
    *,
    symbol: str = "AAPL",
    short_strike: float = 90.0,
    long_strike: float = 88.0,
    pc: str = "P",
    days: int = 30,
    qty: int = 1,
) -> TradeAction:
    exp = _expiry(days)
    return TradeAction(
        strategy_id="CS7", symbol=symbol, order_class="multileg",
        legs=[
            {"option_symbol": _occ(symbol, exp, pc, short_strike),
             "side": "sell_to_open", "quantity": qty},
            {"option_symbol": _occ(symbol, exp, pc, long_strike),
             "side": "buy_to_open", "quantity": qty},
        ],
        price=99.0,  # deliberately dishonest — the gate must re-price from quotes
        side="sell", quantity=1, order_type="credit", tag="HERMES_AI",
        ai_authored=True,
    )


def _gate_deps(*, option_buying_power: float = 100_000.0):
    broker = StubBroker(option_buying_power=option_buying_power)
    db = StubDB()
    wrapped = AsyncBrokerWrapper(broker, db)
    mm = MoneyManager(broker, db, {})
    return broker, db, wrapped, mm


# ── happy path: a valid put credit spread clears every gate ──────────────────
async def test_valid_put_spread_passes_gate():
    broker, db, wrapped, mm = _gate_deps()
    action = _spread_action(short_strike=90.0, long_strike=88.0)

    out, reason = await gate_ai_action(action, broker=wrapped, db=db, mm=mm)

    assert out is not None, reason
    assert out.ai_authored is True
    assert out.strategy_params.get("ai_gated") is True
    assert out.width == 2.0
    # Credit re-priced from live quotes (5.5 - 5.0), NOT the dishonest 99.0.
    assert out.price == 0.5
    assert out.quantity == 1
    assert all(leg["quantity"] == 1 for leg in out.legs)


# ── fail-closed: a naked single leg is not a defined-risk spread ─────────────
async def test_naked_single_leg_rejected():
    broker, db, wrapped, mm = _gate_deps()
    exp = _expiry(30)
    action = TradeAction(
        strategy_id="AI", symbol="AAPL", order_class="option",
        legs=[{"option_symbol": _occ("AAPL", exp, "P", 90.0),
               "side": "sell_to_open", "quantity": 1}],
        price=0.5, side="sell",
    )
    out, reason = await gate_ai_action(action, broker=wrapped, db=db, mm=mm)
    assert out is None
    assert "structure" in reason


# ── delta band ───────────────────────────────────────────────────────────────
async def test_short_delta_above_band_rejected():
    broker, db, wrapped, mm = _gate_deps()
    # Strike 99 put sits ~ATM → |delta| ≈ 0.48, above the 0.45 cap.
    action = _spread_action(short_strike=99.0, long_strike=97.0)
    out, reason = await gate_ai_action(action, broker=wrapped, db=db, mm=mm)
    assert out is None
    assert "delta" in reason


# ── minimum credit ────────────────────────────────────────────────────────────
async def test_credit_below_minimum_rejected():
    broker, db, wrapped, mm = _gate_deps()
    db.settings["ai_gate_min_credit_pct"] = "0.40"  # tighten so 0.5/2.0=25% fails
    action = _spread_action(short_strike=90.0, long_strike=88.0)
    out, reason = await gate_ai_action(action, broker=wrapped, db=db, mm=mm)
    assert out is None
    assert "credit" in reason


# ── DTE window ─────────────────────────────────────────────────────────────────
async def test_dte_outside_window_rejected():
    broker, db, wrapped, mm = _gate_deps()
    action = _spread_action(days=120)  # well beyond the 45-day default ceiling
    out, reason = await gate_ai_action(action, broker=wrapped, db=db, mm=mm)
    assert out is None
    assert "DTE" in reason


# ── POP: short strike too close to the money for any qualifying level ────────
async def test_pop_unprotected_strike_rejected():
    broker, db, wrapped, mm = _gate_deps()
    # Put short at 95 is ABOVE the only ≥75% POP support level (90) → not
    # protected. Delta at 95 (~0.375) is still inside the band, so this
    # isolates the POP gate.
    action = _spread_action(short_strike=95.0, long_strike=93.0)
    out, reason = await gate_ai_action(action, broker=wrapped, db=db, mm=mm)
    assert out is None
    assert "POP" in reason or "closer to spot" in reason


# ── capacity: no buying power → no AI entry ──────────────────────────────────
async def test_zero_capacity_rejected():
    broker, db, wrapped, mm = _gate_deps(option_buying_power=0.0)
    action = _spread_action(short_strike=90.0, long_strike=88.0)
    out, reason = await gate_ai_action(action, broker=wrapped, db=db, mm=mm)
    assert out is None
    assert "capacity" in reason


# ── tuner: clamps to bounds, ignores unknown keys, writes settings ───────────
class _TunerLLM:
    def __init__(self, reply: str):
        self.reply = reply

    def chat(self, messages, images=None):
        return self.reply


async def test_param_tuner_clamps_and_filters():
    db = StubDB()
    # cs7_dte bound is 5..10; 99 must clamp to 10. unknown_key is dropped.
    # ai_gate_min_pop bound is 0.60..0.95; 0.88 is in range and applied.
    reply = (
        '{"adjustments": {"cs7_dte": 99, "ai_gate_min_pop": 0.88, '
        '"unknown_key": 1, "max_orders_per_tick": 50}, '
        '"rationale": "CS7 recently failed; tighten."}'
    )
    o = HermesOverseer(_TunerLLM(reply), db, vision_enabled=False,
                       autonomy="autonomous")
    result = await o.propose_parameter_adjustments()

    assert result["applied"]["cs7_dte"] == 10        # clamped from 99
    assert result["applied"]["ai_gate_min_pop"] == 0.88
    assert db.settings["cs7_dte"] == "10"
    assert db.settings["ai_gate_min_pop"] == "0.88"
    # Out-of-allow-list keys never touch settings.
    assert "unknown_key" not in db.settings
    assert "max_orders_per_tick" not in db.settings
    skipped = " ".join(result["skipped"])
    assert "unknown_key" in skipped and "max_orders_per_tick" in skipped


async def test_param_tuner_advisory_is_noop():
    db = StubDB()
    o = HermesOverseer(_TunerLLM('{"adjustments": {"cs7_dte": 8}}'), db,
                       vision_enabled=False, autonomy="advisory")
    result = await o.propose_parameter_adjustments()
    assert result["applied"] == {}
    assert "cs7_dte" not in db.settings
