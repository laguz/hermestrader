"""Unit tests for HermesAlpha — Hermes's self-directed strategy.

HermesAlpha asks the overseer (LLM) to pick a setup *intent*, then resolves
it into real legs against the live chain and clamps every numeric to a safe
range. These tests drive a real ``HermesOverseer`` with a fake LLM so the
strategy ↔ overseer hand-off is exercised end to end, offline.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from hermes.service1_agent.core import (
    IronCondorBuilder, MoneyManager, TradeAction,
)
from hermes.service1_agent.overseer import HermesOverseer
from hermes.service1_agent.strategies import HermesAlpha

from ._stubs import StubBroker, StubDB, make_trade


class _FakeLLM:
    def __init__(self, reply: str):
        self.reply = reply

    def chat(self, messages, images=None):
        return self.reply


def _build(reply: Optional[str], *, config: Optional[Dict[str, Any]] = None,
           broker: Optional[StubBroker] = None) -> tuple[HermesAlpha, StubBroker, StubDB]:
    broker = broker or StubBroker()
    db = StubDB()
    cfg = config or {}
    mm = MoneyManager(broker, db, cfg)
    ic = IronCondorBuilder(mm)
    overseer = None
    if reply is not None:
        overseer = HermesOverseer(_FakeLLM(reply), db, vision_enabled=False,
                                  autonomy="autonomous")
    strat = HermesAlpha(broker, db, mm, ic, cfg, overseer=overseer)
    return strat, broker, db


def _intent(**over) -> str:
    base = {"verdict": "OPEN", "symbol": "AAPL", "side": "put",
            "target_delta": 0.2, "dte": 30, "width": 2, "lots": 1,
            "rationale": "support holds"}
    base.update(over)
    return json.dumps(base)


# ── entry: guards ─────────────────────────────────────────────────────────────
async def test_no_overseer_stands_down():
    strat, _b, _db = _build(None)
    assert await strat.execute_entries(["AAPL"]) == []


async def test_empty_universe_stands_down():
    strat, _b, _db = _build(_intent())
    assert await strat.execute_entries([]) == []


async def test_pass_verdict_stands_down():
    strat, _b, _db = _build('{"verdict":"PASS"}')
    assert await strat.execute_entries(["AAPL"]) == []


async def test_position_cap_blocks_new_entries():
    strat, _b, db = _build(_intent(), config={"alpha_max_positions": 1})
    db.set_open_trades("HermesAlpha", [make_trade("HermesAlpha", "MSFT", trade_id=1)])
    actions = await strat.execute_entries(["AAPL"])
    assert actions == []
    assert any("position cap reached" in log for log in strat.execution_logs)


async def test_duplicate_side_on_symbol_skipped():
    strat, _b, db = _build(_intent(symbol="AAPL", side="put"))
    db.set_open_trades("HermesAlpha",
                       [make_trade("HermesAlpha", "AAPL", side_type="put", trade_id=1)])
    assert await strat.execute_entries(["AAPL"]) == []


# ── entry: happy path + clamping ─────────────────────────────────────────────
async def test_valid_intent_builds_ai_authored_spread():
    strat, _b, _db = _build(_intent(symbol="aapl", side="put",
                                    target_delta=0.2, dte=30, width=2, lots=1))
    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) == 1
    a = actions[0]
    assert isinstance(a, TradeAction)
    assert a.strategy_id == "HermesAlpha"
    assert a.symbol == "AAPL"
    assert a.side == "sell"
    assert a.order_type == "credit"
    assert a.ai_authored is True
    assert a.ai_rationale == "support holds"
    assert a.price > 0
    sides = sorted(leg["side"] for leg in a.legs)
    assert sides == ["buy_to_open", "sell_to_open"]
    assert a.tag == "HERMES_HermesAlpha"
    assert a.strategy_params["side_type"] == "put"


async def test_universe_spans_all_strategy_watchlists():
    # NVDA is on CS75's watchlist only; the engine hands HermesAlpha nothing.
    # HermesAlpha must still be able to pick it from the desk-wide union.
    strat, _b, db = _build(_intent(symbol="NVDA", side="put"))
    db.set_watchlist("CS75", ["NVDA"])
    actions = await strat.execute_entries([])
    assert len(actions) == 1
    assert actions[0].symbol == "NVDA"


async def test_pick_outside_any_watchlist_rejected():
    # GOOG is on no watchlist anywhere — out of universe, must be refused.
    strat, _b, db = _build(_intent(symbol="GOOG", side="put"))
    db.set_watchlist("CS75", ["NVDA"])
    assert await strat.execute_entries(["AAPL"]) == []


async def test_out_of_range_intent_is_clamped():
    # Absurd LLM values must be clamped to the hard safety bounds, not trusted.
    strat, _b, _db = _build(_intent(target_delta=0.99, dte=999, width=999, lots=99),
                            config={"alpha_max_lots": 1})
    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) == 1
    a = actions[0]
    assert a.width <= HermesAlpha.WIDTH_MAX     # width clamped to 10
    assert all(leg["quantity"] == 1 for leg in a.legs)   # lots clamped to max_lots


async def test_low_credit_setup_rejected():
    strat, _b, db = _build(_intent(width=1), config={"alpha_min_credit_pct": 5.0})
    # An impossible min-credit (500% of width) rejects every spread.
    assert await strat.execute_entries(["AAPL"]) == []
    assert any("< min" in log for log in strat.execution_logs)


# ── entry: buying-power gate ──────────────────────────────────────────────────
async def test_zero_buying_power_blocks_entry():
    # A valid setup that the account cannot margin must not be sent — the
    # MoneyManager scales it to 0 lots and logs the reason.
    broker = StubBroker(option_buying_power=0.0)
    strat, _b, db = _build(_intent(), broker=broker)
    assert await strat.execute_entries(["AAPL"]) == []
    assert any("BLOCKED" in log for log in db.logs)


async def test_buying_power_caps_lots():
    # Enough BP for exactly one 2-wide spread ($200 margin), but the LLM asks
    # for 3 — the MoneyManager scales the order down rather than overcommit.
    broker = StubBroker(option_buying_power=200.0)
    strat, _b, _db = _build(_intent(width=2, lots=3),
                            broker=broker, config={"alpha_max_lots": 3})
    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) == 1
    assert all(leg["quantity"] == 1 for leg in actions[0].legs)


# ── manage_positions: bounded backstop ────────────────────────────────────────
async def test_backstop_take_profit_closes_position():
    # Default stub quotes (bid 99.95 / ask 100.05) give a near-zero close
    # debit — well under the TP floor — so the backstop closes the spread.
    strat, _b, db = _build(None)
    db.set_open_trades("HermesAlpha",
                       [make_trade("HermesAlpha", "AAPL", entry_credit=1.50,
                                   width=5.0, trade_id=1)])
    actions = await strat.manage_positions()
    assert len(actions) == 1
    a = actions[0]
    assert a.order_type == "debit"
    assert a.strategy_params["trade_id"] == 1
    assert "TP" in a.tag
    sides = sorted(leg["side"] for leg in a.legs)
    assert sides == ["buy_to_close", "sell_to_close"]


async def test_backstop_stop_loss_closes_position():
    broker = StubBroker()
    trade = make_trade("HermesAlpha", "AAPL", entry_credit=1.50, width=5.0, trade_id=2)

    def quote(symbols: str):
        # short_ask − long_bid = 4.6 ≥ 3× entry credit (4.5) → SL, and ≤ width.
        out = []
        for s in symbols.split(","):
            s = s.strip()
            if s == trade["short_leg"]:
                out.append({"symbol": s, "bid": 4.5, "ask": 5.0})
            else:
                out.append({"symbol": s, "bid": 0.4, "ask": 0.6})
        return out

    broker.get_quote = quote
    strat, _b, db = _build(None, broker=broker)
    strat.is_morning_unreliable = lambda: False
    db.set_open_trades("HermesAlpha", [trade])
    actions = await strat.manage_positions()
    assert len(actions) == 1
    assert "SL" in actions[0].tag


async def test_backstop_near_expiry_closes_position():
    strat, _b, db = _build(None, config={"alpha_close_dte": 1})
    db.set_open_trades("HermesAlpha",
                       [make_trade("HermesAlpha", "AAPL", days_to_expiry=0, trade_id=3)])
    actions = await strat.manage_positions()
    assert len(actions) == 1
    assert "EXPIRY" in actions[0].tag
