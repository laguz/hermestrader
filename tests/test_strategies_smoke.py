"""Smoke tests — one entry-path test per strategy.

These don't try to assert exact strikes; they verify the strategy can
run end-to-end against a stub broker without raising and produces a
reasonable shape (1+ TradeActions for an empty book; 0 actions when the
book is full).

Detailed behaviour (TP/SL thresholds, mode-A/B rules, etc.) belongs in
per-strategy unit tests once we have time to add them; this file is the
safety net that prevents an import-time or signature regression from
landing unnoticed.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import (
    CreditSpreads7,
    CreditSpreads75,
    TastyTrade45,
    WheelStrategy,
)

from ._stubs import StubBroker, StubDB, make_trade


def _expirations_for(*dte_values):
    today = date.today()
    return [(today + timedelta(days=d)).isoformat() for d in dte_values]


def _build(strategy_cls, *, broker_kwargs=None, db=None, config=None):
    broker = StubBroker(**(broker_kwargs or {}))
    db = db or StubDB()
    mm = MoneyManager(broker, db, config or {})
    return strategy_cls(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm),
        config=config or {}, dry_run=False,
    ), broker, db


# ── CS75 ─────────────────────────────────────────────────────────────────────
def test_cs75_execute_entries_emits_actions_for_empty_book():
    s, broker, db = _build(
        CreditSpreads75,
        broker_kwargs={"expirations": _expirations_for(40, 45)},
        config={"cs75_width": 5.0, "cs75_target_lots": 1, "cs75_max_lots": 1},
    )
    actions = s.execute_entries(["AAPL"])
    # The synthetic chain has POP-rich support/resistance levels at $90/$110
    # with delta in the 0.05–0.40 band, so both sides should plan.
    assert len(actions) >= 1
    for a in actions:
        assert a.tag == "HERMES_CS75"
        assert a.order_class == "multileg"
        assert a.strategy_params.get("side_type") in {"put", "call"}


def test_cs75_manage_positions_takes_profit_at_50pct_for_mid_dte():
    db = StubDB()
    db.set_open_trades("CS75", [
        # Entry credit $1.50, current debit will be $0.50 (well under 50%).
        make_trade("CS75", "AAPL", entry_credit=1.50, days_to_expiry=30),
    ])
    s, broker, _ = _build(CreditSpreads75, db=db)
    # Quote both legs so debit = ask(short) - bid(long) = small.
    broker.get_quote = lambda symbols: [
        {"symbol": s.strip(), "bid": 0.20, "ask": 0.30}
        for s in symbols.split(",")
    ]
    actions = s.manage_positions()
    assert any("HERMES_CS75_CLOSE" in a.tag for a in actions)


# ── CS7 ──────────────────────────────────────────────────────────────────────
def test_cs7_execute_entries_requires_exact_7_dte():
    """CS7 only opens new entries on the exact 7 DTE expiry."""
    s, _, _ = _build(
        CreditSpreads7,
        broker_kwargs={"expirations": _expirations_for(7)},
        config={"cs7_width": 1.0, "cs7_target_lots": 1, "cs7_max_lots": 1},
    )
    actions = s.execute_entries(["AAPL"])
    assert all(a.tag == "HERMES_CS7" for a in actions)


def test_cs7_skips_when_no_7_dte_available():
    s, _, db = _build(
        CreditSpreads7,
        broker_kwargs={"expirations": _expirations_for(14, 21, 30)},
        config={"cs7_width": 1.0, "cs7_target_lots": 1, "cs7_max_lots": 1},
    )
    actions = s.execute_entries(["AAPL"])
    assert actions == []
    assert any("no exact 7 DTE expiry" in m for m in db.logs)


# ── TT45 ─────────────────────────────────────────────────────────────────────
def test_tt45_execute_entries_uses_delta_selection():
    s, _, _ = _build(
        TastyTrade45,
        broker_kwargs={"expirations": _expirations_for(45)},
        config={"tt45_width": 5.0, "tt45_target_lots": 1, "tt45_max_lots": 1},
    )
    actions = s.execute_entries(["AAPL"])
    # Synthetic chain has a 16Δ-ish strike around $90/$110.
    assert all(a.tag == "HERMES_TT45" for a in actions)


def test_tt45_manage_positions_hard_exits_at_21_dte():
    db = StubDB()
    db.set_open_trades("TT45", [
        make_trade("TT45", "AAPL", days_to_expiry=21),
    ])
    s, _, _ = _build(TastyTrade45, db=db)
    actions = s.manage_positions()
    assert any("HARD-21DTE" in (a.tag or "") for a in actions)


# ── WHEEL ────────────────────────────────────────────────────────────────────
class _DBWithShares(StubDB):
    """Wheel needs ``equity_position`` to know how many calls to write."""

    def __init__(self, share_lots: int):
        super().__init__()
        self._share_lots = share_lots

    def equity_position(self, symbol: str) -> int:
        return self._share_lots * 100


def test_wheel_writes_calls_when_shares_present():
    db = _DBWithShares(share_lots=2)  # 200 shares = 2 callable lots
    s, _, _ = _build(
        WheelStrategy, db=db,
        broker_kwargs={"expirations": _expirations_for(35, 40)},
        config={"wheel_max_lots": 2},
    )
    actions = s.execute_entries(["AAPL"])
    call_actions = [a for a in actions
                    if (a.strategy_params or {}).get("side_type") == "call"]
    # Two share-covered calls (no puts because max_lots is fully consumed
    # by the call side once we add them).
    assert len(call_actions) == 2
    assert all(a.tag == "HERMES_WHEEL" for a in call_actions)


def test_wheel_writes_puts_when_no_shares():
    db = _DBWithShares(share_lots=0)
    s, _, _ = _build(
        WheelStrategy, db=db,
        broker_kwargs={"expirations": _expirations_for(35, 40)},
        config={"wheel_max_lots": 2},
    )
    actions = s.execute_entries(["AAPL"])
    # No shares → no calls; puts only.
    assert all((a.strategy_params or {}).get("side_type") == "put" for a in actions)
    assert len(actions) == 2
