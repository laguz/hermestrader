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


from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import CreditSpreads75

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
async def test_cs75_execute_entries_emits_actions_for_empty_book():
    s, broker, db = _build(
        CreditSpreads75,
        broker_kwargs={"expirations": _expirations_for(40, 45)},
        config={"cs75_width": 5.0, "cs75_target_lots": 1, "cs75_max_lots": 1},
    )
    actions = await s.execute_entries(["AAPL"])
    # The synthetic chain has POP-rich support/resistance levels at $90/$110
    # with delta in the 0.05–0.40 band, so both sides should plan.
    assert len(actions) >= 1
    for a in actions:
        assert a.tag == "HERMES_CS75"
        assert a.order_class == "multileg"
        assert a.strategy_params.get("side_type") in {"put", "call"}


async def test_cs75_manage_positions_takes_profit_at_50pct_for_mid_dte():
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
    actions = await s.manage_positions()
    assert any("HERMES_CS75_CLOSE" in a.tag for a in actions)


async def test_cs75_close_limit_is_capped_at_spread_width():
    # A W-wide credit spread can never be worth more than W to close. The
    # stale-quote TIME-EXIT path prices the close off the width itself, so
    # width * 1.05 (= 5.25 on a 5-wide) must be clamped down to 5.00 — the bot
    # must never bid above the width to close.
    s, _broker, _ = _build(CreditSpreads75)
    trade = make_trade("CS75", "AAPL", width=5.0)

    capped = s._close_action(trade, debit=5.0, reason="TIME-EXIT")
    assert capped.price == 5.0                       # 5.25 clamped to width

    # Below the cap, the 5% marketability buffer still applies normally.
    normal = s._close_action(trade, debit=1.00, reason="TP-50")
    assert normal.price == 1.05


