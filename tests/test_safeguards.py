from __future__ import annotations

import pytest
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

from hermes.service1_agent.strategies.cs75 import CreditSpreads75
from hermes.service1_agent.core import MoneyManager, IronCondorBuilder
from tests._stubs import StubBroker, StubDB, make_trade, make_chain

ET = ZoneInfo("America/New_York")


@pytest.mark.asyncio
async def test_stop_loss_width_safety_cap():
    """Verify that Stop Loss exits are blocked if debit is equal to or greater than the spread width."""
    # CS75 call spread, width = 5.00, entry_credit = 1.50. SL threshold is 1.50 * 2.5 = 3.75.
    trade = make_trade(
        strategy_id="CS75",
        symbol="TSLA",
        side_type="call",
        short_strike=445.0,
        long_strike=450.0,
        width=5.0,
        entry_credit=1.50,
        lots=1,
    )

    db = StubDB()
    db.set_open_trades("CS75", [trade])

    # Case A: Debit is $5.50 (>= width of $5.00). Should be blocked.
    broker_a = StubBroker()
    broker_a.current_date = datetime(2026, 6, 9, 15, 0, 0)  # 11:00 AM ET (not morning-blocked)
    broker_a.get_quote = lambda symbols: [
        {"symbol": trade["short_leg"], "bid": 20.00, "ask": 20.75},
        {"symbol": trade["long_leg"], "bid": 15.25, "ask": 16.00},
    ]

    mm_a = MoneyManager(broker_a, db, {})
    strategy_a = CreditSpreads75(
        broker=broker_a,
        db=db,
        money_manager=mm_a,
        ic_builder=IronCondorBuilder(mm_a),
        config={},
    )

    actions_a = await strategy_a.manage_positions()
    assert len(actions_a) == 0  # Blocked by width cap

    # Case B: mid_debit=$4.00 (>= 3.75 SL), exec_debit=$4.50 (< width $5.00). Should trigger close.
    broker_b = StubBroker()
    broker_b.current_date = datetime(2026, 6, 9, 15, 0, 0)
    broker_b.get_quote = lambda symbols: [
        {"symbol": trade["short_leg"], "bid": 9.50, "ask": 10.00},
        {"symbol": trade["long_leg"], "bid": 5.50, "ask": 6.00},
    ]

    mm_b = MoneyManager(broker_b, db, {})
    strategy_b = CreditSpreads75(
        broker=broker_b,
        db=db,
        money_manager=mm_b,
        ic_builder=IronCondorBuilder(mm_b),
        config={},
    )

    actions_b = await strategy_b.manage_positions()
    assert len(actions_b) == 1
    assert actions_b[0].tag == "HERMES_CS75_CLOSE_SL-2.5x"


@pytest.mark.asyncio
async def test_morning_pricing_guard():
    """Verify that exits in loss are deferred before 10:30 AM ET, but TP exits and afternoon exits work."""
    # CS75 call spread, width = 5.00, entry_credit = 1.50. SL threshold = 3.75.
    trade = make_trade(
        strategy_id="CS75",
        symbol="TSLA",
        side_type="call",
        short_strike=445.0,
        long_strike=450.0,
        width=5.0,
        entry_credit=1.50,
        lots=1,
    )

    db = StubDB()
    db.set_open_trades("CS75", [trade])

    # Case A: SL at 10:00 AM ET. mid_debit=$4.00, exec_debit=$4.50. Morning guard blocks.
    broker_a = StubBroker()
    # 10:00 AM ET is 14:00 UTC
    broker_a.current_date = datetime(2026, 6, 9, 14, 0, 0, tzinfo=ZoneInfo("UTC"))
    broker_a.get_quote = lambda symbols: [
        {"symbol": trade["short_leg"], "bid": 9.50, "ask": 10.00},
        {"symbol": trade["long_leg"], "bid": 5.50, "ask": 6.00},
    ]

    mm_a = MoneyManager(broker_a, db, {})
    strategy_a = CreditSpreads75(
        broker=broker_a,
        db=db,
        money_manager=mm_a,
        ic_builder=IronCondorBuilder(mm_a),
        config={},
    )
    
    actions_a = await strategy_a.manage_positions()
    assert len(actions_a) == 0  # Blocked by morning pricing guard

    # Case B: SL at 11:00 AM ET. mid_debit=$4.00 (>= 3.75), exec_debit=$4.50 (< width). Should fire.
    broker_b = StubBroker()
    # 11:00 AM ET is 15:00 UTC
    broker_b.current_date = datetime(2026, 6, 9, 15, 0, 0, tzinfo=ZoneInfo("UTC"))
    broker_b.get_quote = lambda symbols: [
        {"symbol": trade["short_leg"], "bid": 9.50, "ask": 10.00},
        {"symbol": trade["long_leg"], "bid": 5.50, "ask": 6.00},
    ]

    mm_b = MoneyManager(broker_b, db, {})
    strategy_b = CreditSpreads75(
        broker=broker_b,
        db=db,
        money_manager=mm_b,
        ic_builder=IronCondorBuilder(mm_b),
        config={},
    )

    actions_b = await strategy_b.manage_positions()
    assert len(actions_b) == 1
    assert actions_b[0].tag == "HERMES_CS75_CLOSE_SL-2.5x"

    # Case C: Firing Take Profit (debit $0.50 < entry $1.50) at 10:00 AM ET. Should trigger (in profit).
    broker_c = StubBroker()
    broker_c.current_date = datetime(2026, 6, 9, 14, 0, 0, tzinfo=ZoneInfo("UTC"))
    broker_c.get_quote = lambda symbols: [
        {"symbol": trade["short_leg"], "bid": 1.00, "ask": 1.10},
        {"symbol": trade["long_leg"], "bid": 0.60, "ask": 0.70},
    ]

    mm_c = MoneyManager(broker_c, db, {})
    strategy_c = CreditSpreads75(
        broker=broker_c,
        db=db,
        money_manager=mm_c,
        ic_builder=IronCondorBuilder(mm_c),
        config={},
    )

    actions_c = await strategy_c.manage_positions()
    assert len(actions_c) == 1
    assert actions_c[0].tag == "HERMES_CS75_CLOSE_TP-50"


@pytest.mark.asyncio
async def test_reentry_cooldown():
    """Verify that re-entry cooldown prevents opening a position on a recently closed symbol."""
    db = StubDB()
    db.watchlist.set_watchlist("CS75", ["TSLA"])
    # Seed prediction to satisfy POP analyze check
    db.set_prediction("TSLA", {"predicted_return": 0.05, "predicted_price": 105.0, "spot": 100.0})

    broker = StubBroker()
    # Current time: 12:00 UTC
    broker.current_date = datetime(2026, 6, 9, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

    # Case A: Closed 10 minutes (600s) ago. Cooldown = 1800s (30 mins). Should block.
    # 11:50 UTC
    closed_time_a = datetime(2026, 6, 9, 11, 50, 0, tzinfo=ZoneInfo("UTC"))
    db.set_latest_closed_trade_time("CS75", "TSLA", closed_time_a)

    mm = MoneyManager(broker, db, {})
    strategy_a = CreditSpreads75(
        broker=broker,
        db=db,
        money_manager=mm,
        ic_builder=IronCondorBuilder(mm),
        config={"reentry_cooldown_s": 1800},
    )

    actions_a = await strategy_a.execute_entries(["TSLA"])
    assert len(actions_a) == 0  # Blocked by cooldown

    # Case B: Closed 40 minutes (2400s) ago. Cooldown = 1800s. Should allow entry.
    # 11:20 UTC
    closed_time_b = datetime(2026, 6, 9, 11, 20, 0, tzinfo=ZoneInfo("UTC"))
    db.set_latest_closed_trade_time("CS75", "TSLA", closed_time_b)

    # Mock analyze_symbol response to avoid actual web call failure in test
    broker.analyze_symbol = lambda sym, period="6m": {
        "symbol": sym,
        "current_price": 100.0,
        "current_vol": 0.20,
        "avg_vol": 0.20,
        "key_levels": [
            {"price": 90.0,  "type": "support",    "strength": 5, "pop": 0.80},
            {"price": 110.0, "type": "resistance", "strength": 5, "pop": 0.80},
        ],
    }

    # Mock get_option_chains to return valid chain
    today = date(2026, 6, 9)
    target_expiry = (today + timedelta(days=40)).strftime("%Y-%m-%d")
    broker.get_option_expirations = lambda sym: [target_expiry]
    broker.get_option_chains = lambda sym, exp: make_chain(sym, exp, spot=100.0)

    actions_b = await strategy_a.execute_entries(["TSLA"])
    # CS75 should propose entry (since cooldown has expired)
    assert len(actions_b) > 0
