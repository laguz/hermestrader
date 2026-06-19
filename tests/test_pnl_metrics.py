from __future__ import annotations

from datetime import date, datetime, timedelta
import pytest
import pandas as pd

from hermes.db.models import Trade

# ``db`` fixture (fresh throwaway Timescale DB) is provided by tests/conftest.py.


async def test_pnl_metrics_option_spreads(db):
    await db.watchlist.ensure_strategies({
        "CS7": 1,
        "CS75": 2,
        "TT45": 3,
        "WHEEL": 4
    })

    # CS7: FAIL threshold: < 5% (0.05) return, PASS threshold: >= 10% (0.10) return
    # We will create two trades: one FAIL, one PASS
    async with db.AsyncSession() as s:
        # Trade 1: CS7 FAIL
        # Width: 5.0, lots: 1, entry_credit: 1.50 -> risk_capital = 3.50 * 100 = 350
        # realized_pnl = 10.0 (return = 10/350 = 2.8% < 5% FAIL)
        t1 = Trade(
            id=1,
            strategy_id="CS7",
            symbol="AAPL",
            side_type="put",
            short_strike=100.0,
            long_strike=95.0,
            width=5.0,
            lots=1,
            entry_credit=1.50,
            status="CLOSED",
            pnl=10.0,
            closed_at=datetime.utcnow() - timedelta(days=5),
            opened_at=datetime.utcnow() - timedelta(days=10)
        )
        # Trade 2: CS7 PASS
        # Width: 5.0, lots: 1, entry_credit: 1.50 -> risk_capital = 3.50 * 100 = 350
        # realized_pnl = 50.0 (return = 50/350 = 14.2% >= 10% PASS)
        t2 = Trade(
            id=2,
            strategy_id="CS7",
            symbol="AAPL",
            side_type="put",
            short_strike=100.0,
            long_strike=95.0,
            width=5.0,
            lots=1,
            entry_credit=1.50,
            status="CLOSED",
            pnl=50.0,
            closed_at=datetime.utcnow() - timedelta(days=2),
            opened_at=datetime.utcnow() - timedelta(days=10)
        )
        s.add_all([t1, t2])
        await s.commit()
        
    metrics = await db.analytics.get_strategy_performance_metrics(days=30)
    cs7 = metrics["CS7"]
    assert cs7["closed_trades"] == 2
    assert cs7["passed"] == 1
    assert cs7["failed"] == 1
    assert cs7["status"] == "NEUTRAL"
    assert cs7["total_pnl"] == 60.0


async def test_pnl_metrics_cs75_and_tt45(db):
    await db.watchlist.ensure_strategies({
        "CS7": 1,
        "CS75": 2,
        "TT45": 3,
        "WHEEL": 4
    })

    # CS75 FAIL <= 7%, PASS >= 22%
    # TT45 FAIL <= 3%, PASS >= 5%
    async with db.AsyncSession() as s:
        # CS75 PASS
        # Width: 10.0, lots: 2, entry_credit: 2.0 -> risk_capital = (10 - 2) * 2 * 100 = 1600
        # realized_pnl = 400.0 (return = 400/1600 = 25% >= 22% PASS)
        t1 = Trade(
            id=1,
            strategy_id="CS75",
            symbol="MSFT",
            side_type="put",
            short_strike=300.0,
            long_strike=290.0,
            width=10.0,
            lots=2,
            entry_credit=2.0,
            status="CLOSED",
            pnl=400.0,
            closed_at=datetime.utcnow() - timedelta(days=4),
            opened_at=datetime.utcnow() - timedelta(days=20)
        )
        # TT45 FAIL
        # Width: 5.0, lots: 1, entry_credit: 1.0 -> risk_capital = (5 - 1) * 1 * 100 = 400
        # realized_pnl = 5.0 (return = 5/400 = 1.25% <= 3% FAIL)
        t2 = Trade(
            id=2,
            strategy_id="TT45",
            symbol="TSLA",
            side_type="call",
            short_strike=200.0,
            long_strike=205.0,
            width=5.0,
            lots=1,
            entry_credit=1.0,
            status="CLOSED",
            pnl=5.0,
            closed_at=datetime.utcnow() - timedelta(days=1),
            opened_at=datetime.utcnow() - timedelta(days=15)
        )
        s.add_all([t1, t2])
        await s.commit()
        
    metrics = await db.analytics.get_strategy_performance_metrics(days=30)
    
    assert metrics["CS75"]["passed"] == 1
    assert metrics["CS75"]["failed"] == 0
    assert metrics["CS75"]["status"] == "PASS"
    
    assert metrics["TT45"]["passed"] == 0
    assert metrics["TT45"]["failed"] == 1
    assert metrics["TT45"]["status"] == "FAIL"


async def test_pnl_metrics_wheel(make_db):
    # Needs the Timescale bars_* addendum: the test writes daily bars used to
    # value assigned legs at expiry.
    db = make_db(schema=True)
    await db.watchlist.ensure_strategies({
        "CS7": 1,
        "CS75": 2,
        "TT45": 3,
        "WHEEL": 4
    })

    # Symbol 1: AAPL (PASS)
    # - Put assigned: strike 100, lots 1. Stock price on expiry: 95.0. Option pnl = 100 (premium kept)
    # - Call assigned: strike 105, lots 1. Stock price on expiry: 110.0. Option pnl = 50 (premium kept)
    # Stock proceeds: bought at 100 (-10000), sold at 105 (+10500) -> stock cash flow = +500
    # Total PnL: 100 + 50 + 500 = 650 (positive -> PASS)
    
    # Symbol 2: TSLA (FAIL)
    # - Put assigned: strike 200, lots 1. Stock price on expiry: 190. Option pnl = 200.
    # Stock proceeds: bought at 200 (-20000). Current stock price is 150. No call assignment.
    # Net shares: 100. Stock cash flow: -20000. Stock value: 15000.
    # Total PnL: 200 - 20000 + 15000 = -4800 (negative -> FAIL)
    
    expiry_put_aapl = date.today() - timedelta(days=15)
    expiry_call_aapl = date.today() - timedelta(days=5)
    expiry_put_tsla = date.today() - timedelta(days=10)
    
    # Seed daily bars using timeseries engine (decoupled from relational DB)
    df_aapl = pd.DataFrame({
        "ts": [
            datetime.combine(expiry_put_aapl, datetime.min.time()),
            datetime.combine(expiry_call_aapl, datetime.min.time()),
            datetime.utcnow()
        ],
        "close": [95.0, 110.0, 110.0]
    })
    await db.ts_engine.save_daily_bars("AAPL", df_aapl)
    
    df_tsla = pd.DataFrame({
        "ts": [
            datetime.combine(expiry_put_tsla, datetime.min.time()),
            datetime.utcnow()
        ],
        "close": [190.0, 150.0]
    })
    await db.ts_engine.save_daily_bars("TSLA", df_tsla)

    async with db.AsyncSession() as s:
        # Trades
        # AAPL Put
        t1 = Trade(
            id=1,
            strategy_id="WHEEL",
            symbol="AAPL",
            side_type="put",
            short_strike=100.0,
            lots=1,
            entry_credit=1.00,
            status="CLOSED",
            pnl=100.0,
            expiry=expiry_put_aapl,
            close_reason="RECONCILED_BROKER_FLAT",
            closed_at=datetime.combine(expiry_put_aapl, datetime.min.time())
        )
        # AAPL Call
        t2 = Trade(
            id=2,
            strategy_id="WHEEL",
            symbol="AAPL",
            side_type="call",
            short_strike=105.0,
            lots=1,
            entry_credit=0.50,
            status="CLOSED",
            pnl=50.0,
            expiry=expiry_call_aapl,
            close_reason="RECONCILED_BROKER_FLAT",
            closed_at=datetime.combine(expiry_call_aapl, datetime.min.time())
        )
        # TSLA Put
        t3 = Trade(
            id=3,
            strategy_id="WHEEL",
            symbol="TSLA",
            side_type="put",
            short_strike=200.0,
            lots=1,
            entry_credit=2.00,
            status="CLOSED",
            pnl=200.0,
            expiry=expiry_put_tsla,
            close_reason="RECONCILED_BROKER_FLAT",
            closed_at=datetime.combine(expiry_put_tsla, datetime.min.time())
        )
        
        s.add_all([t1, t2, t3])
        await s.commit()
        
    metrics = await db.analytics.get_strategy_performance_metrics(days=30)
    wheel = metrics["WHEEL"]
    assert wheel["passed"] == 1
    assert wheel["failed"] == 1
    assert wheel["status"] == "NEUTRAL"
    
    # Check details
    aapl_details = [d for d in wheel["details"] if d["symbol"] == "AAPL"][0]
    tsla_details = [d for d in wheel["details"] if d["symbol"] == "TSLA"][0]
    
    assert aapl_details["option_pnl"] == 150.0
    assert aapl_details["stock_cash_flow"] == 500.0 # 10500 - 10000
    assert aapl_details["net_shares"] == 0
    assert aapl_details["total_pnl"] == 650.0
    assert aapl_details["outcome"] == "PASS"
    
    assert tsla_details["option_pnl"] == 200.0
    assert tsla_details["stock_cash_flow"] == -20000.0
    assert tsla_details["net_shares"] == 100
    assert tsla_details["current_spot"] == 150.0
    assert tsla_details["total_pnl"] == 200.0 - 20000.0 + 15000.0
    assert tsla_details["outcome"] == "FAIL"
