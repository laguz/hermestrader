import datetime
import pytest
from hermes.service1_agent.backtest_engine import (
    BacktestDatabase,
    BacktestBroker,
    BacktestController,
)
from hermes.service1_agent.strategies.tt45 import TastyTrade45
from tests._stubs import StubBroker

class DummyTimeSeriesEngine:
    async def get_price_on_date(self, symbol, dt):
        return 100.0
    async def daily_bars(self, symbol, lookback_days):
        import pandas as pd
        # Return a simple mock dataframe
        return pd.DataFrame({"close": [100.0] * 30})

@pytest.mark.asyncio
async def test_backtest_database():
    db = BacktestDatabase()
    
    # Logs
    await db.write_log("TEST", "Hello World", level="INFO")
    assert len(db.logs) == 1
    assert db.logs[0]["msg"] == "Hello World"
    
    # Settings
    await db.set_setting("foo", "bar")
    assert await db.get_setting("foo") == "bar"
    assert await db.get_setting("nonexistent", "default") == "default"
    
    # Open trades count starts at 0
    assert await db.count_open_contracts("TEST", "AAPL", "put", "2025-06-20") == 0

@pytest.mark.asyncio
async def test_backtest_broker():
    ts = DummyTimeSeriesEngine()
    broker = BacktestBroker(ts, start_balance=100000.0)
    
    # Balances
    bal = await broker.get_account_balances()
    assert bal["option_buying_power"] == 100000.0
    
    # Quote
    quotes = await broker.get_quote("AAPL")
    assert len(quotes) == 1
    assert quotes[0]["symbol"] == "AAPL"
    assert quotes[0]["last"] == 100.0
    
    # Expirations
    exp = await broker.get_option_expirations("AAPL")
    assert len(exp) == 8
    # Expirations must be Fridays (YYYY-MM-DD)
    first_exp = datetime.datetime.strptime(exp[0], "%Y-%m-%d").date()
    assert first_exp.weekday() == 4
    
    # Option Chains
    chain = await broker.get_option_chains("AAPL", exp[0])
    assert len(chain) > 0
    # verify option contracts contain greeks delta
    assert "delta" in chain[0]["greeks"]

@pytest.mark.asyncio
async def test_backtest_controller_run():
    ts = DummyTimeSeriesEngine()
    start_date = datetime.date(2025, 1, 6) # Monday
    end_date = datetime.date(2025, 1, 20) # 2 weeks later
    
    # Run backtest controller with TastyTrade45 strategy
    controller = BacktestController(
        strategies=[TastyTrade45],
        watchlist=["AAPL"],
        ts_engine=ts,
        start_date=start_date,
        end_date=end_date,
        start_balance=50000.0,
    )
    
    # Run the backtest simulation
    results = await controller.run()
    
    assert results["ticks_run"] == 11 # 11 weekdays in 2-week window
    assert results["final_balance"] >= 0


@pytest.mark.asyncio
async def test_backtest_clock_and_database():
    from hermes.utils import set_virtual_time, utc_now
    from hermes.db.models import Trade
    from sqlalchemy import select

    ts = DummyTimeSeriesEngine()
    start_date = datetime.date(2025, 1, 6)
    end_date = datetime.date(2025, 1, 20)
    
    controller = BacktestController(
        strategies=[TastyTrade45],
        watchlist=["AAPL"],
        ts_engine=ts,
        start_date=start_date,
        end_date=end_date,
        start_balance=50000.0,
    )

    # Set virtual time to a specific past date
    sim_dt = datetime.datetime(2025, 1, 10, 16, 0, 0)
    set_virtual_time(sim_dt)
    
    assert utc_now() == sim_dt

    # Let's insert a trade using the controller's database connection
    async with controller.db.AsyncSession() as session:
        trade = Trade(
            id=1,
            strategy_id="TT45",
            symbol="AAPL",
            side_type="put",
            lots=1,
            status="OPEN"
        )
        session.add(trade)
        await session.commit()

        # Query it back
        q = select(Trade).filter_by(symbol="AAPL")
        res = await session.execute(q)
        fetched = res.scalars().first()
        
        # Verify opened_at matches the virtual clock instead of system time!
        assert fetched.opened_at == sim_dt

    # Clean up virtual clock
    set_virtual_time(None)
    controller.cleanup_sync()

