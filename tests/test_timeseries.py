from datetime import datetime, timedelta
import pandas as pd
import pytest

from hermes.db.timeseries import TimeSeriesEngine


@pytest.fixture
def timeseries_db(make_db):
    # schema=True provisions the raw bars_* hypertables the engine reads/writes.
    return make_db(schema=True)


async def test_save_and_load_daily_bars(timeseries_db):
    engine = TimeSeriesEngine(timeseries_db)
    symbol = "TSLA"

    # 1. Save new daily bars
    base_date = datetime.utcnow() - timedelta(days=5)
    dates = pd.date_range(base_date.strftime("%Y-%m-%d"), periods=5, freq="D")
    df = pd.DataFrame({
        "ts": dates,
        "open": [100.0, 101.0, 102.0, 103.0, 104.0],
        "high": [105.0, 106.0, 107.0, 108.0, 109.0],
        "low": [95.0, 96.0, 97.0, 98.0, 99.0],
        "close": [102.0, 103.0, 104.0, 105.0, 106.0],
        "volume": [1000, 1100, 1200, 1300, 1400],
        "vwap_close": [101.5, 102.5, 103.5, 104.5, 105.5]
    })

    await engine.save_daily_bars(symbol, df)

    # 2. Verify loading works (stable column order)
    loaded = await engine.daily_bars(symbol, lookback_days=10)
    assert loaded is not None
    assert len(loaded) == 5
    assert list(loaded.columns) == ["open", "high", "low", "close", "volume", "vwap_close"]

    # 3. Verify upsert (ON CONFLICT) keeps the latest write for a duplicate ts
    duplicate_df = pd.DataFrame({
        "ts": [dates[4]],  # Same ts as last row
        "open": [999.0],
        "high": [999.0],
        "low": [999.0],
        "close": [999.0],
        "volume": [9999],
        "vwap_close": [999.0]
    })
    await engine.save_daily_bars(symbol, duplicate_df)

    loaded = await engine.daily_bars(symbol, lookback_days=10)
    assert len(loaded) == 5
    assert loaded.iloc[-1]["close"] == 999.0


async def test_last_price_and_price_on_date(timeseries_db):
    engine = TimeSeriesEngine(timeseries_db)
    symbol = "AAPL"

    base_date = datetime.utcnow() - timedelta(days=5)
    dates = pd.date_range(base_date.strftime("%Y-%m-%d"), periods=3, freq="D")
    df = pd.DataFrame({
        "ts": dates,
        "open": [150.0, 151.0, 152.0],
        "high": [155.0, 156.0, 157.0],
        "low": [145.0, 146.0, 147.0],
        "close": [152.0, 153.0, 154.0],
        "volume": [1000, 1100, 1200]
    })
    await engine.save_daily_bars(symbol, df)

    # Last Price
    assert await engine.last_price(symbol) == 154.0

    # Price on specific Date (lookback inclusive)
    target_dt = datetime.strptime(dates[1].strftime("%Y-%m-%d"), "%Y-%m-%d")
    assert await engine.get_price_on_date(symbol, target_dt) == 153.0

    # Prior date fallback (mid-day rolls back to that day's close)
    target_dt_between = target_dt + timedelta(hours=12)
    assert await engine.get_price_on_date(symbol, target_dt_between) == 153.0

    # Date before first bar
    target_dt_before = datetime.strptime(dates[0].strftime("%Y-%m-%d"), "%Y-%m-%d") - timedelta(days=1)
    assert await engine.get_price_on_date(symbol, target_dt_before) is None


async def test_save_and_load_intraday_bars(timeseries_db):
    engine = TimeSeriesEngine(timeseries_db)
    symbol = "MSFT"

    base_date = datetime.utcnow() - timedelta(days=5)
    dates = pd.date_range(base_date.strftime("%Y-%m-%d 09:30:00"), periods=5, freq="min")
    df = pd.DataFrame({
        "ts": dates,
        "open": [200.0, 201.0, 202.0, 203.0, 204.0],
        "high": [205.0, 206.0, 207.0, 208.0, 209.0],
        "low": [195.0, 196.0, 197.0, 198.0, 199.0],
        "close": [202.0, 203.0, 204.0, 205.0, 206.0],
        "volume": [100, 110, 120, 130, 140]
    })
    await engine.save_intraday_bars(symbol, df)

    loaded = await engine.intraday_bars(symbol, lookback_days=10)
    assert len(loaded) == 5
    assert list(loaded.columns) == ["open", "high", "low", "close", "volume"]


async def test_intraday_bars_empty_returns_empty_frame(timeseries_db):
    engine = TimeSeriesEngine(timeseries_db)
    loaded = await engine.intraday_bars("NOPE", lookback_days=10)
    assert loaded is not None
    assert loaded.empty
    assert list(loaded.columns) == ["open", "high", "low", "close", "volume"]


async def test_daily_bars_missing_symbol_returns_none(timeseries_db):
    engine = TimeSeriesEngine(timeseries_db)
    assert await engine.daily_bars("NOPE", lookback_days=10) is None


async def test_get_total_bars_count(timeseries_db):
    engine = TimeSeriesEngine(timeseries_db)

    # 0 counts on empty
    daily, intra = await engine.get_total_bars_count()
    assert daily == 0
    assert intra == 0

    # Save some data
    base_date = datetime.utcnow() - timedelta(days=5)
    dates = pd.date_range(base_date.strftime("%Y-%m-%d"), periods=3, freq="D")
    df = pd.DataFrame({"ts": dates, "close": [10.0, 11.0, 12.0]})

    await engine.save_daily_bars("SYM1", df)
    await engine.save_daily_bars("SYM2", df)
    await engine.save_intraday_bars("SYM1", df)

    daily, intra = await engine.get_total_bars_count()
    assert daily == 6  # 2 symbols * 3 bars
    assert intra == 3  # 1 symbol * 3 bars


