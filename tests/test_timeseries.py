import shutil
import pytest
import duckdb
from datetime import datetime, timedelta
import pandas as pd

from hermes.db.timeseries import TimeSeriesEngine


@pytest.fixture
def tmp_ts_dir(tmp_path):
    d = tmp_path / "ts_test_data"
    d.mkdir()
    yield d
    if d.exists():
        shutil.rmtree(d)


@pytest.fixture
def test_db(make_db):
    # schema=True provisions the raw bars_* tables / hypertables for the
    # Postgres time-series fallback path.
    return make_db(schema=True)


async def test_save_and_load_daily_bars(tmp_ts_dir, test_db):
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
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
    
    # 2. Verify loading works
    loaded = await engine.daily_bars(symbol, lookback_days=10)
    assert loaded is not None
    assert len(loaded) == 5
    assert list(loaded.columns) == ["open", "high", "low", "close", "volume", "vwap_close"]
    
    # 3. Verify deduplication / upsert logic (keep last)
    duplicate_df = pd.DataFrame({
        "ts": [dates[4]], # Same ts as last row
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


async def test_last_price_and_price_on_date(tmp_ts_dir, test_db):
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
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
    
    # Prior date fallback
    target_dt_between = target_dt + timedelta(hours=12)
    assert await engine.get_price_on_date(symbol, target_dt_between) == 153.0
    
    # Date before first bar
    target_dt_before = datetime.strptime(dates[0].strftime("%Y-%m-%d"), "%Y-%m-%d") - timedelta(days=1)
    assert await engine.get_price_on_date(symbol, target_dt_before) is None


async def test_save_and_load_intraday_bars(tmp_ts_dir, test_db):
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
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


async def test_get_total_bars_count(tmp_ts_dir, test_db):
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
    
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


async def test_csv_migration_daily(tmp_ts_dir, test_db):
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
    symbol = "CSV1"
    
    # 1. Create a legacy CSV file manually
    dates = pd.date_range("2026-05-01", periods=3, freq="D")
    df = pd.DataFrame({
        "ts": dates,
        "open": [10.0, 11.0, 12.0],
        "high": [15.0, 16.0, 17.0],
        "low": [9.0, 10.0, 11.0],
        "close": [12.0, 13.0, 14.0],
        "volume": [100, 110, 120],
        "vwap_close": [11.5, 12.5, 13.5]
    })
    path = engine._daily_path(symbol)
    df.to_csv(path, index=False)
    
    # Verify DuckDB is empty initially
    db_df = engine._query_duckdb("daily_bars", symbol, 100)
    assert db_df is None or db_df.empty
    
    # 2. Query daily_bars: should trigger CSV migration
    loaded = await engine.daily_bars(symbol, lookback_days=100)
    assert loaded is not None
    assert len(loaded) == 3
    
    # Verify DuckDB is populated now
    db_df = engine._query_duckdb("daily_bars", symbol, 100)
    assert len(db_df) == 3
    assert db_df.iloc[-1]["close"] == 14.0


def lock_worker(db_path, lock_evt, release_evt):
    import time
    conn = duckdb.connect(db_path)
    lock_evt.set()
    release_evt.wait(timeout=2.0)
    time.sleep(0.3)
    conn.close()


async def test_concurrent_lock_retry(tmp_ts_dir, test_db):
    import multiprocessing
    import threading
    import time as py_time
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
    symbol = "LOCK"
    
    lock_conn_event = multiprocessing.Event()
    release_event = multiprocessing.Event()
    db_path_str = str(engine.db_path)
    
    p = multiprocessing.Process(
        target=lock_worker,
        args=(db_path_str, lock_conn_event, release_event)
    )
    p.start()
    
    lock_conn_event.wait(timeout=2.0)
    
    df = pd.DataFrame({
        "ts": [datetime.utcnow()],
        "open": [100.0],
        "high": [101.0],
        "low": [99.0],
        "close": [100.0],
        "volume": [1000],
        "vwap_close": [100.0]
    })
    
    def delayed_release():
        py_time.sleep(0.2)
        release_event.set()
    threading.Thread(target=delayed_release).start()
    
    start_time = py_time.time()
    await engine.save_daily_bars(symbol, df)
    duration = py_time.time() - start_time
    
    assert duration >= 0.2
    
    loaded = await engine.daily_bars(symbol, lookback_days=10)
    assert len(loaded) == 1
    p.join()
