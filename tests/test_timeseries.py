import os
import shutil
import pytest
from pathlib import Path
from datetime import datetime, date, timedelta, time, timezone
import pandas as pd
import numpy as np

from hermes.db.models import HermesDB, Base, DailyBar, IntradayBar
from hermes.db.timeseries import TimeSeriesEngine


@pytest.fixture
def tmp_ts_dir(tmp_path):
    d = tmp_path / "ts_test_data"
    d.mkdir()
    yield d
    if d.exists():
        shutil.rmtree(d)


@pytest.fixture
def test_db():
    from sqlalchemy import JSON
    from sqlalchemy.dialects.postgresql import JSONB

    # Make SQLite database in memory / temp file
    db_file = "test_ts_fallback.db"
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except OSError:
            pass

    # Standard SQLite table PK/autoincrement fix
    for table in Base.metadata.tables.values():
        composite_pk = len(table.primary_key.columns) > 1
        if composite_pk:
            for col in table.primary_key.columns:
                if col.autoincrement:
                    col.autoincrement = False
        for col in table.columns:
            if isinstance(col.type, JSONB):
                col.type = JSON()

    db_instance = HermesDB(f"sqlite:///{db_file}")
    yield db_instance
    db_instance.engine.dispose()
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except OSError:
            pass


def test_save_and_load_daily_bars(tmp_ts_dir, test_db):
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
    
    engine.save_daily_bars(symbol, df)
    
    # 2. Verify loading works
    loaded = engine.daily_bars(symbol, lookback_days=10)
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
    engine.save_daily_bars(symbol, duplicate_df)
    
    loaded = engine.daily_bars(symbol, lookback_days=10)
    assert len(loaded) == 5
    assert loaded.iloc[-1]["close"] == 999.0


def test_last_price_and_price_on_date(tmp_ts_dir, test_db):
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
    engine.save_daily_bars(symbol, df)
    
    # Last Price
    assert engine.last_price(symbol) == 154.0
    
    # Price on specific Date (lookback inclusive)
    target_dt = datetime.strptime(dates[1].strftime("%Y-%m-%d"), "%Y-%m-%d")
    assert engine.get_price_on_date(symbol, target_dt) == 153.0
    
    # Prior date fallback
    target_dt_between = target_dt + timedelta(hours=12)
    assert engine.get_price_on_date(symbol, target_dt_between) == 153.0
    
    # Date before first bar
    target_dt_before = datetime.strptime(dates[0].strftime("%Y-%m-%d"), "%Y-%m-%d") - timedelta(days=1)
    assert engine.get_price_on_date(symbol, target_dt_before) is None


def test_save_and_load_intraday_bars(tmp_ts_dir, test_db):
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
    engine.save_intraday_bars(symbol, df)
    
    loaded = engine.intraday_bars(symbol, lookback_days=10)
    assert len(loaded) == 5
    assert list(loaded.columns) == ["open", "high", "low", "close", "volume"]


def test_sql_fallback_migration_daily(tmp_ts_dir, test_db):
    # Setup: insert daily bars directly into SQL database tables
    symbol = "NFLX"
    ts_val1 = datetime.utcnow() - timedelta(days=2)
    ts_val2 = datetime.utcnow() - timedelta(days=1)
    
    with test_db.Session() as s:
        b1 = DailyBar(ts=ts_val1, symbol=symbol, open=400.0, high=410.0, low=390.0, close=405.0, volume=5000, vwap_close=402.0)
        b2 = DailyBar(ts=ts_val2, symbol=symbol, open=405.0, high=415.0, low=395.0, close=412.0, volume=6000, vwap_close=410.0)
        s.add_all([b1, b2])
        s.commit()
        
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
    
    # Verify file does not exist initially
    path = engine._daily_path(symbol)
    assert not path.exists()
    
    # Query via daily_bars: triggers fallback SQL query & disk migration
    loaded = engine.daily_bars(symbol, lookback_days=10)
    assert loaded is not None
    assert len(loaded) == 2
    
    # Confirm CSV is now written to disk
    assert path.exists()
    
    # Verify values inside CSV are correct
    csv_df = pd.read_csv(path)
    assert len(csv_df) == 2
    assert csv_df.iloc[-1]["close"] == 412.0


def test_sql_fallback_migration_intraday(tmp_ts_dir, test_db):
    symbol = "NVDA"
    ts_val = datetime.utcnow() - timedelta(hours=5)
    
    with test_db.Session() as s:
        ib = IntradayBar(ts=ts_val, symbol=symbol, open=800.0, high=810.0, low=790.0, close=805.0, volume=100)
        s.add(ib)
        s.commit()
        
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
    path = engine._intraday_path(symbol)
    assert not path.exists()
    
    loaded = engine.intraday_bars(symbol, lookback_days=1)
    assert len(loaded) == 1
    assert path.exists()
    
    csv_df = pd.read_csv(path)
    assert len(csv_df) == 1
    assert csv_df.iloc[0]["close"] == 805.0


def test_get_total_bars_count(tmp_ts_dir, test_db):
    engine = TimeSeriesEngine(test_db, root_path=tmp_ts_dir)
    
    # 0 counts on empty
    daily, intra = engine.get_total_bars_count()
    assert daily == 0
    assert intra == 0
    
    # Save some data
    base_date = datetime.utcnow() - timedelta(days=5)
    dates = pd.date_range(base_date.strftime("%Y-%m-%d"), periods=3, freq="D")
    df = pd.DataFrame({"ts": dates, "close": [10.0, 11.0, 12.0]})
    
    engine.save_daily_bars("SYM1", df)
    engine.save_daily_bars("SYM2", df)
    engine.save_intraday_bars("SYM1", df)
    
    daily, intra = engine.get_total_bars_count()
    assert daily == 6  # 2 symbols * 3 bars
    assert intra == 3  # 1 symbol * 3 bars
