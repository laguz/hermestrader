import os
import logging
import time
from pathlib import Path
from datetime import datetime, date, timedelta, time as datetime_time, timezone
from typing import Optional, Dict, Any, Tuple, List
import pandas as pd
import numpy as np
import duckdb

logger = logging.getLogger("hermes.db.timeseries")


class TimeSeriesEngine:
    """Decoupled flat-file and columnar time-series engine for daily and intraday bars.
    
    Price history is persisted on disk in a DuckDB database file inside a 
    durable volume path to avoid relational database bloating, with automated
    CSV/SQL fallback migration logic.
    """

    def __init__(self, db_repo: Any, root_path: Optional[Path] = None):
        self.db = db_repo
        if root_path:
            self.root = root_path
        else:
            env_root = os.environ.get("HERMES_TS_ROOT")
            if env_root:
                self.root = Path(env_root)
            else:
                # Check if Docker persistent /data is present and writable
                data_dir = Path("/data")
                if data_dir.exists() and os.access(data_dir, os.W_OK):
                    self.root = data_dir / "timeseries"
                else:
                    self.root = Path.home() / ".hermes" / "timeseries"

        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "daily").mkdir(parents=True, exist_ok=True)
        (self.root / "intraday").mkdir(parents=True, exist_ok=True)
        
        self.db_path = self.root / "timeseries.db"
        self._init_db()

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        for i in range(20):
            try:
                return duckdb.connect(database=str(self.db_path))
            except duckdb.IOException as e:
                if "lock" in str(e).lower() or "database is locked" in str(e).lower():
                    time.sleep(0.05 + 0.05 * i)
                    continue
                raise
        raise TimeoutError(f"Could not acquire DuckDB connection at {self.db_path} - database is locked.")

    def _init_db(self):
        conn = self._get_conn()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_bars (
                    symbol VARCHAR,
                    ts TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    vwap_close DOUBLE,
                    PRIMARY KEY (symbol, ts)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS intraday_bars (
                    symbol VARCHAR,
                    ts TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    PRIMARY KEY (symbol, ts)
                )
            """)
        finally:
            conn.close()

    def _daily_path(self, symbol: str) -> Path:
        return self.root / "daily" / f"{symbol.upper()}.csv"

    def _intraday_path(self, symbol: str) -> Path:
        return self.root / "intraday" / f"{symbol.upper()}.csv"

    def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert daily bars for a symbol from a DataFrame into DuckDB."""
        if df.empty:
            return

        if df.index.name == 'ts' or 'ts' not in df.columns:
            reset_df = df.reset_index()
            if 'ts' not in reset_df.columns and 'index' in reset_df.columns:
                reset_df = reset_df.rename(columns={'index': 'ts'})
        else:
            reset_df = df.copy()

        reset_df["ts"] = pd.to_datetime(reset_df["ts"])
        cols_to_keep = ["ts", "open", "high", "low", "close", "volume", "vwap_close"]
        for col in cols_to_keep:
            if col not in reset_df.columns:
                reset_df[col] = np.nan

        write_df = reset_df[cols_to_keep].copy()
        write_df["ts"] = pd.to_datetime(write_df["ts"])
        write_df["open"] = write_df["open"].astype(float)
        write_df["high"] = write_df["high"].astype(float)
        write_df["low"] = write_df["low"].astype(float)
        write_df["close"] = write_df["close"].astype(float)
        write_df["volume"] = pd.to_numeric(write_df["volume"], errors='coerce').fillna(0).astype('int64')
        write_df["vwap_close"] = write_df["vwap_close"].astype(float)

        symbol_upper = symbol.upper()
        conn = self._get_conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO daily_bars SELECT ? as symbol, ts, open, high, low, close, volume, vwap_close FROM write_df",
                (symbol_upper,)
            )
        except Exception as exc:
            logger.exception("Failed to insert daily bars for %s: %s", symbol_upper, exc)
        finally:
            conn.close()

    def save_intraday_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert intraday bars for a symbol from a DataFrame into DuckDB."""
        if df.empty:
            return

        if df.index.name == 'ts' or 'ts' not in df.columns:
            reset_df = df.reset_index()
            if 'ts' not in reset_df.columns and 'index' in reset_df.columns:
                reset_df = reset_df.rename(columns={'index': 'ts'})
        else:
            reset_df = df.copy()

        reset_df["ts"] = pd.to_datetime(reset_df["ts"])
        cols_to_keep = ["ts", "open", "high", "low", "close", "volume"]
        for col in cols_to_keep:
            if col not in reset_df.columns:
                reset_df[col] = np.nan

        write_df = reset_df[cols_to_keep].copy()
        write_df["ts"] = pd.to_datetime(write_df["ts"])
        write_df["open"] = write_df["open"].astype(float)
        write_df["high"] = write_df["high"].astype(float)
        write_df["low"] = write_df["low"].astype(float)
        write_df["close"] = write_df["close"].astype(float)
        write_df["volume"] = pd.to_numeric(write_df["volume"], errors='coerce').fillna(0).astype('int64')

        symbol_upper = symbol.upper()
        conn = self._get_conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO intraday_bars SELECT ? as symbol, ts, open, high, low, close, volume FROM write_df",
                (symbol_upper,)
            )
        except Exception as exc:
            logger.exception("Failed to insert intraday bars for %s: %s", symbol_upper, exc)
        finally:
            conn.close()

    def _query_duckdb(self, table_name: str, symbol: str, lookback_days: int) -> Optional[pd.DataFrame]:
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)
        conn = self._get_conn()
        try:
            res = conn.execute(
                f"SELECT ts, open, high, low, close, volume" + 
                (", vwap_close" if table_name == "daily_bars" else "") +
                f" FROM {table_name} WHERE symbol = ? AND ts >= ? ORDER BY ts",
                (symbol, cutoff)
            )
            return res.fetchdf()
        except Exception as exc:
            logger.error("Failed to query DuckDB table %s for %s: %s", table_name, symbol, exc)
            return None
        finally:
            conn.close()

    def daily_bars(self, symbol: str, lookback_days: int = 400) -> Optional[pd.DataFrame]:
        """Fetch daily bars for a symbol from DuckDB, with legacy fallback migration."""
        symbol_upper = symbol.upper()
        df = self._query_duckdb("daily_bars", symbol_upper, lookback_days)

        if df is None or df.empty:
            # Check for legacy CSV
            path = self._daily_path(symbol_upper)
            if path.exists():
                try:
                    csv_df = pd.read_csv(path)
                    self.save_daily_bars(symbol_upper, csv_df)
                    df = self._query_duckdb("daily_bars", symbol_upper, lookback_days)
                except Exception as exc:
                    logger.error("Failed to migrate daily CSV for %s: %s", symbol_upper, exc)

            # If still empty, fall back to SQL database tables
            if df is None or df.empty:
                df = self._migrate_daily_from_sql(symbol_upper)
                if df is not None and not df.empty:
                    df = self._query_duckdb("daily_bars", symbol_upper, lookback_days)

        if df is None or df.empty:
            return None

        df["ts"] = pd.to_datetime(df["ts"])
        df = df.set_index("ts")
        for col in ["open", "high", "low", "close", "volume", "vwap_close"]:
            if col not in df.columns:
                df[col] = np.nan
        return df

    def intraday_bars(self, symbol: str, lookback_days: int = 10) -> pd.DataFrame:
        """Fetch intraday bars for a symbol from DuckDB, with legacy fallback migration."""
        symbol_upper = symbol.upper()
        df = self._query_duckdb("intraday_bars", symbol_upper, lookback_days)

        if df is None or df.empty:
            # Check for legacy CSV
            path = self._intraday_path(symbol_upper)
            if path.exists():
                try:
                    csv_df = pd.read_csv(path)
                    self.save_intraday_bars(symbol_upper, csv_df)
                    df = self._query_duckdb("intraday_bars", symbol_upper, lookback_days)
                except Exception as exc:
                    logger.error("Failed to migrate intraday CSV for %s: %s", symbol_upper, exc)

            # If still empty, fall back to SQL database tables
            if df is None or df.empty:
                df = self._migrate_intraday_from_sql(symbol_upper)
                if df is not None and not df.empty:
                    df = self._query_duckdb("intraday_bars", symbol_upper, lookback_days)

        if df is None or df.empty:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        df["ts"] = pd.to_datetime(df["ts"])
        df = df.set_index("ts")
        for col in ["open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                df[col] = np.nan
        return df

    def last_price(self, symbol: str) -> Optional[float]:
        """Fetch latest close price."""
        df = self.daily_bars(symbol, lookback_days=400)
        if df is None or df.empty:
            return None
        last_row = df.iloc[-1]
        val = last_row.get("close")
        return float(val) if val is not None and not pd.isna(val) else None

    def get_price_on_date(self, symbol: str, dt: Any) -> Optional[float]:
        """Fetch close price on or before target date."""
        if not dt:
            return None

        if isinstance(dt, datetime):
            dt_end = dt
        elif isinstance(dt, date):
            dt_end = datetime.combine(dt, datetime_time.max)
        else:
            dt_end = dt

        df = self.daily_bars(symbol, lookback_days=1000)
        if df is None or df.empty:
            return None

        df = df.sort_index()
        is_tz_aware = df.index.tz is not None

        if is_tz_aware:
            if dt_end.tzinfo is None:
                dt_end = dt_end.replace(tzinfo=timezone.utc)
            else:
                dt_end = dt_end.astimezone(timezone.utc)
        else:
            if dt_end.tzinfo is not None:
                dt_end = dt_end.astimezone(timezone.utc).replace(tzinfo=None)

        filtered = df[df.index <= dt_end]
        if filtered.empty:
            return None

        val = filtered.iloc[-1].get("close")
        return float(val) if val is not None and not pd.isna(val) else None

    def get_total_bars_count(self) -> Tuple[int, int]:
        """Count unique daily and intraday bar records stored in DuckDB."""
        daily_count = 0
        intraday_count = 0
        conn = self._get_conn()
        try:
            res_daily = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()
            if res_daily:
                daily_count = res_daily[0]
            res_intra = conn.execute("SELECT COUNT(*) FROM intraday_bars").fetchone()
            if res_intra:
                intraday_count = res_intra[0]
        except Exception as exc:
            logger.error("Failed to count bars in DuckDB: %s", exc)
        finally:
            conn.close()
        return daily_count, intraday_count

    def _migrate_daily_from_sql(self, symbol: str) -> Optional[pd.DataFrame]:
        from hermes.db.models import DailyBar
        try:
            with self.db.Session() as session:
                rows = (
                    session.query(DailyBar)
                    .filter(DailyBar.symbol == symbol)
                    .order_by(DailyBar.ts)
                    .all()
                )
                if not rows:
                    return None

                data = []
                for r in rows:
                    data.append({
                        'ts': r.ts,
                        'open': float(r.open) if r.open is not None else None,
                        'high': float(r.high) if r.high is not None else None,
                        'low': float(r.low) if r.low is not None else None,
                        'close': float(r.close) if r.close is not None else None,
                        'volume': int(r.volume) if r.volume is not None else None,
                        'vwap_close': float(r.vwap_close) if r.vwap_close is not None else None,
                    })
                df = pd.DataFrame(data)
                self.save_daily_bars(symbol, df)
                logger.info("Migrated daily bars for %s from SQL to timeseries DuckDB", symbol)
                return df
        except Exception as exc:
            logger.debug("Failed daily bars SQL migration fallback for %s: %s", symbol, exc)
            return None

    def _migrate_intraday_from_sql(self, symbol: str) -> Optional[pd.DataFrame]:
        from hermes.db.models import IntradayBar
        try:
            with self.db.Session() as session:
                rows = (
                    session.query(IntradayBar)
                    .filter(IntradayBar.symbol == symbol)
                    .order_by(IntradayBar.ts)
                    .all()
                )
                if not rows:
                    return None

                data = []
                for r in rows:
                    data.append({
                        'ts': r.ts,
                        'open': float(r.open) if r.open is not None else None,
                        'high': float(r.high) if r.high is not None else None,
                        'low': float(r.low) if r.low is not None else None,
                        'close': float(r.close) if r.close is not None else None,
                        'volume': int(r.volume) if r.volume is not None else None,
                    })
                df = pd.DataFrame(data)
                self.save_intraday_bars(symbol, df)
                logger.info("Migrated intraday bars for %s from SQL to timeseries DuckDB", symbol)
                return df
        except Exception as exc:
            logger.debug("Failed intraday bars SQL migration fallback for %s: %s", symbol, exc)
            return None
