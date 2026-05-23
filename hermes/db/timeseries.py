import os
import logging
from pathlib import Path
from datetime import datetime, date, timedelta, time, timezone
from typing import Optional, Dict, Any, Tuple, List
import pandas as pd
import numpy as np

logger = logging.getLogger("hermes.db.timeseries")


class TimeSeriesEngine:
    """Decoupled flat-file time-series engine for daily and intraday bars.
    
    Price history is persisted on disk as compressed CSV files inside a 
    durable volume path to avoid relational database bloating.
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

    def _daily_path(self, symbol: str) -> Path:
        return self.root / "daily" / f"{symbol.upper()}.csv"

    def _intraday_path(self, symbol: str) -> Path:
        return self.root / "intraday" / f"{symbol.upper()}.csv"

    def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert daily bars for a symbol from a DataFrame."""
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

        new_df = reset_df[cols_to_keep].copy()

        path = self._daily_path(symbol)
        if path.exists():
            try:
                existing_df = pd.read_csv(path)
                existing_df["ts"] = pd.to_datetime(existing_df["ts"])
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=["ts"], keep="last")
                combined_df = combined_df.sort_values(by="ts")
                combined_df.to_csv(path, index=False)
            except Exception as exc:
                logger.exception("Failed to append daily bars for %s: %s", symbol, exc)
                new_df.sort_values(by="ts").to_csv(path, index=False)
        else:
            new_df.sort_values(by="ts").to_csv(path, index=False)

    def save_intraday_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert intraday bars for a symbol from a DataFrame."""
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

        new_df = reset_df[cols_to_keep].copy()

        path = self._intraday_path(symbol)
        if path.exists():
            try:
                existing_df = pd.read_csv(path)
                existing_df["ts"] = pd.to_datetime(existing_df["ts"])
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=["ts"], keep="last")
                combined_df = combined_df.sort_values(by="ts")
                combined_df.to_csv(path, index=False)
            except Exception as exc:
                logger.exception("Failed to append intraday bars for %s: %s", symbol, exc)
                new_df.sort_values(by="ts").to_csv(path, index=False)
        else:
            new_df.sort_values(by="ts").to_csv(path, index=False)

    def daily_bars(self, symbol: str, lookback_days: int = 400) -> Optional[pd.DataFrame]:
        """Fetch daily bars for a symbol with timezone-aware alignment."""
        path = self._daily_path(symbol)
        df = None
        if path.exists():
            try:
                df = pd.read_csv(path)
            except Exception as exc:
                logger.error("Failed to read daily CSV for %s: %s", symbol, exc)

        if df is None or df.empty:
            df = self._migrate_daily_from_sql(symbol)

        if df is None or df.empty:
            return None

        df["ts"] = pd.to_datetime(df["ts"])
        
        # Align cutoff timezone awareness
        if df["ts"].dt.tz is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        else:
            cutoff = datetime.utcnow() - timedelta(days=lookback_days)

        df = df[df["ts"] >= cutoff]
        if df.empty:
            return None

        df = df.set_index("ts")
        for col in ["open", "high", "low", "close", "volume", "vwap_close"]:
            if col not in df.columns:
                df[col] = np.nan
        return df

    def intraday_bars(self, symbol: str, lookback_days: int = 10) -> pd.DataFrame:
        """Fetch intraday bars for a symbol with timezone-aware alignment."""
        path = self._intraday_path(symbol)
        df = None
        if path.exists():
            try:
                df = pd.read_csv(path)
            except Exception as exc:
                logger.error("Failed to read intraday CSV for %s: %s", symbol, exc)

        if df is None or df.empty:
            df = self._migrate_intraday_from_sql(symbol)

        if df is None or df.empty:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        df["ts"] = pd.to_datetime(df["ts"])

        if df["ts"].dt.tz is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        else:
            cutoff = datetime.utcnow() - timedelta(days=lookback_days)

        df = df[df["ts"] >= cutoff]
        if df.empty:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

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
            dt_end = datetime.combine(dt, time.max)
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
        """Count unique daily and intraday bar records stored on disk."""
        daily_count = 0
        intraday_count = 0

        daily_dir = self.root / "daily"
        if daily_dir.exists():
            for p in daily_dir.glob("*.csv"):
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        daily_count += sum(1 for _ in f) - 1
                except Exception:
                    pass

        intraday_dir = self.root / "intraday"
        if intraday_dir.exists():
            for p in intraday_dir.glob("*.csv"):
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        intraday_count += sum(1 for _ in f) - 1
                except Exception:
                    pass

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
                logger.info("Migrated daily bars for %s from SQL to timeseries CSV", symbol)
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
                logger.info("Migrated intraday bars for %s from SQL to timeseries CSV", symbol)
                return df
        except Exception as exc:
            logger.debug("Failed intraday bars SQL migration fallback for %s: %s", symbol, exc)
            return None
