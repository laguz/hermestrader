import logging
from datetime import datetime, date, timedelta, time as datetime_time, timezone
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("hermes.db.timeseries")

# Column order returned by the read paths — kept stable because consumers
# (charts, ML feature engineering, analytics) assert on it.
_DAILY_COLS = ["open", "high", "low", "close", "volume", "vwap_close"]
_INTRADAY_COLS = ["open", "high", "low", "close", "volume"]


def _as_utc(dt: Any) -> Optional[datetime]:
    """Coerce a date/datetime to a tz-aware UTC ``datetime`` (or ``None``)."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        out = dt
    elif isinstance(dt, date):
        out = datetime.combine(dt, datetime_time.min)
    else:
        out = pd.to_datetime(dt).to_pydatetime()
    if out.tzinfo is None:
        return out.replace(tzinfo=timezone.utc)
    return out.astimezone(timezone.utc)


class TimeSeriesEngine:
    """Daily/intraday OHLCV bar store backed by the TimescaleDB hypertables.

    Reads and writes the ``bars_daily`` / ``bars_intraday`` hypertables (declared
    in ``hermes/db/schema.sql``) over the owning :class:`HermesDB`'s async engine,
    so price history lives in the same Postgres/TimescaleDB instance as every
    other piece of system state. The public API is async; callers either await it
    directly or go through ``HermesDB.timeseries`` (the repository delegators).
    """

    def __init__(self, db_repo: Any):
        if db_repo is None:
            raise ValueError("TimeSeriesEngine requires a HermesDB with an async engine")
        self.db = db_repo

    @property
    def _engine(self):
        return self.db.async_engine

    # ---- write helpers ------------------------------------------------------

    @staticmethod
    def _normalize_for_write(symbol: str, df: pd.DataFrame, cols: List[str]) -> List[Dict[str, Any]]:
        """Turn a bars DataFrame into upsert-ready row dicts.

        Accepts ``ts`` as either the index or a column, fills missing OHLCV
        columns with NaN, coerces types, and stamps each row with the symbol and
        a tz-aware UTC timestamp.
        """
        if df is None or df.empty:
            return []

        if df.index.name == "ts" and "ts" in df.columns:
            # Both the index and a column are named "ts" — reset_index() would
            # raise ValueError on the resulting duplicate column, so drop the
            # index and keep the existing "ts" column.
            reset = df.reset_index(drop=True)
        elif df.index.name == "ts" or "ts" not in df.columns:
            reset = df.reset_index()
            if "ts" not in reset.columns and "index" in reset.columns:
                reset = reset.rename(columns={"index": "ts"})
        else:
            reset = df.copy()

        reset["ts"] = pd.to_datetime(reset["ts"])
        for col in cols:
            if col not in reset.columns:
                reset[col] = float("nan")

        symbol_upper = symbol.upper()
        rows: List[Dict[str, Any]] = []
        for _, r in reset.iterrows():
            row: Dict[str, Any] = {"symbol": symbol_upper, "ts": _as_utc(r["ts"])}
            for col in cols:
                val = r[col]
                if col == "volume":
                    row[col] = None if pd.isna(val) else int(val)
                else:
                    row[col] = None if pd.isna(val) else float(val)
            rows.append(row)
        return rows

    async def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        rows = self._normalize_for_write(symbol, df, _DAILY_COLS)
        if not rows:
            return
        stmt = text(
            "INSERT INTO bars_daily (symbol, ts, open, high, low, close, volume, vwap_close) "
            "VALUES (:symbol, :ts, :open, :high, :low, :close, :volume, :vwap_close) "
            "ON CONFLICT (symbol, ts) DO UPDATE SET "
            "open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, "
            "close = EXCLUDED.close, volume = EXCLUDED.volume, vwap_close = EXCLUDED.vwap_close"
        )
        try:
            async with self._engine.begin() as conn:
                await conn.execute(stmt, rows)
        except Exception as exc:
            logger.exception("Failed to upsert daily bars for %s: %s", symbol.upper(), exc)

    async def save_intraday_bars(self, symbol: str, df: pd.DataFrame) -> None:
        rows = self._normalize_for_write(symbol, df, _INTRADAY_COLS)
        if not rows:
            return
        stmt = text(
            "INSERT INTO bars_intraday (symbol, ts, open, high, low, close, volume) "
            "VALUES (:symbol, :ts, :open, :high, :low, :close, :volume) "
            "ON CONFLICT (symbol, ts) DO UPDATE SET "
            "open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, "
            "close = EXCLUDED.close, volume = EXCLUDED.volume"
        )
        try:
            async with self._engine.begin() as conn:
                await conn.execute(stmt, rows)
        except Exception as exc:
            logger.exception("Failed to upsert intraday bars for %s: %s", symbol.upper(), exc)

    # ---- read helpers -------------------------------------------------------

    async def _query_bars(self, table: str, symbol: str, lookback_days: int,
                          cols: List[str]) -> pd.DataFrame:
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        col_sql = ", ".join(cols)
        stmt = text(
            f"SELECT ts, {col_sql} FROM {table} "
            "WHERE symbol = :symbol AND ts >= :cutoff ORDER BY ts"
        )
        async with self._engine.connect() as conn:
            res = await conn.execute(stmt, {"symbol": symbol.upper(), "cutoff": cutoff})
            rows = res.fetchall()

        df = pd.DataFrame(rows, columns=["ts", *cols])
        if df.empty:
            return df
        # TIMESTAMPTZ comes back tz-aware (UTC); normalize to naive UTC so the
        # index semantics match what consumers have always seen.
        df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_localize(None)
        df = df.set_index("ts")
        for col in cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    async def daily_bars(self, symbol: str, lookback_days: int = 400) -> Optional[pd.DataFrame]:
        df = await self._query_bars("bars_daily", symbol, lookback_days, _DAILY_COLS)
        if df.empty:
            return None
        return df

    async def intraday_bars(self, symbol: str, lookback_days: int = 10) -> pd.DataFrame:
        df = await self._query_bars("bars_intraday", symbol, lookback_days, _INTRADAY_COLS)
        if df.empty:
            return pd.DataFrame(columns=_INTRADAY_COLS)
        return df

    async def last_price(self, symbol: str) -> Optional[float]:
        stmt = text(
            "SELECT close FROM bars_daily WHERE symbol = :symbol "
            "ORDER BY ts DESC LIMIT 1"
        )
        async with self._engine.connect() as conn:
            res = await conn.execute(stmt, {"symbol": symbol.upper()})
            row = res.fetchone()
        if row is None or row[0] is None:
            return None
        return float(row[0])

    async def get_price_on_date(self, symbol: str, dt: Any) -> Optional[float]:
        if not dt:
            return None
        # For a bare date, "on or before" means anything up to end-of-day.
        if isinstance(dt, datetime):
            target = _as_utc(dt)
        elif isinstance(dt, date):
            target = _as_utc(datetime.combine(dt, datetime_time.max))
        else:
            target = _as_utc(dt)

        stmt = text(
            "SELECT close FROM bars_daily WHERE symbol = :symbol AND ts <= :target "
            "ORDER BY ts DESC LIMIT 1"
        )
        async with self._engine.connect() as conn:
            res = await conn.execute(stmt, {"symbol": symbol.upper(), "target": target})
            row = res.fetchone()
        if row is None or row[0] is None:
            return None
        return float(row[0])

    async def get_total_bars_count(self) -> Tuple[int, int]:
        async with self._engine.connect() as conn:
            daily = (await conn.execute(text("SELECT COUNT(*) FROM bars_daily"))).scalar()
            intra = (await conn.execute(text("SELECT COUNT(*) FROM bars_intraday"))).scalar()
        return int(daily or 0), int(intra or 0)

    async def save_implied_vol(self, symbol: str, iv: float, ts: Optional[datetime] = None) -> None:
        if ts is None:
            ts = datetime.now(timezone.utc)
        else:
            ts = _as_utc(ts)
        # Truncate ts to daily (date) so we store exactly one observation per day
        ts = datetime.combine(ts.date(), datetime_time.min, tzinfo=timezone.utc)
        stmt = text(
            "INSERT INTO implied_volatility (symbol, ts, iv) "
            "VALUES (:symbol, :ts, :iv) "
            "ON CONFLICT (symbol, ts) DO UPDATE SET iv = EXCLUDED.iv"
        )
        try:
            async with self._engine.begin() as conn:
                await conn.execute(stmt, {"symbol": symbol.upper(), "ts": ts, "iv": float(iv)})
        except Exception as exc:
            logger.exception("Failed to save implied vol for %s: %s", symbol.upper(), exc)

    async def get_implied_vol_history(self, symbol: str, lookback_days: int = 365) -> List[Tuple[datetime, float]]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        stmt = text(
            "SELECT ts, iv FROM implied_volatility "
            "WHERE symbol = :symbol AND ts >= :cutoff ORDER BY ts"
        )
        try:
            async with self._engine.connect() as conn:
                res = await conn.execute(stmt, {"symbol": symbol.upper(), "cutoff": cutoff})
                rows = res.fetchall()
            return [(row[0], float(row[1])) for row in rows]
        except Exception as exc:
            logger.exception("Failed to query implied vol history for %s: %s", symbol.upper(), exc)
            return []


