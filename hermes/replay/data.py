"""Historical bar access for the replay harness.

Bars come either from in-memory DataFrames (tests, CSV fixtures) or from the
``bars_daily`` / ``bars_intraday`` hypertables via plain read-only ``SELECT``s
— this module never opens a writable session and never touches the ORM, so a
replay run cannot mutate the live database it reads from.

Lookahead rule: at simulated instant ``t`` a daily bar is *completed* only if
its date is strictly before ``t``'s ET trading date. The current day's bar
contributes its ``open`` (known at 9:30 ET); its close is only used by ticks
at/after 15:30 ET as a documented end-of-day approximation (or exactly, via
intraday bars when they exist).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time as dt_time, timezone
from typing import Dict, List, Optional

import pandas as pd

from hermes.market_hours import ET

logger = logging.getLogger("hermes.replay.data")

_DAILY_COLS = ["open", "high", "low", "close", "volume"]


def _naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _et_date(sim_dt: datetime) -> date:
    aware = sim_dt.replace(tzinfo=timezone.utc) if sim_dt.tzinfo is None else sim_dt
    return aware.astimezone(ET).date()


def _et_time(sim_dt: datetime) -> dt_time:
    aware = sim_dt.replace(tzinfo=timezone.utc) if sim_dt.tzinfo is None else sim_dt
    return aware.astimezone(ET).time()


class ReplayDataSource:
    """Per-symbol daily (and optional intraday) OHLCV bars, lookahead-safe."""

    def __init__(self, daily: Dict[str, pd.DataFrame],
                 intraday: Optional[Dict[str, pd.DataFrame]] = None):
        self.daily: Dict[str, pd.DataFrame] = {}
        self.intraday: Dict[str, pd.DataFrame] = {}
        for sym, df in (daily or {}).items():
            self.daily[sym.upper()] = self._normalize(df)
        for sym, df in (intraday or {}).items():
            self.intraday[sym.upper()] = self._normalize(df)

    @staticmethod
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        """Sorted copy indexed by naive-UTC ``ts`` with numeric OHLCV columns."""
        out = df.copy()
        if out.index.name != "ts":
            if "ts" in out.columns:
                out = out.set_index("ts")
            elif "date" in out.columns:
                out = out.rename(columns={"date": "ts"}).set_index("ts")
        out.index = pd.to_datetime(out.index)
        if getattr(out.index, "tz", None) is not None:
            out.index = out.index.tz_convert("UTC").tz_localize(None)
        for col in _DAILY_COLS:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
        if "iv_proxy" in out.columns:
            out["iv_proxy"] = pd.to_numeric(out["iv_proxy"], errors="coerce")
        return out.sort_index()


    # ---- constructors -----------------------------------------------------
    @classmethod
    def from_frames(cls, daily: Dict[str, pd.DataFrame],
                    intraday: Optional[Dict[str, pd.DataFrame]] = None) -> "ReplayDataSource":
        return cls(daily, intraday)

    @classmethod
    def from_db(cls, dsn: str, symbols: List[str], start: date, end: date,
                *, lookback_days: int = 300,
                load_intraday: bool = True) -> "ReplayDataSource":
        """Read-only bar load from the ``bars_daily``/``bars_intraday`` tables.

        ``lookback_days`` of history before ``start`` is included so
        analysis (K-Means S/R, realized vol, ATR) has a warm-up window.
        """
        import psycopg

        pq_dsn = dsn.replace("+psycopg", "").replace("+asyncpg", "")
        cutoff = pd.Timestamp(start) - pd.Timedelta(days=lookback_days)
        daily: Dict[str, pd.DataFrame] = {}
        intraday: Dict[str, pd.DataFrame] = {}
        with psycopg.connect(pq_dsn) as conn:
            conn.read_only = True
            for sym in symbols:
                rows = conn.execute(
                    "SELECT ts, open, high, low, close, volume FROM bars_daily "
                    "WHERE symbol = %s AND ts >= %s AND ts <= %s ORDER BY ts",
                    (sym.upper(), cutoff.to_pydatetime(),
                     datetime.combine(end, dt_time.max)),
                ).fetchall()
                if rows:
                    daily[sym.upper()] = pd.DataFrame(
                        rows, columns=["ts", *_DAILY_COLS])
                if load_intraday:
                    irows = conn.execute(
                        "SELECT ts, open, high, low, close, volume FROM bars_intraday "
                        "WHERE symbol = %s AND ts >= %s AND ts <= %s ORDER BY ts",
                        (sym.upper(), datetime.combine(start, dt_time.min),
                         datetime.combine(end, dt_time.max)),
                    ).fetchall()
                    if irows:
                        intraday[sym.upper()] = pd.DataFrame(
                            irows, columns=["ts", *_DAILY_COLS])
        missing = [s for s in symbols if s.upper() not in daily]
        if missing:
            logger.warning("no daily bars found for %s", missing)
        return cls(daily, intraday)

    # ---- lookahead-safe reads ----------------------------------------------
    def completed_daily(self, symbol: str, sim_dt: datetime) -> pd.DataFrame:
        """Daily bars whose ET date is strictly before ``sim_dt``'s ET date."""
        df = self.daily.get(symbol.upper())
        if df is None or df.empty:
            return pd.DataFrame(columns=_DAILY_COLS)
        cutoff = _et_date(_naive_utc(sim_dt))
        return df[[d.date() < cutoff for d in df.index]]

    def iv_proxy(self, symbol: str, sim_dt: datetime) -> Optional[float]:
        """Best lookahead-safe IV proxy estimate at ``sim_dt``.

        Always returns the previous day's completed IV proxy to prevent lookahead.
        """
        completed = self.completed_daily(symbol, sim_dt)
        if not completed.empty and "iv_proxy" in completed.columns:
            val = completed["iv_proxy"].iloc[-1]
            if pd.notna(val):
                return float(val)
        return None


    def today_bar(self, symbol: str, sim_dt: datetime) -> Optional[pd.Series]:
        df = self.daily.get(symbol.upper())
        if df is None or df.empty:
            return None
        today = _et_date(_naive_utc(sim_dt))
        rows = df[[d.date() == today for d in df.index]]
        if rows.empty:
            return None
        return rows.iloc[-1]

    def spot(self, symbol: str, sim_dt: datetime) -> Optional[float]:
        """Best lookahead-safe price estimate at ``sim_dt``.

        Preference order: last intraday close ≤ ``sim_dt``; else today's open
        for morning/midday ticks and today's close for ticks at/after 15:30 ET
        (documented approximation for daily-only data); else the last
        completed daily close.
        """
        sim_dt = _naive_utc(sim_dt)
        idf = self.intraday.get(symbol.upper())
        if idf is not None and not idf.empty:
            upto = idf[idf.index <= sim_dt]
            if not upto.empty:
                val = upto["close"].iloc[-1]
                if pd.notna(val):
                    return float(val)
        bar = self.today_bar(symbol, sim_dt)
        if bar is not None:
            if _et_time(sim_dt) >= dt_time(15, 30) and pd.notna(bar.get("close")):
                return float(bar["close"])
            if pd.notna(bar.get("open")):
                return float(bar["open"])
        completed = self.completed_daily(symbol, sim_dt)
        if not completed.empty and pd.notna(completed["close"].iloc[-1]):
            return float(completed["close"].iloc[-1])
        return None

    def close_on(self, symbol: str, day: date) -> Optional[float]:
        """The daily close on ``day`` (used for expiry settlement after the fact)."""
        df = self.daily.get(symbol.upper())
        if df is None or df.empty:
            return None
        rows = df[[d.date() <= day for d in df.index]]
        if rows.empty:
            return None
        val = rows["close"].iloc[-1]
        return float(val) if pd.notna(val) else None

    def trading_days(self, start: date, end: date) -> List[date]:
        """Union of daily-bar dates across all symbols inside [start, end].

        Data-driven, so weekends/holidays fall out for free.
        """
        days: set = set()
        for df in self.daily.values():
            for d in df.index:
                dd = d.date()
                if start <= dd <= end:
                    days.add(dd)
        return sorted(days)
