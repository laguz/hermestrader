"""Thin delegators to the bars/quotes TimeSeriesEngine.

The heavy lifting lives in :class:`hermes.db.timeseries.TimeSeriesEngine`
(constructed as ``self.ts_engine`` in ``HermesDB.__init__``); these methods
keep the bar/price surface on ``HermesDB`` for existing call-sites.
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd

from .base import Repository


class TimeSeriesRepository(Repository):
    async def daily_bars(self, symbol: str, lookback_days: int = 400) -> Optional[pd.DataFrame]:
        return await self.ts_engine.daily_bars(symbol, lookback_days)

    async def intraday_bars(self, symbol: str, lookback_days: int = 10) -> pd.DataFrame:
        return await self.ts_engine.intraday_bars(symbol, lookback_days)

    async def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert daily bars for a symbol from a DataFrame."""
        await self.ts_engine.save_daily_bars(symbol, df)

    async def save_intraday_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert intraday bars for a symbol from a DataFrame."""
        await self.ts_engine.save_intraday_bars(symbol, df)

    async def last_price(self, symbol: str) -> Optional[float]:
        return await self.ts_engine.last_price(symbol)

    async def get_price_on_date(self, symbol: str, dt: date) -> Optional[float]:
        """Fetch close price of the symbol on or before the specified date."""
        return await self.ts_engine.get_price_on_date(symbol, dt)
