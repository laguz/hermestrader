"""Bot-log writes/reads and engine liveness signals."""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

from hermes.utils import utc_now


from sqlalchemy import select

from hermes.db.orm import BotLog

from .base import Repository

logger = logging.getLogger("hermes.db")



_last_log_ts: datetime | None = None
_log_ts_lock = threading.Lock()


def get_unique_log_ts() -> datetime:
    """Generate a monotonic microsecond-precision UTC timestamp to prevent PK unique violations on fast/concurrent writes."""
    global _last_log_ts
    with _log_ts_lock:
        now_ts = utc_now()
        if _last_log_ts is not None and now_ts <= _last_log_ts:
            now_ts = _last_log_ts + timedelta(microseconds=1)
        _last_log_ts = now_ts
        return now_ts


class LogsRepository(Repository):
    async def write_log(self, strategy_id: str, message: str, level: str = "INFO") -> None:
        async with self.AsyncSession() as s:
            s.add(BotLog(strategy_id=strategy_id, level=level, message=message, ts=get_unique_log_ts()))
            await s.commit()

    async def flag_orphans(self, orphan_symbols) -> None:
        async with self.AsyncSession() as s:
            for sym in orphan_symbols:
                s.add(BotLog(strategy_id="ENGINE", level="WARN",
                             message=f"orphan position: {sym}", ts=get_unique_log_ts()))
            await s.commit()

    async def latest_log_ts(self) -> Optional[datetime]:
        """Most recent bot_logs timestamp — used as the agent's liveness signal."""
        async with self.AsyncSession() as s:
            result = await s.execute(select(BotLog).order_by(BotLog.ts.desc()).limit(1))
            row = result.scalars().first()
            return row.ts if row else None

    async def recent_logs(self, limit: int = 200) -> str:
        from hermes.market_hours import ET
        from datetime import timezone as _tz
        async with self.AsyncSession() as s:
            result = await s.execute(select(BotLog).order_by(BotLog.ts.desc()).limit(limit))
            rows = result.scalars().all()
            out = []
            for r in reversed(rows):
                ts = r.ts
                if ts is None:
                    out.append(f"--:--:-- ET [{r.strategy_id}] {r.message}")
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=_tz.utc)
                local = ts.astimezone(ET)
                out.append(f"{local:%H:%M:%S} ET [{r.strategy_id}] {r.message}")
            return "\n".join(out)

    async def latest_log_ts_async(self) -> Optional[datetime]:
        return await self.latest_log_ts()

    async def recent_logs_async(self, limit: int = 200) -> str:
        return await self.recent_logs(limit)
