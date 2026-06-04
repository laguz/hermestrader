"""Bot-log writes/reads and engine liveness signals."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select

from hermes.db.orm import BotLog

logger = logging.getLogger("hermes.db")


class LogsRepositoryMixin:
    async def write_log(self, strategy_id: str, message: str, level: str = "INFO") -> None:
        async with self.AsyncSession() as s:
            s.add(BotLog(strategy_id=strategy_id, level=level, message=message))
            await s.commit()

    async def flag_orphans(self, orphan_symbols) -> None:
        async with self.AsyncSession() as s:
            for sym in orphan_symbols:
                s.add(BotLog(strategy_id="ENGINE", level="WARN",
                             message=f"orphan position: {sym}"))
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
