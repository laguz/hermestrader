"""Strategy registry and per-strategy watchlist CRUD."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import select

from hermes.common import IPC_CHANNEL_AGENT_COMMANDS
from hermes.common import STRATEGY_PRIORITIES as _COMMON_STRATEGY_PRIORITIES
from hermes.db.orm import Strategy, StrategyWatchlist

from .base import Repository


class WatchlistRepository(Repository):
    _DEFAULT_STRATEGY_PRIORITIES = _COMMON_STRATEGY_PRIORITIES

    # ---- strategies registry (must be populated before watchlists) -------
    async def ensure_strategies(self, strategies: Dict[str, int]) -> None:
        async with self.AsyncSession() as s:
            result = await s.execute(select(Strategy))
            existing = {r.strategy_id for r in result.scalars().all()}
            for sid, priority in strategies.items():
                if sid in existing:
                    continue
                s.add(Strategy(strategy_id=sid, priority=int(priority),
                               status="ACTIVE"))
            await s.commit()

    # ---- watchlist CRUD ---------------------------------------------------
    async def list_watchlist(self, strategy_id: str) -> List[str]:
        from sqlalchemy import text as sa_text
        async with self.AsyncSession() as s:
            result = await s.execute(sa_text(
                "SELECT symbol FROM strategy_watchlists WHERE strategy_id = :sid ORDER BY symbol"
            ), {"sid": strategy_id})
            rows = result.fetchall()
            return [r[0] for r in rows]

    async def list_watchlist_detailed(self, strategy_id: str) -> Dict[str, Dict[str, Any]]:
        from sqlalchemy import text as sa_text
        out = {}
        async with self.AsyncSession() as s:
            try:
                result = await s.execute(sa_text(
                    "SELECT symbol, target_lots FROM strategy_watchlists WHERE strategy_id = :sid"
                ), {"sid": strategy_id})
                rows = result.fetchall()
                for r in rows:
                    out[r[0]] = {"target_lots": r[1]}
            except Exception:
                await s.rollback()
                result = await s.execute(sa_text(
                    "SELECT symbol FROM strategy_watchlists WHERE strategy_id = :sid"
                ), {"sid": strategy_id})
                rows = result.fetchall()
                for r in rows:
                    out[r[0]] = {"target_lots": None}
        return out

    async def all_watchlist_symbols(self) -> List[str]:
        """Deduped union of every strategy's watchlist symbols.

        HermesAlpha trades the whole desk's universe, not just its own list —
        it may pick any symbol any strategy is watching.
        """
        from sqlalchemy import text as sa_text
        async with self.AsyncSession() as s:
            result = await s.execute(sa_text(
                "SELECT DISTINCT symbol FROM strategy_watchlists ORDER BY symbol"
            ))
            return [r[0] for r in result.fetchall()]

    async def list_all_watchlists(self) -> Dict[str, List[str]]:
        from sqlalchemy import text as sa_text
        async with self.AsyncSession() as s:
            result = await s.execute(sa_text(
                "SELECT strategy_id, symbol FROM strategy_watchlists ORDER BY strategy_id, symbol"
            ))
            rows = result.fetchall()
            out: Dict[str, List[str]] = {}
            for sid, sym in rows:
                out.setdefault(sid, []).append(sym)
            return out

    async def add_to_watchlist(self, strategy_id: str, symbol: str) -> bool:
        sym = (symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol must be non-empty")
        async with self.AsyncSession() as s:
            result = await s.execute(select(Strategy).filter_by(strategy_id=strategy_id).limit(1))
            if not result.scalars().first():
                priority = self._DEFAULT_STRATEGY_PRIORITIES.get(strategy_id, 99)
                s.add(Strategy(strategy_id=strategy_id, priority=priority,
                               status="ACTIVE"))
                await s.flush()
            result = await s.execute(select(StrategyWatchlist).filter_by(strategy_id=strategy_id, symbol=sym).limit(1))
            exists = result.scalars().first()
            if exists:
                return False
            s.add(StrategyWatchlist(strategy_id=strategy_id, symbol=sym))
            await s.commit()
    async def set_watchlist(self, strategy_id: str, symbols: List[str]) -> List[str]:
        clean = sorted({(s or "").strip().upper() for s in symbols if (s or "").strip()})
        async with self.AsyncSession() as s:
            result = await s.execute(select(Strategy).filter_by(strategy_id=strategy_id).limit(1))
            if not result.scalars().first():
                priority = self._DEFAULT_STRATEGY_PRIORITIES.get(strategy_id, 99)
                s.add(Strategy(strategy_id=strategy_id, priority=priority,
                               status="ACTIVE"))
                await s.flush()
                
            from hermes.db.events import EventStoreManager, WatchlistChangedEvent
            ev = WatchlistChangedEvent(
                strategy_id=strategy_id,
                symbols=clean,
                updated_at=datetime.utcnow().isoformat()
            )
            await EventStoreManager.record_event(s, ev)
            
            payload = {
                "event_type": "WatchlistChangedEvent",
                "payload": ev.model_dump(mode="json")
            }
            await s.commit()
            try:
                from hermes.ipc import ipc
                await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, payload)
            except Exception:
                pass
        return clean
