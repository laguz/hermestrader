"""Shared agent/watcher runtime settings (key/value)."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from sqlalchemy import select

from hermes.db.orm import SystemSetting


class SettingsRepositoryMixin:
    async def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        async with self.AsyncSession() as s:
            result = await s.execute(select(SystemSetting).filter_by(key=key).limit(1))
            row = result.scalars().first()
            return row.value if row else default

    async def get_settings(self, keys) -> Dict[str, str]:
        """Bulk read: ``{key: value}`` for the subset of ``keys`` that exist.

        One ``WHERE key IN (...)`` query instead of N round-trips — used by
        the strategy tunables loader, which resolves ~10 keys per tick.
        Missing keys are simply absent from the result (callers supply
        their own defaults).
        """
        keys = list(keys)
        if not keys:
            return {}
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(SystemSetting).where(SystemSetting.key.in_(keys)))
            return {row.key: row.value for row in result.scalars().all()}

    async def set_setting(self, key: str, value: str) -> None:
        async with self.AsyncSession() as s:
            result = await s.execute(select(SystemSetting).filter_by(key=key).limit(1))
            row = result.scalars().first()
            if row is None:
                row = SystemSetting(key=key, value=str(value))
                s.add(row)
            else:
                row.value = str(value)
                row.updated_at = datetime.utcnow()
            await s.flush()
            
            # Emit Event-Sourced Event
            from hermes.db.events import EventStoreManager, SystemSettingChangedEvent, DoctrineUpdatedEvent
            if key == "soul_md":
                ev = DoctrineUpdatedEvent(
                    doctrine_text=str(value),
                    updated_at=row.updated_at.isoformat() if row.updated_at else datetime.utcnow().isoformat()
                )
            else:
                ev = SystemSettingChangedEvent(
                    key=key,
                    value=str(value),
                    updated_at=row.updated_at.isoformat() if row.updated_at else datetime.utcnow().isoformat()
                )
            await EventStoreManager.append_event(s, ev)
            await s.commit()

    async def setting_updated_at(self, key: str) -> Optional[datetime]:
        async with self.AsyncSession() as s:
            result = await s.execute(select(SystemSetting).filter_by(key=key).limit(1))
            row = result.scalars().first()
            return row.updated_at if row else None

    async def get_setting_async(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return await self.get_setting(key, default)
