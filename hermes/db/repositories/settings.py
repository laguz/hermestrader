"""Shared agent/watcher runtime settings (key/value)."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional

from sqlalchemy import select

from hermes.common import IPC_CHANNEL_AGENT_COMMANDS
from hermes.db.orm import SystemSetting
from .base import Repository

logger = logging.getLogger("hermes.db.settings")


class SettingsRepository(Repository):
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
        value = str(value)
        async with self.AsyncSession() as s:
            # No-op writes must not emit events: the agent subscribes to its
            # own SYSTEM_SETTING_CHANGED publishes, and its settings-changed
            # handler rewrites status keys (llm_last_error via _build_llm), so
            # an event for an unchanged value feeds back as another write —
            # a self-sustaining publish→handle→write loop that floods
            # event_ledger. Nothing changed, so there is no event to record.
            existing = await s.execute(
                select(SystemSetting).filter_by(key=key).limit(1))
            row = existing.scalars().first()
            if row is not None and row.value == value:
                return

            updated_at = datetime.utcnow().isoformat()
            
            # Emit Event-Sourced Event
            from hermes.db.events import (
                EventStoreManager,
                SystemSettingChangedEvent,
                DoctrineUpdatedEvent,
                ModeChangedEvent,
                PauseChangedEvent,
                AutonomyChangedEvent,
                StrategyToggledEvent,
                CLASS_TO_EVENT_TYPE,
            )
            import re
            
            if key == "hermes_mode":
                ev = ModeChangedEvent(
                    mode=str(value),
                    updated_at=updated_at
                )
            elif key == "agent_paused":
                ev = PauseChangedEvent(
                    paused=(str(value).lower() == "true"),
                    updated_at=updated_at
                )
            elif key == "agent_autonomy":
                ev = AutonomyChangedEvent(
                    autonomy=str(value),
                    updated_at=updated_at
                )
            elif key == "soul_md":
                ev = DoctrineUpdatedEvent(
                    doctrine_text=str(value),
                    updated_at=updated_at
                )
            elif re.match(r"^strategy_([a-zA-Z0-9_]+)_enabled$", key):
                m = re.match(r"^strategy_([a-zA-Z0-9_]+)_enabled$", key)
                strat_id = m.group(1).upper()
                ev = StrategyToggledEvent(
                    strategy_id=strat_id,
                    enabled=(str(value).lower() == "true"),
                    updated_at=updated_at
                )
            else:
                ev = SystemSettingChangedEvent(
                    key=key,
                    value=str(value),
                    updated_at=updated_at
                )
                
            await EventStoreManager.record_event(s, ev)
            
            payload = {
                "event_type": CLASS_TO_EVENT_TYPE[ev.__class__],
                "payload": ev.model_dump(mode="json")
            }
            await s.commit()
            try:
                from hermes.ipc import ipc
                await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, payload)
            except Exception as e:
                logger.warning("IPC publish failure for setting event: %s", e)

    async def get_setting_async(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return await self.get_setting(key, default)
