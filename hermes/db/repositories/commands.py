"""Operator command queue — the watcher's single write surface.

Service-2 (watcher) never mutates canonical state directly. It appends an
*intent* here (``enqueue_*``); Service-1 (agent) drains it (``fetch_pending``)
and applies each command in its own process via the normal event-sourced write
path, then marks it ``APPLIED`` / ``FAILED``. This preserves the single-writer
invariant — only the agent writes ``event_ledger`` and the read models — while
making operator actions durable: a command issued while the agent is down is
still ``PENDING`` and applied on the next drain.

See :class:`hermes.db.orm.OperatorCommand` and
``tests/test_writer_ownership.py``.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from sqlalchemy import select

from hermes.common import IPC_ACTION_DRAIN_COMMANDS, IPC_CHANNEL_AGENT_COMMANDS
from hermes.db.orm import OperatorCommand
from hermes.utils import utc_now

from .base import Repository


class CommandsRepository(Repository):
    # ── watcher side: enqueue intents ───────────────────────────────────────
    async def enqueue_command(self, command_type: str,
                              payload: Dict[str, Any]) -> int:
        """Append a PENDING operator command and nudge the agent to drain it.

        The row is the durable record (survives agent downtime); the IPC publish
        is only a low-latency wake-up — best-effort, never load-bearing.
        """
        async with self.AsyncSession() as s:
            row = OperatorCommand(command_type=command_type, payload=payload)
            s.add(row)
            await s.flush()
            cmd_id = row.id
            await s.commit()
        try:
            from hermes.ipc import ipc
            await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS,
                              {"action": IPC_ACTION_DRAIN_COMMANDS})
        except Exception as e:
            import logging
            logging.getLogger("hermes.db.repositories.commands").warning(
                "Failed to publish agent command drain event to IPC: %s", e
            )
        return cmd_id

    async def enqueue_setting(self, key: str, value: Any) -> int:
        return await self.enqueue_command("SET_SETTING", {"settings": {key: str(value)}})

    async def enqueue_settings(self, settings: Mapping[str, Any]) -> int:
        return await self.enqueue_command(
            "SET_SETTING", {"settings": {k: str(v) for k, v in settings.items()}})

    async def enqueue_decision(self, approval_id: int, decision: str,
                               notes: Optional[str] = None) -> int:
        return await self.enqueue_command(
            "DECIDE_APPROVAL",
            {"approval_id": int(approval_id), "decision": decision, "notes": notes})

    # ── agent side: drain ───────────────────────────────────────────────────
    async def fetch_pending(self, limit: int = 100) -> List[Dict[str, Any]]:
        """PENDING commands in submission (``id``) order — operator intent order."""
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(OperatorCommand)
                .filter(OperatorCommand.status == "PENDING")
                .order_by(OperatorCommand.id.asc())
                .limit(limit)
            )
            return [
                {"id": r.id, "command_type": r.command_type, "payload": r.payload}
                for r in result.scalars().all()
            ]

    async def mark_applied(self, command_id: int) -> None:
        await self._finish(command_id, "APPLIED", None)

    async def mark_failed(self, command_id: int, error: str) -> None:
        await self._finish(command_id, "FAILED", error)

    async def _finish(self, command_id: int, status: str,
                      error: Optional[str]) -> None:
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(OperatorCommand).filter_by(id=command_id).limit(1))
            row = result.scalars().first()
            if row is None:
                return
            row.status = status
            row.applied_at = utc_now()
            row.error = error
            await s.commit()
