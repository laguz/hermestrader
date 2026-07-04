"""Human-approval queue and overseer veto-suppression bookkeeping."""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger("hermes.db.repositories.approvals")
from hermes.utils import utc_now

from sqlalchemy import select

from hermes.common import IPC_CHANNEL_AGENT_COMMANDS
from hermes.db.orm import PendingApproval, VetoSuppression

from .base import Repository


class ApprovalsRepository(Repository):
    async def has_pending_approval(self, strategy_id: str, symbol: str,
                             side_type: Optional[str],
                             expiry: Optional[str]) -> bool:
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingApproval)
                .filter(
                    PendingApproval.strategy_id == strategy_id,
                    PendingApproval.symbol == symbol,
                    PendingApproval.status.in_(["PENDING", "PENDING_AI_REVIEW"])
                )
            )
            rows = result.scalars().all()
            for r in rows:
                aj = r.action_json or {}
                sp = aj.get("strategy_params") or {}
                if side_type is not None:
                    if sp.get("side_type", "").lower() != (side_type or "").lower():
                        continue
                if expiry is not None:
                    if aj.get("expiry") != expiry:
                        continue
                return True
        return False

    async def record_veto(self, strategy_id: str, symbol: str,
                          side_type: Optional[str], expiry: Optional[str],
                          rationale: Optional[str], ttl_seconds: int) -> int:
        """Record (or escalate) a veto suppression for this entry key.

        If an unexpired suppression already exists for the exact
        (strategy, symbol, side_type, expiry) key, bump its ``hits`` and
        extend the window proportionally (linear backoff: window =
        ttl × hits) so a setup that keeps getting re-proposed is muted for
        longer. Returns the resulting ``hits`` count.
        """
        now = utc_now()
        symbol = (symbol or "").upper()
        side_type = side_type.lower() if side_type else None
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(VetoSuppression).filter(
                    VetoSuppression.strategy_id == strategy_id,
                    VetoSuppression.symbol == symbol,
                    VetoSuppression.expires_at > now,
                )
            )
            existing = None
            for r in result.scalars().all():
                if (r.side_type or None) == side_type and (r.expiry or None) == (expiry or None):
                    existing = r
                    break
            if existing is not None:
                existing.hits += 1
                existing.expires_at = now + timedelta(seconds=ttl_seconds * existing.hits)
                if rationale:
                    existing.rationale = rationale
                await s.commit()
                return existing.hits
            s.add(VetoSuppression(
                strategy_id=strategy_id, symbol=symbol, side_type=side_type,
                expiry=expiry, rationale=rationale,
                expires_at=now + timedelta(seconds=ttl_seconds), hits=1,
            ))
            await s.commit()
            return 1

    async def active_veto(self, strategy_id: str, symbol: str,
                          side_type: Optional[str],
                          expiry: Optional[str]) -> Optional[str]:
        """Return the rationale if an unexpired veto covers this entry, else None.

        A stored suppression with NULL ``side_type``/``expiry`` is symbol-wide
        and matches any side/expiry; a populated field must match exactly.
        """
        now = utc_now()
        symbol = (symbol or "").upper()
        side_type = side_type.lower() if side_type else None
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(VetoSuppression).filter(
                    VetoSuppression.strategy_id == strategy_id,
                    VetoSuppression.symbol == symbol,
                    VetoSuppression.expires_at > now,
                ).order_by(VetoSuppression.expires_at.desc())
            )
            for r in result.scalars().all():
                if r.side_type and r.side_type != side_type:
                    continue
                if r.expiry and r.expiry != expiry:
                    continue
                return r.rationale or "previously vetoed"
        return None

    async def expire_stale_approvals(self) -> int:
        now = utc_now()
        expired = 0
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingApproval)
                .filter(
                    PendingApproval.status == "PENDING",
                    PendingApproval.expires_at.isnot(None),
                    PendingApproval.expires_at < now,
                )
            )
            stale = result.scalars().all()
            for row in stale:
                row.status = "EXPIRED"
                row.decided_at = now
                row.notes = (row.notes or "") + " [auto-expired: stale approval past deadline]"
                expired += 1
            if expired:
                await s.commit()
        return expired

    # ---- approval queue --------------------------------------------------
    async def queue_for_approval(self, action_json: Dict[str, Any],
                           action_type: str = "entry",
                           expires_hours: float = 24.0) -> int:
        expires_at = (utc_now() + timedelta(hours=expires_hours)
                      if expires_hours > 0 else None)
        async with self.AsyncSession() as s:
            row = PendingApproval(
                strategy_id=action_json.get("strategy_id", "UNKNOWN"),
                symbol=action_json.get("symbol", ""),
                action_type=action_type,
                action_json=action_json,
                status="PENDING",
                expires_at=expires_at,
            )
            s.add(row)
            await s.flush()
            row_id = row.id
            await s.commit()
            return row_id

    async def list_approvals(self, status: Optional[str] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            q = select(PendingApproval).order_by(PendingApproval.created_at.desc())
            if status:
                q = q.filter(PendingApproval.status == status.upper())
            result = await s.execute(q.limit(limit))
            rows = result.scalars().all()
            return [
                {
                    "id": r.id,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "strategy_id": r.strategy_id,
                    "symbol": r.symbol,
                    "action_type": r.action_type,
                    "action_json": r.action_json,
                    "status": r.status,
                    "notes": r.notes,
                    "decided_at": r.decided_at.isoformat() if r.decided_at else None,
                    "executed_at": r.executed_at.isoformat() if r.executed_at else None,
                    "expires_at": r.expires_at.isoformat() if r.expires_at else None,
                }
                for r in rows
            ]

    async def get_approval(self, approval_id: int) -> Optional[Dict[str, Any]]:
        """Read a single approval row (id/status/…) or None. Read-only —
        used by the watcher to validate an operator decision before enqueueing
        it onto the command channel (the agent owns the actual transition)."""
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingApproval).filter_by(id=approval_id).limit(1))
            r = result.scalars().first()
            if r is None:
                return None
            return {"id": r.id, "status": r.status, "strategy_id": r.strategy_id,
                    "symbol": r.symbol, "action_type": r.action_type}

    async def decide_approval(self, approval_id: int, decision: str,
                        notes: Optional[str] = None) -> bool:
        decision = decision.upper()
        if decision not in ("APPROVED", "REJECTED"):
            raise ValueError(f"decision must be APPROVED or REJECTED, got {decision!r}")
        async with self.AsyncSession() as s:
            result = await s.execute(select(PendingApproval).filter_by(id=approval_id).limit(1))
            row = result.scalars().first()
            if row is None or row.status != "PENDING":
                return False
                
            from hermes.db.events import EventStoreManager, ApprovalDecidedEvent
            ev = ApprovalDecidedEvent(
                approval_id=approval_id,
                status=decision,
                notes=notes or row.notes,
                decided_at=utc_now().isoformat()
            )
            await EventStoreManager.record_event(s, ev)
            
            payload = {
                "event_type": "ApprovalDecidedEvent",
                "payload": ev.model_dump(mode="json")
            }
            await s.commit()
            try:
                from hermes.ipc import ipc
                await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, payload)
            except Exception as exc:
                logger.warning("Failed to publish ApprovalDecidedEvent to IPC: %s", exc)
            return True

    async def fetch_approved_actions(self) -> List[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingApproval)
                .filter_by(status="APPROVED")
                .order_by(PendingApproval.decided_at)
            )
            rows = result.scalars().all()
            return [
                {"id": r.id, "action_json": r.action_json,
                 "strategy_id": r.strategy_id, "symbol": r.symbol}
                for r in rows
            ]

    async def mark_approval_executed(self, approval_id: int, success: bool = True,
                               notes: Optional[str] = None) -> None:
        async with self.AsyncSession() as s:
            result = await s.execute(select(PendingApproval).filter_by(id=approval_id).limit(1))
            row = result.scalars().first()
            if row:
                from hermes.db.events import EventStoreManager, ApprovalDecidedEvent
                ev = ApprovalDecidedEvent(
                    approval_id=approval_id,
                    status="EXECUTED" if success else "FAILED",
                    notes=notes,
                    executed_at=utc_now().isoformat()
                )
                await EventStoreManager.record_event(s, ev)
                
                payload = {
                    "event_type": "ApprovalDecidedEvent",
                    "payload": ev.model_dump(mode="json")
                }
                await s.commit()
                try:
                    from hermes.ipc import ipc
                    await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, payload)
                except Exception as exc:
                    logger.warning("Failed to publish ApprovalDecidedEvent to IPC: %s", exc)

    async def list_approvals_async(self, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return await self.list_approvals(status, limit)

    async def fetch_pending_ai_review_actions(self) -> List[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingApproval)
                .filter_by(status="PENDING_AI_REVIEW")
                .order_by(PendingApproval.created_at)
            )
            rows = result.scalars().all()
            return [
                {"id": r.id, "action_json": r.action_json,
                 "strategy_id": r.strategy_id, "symbol": r.symbol,
                 "action_type": r.action_type}
                for r in rows
            ]

    async def update_approval_status(self, approval_id: int, status: str,
                               action_json: Optional[Dict[str, Any]] = None,
                               notes: Optional[str] = None) -> bool:
        async with self.AsyncSession() as s:
            result = await s.execute(select(PendingApproval).filter_by(id=approval_id).limit(1))
            row = result.scalars().first()
            if row is None:
                return False
            row.status = status.upper()
            row.decided_at = utc_now()
            if action_json is not None:
                row.action_json = action_json
            if notes is not None:
                row.notes = notes
            await s.commit()
            return True
