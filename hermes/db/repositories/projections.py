from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, List
from sqlalchemy import select

from hermes.db.orm import Trade, PendingOrder, SystemSetting, _compute_realized_pnl
from hermes.db.events import (
    BaseEvent,
    OrderSubmittedEvent,
    OrderFilledEvent,
    OrderRejectedEvent,
    OrderExpiredEvent,
    CloseSubmittedEvent,
    CloseFilledEvent,
    CloseReopenedEvent,
    ReconcileFlatEvent,
    SystemSettingChangedEvent,
    DoctrineUpdatedEvent,
)

logger = logging.getLogger("hermes.db.repositories.projections")


class ProjectionsRepository:
    """Repository to apply event projections directly to the DB read models (CQRS)."""

    @staticmethod
    async def apply_event_projection(session, event: BaseEvent) -> None:
        """Apply the event state changes to update relational database read models."""
        
        if isinstance(event, OrderSubmittedEvent):
            # Create a PendingOrder row
            po = PendingOrder(
                id=event.id,
                strategy_id=event.strategy_id,
                symbol=event.symbol,
                side=event.side,
                quantity=event.quantity,
                payload=event.payload,
                status="PENDING",
                submitted_at=datetime.fromisoformat(event.submitted_at) if event.submitted_at else datetime.utcnow()
            )
            session.add(po)
            logger.info("[Projection] Applied ORDER_SUBMITTED: PendingOrder %d created", event.id)

        elif isinstance(event, OrderFilledEvent):
            # Transition PendingOrder to SUBMITTED
            q = select(PendingOrder).where(PendingOrder.id == event.pending_order_id)
            result = await session.execute(q)
            po = result.scalars().first()
            if po:
                po.status = "SUBMITTED"
            
            # Create Trade in OPEN state
            tf = event.trade_fields
            trade = Trade(
                id=event.trade_id,
                strategy_id=tf["strategy_id"],
                symbol=tf["symbol"],
                side_type=tf["side_type"],
                short_leg=tf.get("short_leg"),
                long_leg=tf.get("long_leg"),
                short_strike=tf.get("short_strike"),
                long_strike=tf.get("long_strike"),
                width=tf.get("width"),
                lots=tf["lots"],
                entry_credit=tf.get("entry_credit"),
                entry_debit=tf.get("entry_debit"),
                expiry=datetime.fromisoformat(tf["expiry"]).date() if tf.get("expiry") else None,
                status="OPEN",
                broker_order_id=tf.get("broker_order_id"),
                tag=tf.get("tag"),
                entry_features=tf.get("entry_features"),
                opened_at=datetime.fromisoformat(tf["opened_at"]) if tf.get("opened_at") else datetime.utcnow()
            )
            session.add(trade)
            logger.info("[Projection] Applied ORDER_FILLED: PendingOrder %d -> SUBMITTED, Trade %d -> OPEN", event.pending_order_id, event.trade_id)

        elif isinstance(event, OrderRejectedEvent):
            q = select(PendingOrder).where(PendingOrder.id == event.pending_order_id)
            result = await session.execute(q)
            po = result.scalars().first()
            if po:
                po.status = "REJECTED"
            logger.info("[Projection] Applied ORDER_REJECTED: PendingOrder %d -> REJECTED", event.pending_order_id)

        elif isinstance(event, OrderExpiredEvent):
            q = select(PendingOrder).where(PendingOrder.id == event.pending_order_id)
            result = await session.execute(q)
            po = result.scalars().first()
            if po:
                po.status = "EXPIRED"
            logger.info("[Projection] Applied ORDER_EXPIRED: PendingOrder %d -> EXPIRED", event.pending_order_id)

        elif isinstance(event, CloseSubmittedEvent):
            q = select(PendingOrder).where(PendingOrder.id == event.pending_order_id)
            result = await session.execute(q)
            po = result.scalars().first()
            if po:
                po.status = "SUBMITTED"
            
            q2 = select(Trade).where(Trade.id == event.trade_id)
            result2 = await session.execute(q2)
            trade = result2.scalars().first()
            if trade:
                trade.status = "CLOSING"
                trade.close_reason = event.close_reason
                trade.close_tag = event.close_tag
                trade.exit_price = event.exit_price
                trade.pnl = _compute_realized_pnl(
                    entry_credit=trade.entry_credit,
                    entry_debit=trade.entry_debit,
                    exit_price=trade.exit_price,
                    lots=int(trade.lots or 0)
                )
            logger.info("[Projection] Applied CLOSE_SUBMITTED: PendingOrder %d -> SUBMITTED, Trade %d -> CLOSING", event.pending_order_id, event.trade_id)

        elif isinstance(event, CloseFilledEvent):
            q = select(Trade).where(Trade.id == event.trade_id)
            result = await session.execute(q)
            trade = result.scalars().first()
            if trade:
                trade.status = "CLOSED"
                trade.closed_at = datetime.fromisoformat(event.closed_at) if event.closed_at else datetime.utcnow()
            logger.info("[Projection] Applied CLOSE_FILLED: Trade %d -> CLOSED", event.trade_id)

        elif isinstance(event, CloseReopenedEvent):
            q = select(Trade).where(Trade.id == event.trade_id)
            result = await session.execute(q)
            trade = result.scalars().first()
            if trade:
                trade.status = "OPEN"
            logger.info("[Projection] Applied CLOSE_REOPEN: Trade %d -> OPEN", event.trade_id)

        elif isinstance(event, ReconcileFlatEvent):
            q = select(Trade).where(Trade.id == event.trade_id)
            result = await session.execute(q)
            trade = result.scalars().first()
            if trade:
                trade.status = "CLOSED"
                trade.close_reason = event.close_reason
                trade.closed_at = datetime.fromisoformat(event.closed_at) if event.closed_at else datetime.utcnow()
            logger.info("[Projection] Applied RECONCILE_FLAT: Trade %d -> CLOSED (Reason: %s)", event.trade_id, event.close_reason)

        elif isinstance(event, SystemSettingChangedEvent):
            q = select(SystemSetting).where(SystemSetting.key == event.key)
            result = await session.execute(q)
            setting = result.scalars().first()
            if setting:
                setting.value = event.value
                setting.updated_at = datetime.fromisoformat(event.updated_at) if event.updated_at else datetime.utcnow()
            else:
                setting = SystemSetting(
                    key=event.key,
                    value=event.value,
                    updated_at=datetime.fromisoformat(event.updated_at) if event.updated_at else datetime.utcnow()
                )
                session.add(setting)
            logger.info("[Projection] Applied SYSTEM_SETTING_CHANGED: Key %s -> %s", event.key, event.value)

        elif isinstance(event, DoctrineUpdatedEvent):
            q = select(SystemSetting).where(SystemSetting.key == "soul_md")
            result = await session.execute(q)
            setting = result.scalars().first()
            if setting:
                setting.value = event.doctrine_text
                setting.updated_at = datetime.fromisoformat(event.updated_at) if event.updated_at else datetime.utcnow()
            else:
                setting = SystemSetting(
                    key="soul_md",
                    value=event.doctrine_text,
                    updated_at=datetime.fromisoformat(event.updated_at) if event.updated_at else datetime.utcnow()
                )
                session.add(setting)
            logger.info("[Projection] Applied DOCTRINE_UPDATED: soul_md updated")
