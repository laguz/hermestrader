"""
Transaction Manager FSM.

Orchestrates atomic state transitions for PendingOrder and Trade models.
Resolves and handles status mutations in a centralized way while appending events to the Event Store.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple
from hermes.utils import utc_now

from sqlalchemy import select
from hermes.db.orm import PendingOrder, Trade
from hermes.db.events import (
    EventStoreManager,
    OrderSubmittedEvent,
    OrderFilledEvent,
    OrderRejectedEvent,
    OrderExpiredEvent,
    CloseSubmittedEvent,
    CloseFilledEvent,
    CloseReopenedEvent,
    ReconcileFlatEvent,
)

logger = logging.getLogger("hermes.service1_agent.transaction_manager")


def _to_json_safe(val: Any) -> Any:
    """Helper to convert date and datetime objects inside dictionaries/lists to ISO strings."""
    if isinstance(val, dict):
        return {k: _to_json_safe(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [_to_json_safe(v) for v in val]
    elif isinstance(val, (datetime, date)):
        return val.isoformat()
    return val


async def _get_next_id(session, table_name: str, seq_name: str) -> int:
    dialect_name = "sqlite"
    if session.bind:
        dialect_name = session.bind.dialect.name
    if dialect_name == "sqlite":
        from sqlalchemy import text
        res = await session.execute(text(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}"))
        return (res.scalar() or 0) + 1
    else:
        from sqlalchemy import text
        res = await session.execute(text(f"SELECT nextval('{seq_name}')"))
        return res.scalar()


class TransactionManager:
    @classmethod
    async def place_order(
        cls,
        session,
        strategy_id: str,
        symbol: str,
        side: Optional[str],
        quantity: int,
        payload: Dict[str, Any]
    ) -> PendingOrder:
        """Create and place a pending order with status PENDING, and record event."""
        po_id = await _get_next_id(session, "pending_orders", "pending_orders_id_seq")

        # Emit event
        event = OrderSubmittedEvent(
            id=po_id,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side or "",
            quantity=quantity,
            payload=_to_json_safe(payload),
            submitted_at=utc_now().isoformat()
        )
        await EventStoreManager.record_event(session, event)

        q = select(PendingOrder).where(PendingOrder.id == po_id)
        res = await session.execute(q)
        po = res.scalars().first()
        if not po:
            po = PendingOrder(
                id=po_id,
                strategy_id=strategy_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                payload=payload,
                status="PENDING",
                submitted_at=datetime.fromisoformat(event.submitted_at)
            )

        logger.info(
            f"[FSM place_order] Created PendingOrder {po.id} for {strategy_id} {symbol} side={side} qty={quantity}"
        )
        return po

    @classmethod
    async def _consume_pending(
        cls,
        session,
        strategy_id: str,
        symbol: str,
        side: str
    ) -> Optional[PendingOrder]:
        """Find the latest PENDING order matching criteria."""
        q = (
            select(PendingOrder)
            .filter(
                PendingOrder.strategy_id == strategy_id,
                PendingOrder.symbol == symbol,
                PendingOrder.side == side,
                PendingOrder.status == "PENDING"
            )
            .order_by(PendingOrder.submitted_at.desc())
            .limit(1)
        )
        result = await session.execute(q)
        return result.scalars().first()

    @classmethod
    async def reject(
        cls,
        session,
        strategy_id: str,
        symbol: str,
        side: str,
        lots: int
    ) -> Optional[PendingOrder]:
        """Transition a pending order to REJECTED and record event."""
        po = await cls._consume_pending(
            session=session,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side
        )
        if po:
            event = OrderRejectedEvent(pending_order_id=po.id)
            await EventStoreManager.record_event(session, event)
        return po

    @classmethod
    async def fill(
        cls,
        session,
        strategy_id: str,
        symbol: str,
        side: str,
        lots: int,
        trade_fields: Dict[str, Any]
    ) -> Tuple[Optional[PendingOrder], Trade]:
        """Transition a pending order to SUBMITTED, create an OPEN Trade, and record event."""
        po = await cls._consume_pending(
            session=session,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side
        )

        trade_id = trade_fields.get("id")
        if trade_id is None:
            trade_id = await _get_next_id(session, "trades", "trades_id_seq")

        trade_fields_copy = dict(trade_fields)
        trade_fields_copy["id"] = trade_id
        if "opened_at" in trade_fields_copy and isinstance(trade_fields_copy["opened_at"], datetime):
            trade_fields_copy["opened_at"] = trade_fields_copy["opened_at"].isoformat()
        elif "opened_at" not in trade_fields_copy:
            trade_fields_copy["opened_at"] = utc_now().isoformat()

        # Emit event
        if po:
            event = OrderFilledEvent(
                pending_order_id=po.id,
                trade_id=trade_id,
                trade_fields=_to_json_safe(trade_fields_copy)
            )
            await EventStoreManager.record_event(session, event)

        q = select(Trade).where(Trade.id == trade_id)
        res = await session.execute(q)
        trade = res.scalars().first()
        if not trade:
            trade = Trade(**trade_fields)
            trade.id = trade_id
            trade.status = "OPEN"

        logger.info(
            f"[FSM fill] Trade {trade.id} created for {strategy_id} {symbol} (Trade status: {trade.status})"
        )
        return po, trade

    @classmethod
    async def close(
        cls,
        session,
        strategy_id: str,
        symbol: str,
        side: str,
        lots: int,
        trade: Trade,
        filled: bool,
        exit_price: Optional[float],
        close_reason: str,
        close_tag: Optional[str]
    ) -> Optional[PendingOrder]:
        """Transition a pending order to SUBMITTED, transition the Trade status, and record event."""
        po = await cls._consume_pending(
            session=session,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side
        )

        event = CloseSubmittedEvent(
            pending_order_id=po.id if po else 0,
            trade_id=trade.id,
            exit_price=exit_price,
            close_reason=close_reason,
            close_tag=close_tag
        )
        await EventStoreManager.record_event(session, event)

        if filled:
            event_fill = CloseFilledEvent(
                trade_id=trade.id,
                closed_at=utc_now().isoformat()
            )
            await EventStoreManager.record_event(session, event_fill)

        return po

    @classmethod
    async def reconcile_trade(
        cls,
        session,
        trade: Trade,
        event: str,
        close_reason: Optional[str] = None
    ) -> None:
        """Apply reconciler transitions on a Trade object and append corresponding event."""
        if event == "force_close":
            closed_at = utc_now().isoformat()
            ev = ReconcileFlatEvent(
                trade_id=trade.id,
                closed_at=closed_at,
                close_reason=close_reason or trade.close_reason or "RECONCILED_BROKER_FLAT"
            )
            await EventStoreManager.record_event(session, ev)
        elif event == "finish_close":
            closed_at = utc_now().isoformat()
            ev = CloseFilledEvent(
                trade_id=trade.id,
                closed_at=closed_at
            )
            await EventStoreManager.record_event(session, ev)
        elif event == "reopen":
            ev = CloseReopenedEvent(
                trade_id=trade.id
            )
            await EventStoreManager.record_event(session, ev)

        logger.info(
            f"[FSM reconcile_trade] Trade {trade.id} symbol={trade.symbol} transitioned via {event}"
        )

    @classmethod
    async def expire_order(cls, session, po: PendingOrder) -> None:
        """Transition PendingOrder to EXPIRED and append corresponding event."""
        ev = OrderExpiredEvent(pending_order_id=po.id)
        await EventStoreManager.record_event(session, ev)
        logger.info(f"[FSM expire_order] PendingOrder {po.id} transitioned to EXPIRED")
