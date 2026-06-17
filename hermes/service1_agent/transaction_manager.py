"""
Transaction Manager FSM.

Orchestrates atomic state transitions for PendingOrder and Trade models.
Resolves and handles status mutations in a centralized way while appending events to the Event Store.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import select, func
from hermes.db.orm import PendingOrder, Trade, _compute_realized_pnl
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
        dialect_name = "sqlite"
        if session.bind:
            dialect_name = session.bind.dialect.name
            
        po_id = None
        if dialect_name == "sqlite":
            q = select(func.max(PendingOrder.id))
            res = await session.execute(q)
            max_id = res.scalar() or 0
            po_id = max_id + 1

        po = PendingOrder(
            id=po_id,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            payload=payload,
            status="PENDING"
        )
        session.add(po)
        await session.flush()  # Generate po.id (if postgres) and po.submitted_at
        
        # Emit event
        event = OrderSubmittedEvent(
            id=po.id,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side or "",
            quantity=quantity,
            payload=_to_json_safe(payload),
            submitted_at=po.submitted_at.isoformat()
        )
        await EventStoreManager.append_event(session, event)
        
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
        side: str,
        terminal_status: str
    ) -> Optional[PendingOrder]:
        """Find the latest PENDING order matching criteria and transition its status."""
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
        po = result.scalars().first()
        if po:
            logger.info(
                f"[FSM pending_transition] PendingOrder {po.id} for {strategy_id} {symbol} "
                f"transitioned PENDING -> {terminal_status}"
            )
            po.status = terminal_status
        else:
            logger.debug(
                f"[FSM pending_transition] No matching PENDING order found for "
                f"{strategy_id} {symbol} side={side}"
            )
        return po

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
            side=side,
            terminal_status="REJECTED"
        )
        if po:
            event = OrderRejectedEvent(pending_order_id=po.id)
            await EventStoreManager.append_event(session, event)
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
        # First transition the matching pending order
        po = await cls._consume_pending(
            session=session,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side,
            terminal_status="SUBMITTED"
        )
        
        dialect_name = "sqlite"
        if session.bind:
            dialect_name = session.bind.dialect.name
            
        trade_id = trade_fields.get("id")
        if trade_id is None and dialect_name == "sqlite":
            q = select(func.max(Trade.id))
            res = await session.execute(q)
            max_id = res.scalar() or 0
            trade_id = max_id + 1
            
        trade_fields_copy = dict(trade_fields)
        trade_fields_copy["id"] = trade_id

        # Create and add trade
        trade = Trade(**trade_fields_copy)
        trade.status = "OPEN"
        session.add(trade)
        await session.flush()  # Generate trade.id (if postgres)
        
        # Emit event
        if po:
            event = OrderFilledEvent(
                pending_order_id=po.id,
                trade_id=trade.id,
                trade_fields=_to_json_safe(trade_fields_copy)
            )
            await EventStoreManager.append_event(session, event)
            
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
            side=side,
            terminal_status="SUBMITTED"
        )

        trade.close_reason = close_reason
        trade.close_tag = close_tag
        if exit_price is not None:
            trade.exit_price = exit_price
        
        trade.pnl = _compute_realized_pnl(
            entry_credit=trade.entry_credit,
            entry_debit=trade.entry_debit,
            exit_price=exit_price,
            lots=int(trade.lots or 0),
        )

        if filled:
            # Transition: OPEN/CLOSING -> CLOSED
            trade.force_close()
            trade.closed_at = datetime.utcnow()
            logger.info(
                f"[FSM close] Trade {trade.id} for {strategy_id} {symbol} closed immediately (filled). Status: {trade.status}"
            )
        else:
            # Transition: OPEN -> CLOSING
            trade.begin_close()
            logger.info(
                f"[FSM close] Trade {trade.id} for {strategy_id} {symbol} transitioned to CLOSING. Status: {trade.status}"
            )
            
        await session.flush()
        
        if po:
            event = CloseSubmittedEvent(
                pending_order_id=po.id,
                trade_id=trade.id,
                exit_price=exit_price,
                close_reason=close_reason,
                close_tag=close_tag
            )
            await EventStoreManager.append_event(session, event)
            
            if filled:
                event_fill = CloseFilledEvent(
                    trade_id=trade.id,
                    closed_at=trade.closed_at.isoformat()
                )
                await EventStoreManager.append_event(session, event_fill)
                
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
        old_status = trade.status
        if event == "force_close":
            trade.force_close()
            trade.closed_at = datetime.utcnow()
            if close_reason:
                trade.close_reason = close_reason
            await session.flush()
            ev = ReconcileFlatEvent(
                trade_id=trade.id,
                closed_at=trade.closed_at.isoformat(),
                close_reason=trade.close_reason or "RECONCILED_BROKER_FLAT"
            )
            await EventStoreManager.append_event(session, ev)
        elif event == "finish_close":
            trade.finish_close()
            trade.closed_at = datetime.utcnow()
            await session.flush()
            ev = CloseFilledEvent(
                trade_id=trade.id,
                closed_at=trade.closed_at.isoformat()
            )
            await EventStoreManager.append_event(session, ev)
        elif event == "reopen":
            trade.reopen()
            await session.flush()
            ev = CloseReopenedEvent(
                trade_id=trade.id
            )
            await EventStoreManager.append_event(session, ev)
            
        logger.info(
            f"[FSM reconcile_trade] Trade {trade.id} symbol={trade.symbol} transitioned {old_status} -> {trade.status} via {event}"
        )

    @classmethod
    async def expire_order(cls, session, po: PendingOrder) -> None:
        """Transition PendingOrder to EXPIRED and append corresponding event."""
        old_status = po.status
        po.status = "EXPIRED"
        await session.flush()
        
        ev = OrderExpiredEvent(pending_order_id=po.id)
        await EventStoreManager.append_event(session, ev)
        
        logger.info(f"[FSM expire_order] PendingOrder {po.id} transitioned {old_status} -> EXPIRED")
