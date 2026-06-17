"""
Transaction Manager FSM.

Orchestrates atomic state transitions for PendingOrder and Trade models.
Resolves and handles status mutations in a centralized way.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import select
from hermes.db.orm import PendingOrder, Trade, _compute_realized_pnl

logger = logging.getLogger("hermes.service1_agent.transaction_manager")


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
        """Create and place a pending order with status PENDING."""
        po = PendingOrder(
            strategy_id=strategy_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            payload=payload,
            status="PENDING"
        )
        session.add(po)
        logger.info(
            f"[FSM place_order] Created PendingOrder for {strategy_id} {symbol} side={side} qty={quantity}"
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
        """Transition a pending order to REJECTED."""
        return await cls._consume_pending(
            session=session,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side,
            terminal_status="REJECTED"
        )

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
        """Transition a pending order to SUBMITTED and create a Trade in OPEN state."""
        # First transition the matching pending order
        po = await cls._consume_pending(
            session=session,
            strategy_id=strategy_id,
            symbol=symbol,
            side=side,
            terminal_status="SUBMITTED"
        )
        # Create and add trade
        trade = Trade(**trade_fields)
        trade.status = "OPEN"
        session.add(trade)
        logger.info(
            f"[FSM fill] Trade created for {strategy_id} {symbol} (Trade status: {trade.status})"
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
        """Transition a pending order to SUBMITTED and transition the Trade to CLOSING or CLOSED."""
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
        return po

    @classmethod
    def reconcile_trade(
        cls,
        trade: Trade,
        event: str,
        close_reason: Optional[str] = None
    ) -> None:
        """Apply reconciler transitions on a Trade object."""
        old_status = trade.status
        if event == "force_close":
            trade.force_close()
            trade.closed_at = datetime.utcnow()
            if close_reason:
                trade.close_reason = close_reason
        elif event == "finish_close":
            trade.finish_close()
            trade.closed_at = datetime.utcnow()
        elif event == "reopen":
            trade.reopen()
        logger.info(
            f"[FSM reconcile_trade] Trade {trade.id} symbol={trade.symbol} transitioned {old_status} -> {trade.status} via {event}"
        )

    @classmethod
    def expire_order(cls, po: PendingOrder) -> None:
        """Transition PendingOrder to EXPIRED."""
        old_status = po.status
        po.status = "EXPIRED"
        logger.info(f"[FSM expire_order] PendingOrder {po.id} transitioned {old_status} -> EXPIRED")
