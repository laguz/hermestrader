from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple
from hermes.db.orm import Trade, PendingOrder, _compute_realized_pnl

logger = logging.getLogger("hermes.db.projectors")


class StateProjector:
    """Projector to reconstruct the active state of Trades and PendingOrders from EventLedger records."""

    @staticmethod
    def project(events: List[Any]) -> Tuple[List[PendingOrder], List[Trade]]:
        """Project current active PendingOrders and Trades from a sequence of ledger events.
        
        `events` is a list of objects (or dicts) with event_type and payload attributes.
        """
        pending_orders: Dict[int, PendingOrder] = {}  # keyed by pending_order_id
        trades: Dict[int, Trade] = {}                # keyed by trade_id

        for ev in events:
            ev_type = getattr(ev, "event_type", None) or ev.get("event_type")
            payload = getattr(ev, "payload", None) or ev.get("payload") or {}
            
            if ev_type == "ORDER_SUBMITTED":
                po_id = payload["id"]
                po = PendingOrder(
                    id=po_id,
                    strategy_id=payload["strategy_id"],
                    symbol=payload["symbol"],
                    side=payload["side"],
                    quantity=payload["quantity"],
                    payload=payload["payload"],
                    status="PENDING",
                    submitted_at=datetime.fromisoformat(payload["submitted_at"]) if "submitted_at" in payload else datetime.utcnow()
                )
                pending_orders[po_id] = po
                
            elif ev_type == "ORDER_FILLED":
                po_id = payload.get("pending_order_id")
                if po_id in pending_orders:
                    pending_orders[po_id].status = "SUBMITTED"
                
                trade_id = payload["trade_id"]
                tf = payload["trade_fields"]
                t = Trade(
                    id=trade_id,
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
                    opened_at=datetime.fromisoformat(tf["opened_at"]) if "opened_at" in tf else datetime.utcnow()
                )
                trades[trade_id] = t
                
            elif ev_type == "ORDER_REJECTED":
                po_id = payload.get("pending_order_id")
                if po_id in pending_orders:
                    pending_orders[po_id].status = "REJECTED"
                    
            elif ev_type == "ORDER_EXPIRED":
                po_id = payload.get("pending_order_id")
                if po_id in pending_orders:
                    pending_orders[po_id].status = "EXPIRED"

            elif ev_type == "CLOSE_SUBMITTED":
                po_id = payload.get("pending_order_id")
                if po_id in pending_orders:
                    pending_orders[po_id].status = "SUBMITTED"
                
                trade_id = payload["trade_id"]
                if trade_id in trades:
                    t = trades[trade_id]
                    t.status = "CLOSING"
                    t.close_reason = payload.get("close_reason")
                    t.close_tag = payload.get("close_tag")
                    t.exit_price = payload.get("exit_price")
                    t.pnl = _compute_realized_pnl(
                        entry_credit=t.entry_credit,
                        entry_debit=t.entry_debit,
                        exit_price=t.exit_price,
                        lots=int(t.lots or 0)
                    )

            elif ev_type == "CLOSE_FILLED":
                trade_id = payload["trade_id"]
                if trade_id in trades:
                    t = trades[trade_id]
                    t.status = "CLOSED"
                    t.closed_at = datetime.fromisoformat(payload["closed_at"]) if "closed_at" in payload else datetime.utcnow()

            elif ev_type == "CLOSE_REOPEN":
                trade_id = payload["trade_id"]
                if trade_id in trades:
                    t = trades[trade_id]
                    t.status = "OPEN"

            elif ev_type == "RECONCILE_FLAT":
                trade_id = payload["trade_id"]
                if trade_id in trades:
                    t = trades[trade_id]
                    t.status = "CLOSED"
                    t.close_reason = payload.get("close_reason", "RECONCILED_BROKER_FLAT")
                    t.closed_at = datetime.fromisoformat(payload["closed_at"]) if "closed_at" in payload else datetime.utcnow()

        return list(pending_orders.values()), list(trades.values())
