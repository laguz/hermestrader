from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Type
from pydantic import BaseModel, Field
from sqlalchemy import select

# Import EventLedger from its defining module (orm), not the re-exporter
# (models). models pulls in the repository mixins, one of which imports
# transaction_manager, which imports this module — importing from models here
# closes that cycle and breaks whenever events/control_state is imported before
# models. orm has no such back-edge.
from hermes.db.orm import EventLedger


class BaseEvent(BaseModel):
    """Base Event class for Event Sourcing."""
    event_id: Optional[int] = None
    created_at: Optional[datetime] = None
    event_version: int = 1

    @classmethod
    def upgrade_payload(cls, payload: Dict[str, Any], from_version: int) -> Dict[str, Any]:
        """Hook for subclass-specific migrations of older payload versions."""
        return payload



class OrderSubmittedEvent(BaseEvent):
    """Event triggered when a pending order is submitted."""
    id: int
    strategy_id: str
    symbol: str
    side: str
    quantity: int
    payload: Dict[str, Any] = Field(default_factory=dict)
    submitted_at: str


class OrderFilledEvent(BaseEvent):
    """Event triggered when a pending order is filled at the broker."""
    pending_order_id: int
    trade_id: int
    trade_fields: Dict[str, Any]


class OrderRejectedEvent(BaseEvent):
    """Event triggered when a pending order is rejected."""
    pending_order_id: int


class OrderExpiredEvent(BaseEvent):
    """Event triggered when a pending order is expired."""
    pending_order_id: int


class CloseSubmittedEvent(BaseEvent):
    """Event triggered when a close order is submitted for a trade."""
    pending_order_id: int
    trade_id: int
    exit_price: Optional[float] = None
    close_reason: str
    close_tag: Optional[str] = None


class CloseFilledEvent(BaseEvent):
    """Event triggered when a close order is filled."""
    trade_id: int
    closed_at: str


class CloseReopenedEvent(BaseEvent):
    """Event triggered when a close order is cancelled and the trade is re-opened."""
    trade_id: int


class ReconcileFlatEvent(BaseEvent):
    """Event triggered when a trade is reconciled to flat (closed)."""
    trade_id: int
    closed_at: str
    close_reason: str = "RECONCILED_BROKER_FLAT"


class DoctrineUpdatedEvent(BaseEvent):
    """Event triggered when the doctrine/operator rules are updated."""
    doctrine_text: str
    updated_at: str


class SystemSettingChangedEvent(BaseEvent):
    """Event triggered when a system setting is modified."""
    key: str
    value: str
    updated_at: str


class WatchlistChangedEvent(BaseEvent):
    """Event triggered when a watchlist is modified."""
    strategy_id: str
    symbols: List[str]
    updated_at: str


class ModeChangedEvent(BaseEvent):
    """Event triggered when trading mode changes."""
    mode: str
    updated_at: str


class StrategyToggledEvent(BaseEvent):
    """Event triggered when a strategy is toggled."""
    strategy_id: str
    enabled: bool
    updated_at: str


class AutonomyChangedEvent(BaseEvent):
    """Event triggered when agent autonomy changes."""
    autonomy: str
    updated_at: str


class PauseChangedEvent(BaseEvent):
    """Event triggered when agent pause state changes."""
    paused: bool
    updated_at: str


class ApprovalDecidedEvent(BaseEvent):
    """Event triggered when an approval is decided."""
    approval_id: int
    status: str
    notes: Optional[str] = None
    decided_at: Optional[str] = None
    executed_at: Optional[str] = None


class MlRetrainTick(BaseEvent):
    """Internal timer event to trigger ML retraining."""
    force: bool = False


class CacheWarmTick(BaseEvent):
    """Internal timer event to warm quote/chain cache."""
    pass


class ChartRefreshTick(BaseEvent):
    """Internal timer event to trigger chart vision analysis."""
    pass


class ClockTickEvent(BaseEvent):
    """Internal timer event driving CascadingEngine periodic sweep."""
    pass


# Event Type Registry mapping to database event_type values
EVENT_TYPE_TO_CLASS: Dict[str, Type[BaseEvent]] = {
    "ORDER_SUBMITTED": OrderSubmittedEvent,
    "ORDER_FILLED": OrderFilledEvent,
    "ORDER_REJECTED": OrderRejectedEvent,
    "ORDER_EXPIRED": OrderExpiredEvent,
    "CLOSE_SUBMITTED": CloseSubmittedEvent,
    "CLOSE_FILLED": CloseFilledEvent,
    "CLOSE_REOPEN": CloseReopenedEvent,
    "RECONCILE_FLAT": ReconcileFlatEvent,
    "DOCTRINE_UPDATED": DoctrineUpdatedEvent,
    "SYSTEM_SETTING_CHANGED": SystemSettingChangedEvent,
    "WATCHLIST_CHANGED": WatchlistChangedEvent,
    "MODE_CHANGED": ModeChangedEvent,
    "STRATEGY_TOGGLED": StrategyToggledEvent,
    "AUTONOMY_CHANGED": AutonomyChangedEvent,
    "PAUSE_CHANGED": PauseChangedEvent,
    "APPROVAL_DECIDED": ApprovalDecidedEvent,
    "ML_RETRAIN_TICK": MlRetrainTick,
    "CACHE_WARM_TICK": CacheWarmTick,
    "CHART_REFRESH_TICK": ChartRefreshTick,
    "CLOCK_TICK_EVENT": ClockTickEvent,
}

CLASS_TO_EVENT_TYPE: Dict[Type[BaseEvent], str] = {
    v: k for k, v in EVENT_TYPE_TO_CLASS.items()
}


def deserialize_event(
    event_type: str,
    payload: Dict[str, Any],
    event_id: Optional[int] = None,
    created_at: Optional[datetime] = None,
) -> Optional[BaseEvent]:
    """Deserialize a database event payload into its structured class after upgrading it."""
    cls = EVENT_TYPE_TO_CLASS.get(event_type)
    if not cls:
        return None

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return None

    # Handle older payloads that do not have event_version
    from_version = payload.get("event_version", 1)

    # Run the model-specific payload upgrader
    upgraded_payload = cls.upgrade_payload(payload.copy(), from_version)

    # Ensure event_version field is present and correct in the upgraded payload
    if "event_version" not in upgraded_payload:
        upgraded_payload["event_version"] = cls.model_fields["event_version"].default

    event = cls(**upgraded_payload)
    event.event_id = event_id
    event.created_at = created_at
    return event


class EventStoreManager:
    """Manages appending and loading events from the DB EventLedger."""

    @staticmethod
    async def append_event(session, event: BaseEvent) -> int:
        """Serialize and save an event to the EventLedger."""
        event_type = CLASS_TO_EVENT_TYPE.get(event.__class__)
        if not event_type:
            raise ValueError(f"Unknown event class: {event.__class__}")
            
        payload = event.model_dump(exclude={"event_id", "created_at"})

        # ``id`` is BIGSERIAL — Postgres assigns it on flush.
        row = EventLedger(
            event_type=event_type,
            payload=payload
        )
        session.add(row)
        await session.flush()

        event.event_id = row.id
        event.created_at = row.created_at
        return row.id

    @staticmethod
    async def record_event(session, event: BaseEvent) -> int:
        """Append an event AND apply its projection in the same transaction.

        This is the single write path for the event-sourcing migration: the
        ``event_ledger`` row and the read-model rows it implies (trades,
        pending_orders, system_settings, …) are written under one transaction,
        so the read models are always a deterministic function of the log and
        can never diverge from it on a partial failure. The caller owns the
        commit (so several events can be recorded atomically before one commit).

        Global ordering is carried by ``EventLedger.id`` — a Postgres
        ``BIGSERIAL`` the database assigns on flush.
        """
        # Imported here, not at module scope: projections.py imports the event
        # classes from this module, so a top-level import would be circular.
        from hermes.db.repositories.projections import ProjectionsRepository

        event_id = await EventStoreManager.append_event(session, event)
        await ProjectionsRepository.apply_event_projection(session, event)
        return event_id

    @staticmethod
    async def load_events(session, start_id: int = 0) -> List[BaseEvent]:
        """Load and deserialize events from the EventLedger sorted by ID."""
        q = select(EventLedger).where(EventLedger.id >= start_id).order_by(EventLedger.id.asc())
        result = await session.execute(q)
        rows = result.scalars().all()
        
        events = []
        for row in rows:
            event = deserialize_event(row.event_type, row.payload, row.id, row.created_at)
            if event:
                events.append(event)
            
        return events
