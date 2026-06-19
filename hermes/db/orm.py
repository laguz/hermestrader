"""
[TimescaleDB-Schema] — authoritative SQLAlchemy ORM for the persistence layer.

This module holds the declarative ``Base``, every table class, and the pure
(DB-free) helper functions. It deliberately imports nothing from
``hermes.db.repositories`` or ``hermes.db.models`` so the repository mixins
can import their ORM types from here without a circular import.

The ORM is the **single source of truth** for table/column structure:
``create_all`` provisions every table here on Postgres/Timescale, the Alembic
baseline generates its tables from this metadata, and the boot-time reconciler
(``HermesDB.run_migrations``) derives its column self-heal from it. The only
schema that lives outside the ORM is the irreducible
TimescaleDB layer the ORM cannot express — hypertables, compression policies,
the ``pnl_daily`` view, and the two raw ``bars_*`` tables — which lives in
``schema.sql`` and is applied *after* ``create_all``. ``tests/test_schema_parity.py``
guards that remaining seam (every hypertable-backed ORM table has its
``create_hypertable`` line, and ``schema.sql`` never re-declares an ORM table).

``hermes.db.models`` re-exports every public name defined here, so existing
``from hermes.db.models import Base, Trade, ...`` call-sites keep working.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from hermes.common import close_reason_from_tag
from hermes.utils import utc_now

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime, Float, ForeignKey, Index,
    Integer, Numeric, Sequence, String, Text, text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, reconstructor
from transitions import Machine

logger = logging.getLogger("hermes.db")


class Base(DeclarativeBase):
    pass


class Strategy(Base):
    __tablename__ = "strategies"
    strategy_id = Column(String, primary_key=True)
    priority = Column(Integer, nullable=False)
    status = Column(String, nullable=False, default="ACTIVE")
    created_at = Column(DateTime(timezone=True), default=utc_now)


class StrategyWatchlist(Base):
    __tablename__ = "strategy_watchlists"
    strategy_id = Column(String, ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
                         primary_key=True)
    symbol = Column(String, primary_key=True)
    target_lots = Column(Integer)
    added_at = Column(DateTime(timezone=True), default=utc_now)

    __table_args__ = (
        Index("idx_strategy_watchlists_sid", "strategy_id"),
    )


class Trade(Base):
    __tablename__ = "trades"
    # `id` belongs to the schema's `BIGSERIAL` (sequence `trades_id_seq`).
    # Both this column AND `opened_at` form the composite PK because the
    # underlying TimescaleDB hypertable partitions by `opened_at`.
    # Marking the Sequence here is what tells SQLAlchemy to fetch a value
    # via RETURNING instead of inserting NULL.
    id = Column(BigInteger, Sequence("trades_id_seq"), primary_key=True,
                autoincrement=True)
    opened_at = Column(DateTime(timezone=True), default=utc_now,
                       primary_key=True)
    strategy_id = Column(String, ForeignKey("strategies.strategy_id"), nullable=False)
    symbol = Column(String, nullable=False)
    side_type = Column(String, nullable=False)
    short_leg = Column(String)
    long_leg = Column(String)
    short_strike = Column(Numeric(10, 4))
    long_strike = Column(Numeric(10, 4))
    width = Column(Numeric(10, 4))
    lots = Column(Integer, nullable=False)
    entry_credit = Column(Numeric(10, 4))
    entry_debit = Column(Numeric(10, 4))
    expiry = Column(Date)
    status = Column(String, nullable=False, default="PROPOSED")
    pnl = Column(Numeric(12, 2))
    closed_at = Column(DateTime(timezone=True))
    close_reason = Column(String)
    ai_authored = Column(Boolean, default=False)
    ai_rationale = Column(Text)
    broker_order_id = Column(String)
    # Strategy-tag bookkeeping. ``tag`` is the entry-order tag
    # (e.g. ``HERMES_CS75``); ``close_tag`` is the closing-order tag
    # (e.g. ``HERMES_CS75_CLOSE_TP-50``). ``exit_price`` is the closing
    # fill price; combined with ``entry_credit``/``entry_debit`` and
    # ``lots`` it gives realized P&L (see ``_compute_realized_pnl``).
    tag = Column(String)
    close_tag = Column(String)
    exit_price = Column(Numeric(10, 4))
    # Phase-0 outcome instrumentation: a JSON snapshot of the resolved
    # tunables (the "knobs") plus the market context at entry — POP, short
    # delta, width, credit, DTE. Combined with the realized ``pnl`` on close,
    # this is the labelled ``(context, knobs, outcome)`` row the outcome-driven
    # tuner / contextual bandit trains on. Nullable: trades opened before this
    # column existed, and any path that can't assemble it, simply leave it NULL.
    entry_features = Column(JSONB)

    __table_args__ = (
        Index("idx_trades_strategy_status", "strategy_id", "status", "symbol"),
        # Partial index used to look up the OPEN trade for a broker order id —
        # ``postgresql_where`` lets create_all own it instead of a hand-written
        # CREATE INDEX.
        Index("idx_trades_open_order_id", "broker_order_id",
              postgresql_where=text("status = 'OPEN'")),
    )

    STATES = ["PROPOSED", "PENDING_BROKER", "PARTIAL_FILL", "OPEN", "CLOSING", "CLOSED"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_fsm()

    @reconstructor
    def _init_fsm(self):
        # Prevent recreating machine if called multiple times
        if hasattr(self, 'machine'):
            return

        # The FSM will bind directly to `self.status`
        initial_state = getattr(self, 'status', None) or "PROPOSED"
        self.machine = Machine(
            model=self,
            states=self.STATES,
            initial=initial_state,
            model_attribute='status',
            send_event=True,
            ignore_invalid_triggers=False
        )

        # Valid State Transitions
        self.machine.add_transition('submit_to_broker', 'PROPOSED', 'PENDING_BROKER')
        self.machine.add_transition('broker_reject', 'PENDING_BROKER', 'CLOSED')
        self.machine.add_transition('partial_fill', 'PENDING_BROKER', 'PARTIAL_FILL')
        self.machine.add_transition('fill', ['PENDING_BROKER', 'PARTIAL_FILL'], 'OPEN')
        self.machine.add_transition('begin_close', 'OPEN', 'CLOSING')
        self.machine.add_transition('finish_close', 'CLOSING', 'CLOSED')
        # Re-arm a close that was submitted but never took (async reject /
        # cancel): the broker still holds the position, so it must return to
        # OPEN to be eligible for another close attempt.
        self.machine.add_transition('reopen', 'CLOSING', 'OPEN')
        self.machine.add_transition('force_close', '*', 'CLOSED')


class ExitTick(Base):
    """Per-tick exit-state trajectory for an open position (Phase 3).

    One row per OPEN trade per management tick while the exit-policy capture is
    enabled. Together with the trade's eventual realized P&L these rows form the
    ``(state, action, return)`` trajectories the offline exit policy learns
    from — the data prerequisite for sequential exit-timing RL.

    ``action`` is ``'hold'`` (the rules kept the position this tick) or
    ``'close'`` (a close was issued this tick). ``unrealized_pnl_pct`` is the
    fraction of entry credit currently retained as profit:
    ``(entry_credit - spread_mid) / entry_credit``.

    PK is a plain autoincrement Integer (not the BIGSERIAL/Sequence the other
    hypertables use); SQLAlchemy maps it to ``BIGSERIAL`` on Postgres.
    """

    __tablename__ = "exit_ticks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime(timezone=True), default=utc_now, nullable=False)
    trade_id = Column(BigInteger, nullable=False)
    strategy_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    dte = Column(Integer)
    unrealized_pnl_pct = Column(Float)
    debit = Column(Float)
    entry_credit = Column(Float)
    action = Column(String, nullable=False, default="hold")   # 'hold' | 'close'
    close_reason = Column(String)

    __table_args__ = (
        Index("idx_exit_ticks_trade", "trade_id", "ts"),
    )


class PendingOrder(Base):
    __tablename__ = "pending_orders"
    # Same shape as Trade: composite PK over the BIGSERIAL id and the
    # hypertable's partitioning column.
    id = Column(BigInteger, Sequence("pending_orders_id_seq"), primary_key=True,
                autoincrement=True)
    submitted_at = Column(DateTime(timezone=True), default=utc_now,
                          primary_key=True)
    strategy_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    payload = Column(JSONB, nullable=False)
    status = Column(String, nullable=False, default="PENDING")


class PendingApproval(Base):
    """Human-approval queue for proposed agent trades.

    The agent writes a row here (status=PENDING) instead of calling the broker
    when approval_mode is enabled.  The C2 panel approves or rejects; the
    agent's tick loop executes APPROVED rows and marks them EXECUTED.
    """
    __tablename__ = "pending_approvals"
    id = Column(BigInteger, Sequence("pending_approvals_id_seq"), primary_key=True,
                autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    strategy_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    action_type = Column(String, nullable=False, default="entry")
    action_json = Column(JSONB, nullable=False)
    status = Column(String, nullable=False, default="PENDING")
    notes = Column(Text)
    decided_at = Column(DateTime(timezone=True))
    executed_at = Column(DateTime(timezone=True))
    expires_at = Column(DateTime(timezone=True))


class VetoSuppression(Base):
    """Short-lived record of an overseer VETO so the rules engine stops
    brute-forcing the identical entry every tick.

    A veto consumes no capacity (no Trade/PendingOrder row is written), so
    without this table ``side_aware_capacity`` reports full headroom next
    tick and the strategy re-proposes the same action — which the overseer
    re-vetoes, burning an LLM call each cycle. ``submit`` consults
    ``active_veto`` before emitting a review request and skips the
    re-proposal while a suppression is unexpired.

    Regular (non-hypertable) table: low volume, keyed random-access lookups.
    """
    __tablename__ = "veto_suppressions"
    id = Column(BigInteger, Sequence("veto_suppressions_id_seq"), primary_key=True,
                autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    strategy_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    # NULL side_type/expiry = symbol-wide veto (matches any); otherwise the
    # field must match the incoming action exactly.
    side_type = Column(String)
    expiry = Column(String)
    rationale = Column(Text)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    # Repeat vetoes on the same key bump this and extend the window (backoff).
    hits = Column(Integer, nullable=False, default=1)

    __table_args__ = (
        Index("idx_veto_suppressions_lookup", "strategy_id", "symbol", "expires_at"),
    )


class BotLog(Base):
    __tablename__ = "bot_logs"
    ts = Column(DateTime(timezone=True), default=utc_now, primary_key=True)
    strategy_id = Column(String, nullable=False, primary_key=True)
    level = Column(String, default="INFO")
    message = Column(Text, nullable=False)

    __table_args__ = (
        Index("idx_bot_logs_strategy", "strategy_id", ts.desc()),
    )


class OperatorCommand(Base):
    """Durable command queue from Service-2 (watcher) to Service-1 (agent).

    The watcher never writes canonical state directly; it appends an *intent*
    here (the one table it owns as a writer) and the agent drains it, applying
    each command in its own process via the normal event-sourced write path
    (``record_event`` → ledger + projection). This keeps the single-writer
    invariant intact — the agent is the sole writer of ``event_ledger`` and the
    read models — while surviving agent downtime: a command issued while the
    agent is offline is still ``PENDING`` and applied on the next drain.

    ``status`` flows ``PENDING`` → ``APPLIED`` / ``FAILED``. Apply is idempotent
    (settings are last-write-wins; ``decide_approval`` no-ops off PENDING), so a
    crash between commit and ``mark_applied`` is safe to re-drain.
    """
    __tablename__ = "operator_commands"
    id = Column(BigInteger, Sequence("operator_commands_id_seq"), primary_key=True,
                autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    command_type = Column(String, nullable=False)
    payload = Column(JSONB, nullable=False)
    status = Column(String, nullable=False, default="PENDING")
    applied_at = Column(DateTime(timezone=True))
    error = Column(Text)

    __table_args__ = (
        Index("idx_operator_commands_status_id", "status", "id"),
    )


class EventLedger(Base):
    """Append-only event store — the source-of-truth log for event sourcing.

    Read models (trades, pending_orders, system_settings, …) are projections
    of this log; global event order is carried by ``id`` (a Postgres BIGSERIAL).
    """
    __tablename__ = "event_ledger"
    id = Column(BigInteger, Sequence("event_ledger_id_seq"), primary_key=True,
                autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=utc_now,
                        primary_key=True)
    event_type = Column(String, nullable=False)
    payload = Column(JSONB, nullable=False)

    __table_args__ = (
        Index("idx_event_ledger_type", "event_type", "id"),
    )


class AIDecision(Base):
    __tablename__ = "ai_decisions"
    ts = Column(DateTime(timezone=True), default=utc_now, primary_key=True)
    strategy_id = Column(String)
    symbol = Column(String, primary_key=True)
    autonomy = Column(String, nullable=False)
    decision = Column(JSONB, nullable=False)


class Prediction(Base):
    __tablename__ = "predictions"
    ts = Column(DateTime(timezone=True), default=utc_now, primary_key=True)
    symbol = Column(String, nullable=False, primary_key=True)
    predicted_return = Column(Numeric(12, 6))
    predicted_price = Column(Numeric(12, 4))
    spot = Column(Numeric(12, 4))
    model_tag = Column(String, default="xgb-10feat-v1")

    __table_args__ = (
        Index("idx_predictions_symbol_ts", "symbol", ts.desc()),
    )


class SystemSetting(Base):
    """Small key/value table the agent and watcher both read.

    Used for shared runtime state that the watcher must be able to flip
    without restarting the agent, e.g. the live/paper trading mode and the
    rolling Tradier API health timestamps the agent writes after each tick.
    """
    __tablename__ = "system_settings"
    key = Column(String, primary_key=True)
    value = Column(Text, nullable=False, default="")
    updated_at = Column(DateTime(timezone=True), default=utc_now,
                        onupdate=utc_now)


# ---------------------------------------------------------------------------
# Realized-PnL + tag helpers (module-level so they're trivially testable
# without a live DB).
# ---------------------------------------------------------------------------
def _close_reason_from_tag(tag: Optional[str]) -> Optional[str]:
    """Recover the close reason that a strategy embedded in its order tag.

    Thin re-export of the canonical tag matcher in ``hermes.common`` so the
    ``HERMES_<STRAT>_CLOSE_<REASON>`` shape and the Tradier ``_``↔``-`` quirk
    live in exactly one place (CLAUDE.md safety rule #5).
    """
    return close_reason_from_tag(tag)


def _compute_realized_pnl(*, entry_credit, entry_debit,
                          exit_price, lots: int) -> Optional[float]:
    """Realized P&L on an option spread, in dollars (1 contract = 100 sh).

    For a credit spread: pnl = (entry_credit − exit_debit) × lots × 100.
    For a debit  spread: pnl = (exit_credit − entry_debit) × lots × 100.

    Returns ``None`` if the inputs are insufficient to compute (e.g. the
    closing fill price wasn't supplied) — analytics already treats NULL
    correctly, so we'd rather show "unknown" than a fabricated 0.
    """
    if exit_price is None or not lots:
        return None
    try:
        lots_i = int(lots)
        exit_f = float(exit_price)
    except (TypeError, ValueError):
        return None
    if entry_credit is not None:
        try:
            ec = float(entry_credit)
        except (TypeError, ValueError):
            return None
        return round((ec - exit_f) * lots_i * 100.0, 2)
    if entry_debit is not None:
        try:
            ed = float(entry_debit)
        except (TypeError, ValueError):
            return None
        return round((exit_f - ed) * lots_i * 100.0, 2)
    return None


def sync_to_async_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    return dsn
