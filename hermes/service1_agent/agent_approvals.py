"""
[Service-1: Hermes-Agent-Core] — C2-approved order execution.

Split out of ``main.py`` so the agent entry point keeps only the run loop and
process wiring. ``main`` re-imports these names, so existing call-sites and
test monkeypatches (``hermes.service1_agent.main.X``) keep working unchanged.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

log = logging.getLogger("hermes.agent.main")


def _action_expired(strategy_params: Optional[Dict[str, Any]]) -> bool:
    """True when the action's ``valid_until`` stamp (if any) has passed.

    Time-sensitive strategies (DS0) stamp their entries so an approval acted
    on hours after the trigger expires instead of executing stale. Absent or
    unparseable stamps never block — this is a strategy opt-in, not a gate.
    """
    raw = (strategy_params or {}).get("valid_until")
    if not raw:
        return False
    try:
        valid_until = datetime.fromisoformat(str(raw))
    except ValueError:
        return False
    if valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=timezone.utc)
    from hermes.utils import utc_now
    now = utc_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now > valid_until


# Tradier order statuses that mean the broker did NOT accept the order; the
# approval row must NOT be flipped to EXECUTED for any of these.
_REJECTED_ORDER_STATUSES = {"rejected", "error", "expired", "canceled", "cancelled"}


async def _execute_approved_action(item: Dict[str, Any], *, broker, db) -> str:
    """Execute one C2-approved action and reconcile its approval row.

    Returns one of: ``"executed"``, ``"preview"``, ``"rejected"``, ``"failed"``.
    Exposed at module scope so the lifecycle is unit-testable without
    standing up the full tick loop.

    The approval row's final state must always reflect what the broker
    actually did:

    * ``dry_run=True`` → no broker call; mark FAILED with a preview note so
      the C2 UI cannot mistake a preview for a live order.
    * Broker raises  → ``record_order_response`` rolls the PendingOrder
      back to REJECTED so capacity recovers; approval marked FAILED.
    * Broker returns ``errors`` / a rejected status → approval marked
      FAILED; ``record_order_response`` already wrote ``[ORDER REJECTED]``.
    * Clean response → approval marked EXECUTED and ``[C2 EXECUTED]`` is
      written for the operator feed.
    """
    from hermes.service1_agent.core import TradeAction, AsyncBrokerWrapper

    async_broker = AsyncBrokerWrapper(broker, db)

    approval_id = item["id"]
    action_json = item["action_json"]
    try:
        action = TradeAction(**action_json)
    except Exception as exc:
        log.exception("[C2] Failed to rebuild TradeAction id=%d: %s",
                      approval_id, exc)
        await db.approvals.mark_approval_executed(
            approval_id, success=False,
            notes=f"action rebuild error: {exc}",
        )
        return "failed"

    if _action_expired(action.strategy_params):
        await db.approvals.mark_approval_executed(
            approval_id, success=False,
            notes="entry TTL expired — trigger is stale, not executed",
        )
        log.info("[C2] EXPIRED — approval id=%d past its valid_until stamp",
                 approval_id)
        await db.logs.write_log(
            action.strategy_id,
            f"[C2 EXPIRED] {action.symbol} approval_id={approval_id} — "
            f"entry TTL expired; order NOT sent to broker",
        )
        return "expired"

    # Market-hours gate — C2-approved trades must respect the same
    # off-hours block as strategy-emitted ones. Leave the approval row
    # in PENDING (do NOT mark FAILED) so the next tick during regular
    # session picks it up automatically.
    from hermes.market_hours import should_block_trades
    blocked, reason = should_block_trades()
    if blocked:
        log.info("[C2] OFF-HOURS — deferring approval id=%d (%s)",
                 approval_id, reason)
        await db.logs.write_log(
            action.strategy_id,
            f"[C2 DEFERRED] {action.symbol} approval_id={approval_id} — "
            f"{reason}; will execute on next tick during regular session",
        )
        return "deferred"

    broker_dry_run = bool(getattr(broker, "dry_run", False))
    if broker_dry_run:
        # No broker call happens — don't pretend it did.  Skip
        # record_pending_order so capacity isn't consumed by a row
        # that will never settle.
        await db.approvals.mark_approval_executed(
            approval_id, success=False,
            notes="dry_run=True — no broker order placed",
        )
        log.info("[C2] dry_run preview only — approval id=%d "
                 "NOT submitted to broker", approval_id)
        await db.logs.write_log(
            action.strategy_id,
            f"[C2 PREVIEW] {action.symbol} {action.order_class} "
            f"qty={action.quantity} approval_id={approval_id} — "
            f"dry_run=True, no order sent to broker",
        )
        return "preview"

    is_pure_close = bool(action.legs) and all(
        "to_close" in (leg.get("side") or "").lower() or "to_close" in (leg.get("action") or "").lower()
        for leg in action.legs
    )
    close_method = getattr(db.trades, "close_trade_from_action", None)

    await db.trades.record_pending_order(action)
    # Same order-replacement contract as _engine_pipeline._execute_or_queue:
    # cancel the superseded resting close first, and abort if the cancel
    # fails (it may have just filled — placing anyway double-closes).
    replace_oid = (action.strategy_params or {}).get("replace_broker_order_id")
    if replace_oid is not None:
        try:
            await async_broker.cancel_order(str(replace_oid))
        except Exception as exc:
            err = {"errors": f"replace-cancel failed for order {replace_oid}: {exc}"}
            if is_pure_close and close_method is not None:
                await close_method(action, err)
            else:
                await db.trades.record_order_response(action, err)
            await db.approvals.mark_approval_executed(
                approval_id, success=False,
                notes=f"replace-cancel failed: {exc}",
            )
            log.warning("[C2] replace-cancel failed for approval id=%d "
                        "order %s: %s — replacement NOT placed",
                        approval_id, replace_oid, exc)
            return "failed"
    try:
        resp = await async_broker.place_order_from_action(action)
    except Exception as exc:
        if is_pure_close and close_method is not None:
            await close_method(action, {"errors": str(exc)})
        else:
            await db.trades.record_order_response(action, {"errors": str(exc)})
        await db.approvals.mark_approval_executed(
            approval_id, success=False,
            notes=f"broker raised: {exc}",
        )
        log.exception("[C2] place_order_from_action raised for "
                      "approval id=%d: %s", approval_id, exc)
        await db.logs.write_log(
            action.strategy_id,
            f"[C2 FAILED] {action.symbol} approval_id={approval_id} "
            f"broker raised: {exc}",
        )
        return "failed"

    if is_pure_close and close_method is not None:
        await close_method(action, resp)
    else:
        await db.trades.record_order_response(action, resp)

    order = (resp or {}).get("order") if isinstance(resp, dict) else None
    order_status = ""
    if isinstance(order, dict):
        order_status = str(order.get("status", "")).lower()
    rejected = (
        (isinstance(resp, dict) and "errors" in resp)
        or order_status in _REJECTED_ORDER_STATUSES
    )

    if rejected:
        # record_order_response already wrote [ORDER REJECTED].
        await db.approvals.mark_approval_executed(
            approval_id, success=False,
            notes=f"broker rejected: {resp}",
        )
        log.warning("[C2] broker rejected approval id=%d: %s",
                    approval_id, resp)
        await db.logs.write_log(
            action.strategy_id,
            f"[C2 REJECTED] {action.symbol} approval_id={approval_id}",
        )
        return "rejected"

    await db.approvals.mark_approval_executed(approval_id, success=True)
    log.info("[C2] Executed approved trade: %s %s strategy=%s id=%d",
             action.symbol, action.order_class, action.strategy_id, approval_id)
    await db.logs.write_log(
        action.strategy_id,
        f"[C2 EXECUTED] {action.symbol} {action.order_class} "
        f"qty={action.quantity} approval_id={approval_id}",
    )
    return "executed"
