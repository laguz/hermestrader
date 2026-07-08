"""Approval dedup guard — ``has_pending_approval`` must also block re-queueing
once a proposal has been operator-approved but not yet executed.

Regression for a race between two independent loops: the reactive management
loop re-evaluates open trades every few seconds, while actual broker execution
of an approved action only happens on the slow heartbeat tick (``tick_interval_s``,
e.g. every 900s). ``has_pending_approval`` previously only counted ``PENDING``/
``PENDING_AI_REVIEW`` as in-flight, so the moment an operator approved a close
(status -> ``APPROVED``), the very next reactive tick — still seconds before the
heartbeat could execute it — saw no pending row and queued a fresh duplicate
approval for the same still-open trade, repeating every cycle.
"""
from __future__ import annotations

import pytest


@pytest.mark.anyio
async def test_has_pending_approval_blocks_while_approved_not_yet_executed(db):
    action_json = {
        "strategy_id": "CS7", "symbol": "IWM",
        "strategy_params": {"trade_id": 209, "close_reason": "TP-2pctW"},
    }
    approval_id = await db.approvals.queue_for_approval(action_json, action_type="management")

    assert await db.approvals.has_pending_approval("CS7", "IWM", None, None) is True

    assert await db.approvals.decide_approval(approval_id, "APPROVED") is True

    # Operator approved it, but the heartbeat hasn't executed it yet — a fresh
    # reactive-loop candidate for the same trade must still be deduped.
    assert await db.approvals.has_pending_approval("CS7", "IWM", None, None) is True

    await db.approvals.mark_approval_executed(approval_id, success=True)

    # Once genuinely executed, the slot is free again.
    assert await db.approvals.has_pending_approval("CS7", "IWM", None, None) is False
