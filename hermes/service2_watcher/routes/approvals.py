"""Approval queue endpoints — the operator's primary control surface.

Routes
------
- ``GET    /api/approvals``                   — list pending/decided trades
- ``POST   /api/approvals/{id}/approve``      — approve one
- ``POST   /api/approvals/{id}/reject``       — reject one
- ``POST   /api/approvals/bulk``              — approve or reject all PENDING
- ``PUT    /api/approval-mode``               — toggle approval-mode on/off

When approval mode is enabled, the agent writes proposed trades into
``pending_approvals`` instead of calling the broker. The next agent tick
picks up rows where ``status='APPROVED'`` and submits them.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .._app_state import SETTING_APPROVAL_MODE, db

router = APIRouter()


@router.get("/api/approvals")
def list_approvals(status: Optional[str] = None,
                   limit: int = 100) -> List[Dict[str, Any]]:
    return db.list_approvals(status=status, limit=min(limit, 500))


class ApprovalDecisionBody(BaseModel):
    notes: Optional[str] = None


@router.post("/api/approvals/{approval_id}/approve")
def approve_trade(approval_id: int,
                  body: ApprovalDecisionBody = ApprovalDecisionBody()
                  ) -> Dict[str, Any]:
    ok = db.decide_approval(approval_id, "APPROVED", notes=body.notes)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"Approval {approval_id} not found or not PENDING",
        )
    db.write_log(
        "ENGINE",
        f"[C2] Trade approval_id={approval_id} APPROVED by operator",
    )
    return {"status": "approved", "id": approval_id}


@router.post("/api/approvals/{approval_id}/reject")
def reject_trade(approval_id: int,
                 body: ApprovalDecisionBody = ApprovalDecisionBody()
                 ) -> Dict[str, Any]:
    ok = db.decide_approval(approval_id, "REJECTED", notes=body.notes)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"Approval {approval_id} not found or not PENDING",
        )
    db.write_log(
        "ENGINE",
        f"[C2] Trade approval_id={approval_id} REJECTED by operator"
        + (f": {body.notes}" if body.notes else ""),
    )
    return {"status": "rejected", "id": approval_id}


class BulkDecisionBody(BaseModel):
    action: str          # "approve" | "reject"
    notes: Optional[str] = None


@router.post("/api/approvals/bulk")
def bulk_decide(body: BulkDecisionBody) -> Dict[str, Any]:
    action = body.action.lower()
    if action not in ("approve", "reject"):
        raise HTTPException(
            status_code=400,
            detail="action must be 'approve' or 'reject'",
        )
    status = "APPROVED" if action == "approve" else "REJECTED"
    pending = db.list_approvals(status="PENDING", limit=500)
    count = 0
    for item in pending:
        if db.decide_approval(item["id"], status, notes=body.notes):
            count += 1
    db.write_log(
        "ENGINE",
        f"[C2] Bulk {status} — {count} trades by operator"
        + (f": {body.notes}" if body.notes else ""),
    )
    return {"status": status.lower(), "count": count}


class ApprovalModeBody(BaseModel):
    enabled: bool


@router.put("/api/approval-mode")
def set_approval_mode(body: ApprovalModeBody) -> Dict[str, Any]:
    db.set_setting(SETTING_APPROVAL_MODE, "true" if body.enabled else "false")
    db.write_log(
        "ENGINE",
        f"[C2] Approval mode {'ENABLED' if body.enabled else 'DISABLED'}",
    )
    return {"approval_mode": body.enabled}
