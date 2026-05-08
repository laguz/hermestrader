"""Admin / self-update surface.

Routes
------
- ``GET  /api/admin/instance``  — which instance is this (paper / live, project name, mode, version)
- ``POST /api/admin/upgrade``   — request a host-side rebuild of this container
- ``GET  /api/admin/upgrade``   — current upgrade marker state (queued / in_progress / idle)

Design
------
The container itself cannot rebuild itself — Docker doesn't have
turtles all the way down. So ``POST /api/admin/upgrade`` writes a
marker file to the shared ``HERMES_DATA_DIR`` volume:

    /data/upgrade_requested

A host-side watcher (``scripts/upgrade_runner.sh``) polls that file
and runs ``git pull && docker compose build && docker compose up -d``.
The watcher updates the same marker to track state, so the API can
expose progress back to Hermes.

Security
--------
This endpoint mutates the host indirectly. Two guard rails:

1. Calls are rejected unless the request originates from a trusted
   network — by default the in-cluster Docker bridge or localhost.
   Override with ``HERMES_ADMIN_ALLOW_CIDRS`` (comma-separated CIDRs).

2. The container drops all Linux capabilities, so even if the
   endpoint were tricked into running a shell command, it has no
   privileges to act on the host directly. The host-side runner is
   the only thing with build authority, and it only runs commands it
   reads from a fixed allow-list (see scripts/upgrade_runner.sh).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from ipaddress import ip_address, ip_network
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger("hermes.c2.admin")
router = APIRouter()


# ── Configuration (env-driven so paper and live can diverge) ──────────

_DEFAULT_ALLOW_CIDRS = "127.0.0.1/32,::1/128,172.16.0.0/12,10.0.0.0/8,192.168.0.0/16"


def _allow_cidrs() -> List[ip_network]:
    raw = os.environ.get("HERMES_ADMIN_ALLOW_CIDRS", _DEFAULT_ALLOW_CIDRS)
    nets: List[ip_network] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            nets.append(ip_network(chunk, strict=False))
        except ValueError:
            logger.warning("admin: ignoring invalid CIDR %r", chunk)
    return nets


def _marker_path() -> Path:
    return Path(os.environ.get("HERMES_UPGRADE_MARKER", "/data/upgrade_requested"))


def _require_trusted(request: Request) -> None:
    """Reject requests that don't originate from a trusted CIDR.

    We trust the X-Forwarded-For header *only* if the immediate peer is
    already trusted — otherwise an attacker could spoof the header from
    the open internet. In the default deployment the peer is always the
    Docker bridge, so this is a defence-in-depth check.
    """
    peer = request.client.host if request.client else None
    if peer is None:
        raise HTTPException(status_code=403, detail="no peer address")
    try:
        peer_addr = ip_address(peer)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=f"bad peer address: {peer}") from exc

    nets = _allow_cidrs()
    if not any(peer_addr in n for n in nets):
        logger.warning("admin: rejected upgrade request from %s", peer)
        raise HTTPException(
            status_code=403,
            detail=f"peer {peer} not in HERMES_ADMIN_ALLOW_CIDRS",
        )


# ── Endpoints ────────────────────────────────────────────────────────


@router.get("/api/admin/instance")
def instance_info() -> Dict[str, Any]:
    """Identify which instance is answering. Hermes uses this to make
    sure it's about to upgrade the *right* container (paper, never live,
    unless the operator explicitly opted in)."""
    return {
        "instance": os.environ.get("HERMES_INSTANCE", "unknown"),
        "mode": os.environ.get("HERMES_MODE", "paper"),
        "version": os.environ.get("HERMES_VERSION", "dev"),
        "image_tag": os.environ.get("HERMES_TAG", "latest"),
    }


@router.get("/api/admin/upgrade")
def upgrade_state() -> Dict[str, Any]:
    """Read the current upgrade marker. Returns ``state: idle`` if no
    upgrade is in flight. The host-side runner is responsible for
    advancing the state field as it works."""
    marker = _marker_path()
    if not marker.exists():
        return {"state": "idle"}
    try:
        return json.loads(marker.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.exception("admin: failed reading marker: %s", exc)
        return {"state": "unknown", "error": str(exc)}


@router.post("/api/admin/upgrade")
def request_upgrade(request: Request) -> Dict[str, Any]:
    """Queue an upgrade. Writes a JSON marker that the host-side runner
    will pick up on its next poll cycle (~5s).

    Returns 202 Accepted semantics — the API does not block on the
    actual rebuild. Hermes can poll ``GET /api/admin/upgrade`` to watch
    progress, or subscribe to logs via ``/api/logs``."""
    _require_trusted(request)

    marker = _marker_path()
    marker.parent.mkdir(parents=True, exist_ok=True)

    # Don't clobber an in-flight upgrade — that would orphan the host
    # runner and confuse Hermes' state machine.
    if marker.exists():
        try:
            existing = json.loads(marker.read_text())
        except (OSError, json.JSONDecodeError):
            existing = {"state": "unknown"}
        if existing.get("state") in {"queued", "in_progress"}:
            return {"accepted": False, "current": existing}

    payload = {
        "state": "queued",
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "instance": os.environ.get("HERMES_INSTANCE", "unknown"),
        "mode": os.environ.get("HERMES_MODE", "paper"),
        "requested_by_peer": request.client.host if request.client else None,
    }
    marker.write_text(json.dumps(payload, indent=2))
    logger.info("admin: upgrade queued: %s", payload)
    return {"accepted": True, "current": payload}
