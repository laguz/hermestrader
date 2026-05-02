#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# hermes.sh — CLI wrapper for the HermesTrader Docker ecosystem.
#
# Usage:
#   ./hermes.sh start            Pull latest images & start all services
#   ./hermes.sh stop             Stop all services
#   ./hermes.sh restart          Restart agent + watcher (keeps DB)
#   ./hermes.sh update --check   Check Docker Hub for a newer image
#   ./hermes.sh update           Pull latest image & recreate containers
#   ./hermes.sh logs [service]   Tail container logs (default: all)
#   ./hermes.sh status           Show running containers
#   ./hermes.sh build            Build the image locally (dev workflow)
#   ./hermes.sh push             Push locally-built image to Docker Hub
# ──────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────
HERMES_IMAGE="${HERMES_IMAGE:-laguz3/hermes}"
HERMES_TAG="${HERMES_TAG:-latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

IMAGE_FULL="${HERMES_IMAGE}:${HERMES_TAG}"

# ── Colours ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()  { echo -e "${CYAN}▸${NC} $*"; }
ok()    { echo -e "${GREEN}✓${NC} $*"; }
warn()  { echo -e "${YELLOW}⚠${NC} $*"; }
err()   { echo -e "${RED}✗${NC} $*" >&2; }

# ── Helpers ───────────────────────────────────────────────────────────
_local_digest() {
    docker image inspect "$IMAGE_FULL" --format='{{index .RepoDigests 0}}' 2>/dev/null \
        | sed 's/.*@//' || echo ""
}

_remote_digest() {
    # Use docker manifest inspect to get the remote digest without pulling.
    docker manifest inspect "$IMAGE_FULL" 2>/dev/null \
        | grep -m1 '"digest"' | sed 's/.*"digest": *"//;s/".*//' || echo ""
}

_current_version() {
    # Try 1: read from a running container's label
    local cid
    cid="$(docker compose ps -q watcher 2>/dev/null | head -1)"
    if [ -n "$cid" ]; then
        local v
        v="$(docker inspect --format='{{index .Config.Labels "hermes.version"}}' "$cid" 2>/dev/null || true)"
        if [ -n "$v" ] && [ "$v" != "<no value>" ]; then
            echo "$v"
            return
        fi
    fi
    # Try 2: read from the image itself
    local v2
    v2="$(docker image inspect "$IMAGE_FULL" --format='{{index .Config.Labels "hermes.version"}}' 2>/dev/null || true)"
    if [ -n "$v2" ] && [ "$v2" != "<no value>" ]; then
        echo "$v2"
        return
    fi
    # Try 3: short image ID
    local id
    id="$(docker image inspect "$IMAGE_FULL" --format='{{.Id}}' 2>/dev/null | cut -c8-19 || true)"
    if [ -n "$id" ]; then
        echo "$id"
        return
    fi
    echo "unknown"
}

_pull() {
    info "Pulling ${BOLD}${IMAGE_FULL}${NC} from Docker Hub…"
    if docker compose pull 2>&1; then
        ok "Pull complete"
    else
        warn "Pull failed — will use local image or build from source"
    fi
}

_up() {
    info "Starting Hermes services…"
    docker compose up -d
    echo ""
    ok "Hermes is running"
    _show_version
    echo -e "   ${CYAN}Dashboard${NC}  → http://localhost:8081"
    echo -e "   ${CYAN}Image${NC}      → ${IMAGE_FULL}"
}

_show_version() {
    local ver
    ver=$(_current_version)
    if [ -n "$ver" ] && [ "$ver" != "unknown" ]; then
        echo -e "   ${CYAN}Version${NC}    → ${GREEN}${ver}${NC}"
    fi
}

# ── Commands ──────────────────────────────────────────────────────────
cmd_start() {
    info "Hermes — starting with latest image"
    _pull
    _up
}

cmd_stop() {
    info "Stopping Hermes services…"
    docker compose down
    ok "Stopped"
}

cmd_restart() {
    info "Restarting agent + watcher…"
    docker compose restart agent watcher
    ok "Restarted"
    _show_version
}

cmd_update() {
    if [ "${1:-}" = "--check" ]; then
        cmd_update_check
    else
        cmd_update_apply
    fi
}

cmd_update_check() {
    info "Checking Docker Hub for updates to ${BOLD}${IMAGE_FULL}${NC}…"
    local local_d remote_d
    local_d=$(_local_digest)
    remote_d=$(_remote_digest)

    if [ -z "$remote_d" ]; then
        warn "Could not reach Docker Hub — check your network or image name"
        return 1
    fi

    if [ -z "$local_d" ]; then
        warn "No local image found — an update is available"
        echo -e "   Remote: ${remote_d:0:24}…"
        return 0
    fi

    if [ "$local_d" = "$remote_d" ]; then
        ok "Already up to date"
        echo -e "   Digest: ${local_d:0:24}…"
    else
        warn "Update available!"
        echo -e "   Local:  ${local_d:0:24}…"
        echo -e "   Remote: ${remote_d:0:24}…"
        echo -e "   Run ${BOLD}./hermes.sh update${NC} to apply"
    fi
}

cmd_update_apply() {
    info "Updating Hermes…"
    _pull

    info "Recreating containers with new image…"
    docker compose up -d --force-recreate --no-build
    echo ""
    ok "Update complete — Hermes is running the latest image"
    echo -e "   ${CYAN}Dashboard${NC}  → http://localhost:8081"
    _show_version
}

cmd_logs() {
    local svc="${1:-}"
    if [ -n "$svc" ]; then
        docker compose logs -f "$svc"
    else
        docker compose logs -f
    fi
}

cmd_status() {
    _show_version
    echo ""
    docker compose ps
}

cmd_build() {
    local version
    version="$(git describe --tags --always --dirty 2>/dev/null || echo 'dev')"
    info "Building ${BOLD}${IMAGE_FULL}${NC} (version: ${version})…"
    docker build \
        --build-arg HERMES_VERSION="$version" \
        -t "$IMAGE_FULL" \
        -t "${HERMES_IMAGE}:${version}" \
        .
    ok "Build complete: ${IMAGE_FULL}"
}

cmd_push() {
    info "Pushing ${BOLD}${IMAGE_FULL}${NC} to Docker Hub…"
    docker push "$IMAGE_FULL"
    # Also push the version tag if it differs from "latest"
    local version
    version="$(git describe --tags --always --dirty 2>/dev/null || echo 'dev')"
    if [ "$version" != "latest" ] && [ "$version" != "dev" ]; then
        docker push "${HERMES_IMAGE}:${version}"
    fi
    ok "Pushed to Docker Hub"
}

cmd_help() {
    echo -e "${BOLD}hermes.sh${NC} — HermesTrader CLI"
    echo ""
    echo "Usage: ./hermes.sh <command> [options]"
    echo ""
    echo "Commands:"
    echo "  start            Pull latest images from Docker Hub & start all services"
    echo "  stop             Stop all services"
    echo "  restart          Restart agent + watcher (keeps DB running)"
    echo "  update --check   Check if a newer image is available on Docker Hub"
    echo "  update           Pull latest image & recreate containers"
    echo "  logs [service]   Tail logs (agent, watcher, db, or all)"
    echo "  status           Show running containers"
    echo "  build            Build the Docker image locally"
    echo "  push             Push the local image to Docker Hub"
    echo "  help             Show this help"
    echo ""
    echo "Environment:"
    echo "  HERMES_IMAGE     Docker Hub image (default: laguz3/hermes)"
    echo "  HERMES_TAG       Image tag (default: latest)"
}

# ── Dispatch ──────────────────────────────────────────────────────────
case "${1:-help}" in
    start)          cmd_start ;;
    stop)           cmd_stop ;;
    restart)        cmd_restart ;;
    update)         shift; cmd_update "$@" ;;
    logs)           shift; cmd_logs "$@" ;;
    status)         cmd_status ;;
    build)          cmd_build ;;
    push)           cmd_push ;;
    help|--help|-h) cmd_help ;;
    *)              err "Unknown command: $1"; cmd_help; exit 1 ;;
esac
