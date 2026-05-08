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
HERMES_VERSION="$(cat VERSION 2>/dev/null || echo '0.0.0')"

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
    docker inspect --format='{{index .Config.Labels "hermes.version"}}' \
        "$(docker compose ps -q watcher 2>/dev/null | head -1)" 2>/dev/null || echo "unknown"
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
    echo -e "   ${CYAN}Dashboard${NC}  → http://localhost:8081"
    echo -e "   ${CYAN}Image${NC}      → ${IMAGE_FULL}"
    echo -e "   ${CYAN}Version${NC}    → Hermes Agent v${HERMES_VERSION}"
    _show_version
}

_show_version() {
    local ver
    ver=$(_current_version)
    if [ "$ver" != "" ] && [ "$ver" != "unknown" ]; then
        echo -e "   ${CYAN}Version${NC}    → ${ver}"
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
    docker compose ps
}

cmd_build() {
    info "Building ${BOLD}${IMAGE_FULL}${NC} (version: ${HERMES_VERSION})…"
    docker build \
        --build-arg HERMES_VERSION="$HERMES_VERSION" \
        -t "$IMAGE_FULL" \
        -t "${HERMES_IMAGE}:${HERMES_VERSION}" \
        .
    ok "Build complete: ${IMAGE_FULL}"
}

cmd_rebuild() {
    info "Rebuilding Hermes from scratch (no cache)…"
    info "Stopping containers…"
    docker compose down --remove-orphans

    info "Clearing corrupted Docker build cache to prevent I/O errors…"
    docker builder prune -a -f

    info "Building image with --no-cache…"
    docker build --no-cache \
        --build-arg HERMES_VERSION="$HERMES_VERSION" \
        -t "$IMAGE_FULL" \
        -t "${HERMES_IMAGE}:${HERMES_VERSION}" \
        .
    ok "Build complete"

    info "Starting services…"
    docker compose up -d
    echo ""
    ok "Hermes is running on fresh containers"
    echo -e "   ${CYAN}Dashboard${NC}  → http://localhost:8081"
    echo -e "   ${CYAN}Version${NC}    → Hermes Agent v${HERMES_VERSION}"
}

cmd_nuke() {
    warn "This will DELETE all containers AND all data volumes (DB, settings, logs)."
    warn "Your .env file and source code will NOT be touched."
    echo ""
    read -r -p "$(echo -e "${RED}Type 'yes' to confirm nuclear reset:${NC} ")" confirm
    if [ "$confirm" != "yes" ]; then
        info "Aborted."
        return 0
    fi

    info "Stopping and removing all containers + volumes…"
    docker compose down --volumes --remove-orphans
    ok "Containers and volumes removed"

    info "Clearing Docker build cache…"
    docker builder prune -a -f

    info "Building fresh image (no cache)…"
    docker build --no-cache \
        --build-arg HERMES_VERSION="$HERMES_VERSION" \
        -t "$IMAGE_FULL" \
        -t "${HERMES_IMAGE}:${HERMES_VERSION}" \
        .
    ok "Build complete"

    info "Starting clean services…"
    docker compose up -d
    echo ""
    ok "Nuclear reset complete — Hermes is running on a clean slate"
    echo -e "   ${CYAN}Dashboard${NC}  → http://localhost:8081"
    warn "All previous settings, trades, and logs have been erased."
    warn "Re-enter your Tradier credentials and LLM config in the C2 panel."
}

cmd_check_deps() {
    # Verify that the chart vision layer (matplotlib) is working inside the
    # running watcher container.  Runs a one-liner import so any missing OS
    # library (libfreetype6, fontconfig, etc.) surfaces immediately.
    info "Checking chart-vision dependencies inside the watcher container…"
    local cid
    cid=$(docker compose ps -q watcher 2>/dev/null | head -1)
    if [ -z "$cid" ]; then
        warn "Watcher container is not running — start Hermes first with: ./hermes.sh start"
        return 1
    fi
    if docker exec "$cid" python -c \
        "import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt; print('matplotlib', matplotlib.__version__)" \
        2>&1; then
        ok "matplotlib is available — chart vision is ready"
    else
        err "matplotlib import failed inside the container."
        echo -e "   Rebuild the image with ${BOLD}./hermes.sh rebuild${NC} to pick up the updated Dockerfile."
        return 1
    fi
}

cmd_push() {
    info "Pushing ${BOLD}${IMAGE_FULL}${NC} to Docker Hub…"
    docker push "$IMAGE_FULL"
    # Also push the version tag if it differs from "latest"
    if [ "$HERMES_VERSION" != "latest" ] && [ "$HERMES_VERSION" != "0.0.0" ]; then
        docker push "${HERMES_IMAGE}:${HERMES_VERSION}"
    fi
    ok "Pushed to Docker Hub"
}

cmd_version() {
    local build_date
    build_date=$(date +"%Y.%-m.%-d")
    echo -e "${BOLD}Hermes Agent${NC} v${HERMES_VERSION} (${build_date})"
    echo ""
    echo -e "   ${CYAN}Image${NC}      → ${IMAGE_FULL}"
    # Show container image ID + creation time for each service
    for svc in watcher agent; do
        local cid
        cid=$(docker compose ps -q "$svc" 2>/dev/null | head -1)
        if [ -n "$cid" ]; then
            local img created
            img=$(docker inspect --format='{{.Config.Image}}' "$cid" 2>/dev/null || echo "—")
            created=$(docker inspect --format='{{.Created}}' "$cid" 2>/dev/null | cut -d'.' -f1 || echo "—")
            echo -e "   ${CYAN}${svc}${NC}  → image=${img}  created=${created}"
        fi
    done
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
    echo "  rebuild          Stop, build from scratch (no cache), restart — keeps DB data"
    echo "  nuke             ⚠  Full reset: delete containers + volumes, rebuild clean"
    echo "  update --check   Check if a newer image is available on Docker Hub"
    echo "  update           Pull latest image & recreate containers"
    echo "  logs [service]   Tail logs (agent, watcher, db, or all)"
    echo "  status           Show running containers"
    echo "  build            Build the Docker image locally"
    echo "  push             Push the local image to Docker Hub"
    echo "  check-deps       Verify chart-vision (matplotlib) is working in the container"
    echo "  version          Show the running Hermes version"
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
    rebuild)        cmd_rebuild ;;
    nuke)           cmd_nuke ;;
    update)         shift; cmd_update "$@" ;;
    logs)           shift; cmd_logs "$@" ;;
    status)         cmd_status ;;
    build)          cmd_build ;;
    push)           cmd_push ;;
    check-deps)     cmd_check_deps ;;
    version|-v|--version) cmd_version ;;
    help|--help|-h) cmd_help ;;
    *)              err "Unknown command: $1"; cmd_help; exit 1 ;;
esac
