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
#   ./hermes.sh push   # ── Config ────────────────────────────────────────────────────────────
HERMES_IMAGE="${HERMES_IMAGE:-laguz3/hermes}"
HERMES_TAG="${HERMES_TAG:-latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

IMAGE_FULL="${HERMES_IMAGE}:${HERMES_TAG}"
HERMES_VERSION="$(cat VERSION 2>/dev/null || echo '0.0.0')"

# Detect and load instances (both paper and live if config files exist)
ENV_FILES=()
PROJECT_NAMES=()

# Check if we are in a two-folder setup
TWO_FOLDER_SETUP=false
if [ -d "../hermestrader-live" ] || [[ "$SCRIPT_DIR" == *"-live" ]]; then
    TWO_FOLDER_SETUP=true
fi

if [ "$TWO_FOLDER_SETUP" = "true" ]; then
    # In a two-folder setup, only manage the instance that matches the current folder context
    if [[ "$SCRIPT_DIR" == *"-live" ]]; then
        if [ -f .env.live ]; then
            ENV_FILES+=(".env.live")
            PROJECT_NAMES+=("hermes-live")
        elif [ -f .env ]; then
            ENV_FILES+=(".env")
            PROJECT_NAMES+=("hermes-live")
        fi
    else
        if [ -f .env.paper ]; then
            ENV_FILES+=(".env.paper")
            PROJECT_NAMES+=("hermes-paper")
        elif [ -f .env ]; then
            ENV_FILES+=(".env")
            PROJECT_NAMES+=("hermes-paper")
        fi
    fi
else
    # Single-folder setup: manage both side-by-side if config files exist
    if [ -f .env.paper ]; then
        ENV_FILES+=(".env.paper")
        PROJECT_NAMES+=("hermes-paper")
    fi
    if [ -f .env.live ]; then
        ENV_FILES+=(".env.live")
        PROJECT_NAMES+=("hermes-live")
    fi
    if [ ${#ENV_FILES[@]} -eq 0 ]; then
        if [ -f .env ]; then
            ENV_FILES+=(".env")
            PROJECT_NAMES+=("hermes-paper")
        else
            ENV_FILES+=(".env.paper")
            PROJECT_NAMES+=("hermes-paper")
        fi
    fi
fi


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
    local env_file="$1"
    local proj_name="$2"
    docker inspect --format='{{index .Config.Labels "hermes.version"}}' \
        "$(docker compose --env-file "$env_file" -p "$proj_name" ps -q watcher 2>/dev/null | head -1)" 2>/dev/null || echo "unknown"
}

_get_env_image() {
    local env_file="$1"
    local img tag
    img=$(grep -E "^HERMES_IMAGE=" "$env_file" 2>/dev/null | cut -d= -f2- | tr -d "'\"" || echo "")
    tag=$(grep -E "^HERMES_TAG=" "$env_file" 2>/dev/null | cut -d= -f2- | tr -d "'\"" || echo "")
    [ -z "$img" ] && img="laguz3/hermes"
    [ -z "$tag" ] && tag="latest"
    echo "${img}:${tag}"
}

_pull() {
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        info "Pulling images for ${BOLD}${proj_name}${NC} using ${env_file}…"
        if docker compose --env-file "$env_file" -p "$proj_name" pull -q >/dev/null 2>&1; then
            ok "Pull complete for ${proj_name}"
        else
            warn "Pull failed for ${proj_name} — will use local image or build from source"
        fi
    done
}

_up() {
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        local api_port inst_image
        api_port=$(grep -E "^HERMES_API_PORT=" "$env_file" | cut -d= -f2- | tr -d "'\"" || echo "8080")
        inst_image=$(_get_env_image "$env_file")
        info "Starting Hermes ${BOLD}${proj_name}${NC} services…"
        docker compose --env-file "$env_file" -p "$proj_name" up -d
        ok "Hermes ${proj_name} is running"
        echo -e "   ${CYAN}Dashboard${NC}  → http://localhost:${api_port}"
        echo -e "   ${CYAN}Image${NC}      → ${inst_image}"
        _show_version "$env_file" "$proj_name"
    done
}

_show_version() {
    local env_file="$1"
    local proj_name="$2"
    local ver
    ver=$(_current_version "$env_file" "$proj_name")
    if [ "$ver" != "" ] && [ "$ver" != "unknown" ]; then
        echo -e "   ${CYAN}Version${NC}    → ${ver}"
    fi
}

# ── Commands ──────────────────────────────────────────────────────────
cmd_start() {
    info "Hermes — starting all detected instances (paper/live) with latest images"
    _pull
    _up
}

cmd_stop() {
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        info "Stopping Hermes ${BOLD}${proj_name}${NC} services…"
        docker compose --env-file "$env_file" -p "$proj_name" down
        ok "Stopped ${proj_name}"
    done
}

cmd_restart() {
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        info "Restarting agent + watcher for ${BOLD}${proj_name}${NC}…"
        docker compose --env-file "$env_file" -p "$proj_name" restart watcher
        ok "Restarted ${proj_name}"
        _show_version "$env_file" "$proj_name"
    done
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

    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        local api_port
        api_port=$(grep -E "^HERMES_API_PORT=" "$env_file" | cut -d= -f2- | tr -d "'\"" || echo "8080")
        info "Recreating containers with new image for ${BOLD}${proj_name}${NC}…"
        docker compose --env-file "$env_file" -p "$proj_name" up -d --force-recreate --no-build
        ok "Update complete — Hermes ${proj_name} is running the latest image"
        echo -e "   ${CYAN}Dashboard${NC}  → http://localhost:${api_port}"
        _show_version "$env_file" "$proj_name"
    done
}

cmd_logs() {
    local target="paper"
    local svc=""
    if [ "${1:-}" = "live" ]; then
        target="live"
        svc="${2:-}"
    elif [ "${1:-}" = "paper" ]; then
        target="paper"
        svc="${2:-}"
    else
        svc="${1:-}"
    fi

    local env_file=".env.paper"
    local proj_name="hermes-paper"
    if [ "$target" = "live" ]; then
        env_file=".env.live"
        proj_name="hermes-live"
    fi

    info "Showing logs for ${proj_name}..."
    if [ -n "$svc" ]; then
        docker compose --env-file "$env_file" -p "$proj_name" logs -f "$svc"
    else
        docker compose --env-file "$env_file" -p "$proj_name" logs -f
    fi
}

cmd_status() {
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        echo -e "\n${BOLD}=== ${proj_name} ===${NC}"
        docker compose --env-file "$env_file" -p "$proj_name" ps
    done
}

cmd_build() {
    info "Building ${BOLD}${IMAGE_FULL}${NC} (version: ${HERMES_VERSION})…"
    docker build \
        --build-arg HERMES_VERSION="$HERMES_VERSION" \
        -t "$IMAGE_FULL" \
        -t "${HERMES_IMAGE}:${HERMES_VERSION}" \
        -t "${HERMES_IMAGE}:stable" \
        .
    ok "Build complete: ${IMAGE_FULL} (also tagged as stable)"
}

cmd_rebuild() {
    info "Rebuilding Hermes from scratch (no cache)…"
    info "Stopping containers…"
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        docker compose --env-file "$env_file" -p "$proj_name" down --remove-orphans
    done

    info "Clearing corrupted Docker build cache to prevent I/O errors…"
    docker builder prune -a -f

    cmd_build

    info "Starting services…"
    _up
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
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        docker compose --env-file "$env_file" -p "$proj_name" down --volumes --remove-orphans
    done
    ok "Containers and volumes removed"

    info "Clearing Docker build cache…"
    docker builder prune -a -f

    cmd_build

    info "Starting clean services…"
    _up
    warn "All previous settings, trades, and logs have been erased."
    warn "Re-enter your Tradier credentials and LLM config in the C2 panel."
}

cmd_check_deps() {
    info "Checking chart-vision dependencies inside the watcher container…"
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        local cid
        cid=$(docker compose --env-file "$env_file" -p "$proj_name" ps -q watcher 2>/dev/null | head -1)
        if [ -z "$cid" ]; then
            warn "Watcher container for ${proj_name} is not running — start Hermes first with: ./hermes.sh start"
            continue
        fi
        if docker exec "$cid" python -c \
            "import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt; print('matplotlib', matplotlib.__version__)" \
            2>&1; then
            ok "matplotlib is available in ${proj_name} — chart vision is ready"
        else
            err "matplotlib import failed inside the ${proj_name} container."
            echo -e "   Rebuild the image with ${BOLD}./hermes.sh rebuild${NC} to pick up the updated Dockerfile."
            return 1
        fi
    done
}

cmd_mcp() {
    info "Starting Tradier MCP server..."
    if [ -f .env ]; then
        info "Loading environment variables from .env"
        set -o allexport
        source .env
        set +o allexport
    fi
    if [ -n "${TRADIER_API_KEY:-}" ] && [ -z "${TRADIER_ACCESS_TOKEN:-}" ]; then
        export TRADIER_ACCESS_TOKEN="$TRADIER_API_KEY"
    fi
    python3 -m hermes.mcp.server
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
    # Show container image ID + creation time for each service
    for i in "${!ENV_FILES[@]}"; do
        local env_file="${ENV_FILES[$i]}"
        local proj_name="${PROJECT_NAMES[$i]}"
        local inst_image
        inst_image=$(_get_env_image "$env_file")
        echo -e "\n${BOLD}=== ${proj_name} ===${NC}"
        echo -e "   ${CYAN}Image${NC}      → ${inst_image}"
        for svc in watcher; do
            local cid
            cid=$(docker compose --env-file "$env_file" -p "$proj_name" ps -q "$svc" 2>/dev/null | head -1)
            if [ -n "$cid" ]; then
                local img created
                img=$(docker inspect --format='{{.Config.Image}}' "$cid" 2>/dev/null || echo "—")
                created=$(docker inspect --format='{{.Created}}' "$cid" 2>/dev/null | cut -d'.' -f1 || echo "—")
                echo -e "   ${CYAN}${svc}${NC}  → image=${img}  created=${created}"
            fi
        done
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
    echo "  mcp              Start the Tradier MCP server on the host (loads .env)"
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
    mcp)            cmd_mcp ;;
    version|-v|--version) cmd_version ;;
    help|--help|-h) cmd_help ;;
    *)              err "Unknown command: $1"; cmd_help; exit 1 ;;
esac
