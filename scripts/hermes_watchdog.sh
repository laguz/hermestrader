#!/usr/bin/env bash
# ── hermes_watchdog.sh ─────────────────────────────────────────────────
# Host-side safety net for container crashes.
#
# `docker-compose.yml` sets `restart: unless-stopped` on every service, which
# is normally sufficient — but on some Docker Desktop setups the daemon's own
# restart-policy supervisor doesn't reliably fire (verified 2026-07-08: a
# `docker kill` on a container with `restart: unless-stopped` sat Exited for
# 30+s with zero restart attempts). This script is the belt-and-suspenders
# fallback: it polls for any `hermes-*` container that isn't running and
# starts it back up, so a crash (or the Docker daemon quirk above) can't
# leave Hermes silently offline until a human happens to check.
#
# Usage:
#   bash scripts/hermes_watchdog.sh &          # ad-hoc
#   # or install as a launchd/systemd service for it to survive reboots.
#
# Deliberately does NOT touch a container that's currently "Up" (even if
# unhealthy) — that's the liveness watchdog's job (hermes/service1_agent/
# main.py self-exits a wedged agent process so this script's restart-on-exit
# logic picks it up). This script only reacts to containers that have
# actually stopped.
#
# POSIX/bash-3.2 compatible on purpose (no associative arrays) — macOS's
# default /bin/bash is 3.2 and `env bash` may resolve there ahead of a
# Homebrew bash even when one is installed.

set -uo pipefail

POLL_INTERVAL_S="${HERMES_WATCHDOG_POLL_S:-15}"
# Give up auto-restarting a container that's crash-looping (protects against
# masking a real startup bug behind an infinite restart loop) and demands a
# human instead of silently retrying forever.
MAX_RESTARTS_PER_WINDOW=5
WINDOW_S=600

STATE_DIR="${HERMES_WATCHDOG_STATE_DIR:-/tmp/hermes-watchdog-state}"
mkdir -p "$STATE_DIR"

log() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [hermes-watchdog] $*"
}

# Appends $now to the container's timestamp file, prunes entries older than
# WINDOW_S, and echoes the surviving count.
record_and_count() {
    local name="$1" now="$2" f="$STATE_DIR/${name}.times" kept_file
    kept_file="$(mktemp "$STATE_DIR/.tmp.XXXXXX")"
    [ -f "$f" ] && awk -v now="$now" -v w="$WINDOW_S" '(now - $1) < w' "$f" > "$kept_file"
    echo "$now" >> "$kept_file"
    mv "$kept_file" "$f"
    wc -l < "$f" | tr -d ' '
}

log "starting — polling every ${POLL_INTERVAL_S}s"

while true; do
    now=$(date +%s)
    docker ps -a --filter "name=hermes-" --format '{{.Names}}	{{.Status}}' |
    while IFS="$(printf '\t')" read -r name status; do
        [ -z "$name" ] && continue
        case "$status" in
            Exited*|Dead*|Created*)
                count=$(record_and_count "$name" "$now")
                if [ "$count" -gt "$MAX_RESTARTS_PER_WINDOW" ]; then
                    log "CRITICAL: $name has crashed $count times in the last ${WINDOW_S}s — giving up, needs human attention (status: $status)"
                    continue
                fi
                log "restarting $name (was: $status)"
                if docker start "$name" >/dev/null 2>&1; then
                    log "restarted $name"
                else
                    log "WARNING: failed to restart $name"
                fi
                ;;
        esac
    done
    sleep "$POLL_INTERVAL_S"
done
