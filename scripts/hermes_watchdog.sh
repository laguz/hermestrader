#!/usr/bin/env bash
# ── hermes_watchdog.sh ─────────────────────────────────────────────────
# Host-side safety net for container crashes. Trading depends on this
# process staying up — it NEVER permanently gives up on a container, only
# backs off its retry cadence, because a bot that isn't running can't trade.
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
# Maintenance pause: `touch /tmp/hermes-watchdog-state/PAUSED` (or
# $HERMES_WATCHDOG_STATE_DIR/PAUSED) to stop the watchdog from fighting a
# deliberate `docker compose stop`; remove the file to resume.
#
# Deliberately does NOT touch a container that's currently "Up" (even if
# unhealthy) — that's the liveness watchdog's job (hermes/service1_agent/
# main.py self-exits a wedged agent process so this script's restart-on-exit
# logic picks it up). This script only reacts to containers that have
# actually stopped, or to the Docker daemon itself being unreachable.
#
# POSIX/bash-3.2 compatible on purpose (no associative arrays) — macOS's
# default /bin/bash is 3.2 and `env bash` may resolve there ahead of a
# Homebrew bash even when one is installed.

set -uo pipefail

POLL_INTERVAL_S="${HERMES_WATCHDOG_POLL_S:-15}"
# Fast retries up to this many attempts within WINDOW_S, then back off to
# BACKOFF_S between attempts — forever. Never a permanent give-up: a broken
# deploy still gets retried every BACKOFF_S in case a human fixes it, and a
# transient blip (DB warming up, Docker VM still booting) recovers on its own
# instead of getting stuck because polling itself kept the failure count hot.
MAX_FAST_RESTARTS="${HERMES_WATCHDOG_MAX_FAST_RESTARTS:-5}"
WINDOW_S="${HERMES_WATCHDOG_WINDOW_S:-600}"
BACKOFF_S="${HERMES_WATCHDOG_BACKOFF_S:-120}"

# If `docker` itself is unreachable (daemon/VM down) this many consecutive
# polls, try relaunching Docker Desktop — gated by its own cooldown so we
# don't spam `open` while the VM is still booting.
DAEMON_DOWN_RELAUNCH_THRESHOLD="${HERMES_WATCHDOG_DAEMON_DOWN_THRESHOLD:-4}"
DAEMON_RELAUNCH_COOLDOWN_S="${HERMES_WATCHDOG_DAEMON_RELAUNCH_COOLDOWN_S:-300}"

STATE_DIR="${HERMES_WATCHDOG_STATE_DIR:-/tmp/hermes-watchdog-state}"
mkdir -p "$STATE_DIR"
PAUSE_FILE="$STATE_DIR/PAUSED"
LAST_RELAUNCH_FILE="$STATE_DIR/.last_docker_relaunch"
daemon_down_streak=0

log() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [hermes-watchdog] $*"
}

# Appends $now to the container's attempt-timestamp file (only called when we
# actually attempt a restart — NOT on every poll — so backoff can recover
# once the underlying issue clears), prunes entries older than WINDOW_S, and
# echoes the surviving count.
record_attempt_and_count() {
    local name="$1" now="$2" f="$STATE_DIR/${name}.times" kept_file
    kept_file="$(mktemp "$STATE_DIR/.tmp.XXXXXX")"
    [ -f "$f" ] && awk -v now="$now" -v w="$WINDOW_S" '(now - $1) < w' "$f" > "$kept_file"
    echo "$now" >> "$kept_file"
    mv "$kept_file" "$f"
    wc -l < "$f" | tr -d ' '
}

attempts_in_window() {
    local name="$1" now="$2" f="$STATE_DIR/${name}.times"
    [ -f "$f" ] || { echo 0; return; }
    awk -v now="$now" -v w="$WINDOW_S" '(now - $1) < w' "$f" | wc -l | tr -d ' '
}

last_attempt_ts() {
    local name="$1" f="$STATE_DIR/${name}.times"
    [ -f "$f" ] || { echo 0; return; }
    tail -n1 "$f" 2>/dev/null || echo 0
}

log "starting — polling every ${POLL_INTERVAL_S}s (fast retries up to ${MAX_FAST_RESTARTS}/${WINDOW_S}s, then backs off to one attempt/${BACKOFF_S}s — never gives up)"

while true; do
    now=$(date +%s)

    if [ -f "$PAUSE_FILE" ]; then
        sleep "$POLL_INTERVAL_S"
        continue
    fi

    ps_output="$(docker ps -a --filter "name=hermes-" --format '{{.Names}}	{{.Status}}' 2>&1)"
    ps_rc=$?

    if [ "$ps_rc" -ne 0 ]; then
        daemon_down_streak=$((daemon_down_streak + 1))
        log "WARNING: cannot reach Docker daemon (streak=$daemon_down_streak): $ps_output"
        if [ "$daemon_down_streak" -ge "$DAEMON_DOWN_RELAUNCH_THRESHOLD" ]; then
            last_relaunch=$(cat "$LAST_RELAUNCH_FILE" 2>/dev/null || echo 0)
            if [ $((now - last_relaunch)) -ge "$DAEMON_RELAUNCH_COOLDOWN_S" ]; then
                log "CRITICAL: Docker daemon unreachable for $daemon_down_streak polls — attempting to relaunch Docker Desktop"
                open -a Docker 2>&1 | while read -r line; do log "  open -a Docker: $line"; done
                echo "$now" > "$LAST_RELAUNCH_FILE"
            fi
        fi
        sleep "$POLL_INTERVAL_S"
        continue
    fi
    daemon_down_streak=0

    echo "$ps_output" |
    while IFS="$(printf '\t')" read -r name status; do
        [ -z "$name" ] && continue
        case "$status" in
            Exited*|Dead*|Created*)
                count=$(attempts_in_window "$name" "$now")
                if [ "$count" -ge "$MAX_FAST_RESTARTS" ]; then
                    last=$(last_attempt_ts "$name")
                    if [ $((now - last)) -lt "$BACKOFF_S" ]; then
                        continue  # still in backoff window; try again next cycle
                    fi
                    log "CRITICAL: $name has needed $count restarts in the last ${WINDOW_S}s — backing off to 1 attempt/${BACKOFF_S}s (still retrying, needs human attention)"
                fi
                log "restarting $name (was: $status)"
                record_attempt_and_count "$name" "$now" >/dev/null
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
