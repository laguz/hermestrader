#!/usr/bin/env bash
# ── upgrade_runner.sh ──────────────────────────────────────────────────
# Host-side companion to /api/admin/upgrade.
#
# The container can't rebuild itself, so this script polls the marker
# file the API writes into the shared data volume. When it sees a
# `queued` marker, it runs the rebuild and updates the marker so the
# API (and Hermes) can watch progress.
#
# Usage:
#   bash scripts/upgrade_runner.sh paper      # auto-upgrades paper
#   bash scripts/upgrade_runner.sh live       # requires --i-know-this-is-live
#
# The script intentionally does NOT support a "background daemon" mode
# — keep it under tmux / launchd / systemd so failures are visible.

set -euo pipefail

INSTANCE="${1:-}"
case "$INSTANCE" in
    paper)
        DATA_DIR="${HOME}/.hermes-paper"
        ENV_FILE=".env.paper"
        PROJECT="hermes-paper"
        WORKTREE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
        ;;
    live)
        if [ "${2:-}" != "--i-know-this-is-live" ]; then
            echo "!! Refusing to start the live runner without --i-know-this-is-live"
            echo "!! Live upgrades MUST be operator-approved per promote_to_live.md"
            exit 2
        fi
        DATA_DIR="${HOME}/.hermes-live"
        ENV_FILE=".env.live"
        PROJECT="hermes-live"
        # Live runs out of the sibling worktree.
        WORKTREE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd | sed 's/$/-live/')"
        if [ ! -d "$WORKTREE" ]; then
            WORKTREE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../hermestrader-live" && pwd)"
        fi
        ;;
    *)
        echo "Usage: $0 {paper|live} [--i-know-this-is-live]"
        exit 1
        ;;
esac

MARKER="$DATA_DIR/upgrade_requested"

echo "==> upgrade-runner: instance=$INSTANCE worktree=$WORKTREE marker=$MARKER"
mkdir -p "$DATA_DIR"

write_state() {
    # $1 = state, $2... = extra "key=value" pairs (escaped JSON values)
    local state="$1"; shift
    local extras=""
    for kv in "$@"; do
        extras+=", \"${kv%%=*}\": $(printf '%s' "${kv#*=}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')"
    done
    cat > "$MARKER" <<JSON
{"state": "$state", "instance": "$INSTANCE", "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"$extras}
JSON
}

run_upgrade() {
    write_state "in_progress"
    cd "$WORKTREE"

    echo "==> [$INSTANCE] git pull --ff-only"
    if ! git pull --ff-only 2>&1 | tee /tmp/hermes-upgrade-pull.log; then
        write_state "failed" "step=pull" "log_tail=$(tail -20 /tmp/hermes-upgrade-pull.log)"
        return 1
    fi

    echo "==> [$INSTANCE] docker compose build"
    if ! docker compose --env-file "$ENV_FILE" -p "$PROJECT" build 2>&1 | tee /tmp/hermes-upgrade-build.log; then
        write_state "failed" "step=build" "log_tail=$(tail -30 /tmp/hermes-upgrade-build.log)"
        return 1
    fi

    echo "==> [$INSTANCE] docker compose up -d"
    if ! docker compose --env-file "$ENV_FILE" -p "$PROJECT" up -d 2>&1 | tee /tmp/hermes-upgrade-up.log; then
        write_state "failed" "step=up" "log_tail=$(tail -20 /tmp/hermes-upgrade-up.log)"
        return 1
    fi

    rm -f "$MARKER"
    echo "==> [$INSTANCE] upgrade complete; marker cleared"
    return 0
}

while true; do
    if [ -f "$MARKER" ]; then
        state=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("state","unknown"))' "$MARKER" 2>/dev/null || echo unknown)
        if [ "$state" = "queued" ]; then
            echo "==> [$INSTANCE] queued upgrade detected"
            run_upgrade || echo "!!  upgrade failed; marker left for inspection"
        fi
    fi
    sleep 5
done
