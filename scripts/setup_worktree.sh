#!/usr/bin/env bash
# ── setup_worktree.sh ──────────────────────────────────────────────────
# One-time setup for the dual paper/live layout.
#
# Idempotent — safe to re-run.
#
# After running:
#   ~/Git/hermestrader/         → main branch (paper, port 8081)
#   ~/Git/hermestrader-live/    → live branch (live, port 8082)
#
# The live worktree is a real on-disk folder backed by the same .git/
# directory as the paper folder. `git pull` in either folder updates
# only that folder's branch.

set -euo pipefail

# Find this repo root regardless of where the script was invoked.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIVE_WORKTREE="$(cd "$REPO_ROOT/.." && pwd)/hermestrader-live"

cd "$REPO_ROOT"

echo "==> Repo root:       $REPO_ROOT"
echo "==> Live worktree:   $LIVE_WORKTREE"

# 1. Make sure we have an up-to-date view of the remote.
echo "==> Fetching origin"
git fetch origin --prune

# 2. Create the `live` branch if it doesn't exist anywhere yet. We
#    seed it from the current main so the first promotion is a no-op.
if git show-ref --verify --quiet refs/heads/live; then
    echo "==> Local 'live' branch already exists"
elif git show-ref --verify --quiet refs/remotes/origin/live; then
    echo "==> Tracking remote origin/live"
    git branch live origin/live
else
    echo "==> Creating new 'live' branch from main"
    git branch live main
fi

# 3. Create the worktree if it doesn't exist.
if [ -d "$LIVE_WORKTREE" ]; then
    echo "==> Worktree directory already exists at $LIVE_WORKTREE"
    if ! git worktree list | grep -q "$LIVE_WORKTREE"; then
        echo "!!  Directory exists but git doesn't know about it."
        echo "!!  Move it aside or remove it, then re-run."
        exit 1
    fi
else
    echo "==> Adding worktree"
    git worktree add "$LIVE_WORKTREE" live
fi

# 4. Seed env files from examples if the operator hasn't yet.
seed_env() {
    local dir="$1" name="$2" example="$3"
    if [ -f "$dir/$name" ]; then
        echo "==> $dir/$name already exists — leaving it alone"
        return
    fi
    if [ ! -f "$dir/$example" ]; then
        echo "!!  $dir/$example not found — skipping seed"
        return
    fi
    cp "$dir/$example" "$dir/$name"
    echo "==> Seeded $dir/$name from $example (EDIT IT before starting!)"
}

seed_env "$REPO_ROOT"      ".env.paper" ".env.paper.example"
seed_env "$LIVE_WORKTREE"  ".env.live"  ".env.live.example"

# 5. Make sure each worktree has its own per-instance data dir on the
#    host. These are the bind-mount targets the compose file expects.
mkdir -p "${HOME}/.hermes-paper"
mkdir -p "${HOME}/.hermes-live"

cat <<EOF

==> Done.

Paper (this folder, branch=main, port 8081):
    cd $REPO_ROOT
    \$EDITOR .env.paper      # fill in TRADIER_API_KEY / TRADIER_ACCOUNT_ID
    docker compose --env-file .env.paper -p hermes-paper up -d --build

Live  (sibling folder, branch=live, port 8082):
    cd $LIVE_WORKTREE
    \$EDITOR .env.live       # production credentials
    docker compose --env-file .env.live -p hermes-live up -d --build

Then leave the host-side upgrade runner running in a third terminal
(only paper auto-upgrades; live requires explicit operator approval):

    bash $REPO_ROOT/scripts/upgrade_runner.sh paper

EOF
