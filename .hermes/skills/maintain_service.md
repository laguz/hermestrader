---
name: maintain_service
description: Watch HermesTrader containers, surface errors, pull updates, trigger rebuilds on the paper instance.
triggers: [scheduled:every-2m, on-event:container-error, manual]
authority: paper-only
---

# Maintain HermesTrader (paper instance)

## When to run

- Every two minutes on a timer (the routine sweep).
- Immediately when a container exits non-zero or a health check
  flips from healthy → unhealthy.
- When the operator says "check on Hermes" or equivalent.

## What to check, in order

**1. Container state.** Run:

```bash
docker compose --env-file .env.paper -p hermes-paper ps --format json
```

Every service (`db`, `watcher`, `agent`, `mcp`) should be `running`
and `healthy`. If any is not, jump to the troubleshoot block.

**2. Health endpoint.** Hit the watcher's liveness probe:

```bash
curl -fsS http://localhost:8081/api/health
```

Expected: `{"ok": true, "service": "hermes-c2"}`. Any other shape,
non-200, or timeout means the watcher is wedged.

**3. Recent error logs.** Pull the last two minutes of logs and grep
for `ERROR`, `Traceback`, or `CRITICAL`:

```bash
docker compose --env-file .env.paper -p hermes-paper logs --since 2m | \
  grep -E 'ERROR|Traceback|CRITICAL' || true
```

A clean run produces no output. Anything else is an event.

**4. Code drift.** Check whether `origin/main` has advanced past
local `HEAD`:

```bash
cd ~/Git/hermestrader && git fetch --quiet && \
  git rev-list --count HEAD..origin/main
```

A non-zero count means there are unpulled commits → trigger an upgrade
(see "Triggering an upgrade" below).

## Triggering an upgrade (paper only)

Hermes has authority to upgrade the **paper** instance autonomously.
The flow:

```bash
# 1. Tell the running container an upgrade is coming.
curl -fsS -X POST http://localhost:8081/api/admin/upgrade

# 2. The host-side runner (scripts/upgrade_runner.sh) will see the
#    marker file written by step 1 within ~5 seconds and execute:
#       git pull --ff-only
#       docker compose --env-file .env.paper -p hermes-paper build
#       docker compose --env-file .env.paper -p hermes-paper up -d

# 3. Watch state until it leaves "in_progress":
while true; do
  state=$(curl -fsS http://localhost:8081/api/admin/upgrade | jq -r .state)
  echo "upgrade state: $state"
  [ "$state" = "idle" ] && break
  [ "$state" = "failed" ] && break
  sleep 5
done
```

If the state ends `failed`, hand off to the
`troubleshoot_build` skill before notifying the operator.

## Troubleshoot block

If a container is unhealthy or the upgrade failed:

1. Pull the last 200 lines of logs from the unhealthy service.
2. Identify the failure class: import error, DB connection, missing
   env var, port collision, build failure, OOM kill.
3. For import / DB / env-var issues, attempt the documented fix from
   memory if you have one. Otherwise, invoke the `troubleshoot_build`
   skill — it walks the adaptive-thinking tree before you escalate.
4. After two unsuccessful attempts, **stop trying** and notify Luis
   via the Telegram gateway with: the failure class, what you tried,
   and the relevant log excerpt (≤ 30 lines).

## What you must NOT do

- Touch the `live` instance. The `maintain_service` skill is paper-only.
- Roll back to a previous tag without operator approval — even on paper,
  silent rollbacks make incident analysis impossible.
- Suppress errors by restarting in a loop. If a restart didn't fix it
  the first time, it won't fix it the third time.
