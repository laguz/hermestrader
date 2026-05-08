---
name: troubleshoot_build
description: Adaptive-thinking walkthrough for diagnosing a failed Docker build or unhealthy container before paging the operator.
triggers: [invoked-by:maintain_service, manual]
authority: paper-only (read-only on live)
---

# Troubleshoot a failed build

Invoked by `maintain_service` when a `docker compose build` exits
non-zero or a container fails its health check after deploy. Walk
the tree below in order. Stop at the first matching fix and report
back to the caller; the caller decides whether to escalate.

## 1. Classify the failure

Run:

```bash
docker compose --env-file .env.paper -p hermes-paper build 2>&1 | tail -100 > /tmp/build.log
```

Read `/tmp/build.log`. Match the last error against one of these
classes:

| Class                | Signature in logs                                |
|----------------------|--------------------------------------------------|
| dependency-resolve   | `ERROR: Cannot install` / `ResolutionImpossible` |
| dependency-fetch     | `Failed to establish a new connection`           |
| compile-failure      | `error: command 'gcc' failed`                    |
| import-at-build      | `ModuleNotFoundError` inside the verify step     |
| disk-full            | `no space left on device`                        |
| permission           | `permission denied`                              |
| upstream-image       | `failed to fetch` on the FROM line               |

If none match, classify as `unknown` and skip to step 4.

## 2. Apply the cheap fix per class

- **dependency-resolve** → `pip install` against a fresh venv on the
  host using the same `requirements.txt`. If it succeeds there but
  fails in the container, the cause is almost always a stale base
  image; `docker pull python:3.11-slim` and retry the build.
- **dependency-fetch** → transient network. Wait 30s, retry once.
- **compile-failure** → check the Dockerfile's `apt-get install`
  block hasn't lost a header package. Compare against `git log -p
  Dockerfile`.
- **import-at-build** → the verify step (`python -c "import
  matplotlib..."`) caught a missing dep. Add the missing module to
  `requirements.txt` and rebuild.
- **disk-full** → `docker system prune -af --volumes` is destructive;
  you do NOT have authority to run it. Page Luis with the disk-usage
  numbers (`df -h /`).
- **permission** → almost always a host-side issue with the bind
  mount target. Page Luis.
- **upstream-image** → wait 60s, retry once. Docker Hub blips happen.

## 3. If the fix worked

Re-run the build. If it succeeds, complete the upgrade flow back in
`maintain_service`. Add a memory note describing what fixed it so
next time you can match faster.

## 4. If you're still stuck (or the class is `unknown`)

Stop trying. Two unsuccessful rounds is the limit per `SOUL.md`.
Compose an escalation message for Luis with this exact structure:

```
build failed: <class>
attempts: <n>
last error (last 30 lines):
  <log excerpt>
hypotheses:
  1. <your best guess>
  2. <your second-best guess>
asking: <the specific yes/no question that would unblock you>
```

Send it via the Telegram gateway and exit. Do not loop, do not
auto-revert, do not touch the `live` instance.

## Live instance addendum

If you were invoked against the **live** instance (only legal via
`promote_to_live` after explicit operator approval), the rules
change:

- You may **diagnose** — read logs, classify, run dry-run builds in
  the worktree directory.
- You may **not apply any fix** without a fresh per-attempt approval
  from Luis. Each retry is its own approval.
- A failed live promotion auto-rolls back to the previous tag (the
  host runner does this); your job afterwards is the post-mortem,
  not the recovery.
