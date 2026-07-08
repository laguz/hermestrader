"""Regression tests for the agent's crash supervisor.

2026-07-08: the paper agent crashed outright (`NameError` in
`charts/provider.py`, unrelated bug already fixed separately) and the
container just sat dead — `run()` had no exception handling at all around
`asyncio.run(_run_async(...))`, so any unhandled exception anywhere in
startup or the main loop took the whole process down with nothing to bring
it back. `run()` now catches, logs, and retries in-process with backoff so a
bug can't cost trading uptime while it's being fixed — but a real shutdown
request (SIGTERM/SIGINT, i.e. `_SHUTDOWN_EVENT` already set) must still exit
cleanly instead of looping forever.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

from hermes.service1_agent import main as agent_main


def test_run_retries_in_process_after_unhandled_exception():
    """A crash in `_run_async` must not kill the process — `run()` retries."""
    agent_main._SHUTDOWN_EVENT.clear()
    calls = {"n": 0}

    async def _flaky(chart_provider, conf):
        calls["n"] += 1
        if calls["n"] == 1:
            raise NameError("simulated startup bug")
        # second attempt "succeeds" — represents a clean shutdown afterward
        return None

    with patch.object(agent_main, "_run_async", side_effect=_flaky), \
         patch.object(agent_main.time, "sleep") as mock_sleep:
        agent_main.run(None, {})

    assert calls["n"] == 2, "run() must retry _run_async after it raises"
    mock_sleep.assert_called_once()


def test_run_does_not_retry_when_shutdown_already_requested():
    """A crash racing with an in-flight SIGTERM must exit, not loop forever."""
    agent_main._SHUTDOWN_EVENT.clear()
    calls = {"n": 0}

    async def _crash_during_shutdown(chart_provider, conf):
        calls["n"] += 1
        agent_main._SHUTDOWN_EVENT.set()
        raise RuntimeError("simulated crash mid-shutdown")

    try:
        with patch.object(agent_main, "_run_async", side_effect=_crash_during_shutdown), \
             patch.object(agent_main.time, "sleep") as mock_sleep:
            agent_main.run(None, {})

        assert calls["n"] == 1, "must not retry once a shutdown was requested"
        mock_sleep.assert_not_called()
    finally:
        agent_main._SHUTDOWN_EVENT.clear()


def test_run_returns_normally_on_clean_shutdown():
    """No exception at all (the SIGTERM happy path) must not retry either."""
    agent_main._SHUTDOWN_EVENT.clear()

    with patch.object(agent_main, "_run_async", new=AsyncMock(return_value=None)) as mocked, \
         patch.object(agent_main.time, "sleep") as mock_sleep:
        agent_main.run(None, {})

    mocked.assert_called_once()
    mock_sleep.assert_not_called()
