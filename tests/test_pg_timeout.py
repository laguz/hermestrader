from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.service1_agent.core import TradeAction
from hermes.service1_agent.overseer import HermesOverseer


class _SlowLLM:
    """A fake LLM provider that simulates latency."""
    def __init__(self, latency: float = 1.0, timeout_s: float = 0.2):
        self.latency = latency
        self.timeout_s = timeout_s
        self.calls = 0

    def chat(self, messages, images=None):
        self.calls += 1
        time.sleep(self.latency)
        return '{"verdict": "VETO", "rationale": "too risky"}'


@pytest.mark.anyio
async def test_chat_with_timeout_raises_timeout_error():
    """Verify that _chat_with_timeout raises TimeoutError when the provider takes too long."""
    llm = _SlowLLM(latency=1.0, timeout_s=0.1)
    db = MagicMock()
    overseer = HermesOverseer(llm_client=llm, db=db, vision_enabled=False)
    
    with pytest.raises(asyncio.TimeoutError):
        await overseer._chat_with_timeout([{"role": "user", "content": "hi"}])


@pytest.mark.anyio
async def test_consult_falls_back_on_timeout():
    """Verify that _consult retries on timeouts and falls back to a safe APPROVED verdict."""
    llm = _SlowLLM(latency=1.0, timeout_s=0.05)
    db_mock = AsyncMock()
    overseer = HermesOverseer(llm_client=llm, db=db_mock, vision_enabled=False)
    
    # We patch _LLM_MAX_RETRIES to 2 to keep the test fast
    with patch.object(overseer, "_LLM_MAX_RETRIES", 2), \
         patch.object(overseer, "get_system_prompt", AsyncMock(return_value="system")):
         
        action = TradeAction(
            strategy_id="CS75", symbol="AAPL", order_class="option",
            legs=[], price=1.0, side="sell", quantity=1, expiry="2025-06-20"
        )
        
        decision = await overseer._consult(action)
        
        # Verify that it retried twice (1st attempt + 1 retry)
        assert llm.calls == 2
        
        # Verify it fell back cleanly to APPROVE with appropriate warnings
        assert decision.get("verdict") == "APPROVE"
        assert "LLM unavailable" in decision.get("rationale", "")
        assert decision.get("llm_error_fallback") is True
        db_mock.set_setting.assert_called_with("llm_last_error", "TimeoutError()")
