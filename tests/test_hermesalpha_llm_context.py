"""Regression tests for the HermesAlpha empty-scan fixes (2026-07-16).

Live diagnosis on the paper instance found HermesAlpha had never traded for
two stacked reasons, each with a fix pinned here:

1. **Truncated LLM replies.** The overseer clients were built without
   ``max_tokens``, so the transport default of 1024 applied. Thinking models
   (gemini-2.5-flash via the OpenAI-compat endpoint) count hidden reasoning
   tokens against that budget — measured ~980 of 1024 — so the visible JSON
   came back cut off (``finish_reason=length``), fell into ``_safe_json``'s
   unparseable fallback, and ``propose_intent`` silently skipped the symbol.
   ``_build_llm`` must pass ``DEFAULT_LLM_MAX_TOKENS`` to every client.

2. **Starved intent context.** ``_entry_context`` always mapped ``trend`` and
   ``iv_rank``, but nothing ever populated them — ``analyze_symbol`` computed
   neither, so the LLM saw ``null`` for both every scan and (rationally)
   replied ``no_trade`` for lack of edge. ``analyze_symbol`` now labels the
   trend and ``execute_entries`` injects the computed IV rank.

All offline — stub broker / stub DB, fake overseer.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from hermes.common import DEFAULT_LLM_MAX_TOKENS
from hermes.ml.pop_engine import classify_trend
from hermes.service1_agent.agent_construction import _build_llm
from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import HermesAlpha

from ._stubs import StubBroker, StubDB


# ── fakes ────────────────────────────────────────────────────────────────────
class _ContextCapturingOverseer:
    autonomy = "autonomous"

    def __init__(self):
        self.contexts = {}

    async def propose_intent(self, symbol, context):
        self.contexts[symbol] = context
        return None  # decline — these tests only care about the context


def _build_alpha(*, overseer, config=None):
    broker = StubBroker()
    db = StubDB()
    cfg = {"hermesalpha_width": 5, "hermesalpha_min_credit_pct": 0.0,
           "hermesalpha_target_lots": 1, "hermesalpha_max_lots": 1}
    cfg.update(config or {})
    mm = MoneyManager(broker, db, cfg)
    s = HermesAlpha(broker=broker, db=db, money_manager=mm,
                    ic_builder=IronCondorBuilder(mm), config=cfg,
                    dry_run=False, overseer=overseer)
    return s, broker, db


# ── fix 1: LLM completion budget ─────────────────────────────────────────────
@pytest.mark.parametrize("provider,config", [
    ("gemini", {"llm_model": "gemini-2.5-flash", "llm_api_key": "k"}),
    ("claude", {"llm_model": "claude-x", "llm_api_key": "k"}),
    ("local",  {"llm_base_url": "http://localhost:1234/v1", "llm_model": "m"}),
])
async def test_build_llm_passes_default_max_tokens(provider, config):
    db = StubDB()
    db.settings["llm_provider"] = provider
    for key, value in config.items():
        db.settings[key] = value

    client, _snapshot, _vision = await _build_llm(db)

    assert client.max_tokens == DEFAULT_LLM_MAX_TOKENS, (
        f"{provider}: client built with max_tokens={client.max_tokens}; a "
        f"1024-class budget truncates thinking-model replies into the "
        f"unparseable-JSON fallback"
    )


def test_default_max_tokens_clears_the_thinking_budget():
    # gemini-2.5-flash measured ~980 hidden reasoning tokens on the
    # propose_intent prompt; anything ≲1024 reintroduces the truncation.
    assert DEFAULT_LLM_MAX_TOKENS >= 4096


# ── fix 2a: trend classification ─────────────────────────────────────────────
def test_classify_trend_up_down_sideways():
    n = 60
    rising = pd.Series([100.0 + i for i in range(n)])
    falling = pd.Series([100.0 - i * 0.5 for i in range(n)])
    flat = pd.Series([100.0, 101.0] * (n // 2))
    assert classify_trend(rising) == "up"
    assert classify_trend(falling) == "down"
    assert classify_trend(flat) == "sideways"


def test_classify_trend_short_history_is_unknown():
    assert classify_trend(pd.Series([100.0 + i for i in range(49)])) is None


async def test_tradier_analyze_symbol_labels_trend():
    from hermes.broker.tradier import TradierBroker

    broker = TradierBroker({
        "tradier_access_token": "t", "tradier_account_id": "a",
        "tradier_base_url": "https://example.invalid/v1", "dry_run": True,
    })

    start = date.today() - timedelta(days=90)
    bars = []
    for i in range(60):
        px = 100.0 + i
        bars.append({"date": (start + timedelta(days=i)).isoformat(),
                     "open": px, "high": px + 1, "low": px - 1,
                     "close": px, "volume": 1_000_000})

    async def _fake_history(symbol, **kwargs):
        return bars

    broker.get_history = _fake_history
    analysis = await broker.analyze_symbol("SPY", period="3m")

    assert analysis["trend"] == "up"


# ── fix 2b: IV rank computation and context injection ────────────────────────
async def test_compute_iv_rank_math():
    ov = _ContextCapturingOverseer()
    s, _broker, db = _build_alpha(overseer=ov)

    for i, iv in enumerate([0.10, 0.20, 0.30]):
        await db.timeseries.save_implied_vol("SPY", iv, date.today() - timedelta(days=i + 1))

    async def _current_iv(symbol):
        return 0.25

    s._fetch_current_atm_iv = _current_iv
    ivr = await s.compute_iv_rank("SPY")
    assert ivr == pytest.approx(75.0)  # 0.25 in [0.10, 0.30]


async def test_compute_iv_rank_degrades_to_none():
    ov = _ContextCapturingOverseer()
    s, _broker, _db = _build_alpha(overseer=ov)

    async def _no_iv(symbol):
        return None

    s._fetch_current_atm_iv = _no_iv
    assert await s.compute_iv_rank("SPY") is None


async def test_is_ivr_gated_honors_precomputed_ivr_without_refetching():
    ov = _ContextCapturingOverseer()
    s, _broker, _db = _build_alpha(overseer=ov)

    async def _must_not_fetch(symbol):
        raise AssertionError("gate refetched IV despite a precomputed rank")

    s._fetch_current_atm_iv = _must_not_fetch
    assert await s.is_ivr_gated("SPY", 30.0, ivr=10.0) is True
    assert await s.is_ivr_gated("SPY", 30.0, ivr=55.0) is False


async def test_execute_entries_injects_iv_rank_into_intent_context():
    ov = _ContextCapturingOverseer()
    s, _broker, _db = _build_alpha(overseer=ov)

    async def _ivr(symbol):
        return 42.0

    s.compute_iv_rank = _ivr
    await s.execute_entries(["SPY"])

    assert ov.contexts["SPY"]["iv_rank"] == 42.0, (
        "the overseer context must carry the computed IV rank — a null "
        "iv_rank makes a conservative LLM decline every scan"
    )


async def test_execute_entries_context_carries_broker_trend():
    ov = _ContextCapturingOverseer()
    s, broker, _db = _build_alpha(overseer=ov)

    async def _ivr(symbol):
        return None

    s.compute_iv_rank = _ivr
    base_analysis = broker.analyze_symbol("SPY")

    def _analyze(symbol, period="6m"):
        return {**base_analysis, "trend": "up"}

    broker.analyze_symbol = _analyze
    await s.execute_entries(["SPY"])

    assert ov.contexts["SPY"]["trend"] == "up"
    # Unknown IV rank stays explicit null, not a fabricated number.
    assert ov.contexts["SPY"]["iv_rank"] is None
