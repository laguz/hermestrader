"""Unit tests for HermesOverseer.

The overseer is the LLM review hook in the trade pipeline. There are
three behaviour modes (``advisory`` / ``enforcing`` / ``autonomous``)
that each have a different contract with ``CascadingEngine.submit``:

- ``advisory``  — log the decision, never modify or block.
- ``enforcing`` — APPROVE / VETO / MODIFY the action.
- ``autonomous``— ``enforcing`` + may also originate new actions via ``propose``.

Both LLM clients (``OpenAICompatibleLLM`` and ``OllamaCloudLLM``) are
external HTTP — these tests use a fake client to keep the suite
deterministic and offline.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from hermes.service1_agent.core import TradeAction
from hermes.service1_agent.overseer import HermesOverseer

from ._stubs import StubDB


class _FakeLLM:
    """Returns whatever ``reply`` is set to. Captures the last call args."""

    def __init__(self, reply="{\"verdict\": \"APPROVE\", \"rationale\": \"ok\"}"):
        self.reply = reply
        self.last_messages: Optional[List[Dict[str, Any]]] = None
        self.last_images: Optional[List[Any]] = None

    def chat(self, messages, images=None):
        self.last_messages = list(messages)
        self.last_images = list(images or [])
        return self.reply


def _action(symbol: str = "AAPL") -> TradeAction:
    return TradeAction(
        strategy_id="CS75", symbol=symbol, order_class="multileg",
        legs=[
            {"option_symbol": f"{symbol}250620P00090000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": f"{symbol}250620P00085000", "side": "buy_to_open",  "quantity": 1},
        ],
        price=1.50, side="sell", quantity=1, order_type="credit",
        tag="HERMES_CS75", strategy_params={"side_type": "put"},
        expiry="2025-06-20", width=5.0,
    )


# ── advisory: never modifies ─────────────────────────────────────────────────
def test_advisory_passes_action_through_unchanged():
    db = StubDB()
    o = HermesOverseer(_FakeLLM("{\"verdict\":\"VETO\",\"rationale\":\"bad\"}"),
                       db, vision_enabled=False, autonomy="advisory")
    a = _action()
    out = o.review(a)
    # Even when LLM says VETO, advisory passes the action through unchanged.
    assert out is a
    assert not a.ai_authored


# ── enforcing: VETO drops the action ─────────────────────────────────────────
def test_enforcing_veto_drops_action():
    db = StubDB()
    o = HermesOverseer(_FakeLLM("{\"verdict\":\"VETO\",\"rationale\":\"too risky\"}"),
                       db, vision_enabled=False, autonomy="enforcing")
    assert o.review(_action()) is None


# ── enforcing: APPROVE returns the action ────────────────────────────────────
def test_enforcing_approve_returns_action():
    db = StubDB()
    o = HermesOverseer(_FakeLLM(), db, vision_enabled=False, autonomy="enforcing")
    a = _action()
    out = o.review(a)
    assert out is a
    # APPROVE doesn't set ai_authored — only MODIFY does.
    assert not a.ai_authored


# ── enforcing: MODIFY mutates the action and flags it AI-authored ────────────
def test_enforcing_modify_mutates_action_and_sets_flags():
    db = StubDB()
    o = HermesOverseer(
        _FakeLLM('{"verdict":"MODIFY","rationale":"trim price","modifications":{"price":1.10}}'),
        db, vision_enabled=False, autonomy="enforcing",
    )
    a = _action()
    out = o.review(a)
    assert out is a
    assert a.price == 1.10
    assert a.ai_authored is True
    assert a.ai_rationale == "trim price"


def test_enforcing_modify_ignores_unknown_attrs():
    """MODIFY should only set attrs the dataclass actually has —
    setattr on an unknown attr would create surprises elsewhere."""
    db = StubDB()
    o = HermesOverseer(
        _FakeLLM('{"verdict":"MODIFY","rationale":"x","modifications":{"made_up_field":42,"price":2.0}}'),
        db, vision_enabled=False, autonomy="enforcing",
    )
    a = _action()
    o.review(a)
    assert a.price == 2.0
    assert not hasattr(a, "made_up_field")


# ── LLM unreachable: fail-safe APPROVE ───────────────────────────────────────
class _ExplodingLLM:
    def chat(self, *_a, **_kw):
        raise RuntimeError("LLM unreachable")


def test_llm_failure_passes_action_through_in_enforcing_mode():
    """A network blip shouldn't block trades — the overseer's fail-safe
    is to APPROVE so the rules engine continues to function."""
    db = StubDB()
    o = HermesOverseer(_ExplodingLLM(), db, vision_enabled=False, autonomy="enforcing")
    a = _action()
    out = o.review(a)
    assert out is a
    # And the error is recorded for the watcher to surface.
    assert "LLM unreachable" in db.settings.get("llm_last_error", "")


# ── autonomous: propose() runs only here ─────────────────────────────────────
def test_propose_returns_empty_unless_autonomous():
    db = StubDB()
    for autonomy in ("advisory", "enforcing"):
        o = HermesOverseer(_FakeLLM(), db, vision_enabled=False, autonomy=autonomy)
        assert o.propose(["AAPL"]) == []


def test_propose_builds_trade_actions_from_llm_payload():
    db = StubDB()
    payload = {
        "verdict": "OPEN",
        "action": {
            "strategy_id": "AI",
            "symbol": "AAPL",
            "order_class": "option",
            "legs": [{"option_symbol": "AAPL250620P00090000", "side": "sell_to_open", "quantity": 1}],
            "price": 0.5,
            "side": "sell",
        },
    }
    import json
    o = HermesOverseer(_FakeLLM(json.dumps(payload)), db,
                       vision_enabled=False, autonomy="autonomous")
    proposals = o.propose(["AAPL"])
    assert len(proposals) == 1
    assert proposals[0].symbol == "AAPL"
    assert proposals[0].ai_authored is True


# ── _safe_json: tolerates prose-wrapped JSON ─────────────────────────────────
def test_safe_json_extracts_embedded_json_from_prose():
    text = "Sure, here's my answer:\n{\"verdict\":\"APPROVE\",\"rationale\":\"ok\"}\nLet me know!"
    parsed = HermesOverseer._safe_json(text)
    assert parsed == {"verdict": "APPROVE", "rationale": "ok"}


def test_safe_json_returns_default_on_unparseable_garbage():
    parsed = HermesOverseer._safe_json("not even close to JSON")
    assert parsed["verdict"] == "APPROVE"


def test_safe_json_passes_through_dict():
    """If the LLM client already returned a parsed dict, don't re-decode."""
    parsed = HermesOverseer._safe_json({"verdict": "VETO"})
    assert parsed == {"verdict": "VETO"}


# ── soul appended to system prompt ───────────────────────────────────────────
def test_soul_appended_to_system_prompt():
    db = StubDB()
    o = HermesOverseer(_FakeLLM(), db, vision_enabled=False,
                       autonomy="advisory",
                       soul="Avoid AAPL on FOMC days.")
    assert "Avoid AAPL on FOMC days." in o.SYSTEM_PROMPT


def test_soul_skipped_when_empty():
    db = StubDB()
    o = HermesOverseer(_FakeLLM(), db, vision_enabled=False, autonomy="advisory")
    assert "OPERATOR DOCTRINE" not in o.SYSTEM_PROMPT
