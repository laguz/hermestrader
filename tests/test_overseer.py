"""Unit tests for HermesOverseer.

The overseer is the LLM review hook in the trade pipeline. There are
three behaviour modes (``advisory`` / ``enforcing`` / ``autonomous``)
that each have a different contract with ``CascadingEngine.submit``:

- ``advisory``  — log the decision, never modify or block.
- ``enforcing`` — APPROVE / VETO / MODIFY the action.
- ``autonomous``— reviews exactly like ``enforcing`` here, and additionally
  unlocks HermesAlpha origination/exits (``propose_intent`` / ``decide_exit``;
  covered in ``test_hermes_alpha.py``). These tests pin the review behaviour.

Both LLM clients (``OpenAICompatibleLLM`` and ``OllamaCloudLLM``) are
external HTTP — these tests use a fake client to keep the suite
deterministic and offline.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

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


class _CapturingDB(StubDB):
    """StubDB that records every ``write_ai_decision`` call.

    The base StubDB swallows decision writes; these tests assert on the
    *audit trail* the overseer leaves — which authority level acted and what
    verdict it recorded — so we keep the calls instead of dropping them.
    """

    def __init__(self):
        super().__init__()
        self.ai_decisions: List[Dict[str, Any]] = []

    async def write_ai_decision(self, strategy_id, symbol, label, decision, *a, **kw):
        self.ai_decisions.append(
            {"strategy_id": strategy_id, "symbol": symbol,
             "label": label, "decision": decision}
        )


# ── advisory: never modifies ─────────────────────────────────────────────────
async def test_advisory_passes_action_through_unchanged():
    db = StubDB()
    o = HermesOverseer(_FakeLLM("{\"verdict\":\"VETO\",\"rationale\":\"bad\"}"),
                       db, vision_enabled=False, autonomy="advisory")
    a = _action()
    out = await o.review(a)
    # Even when LLM says VETO, advisory passes the action through unchanged.
    assert out is a
    assert not a.ai_authored


# ── enforcing: VETO drops the action ─────────────────────────────────────────
async def test_enforcing_veto_drops_action():
    db = StubDB()
    o = HermesOverseer(_FakeLLM("{\"verdict\":\"VETO\",\"rationale\":\"too risky\"}"),
                       db, vision_enabled=False, autonomy="enforcing")
    assert await o.review(_action()) is None


# ── enforcing: APPROVE returns the action ────────────────────────────────────
async def test_enforcing_approve_returns_action():
    db = StubDB()
    o = HermesOverseer(_FakeLLM(), db, vision_enabled=False, autonomy="enforcing")
    a = _action()
    out = await o.review(a)
    assert out is a
    # APPROVE doesn't set ai_authored — only MODIFY does.
    assert not a.ai_authored


# ── enforcing: MODIFY mutates the action and flags it AI-authored ────────────
async def test_enforcing_modify_mutates_action_and_sets_flags():
    db = StubDB()
    o = HermesOverseer(
        _FakeLLM('{"verdict":"MODIFY","rationale":"trim price","modifications":{"price":1.10}}'),
        db, vision_enabled=False, autonomy="enforcing",
    )
    a = _action()
    out = await o.review(a)
    assert out is a
    assert a.price == 1.10
    assert a.ai_authored is True
    assert a.ai_rationale == "trim price"


async def test_enforcing_modify_ignores_unknown_attrs():
    """MODIFY should only set attrs the dataclass actually has —
    setattr on an unknown attr would create surprises elsewhere."""
    db = StubDB()
    o = HermesOverseer(
        _FakeLLM('{"verdict":"MODIFY","rationale":"x","modifications":{"made_up_field":42,"price":2.0}}'),
        db, vision_enabled=False, autonomy="enforcing",
    )
    a = _action()
    await o.review(a)
    assert a.price == 2.0
    assert not hasattr(a, "made_up_field")


# ── autonomous: enforces review verdicts (Phase 0: same review path as enforcing)
# In Phase 0 ``autonomous`` carries no extra origination; these pin that it still
# applies VETO/MODIFY on review, so a regression that let autonomous skip
# enforcement would be caught.
async def test_autonomous_veto_drops_action():
    db = StubDB()
    o = HermesOverseer(_FakeLLM('{"verdict":"VETO","rationale":"too risky"}'),
                       db, vision_enabled=False, autonomy="autonomous")
    assert await o.review(_action()) is None


async def test_autonomous_modify_mutates_action():
    db = StubDB()
    o = HermesOverseer(
        _FakeLLM('{"verdict":"MODIFY","rationale":"trim price","modifications":{"price":1.10}}'),
        db, vision_enabled=False, autonomy="autonomous",
    )
    a = _action()
    out = await o.review(a)
    assert out is a
    assert a.price == 1.10
    assert a.ai_authored is True


# ── advisory: still consults + records, but acts on nothing ──────────────────
async def test_advisory_consults_and_records_would_be_verdict():
    """Advisory's whole purpose is the dry-run audit trail: it must still call
    the LLM and record what it *would* have done, while leaving the action
    untouched. (A passthrough that skipped the LLM would record nothing.)"""
    db = _CapturingDB()
    llm = _FakeLLM('{"verdict":"VETO","rationale":"would block"}')
    o = HermesOverseer(llm, db, vision_enabled=False, autonomy="advisory")
    a = _action()
    out = await o.review(a)
    assert out is a                       # never blocks
    assert not a.ai_authored              # never modifies
    assert llm.last_messages is not None  # but the LLM was consulted
    assert len(db.ai_decisions) == 1
    rec = db.ai_decisions[0]
    assert rec["label"] == "advisory"
    assert rec["decision"]["verdict"] == "VETO"   # would-be verdict preserved


async def test_recorded_decision_label_matches_autonomy_mode():
    """Every review writes its decision under the acting authority level, so the
    operator's audit log says *who* decided. All three levels are pinned here."""
    for autonomy in ("advisory", "enforcing", "autonomous"):
        db = _CapturingDB()
        o = HermesOverseer(_FakeLLM(), db, vision_enabled=False, autonomy=autonomy)
        await o.review(_action())
        assert [d["label"] for d in db.ai_decisions] == [autonomy]


# ── LLM unreachable: fail-safe APPROVE ───────────────────────────────────────
class _ExplodingLLM:
    def chat(self, *_a, **_kw):
        raise RuntimeError("LLM unreachable")


async def test_llm_failure_passes_action_through_in_enforcing_mode():
    """A network blip shouldn't block trades — the overseer's fail-safe
    is to APPROVE so the rules engine continues to function."""
    db = StubDB()
    o = HermesOverseer(_ExplodingLLM(), db, vision_enabled=False, autonomy="enforcing")
    a = _action()
    out = await o.review(a)
    assert out is a
    # And the error is recorded for the watcher to surface.
    assert "LLM unreachable" in await db.settings.get_setting("llm_last_error", "")


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


def test_safe_json_extracts_markdown_code_block():
    text = "Here's the result:\n```json\n{\n  \"verdict\": \"VETO\",\n  \"rationale\": \"high risk\"\n}\n```\nHope it helps!"
    parsed = HermesOverseer._safe_json(text)
    assert parsed == {"verdict": "VETO", "rationale": "high risk"}


# ── soul appended to system prompt ───────────────────────────────────────────
async def test_soul_appended_to_system_prompt():
    db = StubDB()
    o = HermesOverseer(_FakeLLM(), db, vision_enabled=False,
                       autonomy="advisory",
                       soul="Avoid AAPL on FOMC days.")
    assert "Avoid AAPL on FOMC days." in await o.get_system_prompt()


async def test_soul_skipped_when_empty():
    db = StubDB()
    o = HermesOverseer(_FakeLLM(), db, vision_enabled=False, autonomy="advisory")
    assert "OPERATOR DOCTRINE" not in await o.get_system_prompt()


# ── Overseer-mode router tests ───────────────────────────────────────────────
# Phase 0 ships a single review mode; these guard that any unknown or retired
# mode resolves to the single review path at the router rather than crashing.

class _ExplodingMacroLLM:
    def __init__(self):
        self.single_called = False

    def chat(self, messages, images=None):
        content = " ".join([m.get("content", "") for m in messages])
        if "Macro Specialist" in content:
            raise RuntimeError("macro LLM timeout")
        if "quantitative options-trading overseer" in content:
            self.single_called = True
            return '{"verdict": "VETO", "rationale": "single veto fallback"}'
        return '{"verdict": "APPROVE", "rationale": "risk officer fallback"}'


async def test_legacy_monolithic_mode_routes_to_single_reviewer():
    """The pre-rename value ``monolithic`` is retired: a stored
    ``overseer_mode='monolithic'`` is now unrecognised, so the router resolves
    it to the default single-LLM path (one review call), not the committee
    path (three). Legacy DB rows keep working without a dedicated alias."""
    db = StubDB()
    llm = _ExplodingMacroLLM()
    o = HermesOverseer(llm, db, vision_enabled=False, autonomy="enforcing", overseer_mode="monolithic")
    await o.review(_action())
    # Single path makes exactly one overseer review call; committee would
    # have invoked the two specialists first.
    assert llm.single_called is True


async def test_unknown_mode_routes_to_single_at_the_router():
    """The router — not just the settings readers — is authoritative: a typo'd
    or unknown mode normalizes to the default (single) path here, so it can't
    silently fall through to an unintended reviewer."""
    db = StubDB()
    llm = _ExplodingMacroLLM()
    o = HermesOverseer(llm, db, vision_enabled=False, autonomy="enforcing", overseer_mode="comittee")
    await o.review(_action())
    assert llm.single_called is True


async def test_live_unknown_mode_is_resolved_at_the_router():
    """Modes set live after construction (control_state / main.py path) also
    resolve through the router: switching the live attribute to a retired or
    unrecognised value still takes the default single path."""
    db = StubDB()
    llm = _ExplodingMacroLLM()
    o = HermesOverseer(llm, db, vision_enabled=False, autonomy="enforcing", overseer_mode="committee")
    o.overseer_mode = "monolithic"   # retired value assigned live
    await o.review(_action())
    assert llm.single_called is True

