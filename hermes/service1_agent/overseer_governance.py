"""
[Service-1: Hermes-Agent-Core] — Out-of-loop governance tuning.

Split out of ``overseer.py`` to separate the overseer's *governance* surface —
the periodic, out-of-loop adjustments it makes to live ``system_settings``
(goal-aware parameter tuning and risk-driven symbol bans) — from the per-action
review path and the trade-origination proposers.

:class:`OverseerGovernor` takes the shared
:class:`~hermes.service1_agent.overseer_context.OverseerContext` (``self.ctx``)
and reads the live state (autonomy, db) and LLM transport (``get_system_prompt``
/ ``chat_with_timeout`` / ``safe_json``) through it — one source of truth, no
per-collaborator forwarding.

Both methods are hard-bounded: every parameter change is clamped to an
allow-listed range, and banned symbols are intersected with the active
watchlist, so the LLM can nudge live settings but can neither invent new ones
nor push a value out of range. Only ``enforcing`` / ``autonomous`` modes mutate
settings — advisory never does.
"""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .overseer_context import OverseerContext

logger = logging.getLogger("hermes.agent.overseer")


class OverseerGovernor:
    """Owns the overseer's out-of-loop settings-tuning paths.

    Reads live state and the LLM transport off the shared
    :class:`~hermes.service1_agent.overseer_context.OverseerContext`
    (``self.ctx``), so there is one source of truth and no forwarding.
    """

    def __init__(self, ctx: "OverseerContext") -> None:
        self.ctx = ctx

    # -- goal-aware parameter tuning -----------------------------------------
    # Allow-list of live-tunable settings the overseer may adjust toward the
    # operator's goal, each as (kind, lo, hi). This is a hard safety boundary:
    # the LLM can nudge these knobs, but can neither invent new settings nor
    # push a value outside its range. Gate-threshold bounds are sourced from
    # entry_gate so the gate and the tuner never drift apart.
    @staticmethod
    def _tunable_params() -> Dict[str, tuple]:
        from .entry_gate import BOUNDS as GATE_BOUNDS
        params: Dict[str, tuple] = {
            # DTE knobs the strategies already read live from system_settings.
            "cs7_dte": ("int", 5, 10),
            "cs75_min_dte": ("int", 30, 45),
            "cs75_max_dte": ("int", 35, 60),
        }
        # AI-entry-gate stringency — "adjust your approval stringency" (soul.md).
        for key, (lo, hi) in GATE_BOUNDS.items():
            kind = "int" if key in ("ai_gate_min_dte", "ai_gate_max_dte") else "float"
            params[key] = (kind, lo, hi)
        return params

    async def propose_parameter_adjustments(self) -> Dict[str, Any]:
        """Let the overseer tune sanctioned knobs toward the operator's goal.

        Rather than inventing arbitrary entry points, the overseer adjusts a
        bounded set of live parameters (DTE windows + AI-gate stringency) in
        response to recent strategy performance and the doctrine in soul.md.
        Only ``enforcing`` / ``autonomous`` modes take effect — advisory never
        mutates live settings.

        Every proposed change is clamped to its allow-listed range and coerced
        to its declared type before it is written; out-of-list keys are ignored.
        Returns ``{"applied": {...}, "rationale": str, "skipped": [...]}``.
        """
        if self.ctx.autonomy not in ("enforcing", "autonomous"):
            return {"applied": {}, "rationale": "advisory mode — no changes", "skipped": []}

        tunables = self._tunable_params()
        current: Dict[str, Any] = {}
        for key, (kind, lo, _hi) in tunables.items():
            raw = await self.ctx.db.settings.get_setting(key)
            if raw is None:
                # Surface the in-effect default so the LLM sees a real baseline.
                from .entry_gate import DEFAULTS as GATE_DEFAULTS
                raw = GATE_DEFAULTS.get(key)
            current[key] = raw

        bounds_desc = {
            k: f"{kind}[{lo}..{hi}]" for k, (kind, lo, hi) in tunables.items()
        }
        system_prompt = await self.ctx.get_system_prompt()
        prompt = (
            "Review recent strategy performance against the operator doctrine "
            "and propose adjustments to the TUNABLE PARAMETERS that move the "
            "system toward the stated goal (capital preservation + consistent "
            "positive returns). Tighten stringency (higher POP, lower delta "
            "cap, higher min-credit) for strategies that recently FAILED; you "
            "may relax modestly only for consistent PASSers.\n"
            f"CURRENT VALUES: {json.dumps(current, default=str)}\n"
            f"ALLOWED RANGES: {json.dumps(bounds_desc)}\n"
            "Reply with strict JSON {adjustments: {key: number, ...}, rationale}. "
            "Only include keys you want to change. Omit anything you'd leave as-is."
        )
        try:
            msg = await self.ctx.chat_with_timeout(
                [{"role": "system", "content": system_prompt},
                 {"role": "user", "content": prompt}],
                images=[],
            )
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("Parameter-tuning LLM call failed: %s", exc)
            return {"applied": {}, "rationale": f"LLM error: {exc}", "skipped": []}

        decision = self.ctx.safe_json(msg)
        adjustments = decision.get("adjustments") or {}
        rationale = decision.get("rationale", "")
        applied: Dict[str, Any] = {}
        skipped: List[str] = []

        for key, value in adjustments.items():
            if key not in tunables:
                skipped.append(f"{key} (not tunable)")
                continue
            kind, lo, hi = tunables[key]
            try:
                num = float(value)
            except (TypeError, ValueError):
                skipped.append(f"{key} (non-numeric {value!r})")
                continue
            num = max(float(lo), min(float(hi), num))   # clamp to allow-listed range
            coerced: Any = int(round(num)) if kind == "int" else round(num, 4)
            old = current.get(key)
            if str(old) == str(coerced):
                continue                                # no-op; don't log churn
            await self.ctx.db.settings.set_setting(key, str(coerced))
            applied[key] = coerced
            await self.ctx.db.logs.write_log(
                "OVERSEER",
                f"[PARAM-TUNE] {key}: {old} → {coerced} (goal-aligned)",
            )

        if applied:
            logger.info("[PARAM-TUNE] applied %s — %s", applied, rationale)
        result = {"applied": applied, "rationale": rationale, "skipped": skipped}
        try:
            await self.ctx.db.decisions.write_ai_decision(
                "OVERSEER", "PARAMS", self.ctx.autonomy,
                {"type": "param_tuning", **result},
            )
        except Exception:                                          # noqa: BLE001
            pass
        return result

    async def propose_risk_restrictions(self) -> Dict[str, Any]:
        """Let the overseer decide which symbols from the active watchlist should be temporarily banned due to risk.

        Only runs in 'enforcing' or 'autonomous' modes — advisory never mutates settings.
        """
        if self.ctx.autonomy not in ("enforcing", "autonomous"):
            return {"banned_symbols": [], "rationale": "advisory mode — no changes"}

        watchlist_syms = set()
        try:
            if self.ctx.db is not None:
                all_wls = await self.ctx.db.watchlist.list_all_watchlists()
                for syms in all_wls.values():
                    watchlist_syms.update(syms)
        except Exception as exc:
            logger.warning("Failed to fetch watchlist symbols for risk restrictions: %s", exc)

        if not watchlist_syms:
            return {"banned_symbols": [], "rationale": "watchlist is empty"}

        recent_logs = await self.ctx.db.logs.recent_logs(limit=200)
        if len(recent_logs) > self.ctx.MAX_LOG_CHARS:
            recent_logs = "[...truncated...]\n" + recent_logs[-self.ctx.MAX_LOG_CHARS:]

        system_prompt = await self.ctx.get_system_prompt()
        prompt = (
            "Review recent strategy performance, operator doctrine, and the active watchlist.\n"
            f"ACTIVE WATCHLIST: {list(watchlist_syms)}\n"
            f"RECENT_LOGS:\n{recent_logs}\n"
            "Identify any symbols on the watchlist that pose excessive short-term risk right now "
            "(e.g., due to imminent earnings, technical breakouts/breakdowns, extreme volatility, macro factors) "
            "and should be temporarily banned from rules-based strategy entries. Banned symbols will be completely "
            "skipped for new entries until the next check.\n"
            "Reply with strict JSON: {banned_symbols: [string, ...], rationale}."
        )
        try:
            msg = await self.ctx.chat_with_timeout(
                [{"role": "system", "content": system_prompt},
                 {"role": "user",   "content": prompt}],
                images=[],
            )
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("Risk-restrictions LLM call failed: %s", exc)
            return {"banned_symbols": [], "rationale": f"LLM error: {exc}"}

        decision = self.ctx.safe_json(msg)
        banned = decision.get("banned_symbols") or []
        rationale = decision.get("rationale", "")
        if not isinstance(banned, list):
            banned = []

        # Intersect with active watchlist to prevent arbitrary symbols being injected
        banned_set = {str(s).upper().strip() for s in banned} & {s.upper() for s in watchlist_syms}
        banned_list = sorted(list(banned_set))

        old_banned = await self.ctx.db.settings.get_setting("banned_symbols") or ""
        new_banned_str = ",".join(banned_list)

        if old_banned != new_banned_str:
            await self.ctx.db.settings.set_setting("banned_symbols", new_banned_str)
            await self.ctx.db.logs.write_log(
                "OVERSEER",
                f"[RISK-RESTRICT] Banned symbols list updated: {old_banned or '-'} -> {new_banned_str or '-'} — {rationale}",
            )
            logger.info("[RISK-RESTRICT] updated banned symbols to %s — %s", banned_list, rationale)

        result = {"banned_symbols": banned_list, "rationale": rationale}
        try:
            await self.ctx.db.decisions.write_ai_decision(
                "OVERSEER", "RISK", self.ctx.autonomy,
                {"type": "risk_restrictions", **result},
            )
        except Exception:                                          # noqa: BLE001
            pass
        return result
