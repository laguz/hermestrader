"""
[Service-1: Hermes-Agent-Core] — Hermes AI Overseer
A local LLM (Gemma 3 Flash / Gemma 4 e4b) reviews proposed TradeActions, may VETO
or MODIFY them, and may PROPOSE new ones from chart-image analysis. The class is
provider-agnostic — `LLMClient` is any object with `.chat(messages, images=...)`.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from hermes.events.bus import EventBus, ReviewRequestEvent, AIApprovalEvent
from .core import TradeAction
from .overseer_committee import CommitteeReviewer
from .overseer_monolithic import MonolithicReviewer

logger = logging.getLogger("hermes.agent.overseer")


class HermesOverseer:
    """Visual + statistical override layer above the rules engine."""

    BASE_SYSTEM_PROMPT = (
        "You are HERMES, a quantitative options-trading overseer. "
        "You review trade actions produced by rule-based strategies and decide: "
        "APPROVE / VETO / MODIFY. You also propose new trades when chart context "
        "shows superior setups or imminent risks the rules missed. "
        "Output strict JSON."
    )

    def __init__(self, llm_client, db, *, vision_enabled: bool = True,
                 chart_provider=None, autonomy: str = "advisory",
                 soul: Optional[str] = None,
                 overseer_mode: str = "monolithic",
                 event_bus: Optional[EventBus] = None):
        """
        autonomy: 'advisory'  → log decisions, never block (default for new deployments)
                  'enforcing' → veto/modify takes effect
                  'autonomous'→ may also originate new actions

        soul: free-text operator doctrine (typically the contents of a
              soul.md the user maintains in the watcher). When non-empty it
              is appended to the base system prompt on every LLM call, so
              the overseer is shaped by both the base instructions and the
              operator's current preferences without anyone touching code.
        """
        self.llm = llm_client
        self.db = db
        self.vision_enabled = vision_enabled
        self.chart_provider = chart_provider
        self.autonomy = autonomy
        self.soul = (soul or "").strip()
        self.overseer_mode = overseer_mode
        self.event_bus = event_bus
        # The two review paths, owned and routed to from _consult per the
        # overseer_mode setting. Both read this overseer's state and reuse its
        # LLM transport via a back-reference: committee for the multi-agent
        # path, monolithic for the single-LLM path (and the committee's own
        # failure fallback).
        self.committee = CommitteeReviewer(self)
        self.monolithic = MonolithicReviewer(self)
        self._queue: Optional[asyncio.Queue[ReviewRequestEvent]] = None
        self._worker_task: Optional[asyncio.Task] = None

    async def _chat_with_timeout(self, messages: List[Dict[str, str]], images: List[Any] = None) -> str:
        """Call the LLM with a strict timeout gate to prevent hanging."""
        timeout_val = getattr(self.llm, "timeout_s", 15.0)
        # Safeguard: if self.llm is a MagicMock, getattr returns a mock object, which is not a float/int
        if not isinstance(timeout_val, (int, float)):
            timeout_s = 15.0
        else:
            timeout_s = timeout_val or 15.0

        return await asyncio.wait_for(
            asyncio.to_thread(self.llm.chat, messages, images=images or []),
            timeout=timeout_s
        )

    async def get_system_prompt(self) -> str:
        """Base instructions + market session context + operator doctrine + strategy metrics."""
        try:
            from hermes.market_hours import session_label
            mkt_line = session_label()
        except Exception:                                        # noqa: BLE001
            mkt_line = ""

        parts = [self.BASE_SYSTEM_PROMPT]
        if mkt_line:
            parts.append(f"\nCURRENT MARKET STATUS: {mkt_line}")

        try:
            if self.db is not None:
                perf_metrics = await self.db.analytics.get_strategy_performance_metrics(days=30)
                perf_lines = []
                for strat, data in perf_metrics.items():
                    perf_lines.append(
                        f"- {strat}: status={data['status']}, closed={data['closed_trades']}, "
                        f"passed={data['passed']}, failed={data['failed']}, total_pnl=${data['total_pnl']:.2f}"
                    )
                perf_str = "\n".join(perf_lines)
                parts.append(f"\nRECENT STRATEGY PERFORMANCE (30-DAY WINDOW):\n{perf_str}")
        except Exception as exc:
            logger.warning("Failed to fetch performance metrics for SYSTEM_PROMPT: %s", exc)

        if self.soul:
            parts.append(
                "\n--- OPERATOR DOCTRINE (soul.md) ---\n"
                f"{self.soul}\n"
                "--- END DOCTRINE ---"
            )
        return "\n".join(parts)

    # -- review existing rule-driven actions ---------------------------------
    async def review(self, action: TradeAction) -> Optional[TradeAction]:
        if self.autonomy == "advisory":
            decision = await self._consult(action)
            await self.db.decisions.write_ai_decision(action.strategy_id, action.symbol,
                                      "advisory", decision)
            return action

        decision = await self._consult(action)
        await self.db.decisions.write_ai_decision(action.strategy_id, action.symbol,
                                  self.autonomy, decision)

        verdict = decision.get("verdict", "APPROVE").upper()
        if verdict == "VETO":
            return None
        if verdict == "MODIFY" and isinstance(decision.get("modifications"), dict):
            for k, v in decision["modifications"].items():
                if hasattr(action, k):
                    setattr(action, k, v)
            action.ai_authored = True
            action.ai_rationale = decision.get("rationale")
        return action

    # -- propose new actions (vision-driven) ---------------------------------
    async def propose(self, watchlist: Iterable[str]) -> List[TradeAction]:
        if self.autonomy != "autonomous":
            return []
        proposed: List[TradeAction] = []
        for symbol in watchlist:
            chart = await self.chart_provider.snapshot(symbol) if self.chart_provider else None
            payload = await self._propose_for(symbol, chart)
            if not payload:
                continue
            try:
                a = TradeAction(**payload, ai_authored=True)
                proposed.append(a)
            except Exception as exc:                                   # noqa: BLE001
                logger.warning("Overseer proposal malformed for %s: %s", symbol, exc)
        return proposed

    # -- close existing positions (autonomous) -------------------------------
    async def propose_closes(self) -> List[TradeAction]:
        """Let the overseer decide which currently-OPEN positions to close.

        The mirror image of :meth:`propose`: where ``propose`` originates new
        entries, this originates exits. Only ``autonomous`` mode acts — the
        overseer must already be trusted to author trades before it may
        unwind them.

        Division of labour matches the entry path: the LLM picks *which*
        trades to close (by id, with a rationale); it never authors raw legs
        or prices. We build the close legs from the real Trade rows and leave
        ``price`` for the engine to fill from live quotes (see
        ``CascadingEngine._price_ai_closes``). Equity positions are out of
        scope here — closing shares needs an equity sell order, not a
        debit-to-close — so only option positions (those carrying a short
        leg) are offered as candidates.
        """
        if self.autonomy != "autonomous":
            return []
        try:
            trades = await self.db.trades.all_open_trades()
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("propose_closes: all_open_trades failed: %s", exc)
            return []

        candidates = [t for t in trades if t.get("short_leg")]
        if not candidates:
            return []

        today = datetime.now(timezone.utc).date()
        summary: List[Dict[str, Any]] = []
        for t in candidates:
            exp = t.get("expiry")
            dte = None
            if exp:
                try:
                    d = exp if hasattr(exp, "isoformat") else \
                        datetime.strptime(str(exp), "%Y-%m-%d").date()
                    dte = (d - today).days
                except Exception:                                  # noqa: BLE001
                    dte = None
            summary.append({
                "trade_id": t["id"], "strategy": t.get("strategy_id"),
                "symbol": t.get("symbol"), "side": t.get("side_type"),
                "lots": t.get("lots"), "entry_credit": t.get("entry_credit"),
                "expiry": str(exp) if exp else None, "dte": dte,
            })

        recent_logs = await self.db.logs.recent_logs(limit=200)
        if len(recent_logs) > self._MAX_LOG_CHARS:
            recent_logs = "[...truncated...]\n" + recent_logs[-self._MAX_LOG_CHARS:]

        system_prompt = await self.get_system_prompt()
        prompt = (
            "Review the currently OPEN option positions below against market "
            "context and the recent execution log. Decide which, if any, to "
            "CLOSE now — to lock in profit or to cut risk before it grows. "
            "Closing is optional: return an empty list if every position "
            "should be held.\n"
            f"OPEN_POSITIONS:\n{json.dumps(summary, default=str)}\n"
            f"RECENT_LOGS:\n{recent_logs}\n"
            "Reply with strict JSON "
            "{closes: [{trade_id: int, rationale: str}, ...]}."
        )
        try:
            msg = await self._chat_with_timeout(
                [{"role": "system", "content": system_prompt},
                 {"role": "user",   "content": prompt}],
                images=[],
            )
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("propose_closes LLM call failed: %s", exc)
            return []

        decision = self._safe_json(msg)
        closes = decision.get("closes")
        if not isinstance(closes, list):
            return []

        by_id = {t["id"]: t for t in candidates}
        actions: List[TradeAction] = []
        for c in closes:
            if not isinstance(c, dict):
                continue
            try:
                tid = int(c.get("trade_id"))
            except (TypeError, ValueError):
                continue
            trade = by_id.get(tid)
            if trade is None:
                logger.info("propose_closes: trade_id %s is not an open "
                            "option position; skip", c.get("trade_id"))
                continue
            action = self._build_close_action(trade, c.get("rationale") or "AI close")
            if action is not None:
                actions.append(action)

        if actions:
            try:
                await self.db.decisions.write_ai_decision(
                    "OVERSEER", "CLOSES", self.autonomy,
                    {"type": "propose_closes",
                     "trade_ids": [a.strategy_params.get("trade_id") for a in actions]},
                )
            except Exception:                                      # noqa: BLE001
                pass
        return actions

    @staticmethod
    def _build_close_action(trade: Dict[str, Any], rationale: str) -> Optional[TradeAction]:
        """Build a debit-to-close TradeAction from a real OPEN Trade row.

        ``price`` is intentionally left ``None``: the engine fills it from
        live quotes at submit time so the close is priced against the market,
        not against a stale entry credit or an LLM guess. The action is
        flagged ``ai_authored`` so the engine routes it as a management close
        and skips re-reviewing the overseer's own decision.
        """
        short_leg = trade.get("short_leg")
        if not short_leg:
            return None
        lots = int(trade.get("lots") or 1)
        legs = [{"option_symbol": short_leg, "side": "buy_to_close", "quantity": lots}]
        order_class = "option"
        long_leg = trade.get("long_leg")
        if long_leg:
            legs.append({"option_symbol": long_leg, "side": "sell_to_close", "quantity": lots})
            order_class = "multileg"
        strat = trade.get("strategy_id")
        exp = trade.get("expiry")
        return TradeAction(
            strategy_id=strat, symbol=trade["symbol"], order_class=order_class,
            legs=legs, price=None, side="buy", quantity=1, order_type="debit",
            tag=f"HERMES_{strat}_CLOSE_AI",
            strategy_params={"trade_id": trade["id"], "close_reason": "AI_CLOSE",
                             "side_type": trade.get("side_type")},
            expiry=str(exp) if exp else None, width=trade.get("width"),
            ai_authored=True, ai_rationale=rationale,
        )

    # -- self-directed setup selection (HermesAlpha) -------------------------
    async def propose_alpha_setup(
        self, universe: Iterable[str], open_positions: Iterable[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Pick ONE self-directed credit-spread setup from a bounded universe.

        Drives the HermesAlpha strategy: the overseer expresses *intent*
        (symbol, side, short-leg delta, DTE, width, lots) and the strategy
        turns it into real legs, clamping every numeric to a safe range.
        Like ``propose`` / ``propose_closes`` the LLM never authors raw legs
        or prices — it only chooses the setup, from a symbol list it may not
        leave. Returns the intent dict, or ``None`` to stand down this tick.

        Unlike ``propose``/``propose_closes`` this is not gated on the
        overseer's ``autonomy`` setting: enabling the HermesAlpha strategy is
        itself the authorisation to let Hermes trade his own book.
        """
        universe = [str(s).upper().strip() for s in (universe or []) if str(s).strip()]
        if not universe or self.llm is None:
            return None

        recent_logs = await self.db.logs.recent_logs(limit=200)
        if len(recent_logs) > self._MAX_LOG_CHARS:
            recent_logs = "[...truncated...]\n" + recent_logs[-self._MAX_LOG_CHARS:]

        system_prompt = await self.get_system_prompt()
        prompt = (
            "You are running the HermesAlpha book — your own self-directed "
            "options strategy. Choose ONE credit spread to SELL now, or stand "
            "down if nothing is compelling.\n"
            f"UNIVERSE (you may only pick a symbol from this list): {universe}\n"
            f"ALREADY OPEN (do not duplicate these): {list(open_positions or [])}\n"
            f"RECENT_LOGS:\n{recent_logs}\n"
            "Decide: side ('put' = bull-put spread below support, 'call' = "
            "bear-call spread above resistance); the short-leg target delta "
            "(0.05-0.45, higher = more premium and more risk); days-to-expiry "
            "(5-45); spread width in strike points (1-10); and lot count (>=1). "
            "Reply with strict JSON {verdict: 'OPEN'|'PASS', symbol, side, "
            "target_delta, dte, width, lots, rationale}. Use PASS to hold fire."
        )
        try:
            msg = await self._chat_with_timeout(
                [{"role": "system", "content": system_prompt},
                 {"role": "user",   "content": prompt}],
                images=[],
            )
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("propose_alpha_setup LLM call failed: %s", exc)
            return None

        data = self._safe_json(msg)
        if not isinstance(data, dict) or str(data.get("verdict", "")).upper() == "PASS":
            return None
        symbol = str(data.get("symbol", "")).upper().strip()
        if symbol not in set(universe):
            logger.info("propose_alpha_setup: %r not in universe; stand down", symbol)
            return None
        try:
            await self.db.decisions.write_ai_decision("HermesAlpha", symbol, "autonomous",
                                            {"type": "alpha_setup", **data})
        except Exception:                                          # noqa: BLE001
            pass
        return data

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
        if self.autonomy not in ("enforcing", "autonomous"):
            return {"applied": {}, "rationale": "advisory mode — no changes", "skipped": []}

        tunables = self._tunable_params()
        current: Dict[str, Any] = {}
        for key, (kind, lo, _hi) in tunables.items():
            raw = await self.db.settings.get_setting(key)
            if raw is None:
                # Surface the in-effect default so the LLM sees a real baseline.
                from .entry_gate import DEFAULTS as GATE_DEFAULTS
                raw = GATE_DEFAULTS.get(key)
            current[key] = raw

        bounds_desc = {
            k: f"{kind}[{lo}..{hi}]" for k, (kind, lo, hi) in tunables.items()
        }
        system_prompt = await self.get_system_prompt()
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
            msg = await self._chat_with_timeout(
                [{"role": "system", "content": system_prompt},
                 {"role": "user", "content": prompt}],
                images=[],
            )
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("Parameter-tuning LLM call failed: %s", exc)
            return {"applied": {}, "rationale": f"LLM error: {exc}", "skipped": []}

        decision = self._safe_json(msg)
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
            await self.db.settings.set_setting(key, str(coerced))
            applied[key] = coerced
            await self.db.logs.write_log(
                "OVERSEER",
                f"[PARAM-TUNE] {key}: {old} → {coerced} (goal-aligned)",
            )

        if applied:
            logger.info("[PARAM-TUNE] applied %s — %s", applied, rationale)
        result = {"applied": applied, "rationale": rationale, "skipped": skipped}
        try:
            await self.db.decisions.write_ai_decision(
                "OVERSEER", "PARAMS", self.autonomy,
                {"type": "param_tuning", **result},
            )
        except Exception:                                          # noqa: BLE001
            pass
        return result

    async def propose_risk_restrictions(self) -> Dict[str, Any]:
        """Let the overseer decide which symbols from the active watchlist should be temporarily banned due to risk.

        Only runs in 'enforcing' or 'autonomous' modes — advisory never mutates settings.
        """
        if self.autonomy not in ("enforcing", "autonomous"):
            return {"banned_symbols": [], "rationale": "advisory mode — no changes"}

        watchlist_syms = set()
        try:
            if self.db is not None:
                all_wls = await self.db.watchlist.list_all_watchlists()
                for syms in all_wls.values():
                    watchlist_syms.update(syms)
        except Exception as exc:
            logger.warning("Failed to fetch watchlist symbols for risk restrictions: %s", exc)

        if not watchlist_syms:
            return {"banned_symbols": [], "rationale": "watchlist is empty"}

        recent_logs = await self.db.logs.recent_logs(limit=200)
        if len(recent_logs) > self._MAX_LOG_CHARS:
            recent_logs = "[...truncated...]\n" + recent_logs[-self._MAX_LOG_CHARS:]

        system_prompt = await self.get_system_prompt()
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
            msg = await self._chat_with_timeout(
                [{"role": "system", "content": system_prompt},
                 {"role": "user",   "content": prompt}],
                images=[],
            )
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("Risk-restrictions LLM call failed: %s", exc)
            return {"banned_symbols": [], "rationale": f"LLM error: {exc}"}

        decision = self._safe_json(msg)
        banned = decision.get("banned_symbols") or []
        rationale = decision.get("rationale", "")
        if not isinstance(banned, list):
            banned = []

        # Intersect with active watchlist to prevent arbitrary symbols being injected
        banned_set = {str(s).upper().strip() for s in banned} & {s.upper() for s in watchlist_syms}
        banned_list = sorted(list(banned_set))

        old_banned = await self.db.settings.get_setting("banned_symbols") or ""
        new_banned_str = ",".join(banned_list)

        if old_banned != new_banned_str:
            await self.db.settings.set_setting("banned_symbols", new_banned_str)
            await self.db.logs.write_log(
                "OVERSEER",
                f"[RISK-RESTRICT] Banned symbols list updated: {old_banned or '-'} -> {new_banned_str or '-'} — {rationale}",
            )
            logger.info("[RISK-RESTRICT] updated banned symbols to %s — %s", banned_list, rationale)

        result = {"banned_symbols": banned_list, "rationale": rationale}
        try:
            await self.db.decisions.write_ai_decision(
                "OVERSEER", "RISK", self.autonomy,
                {"type": "risk_restrictions", **result},
            )
        except Exception:                                          # noqa: BLE001
            pass
        return result

    # -- chart-only analysis (always runs when vision enabled) ---------------
    async def analyze_charts(self, watchlist: Iterable[str]) -> None:
        """Run a vision-only read on each symbol's chart and store the result.

        Runs regardless of autonomy level — purely informational.  Results are
        stored back on the chart_provider so the C2 API can surface them.
        """
        if not self.vision_enabled or self.chart_provider is None:
            return
        for symbol in watchlist:
            chart = await self.chart_provider.snapshot(symbol)
            if chart is None:
                continue
            system_prompt = await self.get_system_prompt()
            prompt = (
                f"Analyse this price chart for {symbol}. "
                "Identify the current trend, key support/resistance levels, "
                "any chart patterns (e.g. head-and-shoulders, cup-and-handle, "
                "double top/bottom, flags, wedges), RSI regime (overbought / "
                "oversold / neutral), and whether the Bollinger Band squeeze "
                "suggests an imminent volatility expansion. "
                "Reply with JSON: "
                "{trend, support, resistance, pattern, rsi_regime, bb_squeeze, "
                "outlook, rationale} — all string values."
            )
            try:
                msg = await self._chat_with_timeout(
                    [{"role": "system", "content": system_prompt},
                     {"role": "user",   "content": prompt}],
                    images=[chart],
                )
                analysis = self._safe_json(msg)
                verdict  = analysis.get("outlook", "NEUTRAL").upper()
                rationale = analysis.get("rationale", "")
                self.chart_provider.record_analysis(symbol, verdict, rationale, analysis)
                await self.db.decisions.write_ai_decision(
                    "CHART", symbol, "vision",
                    {"type": "chart_analysis", **analysis},
                )
                logger.info("Chart analysis %s → %s", symbol, verdict)
            except Exception as exc:                                   # noqa: BLE001
                logger.warning("Chart analysis failed for %s: %s", symbol, exc)

    # Token budget for log context: ~2 000 tokens ≈ 8 000 chars. Keeps cheap
    # local LLMs from overflowing their context window on vision prompts.
    _MAX_LOG_CHARS = 8_000
    # Retry policy for transient LLM failures (network blip, timeout).
    _LLM_MAX_RETRIES = 3

    # -- LLM I/O -------------------------------------------------------------
    async def _chat_with_retry(self, messages: List[Dict[str, str]], images: List[Any] = None) -> str:
        """Call the LLM with a strict timeout and automatic retry logic."""
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt in range(self._LLM_MAX_RETRIES):
            try:
                return await self._chat_with_timeout(messages, images=images)
            except (asyncio.TimeoutError, Exception) as exc:
                last_exc = exc
                if attempt < self._LLM_MAX_RETRIES - 1:
                    wait_s = 2 ** attempt          # 1 s, 2 s
                    logger.warning(
                        "LLM attempt %d/%d failed/timed out; retrying in %ds: %s",
                        attempt + 1, self._LLM_MAX_RETRIES, wait_s, exc,
                    )
                    await asyncio.sleep(wait_s)
        raise last_exc

    async def _consult(self, action: TradeAction) -> Dict[str, Any]:
        """Routes review to the owned reviewer for the active overseer_mode."""
        if self.overseer_mode == "committee":
            return await self.committee.consult(action)
        return await self.monolithic.consult(action)

    async def _consult_monolithic(self, action: TradeAction) -> Dict[str, Any]:
        """Thin delegator preserving the internal surface (the committee's
        failure fallback calls this). The body now lives on
        :class:`MonolithicReviewer`."""
        return await self.monolithic.consult(action)

    async def _propose_for(self, symbol: str, chart) -> Optional[Dict[str, Any]]:
        system_prompt = await self.get_system_prompt()
        prompt = (
            f"Propose ONE high-conviction options TradeAction for {symbol} or null. "
            "Use only fields from the dataclass schema. JSON only."
        )
        try:
            msg = await self._chat_with_timeout(
                [{"role": "system", "content": system_prompt},
                 {"role": "user",   "content": prompt}],
                images=[chart] if chart is not None else [],
            )
        except Exception as exc:                                       # noqa: BLE001
            logger.warning("LLM propose failed for %s: %s", symbol, exc)
            return None
        data = self._safe_json(msg)
        if not data or data.get("verdict") == "PASS":
            return None
        return data.get("action")

    @staticmethod
    def _safe_json(text: str) -> Dict[str, Any]:
        if isinstance(text, dict):
            return text
        if not isinstance(text, str):
            return {"verdict": "APPROVE", "rationale": "Non-string LLM reply; defaulting."}

        clean_text = text.strip()
        if "```" in clean_text:
            parts = clean_text.split("```")
            for i in range(1, len(parts), 2):
                block = parts[i].strip()
                if block.startswith("json"):
                    block = block[4:].strip()
                start, end = block.find("{"), block.rfind("}")
                if start >= 0 and end > start:
                    try:
                        return json.loads(block[start:end + 1])
                    except Exception:                                  # noqa: BLE001
                        pass

        try:
            return json.loads(clean_text)
        except Exception:                                              # noqa: BLE001
            # Try to find a JSON block embedded in prose
            start, end = clean_text.find("{"), clean_text.rfind("}")
            if start >= 0 and end > start:
                try:
                    return json.loads(clean_text[start:end + 1])
                except Exception:                                      # noqa: BLE001
                    pass
        return {"verdict": "APPROVE", "rationale": "Unparseable LLM reply; defaulting."}

    @property
    def queue(self) -> asyncio.Queue[ReviewRequestEvent]:
        """Lazy-initialize queue so synchronous tests don't fail due to missing event loop."""
        if self._queue is None:
            self._queue = asyncio.Queue()
        return self._queue

    async def start(self) -> None:
        """Start the autonomous background worker."""
        if self.event_bus is None:
            return
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._run_loop())
            self.event_bus.subscribe(ReviewRequestEvent, self.handle_review_request)
            logger.info("HermesOverseer background worker started.")

            if self.db is not None:
                try:
                    pending = await self.db.approvals.fetch_pending_ai_review_actions()
                    if pending:
                        logger.info("Found %d pending AI review(s) in database at startup; enqueuing...", len(pending))
                        from .core import TradeAction
                        for item in pending:
                            try:
                                action = TradeAction(**item["action_json"])
                                event = ReviewRequestEvent(
                                    strategy_id=item["strategy_id"],
                                    symbol=item["symbol"],
                                    trade_action=action,
                                    action_type=item["action_type"],
                                    approval_id=item["id"]
                                )
                                await self.queue.put(event)
                            except Exception as parse_exc:
                                logger.error("Failed to parse pending AI review action id=%d: %s", item["id"], parse_exc)
                except Exception as db_exc:
                    logger.error("Failed to fetch pending AI reviews at startup: %s", db_exc)

    async def stop(self) -> None:
        """Stop the autonomous background worker."""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
            logger.info("HermesOverseer background worker stopped.")

    async def handle_review_request(self, event: ReviewRequestEvent) -> None:
        """Puts review requests onto the queue for sequential processing."""
        await self.queue.put(event)

    async def _run_loop(self) -> None:
        """Sequentially processes review requests from the queue."""
        while True:
            try:
                event = await self.queue.get()
                action = event.trade_action
                
                # Execute LLM review
                decision = await self._consult(action)
                
                # Write to database (advisory/enforcing decision)
                if self.db is not None:
                    await self.db.decisions.write_ai_decision(
                        action.strategy_id,
                        action.symbol,
                        self.autonomy,
                        decision
                    )
                
                verdict = decision.get("verdict", "APPROVE").upper()
                modifications = decision.get("modifications") or {}
                rationale = decision.get("rationale") or ""
                
                # Emit AIApprovalEvent onto the event bus
                approval_event = AIApprovalEvent(
                    strategy_id=action.strategy_id,
                    symbol=action.symbol,
                    verdict=verdict,
                    rationale=rationale,
                    modifications=modifications,
                    original_action=action,
                    action_type=getattr(event, "action_type", "entry"),
                    approval_id=getattr(event, "approval_id", None),
                )
                if self.event_bus:
                    self.event_bus.emit(approval_event)
                
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in HermesOverseer worker loop: %s", exc, exc_info=True)
