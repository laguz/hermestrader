"""
[Service-1: Hermes-Agent-Core] — Autonomous origination + vision reads.

Split out of ``overseer.py`` to separate the overseer's *generative* surface
(originating trades, exits, self-directed setups, and informational chart
analysis) from the review path (:mod:`overseer_single` /
:mod:`overseer_committee`), the out-of-loop governance tuning
(:mod:`overseer_governance`), and the event-bus worker (:mod:`overseer_worker`).

:class:`OverseerProposers` is an injected collaborator owned by
:class:`~hermes.service1_agent.overseer.HermesOverseer`: it reads the overseer's
state (autonomy, db, vision/chart, llm) and reuses its LLM transport
(``get_system_prompt`` / ``_chat_with_timeout`` / ``_safe_json``) through a
back-reference, so the method bodies moved out of the overseer unchanged.

Like every proposer here, the LLM never authors raw legs or prices — it picks
*which* trades to open or close, and the engine fills price from live quotes.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from .core import TradeAction

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .overseer import HermesOverseer

logger = logging.getLogger("hermes.agent.overseer")


class OverseerProposers:
    """Owns the overseer's autonomous-origination and chart-analysis paths.

    Reads overseer state via ``self._ov``; the forwarding properties below let
    the method bodies keep reading ``self.db`` / ``self.autonomy`` /
    ``self._safe_json`` etc. unchanged, so the extraction was a move, not a
    rewrite.
    """

    def __init__(self, overseer: "HermesOverseer") -> None:
        self._ov = overseer

    # ── forwarded overseer handles (single source of truth on the overseer) ──
    @property
    def autonomy(self):
        return self._ov.autonomy

    @property
    def db(self):
        return self._ov.db

    @property
    def llm(self):
        return self._ov.llm

    @property
    def vision_enabled(self):
        return self._ov.vision_enabled

    @property
    def chart_provider(self):
        return self._ov.chart_provider

    @property
    def _MAX_LOG_CHARS(self):
        return self._ov._MAX_LOG_CHARS

    @property
    def _chat_with_timeout(self):
        return self._ov._chat_with_timeout

    @property
    def _safe_json(self):
        return self._ov._safe_json

    @property
    def get_system_prompt(self):
        return self._ov.get_system_prompt

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
