"""
[Service-1: Hermes-Agent-Core] — ML knob-tuning / exit-policy mixin for ``CascadingEngine``.

Split out of ``core.py`` to keep the engine's spine readable. These methods
run as part of :class:`~hermes.service1_agent.core.CascadingEngine` (composed
via inheritance); they reference engine state on ``self`` and are not meant to
be used standalone.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from .trade_action import TradeAction

logger = logging.getLogger("hermes.agent.core")


class EngineTuningMixin:
    async def _maybe_tune_parameters(self) -> None:
        """Run the overseer's goal-aware parameter tuning, throttled by interval.

        Defaults to once per hour (``param_tuning_interval_s``); set the
        interval to 0 to disable. Best-effort — a tuning failure must never
        break the trading tick.
        """
        interval = int(self.config.get("param_tuning_interval_s", 3600))
        if interval <= 0:
            return
        tuner = getattr(self.overseer, "propose_parameter_adjustments", None)
        if tuner is None:
            return
        try:
            import time
            now = time.time()
            last_raw = await self.db.get_setting("ai_last_param_tuning_ts")
            last = float(last_raw) if last_raw else 0.0
            if now - last < interval:
                return
            await self.db.set_setting("ai_last_param_tuning_ts", str(now))
            await tuner()

            # Execute risk restrictions check (banned symbols list)
            risk_tuner = getattr(self.overseer, "propose_risk_restrictions", None)
            if risk_tuner is not None:
                await risk_tuner()
        except Exception as exc:                                   # noqa: BLE001
            logger.exception("[PARAM-TUNE] tuning tick failed: %s", exc)

    async def _maybe_run_bandit_tuner(self) -> None:
        """Run the Thompson-bandit knob tuner, throttled and mode-gated.

        Controlled by the ``bandit_tuner_mode`` setting:

        - ``off`` (default) — does nothing.
        - ``shadow``        — computes proposals and audits them to
                              ``ai_decisions``, but never mutates a setting.
        - ``active``        — additionally applies *actionable* (enough data)
                              and *changed* proposals via ``set_setting``, but
                              only when agent autonomy is enforcing/autonomous.

        Best-effort: any failure is swallowed so a tuning hiccup can never break
        the trading tick. The bandit's arm grids are themselves bounded, so an
        applied value can never escape the knob's tunable range.
        """
        try:
            mode = (await self.db.get_setting("bandit_tuner_mode") or "off")
            mode = str(mode).strip().lower()
            if mode not in ("shadow", "active"):
                return

            import time
            interval = int(self.config.get("bandit_tuning_interval_s", 3600))
            now = time.time()
            last_raw = await self.db.get_setting("bandit_last_run_ts")
            last = float(last_raw) if last_raw else 0.0
            if interval > 0 and now - last < interval:
                return
            await self.db.set_setting("bandit_last_run_ts", str(now))

            from hermes.ml.bandit import propose_knob_updates, LEARNABLE_KNOBS

            outcomes = await self.db.fetch_trade_outcomes()
            keys = [k for knobs in LEARNABLE_KNOBS.values() for k in knobs]
            current: Dict[str, Any] = {}
            bulk = getattr(self.db, "get_settings", None)
            if callable(bulk):
                current = await bulk(keys) or {}

            min_obs = int(self.config.get("bandit_min_observations", 20))
            proposals = propose_knob_updates(
                outcomes, current, min_observations=min_obs)

            autonomy = (getattr(self.overseer, "autonomy", "advisory")
                        if self.overseer is not None else "advisory")
            can_apply = mode == "active" and autonomy in ("enforcing", "autonomous")

            applied: Dict[str, Any] = {}
            for p in proposals:
                if can_apply and p["actionable"] and p["changed"]:
                    await self.db.set_setting(p["key"], str(p["proposed"]))
                    applied[p["key"]] = p["proposed"]
                    await self.db.write_log(
                        "BANDIT",
                        f"[BANDIT-TUNE] {p['key']}: {p['current']} → "
                        f"{p['proposed']} (n={p['n_obs']}, mode={mode})",
                    )

            if applied:
                logger.info("[BANDIT-TUNE] applied %s", applied)
            try:
                await self.db.write_ai_decision(
                    "BANDIT", "PARAMS", autonomy,
                    {"type": "bandit_tuning", "mode": mode,
                     "applied": applied, "proposals": proposals,
                     "min_observations": min_obs},
                )
            except Exception:                                      # noqa: BLE001
                pass
        except Exception as exc:                                   # noqa: BLE001
            logger.exception("[BANDIT-TUNE] tuning tick failed: %s", exc)

    async def _maybe_capture_and_advise_exits(self, mgmt_actions) -> None:
        """Capture exit-state trajectories and run the advisory exit policy.

        Controlled by the ``exit_policy_mode`` setting:

        - ``off`` (default) — does nothing (no extra quote traffic).
        - ``shadow``        — records one ``exit_ticks`` row per open position and
                              audits the policy's hold/close advice to
                              ``ai_decisions``; never closes anything.
        - ``active``        — additionally submits a close for positions the
                              policy *confidently* says to close, but only under
                              enforcing/autonomous autonomy and only for trades
                              not already closing this tick.

        Capture is done here at the engine (not inside the strategies) so the
        money-critical exit logic stays untouched — this path only reads marks
        and writes telemetry. Best-effort: failures never break the tick.
        """
        try:
            mode = (await self.db.get_setting("exit_policy_mode") or "off")
            mode = str(mode).strip().lower()
            if mode not in ("shadow", "active"):
                return

            from hermes.utils import utc_now
            from hermes.ml.exit_policy import train_exit_policy, recommend

            open_trades = await self.db.all_open_trades()
            if not open_trades:
                return

            # Trades a close was already issued for this tick — labelled 'close'
            # and never re-closed by the active policy.
            closing_ids = {
                (a.strategy_params or {}).get("trade_id")
                for a in (mgmt_actions or [])
                if (a.strategy_params or {}).get("trade_id") is not None
            }

            # One batched quote fetch for every leg in the book.
            legs = set()
            for tr in open_trades:
                for k in ("short_leg", "long_leg"):
                    if tr.get(k):
                        legs.add(tr[k])
            quotes: Dict[str, Any] = {}
            if legs:
                raw = await self.broker.get_quote(",".join(sorted(legs))) or []
                quotes = {q["symbol"]: q for q in raw if "symbol" in q}

            def _mid(sym):
                q = quotes.get(sym) or {}
                try:
                    bid, ask = float(q.get("bid")), float(q.get("ask"))
                except (TypeError, ValueError):
                    return None
                # A deep-OTM long leg can legitimately have bid 0; require only
                # a positive ask so (0+ask)/2 is a usable mark for telemetry.
                return (bid + ask) / 2.0 if ask > 0 and bid >= 0 else None

            today = utc_now().date()
            autonomy = (getattr(self.overseer, "autonomy", "advisory")
                        if self.overseer is not None else "advisory")
            can_act = mode == "active" and autonomy in ("enforcing", "autonomous")

            policy = train_exit_policy(await self.db.fetch_exit_ticks())
            advice: List[Dict[str, Any]] = []
            acted: List[int] = []

            for tr in open_trades:
                entry_credit = tr.get("entry_credit")
                short_mid = _mid(tr.get("short_leg"))
                long_mid = _mid(tr.get("long_leg")) if tr.get("long_leg") else 0.0
                expiry = tr.get("expiry")
                if not entry_credit or short_mid is None or long_mid is None or not expiry:
                    continue
                debit = round(short_mid - long_mid, 4)
                pnl_pct = round((float(entry_credit) - debit) / float(entry_credit), 4)
                exp_date = expiry if hasattr(expiry, "year") else None
                if exp_date is None:
                    continue
                dte = (exp_date - today).days

                tid = tr.get("id")
                action = "close" if tid in closing_ids else "hold"
                await self.db.record_exit_tick(
                    trade_id=tid, strategy_id=tr.get("strategy_id"),
                    symbol=tr.get("symbol"), dte=dte, unrealized_pnl_pct=pnl_pct,
                    debit=debit, entry_credit=float(entry_credit), action=action,
                    close_reason=("MANAGED" if action == "close" else None),
                )

                rec = recommend(policy, pnl_pct, dte)
                rec.update({"trade_id": tid, "symbol": tr.get("symbol"),
                            "pnl_pct": pnl_pct, "dte": dte})
                advice.append(rec)

                # Width cap: a W-wide credit spread can never be worth more
                # than W to close, so the close limit is capped at the width —
                # a 5-wide spread can never go out at 5.10. The 5% marketability
                # buffer applies only up to that ceiling.
                width = tr.get("width")
                close_price = round(debit * 1.05, 2)
                if width:
                    close_price = min(close_price, round(float(width), 2))

                if (can_act and rec["confident"] and tid not in closing_ids):
                    close = TradeAction(
                        strategy_id=tr.get("strategy_id"), symbol=tr.get("symbol"),
                        order_class="multileg",
                        legs=[
                            {"option_symbol": tr["short_leg"], "side": "buy_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                            {"option_symbol": tr["long_leg"], "side": "sell_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                        ] if tr.get("long_leg") else [
                            {"option_symbol": tr["short_leg"], "side": "buy_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                        ],
                        price=close_price, side="buy", quantity=1,
                        order_type="debit",
                        tag=f"HERMES_{tr.get('strategy_id')}_CLOSE_EXIT-POLICY",
                        strategy_params={"trade_id": tid, "close_reason": "EXIT-POLICY",
                                         "side_type": tr.get("side_type")},
                        # Engine-authored close — skip overseer re-review, like
                        # other automated actions.
                        ai_authored=True,
                    )
                    await self.submit([close], action_type="management")
                    acted.append(tid)
                    await self.db.write_log(
                        "EXITPOLICY",
                        f"[EXIT-POLICY] closing trade {tid} {tr.get('symbol')} "
                        f"pnl%={pnl_pct} dte={dte} (q_close={rec['q_close']} "
                        f"> q_hold={rec['q_hold']})",
                    )

            if acted:
                logger.info("[EXIT-POLICY] closed %s", acted)
            try:
                await self.db.write_ai_decision(
                    "EXITPOLICY", "EXITS", autonomy,
                    {"type": "exit_policy", "mode": mode, "acted": acted,
                     "n_completed_trajectories": policy["n_completed_trajectories"],
                     "advice": advice},
                )
            except Exception:                                      # noqa: BLE001
                pass
        except Exception as exc:                                   # noqa: BLE001
            logger.exception("[EXIT-POLICY] capture/advise tick failed: %s", exc)

    async def _maybe_evaluate_reactive_exit(self, symbol: str, mgmt_actions) -> None:
        """Evaluate continuous exit model reactively on quote changes for a specific option symbol."""
        try:
            mode = (await self.db.get_setting("exit_policy_mode") or "off")
            mode = str(mode).strip().lower()
            if mode not in ("shadow", "active"):
                return

            open_trades = await self.db.all_open_trades()
            if not open_trades:
                return

            # Filter open trades to only those containing this ticking option leg symbol
            trades_for_symbol = [
                t for t in open_trades
                if t.get("short_leg") == symbol or t.get("long_leg") == symbol
            ]
            if not trades_for_symbol:
                return

            # Skip if a close was already issued for this tick
            closing_ids = {
                (a.strategy_params or {}).get("trade_id")
                for a in (mgmt_actions or [])
                if (a.strategy_params or {}).get("trade_id") is not None
            }

            from hermes.utils import utc_now
            from hermes.ml.exit_policy import train_exit_policy, recommend

            today = utc_now().date()
            autonomy = (getattr(self.overseer, "autonomy", "advisory")
                        if self.overseer is not None else "advisory")
            can_act = mode == "active" and autonomy in ("enforcing", "autonomous")

            policy = train_exit_policy(await self.db.fetch_exit_ticks())
            advice: List[Dict[str, Any]] = []
            acted: List[int] = []

            for tr in trades_for_symbol:
                tid = tr.get("id")
                if tid in closing_ids:
                    continue

                entry_credit = tr.get("entry_credit")
                short_leg = tr.get("short_leg")
                long_leg = tr.get("long_leg")
                expiry = tr.get("expiry")

                if not entry_credit or not expiry:
                    continue

                # Retrieve prices from cache
                short_mid = None
                if short_leg in self._quote_cache:
                    q = self._quote_cache[short_leg]
                    try:
                        short_mid = (float(q.get("bid")) + float(q.get("ask"))) / 2.0
                    except (TypeError, ValueError):
                        pass

                long_mid = 0.0
                if long_leg:
                    if long_leg in self._quote_cache:
                        q = self._quote_cache[long_leg]
                        try:
                            long_mid = (float(q.get("bid")) + float(q.get("ask"))) / 2.0
                        except (TypeError, ValueError):
                            pass
                    else:
                        continue

                if short_mid is None:
                    continue

                debit = round(short_mid - long_mid, 4)
                pnl_pct = round((float(entry_credit) - debit) / float(entry_credit), 4)
                exp_date = expiry if hasattr(expiry, "year") else None
                if exp_date is None:
                    continue
                dte = (exp_date - today).days

                rec = recommend(policy, pnl_pct, dte)
                rec.update({"trade_id": tid, "symbol": tr.get("symbol"),
                            "pnl_pct": pnl_pct, "dte": dte})
                advice.append(rec)

                width = tr.get("width")
                close_price = round(debit * 1.05, 2)
                if width:
                    close_price = min(close_price, round(float(width), 2))

                if (can_act and rec["confident"]):
                    close = TradeAction(
                        strategy_id=tr.get("strategy_id"), symbol=tr.get("symbol"),
                        order_class="multileg",
                        legs=[
                            {"option_symbol": tr["short_leg"], "side": "buy_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                            {"option_symbol": tr["long_leg"], "side": "sell_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                        ] if tr.get("long_leg") else [
                            {"option_symbol": tr["short_leg"], "side": "buy_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                        ],
                        price=close_price, side="buy", quantity=1,
                        order_type="debit",
                        tag=f"HERMES_{tr.get('strategy_id')}_CLOSE_EXIT-POLICY-REACTIVE",
                        strategy_params={"trade_id": tid, "close_reason": "EXIT-POLICY-REACTIVE",
                                         "side_type": tr.get("side_type")},
                        ai_authored=True,
                    )
                    await self.submit([close], action_type="management")
                    acted.append(tid)
                    await self.db.write_log(
                        "EXITPOLICY",
                        f"[REACTIVE-EXIT] closing trade {tid} {tr.get('symbol')} "
                        f"pnl%={pnl_pct} dte={dte} (q_close={rec['q_close']} "
                        f"> q_hold={rec['q_hold']})",
                    )

            if acted:
                logger.info("[REACTIVE-EXIT] closed %s", acted)
                try:
                    await self.db.write_ai_decision(
                        "EXITPOLICY", "REACTIVE-EXITS", autonomy,
                        {"type": "exit_policy_reactive", "mode": mode, "acted": acted,
                         "advice": advice},
                    )
                except Exception:
                    pass
        except Exception as exc:
            logger.exception("[REACTIVE-EXIT] evaluation failed: %s", exc)
