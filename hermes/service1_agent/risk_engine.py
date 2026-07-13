from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from hermes.common import OCC_RE
from hermes.service1_agent.trade_action import TradeAction
from hermes.broker import BrokerAdapter
from hermes.portfolio.safety_gateway import SafetyGateway
from hermes.service1_agent.tunables import resolve
from hermes.utils import utc_now, date_today
from .money_manager import resolve_entry_sizing, MoneyManager

logger = logging.getLogger("hermes.agent.risk_engine")


def _action_side_type(action: TradeAction) -> str:
    """Normalize an action to its chain side ('put'/'call').

    Every capacity store — ``Trade.side_type``, ``PendingOrder.side``, the
    normalized broker-order counts — is keyed on the option side, not the
    order direction ('sell'/'buy'), so lookups must use the same key. Mirrors
    the normalization in ``TradesRepository.record_pending_order``.
    """
    side_value = (action.strategy_params or {}).get("side_type")
    if side_value and str(side_value).lower() not in {"buy", "sell"}:
        return str(side_value).lower()
    for leg in (action.legs or []):
        m = OCC_RE.match(str(leg.get("option_symbol", "") or ""))
        if m:
            return "put" if m.group(3) == "P" else "call"
    return str(action.side or "").lower()


class PortfolioRiskEngine:
    def __init__(self, broker: BrokerAdapter, db, config: Dict[str, Any], money_manager: Optional[MoneyManager] = None):
        from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
        self.broker = AsyncBrokerWrapper(broker, db)
        self.db = db
        self.config = config or {}
        self.mm = money_manager
        self._broker_order_counts: Dict[tuple[str, str, str, str], int] = {}

    async def _calculate_current_greeks(self, positions: list) -> tuple[float, float, float]:
        """Aggregate net delta, vega, and theta across all open option positions."""
        if not positions:
            return 0.0, 0.0, 0.0

        opt_symbols = [p["symbol"] for p in positions if p.get("symbol") and OCC_RE.match(p["symbol"])]
        if not opt_symbols:
            return 0.0, 0.0, 0.0

        quotes_list = await self.broker.get_quote(",".join(opt_symbols))
        quotes_by_sym = {q["symbol"]: q for q in quotes_list if q.get("symbol")}

        underlyings = set()
        for sym in opt_symbols:
            m = OCC_RE.match(sym)
            if m:
                underlyings.add(m.group(1))

        underlying_quotes = await self.broker.get_quote(",".join(underlyings))
        spots_by_sym = {
            q["symbol"]: float(q.get("last") if q.get("last") is not None else q.get("price", 0.0))
            for q in underlying_quotes if q.get("symbol")
        }

        net_delta = 0.0
        net_vega = 0.0
        net_theta = 0.0

        from hermes.greeks import black_scholes_greeks, implied_volatility

        for p in positions:
            sym = p.get("symbol", "")
            qty = float(p.get("quantity", 0.0) or 0.0)
            if qty == 0.0:
                continue

            m = OCC_RE.match(sym)
            if not m:
                continue

            underlying, yymmdd, pc, strike_raw = m.groups()
            strike = float(strike_raw) / 1000.0
            expiry = datetime.strptime(yymmdd, "%y%m%d").date()
            option_type = "put" if pc == "P" else "call"

            today_val = date_today()
            dte = max(0, (expiry - today_val).days)
            T = max(1.0, float(dte)) / 365.0

            quote = quotes_by_sym.get(sym)
            greeks_dict = quote.get("greeks") if quote else None

            delta_opt = None
            vega_opt = None
            theta_opt = None

            if greeks_dict:
                delta_opt = greeks_dict.get("delta")
                vega_opt = greeks_dict.get("vega")
                theta_opt = greeks_dict.get("theta")

            def to_float(val):
                if val is None:
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            delta_opt = to_float(delta_opt)
            vega_opt = to_float(vega_opt)
            theta_opt = to_float(theta_opt)

            if delta_opt is not None and vega_opt is not None and theta_opt is not None:
                net_delta += qty * delta_opt * 100.0
                net_vega += qty * vega_opt * 100.0
                net_theta += qty * theta_opt * 100.0
            else:
                spot = spots_by_sym.get(underlying)
                if spot is None or spot <= 0:
                    spot = strike

                sigma = None
                if greeks_dict:
                    sigma = to_float(greeks_dict.get("mid_iv") or greeks_dict.get("smv_vol"))

                if sigma is None and quote:
                    bid = to_float(quote.get("bid"))
                    ask = to_float(quote.get("ask"))
                    if bid is not None and ask is not None and bid > 0 and ask > 0:
                        mid = (bid + ask) / 2.0
                        sigma = implied_volatility(mid, spot, strike, T, 0.05, option_type)

                local_greeks = None
                if sigma is not None and sigma > 0:
                    try:
                        local_greeks = black_scholes_greeks(spot, strike, T, 0.05, sigma, option_type)
                    except Exception as exc:
                        logger.warning("[RiskEngine] Black-Scholes Greeks calc failed for %s: %s", sym, exc)

                if local_greeks:
                    net_delta += qty * float(local_greeks.get("delta", 0.0)) * 100.0
                    net_vega += qty * float(local_greeks.get("vega", 0.0)) * 100.0
                    net_theta += qty * float(local_greeks.get("theta_daily", 0.0)) * 100.0
                else:
                    logger.warning(
                        "[RiskEngine] Greeks unavailable for position %s (qty: %s, spot: %s). Treating conservatively.",
                        sym, qty, spot
                    )
                    max_vega = spot * 0.3989 * math.sqrt(T)
                    if qty < 0:
                        pos_vega = qty * max_vega * 100.0
                    else:
                        pos_vega = 0.0
                    net_vega += pos_vega

                    if qty < 0:
                        pos_delta = qty * 1.0 * 100.0 if option_type == "call" else 0.0
                    else:
                        pos_delta = qty * -1.0 * 100.0 if option_type == "put" else 0.0
                    net_delta += pos_delta

        return net_delta, net_vega, net_theta

    async def _calculate_action_greeks_per_lot(
        self, action: TradeAction, candidate_quotes_by_sym: dict, candidate_spots_by_sym: dict
    ) -> tuple[float, float]:
        """Compute vega and delta contribution per lot for a candidate entry."""
        delta_per_lot = 0.0
        vega_per_lot = 0.0

        from hermes.greeks import black_scholes_greeks, implied_volatility

        for leg in (action.legs or []):
            sym = leg.get("option_symbol")
            if not sym:
                continue
            m = OCC_RE.match(sym)
            if not m:
                continue

            underlying, yymmdd, pc, strike_raw = m.groups()
            strike = float(strike_raw) / 1000.0
            expiry = datetime.strptime(yymmdd, "%y%m%d").date()
            option_type = "put" if pc == "P" else "call"

            today_val = date_today()
            dte = max(0, (expiry - today_val).days)
            T = max(1.0, float(dte)) / 365.0

            side_str = str(leg.get("side", "")).lower()
            if "sell" in side_str:
                coef = -1.0
            elif "buy" in side_str:
                coef = 1.0
            else:
                coef = 1.0

            action_qty = float(action.quantity or 1.0)
            if action_qty <= 0.0:
                action_qty = 1.0
            leg_qty_per_lot = float(leg.get("quantity", 1.0) or 1.0) / action_qty

            quote = candidate_quotes_by_sym.get(sym)
            greeks_dict = quote.get("greeks") if quote else None

            delta_opt = None
            vega_opt = None

            if greeks_dict:
                delta_opt = greeks_dict.get("delta")
                vega_opt = greeks_dict.get("vega")

            def to_float(val):
                if val is None:
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            delta_opt = to_float(delta_opt)
            vega_opt = to_float(vega_opt)

            if delta_opt is not None and vega_opt is not None:
                delta_per_lot += coef * leg_qty_per_lot * delta_opt * 100.0
                vega_per_lot += coef * leg_qty_per_lot * vega_opt * 100.0
            else:
                spot = candidate_spots_by_sym.get(underlying)
                if spot is None or spot <= 0:
                    spot = strike

                sigma = None
                if greeks_dict:
                    sigma = to_float(greeks_dict.get("mid_iv") or greeks_dict.get("smv_vol"))

                if sigma is None and quote:
                    bid = to_float(quote.get("bid"))
                    ask = to_float(quote.get("ask"))
                    if bid is not None and ask is not None and bid > 0 and ask > 0:
                        mid = (bid + ask) / 2.0
                        sigma = implied_volatility(mid, spot, strike, T, 0.05, option_type)

                local_greeks = None
                if sigma is not None and sigma > 0:
                    try:
                        local_greeks = black_scholes_greeks(spot, strike, T, 0.05, sigma, option_type)
                    except Exception as exc:
                        logger.warning("[RiskEngine] BS greeks calc failed for candidate leg %s: %s", sym, exc)

                if local_greeks:
                    delta_per_lot += coef * leg_qty_per_lot * float(local_greeks.get("delta", 0.0)) * 100.0
                    vega_per_lot += coef * leg_qty_per_lot * float(local_greeks.get("vega", 0.0)) * 100.0
                else:
                    logger.warning("[RiskEngine] Greeks unavailable for candidate leg %s. Treating conservatively.", sym)
                    max_vega = spot * 0.3989 * math.sqrt(T)
                    if coef < 0:
                        leg_vega = coef * leg_qty_per_lot * max_vega * 100.0
                    else:
                        leg_vega = 0.0
                    vega_per_lot += leg_vega

                    if coef < 0:
                        leg_delta = coef * leg_qty_per_lot * 1.0 * 100.0 if option_type == "call" else 0.0
                    else:
                        leg_delta = coef * leg_qty_per_lot * -1.0 * 100.0 if option_type == "put" else 0.0
                    delta_per_lot += leg_delta

        return delta_per_lot, vega_per_lot

    async def record_portfolio_greeks(self) -> None:
        """Aggregate and persist portfolio Greeks snapshot (Service-1 only)."""
        try:
            positions = await self.broker.get_positions() or []
            net_delta, net_vega, net_theta = await self._calculate_current_greeks(positions)
            ts_now = utc_now()
            await self.db.trades.save_greeks_snapshot(net_delta, net_vega, net_theta, ts_now)
            logger.info("[RiskEngine] Portfolio Greeks Snapshot persisted: Delta=%.2f, Vega=%.2f, Theta=%.2f", net_delta, net_vega, net_theta)
        except Exception as exc:
            logger.exception("[RiskEngine] record_portfolio_greeks failed: %s", exc)


    async def _sync_broker_orders(self) -> None:
        counts: Dict[tuple, int] = {}
        try:
            orders = await self.broker.get_normalized_active_orders()
            for o in orders:
                key = (o["strategy_id"], o["symbol"], o["side_type"], o["expiry_iso"])
                counts[key] = counts.get(key, 0) + o["lots"]
            self._broker_order_counts = counts
        except Exception as exc:
            logger.exception("[RiskEngine] Failed to sync broker orders: %s", exc)

    async def _available_bp_and_open_trades(self) -> tuple[Dict[str, Any], float, list]:
        """Account balances, buying power net of ``obp_reserve``, and open trades."""
        balances = await self.broker.get_account_balances() or {}
        available_bp = max(0.0, float(balances.get("option_buying_power", 0.0)))
        try:
            reserve_val = await self.db.settings.get_setting("obp_reserve")
            if reserve_val:
                available_bp = max(0.0, available_bp - float(str(reserve_val).strip()))
        except Exception:
            logger.warning("[RiskEngine] obp_reserve read failed; using full buying power")

        db_open_trades = await self.db.trades.all_open_trades() or []
        return balances, available_bp, db_open_trades

    async def evaluate_and_scale(self, actions: List[TradeAction]) -> List[TradeAction]:
        if not actions:
            return []

        await self._sync_broker_orders()

        if self.config.get("portfolio_optimization"):
            _, available_bp, db_open_trades = await self._available_bp_and_open_trades()

            from hermes.portfolio.optimizer import PortfolioOptimizer
            optimizer = PortfolioOptimizer(self.config)
            optimized = await optimizer.optimize(actions, available_bp, db_open_trades)
            return optimized

        strategy_priority = {
            "CS75": 1,
            "CS7": 2,
            "TT45": 3,
            "WHEEL": 4,
            "HERMESALPHA": 5
        }

        sorted_actions = sorted(
            actions,
            key=lambda a: strategy_priority.get(a.strategy_id.upper(), 99)
        )

        balances, available_bp, db_open_trades = await self._available_bp_and_open_trades()
        running_open_trades = list(db_open_trades)

        # 1. Resolve portfolio tunables
        portfolio_tunables = await resolve(self.db, self.config, group="PORTFOLIO")
        max_net_vega = portfolio_tunables.portfolio_max_net_vega
        max_short_delta = portfolio_tunables.portfolio_max_short_delta
        regime_scale_iv_pct = portfolio_tunables.regime_scale_iv_pct
        regime_gross_mult = portfolio_tunables.regime_gross_mult

        # 2. Get current portfolio greeks
        positions = await self.broker.get_positions() or []
        current_net_delta, current_net_vega, _ = await self._calculate_current_greeks(positions)
        running_net_delta = current_net_delta
        running_net_vega = current_net_vega

        account_equity = max(1000.0, float(balances.get("total_equity", 100000.0) or 100000.0))

        # 3. Batch fetch candidate quotes and spots
        candidate_opt_symbols = []
        for action in sorted_actions:
            for leg in (action.legs or []):
                sym = leg.get("option_symbol")
                if sym and OCC_RE.match(sym):
                    candidate_opt_symbols.append(sym)

        candidate_quotes_by_sym = {}
        if candidate_opt_symbols:
            candidate_quotes = await self.broker.get_quote(",".join(candidate_opt_symbols))
            candidate_quotes_by_sym = {q["symbol"]: q for q in candidate_quotes if q.get("symbol")}

        candidate_underlyings = set()
        for sym in candidate_opt_symbols:
            m = OCC_RE.match(sym)
            if m:
                candidate_underlyings.add(m.group(1))

        candidate_spots_by_sym = {}
        if candidate_underlyings:
            candidate_underlying_quotes = await self.broker.get_quote(",".join(candidate_underlyings))
            candidate_spots_by_sym = {
                q["symbol"]: float(q.get("last") if q.get("last") is not None else q.get("price", 0.0))
                for q in candidate_underlying_quotes if q.get("symbol")
            }

        safety_enabled = False
        safety_config = {}
        try:
            enabled_raw = await self.db.settings.get_setting("safety_gateway_enabled")
            if enabled_raw is not None:
                safety_enabled = enabled_raw.lower() == "true"

            max_risk_raw = await self.db.settings.get_setting("safety_max_risk_bp_ratio")
            if max_risk_raw is not None:
                safety_config["safety_max_risk_bp_ratio"] = float(max_risk_raw)
                safety_enabled = True

            max_exp_raw = await self.db.settings.get_setting("safety_max_symbol_exposure_ratio")
            if max_exp_raw is not None:
                safety_config["safety_max_symbol_exposure_ratio"] = float(max_exp_raw)
                safety_enabled = True

            max_trades_raw = await self.db.settings.get_setting("safety_max_symbol_trades")
            if max_trades_raw is not None:
                safety_config["safety_max_symbol_trades"] = int(max_trades_raw)
                safety_enabled = True

            side_lock_raw = await self.db.settings.get_setting("safety_side_lock_enabled")
            if side_lock_raw is not None:
                safety_config["safety_side_lock_enabled"] = side_lock_raw.lower() == "true"
                safety_enabled = True
        except Exception as e:
            logger.warning("[RiskEngine] Failed to load safety settings from DB: %s", e)

        safety_gateway = SafetyGateway(safety_config) if safety_enabled else None

        in_tick_allocated: Dict[tuple[str, str, str, str], int] = {}
        validated_actions = []

        for action in sorted_actions:
            requested_lots, max_lots, requirement_per_lot = \
                resolve_entry_sizing(action, self.config, self.mm)

            if requested_lots <= 0:
                continue

            side_type = _action_side_type(action)
            key = (action.strategy_id, action.symbol, side_type, action.expiry)
            in_tick_used = in_tick_allocated.get(key, 0)

            open_qty = await self.db.trades.count_open_contracts(action.strategy_id, action.symbol, side_type, action.expiry)
            pending = await self.db.trades.count_pending_orders(action.strategy_id, action.symbol, side_type, action.expiry)
            broker_qty = self._broker_order_counts.get((action.strategy_id, action.symbol, side_type, action.expiry), 0)

            total_used = open_qty + pending + broker_qty + in_tick_used
            side_cap = max(0, max_lots - total_used)

            if requirement_per_lot <= 0.0:
                bp_cap = 999_999
            else:
                bp_cap = int(available_bp // requirement_per_lot)

            # Determine baseline scaled quantity (before regime scaling and ceilings)
            scaled = min(requested_lots, bp_cap, side_cap)

            # Compute IV percentile for regime gross scaling
            iv_percentile = None
            try:
                from hermes.service1_agent.iv_tracker import fetch_current_atm_iv
                current_iv = await fetch_current_atm_iv(self.broker, action.symbol, date_today())
                if current_iv is not None:
                    history = await self.db.timeseries.get_implied_vol_history(action.symbol, lookback_days=365)
                    if history:
                        all_ivs = [iv for _, iv in history] + [current_iv]
                        iv_percentile = 100.0 * sum(1 for iv in all_ivs if iv <= current_iv) / len(all_ivs)
            except Exception as exc:
                logger.warning("[RiskEngine] Failed to compute IV percentile for %s: %s", action.symbol, exc)

            scaling_mult = 1.0
            if iv_percentile is not None:
                if regime_scale_iv_pct is not None and iv_percentile > regime_scale_iv_pct:
                    if regime_gross_mult is not None:
                        scaling_mult = min(1.0, float(regime_gross_mult))

            if scaling_mult < 1.0:
                old_scaled = scaled
                scaled = int(scaled * scaling_mult)
                if scaled < old_scaled:
                    logger.info("[RiskEngine] Regime scaling applied on %s: %d -> %d lots (IV pct: %.1f%%, limit: %.1f%%, mult: %.2f)",
                                action.symbol, old_scaled, scaled, iv_percentile, regime_scale_iv_pct, scaling_mult)

            # Apply Ceilings
            max_vega_limit = None
            if max_net_vega is not None:
                max_vega_limit = float(max_net_vega) * (account_equity / 100000.0)

            max_short_delta_limit = float(max_short_delta) if max_short_delta is not None else None

            # Calculate Greeks per lot for this candidate action
            entry_delta_per_lot, entry_vega_per_lot = await self._calculate_action_greeks_per_lot(
                action, candidate_quotes_by_sym, candidate_spots_by_sym
            )

            original_scaled = scaled
            while scaled > 0:
                new_net_vega = running_net_vega + scaled * entry_vega_per_lot
                new_net_delta = running_net_delta + scaled * entry_delta_per_lot

                vega_ok = True
                if max_vega_limit is not None:
                    if abs(new_net_vega) > max_vega_limit:
                        if abs(new_net_vega) > abs(running_net_vega):
                            vega_ok = False

                delta_ok = True
                if max_short_delta_limit is not None:
                    if new_net_delta < 0 and abs(new_net_delta) > max_short_delta_limit:
                        if new_net_delta < running_net_delta:
                            delta_ok = False

                if vega_ok and delta_ok:
                    break
                scaled -= 1

            if scaled == 0 and original_scaled > 0:
                logger.warning(
                    "[RiskEngine] Candidate entry %s dropped: even 1 lot breaches portfolio ceiling. "
                    "Running Vega: %.2f, Entry Vega/lot: %.2f, Limit Vega: %s. "
                    "Running Delta: %.2f, Entry Delta/lot: %.2f, Limit Delta: %s.",
                    action.symbol, running_net_vega, entry_vega_per_lot,
                    f"{max_vega_limit:.2f}" if max_vega_limit is not None else "None",
                    running_net_delta, entry_delta_per_lot,
                    f"{max_short_delta_limit:.2f}" if max_short_delta_limit is not None else "None"
                )
                await self.db.logs.write_log(
                    action.strategy_id,
                    f"[RISK VIOLATION] {action.symbol} dropped: even 1 lot breaches portfolio ceiling. "
                    f"Vega: running={running_net_vega:.2f} entry={entry_vega_per_lot:.2f}/lot limit={f'{max_vega_limit:.2f}' if max_vega_limit is not None else 'None'}. "
                    f"Delta: running={running_net_delta:.2f} entry={entry_delta_per_lot:.2f}/lot limit={f'{max_short_delta_limit:.2f}' if max_short_delta_limit is not None else 'None'}."
                )
            elif scaled < original_scaled:
                logger.info(
                    "[RiskEngine] Candidate entry %s scaled down to fit portfolio ceiling: %d -> %d lots.",
                    action.symbol, original_scaled, scaled
                )
                await self.db.logs.write_log(
                    action.strategy_id,
                    f"[RISK CONTROL] Scaled {action.symbol} {original_scaled}->{scaled} lots to fit portfolio ceiling."
                )

            # Re-apply quantities to action for safety gateway / validate
            if scaled > 0:
                action.quantity = scaled
                for leg in action.legs:
                    leg["quantity"] = scaled

                if safety_gateway is not None:
                    report = safety_gateway.validate_action(
                        action,
                        {**balances, "option_buying_power": available_bp},
                        running_open_trades
                    )
                    if report.decision == "REJECTED":
                        logger.warning("[RiskEngine] Safety gateway rejected %s entry: %s", action.symbol, report.violations)
                        for violation in report.violations:
                            await self.db.logs.write_log(
                                action.strategy_id,
                                f"[SAFETY VIOLATION] {action.symbol} {side_type.upper()}: {violation}"
                            )
                        scaled = 0

            if scaled == 0 and requested_lots > 0:
                if side_cap == 0:
                    reason = f"at capacity exp={action.expiry} (open+pending={max_lots}/{max_lots})"
                elif bp_cap == 0:
                    acct_type = balances.get("account_type", "?")
                    reason = f"insufficient BP (avail=${available_bp:,.0f} need=${requirement_per_lot:,.0f}/lot acct_type={acct_type})"
                else:
                    reason = f"bp_cap={bp_cap} side_cap={side_cap}"
                await self.db.logs.write_log(
                    action.strategy_id,
                    f"[MM] BLOCKED {action.symbol} {side_type.upper()}: {reason} — 0 lots available",
                )
            elif scaled < requested_lots and original_scaled == requested_lots:
                # Log default MM scaling if no risk/regime scaling was applied
                logger.info(
                    "[RiskEngine] Scaled %s/%s %s %d→%d (bp_cap=%d side_cap=%d)",
                    action.strategy_id, action.symbol, side_type, requested_lots, scaled, bp_cap, side_cap,
                )
                await self.db.logs.write_log(
                    action.strategy_id,
                    f"[MM] Scaled {action.symbol} {side_type.upper()} {requested_lots}→{scaled} lots (bp_cap={bp_cap} side_cap={side_cap})",
                )

            if scaled > 0:
                available_bp -= scaled * requirement_per_lot
                in_tick_allocated[key] = in_tick_used + scaled

                running_net_vega += scaled * entry_vega_per_lot
                running_net_delta += scaled * entry_delta_per_lot

                running_open_trades.append({
                    "symbol": action.symbol,
                    "side_type": action.strategy_params.get("side_type"),
                    "width": action.width if action.width is not None else 0.0,
                    "entry_credit": action.price if action.price is not None else 0.0,
                    "lots": scaled,
                    "expiry": action.expiry
                })
                validated_actions.append(action)

        return validated_actions

