from __future__ import annotations

import logging
from typing import Any, Dict, List

from hermes.common import OCC_RE
from hermes.service1_agent.trade_action import TradeAction
from hermes.broker import BrokerAdapter
from hermes.portfolio.safety_gateway import SafetyGateway
from .money_manager import parse_occ_strike

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
    def __init__(self, broker: BrokerAdapter, db, config: Dict[str, Any]):
        from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
        self.broker = AsyncBrokerWrapper(broker, db)
        self.db = db
        self.config = config or {}
        self._broker_order_counts: Dict[tuple[str, str, str, str], int] = {}

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

    async def evaluate_and_scale(self, actions: List[TradeAction]) -> List[TradeAction]:
        if not actions:
            return []

        await self._sync_broker_orders()

        if self.config.get("portfolio_optimization"):
            balances = await self.broker.get_account_balances() or {}
            available_bp = max(0.0, float(balances.get("option_buying_power", 0.0)))
            try:
                reserve_val = await self.db.settings.get_setting("obp_reserve")
                if reserve_val:
                    available_bp = max(0.0, available_bp - float(str(reserve_val).strip()))
            except Exception:
                logger.warning("[RiskEngine] obp_reserve read failed; using full buying power")

            db_open_trades = await self.db.trades.all_open_trades() or []

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

        balances = await self.broker.get_account_balances() or {}
        available_bp = max(0.0, float(balances.get("option_buying_power", 0.0)))
        try:
            reserve_val = await self.db.settings.get_setting("obp_reserve")
            if reserve_val:
                available_bp = max(0.0, available_bp - float(str(reserve_val).strip()))
        except Exception:
            logger.warning("[RiskEngine] obp_reserve read failed; using full buying power")

        db_open_trades = await self.db.trades.all_open_trades() or []
        running_open_trades = list(db_open_trades)

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
            requested_lots = action.quantity
            if action.order_class == "multileg" and action.legs:
                requested_lots = action.legs[0].get("quantity", 1)

            if requested_lots <= 0:
                continue

            strat_id = action.strategy_id.upper()
            max_lots_map = {
                "CS7": 1,
                "CS75": 1,
                "TT45": 1,
                "WHEEL": 5,
                "HERMESALPHA": 1,
            }
            config_key = f"{strat_id.lower()}_max_lots"
            _raw_max_lots = self.config.get(config_key)
            max_lots = int(_raw_max_lots) if _raw_max_lots is not None else max_lots_map.get(strat_id, 1)

            requirement_per_lot = 0.0
            if strat_id == "WHEEL":
                if action.strategy_params.get("side_type") == "put" and action.legs:
                    opt_symbol = action.legs[0].get("option_symbol")
                    if opt_symbol:
                        strike = parse_occ_strike(opt_symbol)
                        if strike:
                            requirement_per_lot = strike * 100.0
            else:
                if action.width is not None:
                    requirement_per_lot = action.width * 100.0

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

            scaled = min(requested_lots, bp_cap, side_cap)

            if safety_gateway is not None and scaled > 0:
                action.quantity = scaled
                for leg in action.legs:
                    leg["quantity"] = scaled

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
            elif scaled < requested_lots:
                logger.info(
                    "[RiskEngine] Scaled %s/%s %s %d→%d (bp_cap=%d side_cap=%d)",
                    action.strategy_id, action.symbol, side_type, requested_lots, scaled, bp_cap, side_cap,
                )
                await self.db.logs.write_log(
                    action.strategy_id,
                    f"[MM] Scaled {action.symbol} {side_type.upper()} {requested_lots}→{scaled} lots (bp_cap={bp_cap} side_cap={side_cap})",
                )

            if scaled > 0:
                action.quantity = scaled
                for leg in action.legs:
                    leg["quantity"] = scaled

                available_bp -= scaled * requirement_per_lot
                in_tick_allocated[key] = in_tick_used + scaled

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
