from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from hermes.common import is_close_tag
from hermes.service1_agent.trade_action import TradeAction

logger = logging.getLogger("hermes.portfolio.safety_gateway")


class SafetyValidationError(Exception):
    """Exception raised when a TradeAction fails safety validation."""
    pass


@dataclass
class SafetyVerificationReport:
    decision: str  # "APPROVED" | "REJECTED"
    metrics: Dict[str, Any]
    violations: List[str]
    timestamp: str


class SafetyGateway:
    """
    Mathematically verifiable safety gateway to enforce risk limits, 
    concentration boundaries, and side-aware locks before sending orders to the broker.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        # Max risk allowed as a ratio of option buying power (default: 5%)
        self.max_risk_bp_ratio = float(self.config.get("safety_max_risk_bp_ratio", 0.05))
        # Max symbol exposure allowed as a ratio of option buying power (default: 20%)
        self.max_symbol_exposure_ratio = float(self.config.get("safety_max_symbol_exposure_ratio", 0.20))
        # Max open trades per underlying symbol (default: 3)
        self.max_symbol_trades = int(self.config.get("safety_max_symbol_trades", 3))
        # Enable side locks (default: True)
        self.side_lock_enabled = bool(self.config.get("safety_side_lock_enabled", True))

    def validate_action(
        self, 
        action: TradeAction, 
        balances: Dict[str, Any], 
        open_trades: List[Dict[str, Any]]
    ) -> SafetyVerificationReport:
        """
        Validate a proposed TradeAction against the safety rules.
        """
        violations = []
        metrics = {}
        timestamp = datetime.utcnow().isoformat()

        # 1. Bypassing closing/risk-reduction trades
        is_closing = False
        if action.legs:
            is_closing = all(
                "to_close" in (leg.get("side") or leg.get("action") or "").lower()
                for leg in action.legs
            )
        if is_close_tag(action.tag):
            is_closing = True

        if is_closing:
            logger.debug("[SAFETY] Action %s is a closing trade, bypassing checks", action.tag)
            return SafetyVerificationReport(
                decision="APPROVED",
                metrics={"is_closing": True},
                violations=[],
                timestamp=timestamp
            )

        # 2. Extract balances info
        obp = float(balances.get("option_buying_power") or 0.0)
        metrics["option_buying_power"] = obp

        # 3. Calculate order max risk
        # Risk for credit spread: (width - entry_credit) * qty * 100
        risk = 0.0
        if action.order_class == "multileg":
            width = float(action.width if action.width is not None else 0.0)
            credit = float(action.price if action.price is not None else 0.0)
            qty = int(action.quantity) if action.quantity is not None else 1
            if credit > width:
                credit = width
            risk = (width - credit) * qty * 100.0
        elif action.order_class == "option":
            qty = int(action.quantity) if action.quantity is not None else 1
            price = float(action.price if action.price is not None else 0.0)
            if action.side == "buy" or (action.legs and "to_open" in (action.legs[0].get("side") or "").lower()):
                risk = price * qty * 100.0
            else:
                risk = 1000.0 * qty
        else:
            qty = int(action.quantity) if action.quantity is not None else 1
            price = float(action.price if action.price is not None else 0.0)
            risk = price * qty

        metrics["calculated_risk"] = risk

        # Rule 1: Risk-to-Buying-Power ratio check
        if obp > 0:
            risk_ratio = risk / obp
            metrics["risk_ratio"] = risk_ratio
            max_allowed_risk = obp * self.max_risk_bp_ratio
            if risk > max_allowed_risk:
                violations.append(
                    f"Max risk ${risk:.2f} exceeds safety limit of {self.max_risk_bp_ratio*100:.1f}% "
                    f"of Option Buying Power (${max_allowed_risk:.2f})"
                )
        else:
            violations.append("Option Buying Power is 0 or negative; blocking entry order.")

        # Rule 2: Symbol Concentration Cap
        symbol = (action.symbol or "").upper().strip()
        metrics["underlying"] = symbol
        
        symbol_open_trades = [t for t in open_trades if (t.get("symbol") or "").upper().strip() == symbol]
        symbol_trade_count = len(symbol_open_trades)
        metrics["symbol_existing_trades"] = symbol_trade_count

        if symbol_trade_count >= self.max_symbol_trades:
            violations.append(
                f"Symbol {symbol} has {symbol_trade_count} open trades, "
                f"violating concentration count limit of {self.max_symbol_trades}"
            )

        existing_symbol_risk = 0.0
        for t in symbol_open_trades:
            t_width = float(t.get("width") if t.get("width") is not None else 0.0)
            t_credit = float(t.get("entry_credit") if t.get("entry_credit") is not None else 0.0)
            t_lots = int(t.get("lots") if t.get("lots") is not None else 1)
            existing_symbol_risk += max(0.0, (t_width - t_credit)) * t_lots * 100.0

        total_symbol_risk = existing_symbol_risk + risk
        metrics["total_symbol_risk"] = total_symbol_risk

        if obp > 0:
            symbol_exposure_ratio = total_symbol_risk / obp
            metrics["symbol_exposure_ratio"] = symbol_exposure_ratio
            max_allowed_exposure = obp * self.max_symbol_exposure_ratio
            if total_symbol_risk > max_allowed_exposure:
                violations.append(
                    f"Total exposure on symbol {symbol} (${total_symbol_risk:.2f}) "
                    f"exceeds safety limit of {self.max_symbol_exposure_ratio*100:.1f}% "
                    f"of Option Buying Power (${max_allowed_exposure:.2f})"
                )

        # Rule 3: Side-Aware Locks
        if self.side_lock_enabled and symbol_open_trades:
            for t in symbol_open_trades:
                t_side_type = (t.get("side_type") or "").lower()
                
                proposed_type = None
                if action.legs:
                    for leg in action.legs:
                        opt_sym = leg.get("option_symbol") or ""
                        if len(opt_sym) > 12:
                            match = re.search(r'[0-9]{6}([PC])[0-9]{8}', opt_sym)
                            if match:
                                char = match.group(1).lower()
                                proposed_type = "put" if char == "p" else "call"
                                break

                if proposed_type and t_side_type == proposed_type:
                    violations.append(
                        f"Side lock violation: An open {t_side_type} position already exists "
                        f"on symbol {symbol}. Duplicate entry on the same side is blocked."
                    )
                    break

        decision = "REJECTED" if violations else "APPROVED"
        return SafetyVerificationReport(
            decision=decision,
            metrics=metrics,
            violations=violations,
            timestamp=timestamp
        )
