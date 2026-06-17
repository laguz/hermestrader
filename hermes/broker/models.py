from __future__ import annotations

from typing import Any, Dict, List, Optional


class AccountBalances(dict):
    """Normalized, dict-compatible account balances representation."""
    def __init__(self, option_buying_power: float, stock_buying_power: float,
                 total_equity: float, cash: float, account_type: str,
                 margin_buying_power: float = 0.0, **kwargs):
        super().__init__(
            option_buying_power=option_buying_power,
            stock_buying_power=stock_buying_power,
            total_equity=total_equity,
            cash=cash,
            account_type=account_type,
            margin_buying_power=margin_buying_power,
            **kwargs
        )

    @property
    def option_buying_power(self) -> float:
        return self["option_buying_power"]

    @property
    def stock_buying_power(self) -> float:
        return self["stock_buying_power"]

    @property
    def total_equity(self) -> float:
        return self["total_equity"]

    @property
    def cash(self) -> float:
        return self["cash"]

    @property
    def account_type(self) -> str:
        return self["account_type"]

    @property
    def margin_buying_power(self) -> float:
        return self["margin_buying_power"]


class BrokerPosition(dict):
    """Normalized, dict-compatible open position representation."""
    def __init__(self, symbol: str, quantity: float, cost_basis: float,
                 date_acquired: str, **kwargs):
        super().__init__(
            symbol=symbol,
            quantity=quantity,
            cost_basis=cost_basis,
            date_acquired=date_acquired,
            **kwargs
        )

    @property
    def symbol(self) -> str:
        return self["symbol"]

    @property
    def quantity(self) -> float:
        return self["quantity"]

    @property
    def cost_basis(self) -> float:
        return self["cost_basis"]

    @property
    def date_acquired(self) -> str:
        return self["date_acquired"]


class BrokerOrder(dict):
    """Normalized, dict-compatible order representation."""
    def __init__(self, order_id: str, symbol: str, status: str, quantity: int,
                 price: float, side: str, tag: str, legs: Optional[List[Dict[str, Any]]] = None,
                 option_symbol: Optional[str] = None, **kwargs):
        # Support both 'leg' (Tradier-style) and 'legs' keys for compatibility
        leg_data = legs or []
        super().__init__(
            id=order_id,
            order_id=order_id,
            symbol=symbol,
            status=status,
            quantity=quantity,
            price=price,
            side=side,
            tag=tag,
            leg=leg_data,
            legs=leg_data,
            option_symbol=option_symbol,
            **kwargs
        )

    @property
    def order_id(self) -> str:
        return self["order_id"]

    @property
    def symbol(self) -> str:
        return self["symbol"]

    @property
    def status(self) -> str:
        return self["status"]

    @property
    def quantity(self) -> int:
        return self["quantity"]

    @property
    def price(self) -> float:
        return self["price"]

    @property
    def side(self) -> str:
        return self["side"]

    @property
    def tag(self) -> str:
        return self["tag"]

    @property
    def legs(self) -> List[Dict[str, Any]]:
        return self["legs"]

    @property
    def option_symbol(self) -> Optional[str]:
        return self.get("option_symbol")


class OptionChainLeg(dict):
    """Normalized, dict-compatible option contract leg representation."""
    def __init__(self, symbol: str, strike: float, option_type: str,
                 bid: float, ask: float, delta: float,
                 greeks: Optional[Dict[str, Any]] = None, **kwargs):
        greek_data = greeks or {"delta": delta}
        super().__init__(
            symbol=symbol,
            strike=strike,
            option_type=option_type,
            bid=bid,
            ask=ask,
            delta=delta,
            greeks=greek_data,
            **kwargs
        )

    @property
    def symbol(self) -> str:
        return self["symbol"]

    @property
    def strike(self) -> float:
        return self["strike"]

    @property
    def option_type(self) -> str:
        return self["option_type"]

    @property
    def bid(self) -> float:
        return self["bid"]

    @property
    def ask(self) -> float:
        return self["ask"]

    @property
    def delta(self) -> float:
        return self["delta"]

    @property
    def greeks(self) -> Dict[str, Any]:
        return self["greeks"]


class MarketQuote(dict):
    """Normalized, dict-compatible market quote representation."""
    def __init__(self, symbol: str, price: float, bid: float, ask: float,
                 volume: int, timestamp: str, **kwargs):
        super().__init__(
            symbol=symbol,
            price=price,
            bid=bid,
            ask=ask,
            volume=volume,
            timestamp=timestamp,
            **kwargs
        )

    @property
    def symbol(self) -> str:
        return self["symbol"]

    @property
    def price(self) -> float:
        return self["price"]

    @property
    def bid(self) -> float:
        return self["bid"]

    @property
    def ask(self) -> float:
        return self["ask"]

    @property
    def volume(self) -> int:
        return self["volume"]

    @property
    def timestamp(self) -> str:
        return self["timestamp"]


class OrderPlacementResult(dict):
    """Normalized order submission result."""
    def __init__(self, order_id: str, status: str, raw_response: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(
            order_id=order_id,
            status=status,
            order={"id": order_id, "status": status},
            raw_response=raw_response or {},
            **kwargs
        )

    @property
    def order_id(self) -> str:
        return self["order_id"]

    @property
    def status(self) -> str:
        return self["status"]
