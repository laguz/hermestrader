"""
Tradier MCP server.

Exposes the TradierBroker as a Model Context Protocol server so any MCP client
(Claude Desktop, Cowork, custom agents) can call the broker over stdio.

Run:
    python -m hermes.mcp.server

Required env: TRADIER_ACCESS_TOKEN, TRADIER_ACCOUNT_ID
Optional env: TRADIER_BASE_URL (default https://api.tradier.com/v1),
              HERMES_DRY_RUN ("true" → orders use Tradier preview mode)

Install dep: pip install "mcp[cli]" requests
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from hermes.broker.tradier import TradierBroker

mcp = FastMCP("tradier")


def _broker() -> TradierBroker:
    # Built lazily so `--help` and discovery don't require credentials.
    global _BROKER
    try:
        return _BROKER  # type: ignore[name-defined]
    except NameError:
        pass
    
    dry_run = os.environ.get("HERMES_DRY_RUN", "").lower() == "true"
    # Honor TRADIER_ENDPOINT from .env if TRADIER_BASE_URL is missing
    base_url = os.environ.get("TRADIER_BASE_URL") or os.environ.get("TRADIER_ENDPOINT")
    
    # If dry_run is on and no URL provided, default to sandbox
    if dry_run and not base_url:
        base_url = "https://sandbox.tradier.com/v1"

    cfg = {
        "dry_run": dry_run,
        "tradier_base_url": base_url
    }
    _BROKER = TradierBroker(cfg)  # type: ignore[name-defined]
    return _BROKER  # type: ignore[name-defined]


# --------------------------------------------------------------------- Account
@mcp.tool()
def get_account_balances() -> Dict[str, Any]:
    """Return Tradier account balances (option_buying_power, total_equity, cash, ...)."""
    return _broker().get_account_balances()


@mcp.tool()
def get_positions() -> List[Dict[str, Any]]:
    """List currently open positions in the configured Tradier account."""
    return _broker().get_positions()


@mcp.tool()
def get_orders() -> List[Dict[str, Any]]:
    """List recent orders (open and historical) for the configured account."""
    return _broker().get_orders()


@mcp.tool()
def cancel_order(order_id: str) -> Dict[str, Any]:
    """Cancel a working order by its Tradier order id."""
    return _broker().cancel_order(order_id)


# --------------------------------------------------------------------- Markets
@mcp.tool()
def get_quote(symbols: str) -> List[Dict[str, Any]]:
    """Quotes for one or more comma-separated symbols (equity or OCC option)."""
    return _broker().get_quote(symbols)


@mcp.tool()
def get_option_expirations(symbol: str) -> List[str]:
    """Return option expirations for `symbol` as YYYY-MM-DD strings."""
    return _broker().get_option_expirations(symbol)


@mcp.tool()
def get_option_chain(symbol: str, expiry: str) -> List[Dict[str, Any]]:
    """Full option chain (calls + puts) with greeks for `symbol` on `expiry` (YYYY-MM-DD)."""
    return _broker().get_option_chains(symbol, expiry)


@mcp.tool()
def get_delta(option_symbol: str) -> float:
    """Return the delta of a single OCC option symbol."""
    return _broker().get_delta(option_symbol)


@mcp.tool()
def get_history(symbol: str, interval: str = "daily",
                start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
    """Historical OHLCV bars. interval: 'daily' | 'weekly' | 'monthly'."""
    return _broker().get_history(symbol, interval=interval, start=start, end=end)


@mcp.tool()
def analyze_symbol(symbol: str, period: str = "6m") -> Dict[str, Any]:
    """Return current price plus put/call entry-point candidates from price-distribution percentiles."""
    return _broker().analyze_symbol(symbol, period=period)


# ----------------------------------------------------------------------- Orders
@mcp.tool()
def place_multileg_order(
    symbol: str,
    legs: List[Dict[str, Any]],
    price: float,
    order_type: str = "credit",
    duration: str = "day",
    tag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Place a multi-leg options order.

    Each leg must include `option_symbol`, `quantity`, and either `action`
    (buy_to_open / sell_to_open / buy_to_close / sell_to_close) or `side`
    (buy/sell — defaults to opening).

    Honors HERMES_DRY_RUN=true → routed through Tradier's preview endpoint.
    """
    from hermes.service1_agent.core import TradeAction
    action = TradeAction(
        strategy_id="mcp",
        symbol=symbol,
        order_class="multileg",
        legs=legs,
        price=price,
        side="sell" if order_type.lower() == "credit" else "buy",
        order_type=order_type,
        duration=duration,
        tag=tag,
    )
    return _broker().place_order_from_action(action)


@mcp.tool()
def place_single_option_order(
    symbol: str,
    option_symbol: str,
    side: str,
    quantity: int,
    price: Optional[float] = None,
    order_type: str = "limit",
    duration: str = "day",
    tag: Optional[str] = None,
) -> Dict[str, Any]:
    """Place a single-leg options order. `side` is buy_to_open / sell_to_open / buy_to_close / sell_to_close."""
    from hermes.service1_agent.core import TradeAction
    action = TradeAction(
        strategy_id="mcp",
        symbol=symbol,
        order_class="option",
        legs=[{"option_symbol": option_symbol, "action": side, "quantity": quantity}],
        price=price,
        side="buy" if "buy" in side else "sell",
        quantity=quantity,
        order_type=order_type,
        duration=duration,
        tag=tag,
    )
    return _broker().place_order_from_action(action)


@mcp.tool()
def place_equity_order(
    symbol: str,
    side: str,
    quantity: int,
    order_type: str = "market",
    price: Optional[float] = None,
    duration: str = "day",
    tag: Optional[str] = None,
) -> Dict[str, Any]:
    """Place an equity order. `side` is buy / sell / sell_short / buy_to_cover."""
    from hermes.service1_agent.core import TradeAction
    action = TradeAction(
        strategy_id="mcp",
        symbol=symbol,
        order_class="equity",
        legs=[{"side": side, "quantity": quantity}],
        price=price,
        side=side,
        quantity=quantity,
        order_type=order_type,
        duration=duration,
        tag=tag,
    )
    return _broker().place_order_from_action(action)


@mcp.tool()
def roll_to_next_month(option_symbol: str) -> str:
    """Return the OCC symbol for the next available expiry at the same strike/side."""
    return _broker().roll_to_next_month(option_symbol)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
