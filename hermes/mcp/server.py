"""
Tradier MCP server.

Exposes the AsyncTradierBroker as a Model Context Protocol server so any MCP client
(Claude Desktop, Cursor, Windsurf) can call the broker asynchronously over stdio or SSE.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from hermes.broker.async_tradier import AsyncTradierBroker

mcp = FastMCP("tradier")


def load_env_file() -> None:
    """Auto-detect and load environment variables from the project's env file
    if credentials are not already present in the environment.
    """
    if "TRADIER_ACCESS_TOKEN" not in os.environ and "TRADIER_API_KEY" not in os.environ:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
        
        env_file = os.environ.get("HERMES_ENV_FILE")
        if env_file:
            if not os.path.isabs(env_file):
                dotenv_path = os.path.join(project_root, env_file)
            else:
                dotenv_path = env_file
        else:
            # Detect based on path name or existence of files
            dotenv_path = os.path.join(project_root, ".env")
            if not os.path.exists(dotenv_path):
                if "live" in project_root.lower() and os.path.exists(os.path.join(project_root, ".env.live")):
                    dotenv_path = os.path.join(project_root, ".env.live")
                elif os.path.exists(os.path.join(project_root, ".env.paper")):
                    dotenv_path = os.path.join(project_root, ".env.paper")
                elif os.path.exists(os.path.join(project_root, ".env.live")):
                    dotenv_path = os.path.join(project_root, ".env.live")

        if os.path.exists(dotenv_path):
            with open(dotenv_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("'\"")
                        if k and v and k not in os.environ:
                            os.environ[k] = v

_BROKERS: Dict[str, AsyncTradierBroker] = {}


async def _broker() -> AsyncTradierBroker:
    """Resolve and cache AsyncTradierBroker per mode to handle dynamic toggling."""
    from hermes.config import settings
    mode = settings.hermes_mode
    if mode not in _BROKERS:
        token, account, url = settings.get_tradier_credentials()
        dry_run = settings.hermes_dry_run if mode == "live" else False
        cfg = {
            "tradier_access_token": token,
            "tradier_account_id": account,
            "tradier_base_url": url,
            "dry_run": dry_run
        }
        _BROKERS[mode] = AsyncTradierBroker(cfg)
    return _BROKERS[mode]


# --------------------------------------------------------------------- Account
@mcp.tool()
async def get_account_balances() -> Dict[str, Any]:
    """Return Tradier account balances (option_buying_power, total_equity, cash, ...)."""
    broker = await _broker()
    return await broker.get_account_balances()


@mcp.tool()
async def get_positions() -> List[Dict[str, Any]]:
    """List currently open positions in the configured Tradier account."""
    broker = await _broker()
    return await broker.get_positions()


@mcp.tool()
async def get_orders() -> List[Dict[str, Any]]:
    """List recent orders (open and historical) for the configured account."""
    broker = await _broker()
    return await broker.get_orders()


@mcp.tool()
async def cancel_order(order_id: str) -> Dict[str, Any]:
    """Cancel a working order by its Tradier order id."""
    broker = await _broker()
    return await broker.cancel_order(order_id)


# --------------------------------------------------------------------- Markets
@mcp.tool()
async def get_quote(symbols: str) -> List[Dict[str, Any]]:
    """Quotes for one or more comma-separated symbols (equity or OCC option)."""
    broker = await _broker()
    return await broker.get_quote(symbols)


@mcp.tool()
async def get_option_expirations(symbol: str) -> List[str]:
    """Return option expirations for `symbol` as YYYY-MM-DD strings."""
    broker = await _broker()
    return await broker.get_option_expirations(symbol)


@mcp.tool()
async def get_option_chain(symbol: str, expiry: str) -> List[Dict[str, Any]]:
    """Full option chain (calls + puts) with greeks for `symbol` on `expiry` (YYYY-MM-DD)."""
    broker = await _broker()
    return await broker.get_option_chains(symbol, expiry)


@mcp.tool()
async def get_delta(option_symbol: str) -> float:
    """Return the delta of a single OCC option symbol."""
    broker = await _broker()
    return await broker.get_delta(option_symbol)


@mcp.tool()
async def get_history(symbol: str, interval: str = "daily",
                 start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
    """Historical OHLCV bars. interval: 'daily' | 'weekly' | 'monthly'."""
    broker = await _broker()
    return await broker.get_history(symbol, interval=interval, start=start, end=end)


@mcp.tool()
async def analyze_symbol(symbol: str, period: str = "6m") -> Dict[str, Any]:
    """Return current price plus put/call entry-point candidates from price-distribution percentiles."""
    broker = await _broker()
    return await broker.analyze_symbol(symbol, period=period)


# ----------------------------------------------------------------------- Orders
@mcp.tool()
async def place_multileg_order(
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
    broker = await _broker()
    return await broker.place_order_from_action(action)


@mcp.tool()
async def place_single_option_order(
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
    broker = await _broker()
    return await broker.place_order_from_action(action)


@mcp.tool()
async def place_equity_order(
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
    broker = await _broker()
    return await broker.place_order_from_action(action)


@mcp.tool()
async def roll_to_next_month(option_symbol: str) -> str:
    """Return the OCC symbol for the next available expiry at the same strike/side."""
    broker = await _broker()
    return await broker.roll_to_next_month(option_symbol)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
