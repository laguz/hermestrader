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


def _broker() -> TradierBroker:
    # Built lazily so `--help` and discovery don't require credentials.
    global _BROKER
    try:
        return _BROKER  # type: ignore[name-defined]
    except NameError:
        pass
    
    load_env_file()
    
    mode = os.environ.get("HERMES_MODE", "paper").lower().strip()
    
    if mode == "paper":
        token = (
            os.environ.get("TRADIER_PAPER_TOKEN")
            or os.environ.get("TRADIER_ACCESS_TOKEN")
            or os.environ.get("TRADIER_API_KEY")
        )
        account = os.environ.get("TRADIER_PAPER_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
        url = os.environ.get("TRADIER_PAPER_BASE_URL", "https://sandbox.tradier.com/v1")
        dry_run = False
    else:
        token = (
            os.environ.get("TRADIER_LIVE_TOKEN")
            or os.environ.get("TRADIER_ACCESS_TOKEN")
            or os.environ.get("TRADIER_API_KEY")
        )
        account = os.environ.get("TRADIER_LIVE_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
        url = os.environ.get("TRADIER_LIVE_BASE_URL", "https://api.tradier.com/v1")
        dry_run = os.environ.get("HERMES_DRY_RUN", "").lower() == "true" or os.environ.get("DRY_RUN", "").lower() == "true"

    # Allow explicit overrides
    if os.environ.get("TRADIER_BASE_URL") or os.environ.get("TRADIER_ENDPOINT"):
        url = os.environ.get("TRADIER_BASE_URL") or os.environ.get("TRADIER_ENDPOINT")
        
    if os.environ.get("HERMES_DRY_RUN"):
        dry_run = os.environ.get("HERMES_DRY_RUN", "").lower() == "true"

    cfg = {
        "tradier_access_token": token,
        "tradier_account_id": account,
        "tradier_base_url": url,
        "dry_run": dry_run
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
