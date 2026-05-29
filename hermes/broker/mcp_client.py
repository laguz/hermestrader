from __future__ import annotations

import os
import json
import logging
import asyncio
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hermes.broker.mcp_client")


class MCPBrokerClient:
    """Model Context Protocol Client wrapper that speaks to the Hermes Tradier MCP server."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.current_date = None
        self._ctx = None
        self._read_write = None
        self._session = None

    async def __aenter__(self) -> MCPBrokerClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None:
            try:
                await self._session.__aexit__(None, None, None)
            except Exception as e:
                logger.debug("Error exiting MCP ClientSession: %s", e)
            self._session = None
        if self._ctx is not None:
            try:
                await self._ctx.__aexit__(None, None, None)
            except Exception as e:
                logger.debug("Error exiting MCP stdio client: %s", e)
            self._ctx = None
            self._read_write = None

    async def _call_mcp(self, tool_name: str, **kwargs) -> Any:
        current_loop = None
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

        if self._session is not None and getattr(self, "_loop", None) != current_loop:
            await self.close()

        if self._session is None:
            self._loop = current_loop
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
            import sys

            python_exe = sys.executable or "python3"
            server_params = StdioServerParameters(
                command=python_exe,
                args=["-m", "hermes.mcp.server"],
                env=os.environ.copy()
            )

            self._ctx = stdio_client(server_params)
            self._read_write = await self._ctx.__aenter__()
            read, write = self._read_write
            self._session = ClientSession(read, write)
            await self._session.__aenter__()
            await self._session.initialize()

        result = await self._session.call_tool(tool_name, arguments=kwargs)

        # Prefer structured content: FastMCP serialises the tool's actual return
        # value here losslessly. This server wraps every return — dicts, lists
        # and scalars alike — under a single "result" key.
        structured = getattr(result, "structuredContent", None)
        if isinstance(structured, dict) and set(structured.keys()) == {"result"}:
            return structured["result"]
        if structured is not None:
            return structured

        # Fallback for transports/tools without structured content. Each content
        # block is one complete value, so decode them individually. The previous
        # implementation concatenated all blocks into one string before parsing,
        # which corrupted list-of-string payloads: option expirations arrive as
        # one block per date ("2026-06-05"), and joining them produced
        # "2026-06-052026-06-08..." that re-parsed into bogus integers — making
        # every expiry fail strptime and look like "no DTE match".
        decoded: List[Any] = []
        for c in result.content:
            text = getattr(c, "text", None)
            if not text:
                continue
            text = text.strip()
            if not text:
                continue
            try:
                decoded.append(json.loads(text))
            except json.JSONDecodeError:
                decoded.append(text)
        if not decoded:
            return None
        if len(decoded) == 1:
            return decoded[0]
        return decoded

    async def get_account_balances(self) -> Dict[str, Any]:
        res = await self._call_mcp("get_account_balances")
        return res if isinstance(res, dict) else {}

    async def get_positions(self) -> List[Dict[str, Any]]:
        res = await self._call_mcp("get_positions")
        return res if isinstance(res, list) else []

    async def get_orders(self) -> List[Dict[str, Any]]:
        res = await self._call_mcp("get_orders")
        return res if isinstance(res, list) else []

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return await self._call_mcp("cancel_order", order_id=order_id)

    async def get_quote(self, symbols: str) -> List[Dict[str, Any]]:
        return await self._call_mcp("get_quote", symbols=symbols)

    async def get_option_expirations(self, symbol: str) -> List[str]:
        return await self._call_mcp("get_option_expirations", symbol=symbol)

    async def get_option_chains(self, symbol: str, expiry: str) -> List[Dict[str, Any]]:
        return await self._call_mcp("get_option_chain", symbol=symbol, expiry=expiry)

    async def get_history(self, symbol: str, interval: str = "daily",
                          start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self._call_mcp("get_history", symbol=symbol, interval=interval, start=start, end=end)

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        return await self._call_mcp("analyze_symbol", symbol=symbol, period=period)

    async def roll_to_next_month(self, option_symbol: str) -> str:
        return await self._call_mcp("roll_to_next_month", option_symbol=option_symbol)

    async def place_order_from_action(self, action) -> Dict[str, Any]:
        legs = action.legs or []
        if not legs:
            raise ValueError("TradeAction has no legs")

        order_class = (action.order_class or "multileg").lower()
        if order_class == "equity":
            return await self._call_mcp(
                "place_equity_order",
                symbol=action.symbol,
                side=action.side,
                quantity=int(action.quantity or 1),
                order_type=action.order_type or "market",
                price=float(action.price) if action.price is not None else None,
                duration=action.duration or "day",
                tag=action.tag,
            )
        elif order_class == "option" and len(legs) == 1:
            leg = legs[0]
            return await self._call_mcp(
                "place_single_option_order",
                symbol=action.symbol,
                option_symbol=leg["option_symbol"],
                side=leg.get("action") or leg.get("side") or "buy_to_open",
                quantity=int(leg.get("quantity", action.quantity or 1)),
                price=float(action.price) if action.price is not None else None,
                order_type=action.order_type or "limit",
                duration=action.duration or "day",
                tag=action.tag,
            )
        else:
            return await self._call_mcp(
                "place_multileg_order",
                symbol=action.symbol,
                legs=[
                    {
                        "option_symbol": l["option_symbol"],
                        "quantity": int(l.get("quantity", action.quantity or 1)),
                        "action": l.get("action") or l.get("side") or "buy_to_open",
                    }
                    for l in legs
                ],
                price=float(action.price) if action.price is not None else 0.0,
                order_type=action.order_type or "credit",
                duration=action.duration or "day",
                tag=action.tag,
            )
