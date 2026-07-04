from __future__ import annotations

import os
import json
import logging
import asyncio
from typing import Any, Dict, List, Optional
from hermes.broker.base import AbstractBroker
from hermes.broker.models import (
    AccountBalances,
    BrokerPosition,
    BrokerOrder,
    OptionChainLeg,
    MarketQuote,
    OrderPlacementResult,
)

logger = logging.getLogger("hermes.broker.mcp_client")


class MCPBrokerClient(AbstractBroker):
    """Model Context Protocol Client wrapper that speaks to the Hermes Tradier MCP server."""

    # The stdio round-trip to the MCP/Tradier sandbox has no protocol-level
    # timeout of its own — every call funnels through _call_mcp, so bounding
    # it here (rather than at each call site) protects the whole pipeline
    # (sync_positions, order placement, ML history sync, ...) from a single
    # stalled response wedging the tick loop forever.
    _CALL_TIMEOUT_S = 30.0
    _CLOSE_TIMEOUT_S = 5.0

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.current_date = None
        self._ctx = None
        self._read_write = None
        self._session = None

    @property
    def dry_run(self) -> bool:
        return self.config.get("dry_run", True)

    async def __aenter__(self) -> MCPBrokerClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None:
            try:
                await asyncio.wait_for(
                    self._session.__aexit__(None, None, None),
                    timeout=self._CLOSE_TIMEOUT_S,
                )
            except Exception as e:
                logger.debug("Error exiting MCP ClientSession: %s", e)
            self._session = None
        if self._ctx is not None:
            try:
                await asyncio.wait_for(
                    self._ctx.__aexit__(None, None, None),
                    timeout=self._CLOSE_TIMEOUT_S,
                )
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

        try:
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
                self._read_write = await asyncio.wait_for(
                    self._ctx.__aenter__(), timeout=self._CALL_TIMEOUT_S)
                read, write = self._read_write
                self._session = ClientSession(read, write)
                await self._session.__aenter__()
                await asyncio.wait_for(
                    self._session.initialize(), timeout=self._CALL_TIMEOUT_S)

            result = await asyncio.wait_for(
                self._session.call_tool(tool_name, arguments=kwargs),
                timeout=self._CALL_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            # The stdio subprocess is wedged (stalled sandbox response, or a
            # handshake that never completes). Reset the session so the next
            # call gets a fresh process instead of retrying the same dead
            # pipe and hanging again immediately; surface this like any other
            # broker failure instead of hanging the caller forever.
            logger.error(
                "[MCP] %s timed out after %ss — resetting session",
                tool_name, self._CALL_TIMEOUT_S,
            )
            await self.close()
            raise

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

    async def get_account_balances(self) -> AccountBalances:
        res = await self._call_mcp("get_account_balances")
        if not isinstance(res, dict):
            res = {}
        return AccountBalances(
            option_buying_power=float(res.get("option_buying_power") or 0.0),
            stock_buying_power=float(res.get("stock_buying_power") or 0.0),
            total_equity=float(res.get("total_equity") or 0.0),
            cash=float(res.get("cash") or 0.0),
            account_type=str(res.get("account_type") or "margin"),
            margin_buying_power=float(res.get("margin_buying_power") or 0.0),
            **{k: v for k, v in res.items() if k not in (
                "option_buying_power", "stock_buying_power", "total_equity", "cash", "account_type", "margin_buying_power"
            )}
        )

    async def get_positions(self) -> List[BrokerPosition]:
        res = await self._call_mcp("get_positions")
        if not isinstance(res, list):
            return []
        return [
            BrokerPosition(
                symbol=str(pos.get("symbol") or ""),
                quantity=float(pos.get("quantity") or 0.0),
                cost_basis=float(pos.get("cost_basis") or 0.0),
                date_acquired=str(pos.get("date_acquired") or ""),
                **{k: v for k, v in pos.items() if k not in (
                    "symbol", "quantity", "cost_basis", "date_acquired"
                )}
            )
            for pos in res
        ]

    async def get_orders(self) -> List[BrokerOrder]:
        res = await self._call_mcp("get_orders")
        if not isinstance(res, list):
            return []
        return [
            BrokerOrder(
                order_id=str(o.get("id") or o.get("order_id") or ""),
                symbol=str(o.get("symbol") or ""),
                status=str(o.get("status") or ""),
                quantity=int(o.get("quantity") or 0),
                price=float(o.get("price") or 0.0),
                side=str(o.get("side") or ""),
                tag=str(o.get("tag") or ""),
                legs=o.get("leg") or o.get("legs") or [],
                option_symbol=o.get("option_symbol"),
                **{k: v for k, v in o.items() if k not in (
                    "order_id", "symbol", "status", "quantity", "price", "side",
                    "tag", "legs", "option_symbol", "id", "leg"
                )}
            )
            for o in res
        ]

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return await self._call_mcp("cancel_order", order_id=order_id)

    async def get_quote(self, symbols: str) -> List[MarketQuote]:
        res = await self._call_mcp("get_quote", symbols=symbols)
        if not isinstance(res, list):
            return []
        return [
            MarketQuote(
                symbol=str(q.get("symbol") or ""),
                price=float(q.get("price") or q.get("last") or 0.0),
                bid=float(q.get("bid") or 0.0),
                ask=float(q.get("ask") or 0.0),
                volume=int(q.get("volume") or 0),
                timestamp=str(q.get("timestamp") or ""),
                **{k: v for k, v in q.items() if k not in (
                    "symbol", "price", "bid", "ask", "volume", "timestamp"
                )}
            )
            for q in res
        ]

    async def get_delta(self, option_symbol: str) -> float:
        quotes = await self.get_quote(option_symbol)
        if not quotes:
            return 0.0
        greeks = (quotes[0].get("greeks") or {}) if quotes else {}
        return float(greeks.get("delta", 0.0) or 0.0)

    async def get_option_expirations(self, symbol: str) -> List[str]:
        return await self._call_mcp("get_option_expirations", symbol=symbol)

    async def get_option_chains(self, symbol: str, expiry: str) -> List[OptionChainLeg]:
        res = await self._call_mcp("get_option_chain", symbol=symbol, expiry=expiry)
        if not isinstance(res, list):
            return []
        return [
            OptionChainLeg(
                symbol=str(leg.get("symbol") or ""),
                strike=float(leg.get("strike") or 0.0),
                option_type=str(leg.get("option_type") or leg.get("type") or "put"),
                bid=float(leg.get("bid") or 0.0),
                ask=float(leg.get("ask") or 0.0),
                delta=float(leg.get("delta") or (leg.get("greeks") or {}).get("delta") or 0.0),
                greeks=leg.get("greeks"),
                **{k: v for k, v in leg.items() if k not in (
                    "symbol", "strike", "option_type", "bid", "ask", "delta", "greeks"
                )}
            )
            for leg in res
        ]

    async def get_history(self, symbol: str, interval: str = "daily",
                          start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self._call_mcp("get_history", symbol=symbol, interval=interval, start=start, end=end)

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        return await self._call_mcp("analyze_symbol", symbol=symbol, period=period)

    async def roll_to_next_month(self, option_symbol: str) -> str:
        return await self._call_mcp("roll_to_next_month", option_symbol=option_symbol)

    async def place_order_from_action(self, action) -> OrderPlacementResult:
        legs = action.legs or []
        if not legs:
            raise ValueError("TradeAction has no legs")

        order_class = (action.order_class or "multileg").lower()
        if order_class == "equity":
            res = await self._call_mcp(
                "place_equity_order",
                symbol=action.symbol,
                side=action.side,
                quantity=int(action.quantity) if action.quantity is not None else 1,
                order_type=action.order_type or "market",
                price=float(action.price) if action.price is not None else None,
                duration=action.duration or "day",
                tag=action.tag,
            )
        elif order_class == "option" and len(legs) == 1:
            leg = legs[0]
            res = await self._call_mcp(
                "place_single_option_order",
                symbol=action.symbol,
                option_symbol=leg["option_symbol"],
                side=leg.get("action") or leg.get("side") or "buy_to_open",
                quantity=int(leg.get("quantity") if leg.get("quantity") is not None else (action.quantity if action.quantity is not None else 1)),
                price=float(action.price) if action.price is not None else None,
                order_type=action.order_type or "limit",
                duration=action.duration or "day",
                tag=action.tag,
            )
        else:
            res = await self._call_mcp(
                "place_multileg_order",
                symbol=action.symbol,
                legs=[
                    {
                        "option_symbol": leg["option_symbol"],
                        "quantity": int(leg.get("quantity") if leg.get("quantity") is not None else (action.quantity if action.quantity is not None else 1)),
                        "action": leg.get("action") or leg.get("side") or "buy_to_open",
                    }
                    for leg in legs
                ],
                price=float(action.price) if action.price is not None else 0.0,
                order_type=action.order_type or "credit",
                duration=action.duration or "day",
                tag=action.tag,
            )

        return OrderPlacementResult.from_broker_response(res)
