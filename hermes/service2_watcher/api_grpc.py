from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import grpc

from hermes.protos import broker_pb2, broker_pb2_grpc

logger = logging.getLogger("hermes.service2_watcher.api_grpc")


class BrokerServiceServicer(broker_pb2_grpc.BrokerServiceServicer):
    """gRPC service provider hosted by the Watcher (C2) to orchestrate agent requests."""

    def __init__(self):
        # Use TradierBroker if credentials are set, otherwise fallback to mock
        token = os.environ.get("TRADIER_ACCESS_TOKEN")
        account_id = os.environ.get("TRADIER_ACCOUNT_ID")
        
        if token and account_id:
            try:
                from hermes.broker.tradier import TradierBroker
                self.broker = TradierBroker()
                logger.info("[gRPC] Initialized TradierBroker backend for order execution")
            except Exception as exc:
                logger.error("[gRPC] Failed to initialize TradierBroker, using MockAsyncTradierBroker: %s", exc)
                from hermes.broker.mock_engine import MockAsyncTradierBroker
                self.broker = MockAsyncTradierBroker()
        else:
            from hermes.broker.mock_engine import MockAsyncTradierBroker
            self.broker = MockAsyncTradierBroker()
            logger.info("[gRPC] Using MockAsyncTradierBroker backend (no credentials detected)")

    async def StreamCommands(self, request, context):
        """Stream system commands (sync settings, approvals, ML triggers) to the agent."""
        from hermes.messaging.bus import MessageBus
        bus = MessageBus()
        await bus.connect()

        queue = asyncio.Queue()

        async def bus_callback(data: Dict[str, Any]):
            await queue.put(data)

        await bus.subscribe("agent_commands", bus_callback)
        logger.info("[gRPC] Client subscribed to StreamCommands")

        try:
            while True:
                data = await queue.get()
                yield broker_pb2.SystemCommand(
                    command_type=data.get("type", "UNKNOWN"),
                    payload_json=json.dumps(data.get("payload", {})),
                    timestamp=datetime.utcnow().isoformat()
                )
        except asyncio.CancelledError:
            logger.info("[gRPC] Client unsubscribed from StreamCommands (cancelled)")
        finally:
            await bus.unsubscribe("agent_commands", bus_callback)
            await bus.disconnect()

    async def SubmitOrder(self, request, context):
        """Submit a multileg or single order to the broker and return the execution report."""
        from hermes.service1_agent.trade_action import TradeAction
        
        logger.info("[gRPC] Received SubmitOrder request for %s (strategy=%s)", request.symbol, request.strategy_id)
        
        legs = []
        for leg in request.legs:
            legs.append({
                "option_symbol": leg.option_symbol,
                "quantity": leg.quantity,
                "side": leg.side,
                "action": leg.action
            })

        action = TradeAction(
            strategy_id=request.strategy_id,
            symbol=request.symbol,
            order_class=request.order_class,
            legs=legs,
            price=request.price if request.price > 0 else None,
            side=request.side,
            quantity=request.quantity,
            duration=request.duration,
            width=request.width if request.width > 0 else None,
            tag=request.tag
        )

        try:
            res = await self.broker.place_order_from_action(action)
            return broker_pb2.ExecutionReport(
                order_id=str(res.get("order_id", "")),
                status=res.get("status", "SUBMITTED"),
                filled_quantity=res.get("filled_quantity", 0),
                avg_fill_price=res.get("avg_fill_price", 0.0),
                transaction_time=datetime.utcnow().isoformat(),
                raw_response_json=json.dumps(res)
            )
        except Exception as exc:
            logger.exception("[gRPC] SubmitOrder failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.ExecutionReport()

    async def QueryPositions(self, request, context):
        """Stream active broker positions to the agent."""
        logger.info("[gRPC] Received QueryPositions request")
        try:
            positions = await self.broker.get_positions() or []
            for pos in positions:
                yield broker_pb2.BrokerPosition(
                    symbol=pos.get("symbol", ""),
                    quantity=float(pos.get("quantity", 0.0)),
                    cost_basis=float(pos.get("cost_basis", 0.0)),
                    date_acquired=pos.get("date_acquired", "")
                )
        except Exception as exc:
            logger.exception("[gRPC] QueryPositions failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))

    async def StreamQuotes(self, request, context):
        """Mock stream market data quotes to the agent."""
        logger.info("[gRPC] Received StreamQuotes request")
        try:
            while True:
                yield broker_pb2.MarketQuote(
                    symbol="SPY",
                    price=500.0,
                    bid=499.9,
                    ask=500.1,
                    volume=1000000,
                    timestamp=datetime.utcnow().isoformat()
                )
                await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            logger.info("[gRPC] Client unsubscribed from StreamQuotes (cancelled)")

    async def GetAccountBalances(self, request, context):
        """Fetch option and stock buying power, total equity, cash, and account type."""
        try:
            res = await self.broker.get_account_balances() or {}
            return broker_pb2.BalancesResponse(
                option_buying_power=float(res.get("option_buying_power") or 0.0),
                stock_buying_power=float(res.get("stock_buying_power") or 0.0),
                total_equity=float(res.get("total_equity") or 0.0),
                cash=float(res.get("cash") or 0.0),
                account_type=str(res.get("account_type") or ""),
                margin_buying_power=float(res.get("margin_buying_power") or 0.0)
            )
        except Exception as exc:
            logger.exception("[gRPC] GetAccountBalances failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.BalancesResponse()

    async def GetOrders(self, request, context):
        """Fetch list of working and completed orders."""
        try:
            res = await self.broker.get_orders() or []
            return broker_pb2.JSONResponse(data_json=json.dumps(res))
        except Exception as exc:
            logger.exception("[gRPC] GetOrders failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.JSONResponse()

    async def CancelOrder(self, request, context):
        """Cancel a pending/working order."""
        try:
            res = await self.broker.cancel_order(request.order_id)
            return broker_pb2.JSONResponse(data_json=json.dumps(res))
        except Exception as exc:
            logger.exception("[gRPC] CancelOrder failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.JSONResponse()

    async def GetOptionExpirations(self, request, context):
        """Fetch available options expiration dates for a symbol."""
        try:
            res = await self.broker.get_option_expirations(request.symbol) or []
            return broker_pb2.ExpirationsResponse(expirations=res)
        except Exception as exc:
            logger.exception("[gRPC] GetOptionExpirations failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.ExpirationsResponse()

    async def GetOptionChains(self, request, context):
        """Fetch the options chain for a symbol at a specific expiration."""
        try:
            res = await self.broker.get_option_chains(request.symbol, request.expiry) or []
            return broker_pb2.JSONResponse(data_json=json.dumps(res))
        except Exception as exc:
            logger.exception("[gRPC] GetOptionChains failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.JSONResponse()

    async def GetQuote(self, request, context):
        """Fetch market quotes for one or more comma-separated symbols."""
        try:
            res = await self.broker.get_quote(request.symbols) or []
            return broker_pb2.JSONResponse(data_json=json.dumps(res))
        except Exception as exc:
            logger.exception("[gRPC] GetQuote failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.JSONResponse()

    async def GetDelta(self, request, context):
        """Fetch the delta greek value for an option symbol."""
        try:
            res = await self.broker.get_delta(request.option_symbol)
            return broker_pb2.DeltaResponse(delta=float(res or 0.0))
        except Exception as exc:
            logger.exception("[gRPC] GetDelta failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.DeltaResponse()

    async def GetHistory(self, request, context):
        """Fetch historical price bars for an equity/symbol."""
        try:
            res = await self.broker.get_history(
                request.symbol,
                interval=request.interval or "daily",
                start=request.start or None,
                end=request.end or None
            ) or []
            return broker_pb2.JSONResponse(data_json=json.dumps(res))
        except Exception as exc:
            logger.exception("[gRPC] GetHistory failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.JSONResponse()

    async def AnalyzeSymbol(self, request, context):
        """Run technical analysis on a symbol."""
        try:
            res = await self.broker.analyze_symbol(request.symbol, period=request.period or "6m") or {}
            return broker_pb2.JSONResponse(data_json=json.dumps(res))
        except Exception as exc:
            logger.exception("[gRPC] AnalyzeSymbol failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.JSONResponse()

    async def RollToNextMonth(self, request, context):
        """Helper to find the next monthly expiration and construct the rolled OCC option symbol."""
        try:
            res = await self.broker.roll_to_next_month(request.option_symbol) or ""
            return broker_pb2.RollResponse(next_option_symbol=res)
        except Exception as exc:
            logger.exception("[gRPC] RollToNextMonth failed: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return broker_pb2.RollResponse()


async def start_grpc_server(host: str = "0.0.0.0", port: int = 50051) -> grpc.aio.Server:
    """Initialize and start the gRPC asynchronous server."""
    server = grpc.aio.server()
    broker_pb2_grpc.add_BrokerServiceServicer_to_server(BrokerServiceServicer(), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    logger.info("gRPC server started on %s:%d", host, port)
    return server
