from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import grpc

from hermes.broker.base import AbstractBroker
from hermes.broker.models import (
    AccountBalances,
    BrokerPosition,
    BrokerOrder,
    OptionChainLeg,
    MarketQuote,
    OrderPlacementResult,
)
from hermes.protos import broker_pb2, broker_pb2_grpc

logger = logging.getLogger("hermes.broker.grpc_client")


class GRPCBrokerClient(AbstractBroker):
    """Broker client that routes AbstractBroker calls over gRPC to the Watcher's gRPC server."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.target = self.config.get("grpc_target", "localhost:50051")
        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[broker_pb2_grpc.BrokerServiceStub] = None

    def _get_stub(self) -> broker_pb2_grpc.BrokerServiceStub:
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(self.target)
            self._stub = broker_pb2_grpc.BrokerServiceStub(self._channel)
        return self._stub

    async def get_positions(self) -> List[BrokerPosition]:
        stub = self._get_stub()
        positions = []
        try:
            response_stream = stub.QueryPositions(broker_pb2.Empty())
            async for pos in response_stream:
                positions.append(
                    BrokerPosition(
                        symbol=pos.symbol,
                        quantity=float(pos.quantity),
                        cost_basis=float(pos.cost_basis),
                        date_acquired=pos.date_acquired or "",
                    )
                )
        except Exception as exc:
            logger.error("[gRPC Client] Failed to query positions over gRPC: %s", exc)
        return positions

    async def get_account_balances(self) -> AccountBalances:
        stub = self._get_stub()
        try:
            res = await stub.GetAccountBalances(broker_pb2.Empty())
            return AccountBalances(
                option_buying_power=float(res.option_buying_power),
                stock_buying_power=float(res.stock_buying_power),
                total_equity=float(res.total_equity),
                cash=float(res.cash),
                account_type=str(res.account_type),
                margin_buying_power=float(res.margin_buying_power),
            )
        except Exception as exc:
            logger.error("[gRPC Client] Failed to get account balances: %s", exc)
            return AccountBalances(0.0, 0.0, 0.0, 0.0, "margin")

    async def get_orders(self) -> List[BrokerOrder]:
        stub = self._get_stub()
        try:
            res = await stub.GetOrders(broker_pb2.Empty())
            if res.data_json:
                data = json.loads(res.data_json)
                if isinstance(data, list):
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
                        for o in data
                    ]
        except Exception as exc:
            logger.error("[gRPC Client] Failed to get orders: %s", exc)
        return []

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        stub = self._get_stub()
        try:
            res = await stub.CancelOrder(broker_pb2.CancelOrderRequest(order_id=order_id))
            if res.data_json:
                return json.loads(res.data_json)
        except Exception as exc:
            logger.error("[gRPC Client] Failed to cancel order %s: %s", order_id, exc)
        return {}

    async def get_option_expirations(self, symbol: str) -> List[str]:
        stub = self._get_stub()
        try:
            res = await stub.GetOptionExpirations(broker_pb2.ExpirationsRequest(symbol=symbol))
            return list(res.expirations)
        except Exception as exc:
            logger.error("[gRPC Client] Failed to get option expirations for %s: %s", symbol, exc)
            return []

    async def get_option_chains(self, symbol: str, expiry: str) -> List[OptionChainLeg]:
        stub = self._get_stub()
        try:
            res = await stub.GetOptionChains(broker_pb2.OptionChainsRequest(symbol=symbol, expiry=expiry))
            if res.data_json:
                data = json.loads(res.data_json)
                if isinstance(data, list):
                    return [
                        OptionChainLeg(
                            symbol=str(leg.get("symbol") or ""),
                            strike=float(leg.get("strike") or 0.0),
                            option_type=str(leg.get("option_type") or "put"),
                            bid=float(leg.get("bid") or 0.0),
                            ask=float(leg.get("ask") or 0.0),
                            delta=float(leg.get("delta") or (leg.get("greeks") or {}).get("delta") or 0.0),
                            greeks=leg.get("greeks"),
                            **{k: v for k, v in leg.items() if k not in (
                                "symbol", "strike", "option_type", "bid", "ask", "delta", "greeks"
                            )}
                        )
                        for leg in data
                    ]
        except Exception as exc:
            logger.error("[gRPC Client] Failed to get option chains for %s on %s: %s", symbol, expiry, exc)
        return []

    async def get_quote(self, symbols: str) -> List[MarketQuote]:
        stub = self._get_stub()
        try:
            res = await stub.GetQuote(broker_pb2.QuoteRequest(symbols=symbols))
            if res.data_json:
                data = json.loads(res.data_json)
                if isinstance(data, list):
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
                        for q in data
                    ]
        except Exception as exc:
            logger.error("[gRPC Client] Failed to get quotes for %s: %s", symbols, exc)
        return []

    async def get_delta(self, option_symbol: str) -> float:
        stub = self._get_stub()
        try:
            res = await stub.GetDelta(broker_pb2.DeltaRequest(option_symbol=option_symbol))
            return res.delta
        except Exception as exc:
            logger.error("[gRPC Client] Failed to get delta for %s: %s", option_symbol, exc)
            return 0.0

    async def get_history(
        self, symbol: str, *, interval: str = "daily",
        start: Optional[str] = None, end: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        stub = self._get_stub()
        try:
            res = await stub.GetHistory(broker_pb2.HistoryRequest(
                symbol=symbol,
                interval=interval,
                start=start or "",
                end=end or ""
            ))
            if res.data_json:
                return json.loads(res.data_json)
        except Exception as exc:
            logger.error("[gRPC Client] Failed to get history for %s: %s", symbol, exc)
            return []

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        stub = self._get_stub()
        try:
            res = await stub.AnalyzeSymbol(broker_pb2.AnalyzeRequest(symbol=symbol, period=period))
            if res.data_json:
                return json.loads(res.data_json)
        except Exception as exc:
            logger.error("[gRPC Client] Failed to analyze symbol %s: %s", symbol, exc)
            return {}

    async def roll_to_next_month(self, option_symbol: str) -> str:
        stub = self._get_stub()
        try:
            res = await stub.RollToNextMonth(broker_pb2.RollRequest(option_symbol=option_symbol))
            return res.next_option_symbol
        except Exception as exc:
            logger.error("[gRPC Client] Failed to roll %s to next month: %s", option_symbol, exc)
            return ""

    async def place_order_from_action(self, action) -> OrderPlacementResult:
        stub = self._get_stub()
        
        legs = []
        for leg in (action.legs or []):
            legs.append(broker_pb2.OptionLeg(
                option_symbol=leg.get("option_symbol", ""),
                quantity=int(leg.get("quantity", 1)),
                side=leg.get("side", ""),
                action=leg.get("action", "")
            ))
            
        req = broker_pb2.MultiLegOrder(
            strategy_id=action.strategy_id or "",
            symbol=action.symbol or "",
            order_class=action.order_class or "multileg",
            legs=legs,
            price=float(action.price or 0.0),
            side=action.side or "buy",
            quantity=int(action.quantity or 1),
            duration=action.duration or "day",
            order_type=action.order_type or "credit",
            tag=action.tag or "",
            expiry=action.expiry or "",
            width=float(action.width or 0.0)
        )
        
        try:
            report = await stub.SubmitOrder(req)
            res = {
                "order_id": report.order_id,
                "status": report.status,
                "filled_quantity": report.filled_quantity,
                "avg_fill_price": report.avg_fill_price
            }
            if report.raw_response_json:
                try:
                    res.update(json.loads(report.raw_response_json))
                except json.JSONDecodeError:
                    pass

            order_dict = res.get("order") or {}
            order_id = str(order_dict.get("id") or res.get("order_id") or res.get("order_id") or "")
            status = str(order_dict.get("status") or res.get("status") or "ok")
            kwargs = {k: v for k, v in res.items() if k not in ("order_id", "status", "order")}
            return OrderPlacementResult(
                order_id=order_id,
                status=status,
                raw_response=res,
                **kwargs
            )
        except Exception as exc:
            logger.error("[gRPC Client] Failed to place order over gRPC: %s", exc)
            raise exc

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._stub = None
            logger.info("[gRPC Client] Closed gRPC channel connection")
