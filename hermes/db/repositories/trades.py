"""Trade lifecycle: order recording, fills, position reconciliation, capacity reads."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from hermes.utils import utc_now
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from hermes.common import OCC_RE as _OCC_RE
from hermes.db.orm import (
    PendingApproval, PendingOrder, Trade,
    _close_reason_from_tag,
)
from hermes.service1_agent.transaction_manager import TransactionManager

from .base import Repository

logger = logging.getLogger("hermes.db")


class TradesRepository(Repository):
    _REJECT_STATUSES = {"rejected", "error", "expired", "canceled", "cancelled"}

    @staticmethod
    def _resolve_lots(action, default, include_close: bool = False):
        """Lots from the first sell/open (optionally close) leg, else ``default``."""
        lots = default
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if ("sell" in leg_side or "open" in leg_side
                    or (include_close and "close" in leg_side)):
                try:
                    lots = int(leg["quantity"])
                except (KeyError, TypeError, ValueError):
                    pass
                break
        return lots

    @staticmethod
    def _derive_side_type(action, fallback_to_action_side: bool = False):
        """Chain side ('put'/'call') from strategy_params, else the legs' OCC symbols."""
        side_value = (action.strategy_params or {}).get("side_type")
        if not side_value or side_value.lower() in {"buy", "sell"}:
            side_value = None
            for leg in (action.legs or []):
                m = _OCC_RE.match(str(leg.get("option_symbol", "") or ""))
                if m:
                    side_value = "put" if m.group(3) == "P" else "call"
                    break
            if side_value is None and fallback_to_action_side:
                side_value = action.side
        return side_value

    @classmethod
    def _parse_order_response(cls, response):
        """(order_status, broker_order_id, rejected) from a broker order response."""
        order = (response or {}).get("order") if isinstance(response, dict) else None
        order_status = ""
        broker_order_id: Optional[str] = None
        if isinstance(order, dict):
            order_status = str(order.get("status", "")).lower()
            broker_order_id = (
                str(order["id"]) if order.get("id") is not None else None
            )
        rejected = (
            (isinstance(response, dict) and "errors" in response)
            or order_status in cls._REJECT_STATUSES
        )
        return order_status, broker_order_id, rejected

    # ---- writes -----------------------------------------------------------
    async def record_pending_order(self, action) -> None:
        lots = self._resolve_lots(action, default=action.quantity)
        side_value = self._derive_side_type(action, fallback_to_action_side=True)

        # Detect a pure-close action (every leg is _to_close) so we can flip
        # the Trade to CLOSING in the same transaction as the PendingOrder.
        # This prevents manage_positions from seeing the trade as OPEN on the
        # next strategy pass within the same tick (double-submit guard).
        is_pure_close = bool(action.legs) and all(
            "_to_close" in (leg.get("side") or "").lower()
            for leg in action.legs
        )

        async with self.AsyncSession() as s:
            await TransactionManager.place_order(
                session=s,
                strategy_id=action.strategy_id,
                symbol=action.symbol,
                side=side_value,
                quantity=lots,
                payload={
                    "legs": action.legs, "price": action.price,
                    "tag": action.tag, "ai_authored": action.ai_authored,
                    "ai_rationale": action.ai_rationale,
                    "expiry": action.expiry,
                    "mid_at_submit": (action.strategy_params or {}).get("mid_at_submit"),
                }
            )

            if is_pure_close:
                sp = action.strategy_params or {}
                trade_id = sp.get("trade_id")
                trade_row: Optional[Trade] = None
                if trade_id is not None:
                    res = await s.execute(
                        select(Trade).filter(
                            Trade.id == int(trade_id), Trade.status == "OPEN"
                        ).limit(1)
                    )
                    trade_row = res.scalars().first()
                if trade_row is None:
                    # Fallback: match by the buy_to_close leg's option symbol.
                    short_leg_sym = next(
                        (leg.get("option_symbol") for leg in action.legs
                         if "buy_to_close" in (leg.get("side") or "").lower()),
                        None,
                    )
                    if short_leg_sym:
                        res = await s.execute(
                            select(Trade).filter(
                                Trade.strategy_id == action.strategy_id,
                                Trade.short_leg == short_leg_sym,
                                Trade.status == "OPEN",
                            ).limit(1)
                        )
                        trade_row = res.scalars().first()
                if trade_row is not None:
                    trade_row.status = "CLOSING"

            await s.commit()

    async def record_order_response(self, action, response) -> None:
        order_status, broker_order_id, rejected = self._parse_order_response(response)
        lots = self._resolve_lots(
            action, default=action.quantity if action.quantity is not None else 1)
        side_value = self._derive_side_type(action)

        sp = action.strategy_params or {}
        short_leg = sp.get("short_leg")
        long_leg = sp.get("long_leg")
        if not short_leg or not long_leg:
            for leg in (action.legs or []):
                ls = (leg.get("side") or "").lower()
                osym = leg.get("option_symbol")
                if not osym:
                    continue
                if not short_leg and "sell" in ls:
                    short_leg = osym
                elif not long_leg and "buy" in ls:
                    long_leg = osym

        short_strike = self._extract_strike(short_leg)
        long_strike = self._extract_strike(long_leg)
        width = action.width
        if width is None and short_strike is not None and long_strike is not None:
            width = abs(float(short_strike) - float(long_strike))

        expiry_date = None
        if action.expiry:
            try:
                expiry_date = datetime.strptime(str(action.expiry), "%Y-%m-%d").date()
            except (TypeError, ValueError):
                expiry_date = None

        entry_credit = None
        entry_debit = None
        ot = (action.order_type or "").lower()
        if action.price is not None:
            if ot == "credit" or (ot == "" and (action.side or "").lower() == "sell"):
                entry_credit = float(action.price)
            else:
                entry_debit = float(action.price)

        mid_at_submit = (action.strategy_params or {}).get("mid_at_submit")
        if mid_at_submit is not None:
            mid_at_submit = float(mid_at_submit)

        async with self.AsyncSession() as s:
            if rejected:
                await TransactionManager.reject(
                    session=s,
                    strategy_id=action.strategy_id,
                    symbol=action.symbol,
                    side=(side_value or action.side or "").lower(),
                    lots=lots,
                )
                await s.commit()
                await self._db.logs.write_log(
                    action.strategy_id,
                    f"[ORDER REJECTED] {action.symbol} side={side_value} "
                    f"qty={lots} response={response}",
                )
                return

            trade_fields = {
                "strategy_id": action.strategy_id,
                "symbol": action.symbol,
                "side_type": (side_value or "unknown").lower(),
                "short_leg": short_leg,
                "long_leg": long_leg,
                "short_strike": short_strike,
                "long_strike": long_strike,
                "width": float(width) if width is not None else None,
                "lots": lots,
                "entry_credit": entry_credit,
                "entry_debit": entry_debit,
                "expiry": expiry_date,
                "ai_authored": bool(getattr(action, "ai_authored", False)),
                "ai_rationale": getattr(action, "ai_rationale", None),
                "broker_order_id": broker_order_id,
                "tag": getattr(action, "tag", None),
                "entry_features": (action.strategy_params or {}).get("entry_features"),
                "mid_at_submit": mid_at_submit,
            }
            await TransactionManager.fill(
                session=s,
                strategy_id=action.strategy_id,
                symbol=action.symbol,
                side=(side_value or action.side or "").lower(),
                lots=lots,
                trade_fields=trade_fields,
            )
            await s.commit()

        await self._db.logs.write_log(
            action.strategy_id,
            f"[ORDER ACCEPTED] {action.symbol} side={side_value} qty={lots} "
            f"order_id={broker_order_id} status={order_status or 'ok'}",
        )

    async def apply_entry_fill_price(self, broker_order_id: Optional[str],
                                     fill_price: Optional[float]) -> bool:
        """Reconcile ``entry_credit``/``entry_debit`` with the broker's actual
        average fill price once the entry order reports filled.

        The value recorded at submission is the *limit* price; the broker can
        fill a credit at the limit or better (higher) and a debit at the limit
        or better (lower). ``Trade.broker_order_id`` is written only by the
        entry path, so a match here can never be a closing order. TP/SL
        management and realized P&L both read these columns, so the update is
        band-guarded: a fill through the wrong side of the limit, or an
        implausibly large improvement (beyond 1.5× / a $0.10 allowance —
        e.g. Tradier reporting a per-leg price on a multileg order), is
        treated as a data anomaly and ignored with a warning. Returns True
        only when the entry price was actually reconciled.

        Also writes ``entry_slippage`` (fill vs the ``mid_at_submit`` captured
        at submission; positive = filled worse than mid) whenever the fill is
        plausible and a mid was recorded — including a fill exactly at the
        limit, which changes no entry price but is still a real measurement.
        No mid → slippage stays NULL ("unknown"), never a fabricated 0.0.
        """
        if not broker_order_id or fill_price is None:
            return False
        fill = float(fill_price)
        if not (fill > 0):
            return False
        async with self.AsyncSession() as s:
            q = (select(Trade)
                 .filter(Trade.broker_order_id == str(broker_order_id),
                         Trade.status.in_(["OPEN", "CLOSING"]))
                 .order_by(Trade.opened_at.desc())
                 .limit(1))
            row = (await s.execute(q)).scalars().first()
            if row is None:
                return False
            eps = 1e-9
            mid = float(row.mid_at_submit) if row.mid_at_submit is not None else None
            if row.entry_credit is not None:
                limit = float(row.entry_credit)
                hi = max(limit * 1.5, limit + 0.10)
                if not (limit - eps <= fill <= hi + eps):
                    logger.warning(
                        "[DB] entry fill %.4f outside credit band [%.4f, %.4f] "
                        "for order %s (%s); keeping limit price",
                        fill, limit, hi, broker_order_id, row.symbol)
                    return False
                if mid is not None:
                    row.entry_slippage = mid - fill
                if abs(fill - limit) <= eps:
                    if mid is not None:
                        await s.commit()
                    return False
                row.entry_credit = fill
            elif row.entry_debit is not None:
                limit = float(row.entry_debit)
                lo = min(limit / 1.5, limit - 0.10)
                if not (lo - eps <= fill <= limit + eps):
                    logger.warning(
                        "[DB] entry fill %.4f outside debit band [%.4f, %.4f] "
                        "for order %s (%s); keeping limit price",
                        fill, lo, limit, broker_order_id, row.symbol)
                    return False
                if mid is not None:
                    row.entry_slippage = fill - mid
                if abs(fill - limit) <= eps:
                    if mid is not None:
                        await s.commit()
                    return False
                row.entry_debit = fill
            else:
                # Submitted without a price (no limit recorded) — can't tell
                # credit from debit, so leave the row alone.
                return False
            strategy_id, symbol = row.strategy_id, row.symbol
            slippage = (float(row.entry_slippage)
                        if row.entry_slippage is not None else None)
            await s.commit()
        slip = (f" slippage_vs_mid={slippage:.4f}"
                if slippage is not None else "")
        await self._db.logs.write_log(
            strategy_id,
            f"[ENTRY FILL] {symbol} order_id={broker_order_id} "
            f"limit={limit:.4f} filled={fill:.4f}{slip} — entry price reconciled",
        )
        return True

    async def close_trade_from_action(self, action, response) -> None:
        order_status, broker_order_id, rejected = self._parse_order_response(response)
        lots = self._resolve_lots(
            action, default=action.quantity if action.quantity is not None else 1,
            include_close=True)
        side_value = self._derive_side_type(action)

        sp = action.strategy_params or {}
        trade_id = sp.get("trade_id")
        close_reason = sp.get("close_reason") or _close_reason_from_tag(
            getattr(action, "tag", None)) or "MANAGED_CLOSE"
        exit_price = float(action.price) if action.price is not None else None

        async with self.AsyncSession() as s:
            if rejected:
                await TransactionManager.reject(
                    session=s,
                    strategy_id=action.strategy_id,
                    symbol=action.symbol,
                    side=(side_value or action.side or "").lower(),
                    lots=lots,
                )
                # Re-arm any CLOSING trade so it's retried next tick rather
                # than waiting for upsert_positions to reopen it.
                _sp = action.strategy_params or {}
                _tid = _sp.get("trade_id")
                if _tid is not None:
                    _res = await s.execute(
                        select(Trade).filter(
                            Trade.id == int(_tid), Trade.status == "CLOSING"
                        ).limit(1)
                    )
                    _closing = _res.scalars().first()
                    if _closing is not None:
                        _closing.status = "OPEN"
                await s.commit()
                await self._db.logs.write_log(
                    action.strategy_id,
                    f"[CLOSE REJECTED] {action.symbol} side={side_value} "
                    f"qty={lots} response={response}",
                )
                return

            row: Optional[Trade] = None
            if trade_id is not None:
                q = select(Trade).filter(
                    Trade.id == int(trade_id),
                    Trade.status.in_(["OPEN", "CLOSING"]),
                ).limit(1)
                result = await s.execute(q)
                row = result.scalars().first()
            if row is None:
                leg_syms = [
                    str(leg.get("option_symbol") or "")
                    for leg in (action.legs or [])
                    if leg.get("option_symbol")
                ]
                if leg_syms:
                    q = (select(Trade)
                           .filter(Trade.status.in_(["OPEN", "CLOSING"]),
                                   Trade.symbol == action.symbol,
                                   Trade.strategy_id == action.strategy_id,
                                   Trade.short_leg.in_(leg_syms))
                           .order_by(Trade.opened_at.desc())
                           .limit(1))
                    result = await s.execute(q)
                    row = result.scalars().first()

            if row is None:
                # Still consume the matching pending order even if trade is orphan
                po = await TransactionManager._consume_pending(
                    session=s,
                    strategy_id=action.strategy_id,
                    symbol=action.symbol,
                    side=(side_value or action.side or "").lower()
                )
                if po:
                    po.status = "SUBMITTED"
                await s.commit()
                await self._db.logs.write_log(
                    action.strategy_id,
                    f"[CLOSE ORPHAN] {action.symbol} no matching OPEN trade for "
                    f"trade_id={trade_id} legs={[leg.get('option_symbol') for leg in (action.legs or [])]} "
                    f"order_id={broker_order_id}",
                )
                return

            filled = order_status == "filled"
            await TransactionManager.close(
                session=s,
                strategy_id=action.strategy_id,
                symbol=action.symbol,
                side=(side_value or action.side or "").lower(),
                lots=lots,
                trade=row,
                filled=filled,
                exit_price=exit_price,
                close_reason=close_reason,
                close_tag=getattr(action, "tag", None)
            )
            await s.commit()

        verb = "FILLED" if filled else "SUBMITTED"
        await self._db.logs.write_log(
            action.strategy_id,
            f"[CLOSE {verb}] {action.symbol} trade_id={trade_id} reason={close_reason} "
            f"exit={exit_price} pnl={float(row.pnl) if row.pnl is not None else None} "
            f"order_id={broker_order_id} status={order_status or 'ok'}"
            + ("" if filled else " — awaiting broker fill"),
        )

    @staticmethod
    def _extract_strike(occ_symbol: Optional[str]):
        if not occ_symbol:
            return None
        m = _OCC_RE.match(str(occ_symbol))
        if not m:
            return None
        try:
            return int(m.group(4)) / 1000.0
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Reconciler — keep the trades table in sync with broker positions.
    # Called every tick (CascadingEngine.sync_positions).
    # ------------------------------------------------------------------
    async def upsert_positions(self, positions: List[Dict[str, Any]],
                          active_order_legs: Optional[Any] = None,
                          opening_order_legs: Optional[Any] = None) -> None:
        """Reconcile OPEN trades against broker truth."""
        broker_qty: Dict[str, int] = {}
        for p in positions or []:
            sym = str(p.get("symbol", "") or "")
            m = _OCC_RE.match(sym)
            if not m:
                continue                          # skip equities
            try:
                qty = int(round(float(p.get("quantity", 0) or 0)))
            except (TypeError, ValueError):
                qty = 0
            if qty == 0:
                continue
            broker_qty[sym] = broker_qty.get(sym, 0) + abs(qty)

        active_legs: set = set(active_order_legs or [])
        opening_legs: set = set(opening_order_legs or [])
        broker_legs: set = set(broker_qty.keys())
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(Trade).filter(Trade.status.in_(["OPEN", "CLOSING"])))
            rows = result.scalars().all()
            closed = 0
            reopened = 0
            held_pending_entry = 0
            for t in rows:
                legs = {leg for leg in (t.short_leg, t.long_leg) if leg}
                held = bool(legs & broker_legs)
                resting = bool(legs & active_legs)
                if t.status == "OPEN":
                    # Broker-flat OPEN trade the bot never explicitly closed —
                    # flatten it so the book matches broker truth.
                    if held or resting:
                        continue
                    close_reason = t.close_reason or "RECONCILED_BROKER_FLAT"
                    await TransactionManager.reconcile_trade(s, t, "force_close", close_reason=close_reason)
                    closed += 1
                else:  # CLOSING — a close was submitted; confirm its fate.
                    if legs & opening_legs:
                        # The ENTRY order is still working broker-side, so no
                        # position exists yet for the close to have filled —
                        # broker-flat here means "entry unfilled", not "close
                        # filled". Hold until the entry resolves (2026-07-10
                        # race: finalizing here fabricated a P&L, then the
                        # real fill was orphan-adopted as a duplicate trade).
                        held_pending_entry += 1
                        continue
                    if not held:
                        # Legs went flat → the close filled. Finalize, keeping
                        # the close_reason / exit / pnl stashed at submit time.
                        await TransactionManager.reconcile_trade(s, t, "finish_close")
                        closed += 1
                    elif resting:
                        continue            # close order still working
                    else:
                        # Still held with no resting order → the close didn't
                        # take (rejected/canceled async). Re-arm for retry.
                        await TransactionManager.reconcile_trade(s, t, "reopen")
                        reopened += 1
            if closed or reopened:
                await s.commit()

        await self._db.logs.write_log(
            "ENGINE",
            f"reconciled {len(positions or [])} broker pos; "
            f"closed_orphans={closed} reopened={reopened} "
            f"held_pending_entry={held_pending_entry} "
            f"positions_legs={len(broker_qty)} resting_legs={len(active_legs)}",
        )

    # ---- reads ------------------------------------------------------------
    async def open_trades(self, strategy_id: str) -> List[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            result = await s.execute(select(Trade).filter_by(strategy_id=strategy_id, status="OPEN"))
            rows = result.scalars().all()
            return [self._trade_dict(r) for r in rows]

    async def closing_trades(self, strategy_id: str) -> List[Dict[str, Any]]:
        """Trades with a submitted-but-unfilled close order still working.

        DS0's 3 PM sweep must be able to re-price a trade whose resting
        take-profit never filled; every other strategy only ever looks at
        OPEN trades.
        """
        async with self.AsyncSession() as s:
            result = await s.execute(select(Trade).filter_by(strategy_id=strategy_id, status="CLOSING"))
            rows = result.scalars().all()
            return [self._trade_dict(r) for r in rows]

    async def count_trades_for_expiry(self, strategy_id: str, symbol: str,
                                      side: str, expiry: str) -> int:
        """Trades of ANY status for (strategy, symbol, side, expiry).

        DS0's one-shot-per-side-per-day gate: an open position, a working
        close and a closed trade (win or loss) all block re-entry alike.
        """
        try:
            exp_date = datetime.strptime(str(expiry), "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                f"count_trades_for_expiry: invalid expiry {expiry!r}; "
                "expected YYYY-MM-DD"
            ) from exc
        async with self.AsyncSession() as s:
            q = (select(Trade).filter_by(strategy_id=strategy_id, symbol=symbol)
                 .filter(Trade.side_type == side, Trade.expiry == exp_date))
            result = await s.execute(q)
            return len(result.scalars().all())

    async def all_open_trades(self) -> List[Dict[str, Any]]:
        """Every OPEN trade across all strategies.

        ``open_trades`` is per-strategy; the overseer's autonomous close
        path needs the whole book at once so it can decide which positions
        — regardless of which strategy opened them — to close.
        """
        async with self.AsyncSession() as s:
            result = await s.execute(select(Trade).filter_by(status="OPEN"))
            rows = result.scalars().all()
            return [self._trade_dict(r) for r in rows]

    async def open_legs(self, strategy_id: str, symbol: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        trades = await self.open_trades(strategy_id)
        for t in trades:
            if t["symbol"] != symbol:
                continue
            expiry_val = t.get("expiry")
            expiry_iso = None
            if expiry_val:
                if isinstance(expiry_val, date):
                    expiry_iso = expiry_val.isoformat()
                else:
                    try:
                        expiry_iso = datetime.strptime(str(expiry_val), "%Y-%m-%d").date().isoformat()
                    except (ValueError, TypeError):
                        logger.warning("[DB] Skipping invalid trade expiry: %r", expiry_val)
                        continue
            side = t.get("side_type")
            for leg_key in ("short_leg", "long_leg"):
                opt = t.get(leg_key)
                if opt:
                    out.append({"option_symbol": opt, "side": side,
                                "expiry": expiry_iso})
        return out

    async def count_open_contracts(self, strategy_id: str, symbol: str, side: str,
                              expiry: str) -> int:
        if not expiry:
            raise ValueError(
                "count_open_contracts requires an expiry (YYYY-MM-DD); the "
                "symbol-wide global mode has been removed."
            )
        try:
            exp_date = datetime.strptime(str(expiry), "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                f"count_open_contracts: invalid expiry {expiry!r}; "
                "expected YYYY-MM-DD"
            ) from exc
        async with self.AsyncSession() as s:
            q = (select(Trade).filter_by(strategy_id=strategy_id, symbol=symbol, status="OPEN")
                 .filter(Trade.side_type == side, Trade.expiry == exp_date))
            result = await s.execute(q)
            rows = result.scalars().all()
            return sum(int(r.lots or 0) for r in rows)

    async def count_pending_orders(self, strategy_id: str, symbol: str, side: str,
                              expiry: str) -> int:
        if not expiry:
            raise ValueError(
                "count_pending_orders requires an expiry (YYYY-MM-DD); the "
                "symbol-wide global mode has been removed."
            )
        side_lower = side.lower()
        async with self.AsyncSession() as s:
            # 1) Directly-submitted pending orders — scoped to this chain
            result = await s.execute(select(PendingOrder).filter_by(strategy_id=strategy_id, symbol=symbol, side=side_lower, status="PENDING"))
            po_rows = result.scalars().all()
            po_lots = 0
            for r in po_rows:
                payload = r.payload or {}
                if payload.get("expiry") != expiry:
                    continue
                po_lots += int(r.quantity or 0)

            # 2) Approval-queued trades not yet executed — scoped to this chain
            result = await s.execute(select(PendingApproval).filter_by(strategy_id=strategy_id, symbol=symbol, status="PENDING"))
            pa_rows = result.scalars().all()
            pa_lots = 0
            for r in pa_rows:
                aj = r.action_json or {}
                sp = aj.get("strategy_params") or {}
                if sp.get("side_type", "").lower() != side_lower:
                    continue
                if aj.get("expiry") != expiry:
                    continue
                for leg in (aj.get("legs") or []):
                    leg_side = (leg.get("side") or "").lower()
                    if "sell" in leg_side or "open" in leg_side:
                        try:
                            pa_lots += int(leg["quantity"])
                        except (KeyError, TypeError, ValueError):
                            pa_lots += 1
                        break

        return po_lots + pa_lots

    async def expire_stale_pending_orders(self, older_than_seconds: int) -> int:
        cutoff = utc_now() - timedelta(seconds=older_than_seconds)
        expired = 0
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingOrder)
                .filter(
                    PendingOrder.status == "PENDING",
                    PendingOrder.submitted_at < cutoff,
                )
            )
            stale = result.scalars().all()
            for row in stale:
                await TransactionManager.expire_order(s, row)
                expired += 1
            if expired:
                await s.commit()
        return expired

    async def tracked_option_symbols(self) -> set:
        async with self.AsyncSession() as s:
            # CLOSING trades still hold their legs at the broker until the close
            # fills, so they must count as tracked — otherwise a position mid-close
            # would be misflagged as an orphan.
            result = await s.execute(
                select(Trade).filter(Trade.status.in_(["OPEN", "CLOSING"])))
            rows = result.scalars().all()
            symbols = set()
            for r in rows:
                if r.short_leg:
                    symbols.add(r.short_leg)
                if r.long_leg:
                    symbols.add(r.long_leg)
            return symbols

    async def closed_trades_entry_features(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Most-recent CLOSED trades carrying an entry-feature snapshot and a
        realized pnl — the labelled rows POP outcome-calibration trains on."""
        async with self.AsyncSession() as s:
            q = (select(Trade)
                 .filter(Trade.status == "CLOSED",
                         Trade.pnl.isnot(None),
                         Trade.entry_features.isnot(None))
                 .order_by(Trade.closed_at.desc())
                 .limit(limit))
            result = await s.execute(q)
            rows = result.scalars().all()
            return [
                {
                    "strategy_id": r.strategy_id,
                    "symbol": r.symbol,
                    "entry_features": r.entry_features,
                    "pnl": float(r.pnl),
                    "closed_at": r.closed_at,
                }
                for r in rows
            ]

    async def equity_position(self, symbol: str) -> int:
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(Trade)
                .filter_by(symbol=symbol, side_type="equity", status="OPEN")
                .order_by(Trade.opened_at.desc())
                .limit(1)
            )
            row = result.scalars().first()
            return int(row.lots) if row else 0

    async def latest_closed_trade_time(self, strategy_id: str, symbol: str) -> Optional[datetime]:
        from sqlalchemy import desc
        async with self.AsyncSession() as session:
            q = select(Trade.closed_at).where(
                Trade.strategy_id == strategy_id,
                Trade.symbol == symbol,
                Trade.status == "CLOSED",
                Trade.closed_at.isnot(None)
            ).order_by(desc(Trade.closed_at)).limit(1)
            res = await session.execute(q)
            row = res.first()
            return row[0] if row else None

    @staticmethod
    def _trade_dict(r: Trade) -> Dict[str, Any]:
        return {
            "id": r.id, "strategy_id": r.strategy_id, "symbol": r.symbol,
            "side_type": r.side_type, "short_leg": r.short_leg, "long_leg": r.long_leg,
            "short_strike": float(r.short_strike) if r.short_strike else None,
            "long_strike": float(r.long_strike) if r.long_strike else None,
            "width": float(r.width) if r.width else None,
            "lots": int(r.lots), "entry_credit": float(r.entry_credit or 0),
            "entry_debit": float(r.entry_debit) if r.entry_debit is not None else None,
            "expiry": r.expiry, "status": r.status,
        }
