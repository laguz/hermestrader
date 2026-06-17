"""Trade lifecycle: order recording, fills, position reconciliation, capacity reads."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from hermes.common import OCC_RE as _OCC_RE
from hermes.db.orm import (
    PendingApproval, PendingOrder, Trade,
    _close_reason_from_tag, _compute_realized_pnl,
)

logger = logging.getLogger("hermes.db")


class TradesRepositoryMixin:
    _REJECT_STATUSES = {"rejected", "error", "expired", "canceled", "cancelled"}

    # ---- writes -----------------------------------------------------------
    async def record_pending_order(self, action) -> None:
        lots = action.quantity  # fallback
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if "sell" in leg_side or "open" in leg_side:
                try:
                    lots = int(leg["quantity"])
                except (KeyError, TypeError, ValueError):
                    pass
                break

        side_value = (action.strategy_params or {}).get("side_type")
        if not side_value or side_value.lower() in {"buy", "sell"}:
            side_value = None
            for leg in (action.legs or []):
                m = _OCC_RE.match(str(leg.get("option_symbol", "") or ""))
                if m:
                    side_value = "put" if m.group(3) == "P" else "call"
                    break
            if side_value is None:
                side_value = action.side

        async with self.AsyncSession() as s:
            s.add(PendingOrder(
                strategy_id=action.strategy_id, symbol=action.symbol,
                side=side_value,
                quantity=lots,          # lot count, not order count
                payload={
                    "legs": action.legs, "price": action.price,
                    "tag": action.tag, "ai_authored": action.ai_authored,
                    "ai_rationale": action.ai_rationale,
                    "expiry": action.expiry,
                },
            ))
            await s.commit()

    async def record_order_response(self, action, response) -> None:
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
            or order_status in self._REJECT_STATUSES
        )

        # Resolve lots from the first sell/open leg (matches record_pending_order)
        lots = action.quantity or 1
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if "sell" in leg_side or "open" in leg_side:
                try:
                    lots = int(leg["quantity"])
                except (KeyError, TypeError, ValueError):
                    pass
                break

        side_value = (action.strategy_params or {}).get("side_type")
        if not side_value or side_value.lower() in {"buy", "sell"}:
            side_value = None
            for leg in (action.legs or []):
                m = _OCC_RE.match(str(leg.get("option_symbol", "") or ""))
                if m:
                    side_value = "put" if m.group(3) == "P" else "call"
                    break

        await self._consume_matching_pending(
            strategy_id=action.strategy_id, symbol=action.symbol,
            side=(side_value or action.side or "").lower(), lots=lots,
            terminal_status="REJECTED" if rejected else "SUBMITTED",
        )

        if rejected:
            await self.write_log(
                action.strategy_id,
                f"[ORDER REJECTED] {action.symbol} side={side_value} "
                f"qty={lots} response={response}",
            )
            return

        sp = action.strategy_params or {}
        short_leg = sp.get("short_leg")
        long_leg = sp.get("long_leg")
        if not short_leg or not long_leg:
            for leg in (action.legs or []):
                ls = (leg.get("side") or "").lower()
                osym = leg.get("option_symbol")
                if not osym:
                    continue
                if not short_leg and ("sell" in ls or "open" in ls and "sell" in ls):
                    short_leg = osym
                elif not long_leg and ("buy" in ls or "open" in ls and "buy" in ls):
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

        async with self.AsyncSession() as s:
            s.add(Trade(
                strategy_id=action.strategy_id,
                symbol=action.symbol,
                side_type=(side_value or "unknown").lower(),
                short_leg=short_leg,
                long_leg=long_leg,
                short_strike=short_strike,
                long_strike=long_strike,
                width=float(width) if width is not None else None,
                lots=lots,
                entry_credit=entry_credit,
                entry_debit=entry_debit,
                expiry=expiry_date,
                status="OPEN",
                ai_authored=bool(getattr(action, "ai_authored", False)),
                ai_rationale=getattr(action, "ai_rationale", None),
                broker_order_id=broker_order_id,
                tag=getattr(action, "tag", None),
                entry_features=(action.strategy_params or {}).get("entry_features"),
            ))
            await s.commit()

        await self.write_log(
            action.strategy_id,
            f"[ORDER ACCEPTED] {action.symbol} side={side_value} qty={lots} "
            f"order_id={broker_order_id} status={order_status or 'ok'}",
        )

    async def close_trade_from_action(self, action, response) -> None:
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
            or order_status in self._REJECT_STATUSES
        )

        lots = action.quantity or 1
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if "sell" in leg_side or "open" in leg_side or "close" in leg_side:
                try:
                    lots = int(leg["quantity"])
                except (KeyError, TypeError, ValueError):
                    pass
                break

        side_value = (action.strategy_params or {}).get("side_type")
        if not side_value or side_value.lower() in {"buy", "sell"}:
            side_value = None
            for leg in (action.legs or []):
                m = _OCC_RE.match(str(leg.get("option_symbol", "") or ""))
                if m:
                    side_value = "put" if m.group(3) == "P" else "call"
                    break

        await self._consume_matching_pending(
            strategy_id=action.strategy_id, symbol=action.symbol,
            side=(side_value or action.side or "").lower(), lots=lots,
            terminal_status="REJECTED" if rejected else "SUBMITTED",
        )

        if rejected:
            await self.write_log(
                action.strategy_id,
                f"[CLOSE REJECTED] {action.symbol} side={side_value} "
                f"qty={lots} response={response}",
            )
            return

        sp = action.strategy_params or {}
        trade_id = sp.get("trade_id")
        close_reason = sp.get("close_reason") or _close_reason_from_tag(
            getattr(action, "tag", None)) or "MANAGED_CLOSE"
        exit_price = float(action.price) if action.price is not None else None

        async with self.AsyncSession() as s:
            row: Optional[Trade] = None
            if trade_id is not None:
                q = select(Trade).filter(Trade.id == int(trade_id), Trade.status == "OPEN").limit(1)
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
                           .filter(Trade.status == "OPEN",
                                   Trade.symbol == action.symbol,
                                   Trade.strategy_id == action.strategy_id,
                                   Trade.short_leg.in_(leg_syms))
                           .order_by(Trade.opened_at.desc())
                           .limit(1))
                    result = await s.execute(q)
                    row = result.scalars().first()

            if row is None:
                await self.write_log(
                    action.strategy_id,
                    f"[CLOSE ORPHAN] {action.symbol} no matching OPEN trade for "
                    f"trade_id={trade_id} legs={[leg.get('option_symbol') for leg in (action.legs or [])]} "
                    f"order_id={broker_order_id}",
                )
                return

            # Stash the close economics now, regardless of fill timing, so they
            # survive into the final CLOSED row whether we finalize here (on a
            # confirmed fill) or later via the position-sync reconciler.
            row.close_reason = close_reason
            row.close_tag = getattr(action, "tag", None)
            if exit_price is not None:
                row.exit_price = exit_price
            row.pnl = _compute_realized_pnl(
                entry_credit=row.entry_credit,
                entry_debit=row.entry_debit,
                exit_price=exit_price,
                lots=int(row.lots or 0),
            )

            # Only finalize CLOSED when the broker confirms the fill. On mere
            # acceptance the order may still rest unfilled or be rejected
            # asynchronously — marking CLOSED here is what stranded such closes
            # as orphans (DB said closed while the broker still held the legs).
            # Move to CLOSING instead; ``upsert_positions`` finalizes CLOSED
            # once the legs go flat, or reopens the trade if the close fails.
            filled = order_status == "filled"
            if filled:
                row.force_close()
                row.closed_at = datetime.utcnow()
            else:
                row.begin_close()
            await s.commit()

        verb = "FILLED" if filled else "SUBMITTED"
        await self.write_log(
            action.strategy_id,
            f"[CLOSE {verb}] {action.symbol} trade_id={trade_id} reason={close_reason} "
            f"exit={exit_price} pnl={float(row.pnl) if row.pnl is not None else None} "
            f"order_id={broker_order_id} status={order_status or 'ok'}"
            + ("" if filled else " — awaiting broker fill"),
        )

    async def _consume_matching_pending(self, *, strategy_id: str, symbol: str,
                                  side: str, lots: int,
                                  terminal_status: str) -> None:
        """Delete the most-recent matching PendingOrder so capacity isn't
        double-counted with the freshly-written Trade row."""
        async with self.AsyncSession() as s:
            q = (select(PendingOrder)
                   .filter(
                       PendingOrder.strategy_id == strategy_id,
                       PendingOrder.symbol == symbol,
                       PendingOrder.side == side,
                       PendingOrder.status == "PENDING",
                   )
                   .order_by(PendingOrder.submitted_at.desc())
                   .limit(1))
            result = await s.execute(q)
            row = result.scalars().first()
            if row is None:
                return
            row.status = terminal_status
            await s.commit()

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
                          active_order_legs: Optional[Any] = None) -> None:
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
        broker_legs: set = set(broker_qty.keys())
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(Trade).filter(Trade.status.in_(["OPEN", "CLOSING"])))
            rows = result.scalars().all()
            closed = 0
            reopened = 0
            for t in rows:
                legs = {leg for leg in (t.short_leg, t.long_leg) if leg}
                held = bool(legs & broker_legs)
                resting = bool(legs & active_legs)
                if t.status == "OPEN":
                    # Broker-flat OPEN trade the bot never explicitly closed —
                    # flatten it so the book matches broker truth.
                    if held or resting:
                        continue
                    t.force_close()
                    t.closed_at = datetime.utcnow()
                    if not t.close_reason:
                        t.close_reason = "RECONCILED_BROKER_FLAT"
                    closed += 1
                else:  # CLOSING — a close was submitted; confirm its fate.
                    if not held:
                        # Legs went flat → the close filled. Finalize, keeping
                        # the close_reason / exit / pnl stashed at submit time.
                        t.finish_close()
                        t.closed_at = datetime.utcnow()
                        closed += 1
                    elif resting:
                        continue            # close order still working
                    else:
                        # Still held with no resting order → the close didn't
                        # take (rejected/canceled async). Re-arm for retry.
                        t.reopen()
                        reopened += 1
            if closed or reopened:
                await s.commit()

        await self.write_log(
            "ENGINE",
            f"reconciled {len(positions or [])} broker pos; "
            f"closed_orphans={closed} reopened={reopened} "
            f"positions_legs={len(broker_qty)} resting_legs={len(active_legs)}",
        )

    # ---- reads ------------------------------------------------------------
    async def open_trades(self, strategy_id: str) -> List[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            result = await s.execute(select(Trade).filter_by(strategy_id=strategy_id, status="OPEN"))
            rows = result.scalars().all()
            return [self._trade_dict(r) for r in rows]

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

    async def fetch_trade_outcomes(
        self,
        *,
        strategy_id: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Labelled ``(entry context, knobs, realized outcome)`` rows.

        The training surface for outcome-driven tuning (Phase 0). Returns one
        dict per CLOSED trade that has a realized ``pnl``, pairing the
        at-entry ``entry_features`` snapshot with the realized result:

            {
              "trade_id", "strategy_id", "symbol", "side_type",
              "opened_at", "closed_at", "close_reason",
              "realized_pnl", "won" (pnl > 0),
              "hold_days", "entry_features" {knobs, pop, short_delta, ...}
            }

        Trades opened before the ``entry_features`` column existed come back
        with ``entry_features=None`` — callers decide whether to drop them.
        """
        q = select(Trade).filter(Trade.status == "CLOSED",
                                 Trade.pnl.is_not(None))
        if strategy_id is not None:
            q = q.filter(Trade.strategy_id == strategy_id)
        if since is not None:
            q = q.filter(Trade.opened_at >= since)
        q = q.order_by(Trade.opened_at.desc())
        if limit is not None:
            q = q.limit(int(limit))

        async with self.AsyncSession() as s:
            rows = (await s.execute(q)).scalars().all()

        out: List[Dict[str, Any]] = []
        for r in rows:
            pnl = float(r.pnl) if r.pnl is not None else None
            hold_days = None
            if r.opened_at is not None and r.closed_at is not None:
                hold_days = round(
                    (r.closed_at - r.opened_at).total_seconds() / 86400.0, 3)
            out.append({
                "trade_id": r.id,
                "strategy_id": r.strategy_id,
                "symbol": r.symbol,
                "side_type": r.side_type,
                "opened_at": r.opened_at,
                "closed_at": r.closed_at,
                "close_reason": r.close_reason,
                "realized_pnl": pnl,
                "won": (pnl > 0.0) if pnl is not None else None,
                "hold_days": hold_days,
                "entry_features": r.entry_features,
            })
        return out

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
        cutoff = datetime.utcnow() - timedelta(seconds=older_than_seconds)
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
                row.status = "EXPIRED"
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
        from sqlalchemy import select, desc
        from ..orm import Trade
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
            "expiry": r.expiry, "status": r.status,
        }
