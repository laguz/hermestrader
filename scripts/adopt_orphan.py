#!/usr/bin/env python3
"""
[Manual Orphan Adoption]
Adopt an orphan broker position into a tracked, managed Trade row.

The tick pipeline's automatic adoption (``reconcile_orphans``) can only match
an orphan leg to a *recent* Hermes-tagged broker order — a fill whose local
bookkeeping was lost during an outage ages out of the broker's order listing
and is then flagged as ``orphan position: <OCC>`` every tick, forever, with
no TP/SL/exit management. This script is the operator's escape hatch: it
re-runs the legs through the exact same fill-recording path a live order
uses (``TradesRepository.record_order_response``), so the resulting Trade is
indistinguishable from one recorded at fill time.

Deliberately manual (run by the operator, never by the agent): an untagged
broker position may be a genuine hand-opened trade, and adopting it puts the
strategy's automated exits in charge of it — that attribution decision
belongs to a human.

Usage (net credit of the spread, per share, from broker cost basis):

    python -m scripts.adopt_orphan \\
        --dsn postgresql+psycopg://hermes:hermes@localhost:5434/hermes \\
        --strategy CS7 \\
        --short IWM260714P00292000 --long IWM260714P00291000 \\
        --credit 0.18 --lots 1 --yes

Without ``--yes`` it prints the Trade it would create and exits.

Exit codes: 0 adopted (or dry-run), 1 error, 2 refused (legs already tracked).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone

logger = logging.getLogger("hermes.scripts.adopt_orphan")


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", type=str, default=None, help="Override HERMES_DSN")
    parser.add_argument("--strategy", type=str, required=True,
                        help="Strategy to own the trade (e.g. CS7, CS75, WHEEL)")
    parser.add_argument("--short", type=str, required=True,
                        help="OCC symbol of the short leg")
    parser.add_argument("--long", type=str, default=None,
                        help="OCC symbol of the long leg (omit for single-leg)")
    parser.add_argument("--credit", type=float, required=True,
                        help="Net entry credit per share (e.g. 0.18)")
    parser.add_argument("--lots", type=int, default=1)
    parser.add_argument("--yes", action="store_true",
                        help="Actually write; without it, dry-run only")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        import os
        from hermes.db.models import HermesDB
        from hermes.service1_agent.core import TradeAction
        from hermes.service1_agent.strategies._helpers import parse_occ
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("import failed: %s", exc)
        return 1

    strategy_id = args.strategy.upper()
    short_occ = args.short.upper()
    long_occ = args.long.upper() if args.long else None

    parsed = parse_occ(short_occ)
    if not parsed:
        logger.error("--short %r is not a valid OCC symbol", short_occ)
        return 1
    if long_occ and not parse_occ(long_occ):
        logger.error("--long %r is not a valid OCC symbol", long_occ)
        return 1
    if not (args.credit > 0):
        logger.error("--credit must be positive (got %s)", args.credit)
        return 1
    if args.lots < 1:
        logger.error("--lots must be >= 1 (got %s)", args.lots)
        return 1

    symbol = parsed["underlying"]
    expiry = parsed["expiry"].isoformat()

    dsn = args.dsn or os.environ.get(
        "HERMES_DSN",
        "postgresql+psycopg://hermes:hermes@localhost:5432/hermes",
    )
    db = HermesDB(dsn)

    # Refuse if any leg is already tracked by an OPEN/CLOSING trade —
    # double-adoption would have two Trade rows managing one position.
    tracked = await db.trades.tracked_option_symbols()
    legs_set = {short_occ} | ({long_occ} if long_occ else set())
    already = legs_set & set(tracked or [])
    if already:
        logger.error("refusing: leg(s) already tracked by an open trade: %s",
                     sorted(already))
        return 2

    legs = [{"option_symbol": short_occ, "side": "sell_to_open",
             "quantity": args.lots}]
    if long_occ:
        legs.append({"option_symbol": long_occ, "side": "buy_to_open",
                     "quantity": args.lots})

    action = TradeAction(
        strategy_id=strategy_id,
        symbol=symbol,
        order_class="multileg" if long_occ else "option",
        legs=legs,
        price=float(args.credit),
        side="sell",
        quantity=args.lots,
        order_type="credit",
        expiry=expiry,
        tag=f"HERMES_{strategy_id}",
    )
    synthetic_id = "MANUAL-ADOPT-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    logger.info("adopting as %s: %s %s exp=%s short=%s long=%s lots=%d "
                "entry_credit=%.4f order_id=%s",
                strategy_id, symbol, action.order_class, expiry,
                short_occ, long_occ, args.lots, args.credit, synthetic_id)
    if not args.yes:
        logger.info("dry-run (pass --yes to write)")
        return 0

    resp = {"order": {"id": synthetic_id, "status": "filled"}}
    await db.trades.record_order_response(action, resp)
    await db.logs.write_log(
        strategy_id,
        f"[ORPHAN ADOPTED MANUAL] {symbol} exp={expiry} short={short_occ} "
        f"long={long_occ} lots={args.lots} credit={args.credit:.4f} "
        f"order_id={synthetic_id} — reopened as a tracked Trade",
    )
    logger.info("adopted — the next tick manages it like any other open trade")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
