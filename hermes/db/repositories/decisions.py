"""AI-decision audit trail and XGB prediction storage."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import select

from hermes.db.orm import AIDecision, Prediction

from .base import Repository


class DecisionsRepository(Repository):
    async def write_ai_decision(self, strategy_id: str, symbol: str,
                          autonomy: str, decision: Dict[str, Any]) -> None:
        async with self.AsyncSession() as s:
            s.add(AIDecision(strategy_id=strategy_id, symbol=symbol or "*",
                             autonomy=autonomy, decision=decision))
            await s.commit()

    async def recent_ai_decisions(self, strategy_id: Optional[str] = None,
                            symbol: Optional[str] = None,
                            limit: int = 20) -> List[Dict[str, Any]]:
        """Return recent ai_decisions rows, newest-first.

        Optionally filter by strategy_id and/or symbol.
        Returns a list of plain dicts ready for JSON serialisation.
        """
        async with self.AsyncSession() as s:
            q = select(AIDecision).order_by(AIDecision.ts.desc())
            if strategy_id is not None:
                q = q.filter(AIDecision.strategy_id == strategy_id)
            if symbol is not None:
                q = q.filter(AIDecision.symbol == symbol.upper())
            result = await s.execute(q.limit(limit))
            rows = result.scalars().all()
            return [
                {
                    "ts": r.ts.isoformat() if r.ts else None,
                    "strategy_id": r.strategy_id,
                    "symbol": r.symbol,
                    "autonomy": r.autonomy,
                    "decision": r.decision,
                }
                for r in rows
            ]

    async def write_prediction(self, symbol: str, ret: float, price: float, spot: float = 0.0) -> None:
        async with self.AsyncSession() as s:
            s.add(Prediction(symbol=symbol, predicted_return=ret, predicted_price=price, spot=spot))
            await s.commit()

    async def latest_prediction(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            q = select(Prediction).filter_by(symbol=symbol).order_by(Prediction.ts.desc()).limit(1)
            result = await s.execute(q)
            row = result.scalars().first()
            if row:
                return {
                    "predicted_return": float(row.predicted_return or 0),
                    "predicted_price": float(row.predicted_price or 0),
                    "asof": row.ts,
                }
            return None

    async def latest_predictions_batch(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch the latest prediction for multiple symbols in one query."""
        if not symbols:
            return {}
        from sqlalchemy import bindparam, text as sa_text
        # Postgres-specific DISTINCT ON for efficient latest-per-group
        sql = sa_text("""
            SELECT DISTINCT ON (symbol)
                symbol, predicted_return, predicted_price, ts
            FROM predictions
            WHERE symbol IN :symbols
            ORDER BY symbol, ts DESC
        """).bindparams(bindparam("symbols", expanding=True))
        results = {}
        async with self.AsyncSession() as s:
            result = await s.execute(sql, {"symbols": list(symbols)})
            rows = result.fetchall()
            for r in rows:
                results[r.symbol] = {
                    "predicted_return": float(r.predicted_return or 0),
                    "predicted_price": float(r.predicted_price or 0),
                    "asof": r.ts,
                }
        return results
