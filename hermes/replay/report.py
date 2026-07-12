"""Replay result metrics: per-strategy P&L, win rate, POP calibration, drawdown."""
from __future__ import annotations

from typing import Any, Dict, List


def _max_drawdown(series: List[float]) -> float:
    """Largest peak-to-trough drop (a non-negative dollar figure)."""
    peak = float("-inf")
    worst = 0.0
    for v in series:
        peak = max(peak, v)
        worst = max(worst, peak - v)
    return round(worst, 2)


def build_report(trades: List[Dict[str, Any]],
                 equity_curve: List[Dict[str, Any]]) -> Dict[str, Any]:
    strategies = sorted({t["strategy_id"] for t in trades}
                        | {s for pt in equity_curve for s in pt["per_strategy"]})
    per: Dict[str, Any] = {}
    for sid in strategies:
        closed = [t for t in trades
                  if t["strategy_id"] == sid and t["status"] == "CLOSED"]
        resolved = [t for t in closed if t.get("pnl") is not None]
        wins = [t for t in resolved if float(t["pnl"]) > 0]
        pops = [float(t["pop"]) for t in resolved if t.get("pop") is not None]
        curve = [pt["per_strategy"].get(sid, 0.0) for pt in equity_curve]
        per[sid] = {
            "trades_opened": sum(1 for t in trades if t["strategy_id"] == sid),
            "trades_closed": len(closed),
            "trades_resolved": len(resolved),
            "total_pnl": round(sum(float(t["pnl"]) for t in resolved), 2),
            "win_rate": (round(len(wins) / len(resolved), 4) if resolved else None),
            "predicted_pop": (round(sum(pops) / len(pops), 4) if pops else None),
            "realized_pop": (round(len(wins) / len(resolved), 4) if resolved else None),
            "max_drawdown": _max_drawdown(curve),
        }
    total_curve = [pt["total"] for pt in equity_curve]
    overall_resolved = [t for t in trades
                        if t["status"] == "CLOSED" and t.get("pnl") is not None]
    return {
        "strategies": per,
        "overall": {
            "total_pnl": round(sum(float(t["pnl"]) for t in overall_resolved), 2),
            "trades_resolved": len(overall_resolved),
            "max_drawdown": _max_drawdown(total_curve),
            "final_equity_mark": (total_curve[-1] if total_curve else 0.0),
        },
    }


def render_report(report: Dict[str, Any]) -> str:
    lines = []
    header = (f"{'strategy':<12} {'opened':>7} {'closed':>7} {'pnl $':>12} "
              f"{'win%':>7} {'pred POP':>9} {'real POP':>9} {'maxDD $':>10}")
    lines.append(header)
    lines.append("-" * len(header))

    def _pct(v):
        return f"{v * 100:.1f}%" if v is not None else "-"

    for sid, m in sorted(report["strategies"].items()):
        lines.append(
            f"{sid:<12} {m['trades_opened']:>7} {m['trades_closed']:>7} "
            f"{m['total_pnl']:>12,.2f} {_pct(m['win_rate']):>7} "
            f"{_pct(m['predicted_pop']):>9} {_pct(m['realized_pop']):>9} "
            f"{m['max_drawdown']:>10,.2f}")
    o = report["overall"]
    lines.append("-" * len(header))
    lines.append(
        f"{'TOTAL':<12} {'':>7} {o['trades_resolved']:>7} "
        f"{o['total_pnl']:>12,.2f} {'':>7} {'':>9} {'':>9} "
        f"{o['max_drawdown']:>10,.2f}")
    lines.append(f"final open+realized equity mark: {o['final_equity_mark']:,.2f} $")
    return "\n".join(lines)
