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
                 equity_curve: List[Dict[str, Any]],
                 synthetic_pricing: bool = False) -> Dict[str, Any]:
    from hermes.ml.pop_calibration import extract_calibration_rows

    strategies = sorted({t["strategy_id"] for t in trades}
                        | {s for pt in equity_curve for s in pt["per_strategy"]})
    per: Dict[str, Any] = {}
    for sid in strategies:
        closed = [t for t in trades
                  if t["strategy_id"] == sid and t["status"] == "CLOSED"]
        resolved = [t for t in closed if t.get("pnl") is not None]
        wins = [t for t in resolved if float(t["pnl"]) > 0]
        losses = [t for t in resolved if float(t["pnl"]) <= 0]
        
        avg_win = round(sum(float(t["pnl"]) for t in wins) / len(wins), 2) if wins else 0.0
        avg_loss = round(sum(float(t["pnl"]) for t in losses) / len(losses), 2) if losses else 0.0
        
        pops = [float(t["pop"]) for t in resolved if t.get("pop") is not None]
        curve = [pt["per_strategy"].get(sid, 0.0) for pt in equity_curve]
        
        # Build calibration table if we have POP predictions
        calibration_table = []
        cal_pops, cal_outcomes = extract_calibration_rows(resolved)
        if cal_pops:
            thresholds = [0.0, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 1.01]
            for i in range(len(thresholds) - 1):
                low = thresholds[i]
                high = thresholds[i+1]
                bucket_data = [(p, o) for p, o in zip(cal_pops, cal_outcomes) if low <= p < high]
                if not bucket_data:
                    continue
                count = len(bucket_data)
                avg_pred = sum(p for p, _ in bucket_data) / count
                realized_wr = sum(o for _, o in bucket_data) / count
                calibration_table.append({
                    "bucket_range": f"{low:.0%} to {high:.0%}" if high <= 1.0 else f"≥{low:.0%}",
                    "count": count,
                    "avg_predicted": round(avg_pred, 4),
                    "realized_win_rate": round(realized_wr, 4)
                })

        per[sid] = {
            "trades_opened": sum(1 for t in trades if t["strategy_id"] == sid),
            "trades_closed": len(closed),
            "trades_resolved": len(resolved),
            "total_pnl": round(sum(float(t["pnl"]) for t in resolved), 2),
            "win_rate": (round(len(wins) / len(resolved), 4) if resolved else None),
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "predicted_pop": (round(sum(pops) / len(pops), 4) if pops else None),
            "realized_pop": (round(len(wins) / len(resolved), 4) if resolved else None),
            "max_drawdown": _max_drawdown(curve),
            "calibration_table": calibration_table if calibration_table else None,
        }
    total_curve = [pt["total"] for pt in equity_curve]
    overall_resolved = [t for t in trades
                        if t["status"] == "CLOSED" and t.get("pnl") is not None]
    return {
        "synthetic_pricing": synthetic_pricing,
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
    
    if report.get("synthetic_pricing", False):
        lines.append("=" * 80)
        lines.append("APPROXIMATE RESULTS (SYNTHETIC OPTION PRICING)")
        lines.append("Note: Options were priced using B-S model with daily realized IV proxy.")
        lines.append("=" * 80 + "\n")

    header = (f"{'strategy':<12} {'opened':>7} {'closed':>7} {'pnl $':>12} "
              f"{'win%':>7} {'avg win':>10} {'avg loss':>10} {'maxDD $':>10}")
    lines.append(header)
    lines.append("-" * len(header))

    def _pct(v):
        return f"{v * 100:.1f}%" if v is not None else "-"

    for sid, m in sorted(report["strategies"].items()):
        lines.append(
            f"{sid:<12} {m['trades_opened']:>7} {m['trades_closed']:>7} "
            f"{m['total_pnl']:>12,.2f} {_pct(m['win_rate']):>7} "
            f"{m['avg_win']:>10,.2f} {m['avg_loss']:>10,.2f} "
            f"{m['max_drawdown']:>10,.2f}")
    o = report["overall"]
    lines.append("-" * len(header))
    lines.append(
        f"{'TOTAL':<12} {'':>7} {o['trades_resolved']:>7} "
        f"{o['total_pnl']:>12,.2f} {'':>7} {'':>10} {'':>10} "
        f"{o['max_drawdown']:>10,.2f}")
    lines.append(f"final open+realized equity mark: {o['final_equity_mark']:,.2f} $")
    
    # Render calibration tables if present
    has_cal = False
    for sid, m in sorted(report["strategies"].items()):
        if m.get("calibration_table"):
            if not has_cal:
                lines.append("\nPOP Calibration Tables:")
                lines.append("=======================")
                has_cal = True
            lines.append(f"\nStrategy: {sid}")
            lines.append(f"{'POP Bucket':<15} {'Trades':>8} {'Avg Pred':>10} {'Realized WR':>12}")
            lines.append("-" * 49)
            for row in m["calibration_table"]:
                lines.append(
                    f"{row['bucket_range']:<15} {row['count']:>8} "
                    f"{row['avg_predicted']*100:>9.1f}% {row['realized_win_rate']*100:>11.1f}%"
                )
                
    return "\n".join(lines)
