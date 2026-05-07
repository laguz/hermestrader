"""
hermes/charts/provider.py — Chart snapshot provider for the Hermes vision layer.

Fetches OHLCV bars from TimescaleDB (bars_daily), renders a dark-theme
candlestick chart with SMA20/50, Bollinger Bands, RSI, and volume, then
returns raw PNG bytes.

The HermesOverseer calls `provider.snapshot(symbol)` before each LLM call
when vision_enabled=True.  The PNG bytes are coerced to a base64 data-URL
by `_image_to_data_url()` in llm/clients.py and embedded in the user message.

Usage:
    provider = HermesChartProvider(db, lookback_days=60)
    provider.start()                    # warms up SPY chart in background
    png_bytes = provider.snapshot("AAPL")   # returns cached PNG or None
"""
from __future__ import annotations

import io
import logging
import time
import threading
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("hermes.charts")


# ---------------------------------------------------------------------------
# Technical indicators
# ---------------------------------------------------------------------------
def _sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n).mean()


def _bollinger(series: pd.Series, n: int = 20, k: float = 2.0):
    mid = series.rolling(n).mean()
    std = series.rolling(n).std()
    return mid + k * std, mid, mid - k * std


def _rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(n).mean()
    loss = (-delta.clip(upper=0)).rolling(n).mean()
    rs = gain / loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


# ---------------------------------------------------------------------------
# Chart renderer
# ---------------------------------------------------------------------------
def render_chart(df: pd.DataFrame, symbol: str, lookback: int = 210) -> bytes:
    """Render a dark-theme candlestick chart with overlays. Returns PNG bytes.

    Raises RuntimeError if matplotlib is not installed, ValueError if there
    are too few bars to render.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.gridspec import GridSpec
    except ImportError as exc:
        raise RuntimeError(
            "matplotlib not installed — add it to requirements.txt"
        ) from exc

    df = df.tail(lookback).copy()
    df.index = pd.to_datetime(df.index)
    n = len(df)
    if n < 5:
        raise ValueError(
            f"Too few bars for {symbol}: need ≥5, got {n}"
        )

    x = np.arange(n)
    closes  = df["close"].values.astype(float)
    opens   = df["open"].values.astype(float)
    highs   = df["high"].values.astype(float)
    lows    = df["low"].values.astype(float)
    volumes = df["volume"].values.astype(float) if "volume" in df.columns else np.zeros(n)

    sma20 = _sma(df["close"], 20).values
    sma50 = _sma(df["close"], min(50, n - 1)).values
    bb_upper, bb_mid, bb_lower = [s.values for s in _bollinger(df["close"])]
    rsi_vals = _rsi(df["close"]).values

    # ── Figure layout: price | volume | RSI ─────────────────────────────────
    fig = plt.figure(figsize=(13, 7), facecolor="#0d1117")
    gs  = GridSpec(3, 1, height_ratios=[4, 1, 1.4], hspace=0.06, figure=fig)
    ax_price = fig.add_subplot(gs[0])
    ax_vol   = fig.add_subplot(gs[1], sharex=ax_price)
    ax_rsi   = fig.add_subplot(gs[2], sharex=ax_price)

    for ax in (ax_price, ax_vol, ax_rsi):
        ax.set_facecolor("#161b22")
        ax.tick_params(colors="#8b949e", labelsize=8)
        ax.spines[:].set_color("#30363d")
        ax.grid(color="#30363d", linewidth=0.4, linestyle="--", alpha=0.5)

    GREEN = "#3fb950"
    RED   = "#f85149"
    W_BODY = 0.55
    W_WICK = 0.10

    # ── Candlesticks ─────────────────────────────────────────────────────────
    for i in range(n):
        col   = GREEN if closes[i] >= opens[i] else RED
        body_lo = min(opens[i], closes[i])
        body_hi = max(opens[i], closes[i])
        ax_price.bar(x[i], body_hi - body_lo, bottom=body_lo,
                     width=W_BODY, color=col, linewidth=0)
        ax_price.bar(x[i], highs[i] - body_hi, bottom=body_hi,
                     width=W_WICK, color=col, linewidth=0)
        ax_price.bar(x[i], body_lo - lows[i],  bottom=lows[i],
                     width=W_WICK, color=col, linewidth=0)

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    ax_price.fill_between(x, bb_upper, bb_lower,
                          alpha=0.07, color="#bc8cff")
    ax_price.plot(x, bb_upper, color="#bc8cff", linewidth=0.7,
                  linestyle="--", label="BB(20,2)")
    ax_price.plot(x, bb_lower, color="#bc8cff", linewidth=0.7,
                  linestyle="--")

    # ── Moving averages ───────────────────────────────────────────────────────
    ax_price.plot(x, sma20, color="#58a6ff", linewidth=1.3,
                  label="SMA20", zorder=3)
    ax_price.plot(x, sma50, color="#d29922", linewidth=1.3,
                  label="SMA50", zorder=3)

    # ── Last-price annotation ─────────────────────────────────────────────────
    last = closes[-1]
    ax_price.axhline(last, color="#8b949e", linewidth=0.5, linestyle=":")
    ax_price.annotate(f"  ${last:.2f}", xy=(n - 1, last),
                      color="#e6edf3", fontsize=8, va="center")

    # Derive approximate month span from trading-day count (≈21 days/month)
    month_span = round(n / 21)
    span_label = f"~{month_span}mo" if month_span > 1 else f"{n}d"
    ax_price.set_title(
        f"{symbol}  ·  {span_label} daily candlestick  ·  SMA20/50  ·  BB(20,2)  ·  RSI(14)",
        color="#e6edf3", fontsize=10, fontweight="bold", pad=8
    )
    ax_price.legend(fontsize=7, facecolor="#161b22", edgecolor="#30363d",
                    labelcolor="#8b949e", loc="upper left")
    ax_price.set_ylabel("Price ($)", color="#8b949e", fontsize=8)
    plt.setp(ax_price.get_xticklabels(), visible=False)

    # ── Volume ────────────────────────────────────────────────────────────────
    vol_colors = [GREEN if closes[i] >= opens[i] else RED for i in range(n)]
    ax_vol.bar(x, volumes, color=vol_colors, alpha=0.65, linewidth=0)
    ax_vol.set_ylabel("Vol", color="#8b949e", fontsize=7)
    plt.setp(ax_vol.get_xticklabels(), visible=False)

    # ── RSI ───────────────────────────────────────────────────────────────────
    ax_rsi.plot(x, rsi_vals, color="#e3771e", linewidth=1.2)
    ax_rsi.axhline(70, color=RED,   linewidth=0.7, linestyle="--")
    ax_rsi.axhline(50, color="#30363d", linewidth=0.5)
    ax_rsi.axhline(30, color=GREEN, linewidth=0.7, linestyle="--")
    ax_rsi.fill_between(x, rsi_vals, 70,
                        where=(rsi_vals >= 70), alpha=0.13, color=RED)
    ax_rsi.fill_between(x, rsi_vals, 30,
                        where=(rsi_vals <= 30), alpha=0.13, color=GREEN)
    ax_rsi.set_ylim(0, 100)
    ax_rsi.set_ylabel("RSI(14)", color="#8b949e", fontsize=7)

    # X-axis date labels
    step   = max(1, n // 8)
    ticks  = list(range(0, n, step))
    labels = [df.index[i].strftime("%b %d") for i in ticks]
    ax_rsi.set_xticks(ticks)
    ax_rsi.set_xticklabels(labels, fontsize=7, color="#8b949e")

    # Watermark
    fig.text(0.99, 0.01,
             f"Hermes · {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
             ha="right", va="bottom", color="#30363d", fontsize=7)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight",
                facecolor=fig.get_facecolor(), dpi=120)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------
class HermesChartProvider:
    """Render a chart snapshot for any symbol in the watchlist.

    Pulls bars from the Hermes DB (bars_daily), renders via matplotlib,
    caches results for `cache_ttl_s` seconds to avoid re-rendering on
    every tick.  Thread-safe.

    Call `.start(symbols)` once after construction to warm up the cache
    for the watchlist in a background thread.

    Usage
    -----
        provider = HermesChartProvider(db)
        provider.start(["AAPL", "SPY", "QQQ"])   # background warm-up
        png_bytes = provider.snapshot("AAPL")     # returns bytes or None
    """

    def __init__(self, db, lookback_days: int = 210, cache_ttl_s: int = 300):
        self.db           = db
        self.lookback     = lookback_days
        self.cache_ttl_s  = cache_ttl_s
        self._lock        = threading.Lock()
        # {symbol: (monotonic_time, png_bytes)}
        self._cache: Dict[str, Tuple[float, bytes]] = {}
        # Latest AI analysis per symbol: {symbol: {"verdict":…, "rationale":…, "ts":…}}
        self._analysis: Dict[str, dict] = {}

    def start(self, symbols) -> None:
        """Warm up the cache for `symbols` in a background thread."""
        def _warm():
            for sym in symbols:
                try:
                    self._render_and_cache(sym)
                except Exception as exc:                         # noqa: BLE001
                    logger.debug("Chart warm-up failed for %s: %s", sym, exc)
        t = threading.Thread(target=_warm, name="chart-warmup", daemon=True)
        t.start()

    def snapshot(self, symbol: str) -> Optional[bytes]:
        """Return cached PNG bytes, re-rendering if stale. Returns None on failure."""
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and (now - cached[0]) < self.cache_ttl_s:
                return cached[1]
        return self._render_and_cache(symbol)

    def _render_and_cache(self, symbol: str) -> Optional[bytes]:
        try:
            bars = self.db.daily_bars(symbol, lookback_days=self.lookback + 10)
            if bars is None or bars.empty:
                logger.warning("No bars for %s — chart skipped", symbol)
                return None
            png = render_chart(bars, symbol, lookback=self.lookback)
            with self._lock:
                self._cache[symbol] = (time.monotonic(), png)
            logger.debug("Chart rendered for %s: %d bytes", symbol, len(png))
            return png
        except Exception as exc:                                 # noqa: BLE001
            logger.warning("Chart render failed for %s: %s", symbol, exc)
            return None

    def record_analysis(self, symbol: str, verdict: str,
                        rationale: str, features: dict | None = None) -> None:
        """Store the latest LLM chart analysis for a symbol (used by the API)."""
        with self._lock:
            self._analysis[symbol] = {
                "verdict":   verdict,
                "rationale": rationale,
                "features":  features or {},
                "ts":        datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }

    def latest_analysis(self, symbol: str) -> Optional[dict]:
        with self._lock:
            return self._analysis.get(symbol)

    def all_analyses(self) -> dict:
        with self._lock:
            return dict(self._analysis)
