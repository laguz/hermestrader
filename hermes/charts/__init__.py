"""Chart rendering for the Hermes vision layer.

``provider.HermesChartProvider`` pulls OHLCV bars from ``bars_daily`` and
renders a dark-theme candlestick PNG with SMA20/50, Bollinger Bands, RSI,
and volume. The ``HermesOverseer`` calls ``provider.snapshot(symbol)``
before each LLM call when ``vision_enabled=True``; the bytes are coerced
into a base64 data-URL by ``hermes.llm.clients._image_to_data_url``.

Optional dependency: matplotlib. If it isn't installed the agent runs
without vision (charts disabled, overseer still works text-only).
"""
