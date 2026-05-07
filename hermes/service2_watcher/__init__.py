"""Service-2 — the Hermes operator panel (Watcher / C2).

Process entry point: ``hermes/service2_watcher/api.py``. FastAPI app that
reads the same TimescaleDB the agent writes and exposes a control panel:

- Approve / reject / view the trade approval queue
- Edit the agent's "soul" (free-text doctrine appended to LLM prompts)
- Toggle individual strategies on/off, pause the agent
- Switch paper ↔ live mode (the agent reconciles next tick)
- Configure the LLM overseer backend (mock / local OpenAI-compat / Ollama Cloud)
- Surface agent / Tradier / LLM health status

The panel is intentionally read-mostly. The two writes that drive agent
behaviour are approval decisions and ``system_settings`` updates; the
agent picks them up at the start of the next tick.

Static dashboard assets live in ``hermes/service2_watcher/static/``.
"""
