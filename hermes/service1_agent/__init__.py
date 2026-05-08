"""Service-1 — the Hermes trading agent.

Process entry point: ``hermes/service1_agent/main.py``. Runs a fixed-interval
tick loop that drives the cascading strategy engine. No HTTP surface;
all state goes through ``HermesDB``.

Module layout
-------------

- ``core``        — ``CascadingEngine``, ``MoneyManager``, ``IronCondorBuilder``,
                    ``AbstractStrategy``, ``TradeAction``. The orchestration
                    primitives every strategy is built on.
- ``strategies``  — Concrete strategies (``CreditSpreads75``, ``CreditSpreads7``,
                    ``TastyTrade45``, ``WheelStrategy``). Each declares a
                    ``PRIORITY`` that controls cascading order.
- ``overseer``    — ``HermesOverseer`` LLM review hook. Reviews every
                    ``TradeAction`` (advisory / enforcing / autonomous).
- ``main``        — Entry point + tick loop + config reconciliation
                    against the watcher's settings.
- ``mock_broker`` — ``MockBroker`` and ``MockLLM`` stand-ins for dev mode
                    (no Tradier credentials) and tests.

See ``ARCHITECTURE.md`` for how these fit together at runtime.
"""
