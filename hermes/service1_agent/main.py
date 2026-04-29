"""
[Service-1: Hermes-Agent-Core] — Entry point.
Wires broker → DB → strategies → cascading engine → overseer, then ticks
on a schedule. Runs as its own process.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict

from hermes.db.models import HermesDB
from hermes.service1_agent.core import CascadingEngine, IronCondorBuilder, MoneyManager
from hermes.service1_agent.overseer import HermesOverseer
from hermes.service1_agent.strategies import (
    CreditSpreads7, CreditSpreads75, TastyTrade45, WheelStrategy,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("hermes.agent.main")


def build(broker, llm_client, chart_provider, config: Dict[str, Any]) -> CascadingEngine:
    db = HermesDB(os.environ.get("HERMES_DSN",
                                 "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
    mm = MoneyManager(broker, db, config)
    ic = IronCondorBuilder(mm)

    overseer = HermesOverseer(
        llm_client=llm_client, db=db, vision_enabled=True,
        chart_provider=chart_provider,
        autonomy=config.get("ai_autonomy", "advisory"),
    )

    common = dict(broker=broker, db=db, money_manager=mm, ic_builder=ic,
                  config=config, overseer=overseer,
                  dry_run=config.get("dry_run", False))
    strategies = [
        CreditSpreads75(**common),
        CreditSpreads7(**common),
        TastyTrade45(**common),
        WheelStrategy(**common),
    ]
    return CascadingEngine(broker, db, strategies, overseer=overseer)


def run(broker, llm_client, chart_provider, config: Dict[str, Any]) -> None:
    engine = build(broker, llm_client, chart_provider, config)
    watchlist = config["watchlist"]
    interval_s = int(config.get("tick_interval_s", 300))
    log.info("Hermes Agent started; %d strategies", len(engine.strategies))
    while True:
        try:
            stats = engine.tick(watchlist)
            log.info("tick complete: %s", stats)
        except Exception as exc:                                       # noqa: BLE001
            log.exception("tick failed: %s", exc)
        time.sleep(interval_s)


def _build_broker(conf: Dict[str, Any]):
    """Use the real Tradier broker when credentials are present, else fall back to the mock."""
    if os.environ.get("TRADIER_ACCESS_TOKEN") and os.environ.get("TRADIER_ACCOUNT_ID"):
        from hermes.broker.tradier import TradierBroker
        log.info("Initializing TradierBroker (base=%s, dry_run=%s)",
                 os.environ.get("TRADIER_BASE_URL", "https://api.tradier.com/v1"),
                 conf.get("dry_run"))
        return TradierBroker(conf)
    from hermes.service1_agent.mock_broker import MockBroker
    log.warning("TRADIER_ACCESS_TOKEN/TRADIER_ACCOUNT_ID not set — using MockBroker")
    return MockBroker(conf)


if __name__ == "__main__":
    from hermes.service1_agent.mock_broker import MockLLM

    conf = {
        "watchlist": os.environ.get("HERMES_WATCHLIST", "AAPL,SPY,QQQ").split(","),
        "min_obp_reserve": float(os.environ.get("HERMES_MIN_OBP_RESERVE", 5000.0)),
        "ai_autonomy": os.environ.get("HERMES_AI_AUTONOMY", "advisory"),
        "tick_interval_s": int(os.environ.get("HERMES_TICK_INTERVAL", 300)),
        "dry_run": os.environ.get("HERMES_DRY_RUN", "true").lower() == "true",
    }

    broker = _build_broker(conf)
    llm = MockLLM()
    charts = None

    run(broker, llm, charts, conf)
