"""Unit tests for MoneyManager initialization and parsing.

Exercises constructor robustness and the complex regex/branching logic in
``sync_broker_orders``.
"""
from __future__ import annotations

import pytest
import logging
from hermes.service1_agent.core import MoneyManager
from ._stubs import StubBroker, StubDB

def test_init_stores_attributes():
    broker = StubBroker()
    db = StubDB()
    config = {"min_obp_reserve": 1000.0}
    mm = MoneyManager(broker, db, config)
    assert mm.broker is broker
    assert mm.db is db
    assert mm.config == config
    assert mm._broker_order_counts == {}

def test_init_handles_none_config():
    mm = MoneyManager(StubBroker(), StubDB(), None)
    assert mm.config == {}

@pytest.mark.parametrize("tag, expected_strat", [
    ("HERMES_CS75", "CS75"),
    ("HERMES-TT45", "TT45"),
    ("HERMES_WHEEL_v1", "WHEEL"),
    ("HERMES-CS7-2023-10-27", "CS7"),
])
def test_sync_extracts_strategy_id_correctly(tag, expected_strat):
    """Verify strategy ID extraction from various tag shapes."""
    orders = [{
        "status": "open",
        "tag": tag,
        "symbol": "AAPL",
        "quantity": 1,
        "option_symbol": "AAPL250620P00150000",
    }]
    mm = MoneyManager(StubBroker(orders=orders), StubDB(), {})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {(expected_strat, "AAPL", "put"): 1}

def test_sync_handles_missing_fields_gracefully():
    """Ensure sync doesn't crash on incomplete broker dictionaries."""
    malformed_orders = [
        {},  # empty
        {"status": "open"},  # no tag
        {"status": "open", "tag": "HERMES_CS75"},  # no symbol
        {"status": "open", "tag": "HERMES_CS75", "symbol": "AAPL"},  # no legs/opt_sym
    ]
    mm = MoneyManager(StubBroker(orders=malformed_orders), StubDB(), {})
    # Should not raise
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {}

def test_sync_handles_malformed_occ_symbols():
    orders = [{
        "status": "open",
        "tag": "HERMES_CS75",
        "symbol": "AAPL",
        "option_symbol": "INVALID_OCC",
    }]
    mm = MoneyManager(StubBroker(orders=orders), StubDB(), {})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {}

def test_sync_survives_broker_exception(caplog):
    broker = StubBroker()
    def raise_exc():
        raise RuntimeError("Tradier Down")
    broker.get_orders = raise_exc

    mm = MoneyManager(broker, StubDB(), {})
    with caplog.at_level(logging.ERROR):
        mm.sync_broker_orders()

    assert mm._broker_order_counts == {}
    assert "Failed to fetch broker orders for sync" in caplog.text

def test_sync_handles_leg_as_dict():
    """Tradier sometimes returns a single leg as a dict instead of a list of dicts."""
    orders = [{
        "status": "open",
        "tag": "HERMES_CS75",
        "symbol": "AAPL",
        "quantity": 2,
        "leg": {"option_symbol": "AAPL250620C00150000", "quantity": 2},
    }]
    mm = MoneyManager(StubBroker(orders=orders), StubDB(), {})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {("CS75", "AAPL", "call"): 2}

def test_sync_skips_empty_strategy_id():
    orders = [{
        "status": "open",
        "tag": "HERMES_", # prefix only
        "symbol": "AAPL",
        "option_symbol": "AAPL250620P00150000",
    }]
    mm = MoneyManager(StubBroker(orders=orders), StubDB(), {})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {}
