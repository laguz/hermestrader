import pytest
from datetime import datetime
from transitions.core import MachineError
from hermes.db.models import Trade

def test_trade_fsm_initial_state():
    trade = Trade(strategy_id="CS75", symbol="SPY", side_type="call", lots=1)
    assert trade.status == "PROPOSED"

def test_trade_fsm_valid_transitions():
    trade = Trade(strategy_id="CS75", symbol="SPY", side_type="call", lots=1)
    
    # Simulate happy path
    trade.submit_to_broker()
    assert trade.status == "PENDING_BROKER"
    
    trade.partial_fill()
    assert trade.status == "PARTIAL_FILL"
    
    trade.fill()
    assert trade.status == "OPEN"
    
    trade.begin_close()
    assert trade.status == "CLOSING"
    
    trade.finish_close()
    assert trade.status == "CLOSED"

def test_trade_fsm_invalid_transition():
    trade = Trade(strategy_id="CS75", symbol="SPY", side_type="call", lots=1)
    
    # Should not be able to fill an order that hasn't been submitted to broker
    with pytest.raises(MachineError):
        trade.fill()

def test_trade_fsm_force_close():
    # Force close should work from any state (e.g. reconciler finding an orphan)
    trade = Trade(strategy_id="CS75", symbol="SPY", side_type="call", lots=1)
    trade.force_close()
    assert trade.status == "CLOSED"

    trade2 = Trade(strategy_id="CS75", symbol="SPY", status="OPEN")
    trade2.force_close()
    assert trade2.status == "CLOSED"
