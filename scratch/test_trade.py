import os
import logging
from hermes.broker.tradier import TradierBroker
from hermes.service1_agent.core import TradeAction

# Setup logging to see the request/response details
logging.basicConfig(level=logging.INFO)

def test_dry_run_trade():
    # Load configuration from environment
    # Ensure TRADIER_ACCESS_TOKEN and TRADIER_ACCOUNT_ID are set in your shell or .env
    config = {
        "dry_run": True, # CRITICAL: This ensures no real money is spent
    }
    
    try:
        broker = TradierBroker(config)
        print(f"--- Broker initialized (Mode: {'Dry Run' if broker.dry_run else 'LIVE'}) ---")
        
        # 1. Test Connectivity: Fetch account balances
        balances = broker.get_account_balances()
        print(f"Connected! Account Equity: ${balances.get('total_equity', 0)}")
        
        # 2. Test Market Data: Get a quote for SPY
        quote = broker.get_quote("SPY")
        last_price = quote[0].get('last')
        print(f"Market Data OK: SPY last price is ${last_price}")
        
        # 3. Test Trade Placement (Dry Run)
        # We'll create a dummy equity order for 1 share of SPY
        action = TradeAction(
            strategy_id="manual_test",
            symbol="SPY",
            order_class="equity",
            side="buy",
            quantity=1,
            order_type="market",
            duration="day",
            tag="HERMES_TEST"
        )
        
        print("\nSubmitting DRY RUN order...")
        response = broker.place_order_from_action(action)
        
        if "order" in response:
            order_info = response["order"]
            print(f"SUCCESS! Tradier accepted the preview.")
            print(f"Status: {order_info.get('status')}")
            print(f"Commission estimate: ${order_info.get('commission', 0)}")
        else:
            print("Response received, but format unexpected:")
            print(response)
            
    except Exception as e:
        print(f"\nFAILED: {e}")

if __name__ == "__main__":
    test_dry_run_trade()
