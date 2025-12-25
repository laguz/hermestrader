from dotenv import load_dotenv
load_dotenv()
from services.container import Container
from bot.strategies.credit_spreads import CreditSpreadStrategy
import pprint

def debug_limits():
    print("--- Debugging Trade Constraints ---")
    tradier = Container.get_tradier_service()
    
    # We need to simulate the strategy context
    db = Container.get_db()
    strategy = CreditSpreadStrategy(tradier, db, dry_run=False)
    
    # Fetch Config
    config = {}
    if db is not None:
        bot_config = db.bot_config.find_one({"_id": "main_bot"}) or {}
        config = bot_config.get('settings', {})
    
    max_c_spreads = int(config.get('max_credit_spreads_per_symbol', 5))
    max_wheel = int(config.get('max_wheel_contracts_per_symbol', 1))
    
    print(f"DEBUG: Max Credit Spreads = {max_c_spreads}")
    print(f"DEBUG: Max Wheel Contracts = {max_wheel}")

    # Get positions and orders to print summary first
    print("Fetching LIVE Data...")
    positions = tradier.get_positions() or []
    orders = tradier.get_orders() or []
    
    print(f"Total Positions: {len(positions)}")
    print(f"Total Orders: {len(orders)}")
    
    # Group by Underlying to find the problematic one
    underlyings = set()
    for p in positions:
        u = strategy._get_underlying_from_pos(p)
        if u: underlyings.add(u)
    for o in orders:
        underlyings.add(o.get('symbol'))  # Symbol is underlying for multileg
        
    print(f"Underlyings found: {underlyings}")
    
    # Instantiate Wheel Strategy too
    from bot.strategies.wheel import WheelStrategy
    wheel_strategy = WheelStrategy(tradier, db, dry_run=False)

    # Run check for each
    for symbol in underlyings:
        print(f"\n🔎 Checking Limits for {symbol}...")
        
        # --- CREDIT SPREADS ---
        print(f"  [CREDIT SPREADS]")
        full_puts = strategy._check_expiry_constraints(symbol, is_put=True, max_lots=max_c_spreads)
        full_calls = strategy._check_expiry_constraints(symbol, is_put=False, max_lots=max_c_spreads)
        
        if full_puts: print(f"    ⚠️ Put Spread Expiries Full (>= {max_c_spreads}): {full_puts}")
        else: print(f"    ✅ Put Spreads OK (< {max_c_spreads})")
            
        if full_calls: print(f"    ⚠️ Call Spread Expiries Full (>= {max_c_spreads}): {full_calls}")
        else: print(f"    ✅ Call Spreads OK (< {max_c_spreads})")

        # --- WHEEL STRATEGY ---
        print(f"  [WHEEL STRATEGY]")
        # Wheel check_expiry_constraints returns list of full expiries
        full_wheel = wheel_strategy._check_expiry_constraints(symbol, max_lots=max_wheel)
        if full_wheel: print(f"    ⚠️ Wheel Expiries Full (>= {max_wheel}): {full_wheel}")
        else: print(f"    ✅ Wheel Contracts OK (< {max_wheel})")

if __name__ == "__main__":
    debug_limits()
