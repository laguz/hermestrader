from dotenv import load_dotenv
load_dotenv()
from services.container import Container
from bot.strategies.credit_spreads import CreditSpreadStrategy
import pprint

def debug_limits():
    print("--- Debugging Trade Constraints ---")
    tradier = Container.get_tradier_service()
    
    # We need to simulate the strategy context
    # Passing None for DB as we just want to test the checker logic (mostly)
    # But wait, does it rely on DB? Only for logging.
    strategy = CreditSpreadStrategy(tradier, None, dry_run=False)
    
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
        # Order symbol is underlying for multileg
        underlyings.add(o.get('symbol'))
        
    print(f"Underlyings found: {underlyings}")
    
    # Run check for each
    for symbol in underlyings:
        print(f"\n🔎 Checking Limits for {symbol}...")
        
        # Check Puts
        print(f"  [PUTS]")
        # We invoke the internal method. 
        # Note: _check_expiry_constraints is what calculates the counts.
        # It calls get_positions/orders internally. 
        # That's inefficient if we call it in loop here, but mirrors the bot.
        
        # We'll temporarily override strategy.tradier.get_positions/orders to use cached 
        # or just let it fetch (Sandbox is fast). Let's let it fetch.
        
        # We need to hook into the logic that produces the LOGS about counts.
        # Since _check_expiry_constraints returns 'full_expiries', we can see if it returns anything.
        
        full_puts = strategy._check_expiry_constraints(symbol, is_put=True)
        print(f"    Full Put Expiries (>= 5 lots): {full_puts}")
        
        print(f"  [CALLS]")
        full_calls = strategy._check_expiry_constraints(symbol, is_put=False)
        print(f"    Full Call Expiries (>= 5 lots): {full_calls}")

if __name__ == "__main__":
    debug_limits()
