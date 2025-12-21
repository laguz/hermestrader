
import sys
import os
sys.path.append(os.getcwd())

from services.container import Container
from dotenv import load_dotenv
from bot.strategies.wheel import WheelStrategy
from datetime import date, datetime

load_dotenv()

class DebugWheel(WheelStrategy):
    def _log(self, message):
        print(f"[DEBUG_WHEEL] {message}")

def deep_debug_put(symbol):
    print(f"\n⚡️ Deep Debugging Put Logic for {symbol} ⚡️")
    
    tradier = Container.get_tradier_service()
    db = Container.get_db()
    analysis_service = Container.get_analysis_service()
    
    bot = DebugWheel(tradier, db, dry_run=True)
    
    # 1. State Check (Manual)
    print("--- 1. State Check ---")
    positions = tradier.get_positions()
    bot._process_symbol(symbol, positions, analysis_service)
    
    # 2. Force Entry Logic Check (Even if state was skipped above, we run this manually)
    print("\n--- 2. Forcing Entry Logic Execution ---")
    analysis = analysis_service.analyze_symbol(symbol)
    current_price = analysis.get('current_price')
    print(f"Current Price: {current_price}")
    
    # Run helper manually to see internals
    print("--- Internal Steps ---")
    
    # Expiry
    expiry = bot._find_expiry(symbol, weeks=6)
    print(f"Target Expiry (6w): {expiry}")
    
    # Support
    put_entries = analysis.get('put_entry_points', [])
    print(f"Found {len(put_entries)} Support Levels")
    
    # Filter
    valid_supports = [
        ep for ep in put_entries 
        if ep['price'] < current_price and bot.MIN_POP <= ep.get('pop', 0) <= bot.MAX_POP
    ]
    print(f"Valid Supports (Price < {current_price} & POP 55-70):")
    for vs in valid_supports:
        print(f"  • {vs}")
        
    if not valid_supports:
        print("  ❌ No valid supports found.")
        
        # Delta Check
        print("--- Delta Fallback Check ---")
        if not expiry:
            print("  ❌ No expiry, cannot fetch chain.")
            return

        print(f"Fetching Chain for {expiry}...")
        chain = tradier.get_option_chains(symbol, expiry)
        if not chain:
            print(f"  ❌ No chain found for {expiry}")
        else:
            print(f"  Chain Length: {len(chain)}")
            target_strike, delta = bot._find_delta_strike(chain, 'put', bot.DELTA_MIN, bot.DELTA_MAX)
            
            # Print Candidates
            candidates = []
            for opt in chain:
                if opt['option_type'] == 'put':
                    d = opt.get('greeks', {}).get('delta')
                    if d:
                        candidates.append((opt['strike'], abs(d)))
            
            candidates.sort(key=lambda x: x[0])
            print("  Delta Candidates (Strike, AbsDelta):")
            for c in candidates:
                 marker = "✅" if 0.30 <= c[1] <= 0.37 else ""
                 if marker: print(f"    {c[0]} : {c[1]} {marker}")
            
            if target_strike:
                print(f"  ✅ Selected Delta Strike: {target_strike} (Delta {delta})")
            else:
                print(f"  ❌ No Delta Strike found in range 0.30-0.37")

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "SPY"
    deep_debug_put(target)
