
import sys
import os
sys.path.append(os.getcwd())

from services.container import Container
from dotenv import load_dotenv
import json

load_dotenv()

def debug_analysis(symbol):
    print(f"Debugging Analysis for {symbol}...")
    
    # Mock Tradier if needed or use real? Real is better for data check.
    # Assuming real creds are loaded.
    
    service = Container.get_analysis_service()
    analysis = service.analyze_symbol(symbol)
    
    if not analysis or 'error' in analysis:
        print(f"Error: {analysis}")
        return

    current_price = analysis['current_price']
    print(f"Current Price: {current_price}")
    
    print("\n--- Support Levels (Put Entry) ---")
    put_entries = analysis.get('put_entry_points', [])
    put_entries.sort(key=lambda x: x['price'], reverse=True)
    
    for ep in put_entries:
        price = ep['price']
        pop = ep.get('pop', 'N/A')
        
        relevant = price < current_price
        pop_ok = False
        if isinstance(pop, (int, float)):
             pop_ok = 55 <= pop <= 70
        
        status = "✅" if relevant and pop_ok else "❌"
        if not relevant: status += " (Price >= Current)"
        elif not pop_ok: status += f" (POP {pop} not in 55-70)"
            
        print(f"Strike: {price} | POP: {pop}% | {status}")

    print("\n--- Resistance Levels (Call Entry) ---")
    call_entries = analysis.get('call_entry_points', [])
    for ep in call_entries:
        price = ep['price']
        pop = ep.get('pop', 'N/A')
        print(f"Strike: {price} | POP: {pop}%")

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "SPY"
    debug_analysis(target)
