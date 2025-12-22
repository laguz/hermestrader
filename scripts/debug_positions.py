from services.container import Container
import pprint
from dotenv import load_dotenv
load_dotenv()

def debug_positions():
    tradier = Container.get_tradier_service()
    try:
        positions = tradier.get_positions()
        print("--- Raw Positions ---")
        pprint.pprint(positions)
        
        print("\n--- Filtered for RIOT ---")
        riot_pos = [p for p in positions if p.get('symbol') == 'RIOT' or p.get('underlying') == 'RIOT']
        pprint.pprint(riot_pos)
        
        print("\n--- Analysis ---")
        for p in riot_pos:
            qty = p.get('quantity')
            sym = p.get('symbol')
            print(f"Symbol: {sym}, Qty: {qty} ({type(qty)}), Type: {p.get('option_type')}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_positions()
