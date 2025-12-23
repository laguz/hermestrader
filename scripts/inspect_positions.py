import sys
import os
sys.path.append(os.getcwd())

from services.container import Container
from flask import Flask

# Need flask app context for container if it uses current_app (it doesn't seem to for basic services but good practice)
app = Flask(__name__)

def inspect_positions():
    with app.app_context():
        # Load env vars if needed? App usually loads them. 
        # But we are running script. 
        # Assuming .env is loaded or env vars present.
        # Container.get_tradier_service checks os.getenv.
        
        from dotenv import load_dotenv
        load_dotenv()
        
        tradier = Container.get_tradier_service()
        
        print("Fetching positions...")
        positions = tradier.get_positions()
        print(f"Total Positions: {len(positions)}")
        
        for i, p in enumerate(positions):
            print(f"\n--- Position {i+1} ---")
            print(p)
            
            # Check Critical Fields
            qty = p.get('quantity')
            op_type = p.get('option_type')
            
            print(f"DEBUG CHECK:")
            print(f"  Quantity: {qty} (Type: {type(qty)})")
            print(f"  Option Type: {op_type}")
            
            is_short = False
            if isinstance(qty, (int, float)) and qty < 0: is_short = True
            
            is_option = op_type in ['put', 'call']
            
            print(f"  Is Short (< 0)? {is_short}")
            print(f"  Is Option/Call? {is_option}")
            print(f"  COUNTS AS SPREAD LEG? {is_short and is_option}")

        print("\nFetching Orders...")
        orders = []
        try:
             orders = tradier.get_orders()
        except: pass
        print(f"Total Orders: {len(orders)}")
        for x in orders[:5]:
             print(x)


if __name__ == "__main__":
    inspect_positions()
