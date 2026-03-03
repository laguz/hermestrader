import os
from dotenv import load_dotenv

# Load env before importing services so they pick up the values
load_dotenv()

from services.tradier_service import TradierService

def run():
    access_token = os.environ.get('TRADIER_ACCESS_TOKEN')
    account_id = os.environ.get('TRADIER_ACCOUNT_ID')
    
    if not access_token or not account_id:
        print("Missing TRADIER_ACCESS_TOKEN or TRADIER_ACCOUNT_ID!")
        return
        
    service = TradierService()
    
    print("Fetching positions...")
    positions = service.get_positions()
    print(f"Positions: {positions}")
    print(f"Positions type: {type(positions)}")

    print("\nFetching orders...")
    orders = service.get_orders()
    print(f"Orders: {orders}")
    print(f"Orders type: {type(orders)}")

if __name__ == '__main__':
    run()
