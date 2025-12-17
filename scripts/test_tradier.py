import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.tradier_service import TradierService
from dotenv import load_dotenv

def main():
    load_dotenv()
    
    print("Testing Tradier API Connection...")
    token = os.getenv('TRADIER_API_KEY')
    account = os.getenv('TRADIER_ACCOUNT_ID')
    print(f"API Key present: {'Yes' if token else 'No'}")
    print(f"Account ID present: {'Yes' if account else 'No'}")
    
    service = TradierService()
    if service.check_connection():
        print("✅ Connection Successful!")
        quote = service.get_quote('SPY')
        print(f"SPY Quote: {quote}")
    else:
        print("❌ Connection Failed. Check your TRADIER_ACCESS_TOKEN and TRADIER_ENDPOINT (default: sandbox).")

if __name__ == "__main__":
    main()