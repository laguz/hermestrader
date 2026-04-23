import os
import sys

# Mock Flask and Container to test TradierService in isolation
class MockContainer:
    @staticmethod
    def get_auth_service():
        return MockAuthService()

class MockAuthService:
    def get_api_key(self, mode='paper'):
        return f"mock_{mode}_key"
    def get_account_id(self, mode='paper'):
        return f"mock_{mode}_account"
    def get_endpoints(self, user_id):
        return {"paper": "https://paper.api", "live": "https://live.api"}

# Inject mocks
sys.modules['services.container'] = MockContainer
import services.tradier_service as ts

def test_tradier_service():
    print("Testing TradierService...")
    svc = ts.TradierService()
    
    # Default is paper
    print(f"Initial mode: {svc.get_trading_mode()}")
    print(f"Initial endpoint: {svc._get_endpoint()}")
    
    # Switch to live
    svc.set_trading_mode('live')
    print(f"Mode after switch: {svc.get_trading_mode()}")
    # Note: _get_endpoint uses flask.has_request_context() which will be False here
    # So it should fall back to env or init value
    print(f"Endpoint (no request context): {svc._get_endpoint()}")
    
    print("Test complete.")

if __name__ == "__main__":
    test_tradier_service()
