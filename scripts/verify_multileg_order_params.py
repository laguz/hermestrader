import sys
import os
sys.path.append(os.getcwd())

from unittest.mock import MagicMock, patch
from services.tradier_service import TradierService

def test_multileg_order_params():
    # Mock requests to avoid actual network call
    with patch('requests.post') as mock_post:
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'id': '12345', 'status': 'ok'}
        mock_post.return_value = mock_response

        # Initialize service
        service = TradierService(access_token='mock_token', account_id='mock_account')

        # Test params for a Credit Spread
        legs = [
            {'option_symbol': 'TEST230120P00100000', 'side': 'sell_to_open', 'quantity': 1},
            {'option_symbol': 'TEST230120P00095000', 'side': 'buy_to_open', 'quantity': 1}
        ]
        
        print("Testing Credit Spread Order Placement...")
        service.place_order(
            account_id='mock_account',
            symbol='TEST',
            side='sell', # dummy
            quantity=1,
            order_type='credit', # The fix we made
            duration='day',
            price=0.50,
            order_class='multileg',
            legs=legs
        )

        # Verify arguments passed to requests.post
        args, kwargs = mock_post.call_args
        data = kwargs['data']
        
        print(f"Payload sent: {data}")
        
        if data.get('type') == 'credit' and data.get('class') == 'multileg':
            print("✅ SUCCESS: Order type 'credit' correctly passed for multileg.")
        else:
            print(f"❌ FAILURE: Expected type='credit', class='multileg'. Got type='{data.get('type')}', class='{data.get('class')}'")

        # Reset mock
        mock_post.reset_mock()

        # Test Roll (Debit)
        print("\nTesting Roll Order Placement (Debit)...")
        service.place_order(
            account_id='mock_account',
            symbol='TEST',
            side='buy',
            quantity=1,
            order_type='debit', # The fix for rolls
            duration='day',
            price=0.90,
            order_class='multileg',
            legs=legs
        )
        
        args, kwargs = mock_post.call_args
        data = kwargs['data']
        print(f"Payload sent: {data}")

        if data.get('type') == 'debit' and data.get('class') == 'multileg':
            print("✅ SUCCESS: Order type 'debit' correctly passed for multileg.")
        else:
            print(f"❌ FAILURE: Expected type='debit', class='multileg'. Got type='{data.get('type')}', class='{data.get('class')}'")

if __name__ == "__main__":
    test_multileg_order_params()
