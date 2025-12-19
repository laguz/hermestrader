import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.tradier_service import TradierService

class TestTradingAPI(unittest.TestCase):
    def setUp(self):
        self.service = TradierService(access_token='mock_token', account_id='mock_account')

    @patch('services.tradier_service.requests.get')
    def test_get_expirations(self, mock_get):
        # Mock Response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'expirations': {'date': ['2023-01-20', '2023-02-17']}
        }
        mock_get.return_value = mock_response

        # Test
        exps = self.service.get_option_expirations('SPY')
        print(f"Expirations: {exps}")
        
        self.assertEqual(exps, ['2023-01-20', '2023-02-17'])
        mock_get.assert_called_with(
            'https://sandbox.tradier.com/v1/markets/options/expirations', 
            params={'symbol': 'SPY'}, 
            headers={'Authorization': 'Bearer mock_token', 'Accept': 'application/json'}
        )

    @patch('services.tradier_service.requests.get')
    def test_get_chain(self, mock_get):
        # Mock Response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'options': {'option': [
                {
                    'symbol': 'SPY230120C00400000', 
                    'strike': 400.0, 
                    'option_type': 'call', 
                    'last': 5.20,
                    'greeks': {'delta': 0.54}
                },
                {
                    'symbol': 'SPY230120P00390000', 
                    'strike': 390.0, 
                    'option_type': 'put', 
                    'last': 4.50,
                    'greeks': {'delta': -0.32}
                }
            ]}
        }
        mock_get.return_value = mock_response

        # Test
        chain = self.service.get_option_chains('SPY', '2023-01-20')
        print(f"Chain item count: {len(chain)}")
        print(f"First item delta: {chain[0].get('greeks', {}).get('delta')}")
        
        self.assertEqual(len(chain), 2)
        self.assertEqual(chain[0]['symbol'], 'SPY230120C00400000')
        self.assertEqual(chain[0]['greeks']['delta'], 0.54)
        mock_get.assert_called_with(
            'https://sandbox.tradier.com/v1/markets/options/chains', 
            params={'symbol': 'SPY', 'expiration': '2023-01-20', 'greeks': 'true'}, 
            headers={'Authorization': 'Bearer mock_token', 'Accept': 'application/json'}
        )

if __name__ == '__main__':
    unittest.main()
