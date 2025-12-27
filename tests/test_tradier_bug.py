import unittest
from unittest.mock import MagicMock, patch
from services.tradier_service import TradierService

class TestTradierBug(unittest.TestCase):
    def setUp(self):
        self.tradier = TradierService(access_token="test", account_id="test")

    @patch('requests.get')
    def test_get_gainloss_handle_null_string(self, mock_get):
        # Symulate the API returning "null" or some string structure for gainloss
        # This mirrors the user's error: AttributeError: 'str' object has no attribute 'get'
        # which happens when gl_data is a string, and we try to call .get() on it.
        
        # Structure that causes the error:
        # data = {'gainloss': "some_string"}
        # gl_data = data.get('gainloss') -> "some_string"
        # gl_data.get('closed_position') -> AttributeError
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'gainloss': "null"} # Or just "null" string
        mock_get.return_value = mock_response

        # This should now safely return empty list
        positions = self.tradier.get_gainloss()
        self.assertEqual(positions, [])
        print("Test passed: Safely handled null gainloss string.")

if __name__ == '__main__':
    unittest.main()
