import os
import requests
from flask import current_app

class TradierService:
    def __init__(self, access_token=None, account_id=None, endpoint=None):
        self.access_token = access_token or os.getenv('TRADIER_API_KEY')
        self.account_id = account_id or os.getenv('TRADIER_ACCOUNT_ID')
        self.endpoint = endpoint or os.getenv('TRADIER_ENDPOINT', 'https://sandbox.tradier.com/v1')
        
        if not self.access_token:
            print("Warning: TRADIER_API_KEY not found in environment variables.")
        if not self.account_id:
             print("Warning: TRADIER_ACCOUNT_ID not found in environment variables.")

    def _get_headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }

    def get_quote(self, symbol):
        """Fetch real-time or delayed quote for a symbol."""
        url = f"{self.endpoint}/markets/quotes"
        params = {'symbols': symbol}
        try:
            response = requests.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            # Tradier response structure for quotes can be nested
            return data.get('quotes', {}).get('quote', {})
        except requests.RequestException as e:
            print(f"Error fetching quote for {symbol}: {e}")
            return None

    def get_option_chains(self, symbol, expiration):
        """Fetch option chains for a given symbol and expiration date."""
        url = f"{self.endpoint}/markets/options/chains"
        params = {'symbol': symbol, 'expiration': expiration}
        try:
            response = requests.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            return data.get('options', {}).get('option', [])
        except requests.RequestException as e:
            print(f"Error fetching option chains for {symbol} on {expiration}: {e}")
            return None

    def get_historical_pricing(self, symbol, start_date, end_date, interval='daily'):
        """
        Fetch historical pricing data.
        Note: Sandbox might have limitations on historical data.
        """
        url = f"{self.endpoint}/markets/history"
        params = {
            'symbol': symbol,
            'start': start_date,
            'end': end_date,
            'interval': interval
        }
        try:
            response = requests.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            return data.get('history', {}).get('day', [])
        except requests.RequestException as e:
            print(f"Error fetching history for {symbol}: {e}")
            return None

    def check_connection(self):
        """Simple check to verify connectivity/auth by fetching a quote for SPY."""
        quote = self.get_quote('SPY')
        return quote is not None
