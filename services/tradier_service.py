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

    def get_option_expirations(self, symbol):
        """Fetch option expirations for a given symbol."""
        url = f"{self.endpoint}/markets/options/expirations"
        params = {'symbol': symbol}
        try:
            response = requests.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            # Tradier returns {'expirations': {'date': ['2023-01-01', ...]}}
            # or just a list if only one? Sandbox behavior can vary.
            # Usually it's date list.
            exps = data.get('expirations', {}).get('date', [])
            if isinstance(exps, str):
                return [exps]
            return exps
        except requests.RequestException as e:
            print(f"Error fetching expirations for {symbol}: {e}")
            return []

    def get_option_chains(self, symbol, expiration):
        """Fetch option chains for a given symbol and expiration date."""
        url = f"{self.endpoint}/markets/options/chains"
        params = {'symbol': symbol, 'expiration': expiration, 'greeks': 'true'}
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

    def get_account_balances(self):
        """Fetch account balances including total equity and option buying power."""
        url = f"{self.endpoint}/accounts/{self.account_id}/balances"
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            balances = data.get('balances', {})
            
            # Buying power can be nested under 'pdt', 'margin', or 'cash'
            # Priority: pdt > margin > cash
            account_type = balances.get('account_type')
            sub_account = balances.get('pdt') or balances.get('margin') or balances.get('cash') or {}
            
            option_bp = sub_account.get('option_buying_power')
            stock_bp = sub_account.get('stock_buying_power')
            
            # Fallback for cash accounts
            if account_type == 'cash':
                 # In cash accounts, stock bp is usually cash available
                 if stock_bp is None:
                     stock_bp = balances.get('total_cash')
                 if option_bp is None:
                     option_bp = balances.get('total_cash')

            return {
                "total_equity": balances.get('total_equity'),
                "option_buying_power": option_bp,
                "stock_buying_power": stock_bp,
                "cash": balances.get('total_cash')
            }
        except requests.RequestException as e:
            print(f"Error fetching account balances: {e}")
            return None

    def get_positions(self):
        """Fetch open positions for the account."""
        url = f"{self.endpoint}/accounts/{self.account_id}/positions"
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            # positions can be None if empty, or a dict with 'position' list/dict
            positions_data = data.get('positions', {})
            if positions_data == 'null' or positions_data is None:
                return []
                
            position_entry = positions_data.get('position', [])
            if isinstance(position_entry, dict):
                return [position_entry]
            return position_entry
        except requests.RequestException as e:
            print(f"Error fetching positions: {e}")
            return []

    def check_connection(self):
        """Simple check to verify connectivity/auth by fetching a quote for SPY."""
        quote = self.get_quote('SPY')
        return quote is not None

    def place_order(self, account_id, symbol, side, quantity, order_type, duration='day', price=None, stop=None, option_symbol=None, order_class='equity', legs=None):
        """
        Place an order with Tradier.
        Params:
            account_id: str
            symbol: str (Underlying symbol)
            side: str ('buy', 'sell', 'buy_to_open', etc.)
            quantity: int
            order_type: str ('market', 'limit', 'stop', 'stop_limit')
            duration: str ('day', 'gtc')
            price: float (Required for limit orders)
            stop: float (Required for stop orders)
            option_symbol: str (Required for option orders)
            order_class: str ('equity', 'option', 'multileg', 'combo')
            legs: list of dicts [{'option_symbol': '...', 'side': '...', 'quantity': 1}, ...] (Required for multileg)
        """
        url = f"{self.endpoint}/accounts/{account_id}/orders"
        
        data = {
            'class': order_class,
            'symbol': symbol,
            'type': order_type,
            'duration': duration,
        }
        
        # For equity/option/combo, use top-level fields
        if order_class in ['equity', 'option', 'combo']:
            data['side'] = side
            data['quantity'] = quantity
            if option_symbol:
                data['option_symbol'] = option_symbol

        # For multileg, map legs to indexed fields
        if order_class == 'multileg' and legs:
            for i, leg in enumerate(legs):
                data[f'option_symbol[{i}]'] = leg.get('option_symbol')
                data[f'side[{i}]'] = leg.get('side')
                data[f'quantity[{i}]'] = leg.get('quantity')
            
        if price:
            data['price'] = price
            
        if stop:
            data['stop'] = stop
            
        try:
            print(f"Placing order: {data}")
            response = requests.post(url, data=data, headers=self._get_headers())
            
            if response.status_code not in [200, 201]:
                print(f"Order failed: {response.text}")
                return {'error': response.text} # Return error structure
            
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error placing order: {e}")
            return {'error': str(e)}
