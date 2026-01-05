import os
import requests
from flask import current_app

class TradierService:
    def __init__(self, access_token=None, account_id=None, endpoint=None):
        self.access_token = access_token or os.getenv('TRADIER_ACCESS_TOKEN')
        self.account_id = account_id or os.getenv('TRADIER_ACCOUNT_ID')
        self.endpoint = endpoint or os.getenv('TRADIER_ENDPOINT', 'https://sandbox.tradier.com/v1')
        
        # Note: API Key might be missing on init if using Vault.
        
    def update_access_token(self, token):
        self.access_token = token

    def update_account_id(self, account_id):
        self.account_id = account_id

    def _get_headers(self):
        if not self.access_token:
            # Check if we can get it from AuthService (Singleton proxy)
            try:
                from services.container import Container
                auth = Container.get_auth_service()
                self.access_token = auth.get_api_key()
            except:
                pass
                
        if not self.access_token:
            print("WARNING: Tradier access_token is missing. Unauthorized error likely.")
            
        return {
            'Authorization': f'Bearer {self.access_token or ""}',
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
            return (data.get('quotes') or {}).get('quote', {})
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
            exps = (data.get('expirations') or {}).get('date', [])
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
            return (data.get('options') or {}).get('option', [])
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
            return (data.get('history') or {}).get('day', [])
        except requests.RequestException as e:
            print(f"Error fetching history for {symbol}: {e}")
            return None

    def get_account_balances(self):
        """Fetch account balances including total equity and option buying power."""
        if not self.account_id:
             print("Error fetching account balances: account_id is missing.")
             return None
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
            total_equity = balances.get('total_equity')
            total_cash = balances.get('total_cash')
            
            # Fallback for cash accounts
            if account_type == 'cash':
                 # In cash accounts, stock bp is usually cash available
                 if stock_bp is None:
                     stock_bp = total_cash
                 if option_bp is None:
                     option_bp = total_cash

            return {
                "total_equity": float(total_equity) if total_equity is not None else 0.0,
                "option_buying_power": float(option_bp) if option_bp is not None else 0.0,
                "stock_buying_power": float(stock_bp) if stock_bp is not None else 0.0,
                "cash": float(total_cash) if total_cash is not None else 0.0
            }
        except requests.RequestException as e:
            print(f"Error fetching account balances: {e}")
            return None

    def get_positions(self):
        """Fetch open positions for the account."""
        if not self.account_id:
            print("Error fetching positions: account_id is missing.")
            return []
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

    def get_orders(self, page=1, limit=100):
        """Fetch orders for the account."""
        if not self.account_id:
            print("Error fetching orders: account_id is missing.")
            return []
        url = f"{self.endpoint}/accounts/{self.account_id}/orders"
        params = {'page': page, 'limit': limit}
        try:
            response = requests.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            orders_data = data.get('orders', {})
            if orders_data == 'null' or orders_data is None:
                return []
            
            order_entry = orders_data.get('order', [])
            if isinstance(order_entry, dict):
                return [order_entry]
            return order_entry
        except requests.RequestException as e:
            print(f"Error fetching orders: {e}")
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

    def get_clock(self):
        """Fetch market clock/status."""
        url = f"{self.endpoint}/markets/clock"
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            return data.get('clock', {})
        except requests.RequestException as e:
            print(f"Error fetching market clock: {e}")
            return None

    def get_gainloss(self, page=1, limit=100, start_date=None, end_date=None, symbol=None):
        """
        Fetch realized gain/loss data from Tradier.
        useful for tracking closed positions.
        """
        url = f"{self.endpoint}/accounts/{self.account_id}/gainloss"
        params = {
            'page': page,
            'limit': limit,
            'sortBy': 'closeDate',
            'sort': 'desc'
        }
        if start_date: params['start'] = start_date
        if end_date: params['end'] = end_date
        if symbol: params['symbol'] = symbol
        
        try:
            response = requests.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            data = response.json()
            # Structure: {'gainloss': {'closed_position': [...]}}
            gl_data = data.get('gainloss', {})
            if gl_data is None or not isinstance(gl_data, dict):
                 return []
            
            positions = gl_data.get('closed_position', [])
            if isinstance(positions, dict):
                return [positions]
            return positions
        except requests.RequestException as e:
            print(f"Error fetching gainloss: {e}")
            return []
