import os
import logging
from typing import Optional
import requests

logger = logging.getLogger(__name__)

# Default timeout for all HTTP requests (connect, read) in seconds
REQUEST_TIMEOUT = (5, 15)


class TradierService:
    def __init__(self, access_token: Optional[str] = None,
                 account_id: Optional[str] = None,
                 endpoint: Optional[str] = None):
        self.access_token = access_token or os.getenv('TRADIER_ACCESS_TOKEN')
        self.account_id = account_id or os.getenv('TRADIER_ACCOUNT_ID')
        self.endpoint = endpoint or os.getenv(
            'TRADIER_ENDPOINT', 'https://sandbox.tradier.com/v1'
        )
        
    def update_access_token(self, token: str) -> None:
        self.access_token = token

    def update_account_id(self, account_id: str) -> None:
        self.account_id = account_id

    def _get_headers(self) -> dict:
        auth_token = self.access_token
        
        try:
            from flask import session, has_request_context
            if has_request_context() and session.get('tradier_key'):
                auth_token = session['tradier_key']
        except ImportError:
            pass
            
        if not auth_token:
            try:
                from services.container import Container
                auth = Container.get_auth_service()
                if auth.get_api_key():
                    auth_token = auth.get_api_key()
            except Exception:
                pass
                
        if not auth_token:
            logger.warning("Tradier access_token is missing. Unauthorized error likely.")
            
        return {
            'Authorization': f'Bearer {auth_token or ""}',
            'Accept': 'application/json'
        }

    def get_quote(self, symbol: str) -> Optional[dict]:
        """Fetch real-time or delayed quote for a symbol."""
        url = f"{self.endpoint}/markets/quotes"
        params = {'symbols': symbol}
        try:
            response = requests.get(url, params=params,
                                    headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            return (data.get('quotes') or {}).get('quote', {})
        except requests.RequestException as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return None

    def get_option_expirations(self, symbol: str) -> list:
        """Fetch option expirations for a given symbol."""
        url = f"{self.endpoint}/markets/options/expirations"
        params = {'symbol': symbol}
        try:
            response = requests.get(url, params=params,
                                    headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            exps = (data.get('expirations') or {}).get('date', [])
            if isinstance(exps, str):
                return [exps]
            return exps
        except requests.RequestException as e:
            logger.error(f"Error fetching expirations for {symbol}: {e}")
            return []

    def get_option_chains(self, symbol: str, expiration: str) -> Optional[list]:
        """Fetch option chains for a given symbol and expiration date."""
        url = f"{self.endpoint}/markets/options/chains"
        params = {'symbol': symbol, 'expiration': expiration, 'greeks': 'true'}
        try:
            response = requests.get(url, params=params,
                                    headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            return (data.get('options') or {}).get('option', [])
        except requests.RequestException as e:
            logger.error(f"Error fetching option chains for {symbol} on {expiration}: {e}")
            return None

    def get_historical_pricing(self, symbol: str, start_date: str,
                               end_date: str, interval: str = 'daily') -> Optional[list]:
        """Fetch historical pricing data."""
        url = f"{self.endpoint}/markets/history"
        params = {
            'symbol': symbol,
            'start': start_date,
            'end': end_date,
            'interval': interval
        }
        try:
            response = requests.get(url, params=params,
                                    headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            return (data.get('history') or {}).get('day', [])
        except requests.RequestException as e:
            logger.error(f"Error fetching history for {symbol}: {e}")
            return None

    def _get_account_id(self) -> Optional[str]:
        acct_id = self.account_id
        try:
            from flask import session, has_request_context
            if has_request_context() and session.get('account_id'):
                acct_id = session['account_id']
        except ImportError:
            pass
            
        if not acct_id:
            import os
            acct_id = os.getenv('TRADIER_ACCOUNT_ID')
            
        return acct_id

    def get_account_balances(self) -> Optional[dict]:
        """Fetch account balances including total equity and option buying power."""
        current_account_id = self._get_account_id()
        if not current_account_id:
            logger.error("Cannot fetch account balances: account_id is missing.")
            return None
        url = f"{self.endpoint}/accounts/{current_account_id}/balances"
        try:
            response = requests.get(url, headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            balances = data.get('balances', {})
            
            # Buying power can be nested under 'pdt', 'margin', or 'cash'
            account_type = balances.get('account_type')
            sub_account = balances.get('pdt') or balances.get('margin') or balances.get('cash') or {}
            
            option_bp = sub_account.get('option_buying_power')
            stock_bp = sub_account.get('stock_buying_power')
            total_equity = balances.get('total_equity')
            total_cash = balances.get('total_cash')
            
            # Fallback for cash accounts
            if account_type == 'cash':
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
            logger.error(f"Error fetching account balances: {e}")
            return None

    def get_positions(self) -> list:
        """Fetch open positions for the account."""
        current_account_id = self._get_account_id()
        if not current_account_id:
            logger.error("Cannot fetch positions: account_id is missing.")
            return []
        url = f"{self.endpoint}/accounts/{current_account_id}/positions"
        try:
            response = requests.get(url, headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            positions_data = data.get('positions', {})
            if positions_data == 'null' or positions_data is None:
                return []
                
            position_entry = positions_data.get('position', [])
            if isinstance(position_entry, dict):
                return [position_entry]
            return position_entry
        except requests.RequestException as e:
            logger.error(f"Error fetching positions: {e}")
            return []

    def get_orders(self, page: int = 1, limit: int = 100) -> list:
        """Fetch orders for the account."""
        current_account_id = self._get_account_id()
        if not current_account_id:
            logger.error("Cannot fetch orders: account_id is missing.")
            return []
        url = f"{self.endpoint}/accounts/{current_account_id}/orders"
        params = {'page': page, 'limit': limit}
        try:
            response = requests.get(url, params=params,
                                    headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
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
            logger.error(f"Error fetching orders: {e}")
            return []

    def check_connection(self) -> bool:
        """Simple check to verify connectivity/auth by fetching a quote for SPY."""
        quote = self.get_quote('SPY')
        return quote is not None

    def place_order(self, account_id: str, symbol: str, side: str,
                    quantity: int, order_type: str, duration: str = 'day',
                    price: Optional[float] = None, stop: Optional[float] = None,
                    option_symbol: Optional[str] = None,
                    order_class: str = 'equity', legs: Optional[list] = None,
                    tag: Optional[str] = None) -> dict:
        """
        Place an order with Tradier.
        
        Args:
            account_id: str
            symbol: Underlying symbol
            side: 'buy', 'sell', 'buy_to_open', etc.
            quantity: Number of shares/contracts
            order_type: 'market', 'limit', 'stop', 'stop_limit'
            duration: 'day' or 'gtc'
            price: Required for limit orders
            stop: Required for stop orders
            option_symbol: Required for option orders
            order_class: 'equity', 'option', 'multileg', 'combo'
            legs: List of leg dicts for multileg orders
            tag: Optional tag for tracking
        """
        url = f"{self.endpoint}/accounts/{account_id}/orders"
        
        data = {
            'class': order_class,
            'symbol': symbol,
            'type': order_type,
            'duration': duration,
        }
        
        if tag:
            data['tag'] = tag

        if order_class in ['equity', 'option', 'combo']:
            data['side'] = side
            data['quantity'] = quantity
            if option_symbol:
                data['option_symbol'] = option_symbol

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
            logger.info(f"Placing order: {data}")
            response = requests.post(url, data=data,
                                     headers=self._get_headers(),
                                     timeout=REQUEST_TIMEOUT)
            
            if response.status_code not in [200, 201]:
                logger.error(f"Order failed: {response.text}")
                return {'error': response.text}
            
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error placing order: {e}")
            return {'error': str(e)}

    def get_clock(self) -> Optional[dict]:
        """Fetch market clock/status."""
        url = f"{self.endpoint}/markets/clock"
        try:
            response = requests.get(url, headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            return data.get('clock', {})
        except requests.RequestException as e:
            logger.error(f"Error fetching market clock: {e}")
            return None

    def get_gainloss(self, page: int = 1, limit: int = 100,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None,
                     symbol: Optional[str] = None) -> list:
        """Fetch realized gain/loss data from Tradier."""
        current_account_id = self._get_account_id()
        if not current_account_id:
            logger.error("Cannot fetch gain/loss: account_id is missing.")
            return []
            
        url = f"{self.endpoint}/accounts/{current_account_id}/gainloss"
        params = {
            'page': page,
            'limit': limit,
            'sortBy': 'closeDate',
            'sort': 'desc'
        }
        if start_date:
            params['start'] = start_date
        if end_date:
            params['end'] = end_date
        if symbol:
            params['symbol'] = symbol
        
        try:
            response = requests.get(url, params=params,
                                    headers=self._get_headers(),
                                    timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            gl_data = data.get('gainloss', {})
            if gl_data is None or not isinstance(gl_data, dict):
                return []
            
            positions = gl_data.get('closed_position', [])
            if isinstance(positions, dict):
                return [positions]
            return positions
        except requests.RequestException as e:
            logger.error(f"Error fetching gainloss: {e}")
            return []
