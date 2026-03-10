import re
import uuid
import numpy as np
from scipy import stats
from datetime import datetime, timedelta

from utils.indicators import calculate_option_price, calculate_prob_it_expires_otm

class MockTradierService:
    def __init__(self):
        self.account_id = "mock_account"
        self.current_date = None
        self.current_price = 0.0
        self.current_volatility = 0.0
        
        self.positions = []  # List of positions dicts
        self.orders = []     # List of order dicts
        
        self.new_orders = []  # Orders placed in current step
        self.cash = 100000.0   # Default starting cash

    def set_context(self, date_str, price, volatility):
        # Set time to 15:30 to ensure manage_positions runs
        self.current_date = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=15, minute=30)
        self.current_price = price
        self.current_volatility = volatility
        self.new_orders = []

    def get_positions(self):
        return self.positions

    def get_orders(self):
        return self.orders

    def get_account_balances(self):
        return {
            'option_buying_power': self.cash
        }
        
    def get_quote(self, symbol):
        return {'last': self.current_price, 'symbol': symbol}
        
    def get_quotes(self, symbols):
        # Return synthetic quotes for options based on BS model
        quotes = []
        for sym in symbols:
            details = self._parse_option_symbol(sym)
            if not details:
                # Underlying (Equity)
                quotes.append({
                    'symbol': sym, 'last': self.current_price,
                    'bid': self.current_price, 'ask': self.current_price
                })
                continue
                
            # Calculate Price with skew
            dte_days = (details['expiry'] - self.current_date).days
            t_years = max(0, dte_days / 365.0)

            # Apply volatility skew for OTM options
            strike_vol = self._apply_vol_skew(
                self.current_volatility, details['strike'],
                self.current_price, details['type']
            )
            
            price = calculate_option_price(
                self.current_price, 
                details['strike'], 
                t_years, 
                strike_vol, 
                option_type=details['type']
            )
            
            # Simulated Bid/Ask Spread
            spread = max(0.05, price * 0.05)
            bid = max(0, price - spread / 2)
            ask = price + spread / 2
            
            quotes.append({
                'symbol': sym,
                'last': price,
                'bid': bid,
                'ask': ask,
                'greeks': {'delta': 0.5}  # Placeholder
            })
        return quotes

    def get_option_expirations(self, symbol):
        # Generate next 12 Fridays
        expirations = []
        d = self.current_date
        while len(expirations) < 12:
            if d.weekday() == 4:  # Friday
                expirations.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        return expirations

    def get_option_chains(self, symbol, expiry_date_str):
        # Generate synthetic chain around current price
        expiry = datetime.strptime(expiry_date_str, "%Y-%m-%d")
        dte_days = (expiry - self.current_date).days
        t_years = max(0.001, dte_days / 365.0)
        
        chain = []
        
        # Strikes: +/- 20% in $1 increments universally to support dynamic algorithm width logic
        low = self.current_price * 0.8
        high = self.current_price * 1.2
        step = 1
        
        start_strike = round(low / step) * step
        end_strike = round(high / step) * step
        
        for k in range(int(start_strike), int(end_strike) + step, step):
            strike = float(k)
            for opt_type in ['call', 'put']:
                # Apply volatility skew
                strike_vol = self._apply_vol_skew(
                    self.current_volatility, strike,
                    self.current_price, opt_type
                )

                price = calculate_option_price(
                    self.current_price, strike, t_years, strike_vol, option_type=opt_type
                )
                
                # Filter penny options
                if price < 0.01:
                    continue
                
                # Approximate Delta
                try:
                    d1 = (np.log(self.current_price / strike) + (0.04 + 0.5 * strike_vol ** 2) * t_years) / (strike_vol * np.sqrt(t_years))
                    if opt_type == 'call':
                        delta = stats.norm.cdf(d1)
                    else:
                        delta = stats.norm.cdf(d1) - 1
                except Exception:
                    delta = 0.5
                
                # Symbol
                expiry_fmt = expiry.strftime("%y%m%d")
                type_char = 'C' if opt_type == 'call' else 'P'
                strike_fmt = f"{int(strike*1000):08d}"
                sym_str = f"{symbol}{expiry_fmt}{type_char}{strike_fmt}"
                
                # Realistic bid/ask spread
                spread_width = max(0.05, price * 0.05)
                bid = max(0.01, price - spread_width / 2)
                ask = price + spread_width / 2

                chain.append({
                    'symbol': sym_str,
                    'strike': strike,
                    'option_type': opt_type,
                    'last': price,
                    'bid': round(bid, 2),
                    'ask': round(ask, 2),
                    'greeks': {'delta': delta}
                })
        return chain

    def place_order(self, account_id, symbol, side, quantity, order_type, duration,
                    price=None, stop=None, option_symbol=None, order_class='equity', legs=None, **kwargs):
        order = {
            'id': f"ord_{len(self.orders)+1}",
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'type': order_type,
            'status': 'open',
            'class': order_class,
            'legs': legs,
            'option_symbol': option_symbol,
            'price': price,
            'create_date': self.current_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        self.orders.append(order)
        self.new_orders.append(order)
        return {'id': order['id'], 'status': 'ok'}

    def _parse_option_symbol(self, sym):
        m = re.match(r'([A-Z]+)(\d{6})([CP])(\d+)', sym)
        if m:
            root, date_str, type_char, strike_str = m.groups()
            return {
                'root': root,
                'expiry': datetime.strptime(date_str, "%y%m%d"),
                'type': 'call' if type_char == 'C' else 'put',
                'strike': int(strike_str) / 1000.0
            }
        return None

    @staticmethod
    def _apply_vol_skew(base_vol, strike, spot, option_type):
        """
        Apply a simple volatility skew model.
        OTM puts get higher IV, OTM calls slightly lower.
        """
        moneyness = (spot - strike) / spot  # positive = OTM put, negative = OTM call
        if option_type == 'put':
            # OTM puts: boost vol significantly (real markets show strong put skew)
            skew = 1.0 + max(0, moneyness) * 2.5
        else:
            # OTM calls: slight vol reduction
            skew = 1.0 - max(0, -moneyness) * 0.3
        return base_vol * max(0.5, min(skew, 2.5))  # Clamp to reasonable range


class MockAnalysisService:
    def __init__(self):
        self.current_context = {}

    def set_context(self, price, key_levels, rsi, volatility, sma_200=None, hv_rank=50):
        self.current_context = {
            'current_price': price,
            'key_levels': key_levels,
            'rsi': rsi,
            'volatility': volatility,
            'sma_200': sma_200,
            'hv_rank': hv_rank
        }

    def analyze_symbol(self, symbol, period=None):
        price = self.current_context['current_price']
        vol = self.current_context['volatility']
        key_levels = self.current_context['key_levels']
        
        put_entry_points = []
        call_entry_points = []
        
        for level in key_levels:
            pop = calculate_prob_it_expires_otm(price, level['price'], vol, days_to_expiry=30) * 100
            point = {
                'price': level['price'],
                'type': level['type'],
                'strength': level['strength'],
                'pop': pop
            }
            if level['type'] == 'support':
                put_entry_points.append(point)
            elif level['type'] == 'resistance':
                call_entry_points.append(point)
                
        return {
            'symbol': symbol,
            'current_price': price,
            'rsi': self.current_context['rsi'],
            'put_entry_points': put_entry_points,
            'call_entry_points': call_entry_points,
            'recommendation': 'NEUTRAL',
            'indicators': {
                'rsi': self.current_context['rsi'],
                'hv_rank': self.current_context.get('hv_rank', 50),
                'sma_200': self.current_context.get('sma_200'),
                'historical_volatility': vol
            }
        }


class MockCollection:
    def __init__(self, data=None):
        self.data = data if data is not None else []

    def _match_query(self, item, query):
        """Check if an item matches a MongoDB-style query, supporting $regex."""
        for k, v in query.items():
            item_val = item.get(k)
            if isinstance(v, dict) and '$regex' in v:
                import re as _re
                if not item_val or not _re.search(v['$regex'], str(item_val)):
                    return False
            else:
                if item_val != v:
                    return False
        return True

    def find(self, query):
        return [item for item in self.data if self._match_query(item, query)]

    def count_documents(self, query):
        return len(self.find(query))

    def insert_one(self, document):
        if "_id" not in document:
            document["_id"] = str(uuid.uuid4())
        self.data.append(document)
        return type('obj', (object,), {'inserted_id': document["_id"]})

    def update_one(self, query, update):
        item = None
        for i in self.data:
            if self._match_query(i, query):
                item = i
                break
        
        if item:
            if "$set" in update:
                for k, v in update["$set"].items():
                    item[k] = v
            if "$push" in update:
                for k, v in update["$push"].items():
                    val = v
                    if isinstance(v, dict) and "$each" in v:
                        to_add = v["$each"]
                        if k not in item:
                            item[k] = []
                        item[k].extend(to_add)
                        if "$slice" in v:
                            sl = v["$slice"]
                            if sl < 0:
                                item[k] = item[k][sl:]
                            else:
                                item[k] = item[k][:sl]
                    else:
                        if k not in item:
                            item[k] = []
                        item[k].append(val)
        return None


class MockDB:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = MockCollection()
        return self.collections[name]
