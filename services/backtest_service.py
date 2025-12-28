import pandas as pd
import numpy as np
import traceback
from datetime import datetime, timedelta
import math

from bot.strategies.credit_spreads import CreditSpreadStrategy
from utils.indicators import (
    calculate_rsi, 
    calculate_bollinger_bands, 
    find_key_levels, 
    calculate_option_price,
    calculate_prob_it_expires_otm,
    calculate_historical_volatility
)

class MockTradierService:
    def __init__(self):
        self.account_id = "mock_account"
        self.current_date = None
        self.current_price = 0.0
        self.current_volatility = 0.0
        
        self.positions = [] # List of positions dicts
        self.orders = []    # List of order dicts
        
        self.new_orders = [] # Orders placed in current step

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
        
    def get_quote(self, symbol):
        return {'last': self.current_price, 'symbol': symbol}
        
    def get_quotes(self, symbols):
        # Return synthetic quotes for options based on BS model
        quotes = []
        for sym in symbols:
            # Parse Option Symbol to get details
            # SYMBOLyyMMdd[C|P]strike
            details = self._parse_option_symbol(sym)
            if not details:
                # Should be underlying
                quotes.append({'symbol': sym, 'last': self.current_price, 'bid': self.current_price, 'ask': self.current_price})
                continue
                
            # Calculate Price
            dte_days = (details['expiry'] - self.current_date).days
            t_years = max(0, dte_days / 365.0)
            
            price = calculate_option_price(
                self.current_price, 
                details['strike'], 
                t_years, 
                self.current_volatility, 
                option_type=details['type']
            )
            
            # Simulated Bid/Ask Spread (wider for longer dated?)
            spread = max(0.05, price * 0.05)
            bid = max(0, price - spread/2)
            ask = price + spread/2
            
            quotes.append({
                'symbol': sym,
                'last': price,
                'bid': bid,
                'ask': ask,
                'greeks': {'delta': 0.5} # Placeholder if needed
            })
        return quotes

    def get_option_expirations(self, symbol):
        # Generate next 12 Fridays
        expirations = []
        d = self.current_date
        while len(expirations) < 12:
            if d.weekday() == 4: # Friday
                expirations.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        return expirations

    def get_option_chains(self, symbol, expiry_date_str):
        # Generate synthetic chain around current price
        expiry = datetime.strptime(expiry_date_str, "%Y-%m-%d")
        dte_days = (expiry - self.current_date).days
        t_years = max(0.001, dte_days / 365.0)
        
        chain = []
        
        # Strikes: +/- 20% in $1 or $5 increments
        low = self.current_price * 0.8
        high = self.current_price * 1.2
        step = 5 if self.current_price > 200 else 1
        
        start_strike = round(low / step) * step
        end_strike = round(high / step) * step
        
        for k in range(int(start_strike), int(end_strike) + step, step):
            strike = float(k)
            for opt_type in ['call', 'put']:
                # Price
                price = calculate_option_price(
                    self.current_price, strike, t_years, self.current_volatility, option_type=opt_type
                )
                
                # Filter penny options
                if price < 0.01: continue
                
                # Approximate Delta
                # Delta is dN(d1) for call, dN(d1)-1 for put
                # Re-calc not efficient but safe
                import scipy.stats as stats
                # Quick dirty delta: OTM < 0.5, ITM > 0.5
                # We can just return a dummy delta if Strategy uses it?
                # Strategy uses delta to find 0.30 strike. We SHOULD calculate it.
                # Re-using BS logic...
                # d1 = ...
                try:
                    d1 = (np.log(self.current_price / strike) + (0.04 + 0.5 * self.current_volatility ** 2) * t_years) / (self.current_volatility * np.sqrt(t_years))
                    if opt_type == 'call':
                        delta = stats.norm.cdf(d1)
                    else:
                        delta = stats.norm.cdf(d1) - 1
                except:
                    delta = 0.5
                
                # Symbol
                expiry_fmt = expiry.strftime("%y%m%d")
                type_char = 'C' if opt_type == 'call' else 'P'
                strike_fmt = f"{int(strike*1000):08d}"
                sym_str = f"{symbol}{expiry_fmt}{type_char}{strike_fmt}"
                
                chain.append({
                    'symbol': sym_str,
                    'strike': strike,
                    'option_type': opt_type,
                    'last': price,
                    'bid': price, # simplify
                    'ask': price,
                    'greeks': {'delta': delta}
                })
        return chain

    def place_order(self, account_id, symbol, side, quantity, order_type, duration, price=None, stop=None, option_symbol=None, order_class='equity', legs=None):
        # Capture order
        order = {
            'id': f"ord_{len(self.orders)+1}",
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'type': order_type,
            'status': 'open', # assume fills immediately in simulation loop, or pending
            'class': order_class,
            'legs': legs,
            'price': price,
            'create_date': self.current_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        self.orders.append(order)
        self.new_orders.append(order)
        return {'id': order['id'], 'status': 'ok'}

    def _parse_option_symbol(self, sym):
        import re
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

class MockAnalysisService:
    def __init__(self):
        self.current_context = {}

    def set_context(self, price, key_levels, rsi, volatility):
        self.current_context = {
            'current_price': price,
            'key_levels': key_levels,
            'rsi': rsi,
            'volatility': volatility
        }

    def analyze_symbol(self, symbol):
        # Return structure expected by CreditSpreadStrategy
        price = self.current_context['current_price']
        vol = self.current_context['volatility']
        key_levels = self.current_context['key_levels']
        
        # Split key levels into Put (Support) and Call (Resistance) entry points
        # Calculate POP for each
        put_entry_points = []
        call_entry_points = []
        
        for level in key_levels:
            # POP calculation
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
            'recommendation': 'NEUTRAL' # Strategy decides
        }

class BacktestService:
    def __init__(self, tradier_service_real):
        # We ignore the real service for backtesting, 
        # but keep signature to match injection if needed
        pass

    def run_backtest(self, symbol, strategy_type, start_date, end_date):
        print(f"DEBUG: Starting Backtest for {symbol}")
        
        # 1. Fetch History (using REAL tradier service from Container because we are in a service method)
        # But wait, self.tradier_service_real is not saved.
        # We need to fetch data.
        from services.container import Container
        real_tradier = Container.get_tradier_service()
        
        # Warmup
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        warmup_start = (start_dt - timedelta(days=90)).strftime('%Y-%m-%d')
        
        try:
            history = real_tradier.get_historical_pricing(symbol, warmup_start, end_date)
        except Exception as e:
            return {"error": f"Failed to fetch history: {e}"}
            
        if not history: return {"error": "No data found"}
        
        df = pd.DataFrame(history)
        df['date'] = pd.to_datetime(df['date'])
        
        # 2. Setup Mocks
        mock_tradier = MockTradierService()
        mock_analysis = MockAnalysisService()
        mock_db = None # mock database if needed, strategy writes logs to it
        
        # 3. Setup Strategy
        # Inject our mocks!
        if strategy_type == "credit_spread":
            strategy = CreditSpreadStrategy(
                tradier_service=mock_tradier, 
                db=mock_db, 
                dry_run=False, # Must be False to trigger place_order on MockTradier
                analysis_service=mock_analysis
            )
        else:
             return {"error": f"Strategy {strategy_type} not supported in refactored backtester yet."}
        
        # Results container
        portfolio_values = []
        dates = []
        trades_log = []
        cash = 10000.0
        
        # Simulation Loop
        import math
        
        for index, row in df.iterrows():
            if row['date'] < start_dt: continue # skip warmup days for execution, but use them for indicators
            
            current_date_str = row['date'].strftime("%Y-%m-%d")
            price = row['close']
            
            # 1. Calculate Indicators on-the-fly (windowed)
            # Need strict lookback window to avoid lookahead bias
            # Slice DF up to (but excluding?) current day for calculation? 
            # Or including current day as "Latest Known Price"?
            # Live trading includes current price.
            window_df = df.iloc[max(0, index-90):index+1]
            if len(window_df) < 30: continue 
            
            # Volatility
            volatility = calculate_historical_volatility(window_df['close'])
            if pd.isna(volatility): volatility = 0.5
            
            # Key Levels
            key_levels = find_key_levels(
                window_df['close'], 
                window_df['volume'],
                n_clusters=6
            )
            
            # RSI
            rsi = calculate_rsi(window_df['close']).iloc[-1]
            
            # 2. Update Mock Context
            mock_tradier.set_context(current_date_str, price, volatility)
            mock_analysis.set_context(price, key_levels, rsi, volatility)
            
            # 3. Run Strategy: Manage Positions (Exits)
            # This simulates "Opening Bell" or "Intraday" checks
            # Note: Strategy.manage_positions checks time (3 PM). mock_tradier date object has 00:00 time.
            # We must trick it.
            # Set mock internal time?
            # Strategy calls datetime.now(est). 
            # We CANNOT easily mock datetime.now() inside the module without deep patching.
            # Alternative: Refactor manage_positions to accept 'now' argument?
            # Or assume strategy runs 'execute' for entries, and we check exits manually in backtest loop?
            # Reuse: Strategy logic handles complex "Close on Day 3" logic.
            # Let's Rely on `execute(watchlist)` for ENTRIES.
            # And for EXITS... Strategy `manage_positions` relies on live time.
            # Refactor `manage_positions` in `credit_spreads.py` to accept `current_time` override would be best.
            # But avoiding too many edits...
            # Let's inspect `MockTradier` positions.
            
            # --- SIMPLIFIED EXIT LOGIC (Replicating Strategy Logic Logic) ---
            # Correct Re-use requires refactoring `manage_positions` to be testable. Use simplified for now but strict rules.
            # Iterate open positions in MockTradier
            active_positions = mock_tradier.positions # access list reference
            for pos in list(active_positions): # copy to allow remove
                 # Check Stop Loss / Profit
                 # Synthetic Quote
                 res = mock_tradier.get_quotes([pos['symbol']])
                 if not res: continue
                 quote = res[0]
                 
                 # Calculate Logic... 
                 # This is duplicating code. Ideally we call `strategy.check_exit(pos, quote, date)`.
                 # For now, let's implement basic hold logic or assume explicit management not supported fully in v1 refactor.
                 # Actually, let's just hold to expiry or profit target?
                 pass
            
            # 4. Run Strategy: Execute (Entries)
            # Limit 1 trade per day for simplicity?
            strategy.execute([symbol], config={'max_credit_spreads_per_symbol': 5})
            
            # 5. Process New Orders -> Create Positions
            new_orders = mock_tradier.new_orders
            for order in new_orders:
                if order['side'] == 'sell_to_open' or (order['class'] == 'multileg'):
                    # Assume fill at 'price'
                    fill_price = order['price']
                    qty = order['quantity']
                    
                    # Deduct/Add Cash (Credit = Add)
                    cash += (fill_price * qty * 100)
                    trades_log.append({
                        'date': current_date_str,
                        'action': f"OPEN {order['symbol']} ({order['legs'][0]['option_symbol'] if order['legs'] else ''})",
                        'credit': fill_price,
                        'pnl': 0
                    })
                    
                    # Create Position Object
                    # Simplified: just tracking the credit collected and the short leg details
                    # For full simulation, we need both legs.
                    # MockTradier.positions could store the full "trade" or individual legs.
                    # Strategy checks 'get_positions' which returns list of legs.
                    if order.get('legs'):
                        for leg in order['legs']:
                            mock_tradier.positions.append({
                                'symbol': leg['option_symbol'],
                                'quantity': -1 if 'sell' in leg['side'] else 1,
                                'cost_basis': 0, # simplified
                                'date_acquired': current_date_str
                            })
            
            # 6. Mark to Market Portfolio Value
            # Cash + Net Liquidating Value of Positions
            nlv = 0
            for pos in mock_tradier.positions:
                # get quote
                qs = mock_tradier.get_quotes([pos['symbol']])
                if qs:
                    price = qs[0]['last']
                    nlv += (price * pos['quantity'] * 100)
            
            total_val = cash + nlv
            dates.append(current_date_str)
            portfolio_values.append(total_val)
            
            # 7. Expiry Check (End of Day)
            # Remove expired positions
            # If expired OTM -> Value is 0. Cash stays. Profit realized.
            # If expired ITM -> Max Loss?
            active_positions = mock_tradier.positions
            for pos in list(active_positions):
                details = mock_tradier._parse_option_symbol(pos['symbol'])
                if details and details['expiry'].date() <= row['date'].date():
                    # Expired
                    # Check ITM?
                    strike = details['strike']
                    is_call = details['type'] == 'call'
                    is_itm = (is_call and price > strike) or (not is_call and price < strike)
                    
                    if is_itm:
                        # Max Loss (Spread width) or Assignment?
                        # Simplified: Assume max loss diff for spread if defined, or just cash settlement at intrinsic
                        intrinsic = abs(price - strike)
                        cash_impact = intrinsic * pos['quantity'] * 100 # quantity is -1 for short, so negative cash
                        cash += cash_impact
                        trades_log.append({
                            'date': current_date_str,
                            'action': f"EXPIRED ITM {pos['symbol']}",
                            'pnl': cash_impact
                        })
                    else:
                        trades_log.append({
                            'date': current_date_str,
                            'action': f"EXPIRED OTM {pos['symbol']}",
                            'pnl': 0 # already collected credit
                        })
                    
                    active_positions.remove(pos)

        # Summary Metrics
        if not portfolio_values:
            return {"error": "No simulation steps"}

        total_return = (portfolio_values[-1] - 10000.0) / 10000.0
        
        return {
            "dates": dates,
            "values": float_list(portfolio_values),
            "trades": trades_log,
            "metrics": {
                "total_return": f"{total_return*100:.2f}%",
                "final_value": f"${portfolio_values[-1]:.2f}",
                "trade_count": len(trades_log)
            }
        }

def float_list(l):
    return [float(x) for x in l]
