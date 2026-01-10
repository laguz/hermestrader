import pandas as pd
import numpy as np

from datetime import datetime, timedelta


from bot.strategies.credit_spreads import CreditSpreadStrategy
from bot.strategies.wheel import WheelStrategy
from bot.strategies.credit_spread_rulebase import CreditSpreadRulebaseStrategy
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
        self.cash = 10000.0   # Default starting cash

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
            # Parse Option Symbol to get details
            # SYMBOLyyMMdd[C|P]strike
            details = self._parse_option_symbol(sym)
            if not details:
                # Should be underlying (Equity)
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
                import scipy.stats as stats
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
            'option_symbol': option_symbol,
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

    def analyze_symbol(self, symbol, period=None):
        # Return structure expected by CreditSpreadStrategy
        price = self.current_context['current_price']
        vol = self.current_context['volatility']
        key_levels = self.current_context['key_levels']
        
        # Split key levels into Put (Support) and Call (Resistance) entry points
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

class MockCollection:
    def __init__(self, data=None):
        self.data = data if data is not None else []

    def find(self, query):
        # ROI: Supports basic exact match query
        results = []
        for item in self.data:
            match = True
            for k, v in query.items():
                if item.get(k) != v:
                    match = False
                    break
            if match:
                results.append(item)
        return results

    def insert_one(self, document):
        if "_id" not in document:
            import uuid
            document["_id"] = str(uuid.uuid4())
        self.data.append(document)
        return type('obj', (object,), {'inserted_id': document["_id"]})

    def update_one(self, query, update):
        # ROI: Supports $set, $push
        # Find item
        item = None
        for i in self.data:
            match = True
            for k, v in query.items():
                if i.get(k) != v:
                    match = False
                    break
            if match:
                item = i
                break
        
        if item:
            if "$set" in update:
                for k, v in update["$set"].items():
                    item[k] = v
            if "$push" in update:
                for k, v in update["$push"].items():
                    # Handle $each and $slice if present (common in logging)
                    val = v
                    if isinstance(v, dict) and "$each" in v:
                        to_add = v["$each"]
                        if k not in item: item[k] = []
                        item[k].extend(to_add)
                        if "$slice" in v:
                            sl = v["$slice"]
                            if sl < 0:
                                item[k] = item[k][sl:]
                            else:
                                item[k] = item[k][:sl]
                    else:
                        if k not in item: item[k] = []
                        item[k].append(val)
        return None

class MockDB:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = MockCollection()
        return self.collections[name]

class BacktestService:
    def __init__(self, tradier_service_real):
        # We ignore the real service for backtesting, 
        # but keep signature to match injection if needed
        pass

    def run_backtest(self, symbol, strategy_type, start_date, end_date):
        print(f"DEBUG: Starting Backtest for {symbol}")
        
        # 1. Fetch History (using REAL tradier service from Container because we are in a service method)
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
        mock_db = MockDB() # Use MockDB!
        
        # 3. Setup Strategy
        # Inject our mocks!
        if strategy_type == "credit_spread":
            strategy = CreditSpreadStrategy(
                tradier_service=mock_tradier, 
                db=mock_db, 
                dry_run=False, # Must be False to trigger place_order on MockTradier
                analysis_service=mock_analysis
            )
        elif strategy_type == "wheel":
            strategy = WheelStrategy(
                tradier_service=mock_tradier,
                db=mock_db,
                dry_run=False,
                analysis_service=mock_analysis
            )
        elif strategy_type == "credit_spread_rulebase":
            strategy = CreditSpreadRulebaseStrategy(
                tradier_service=mock_tradier,
                db=mock_db,
                dry_run=False,
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
        
        for index, row in df.iterrows():
            if row['date'] < start_dt: continue # skip warmup days for execution, but use them for indicators
            
            current_date_str = row['date'].strftime("%Y-%m-%d")
            price = row['close']
            
            # 1. Calculate Indicators on-the-fly (windowed)
            window_df = df.iloc[max(0, index-90):index+1]
            if len(window_df) < 30: continue 
            
            # Volatility
            volatility = calculate_historical_volatility(window_df['close'])
            if pd.isna(volatility): volatility = 0.5
            
            # IV Proxy: Boost Volatility by 1.2x to simulate Implied Volatility (usually > HV)
            volatility = volatility * 1.2
            
            # Key Levels
            key_levels = find_key_levels(
                window_df['close'], 
                window_df['volume'],
                n_clusters=6
            )
            
            # RSI
            rsi = calculate_rsi(window_df['close']).iloc[-1]
            
            # 2. Update Mock Context
            mock_tradier.cash = cash # Sync cash
            mock_tradier.set_context(current_date_str, price, volatility)
            mock_analysis.set_context(price, key_levels, rsi, volatility)
            
            # 3. Run Strategy: Manage Positions (Exits)
            # This triggers checks for Profit Targets and Stop Losses, and Wheel rolls/calls
            # WheelStrategy also uses _manage_positions
            if strategy_type in ["credit_spread", "credit_spread_rulebase"]:
                strategy.manage_positions()
            elif strategy_type == "wheel":
                # Wheel strategy logic calls _manage_positions potentially in execute or we call it explicitly?
                # WheelStrategy doesn't have public check method. It checks in execute.
                # Actually, check code: _manage_positions() is internal.
                # Wheel usually checks existing positions first.
                # Let's verify WheelStrategy structure later. Assuming for now we call execute() which does both?
                # No, standard is distinct. We might need to call strategy.execute() which does management.
                pass 
                
            
            # Process Exits
            exit_orders = list(mock_tradier.new_orders) # Copy
            mock_tradier.new_orders = [] # Clear for next phase
            
            for order in exit_orders:
                if 'close' in order['side'] or 'buy' in order['side']: # buy_to_close, sell_to_close (or just buy market)
                     # Execute Close
                     # 1. Determine Fill Price
                     fill_price = order.get('price')
                     
                     # Market Order handling
                     if not fill_price: 
                         # Get current quote
                         fill_price = 0.0
                         if order.get('legs'):
                             for leg in order['legs']:
                                 opt_sym = leg['option_symbol']
                                 qs = mock_tradier.get_quotes([opt_sym])
                                 if qs:
                                     q = qs[0]
                                     leg_price = q['ask'] if 'buy' in leg['side'] else q['bid']
                                     fill_price += leg_price
                         else:
                             # Fallback non-multileg (Equity or Single Option)
                             qs = mock_tradier.get_quotes([order['symbol']])
                             if qs:
                                 fill_price = qs[0]['ask'] # Buying to close
                     
                     # 2. Apply Slippage (Debit = paying more)
                     slippage = 0.01 * (len(order.get('legs') or []) or 1)
                     final_price = fill_price + slippage
                     
                     cost = final_price * abs(order['quantity']) * 100
                     if order.get('class') == 'equity':
                         cost = final_price * abs(order['quantity']) # Equity is x1 not x100
                         
                     cash -= cost # Deduct cash for closing debit
                     
                     trades_log.append({
                        'date': current_date_str,
                        'action': f"CLOSE {order['symbol']}",
                        'debit': final_price,
                        'slippage': slippage,
                        'pnl': 0 # PnL is realized relative to entry. Hard to track here.
                    })
                    
                     # 3. Remove Position
                     if order.get('legs'):
                         for leg in order['legs']:
                             symbol_to_remove = leg['option_symbol']
                             for p in list(mock_tradier.positions):
                                 if p['symbol'] == symbol_to_remove:
                                     mock_tradier.positions.remove(p)
                                     break
                     else:
                         # Single leg removal
                         for p in list(mock_tradier.positions):
                                 if p['symbol'] == order['symbol']:
                                     mock_tradier.positions.remove(p)
                                     break
            
            
            # 4. Run Strategy: Execute (Entries)
            if strategy_type == "credit_spread":
                strategy.execute([symbol], config={'max_credit_spreads_per_symbol': 5})
            elif strategy_type == "credit_spread_rulebase":
                strategy.execute([symbol], config={'max_credit_spread_rulebase_lots': 5})
            elif strategy_type == "wheel":
                # Ensure Wheel runs logic
                strategy.execute([symbol])
            
            # 5. Process New Orders -> Create Positions (Entries)
            entry_orders = mock_tradier.new_orders
            for order in entry_orders:
                if order['side'] == 'sell_to_open' or (order['class'] == 'multileg'):
                    # Assume fill at 'price'. This is usually a Limit Order for Credit.
                    requested_price = order['price']
                    
                    # Slippage on Entry
                    slippage = 0.01 * (len(order.get('legs') or []) or 1)
                    fill_price = max(0, requested_price - slippage)
                    
                    qty = order['quantity']
                    
                    # Deduct/Add Cash (Credit = Add)
                    multiplier = 100
                    if order.get('class') == 'equity': multiplier = 1
                    
                    cash += (fill_price * qty * multiplier)
                    trades_log.append({
                        'date': current_date_str,
                        'action': f"OPEN {order['symbol']} ({order['legs'][0]['option_symbol'] if order['legs'] else ''})",
                        'credit': fill_price,
                        'slippage': slippage,
                        'pnl': 0
                    })
                    
                    # Create Position Object
                    if order.get('legs'):
                        for leg in order['legs']:
                            mock_tradier.positions.append({
                                'symbol': leg['option_symbol'],
                                'quantity': -1 if 'sell' in leg['side'] else 1,
                                'cost_basis': 0, 
                                'date_acquired': current_date_str
                            })
                    else:
                        # Single Leg / Equity
                         # Use option_symbol if available, else symbol (Equity)
                         pos_symbol = order.get('option_symbol') or order['symbol']
                         mock_tradier.positions.append({
                            'symbol': pos_symbol,
                            'quantity': -qty if 'sell' in order['side'] else qty,
                            'cost_basis': fill_price,
                            'date_acquired': current_date_str
                        })

            
            # 6. Mark to Market Portfolio Value
            # Cash + Net Liquidating Value of Positions
            nlv = 0
            for pos in mock_tradier.positions:
                # get quote
                qs = mock_tradier.get_quotes([pos['symbol']])
                if qs:
                    price_q = qs[0]['last']
                    multiplier = 100
                    # if equity, mult 1. Check if symbol is equity (no option parse)
                    if not mock_tradier._parse_option_symbol(pos['symbol']):
                         multiplier = 1
                    nlv += (price_q * pos['quantity'] * multiplier)
            
            total_val = cash + nlv
            dates.append(current_date_str)
            portfolio_values.append(total_val)
            
            # 7. Expiry Check (End of Day) - ASSIGNMENT LOGIC
            active_positions = mock_tradier.positions
            for pos in list(active_positions):
                details = mock_tradier._parse_option_symbol(pos['symbol'])
                if details and details['expiry'].date() <= row['date'].date():
                    # Expired
                    strike = details['strike']
                    is_call = details['type'] == 'call'
                    is_itm = (is_call and price > strike) or (not is_call and price < strike)
                    
                    if is_itm:
                        # ITM Assignment
                        # If Short Put -> Buy Stock at Strike
                        # If Short Call -> Sell Stock at Strike
                        
                        qty = pos['quantity'] # -1 for short
                        is_short = qty < 0
                        
                        if is_short and not is_call: # Short Put ITM
                             # ASSIGNMENT: Buy Stock
                             # 1. Close Option (Value 0 now, but we take delivery)
                             active_positions.remove(pos)
                             
                             # 2. Add Stock Position
                             num_shares = abs(qty) * 100
                             stock_cost = strike * num_shares
                             cash -= stock_cost # Buy shares
                             
                             mock_tradier.positions.append({
                                 'symbol': details['root'], # SPY
                                 'quantity': num_shares,
                                 'cost_basis': strike,
                                 'date_acquired': current_date_str
                             })
                             
                             trades_log.append({
                                'date': current_date_str,
                                'action': f"ASSIGNED (PUT) {pos['symbol']}: Bought {num_shares} {details['root']} @ {strike}",
                                'debit': strike,
                                'pnl': 0
                            })
                            
                        elif is_short and is_call: # Short Call ITM
                             # ASSIGNMENT: Sell Stock (Called Away)
                             # 1. Close Option
                             active_positions.remove(pos)
                             
                             # 2. Remove Stock Position (Assuming Covered Call)
                             # Find stock
                             shares_needed = abs(qty) * 100
                             # Reduce stock holding
                             stock_found = False
                             for sp in list(mock_tradier.positions):
                                 if sp['symbol'] == details['root'] and sp['quantity'] > 0:
                                     # Sell these shares
                                     # Simplified: Assume enough shares or go short
                                     # Taking shares away:
                                     cash += (strike * shares_needed) # Receive cash strike
                                     sp['quantity'] -= shares_needed
                                     if sp['quantity'] <= 0:
                                         mock_tradier.positions.remove(sp)
                                     
                                     stock_found = True
                                     trades_log.append({
                                        'date': current_date_str,
                                        'action': f"CALLED AWAY {pos['symbol']}: Sold {shares_needed} {details['root']} @ {strike}",
                                        'credit': strike,
                                        'pnl': 0
                                    })
                                     break
                             
                             if not stock_found:
                                 # Naked Call Assignment? (Short Stock)
                                 # Cash += Strike * Shares. Position = -Shares.
                                 cash += (strike * shares_needed)
                                 mock_tradier.positions.append({
                                     'symbol': details['root'],
                                     'quantity': -shares_needed,
                                     'cost_basis': strike,
                                     'date_acquired': current_date_str
                                 })

                        else:
                            # Long Option ITM ? Exercise?
                            # Backtest usually handles Shorts.
                            # Just Cash Settle for simplicity if Long.
                            intrinsic = abs(price - strike)
                            cash_impact = intrinsic * pos['quantity'] * 100
                            cash += cash_impact
                            trades_log.append({
                                'date': current_date_str,
                                'action': f"EXERCISED ITM {pos['symbol']}",
                                'pnl': cash_impact
                            })
                            active_positions.remove(pos)

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
