import re
import logging
import uuid
import numpy as np
import pandas as pd
from scipy import stats
import math
import traceback

from datetime import datetime, timedelta

from bot.strategies.credit_spreads import CreditSpreadStrategy
from bot.strategies.wheel import WheelStrategy
from bot.strategies.credit_spread_rulebase import CreditSpreadRulebaseStrategy
from utils.indicators import (
    calculate_rsi, 
    find_key_levels, 
    calculate_option_price,
    calculate_prob_it_expires_otm,
    calculate_historical_volatility
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mock Services for Backtesting
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Backtest Engine
# ---------------------------------------------------------------------------

class BacktestService:
    # Default configuration
    COMMISSION_PER_CONTRACT = 0.50   # $0.50 per contract
    IV_HV_MULTIPLIER = 1.5          # IV proxy = HV * multiplier (IV typically > HV)
    RISK_FREE_RATE = 0.04           # 4% default

    def __init__(self, tradier_service_real):
        self.real_tradier = tradier_service_real

    def _generate_synthetic_history(self, symbol, start_date, end_date):
        """Generate synthetic price history using Geometric Brownian Motion (GBM).
        Used as fallback when Tradier API is unavailable."""
        # Approximate starting prices for common symbols
        default_prices = {
            'SPY': 450.0, 'QQQ': 380.0, 'IWM': 200.0,
            'AAPL': 190.0, 'MSFT': 370.0, 'TSLA': 250.0,
            'AMZN': 180.0, 'NVDA': 120.0, 'RIOT': 12.0,
        }
        start_price = default_prices.get(symbol, 100.0)
        
        # GBM parameters
        annual_return = 0.10   # 10% annualized drift
        annual_vol = 0.20      # 20% annualized volatility
        dt = 1.0 / 252.0       # Daily time step
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        history = []
        price = start_price
        current_dt = start_dt
        
        np.random.seed(42)  # Reproducible results
        
        while current_dt <= end_dt:
            # Skip weekends
            if current_dt.weekday() >= 5:
                current_dt += timedelta(days=1)
                continue
            
            # GBM step
            drift = (annual_return - 0.5 * annual_vol**2) * dt
            shock = annual_vol * np.sqrt(dt) * np.random.randn()
            price = price * np.exp(drift + shock)
            
            # Generate OHLCV
            daily_range = price * 0.015  # ~1.5% intraday range
            open_price = price + np.random.uniform(-daily_range/2, daily_range/2)
            high = max(price, open_price) + abs(np.random.normal(0, daily_range/3))
            low = min(price, open_price) - abs(np.random.normal(0, daily_range/3))
            volume = int(np.random.uniform(50_000_000, 150_000_000))
            
            history.append({
                'date': current_dt.strftime('%Y-%m-%d'),
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(price, 2),
                'volume': volume
            })
            
            current_dt += timedelta(days=1)
        
        logger.info(f"Generated {len(history)} synthetic data points for {symbol} "
                     f"(${start_price:.0f} → ${price:.2f})")
        return history

    def run_backtest(self, symbol, strategy_type, start_date, end_date,
                     commission=None, iv_multiplier=None, risk_free_rate=None, slippage_per_leg=0.01, risk_per_trade_pct=0.02):
        logger.info(f"Starting Backtest for {symbol} ({strategy_type}) {start_date} → {end_date}")

        # Apply configurable parameters
        commission_per = commission if commission is not None else self.COMMISSION_PER_CONTRACT
        iv_mult = iv_multiplier if iv_multiplier is not None else self.IV_HV_MULTIPLIER
        slippage_rate = slippage_per_leg
        risk_pct = risk_per_trade_pct
        
        # 1. Fetch History
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        warmup_start = (start_dt - timedelta(days=90)).strftime('%Y-%m-%d')
        
        history = None
        from services.container import Container
        db = Container.get_db()

        # Try MongoDB cache first
        if db is not None:
            try:
                cache_col = db['historical_prices']
                cached_doc = cache_col.find_one({"symbol": symbol, "start_date": warmup_start, "end_date": end_date})
                if cached_doc:
                    logger.info(f"Loaded historical data for {symbol} from MongoDB cache.")
                    history = cached_doc.get("data")
            except Exception as e:
                logger.warning(f"Failed to read from MongoDB cache: {e}")

        # If cache miss, fetch from API and cache it
        if not history:
            try:
                history = self.real_tradier.get_historical_pricing(symbol, warmup_start, end_date)
                if history and db is not None:
                    try:
                        cache_col = db['historical_prices']
                        cache_col.update_one(
                            {"symbol": symbol, "start_date": warmup_start, "end_date": end_date},
                            {"$set": {"data": history}},
                            upsert=True
                        )
                        logger.info(f"Saved historical data for {symbol} to MongoDB cache.")
                    except Exception as e:
                        logger.warning(f"Failed to write to MongoDB cache: {e}")
            except Exception as e:
                logger.warning(f"Failed to fetch history from Tradier: {e}")
            
        if not history:
            logger.warning(f"No Tradier data for {symbol}. Generating synthetic data.")
            history = self._generate_synthetic_history(symbol, warmup_start, end_date)
        
        if not history:
            return {"error": "No data found"}
        
        df = pd.DataFrame(history)
        df['date'] = pd.to_datetime(df['date'])
        
        # --- Pre-compute Vectorized Indicators ---
        logger.info("Pre-computing vectorized indicators for backtest speedup...")
        
        from utils.indicators import calculate_rsi
        df['rsi'] = calculate_rsi(df['close'])
        df['sma_200'] = df['close'].rolling(window=200, min_periods=1).mean()
        
        # Historical Volatility (20-day rolling std dev of log returns, annualized)
        df['log_return'] = np.log(df['close'] / df['close'].shift(1))
        df['hv'] = df['log_return'].rolling(window=20).std() * np.sqrt(252)
        
        # Create a rolling HV rank (percentile within last 252 days)
        # Using a rank function: % of days in the lookback window where HV was lower than today's HV
        df['hv_rank'] = df['hv'].rolling(window=252, min_periods=20).apply(
            lambda x: (x < x.iloc[-1]).sum() / len(x) * 100 if len(x) > 0 else 50, raw=False
        )
        
        # Basic forward-fill and defaults for early rows
        df['hv'] = df['hv'].fillna(0.5)
        df['hv_rank'] = df['hv_rank'].fillna(50.0)
        df['rsi'] = df['rsi'].fillna(50.0)
        
        # Key Level Caching Mechanism
        # K-Means clustering is CPU intensive. Rather than calculating 
        # Support/Resistance every single simulated day, we'll recalculate every 5 trading days.
        cached_key_levels = []
        days_since_last_level_calc = 999
        from utils.indicators import find_key_levels
        # -----------------------------------------
        
        # 2. Setup Mocks
        mock_tradier = MockTradierService()
        mock_analysis = MockAnalysisService()
        mock_db = MockDB()
        
        # 3. Setup Strategy (Dynamic Injection)
        strategy = None
        # Use lazy instantiation mapping instead of hardcoded if/elif block
        from bot.strategies.credit_spreads import CreditSpreadStrategy
        from bot.strategies.wheel import WheelStrategy
        from bot.strategies.credit_spread_rulebase import CreditSpreadRulebaseStrategy
        
        strategy_registry = {
            "credit_spread": CreditSpreadStrategy,
            "wheel": WheelStrategy,
            "credit_spread_rulebase": CreditSpreadRulebaseStrategy
        }
        
        strategy_class = strategy_registry.get(strategy_type)
        if not strategy_class:
            return {"error": f"Strategy '{strategy_type}' not supported in Backtester."}
            
        strategy = strategy_class(
            tradier_service=mock_tradier, 
            db=mock_db, 
            dry_run=False,
            analysis_service=mock_analysis
        )
        
        # Results containers
        portfolio_values = []
        dates = []
        trades_log = []
        starting_cash = 100000.0
        cash = starting_cash
        total_commissions = 0.0

        # Per-trade P&L tracking: maps position symbol → entry info
        open_trade_credits = {}  # symbol -> {'credit': float, 'date': str, 'contracts': int}
        
        # Buy & hold benchmark
        benchmark_values = []
        benchmark_start_price = None
        
        # Simulation Loop
        for index, row in df.iterrows():
            if row['date'] < start_dt:
                continue  # Skip warmup days
            
            current_date_str = row['date'].strftime("%Y-%m-%d")
            price = row['close']

            # Initialize benchmark
            if benchmark_start_price is None:
                benchmark_start_price = price
            benchmark_values.append(starting_cash * (price / benchmark_start_price))
            
            # 1. Fetch Vectorized Indicators
            # Lookups are near-instantaneous
            price = row['close']
            volatility = row['hv'] if not pd.isna(row['hv']) else 0.5
            implied_vol = volatility * iv_mult
            rsi = row['rsi']
            sma_200 = row['sma_200']
            hv_rank = row['hv_rank']

            # 2. Key Level Caching
            # Recalculate support/resistance nodes every 5 trading days
            if days_since_last_level_calc >= 5:
                # We still need a window of data for the clustering
                window_df = df.iloc[max(0, index-90):index+1]
                if len(window_df) >= 30:
                    cached_key_levels = find_key_levels(
                        window_df['close'], 
                        window_df['volume'],
                        n_clusters=6
                    )
                days_since_last_level_calc = 0
            else:
                days_since_last_level_calc += 1
            
            # 3. Update Mock Context
            mock_tradier.cash = cash
            mock_tradier.set_context(current_date_str, price, implied_vol)
            mock_analysis.set_context(price, cached_key_levels, rsi, implied_vol, sma_200=sma_200, hv_rank=hv_rank)
            
            # Debug: Log first few days and periodic updates
            if len(dates) < 3 or len(dates) % 20 == 0:
                support_levels = [l for l in cached_key_levels if l.get('type') == 'support']
                resist_levels = [l for l in cached_key_levels if l.get('type') == 'resistance']
                logger.debug(
                    f"[BT {current_date_str}] Price={price:.2f} IV={implied_vol:.3f} RSI={rsi:.1f} "
                    f"Supports={len(support_levels)} Resists={len(resist_levels)} "
                    f"Cash={cash:.2f} Positions={len(mock_tradier.positions)}"
                )

            # 3. Run Strategy: Manage Positions (Exits/Rolls)
            if strategy_type in ["credit_spread", "credit_spread_rulebase"]:
                strategy.manage_positions()
            elif strategy_type == "wheel":
                # Wheel handles management inside execute(), but we also
                # need to process any management-only orders separately
                strategy.execute([symbol])

            # Split orders into exits vs entries
            all_orders = list(mock_tradier.new_orders)
            mock_tradier.new_orders = []
            exit_orders = [o for o in all_orders if 'close' in o.get('side', '')]
            wheel_entry_orders = [o for o in all_orders if 'open' in o.get('side', '')] if strategy_type == 'wheel' else []
            
            # Debug: Log strategy execution logs if any useful info
            if hasattr(strategy, 'execution_logs') and strategy.execution_logs:
                for log_entry in strategy.execution_logs[-5:]:
                    logger.debug(f"[BT Strategy] {log_entry}")
                strategy.execution_logs = []

            
            for order in exit_orders:
                if 'close' in order['side'] or order['side'] == 'buy_to_close':
                    # Determine Fill Price
                    fill_price = order.get('price')
                    
                    if not fill_price: 
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
                            qs = mock_tradier.get_quotes([order['symbol']])
                            if qs:
                                fill_price = qs[0]['ask']
                    
                    # Slippage (Debit = paying more)
                    num_legs = len(order.get('legs') or []) or 1
                    slippage = slippage_rate * num_legs
                    final_price = fill_price + slippage
                    
                    qty = abs(order['quantity'])
                    multiplier = 1 if order.get('class') == 'equity' else 100
                    cost = final_price * qty * multiplier
                    
                    # Commission
                    commission_cost = commission_per * qty * num_legs
                    total_commissions += commission_cost
                    
                    cash -= (cost + commission_cost)
                    
                    # Compute per-trade realized P&L
                    close_symbol = order['symbol']
                    entry_info = open_trade_credits.pop(close_symbol, None)
                    realized_pnl = 0
                    if entry_info:
                        realized_pnl = round((entry_info['credit'] - final_price) * qty * multiplier, 2)
                    
                    trades_log.append({
                        'date': current_date_str,
                        'action': f"CLOSE {close_symbol}",
                        'debit': round(final_price, 4),
                        'slippage': slippage,
                        'commission': round(commission_cost, 2),
                        'pnl': realized_pnl
                    })
                    
                    # Remove Position
                    if order.get('legs'):
                        for leg in order['legs']:
                            symbol_to_remove = leg['option_symbol']
                            for p in list(mock_tradier.positions):
                                if p['symbol'] == symbol_to_remove:
                                    mock_tradier.positions.remove(p)
                                    break
                    else:
                        for p in list(mock_tradier.positions):
                            if p['symbol'] == order['symbol']:
                                mock_tradier.positions.remove(p)
                                break
            
            # 4. Run Strategy: Execute (Entries) — skip for Wheel (already called above)
            if strategy_type == "credit_spread":
                strategy.execute([symbol], config={'max_credit_spreads_per_symbol': 5})
            elif strategy_type == "credit_spread_rulebase":
                print(f"[DEBUG BACKTEST] Exectuting Rulebase for {symbol} | Price: {price} | RSI: {rsi} | VIX (IV): {implied_vol*100} | Date: {current_date_str}")
                strategy.execute([symbol], config={
                    'max_credit_spread_rulebase_lots': 5,
                    'min_credit_pct': 0.10,  # 10% of width (relaxed for synthetic pricing)
                    'max_capital_per_symbol': 2500
                })
            # Wheel entries already captured in wheel_entry_orders from step 3
            
            # 5. Process New Orders → Create Positions (Entries)
            entry_orders = list(mock_tradier.new_orders) + wheel_entry_orders
            mock_tradier.new_orders = []
            for order in entry_orders:
                if order['side'] == 'sell_to_open' or (order['class'] == 'multileg'):
                    requested_price = order['price']
                    
                    # Slippage on Entry (Credit = receive less)
                    num_legs = len(order.get('legs') or []) or 1
                    slippage = slippage_rate * num_legs
                    fill_price = max(0, requested_price - slippage)
                    
                    qty = order['quantity']
                    multiplier = 1 if order.get('class') == 'equity' else 100
                    
                    if risk_pct > 0:
                        current_pv = portfolio_values[-1] if portfolio_values else starting_cash
                        risk_amount = current_pv * risk_pct
                        
                        trade_risk = 0
                        if order.get('class') == 'multileg' and order.get('legs'):
                            strikes = []
                            for leg in order['legs']:
                                details = mock_tradier._parse_option_symbol(leg['option_symbol'])
                                if details: strikes.append(details['strike'])
                            if len(strikes) >= 2:
                                width = abs(strikes[0] - strikes[-1])
                                trade_risk = max(0.01, (width - fill_price) * 100)
                        else:
                            pos_sym = order.get('option_symbol') or ''
                            details = mock_tradier._parse_option_symbol(pos_sym)
                            if details:
                                trade_risk = max(0.01, (details['strike'] - fill_price) * 100)
                            else:
                                trade_risk = max(0.01, fill_price * multiplier)
                                
                        if trade_risk > 0:
                            qty = max(1, int(risk_amount / trade_risk))
                    
                    # Commission
                    commission_cost = commission_per * qty * num_legs
                    total_commissions += commission_cost
                    
                    cash += (fill_price * qty * multiplier) - commission_cost

                    # Track entry credit for P&L calculation on close
                    open_trade_credits[order['symbol']] = {
                        'credit': fill_price,
                        'date': current_date_str,
                        'contracts': qty
                    }
                    
                    leg_desc = order['legs'][0]['option_symbol'] if order.get('legs') else ''
                    trades_log.append({
                        'date': current_date_str,
                        'action': f"OPEN {order['symbol']} ({leg_desc})",
                        'credit': round(fill_price, 4),
                        'slippage': slippage,
                        'commission': round(commission_cost, 2),
                        'pnl': 0  # P&L realized on close
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
                        pos_symbol = order.get('option_symbol') or order['symbol']
                        mock_tradier.positions.append({
                            'symbol': pos_symbol,
                            'quantity': -qty if 'sell' in order['side'] else qty,
                            'cost_basis': fill_price,
                            'date_acquired': current_date_str
                        })

            # 6. Mark to Market Portfolio Value
            nlv = 0
            for pos in mock_tradier.positions:
                qs = mock_tradier.get_quotes([pos['symbol']])
                if qs:
                    price_q = qs[0]['last']
                    mult = 1 if not mock_tradier._parse_option_symbol(pos['symbol']) else 100
                    nlv += (price_q * pos['quantity'] * mult)
            
            total_val = cash + nlv
            dates.append(current_date_str)
            portfolio_values.append(total_val)
            
            # 7. Expiry Check — Assignment Logic
            for pos in list(mock_tradier.positions):
                details = mock_tradier._parse_option_symbol(pos['symbol'])
                if details and details['expiry'].date() <= row['date'].date():
                    strike = details['strike']
                    is_call = details['type'] == 'call'
                    is_itm = (is_call and price > strike) or (not is_call and price < strike)
                    
                    if is_itm:
                        qty = pos['quantity']
                        is_short = qty < 0
                        
                        if is_short and not is_call:  # Short Put ITM → Assignment
                            mock_tradier.positions.remove(pos)
                            num_shares = abs(qty) * 100
                            stock_cost = strike * num_shares
                            cash -= stock_cost
                            
                            mock_tradier.positions.append({
                                'symbol': details['root'],
                                'quantity': num_shares,
                                'cost_basis': strike,
                                'date_acquired': current_date_str
                            })
                            
                            # Realize P&L from the option itself
                            entry = open_trade_credits.pop(pos['symbol'], None)
                            option_pnl = 0
                            if entry:
                                # Put expired ITM: the option is worth intrinsic
                                intrinsic = strike - price
                                option_pnl = round((entry['credit'] - intrinsic) * abs(qty) * 100, 2)
                            
                            trades_log.append({
                                'date': current_date_str,
                                'action': f"ASSIGNED (PUT) {pos['symbol']}: Bought {num_shares} {details['root']} @ {strike}",
                                'debit': strike,
                                'pnl': option_pnl
                            })
                            
                        elif is_short and is_call:  # Short Call ITM → Called Away
                            mock_tradier.positions.remove(pos)
                            shares_needed = abs(qty) * 100
                            stock_found = False
                            
                            for sp in list(mock_tradier.positions):
                                if sp['symbol'] == details['root'] and sp['quantity'] > 0:
                                    cash += (strike * shares_needed)
                                    sp['quantity'] -= shares_needed
                                    if sp['quantity'] <= 0:
                                        mock_tradier.positions.remove(sp)
                                    
                                    stock_found = True
                                    
                                    # Realize stock P&L
                                    stock_pnl = round((strike - sp.get('cost_basis', strike)) * shares_needed, 2)
                                    entry = open_trade_credits.pop(pos['symbol'], None)
                                    call_credit_pnl = 0
                                    if entry:
                                        call_credit_pnl = round(entry['credit'] * abs(qty) * 100, 2)
                                    
                                    trades_log.append({
                                        'date': current_date_str,
                                        'action': f"CALLED AWAY {pos['symbol']}: Sold {shares_needed} {details['root']} @ {strike}",
                                        'credit': strike,
                                        'pnl': stock_pnl + call_credit_pnl
                                    })
                                    break
                            
                            if not stock_found:
                                # Naked Call Assignment → Short Stock
                                cash += (strike * shares_needed)
                                mock_tradier.positions.append({
                                    'symbol': details['root'],
                                    'quantity': -shares_needed,
                                    'cost_basis': strike,
                                    'date_acquired': current_date_str
                                })

                        else:
                            # Long Option ITM → Cash Settle
                            intrinsic = abs(price - strike)
                            cash_impact = intrinsic * pos['quantity'] * 100
                            cash += cash_impact
                            trades_log.append({
                                'date': current_date_str,
                                'action': f"EXERCISED ITM {pos['symbol']}",
                                'pnl': round(cash_impact, 2)
                            })
                            mock_tradier.positions.remove(pos)

                    else:
                        # Expired OTM — full credit kept
                        entry = open_trade_credits.pop(pos['symbol'], None)
                        expired_pnl = 0
                        if entry:
                            expired_pnl = round(entry['credit'] * entry['contracts'] * 100, 2)
                        
                        trades_log.append({
                            'date': current_date_str,
                            'action': f"EXPIRED OTM {pos['symbol']}",
                            'pnl': expired_pnl
                        })
                        mock_tradier.positions.remove(pos)

        # ---------------------------------------------------------------
        # Summary Metrics
        # ---------------------------------------------------------------
        if not portfolio_values:
            return {"error": "No simulation steps"}

        metrics = self._compute_metrics(
            portfolio_values, benchmark_values, dates,
            trades_log, starting_cash, total_commissions
        )
        
        return {
            "dates": dates,
            "values": [float(x) for x in portfolio_values],
            "benchmark_values": [float(x) for x in benchmark_values],
            "trades": trades_log,
            "metrics": metrics
        }

    @staticmethod
    def _compute_metrics(portfolio_values, benchmark_values, dates,
                         trades_log, starting_cash, total_commissions):
        """Compute comprehensive risk and performance metrics."""
        pv = np.array(portfolio_values, dtype=float)

        # --- Return metrics ---
        total_return = (pv[-1] - starting_cash) / starting_cash
        n_days = len(pv)
        annualized_return = ((pv[-1] / starting_cash) ** (252 / max(n_days, 1))) - 1

        # --- Benchmark ---
        bv = np.array(benchmark_values, dtype=float) if benchmark_values else np.full(len(pv), starting_cash)
        benchmark_return = (bv[-1] - starting_cash) / starting_cash

        # --- Max Drawdown ---
        running_max = np.maximum.accumulate(pv)
        drawdowns = (pv - running_max) / running_max
        max_drawdown = float(np.min(drawdowns))
        
        # --- Drawdown Series for Charting ---
        drawdown_series = []
        for i, date_str in enumerate(dates):
            drawdown_series.append({"time": date_str, "value": round(float(drawdowns[i] * 100), 2)})

        # --- Monthly Returns for Heatmap ---
        monthly_returns = {}
        month_groups = {}
        # Group portfolio values by YYYY-MM
        for i, date_str in enumerate(dates):
            ym = date_str[:7]
            if ym not in month_groups:
                month_groups[ym] = []
            month_groups[ym].append(pv[i])
            
        for ym, vals in month_groups.items():
            start_val = vals[0]
            end_val = vals[-1]
            ret = (end_val - start_val) / start_val * 100
            year, month = ym.split("-")
            if year not in monthly_returns:
                monthly_returns[year] = {}
            monthly_returns[year][month] = round(ret, 2)

        # --- Daily Returns & Sharpe Ratio ---
        daily_returns = np.diff(pv) / pv[:-1] if len(pv) > 1 else np.array([0.0])
        avg_daily = np.mean(daily_returns)
        std_daily = np.std(daily_returns)
        sharpe_ratio = (avg_daily / std_daily * np.sqrt(252)) if std_daily > 0 else 0.0

        # --- Trade-level metrics ---
        trade_pnls = [t['pnl'] for t in trades_log if t.get('pnl', 0) != 0]
        wins = [p for p in trade_pnls if p > 0]
        losses = [p for p in trade_pnls if p < 0]
        
        total_trades = len(trade_pnls)
        win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0.0
        avg_win = np.mean(wins) if wins else 0.0
        avg_loss = np.mean(losses) if losses else 0.0
        
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float('inf') if gross_profit > 0 else 0.0

        # Max consecutive losses
        max_consec_losses = 0
        current_streak = 0
        for p in trade_pnls:
            if p < 0:
                current_streak += 1
                max_consec_losses = max(max_consec_losses, current_streak)
            else:
                current_streak = 0

        # --- Advanced Metrics ---
        loss_rate = 100.0 - win_rate
        expectancy = ((win_rate / 100) * avg_win) + ((loss_rate / 100) * avg_loss)
        
        calmar_ratio = 0.0
        if max_drawdown < 0:
            calmar_ratio = annualized_return / abs(max_drawdown)

        return {
            "total_return": f"{total_return * 100:.2f}%",
            "annualized_return": f"{annualized_return * 100:.2f}%",
            "final_value": f"${pv[-1]:.2f}",
            "benchmark_return": f"{benchmark_return * 100:.2f}%",
            "max_drawdown": f"{max_drawdown * 100:.2f}%",
            "sharpe_ratio": round(float(sharpe_ratio), 2),
            "calmar_ratio": round(float(calmar_ratio), 2),
            "expectancy": f"${expectancy:.2f}",
            "trade_count": len(trades_log),
            "closed_trades": total_trades,
            "win_rate": f"{win_rate:.1f}%",
            "avg_win": f"${avg_win:.2f}",
            "avg_loss": f"${avg_loss:.2f}",
            "profit_factor": round(float(profit_factor), 2) if profit_factor != float('inf') else "∞",
            "max_consecutive_losses": max_consec_losses,
            "total_commissions": f"${total_commissions:.2f}",
            "drawdown_series": drawdown_series,
            "monthly_returns": monthly_returns
        }
