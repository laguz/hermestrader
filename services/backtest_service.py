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


from tests.mocks.backtest_mocks import MockTradierService, MockAnalysisService, MockDB
from utils.data_generator import generate_synthetic_history

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



    def _prepare_data(self, symbol, start_date, end_date):
        """Fetches base historical data and applies vectorized indicators needed for backtesting."""
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
            history = generate_synthetic_history(symbol, warmup_start, end_date)
        
        if not history:
            return None
        
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
        df['hv_rank'] = df['hv'].rolling(window=252, min_periods=20).apply(
            lambda x: (x < x.iloc[-1]).sum() / len(x) * 100 if len(x) > 0 else 50, raw=False
        )
        
        # Basic forward-fill and defaults for early rows
        df['hv'] = df['hv'].fillna(0.5)
        df['hv_rank'] = df['hv_rank'].fillna(50.0)
        df['rsi'] = df['rsi'].fillna(50.0)
        return df

    def run_backtest(self, symbol, strategy_type, start_date, end_date,
                     commission=None, iv_multiplier=None, risk_free_rate=None, slippage_per_leg=0.01, risk_per_trade_pct=0.02):
        logger.info(f"Starting Backtest for {symbol} ({strategy_type}) {start_date} → {end_date}")

        # Apply configurable parameters
        commission_per = commission if commission is not None else self.COMMISSION_PER_CONTRACT
        iv_mult = iv_multiplier if iv_multiplier is not None else self.IV_HV_MULTIPLIER
        slippage_rate = slippage_per_leg
        risk_pct = risk_per_trade_pct

        df = self._prepare_data(symbol, start_date, end_date)
        if df is None:
            return {"error": "No data found"}
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')

        

        
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
                        
            # 5. Process New Orders (Assume filled at current mid-prices with slippage)
            try:
                # Combine new orders from strategy with any wheel-specific entry orders
                all_entry_orders = list(mock_tradier.new_orders) + wheel_entry_orders
                mock_tradier.new_orders = [] # Clear new orders after processing
                
                # Pass total_commissions by reference or return it
                cash, total_commissions = self._process_open_orders(
                    mock_tradier, current_date_str, risk_pct, slippage_rate, 
                    commission_per, trades_log, open_trade_credits, all_entry_orders,
                    cash, total_commissions, portfolio_values, starting_cash
                )
            except Exception as e:
                print(f"{current_date_str} - {strategy_type} Strategy Exception: {e}")
                            
            # 6. Mark to Market Portfolio Value
            total_val = self._mark_to_market(mock_tradier, cash)
            dates.append(current_date_str)
            portfolio_values.append(total_val)
            
            # 7. Expiry Check — Assignment Logic
            cash = self._process_expirations(mock_tradier, row['date'].date(), price, cash, open_trade_credits, trades_log, current_date_str)

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

    def _process_open_orders(self, mock_tradier, current_date_str, risk_pct, num_legs, slippage_rate, 
                             commission_per, trades_log, open_trade_credits):
        """Processes new orders, applying slippage, calculating position sizing, and deducting commissions."""
        if not mock_tradier.new_orders:
            return
            
        total_val = mock_tradier.cash + sum([p.get('cost_basis', 0) for p in mock_tradier.positions]) # Rough approx for sizing
            
        for order in mock_tradier.new_orders:
            # Calculate fill price with slippage
            fill_price = order['price']
            if 'sell' in order['side']:
                fill_price -= slippage_rate  # Worse fill on sell
            else:
                fill_price += slippage_rate  # Worse fill on buy
                
            slippage = slippage_rate * num_legs
            
            # Position Sizing
            qty = order.get('quantity', 1)
            multiplier = 100 if order.get('legs') or order.get('option_symbol') else 1
            
            # Very basic risk sizing approximation for credit spreads
            if 'credit' in order.get('action', '').lower():
                risk_amount = total_val * risk_pct
                trade_risk = 0
                if order.get('legs') and len(order['legs']) >= 2:
                    short_strike = order['legs'][0]['option_symbol']
                    long_strike = order['legs'][1]['option_symbol']
                    s_det = mock_tradier._parse_option_symbol(short_strike)
                    l_det = mock_tradier._parse_option_symbol(long_strike)
                    if s_det and l_det:
                        width = abs(s_det['strike'] - l_det['strike'])
                        trade_risk = (width - fill_price) * 100
                
                if trade_risk <= 0:
                    pos_sym = order.get('option_symbol') or order['symbol']
                    details = mock_tradier._parse_option_symbol(pos_sym)
                    if details:
                        trade_risk = max(0.01, (details['strike'] - fill_price) * 100)
                    else:
                        trade_risk = max(0.01, fill_price * multiplier)
                        
                if trade_risk > 0:
                    qty = max(1, int(risk_amount / trade_risk))
            
            # Commission
            commission_cost = commission_per * qty * num_legs
            
            mock_tradier.cash += (fill_price * qty * multiplier) - commission_cost

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
                        'quantity': -qty if 'sell' in leg['side'] else qty, # Fixed qty assignment
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

    def _mark_to_market(self, mock_tradier, cash):
        """Calculates the Net Liquidation Value (NLV) of the portfolio."""
        nlv = 0
        symbols_to_quote = [pos['symbol'] for pos in mock_tradier.positions]
        if symbols_to_quote:
            qs = mock_tradier.get_quotes(symbols_to_quote)
            quotes_map = {q['symbol']: q['last'] for q in qs}
            
            for pos in mock_tradier.positions:
                price_q = quotes_map.get(pos['symbol'], 0)
                mult = 1 if not mock_tradier._parse_option_symbol(pos['symbol']) else 100
                nlv += (price_q * pos['quantity'] * mult)
                
        return cash + nlv

    def _process_expirations(self, mock_tradier, current_date_obj, price, cash, open_trade_credits, trades_log, current_date_str):
        """Handles option assignment, exercise, and expiration."""
        for pos in list(mock_tradier.positions):
            details = mock_tradier._parse_option_symbol(pos['symbol'])
            if details and details['expiry'].date() <= current_date_obj:
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
        return cash

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
