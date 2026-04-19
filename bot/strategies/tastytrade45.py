import re
import math
import traceback
import statistics
from datetime import datetime, timedelta, date
from bot.strategies.base_strategy import AbstractStrategy
from bot.utils import Colors, get_expiry_str, get_underlying

class TastyTrade45Strategy(AbstractStrategy):
    def __init__(self, tradier_service, db, dry_run=False, analysis_service=None, trade_manager=None):
        super().__init__(tradier_service, db, dry_run, analysis_service, trade_manager=trade_manager)
        self.hv_cache = {}

    def _log(self, message):
        super()._log(message, strategy_name="TASTYTRADE45")

    def _calculate_hv_rank(self, symbol):
        """Approximate an IV Rank equivalent using Historical Volatility (HV Rank)."""
        if symbol in self.hv_cache:
            return self.hv_cache[symbol]

        end_date = self._get_current_date()
        start_date = end_date - timedelta(days=365) # Approx 252 trading days
        
        history = self.tradier.get_historical_pricing(
            symbol, 
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d')
        )
        
        if not history or len(history) < 30:
            return 50 # Default safe fallback
            
        # Calculate daily log returns
        returns = []
        last_close = None
        for day in history:
            try:
                close = float(day['close'])
                if last_close and last_close > 0:
                    ret = math.log(close / last_close)
                    returns.append(ret)
                last_close = close
            except Exception:
                pass
                
        if len(returns) < 30:
            return 50
            
        # Calculate 30-day HV rolling blocks to find min/max
        windows = []
        for i in range(len(returns) - 20):
            block = returns[i:i+20] # Short term window for IV simulation
            std_dev = statistics.stdev(block)
            hv = std_dev * math.sqrt(252) * 100
            windows.append(hv)
            
        if not windows:
            return 50
            
        min_hv = min(windows)
        max_hv = max(windows)
        current_hv = windows[-1]
        
        if max_hv == min_hv:
            rank = 50
        else:
            rank = ((current_hv - min_hv) / (max_hv - min_hv)) * 100
            
        self.hv_cache[symbol] = rank
        return rank

    def execute(self, watchlist, config=None):
        config = config or {}
        """
        Execute TastyTrade45 strategy.
        Mode A: Initial Open (no open spreads, 30-60 DTE)
        Mode B: Iron Condor Completion (1 side open, 30-60 DTE)
        """
        for symbol in watchlist:
            try:
                if self.dry_run:
                    print(f"\n{Colors.HEADER}📦 Analyzing TT45 {symbol}...{Colors.ENDC}")
                else:
                    self._log(f"Analyzing {symbol}...")
                
                # Check Volatility Rank threshold (> 30)
                hv_rank = self._calculate_hv_rank(symbol)
                if hv_rank < 30:
                    self._log(f"Skipping {symbol}: IVR/HVR Proxy is {hv_rank:.1f} (requires > 30).")
                    continue
                    
                q = self.tradier.get_quote(symbol)
                if not q: continue
                current_price = q.get('last') or q.get('close')
                if not current_price: continue

                # 1. Check for existing open positions on this symbol
                open_info = self._get_open_sides_for_symbol(symbol)
                
                if not open_info:
                    # Mode A: Initial Open
                    self._log(f"ℹ️ {symbol}: No open positions. Attempting Initial Open.")
                    expiry = self._find_best_dte_expiry(symbol)
                    if not expiry:
                        continue

                    # Try to open put and call independently
                    self._process_side(symbol, current_price, expiry, is_put=True, config=config)
                    self._process_side(symbol, current_price, expiry, is_put=False, config=config)
                else:
                    # Mode B: Iron Condor Completion
                    open_sides = open_info['sides']  # set of 'put' and/or 'call'
                    dte = open_info['dte']
                    expiry = open_info['expiry']

                    # Already a full iron condor (both sides open)
                    if 'put' in open_sides and 'call' in open_sides:
                        self._log(f"ℹ️ {symbol}: Already an iron condor (both sides open, {dte} DTE). Skipping.")
                        continue

                    # Check DTE window for condor completion (30-60 DTE)
                    if dte < 30 or dte > 60:
                        open_side = list(open_sides)[0]
                        self._log(f"ℹ️ {symbol}: Open {open_side} spread with {dte} DTE — outside 30–60 DTE window. Skipping condor completion.")
                        continue

                    # Single side open, within 30–60 DTE — try the opposite side
                    open_side = list(open_sides)[0]
                    opposite_is_put = (open_side == 'call')
                    opposite_name = 'put' if opposite_is_put else 'call'

                    self._log(f"🦅 {symbol}: Completing Iron Condor — open {open_side} spread ({dte} DTE). Trying {opposite_name} side on expiry {expiry}.")

                    # Process the opposite side using the SAME expiry as the open position
                    self._process_side(symbol, current_price, expiry, is_put=opposite_is_put, config=config)
                
            except Exception as e:
                self._log(f"❌ Error processing {symbol}: {e}")
                traceback.print_exc()
        
        return self.execution_logs

    def _get_open_sides_for_symbol(self, symbol):
        """
        Check existing OPEN trades for this symbol under this strategy.
        Returns dict with open sides info, or None if no open positions.
        
        Returns:
            {
                'sides': {'put'} or {'call'} or {'put', 'call'},
                'expiry': '2026-05-30',   # expiry of the open position(s)
                'dte': 35                 # days remaining to expiry
            }
            or None if no open positions.
        """
        open_trades = self.get_open_trades()
        if not open_trades:
            return None
        
        today = self._get_current_date()
        sides = set()
        latest_expiry = None
        latest_dte = None
        
        for trade in open_trades:
            if trade.get('symbol') != symbol:
                continue
            
            short_leg = trade.get('short_leg', '')
            if not short_leg:
                continue
            
            # Parse OCC symbol: e.g. SPY260530P00520000
            match = re.search(r'[A-Z]+(\d{6})([PC])(\d{8})', short_leg)
            if not match:
                continue
            
            try:
                expiry_date = datetime.strptime(match.group(1), '%y%m%d').date()
                option_type = 'put' if match.group(2) == 'P' else 'call'
                dte = (expiry_date - today).days
                
                sides.add(option_type)
                
                if latest_expiry is None or expiry_date > datetime.strptime(latest_expiry, '%Y-%m-%d').date():
                    latest_expiry = expiry_date.strftime('%Y-%m-%d')
                    latest_dte = dte
            except ValueError:
                continue
        
        if not sides:
            return None
        
        return {
            'sides': sides,
            'expiry': latest_expiry,
            'dte': latest_dte
        }

    def _find_best_dte_expiry(self, symbol):
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations:
            return None
            
        today = self._get_current_date()
        valid_expiries = []
        
        for e in expirations:
            if isinstance(e, str):
                try: d = datetime.strptime(e, "%Y-%m-%d").date()
                except ValueError: continue
            else:
                d = e.date() if hasattr(e, 'date') else e
                
            dte = (d - today).days
            if 30 <= dte <= 60:
                score = abs(dte - 45)
                valid_expiries.append({'date': d, 'str': d.strftime("%Y-%m-%d"), 'dte': dte, 'score': score})
                
        if not valid_expiries:
            self._log(f"Skipping {symbol}: No expirations exist between 30 and 60 DTE.")
            return None
            
        valid_expiries.sort(key=lambda x: x['score'])
        best = valid_expiries[0]
        self._log(f"{symbol} selected DTE: {best['dte']} for Tastytrade45 setup.")
        return best['str']

    def _process_side(self, symbol, current_price, expiry, is_put, config=None):
        config = config or {}
        side_name = "Put" if is_put else "Call"
        
        chain = self.tradier.get_option_chains(symbol, expiry)
        if not chain:
            return
            
        opt_type = 'put' if is_put else 'call'
        options = [o for o in chain if o['option_type'] == opt_type]
        if not options:
            return
            
        # Target 16 Delta short strike
        target_delta = 0.16
        
        valid_shorts = []
        for o in options:
            delta = o.get('greeks', {}).get('delta')
            if delta is None: continue
            abs_delta = abs(float(delta))
            valid_shorts.append({'opt': o, 'abs_delta': abs_delta, 'diff': abs(abs_delta - target_delta)})
            
        if not valid_shorts:
            self._log(f"Skipping {symbol} {side_name}: No greeks available to determine Delta.")
            return
            
        # Sort by closest to 16 Delta
        valid_shorts.sort(key=lambda x: x['diff'])
        best_short = valid_shorts[0]['opt']
        short_strike = best_short['strike']
        short_delta = abs(float(best_short.get('greeks', {}).get('delta', 0)))
        
        self._log(f"Targeting {symbol} {side_name} | Strike: {short_strike} | Delta: {short_delta:.2f} (Target 0.16)")

        target_width = 5.00
        long_strike = round(short_strike - target_width, 2) if is_put else round(short_strike + target_width, 2)
        
        long_leg = next((o for o in options if abs(o['strike'] - long_strike) < 0.01), None)
        if not long_leg:
            self._log(f"Skipping {symbol} {side_name}: Required Long Leg {long_strike} for 5.0 parameter width is missing.")
            return
            
        short_price = round((best_short['bid'] + best_short['ask']) / 2, 2)
        long_price = round((long_leg['bid'] + long_leg['ask']) / 2, 2)
        
        if best_short['bid'] == 0 and best_short['ask'] == 0:
            return
            
        net_credit = round(short_price - long_price, 2)
        
        # Require net credit >= 10% of width (0.50 for 5.0 wide)
        min_required_credit = round(target_width * 0.10, 2)
        if net_credit < min_required_credit:
            self._log(f"Skipping {symbol} {side_name}: Net credit {net_credit} < 10% limit ({min_required_credit}) for width {target_width}.")
            return
            
        requirement_per_lot = target_width * 100
        available_bp = self._get_available_bp(config)
        max_lots_config = config.get('max_tastytrade45_per_symbol', 5)
        
        dynamic_lots = int(available_bp // requirement_per_lot)
        if dynamic_lots < 1:
            self._log(f"Skipping {symbol}: Spread req (${requirement_per_lot:,.2f}) > BP (${available_bp:,.2f}).")
            return
            
        dynamic_lots = min(dynamic_lots, max_lots_config)

        # Per-chain limit: subtract existing positions on this expiry
        existing = self._count_existing_on_expiry(symbol, expiry)
        dynamic_lots = min(dynamic_lots, max_lots_config - existing)
        if dynamic_lots < 1:
            self._log(f"ℹ️ {symbol}: TT45 chain {expiry} at max ({existing}/{max_lots_config}). Skipping.")
            return
        self._log(f"📦 {symbol}: TT45 chain {expiry} has {existing}/{max_lots_config} lots. Opening {dynamic_lots} more.")

        total_requirement = requirement_per_lot * dynamic_lots
        if not self._is_bp_sufficient(total_requirement, config):
            self._log(f"Skipping {symbol}: BP insufficient.")
            return

        self._log(f"✅ Placing TT45 {symbol} {side_name} | Short: {short_strike} | Long: {long_strike} | Credit: {net_credit} | Lots: {dynamic_lots}")
        
        legs = [
            {'option_symbol': best_short['symbol'], 'side': 'sell_to_open', 'quantity': dynamic_lots},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': dynamic_lots}
        ]
        
        if self.dry_run:
            self._log(f"[DRY RUN] Simulating {side_name} Spread Order for {symbol}")
            response = {'id': 'mock_order_id', 'status': 'ok'}
        else:
            if getattr(self, 'trade_manager', None):
                response = self.trade_manager.execute_strategy_order(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side='sell',
                    quantity=1,
                    price=net_credit,
                    order_class='multileg',
                    legs=legs,
                    tag=self.strategy_id,
                    strategy_params={'short_leg': best_short['symbol'], 'long_leg': long_leg['symbol']}
                )
            else:
                response = self.tradier.place_order(
                    account_id=self.tradier.account_id,
                    symbol=symbol,
                    side='sell',
                    quantity=1,
                    order_type='credit',
                    duration='day',
                    price=net_credit,
                    order_class='multileg',
                    legs=legs,
                    tag="TT45"
                )
            
        if 'error' in response:
             self._log(f"Order failed: {response['error']}")
        else:
             self._log(f"Order placed: {response}")
             if not getattr(self, 'trade_manager', None):
                 legs_info = {
                     'short_leg': best_short['symbol'],
                     'long_leg': long_leg['symbol']
                 }
                 self._record_trade(symbol, f"TastyTrade45 {side_name}", net_credit, response, legs_info)

    def manage_positions(self, simulation_mode=False):
        if simulation_mode: self.execution_logs = []
        if self.db is None: return

        active_option_symbols = {}
        if not simulation_mode:
            try:
                positions = self.tradier.get_positions()
                active_option_symbols = {p['symbol']: p for p in positions}
            except Exception as e:
                self._log(f"Error fetching positions for management: {e}")
                return self.execution_logs if simulation_mode else None

        open_trades = self.get_open_trades()

        if not open_trades: return self.execution_logs if simulation_mode else None

        # Process standard exits
        for trade in open_trades:
            self._evaluate_trade_exit(trade, active_option_symbols, simulation_mode)
            
        # Process neutral rolling defense
        if not simulation_mode:
            self._process_rolling_defense(open_trades, active_option_symbols)

        return self.execution_logs if simulation_mode else None

    def _evaluate_trade_exit(self, trade, active_option_symbols, simulation_mode):
        symbol = trade['symbol']
        short_leg = trade.get('short_leg')
        long_leg = trade.get('long_leg')
        entry_credit = float(trade.get('price', 0))

        if not simulation_mode and (not short_leg or short_leg not in active_option_symbols):
             return # Ignoring orphan trade

        match = re.search(r'[A-Z]+(\d{6})[PC]', short_leg)
        if not match: return
        try:
            expiry_date = datetime.strptime(match.group(1), '%y%m%d').date()
            dte = (expiry_date - self._get_current_date()).days
        except ValueError: return

        should_close = False
        close_reason = ""
        current_debit = 0.0

        if long_leg:
            legs_str = f"{short_leg},{long_leg}"
            try:
                q_data = self.tradier.get_quote(legs_str)
                legs_quotes = [q_data] if isinstance(q_data, dict) else q_data if isinstance(q_data, list) else []
                sq = next((q for q in legs_quotes if q['symbol'] == short_leg), None)
                lq = next((q for q in legs_quotes if q['symbol'] == long_leg), None)

                if sq and lq:
                    sq_ask = float(sq.get('ask', 0))
                    lq_bid = float(lq.get('bid', 0))
                    current_debit = round(sq_ask - lq_bid, 2)
                    
                    # Take Profit: 50% only if DTE >= 22
                    if dte >= 22 and current_debit <= (entry_credit * 0.50):
                        should_close = True
                        close_reason = f"Take Profit 50% (Debit ${current_debit:.2f} <= 50% of Setup ${entry_credit:.2f}) — DTE {dte} (>=22)"
            except Exception as e:
                pass

        if not should_close and dte <= 21:
            should_close = True
            close_reason = f"Hard 21 DTE Ruleset Engine (DTE {dte} <= 21)"

        if should_close:
            self._log(f"🚨 Closing TT45 {symbol} ({short_leg}) — Reason: {close_reason}")
            limit_price = round(current_debit * 1.05, 2) if current_debit > 0 else 0.05
            self._execute_close(trade, limit_price=limit_price, simulation_mode=simulation_mode)

    def _process_rolling_defense(self, open_trades, active_option_symbols):
        """
        Groups symbols and expiries. If a Put is tested (short delta > 0.30), roll the corresponding Call up to neutralize.
        """
        # Exclude trades we just marked CLOSED in this run loop
        active_trades = [t for t in open_trades if t.get('status') == 'OPEN']
        
        # Group by symbol & expiry
        groups = {}
        for trade in active_trades:
            symbol = trade['symbol']
            short_leg = trade.get('short_leg')
            if not short_leg or short_leg not in active_option_symbols: continue

            match = re.search(r'[A-Z]+(\d{6})([PC])', short_leg)
            if not match: continue
            
            expiry_str = match.group(1)
            side = 'Put' if match.group(2) == 'P' else 'Call'
            
            key = f"{symbol}_{expiry_str}"
            if key not in groups:
                 groups[key] = {}
            groups[key][side] = trade
            
        for key, legs in groups.items():
            if 'Put' in legs and 'Call' in legs:
                put_trade = legs['Put']
                call_trade = legs['Call']
                
                try:
                    symbol = put_trade['symbol']
                    expiry_date_str = '20' + key.split('_')[1]
                    expiry_fmt = f"{expiry_date_str[:4]}-{expiry_date_str[4:6]}-{expiry_date_str[6:]}"
                    
                    chain = self.tradier.get_option_chains(symbol, expiry_fmt)
                    if not chain: continue
                    
                    put_opt = next((o for o in chain if o['symbol'] == put_trade['short_leg']), None)
                    call_opt = next((o for o in chain if o['symbol'] == call_trade['short_leg']), None)
                    
                    if not put_opt or not call_opt: continue
                    
                    put_delta = abs(float(put_opt.get('greeks', {}).get('delta', 0)))
                    call_delta = abs(float(call_opt.get('greeks', {}).get('delta', 0)))
                    
                    if put_delta > 0.30 and call_delta < put_delta - 0.05:
                        self._log(f"🛡️ TT45 Defense: Put ({put_delta:.2f}Δ) is challenged. Rolling Call ({call_delta:.2f}Δ) to neutralize.")
                        self._roll_untested_side(call_trade, put_delta, chain, is_put=False)
                        
                    elif call_delta > 0.30 and put_delta < call_delta - 0.05:
                        self._log(f"🛡️ TT45 Defense: Call ({call_delta:.2f}Δ) is challenged. Rolling Put ({put_delta:.2f}Δ) to neutralize.")
                        self._roll_untested_side(put_trade, call_delta, chain, is_put=True)
                        
                except Exception as e:
                     self._log(f"Error evaluating TT45 defense logic for {key}: {e}")

    def _roll_untested_side(self, tradeToClose, target_delta, chain, is_put):
        try:
            self._log(f"Closing old untested leg: {tradeToClose['short_leg']}")
            self._execute_close(tradeToClose, limit_price=0.01) # Default market-ish routing on panic defenses
        except Exception as e:
            self._log(f"Failed to close old untested leg: {e}")
            return
            
        opt_type = 'put' if is_put else 'call'
        options = [o for o in chain if o['option_type'] == opt_type]
        
        valid_shorts = []
        for o in options:
            delta = o.get('greeks', {}).get('delta')
            if delta is None: continue
            abs_delta = abs(float(delta))
            valid_shorts.append({'opt': o, 'abs_delta': abs_delta, 'diff': abs(abs_delta - target_delta)})
            
        if not valid_shorts: return
        valid_shorts.sort(key=lambda x: x['diff'])
        
        best_short = valid_shorts[0]['opt']
        short_strike = best_short['strike']
        target_width = 5.0
        long_strike = round(short_strike - target_width, 2) if is_put else round(short_strike + target_width, 2)
        
        long_leg = next((o for o in options if abs(o['strike'] - long_strike) < 0.01), None)
        if not long_leg: return
        
        short_price = round((best_short['bid'] + best_short['ask']) / 2, 2)
        long_price = round((long_leg['bid'] + long_leg['ask']) / 2, 2)
        net_credit = round(short_price - long_price, 2)
        
        min_required_credit = round(target_width * 0.10, 2)
        if net_credit < min_required_credit:
            self._log(f"Skipping TT45 Roll: Net credit {net_credit} < 10% limit ({min_required_credit}) for width {target_width}.")
            return

        qty = tradeToClose.get('quantity', 1)
            
        legs = [
            {'option_symbol': best_short['symbol'], 'side': 'sell_to_open', 'quantity': qty},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': qty}
        ]
        
        symbol = tradeToClose['symbol']
        side_name = "Put" if is_put else "Call"
        
        self._log(f"🛡️ TT45 Rolling -> {symbol} new {side_name} at {short_strike} Strike | Delta: {valid_shorts[0]['abs_delta']:.2f}")
        if getattr(self, 'trade_manager', None):
            response = self.trade_manager.execute_strategy_order(
                strategy_id=self.strategy_id,
                symbol=symbol,
                side='sell',
                quantity=1, 
                price=net_credit if net_credit > 0 else None,
                order_class='multileg',
                legs=legs,
                tag=self.strategy_id,
                strategy_params={'short_leg': best_short['symbol'], 'long_leg': long_leg['symbol']}
            )
        else:
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='sell',
                quantity=1, 
                order_type='credit',
                duration='day',
                price=net_credit if net_credit > 0 else None,
                order_class='multileg',
                legs=legs,
                tag="TT45"
            )
        if 'error' not in response:
            if not getattr(self, 'trade_manager', None):
                legs_info = {'short_leg': best_short['symbol'], 'long_leg': long_leg['symbol']}
                self._record_trade(symbol, f"TastyTrade45 {side_name}", net_credit, response, legs_info)

    def _execute_close(self, trade, limit_price=None, simulation_mode=False):
        short_leg = trade['short_leg']
        long_leg = trade['long_leg']
        legs = [
            {'option_symbol': short_leg, 'side': 'buy_to_close', 'quantity': 1},
            {'option_symbol': long_leg, 'side': 'sell_to_close', 'quantity': 1}
        ]

        if self.dry_run or simulation_mode:
            self._log(f"[DRY RUN/SIM] Closing TT45 {trade['symbol']} spread.")
            if not simulation_mode:
                if getattr(self, 'trade_manager', None):
                    self.trade_manager.mark_trade_closed(trade['_id'], limit_price=limit_price, response_id=None)
                else:
                    self.db['active_trades'].update_one(
                        {"_id": trade['_id']},
                        {"$set": {"status": "CLOSED", "close_date": datetime.now(), "exit_price": limit_price}}
                    )
        else:
            p = limit_price if limit_price and limit_price > 0 else 0.01 
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=trade['symbol'],
                side='buy',
                quantity=1,
                order_type='debit',
                duration='day',
                price=p,
                order_class='multileg',
                legs=legs,
                tag="TT45_CLOSE"
            )
            if 'error' not in response:
                if getattr(self, 'trade_manager', None):
                    self.trade_manager.mark_trade_closed(trade['_id'], limit_price=limit_price, response_id=response.get('id'))
                else:
                    self.db['active_trades'].update_one(
                        {"_id": trade['_id']},
                        {"$set": {
                            "status": "CLOSED", 
                            "close_date": datetime.now(), 
                            "close_order_id": response.get('id'),
                            "exit_price": limit_price
                        }}
                    )
