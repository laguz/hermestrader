import re
import traceback
from datetime import datetime, timedelta
from bot.strategies.base_strategy import AbstractStrategy
from bot.utils import Colors
from bot.trade_manager import TradeAction

class CreditSpreads7Strategy(AbstractStrategy):
    def __init__(self, tradier_service, db, dry_run=False, analysis_service=None, trade_manager=None):
        super().__init__(tradier_service, db, dry_run, analysis_service, trade_manager=trade_manager)

    def _log(self, message):
        super()._log(message, strategy_name="CREDIT_SPREADS_7")

    def execute(self, watchlist, config=None):
        config = config or {}
        """
        Execute 7DTE Credit Spreads strategy.
        Mode A: Initial Open (no open spreads, exact 7DTE)
        Mode B: Iron Condor Completion (1 side open, 3-6 DTE)
        """
        for symbol in watchlist:
            try:
                if self.dry_run:
                    print(f"\n{Colors.HEADER}📦 Analyzing {symbol}...{Colors.ENDC}")
                else:
                    self._log(f"Analyzing {symbol}...")
                
                # 1. Analyze 3m data
                analysis = self.analysis_service.analyze_symbol(symbol, period='3m')
                if not analysis or 'error' in analysis:
                    self._log(f"⚠️  Analysis failed for {symbol}: {analysis.get('error')}")
                    continue
                
                current_price = analysis.get('current_price')
                if not current_price:
                    self._log(f"⚠️  Missing current price for {symbol}")
                    continue

                # 2. Check for existing open positions on this symbol
                open_info = self._get_open_sides_for_symbol(symbol)
                
                if not open_info:
                    # Mode A: Initial Open
                    self._log(f"ℹ️ {symbol}: No open positions. Attempting Initial Open.")
                    expiry = self._find_exact_7dte_expiry(symbol)
                    if not expiry:
                        continue

                    # Try to open put and call independently
                    self._process_side(symbol, current_price, analysis, expiry, is_put=True, config=config)
                    self._process_side(symbol, current_price, analysis, expiry, is_put=False, config=config)
                else:
                    # Mode B: Iron Condor Completion
                    open_sides = open_info['sides']  # set of 'put' and/or 'call'
                    dte = open_info['dte']
                    expiry = open_info['expiry']

                    # Already a full iron condor (both sides open)
                    if 'put' in open_sides and 'call' in open_sides:
                        self._log(f"ℹ️ {symbol}: Already an iron condor (both sides open, {dte} DTE). Skipping.")
                        continue

                    # Check DTE window for condor completion
                    if dte < 3 or dte > 6:
                        open_side = list(open_sides)[0]
                        self._log(f"ℹ️ {symbol}: Open {open_side} spread with {dte} DTE — outside 3–6 DTE window. Skipping condor completion.")
                        continue

                    # Single side open, within 3–6 DTE — try the opposite side
                    open_side = list(open_sides)[0]
                    opposite_is_put = (open_side == 'call')
                    opposite_name = 'put' if opposite_is_put else 'call'

                    self._log(f"🦅 {symbol}: Completing Iron Condor — open {open_side} spread ({dte} DTE). Trying {opposite_name} side on expiry {expiry}.")

                    # Process the opposite side using the SAME expiry as the open position
                    self._process_side(symbol, current_price, analysis, expiry, is_put=opposite_is_put, config=config)
                
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
                'expiry': '2026-04-24',   # expiry of the open position(s)
                'dte': 5                  # days remaining to expiry
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
            
            # Parse OCC symbol: e.g. SPY260424P00520000
            # The P or C character tells us put vs call
            match = re.search(r'[A-Z]+(\d{6})([PC])(\d{8})', short_leg)
            if not match:
                continue
            
            try:
                expiry_date = datetime.strptime(match.group(1), '%y%m%d').date()
                option_type = 'put' if match.group(2) == 'P' else 'call'
                dte = (expiry_date - today).days
                
                sides.add(option_type)
                
                # Track the expiry (all legs on same symbol should share expiry,
                # but use the latest if there are multiple)
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

    def _find_exact_7dte_expiry(self, symbol):
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations:
            self._log(f"No expirations found for {symbol}.")
            return None
        
        today = self._get_current_date()
        target_date = today + timedelta(days=7)
        
        for e in expirations:
            if isinstance(e, str):
                try:
                    d = datetime.strptime(e, "%Y-%m-%d").date()
                except ValueError:
                    continue
            else:
                d = e.date() if hasattr(e, 'date') else e
            
            if d == target_date:
                # Found exact 7DTE
                return target_date.strftime("%Y-%m-%d")
                
        self._log(f"Skipping {symbol}: Exact 7DTE ({target_date}) not available.")
        return None

    def _process_side(self, symbol, current_price, analysis, expiry, is_put, config=None):
        config = config or {}
        side_name = "Put" if is_put else "Call"

        # 1. Select the POP level
        short_strike, pop = self._select_entry_point(symbol, current_price, analysis, is_put, side_name)
        if short_strike is None:
            return

        # Get options and short leg
        options, short_leg = self._get_options_and_short_leg(symbol, expiry, short_strike, is_put, side_name)
        if not options or not short_leg:
            return

        # 2. Universal Dynamic Width
        dynamic_width = self._determine_dynamic_width(symbol, options, short_strike, side_name)
        if dynamic_width is None:
            return

        # 3. Find Long Leg and calculate 12% Rule
        long_leg, current_width, net_credit = self._find_long_leg_and_credit(
            symbol, options, short_strike, is_put, dynamic_width, side_name, short_leg
        )
        if not long_leg:
            return

        # 4. Determine Capital Risk & BP Verification
        dynamic_lots = self._determine_lots_and_verify_bp(symbol, expiry, current_width, config, side_name)
        if dynamic_lots is None:
            return

        # 5. Place the order
        self._place_order(symbol, short_strike, current_width, net_credit, dynamic_lots, short_leg, long_leg, side_name)

    def _select_entry_point(self, symbol, current_price, analysis, is_put, side_name):
        entry_key = 'put_entry_points' if is_put else 'call_entry_points'
        entry_points = analysis.get(entry_key, [])
        
        # Strict > 75% POP filter
        valid_points = []
        for ep in entry_points:
            pop = ep.get('pop', 0)
            price = ep.get('price')
            
            if pop > 75:
                # Ensure the entry point is OTM conceptually
                if is_put and price < current_price:
                    valid_points.append(ep)
                elif not is_put and price > current_price:
                    valid_points.append(ep)
                    
        if not valid_points:
            self._log(f"Skipping {symbol} {side_name}: No support/resistance levels with strict POP > 75%. (Aborted)")
            return None, None
            
        # Find closest to 75%
        target_ep = min(valid_points, key=lambda x: abs(x['pop'] - 75))
        short_strike = target_ep['price']
        pop = target_ep['pop']
        
        self._log(f"Targeting {symbol} {side_name} at exactly 7DTE | Strike: {short_strike} | POP: {pop:.2f}% | Nearest to 75% limit.")
        return short_strike, pop

    def _get_options_and_short_leg(self, symbol, expiry, short_strike, is_put, side_name):
        chain = self.tradier.get_option_chains(symbol, expiry)
        if not chain:
            self._log(f"Skipping {symbol}: Failed to fetch option chain for {expiry}.")
            return None, None
            
        opt_type = 'put' if is_put else 'call'
        options = [o for o in chain if o['option_type'] == opt_type]
        if not options:
            self._log(f"Skipping {symbol}: No {opt_type}s available in chain for {expiry}.")
            return None, None

        # Check if short strike is available
        short_leg = next((o for o in options if o['strike'] == short_strike), None)
        if not short_leg:
            self._log(f"Skipping {symbol} {side_name}: Target short strike {short_strike} is not available in the chain.")
            return None, None

        return options, short_leg

    def _determine_dynamic_width(self, symbol, options, short_strike, side_name):
        # 2. Universal Dynamic Width
        # Find minimal width from strikes in the chain around the target
        strikes = sorted(set([o['strike'] for o in options]))
        if len(strikes) < 2:
            self._log(f"Skipping {symbol} {side_name}: Not enough strikes available to determine dynamic width.")
            return None
            
        # Find the gap near the short strike
        idx = -1
        try:
            idx = strikes.index(short_strike)
        except ValueError:
            pass # We already verified it exists so this shouldn't happen
            
        dynamic_width = None
        if idx > 0 and idx < len(strikes) - 1:
            dynamic_width = min(strikes[idx] - strikes[idx-1], strikes[idx+1] - strikes[idx])
        elif idx == 0:
            dynamic_width = strikes[1] - strikes[0]
        elif idx == len(strikes) - 1:
            dynamic_width = strikes[idx] - strikes[idx-1]
            
        if dynamic_width is None or dynamic_width <= 0:
            self._log(f"Skipping {symbol} {side_name}: Invalid dynamic width detected.")
            return None
            
        return round(dynamic_width, 2)

    def _find_long_leg_and_credit(self, symbol, options, short_strike, is_put, dynamic_width, side_name, short_leg):
        # 3. Find Long Leg and calculate 12% Rule (incorporating expansion logic up to max 10 wide)
        long_leg = None
        current_width = dynamic_width
        MAX_WIDTH = 10.0
        
        while True:
            if current_width > MAX_WIDTH:
                self._log(f"Skipping {symbol} {side_name}: Expanded width {current_width} exceeds maximum allowed width of {MAX_WIDTH}.")
                return None, None, None

            expected_long_strike = short_strike - current_width if is_put else short_strike + current_width
            expected_long_strike = round(expected_long_strike, 2) # Prevent float comparison issues
            
            long_leg = next((o for o in options if abs(o['strike'] - expected_long_strike) < 0.01), None)
            
            if not long_leg:
                # Expand to next available strike width
                self._log(f"Long strike {expected_long_strike} missing. Expanding width from {current_width} to {current_width + dynamic_width}...")
                current_width = round(current_width + dynamic_width, 2)
                continue
                
            # For a safer fill check, we use natural debit or mid
            short_price = round((short_leg['bid'] + short_leg['ask']) / 2, 2)
            long_price = round((long_leg['bid'] + long_leg['ask']) / 2, 2)
            
            # If quotes are 0 (bad chain), abort or skip
            if short_leg['bid'] == 0 and short_leg['ask'] == 0:
                self._log(f"Skipping {symbol} {side_name}: Options have 0.0 pricing.")
                return None, None, None
                
            net_credit = round(short_price - long_price, 2)
            min_required_credit = round(current_width * 0.12, 2)
            
            if net_credit < min_required_credit:
                self._log(f"Skipping {symbol} {side_name}: Net Credit {net_credit} < 12% limit ({min_required_credit}) for width {current_width}.")
                # Expand width and try again for better credit
                current_width = round(current_width + dynamic_width, 2)
                continue
            else:
                # Passes all filters!
                return long_leg, current_width, net_credit

    def _determine_lots_and_verify_bp(self, symbol, expiry, current_width, config, side_name):
        # 4. Determine Capital Risk & BP Verification
        requirement_per_lot = current_width * 100
        available_bp = self._get_available_bp(config)
        max_lots_config = config.get('max_credit_spreads_per_symbol', 5) if config else 5
        dynamic_lots = int(available_bp // requirement_per_lot)
        
        if dynamic_lots < 1:
            self._log(f"Skipping {symbol}: Spread req (${requirement_per_lot:,.2f}) > BP (${available_bp:,.2f}).")
            return None
            
        dynamic_lots = min(dynamic_lots, max_lots_config)

        # Per-symbol limit: subtract existing positions on this symbol across all expiries
        existing = self._count_existing_on_symbol(symbol)
        dynamic_lots = min(dynamic_lots, max_lots_config - existing)
        if dynamic_lots < 1:
            self._log(f"ℹ️ {symbol} {side_name}: Symbol already at max ({existing}/{max_lots_config}). Skipping.")
            return None
        self._log(f"📦 {symbol} {side_name}: Symbol has {existing}/{max_lots_config} lots. Opening {dynamic_lots} more.")

        total_requirement = requirement_per_lot * dynamic_lots
        if not self._is_bp_sufficient(total_requirement, config):
            self._log(f"Skipping {symbol}: BP insufficient for {dynamic_lots} lots.")
            return None
            
        return dynamic_lots

    def _place_order(self, symbol, short_strike, current_width, net_credit, dynamic_lots, short_leg, long_leg, side_name):
        min_required_credit = round(current_width * 0.12, 2)
        self._log(f"✅ Placing {symbol} {side_name} Spread | Short: {short_strike} | Long: {long_leg['strike']} | Width: {current_width} | Lots: {dynamic_lots} | Credit: {net_credit} (Req: {min_required_credit})")
        
        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': dynamic_lots},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': dynamic_lots}
        ]
        
        if self.dry_run:
            self._log(f"[DRY RUN] Simulating {side_name} Spread Order for {symbol}")
            response = {'id': 'mock_order_id', 'status': 'ok'}
        else:
            if getattr(self, 'trade_manager', None):
                action = TradeAction(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    order_class='multileg',
                    legs=legs,
                    price=net_credit,
                    side='sell',
                    quantity=1,
                    tag=self.strategy_id,
                    strategy_params={'short_leg': short_leg['symbol'], 'long_leg': long_leg['symbol']}
                )
                response = self.trade_manager.execute_strategy_order(action)
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
                    tag="CREDSPRD_7DTE"
                )
            
        if 'error' in response:
             self._log(f"Order failed: {response['error']}")
        else:
             self._log(f"Order placed: {response}")
             if not getattr(self, 'trade_manager', None):
                 legs_info = {
                     'short_leg': short_leg['symbol'],
                     'long_leg': long_leg['symbol']
                 }
                 self._record_trade(symbol, f"Credit Spreads 7 {side_name}", net_credit, response, legs_info)
    def manage_positions(self, simulation_mode=False):
        """
        Check open positions for exit conditions.
        """
        if simulation_mode:
            self.execution_logs = []

        if self.db is None: return

        # 1. Fetch OPEN trades specific to this strategy
        open_trades = self.get_open_trades()

        if not open_trades:
            return self.execution_logs if simulation_mode else None

        active_option_symbols = {}
        if not simulation_mode:
            try:
                positions = self.tradier.get_positions()
                active_option_symbols = {p['symbol']: p for p in positions}
            except Exception as e:
                self._log(f"Error fetching positions for management: {e}")
                return self.execution_logs if simulation_mode else None

        for trade in open_trades:
            symbol = trade['symbol']
            short_leg = trade.get('short_leg')
            long_leg = trade.get('long_leg')
            entry_credit = float(trade.get('price', 0))

            if not simulation_mode and (not short_leg or short_leg not in active_option_symbols):
                 self._log(f"⚠️ Trade {symbol} ({short_leg}) not found in active positions. Ignoring.")
                 continue

            # Parse expiration and strike to calculate width
            match_short = re.search(r'[A-Z]+(\d{6})[PC](\d{8})', short_leg)
            match_long = re.search(r'[A-Z]+\d{6}[PC](\d{8})', long_leg) if long_leg else None
            
            if not match_short or not match_long:
                 continue
                 
            try:
                expiry_date = datetime.strptime(match_short.group(1), '%y%m%d').date()
                dte = (expiry_date - self._get_current_date()).days
                short_strike = float(match_short.group(2)) / 1000.0
                long_strike = float(match_long.group(1)) / 1000.0
                spread_width = abs(short_strike - long_strike)
            except ValueError:
                continue

            # Get natural debit
            should_close = False
            close_reason = ""
            current_debit = 0.0

            try:
                if long_leg:
                    legs_str = f"{short_leg},{long_leg}"
                    q_data = self.tradier.get_quote(legs_str)
                    if isinstance(q_data, dict): legs_quotes = [q_data]
                    elif isinstance(q_data, list): legs_quotes = q_data
                    else: legs_quotes = []

                    sq = next((q for q in legs_quotes if q['symbol'] == short_leg), None)
                    lq = next((q for q in legs_quotes if q['symbol'] == long_leg), None)

                    if sq and lq:
                        sq_ask = float(sq.get('ask', 0))
                        lq_bid = float(lq.get('bid', 0))
                        
                        current_debit = round(sq_ask - lq_bid, 2)
                        
                        # Take Profit (<= 2% of width)
                        target_tp_debit = spread_width * 0.02
                        target_sl_debit = spread_width * 0.36
                        
                        if current_debit <= target_tp_debit:
                            should_close = True
                            close_reason = f"Take Profit (Debit ${current_debit:.2f} <= 2% of width ${spread_width:.2f} [${target_tp_debit:.2f}])"
                            
                        # Stop Loss (>= 36% of width)
                        elif current_debit >= target_sl_debit:
                            should_close = True
                            close_reason = f"Stop Loss (Debit ${current_debit:.2f} >= 36% of width ${spread_width:.2f} [${target_sl_debit:.2f}])"
            except Exception as e:
                self._log(f"Error quoting {short_leg}: {e}")

            # Time Exit check overrides 
            if not should_close and dte <= 0:
                should_close = True
                close_reason = f"Time Exit (DTE {dte} <= 0)"

            if should_close:
                self._log(f"🚨 Closing {symbol} Spread ({short_leg}) — Reason: {close_reason}")
                limit_price = round(current_debit * 1.05, 2) if current_debit > 0 else 0.05
                self._execute_close(trade, limit_price=limit_price, simulation_mode=simulation_mode)

        return self.execution_logs if simulation_mode else None

    def _execute_close(self, trade, limit_price=None, simulation_mode=False):
        short_leg = trade['short_leg']
        long_leg = trade['long_leg']
        legs = [
            {'option_symbol': short_leg, 'side': 'buy_to_close', 'quantity': 1},
            {'option_symbol': long_leg, 'side': 'sell_to_close', 'quantity': 1}
        ]

        if self.dry_run or simulation_mode:
            self._log(f"[DRY RUN/SIM] Closing {trade['symbol']} spread. Limit: {limit_price}")
            if not simulation_mode:
                if getattr(self, 'trade_manager', None):
                    self.trade_manager.mark_trade_closed(trade['_id'], limit_price=limit_price, response_id=None)
                else:
                    self.db['active_trades'].update_one(
                        {"_id": trade['_id']},
                        {"$set": {"status": "CLOSED", "close_date": datetime.now(), "exit_price": limit_price}}
                    )
        else:
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=trade['symbol'],
                side='buy',
                quantity=1,
                order_type='debit',
                duration='day',
                price=limit_price,
                order_class='multileg',
                legs=legs,
                tag="CREDSPRD_7_CLOSE"
            )
            if 'error' in response:
                self._log(f"Close Output Failed: {response['error']}")
            else:
                self._log(f"Close Ordered: {response.get('id')}")
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
