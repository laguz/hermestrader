import traceback
import pytz
from datetime import datetime, timedelta
from bot.strategies.base_strategy import AbstractStrategy
from bot.utils import Colors

class CreditSpreads7Strategy(AbstractStrategy):
    def __init__(self, tradier_service, db, dry_run=False, analysis_service=None, trade_manager=None):
        super().__init__(tradier_service, db, dry_run, analysis_service, trade_manager=trade_manager)

    def _log(self, message):
        super()._log(message, strategy_name="CREDIT_SPREADS_7")

    def execute(self, watchlist, config=None):
        """
        Execute 7DTE Credit Spreads strategy.
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

                # Get expiry exactly 7DTE
                expiry = self._find_exact_7dte_expiry(symbol)
                if not expiry:
                    continue
                
                # Process Put Spreads and Call Spreads Independently
                self._process_side(symbol, current_price, analysis, expiry, is_put=True, config=config)
                self._process_side(symbol, current_price, analysis, expiry, is_put=False, config=config)
                
            except Exception as e:
                self._log(f"❌ Error processing {symbol}: {e}")
                traceback.print_exc()
        
        return self.execution_logs

    def _find_exact_7dte_expiry(self, symbol):
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations:
            self._log(f"No expirations found for {symbol}.")
            return None
        
        today = self._get_current_date()
        target_date = today + timedelta(days=7)
        
        from datetime import date
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
        # 1. Select the POP level
        side_name = "Put" if is_put else "Call"
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
            return
            
        # Find closest to 75%
        target_ep = min(valid_points, key=lambda x: abs(x['pop'] - 75))
        short_strike = target_ep['price']
        pop = target_ep['pop']
        
        self._log(f"Targeting {symbol} {side_name} at exactly 7DTE | Strike: {short_strike} | POP: {pop:.2f}% | Nearest to 75% limit.")

        chain = self.tradier.get_option_chains(symbol, expiry)
        if not chain:
            self._log(f"Skipping {symbol}: Failed to fetch option chain for {expiry}.")
            return
            
        opt_type = 'put' if is_put else 'call'
        options = [o for o in chain if o['option_type'] == opt_type]
        if not options:
            self._log(f"Skipping {symbol}: No {opt_type}s available in chain for {expiry}.")
            return

        # Check if short strike is available
        short_leg = next((o for o in options if o['strike'] == short_strike), None)
        if not short_leg:
            self._log(f"Skipping {symbol} {side_name}: Target short strike {short_strike} is not available in the chain.")
            return

        # 2. Universal Dynamic Width
        # Find minimal width from strikes in the chain around the target
        strikes = sorted(set([o['strike'] for o in options]))
        if len(strikes) < 2:
            self._log(f"Skipping {symbol} {side_name}: Not enough strikes available to determine dynamic width.")
            return
            
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
            return
            
        dynamic_width = round(dynamic_width, 2)
        
        # 3. Find Long Leg and calculate 16% Rule (incorporating expansion logic)
        long_leg = None
        current_width = dynamic_width
        
        while True:
            expected_long_strike = short_strike - current_width if is_put else short_strike + current_width
            expected_long_strike = round(expected_long_strike, 2) # Prevent float comparison issues
            
            long_leg = next((o for o in options if abs(o['strike'] - expected_long_strike) < 0.01), None)
            
            if not long_leg:
                # Expand to next available strike width
                self._log(f"Long strike {expected_long_strike} missing. Expanding width from {current_width} to {current_width + dynamic_width}...")
                current_width = round(current_width + dynamic_width, 2)
                if current_width > dynamic_width * 10:  # safety net to avoid infinite loops
                    self._log(f"Skipping {symbol} {side_name}: Width expanded too far without finding a strike, aborting.")
                    return
                continue
                
            # For a safer fill check, we use natural debit or mid
            short_price = round((short_leg['bid'] + short_leg['ask']) / 2, 2)
            long_price = round((long_leg['bid'] + long_leg['ask']) / 2, 2)
            
            # If quotes are 0 (bad chain), abort or skip
            if short_leg['bid'] == 0 and short_leg['ask'] == 0:
                self._log(f"Skipping {symbol} {side_name}: Options have 0.0 pricing.")
                return
                
            net_credit = round(short_price - long_price, 2)
            min_required_credit = round(current_width * 0.16, 2)
            
            if net_credit < min_required_credit:
                self._log(f"Skipping {symbol} {side_name}: Net Credit {net_credit} < 16% limit ({min_required_credit}) for width {current_width}.")
                return
            else:
                # Passes all filters!
                break
                
        # 4. Determine Capital Risk & BP Verification
        requirement_per_lot = current_width * 100
        available_bp = self._get_available_bp(config)
        max_lots_config = config.get('max_credit_spreads_per_symbol', 5) if config else 5
        dynamic_lots = int(available_bp // requirement_per_lot)
        
        if dynamic_lots < 1:
            self._log(f"Skipping {symbol}: Spread req (${requirement_per_lot:,.2f}) > BP (${available_bp:,.2f}).")
            return
            
        dynamic_lots = min(dynamic_lots, max_lots_config)

        # Per-chain limit: subtract existing positions on this expiry
        existing = self._count_existing_on_expiry(symbol, expiry)
        dynamic_lots = min(dynamic_lots, max_lots_config - existing)
        if dynamic_lots < 1:
            self._log(f"ℹ️ {symbol} {side_name}: Chain {expiry} already at max ({existing}/{max_lots_config}). Skipping.")
            return
        self._log(f"📦 {symbol} {side_name}: Chain {expiry} has {existing}/{max_lots_config} lots. Opening {dynamic_lots} more.")

        total_requirement = requirement_per_lot * dynamic_lots
        if not self._is_bp_sufficient(total_requirement, config):
            self._log(f"Skipping {symbol}: BP insufficient for {dynamic_lots} lots.")
            return
            
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
                response = self.trade_manager.execute_strategy_order(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side='sell',
                    quantity=1,
                    order_type='credit',
                    duration='day',
                    price=net_credit,
                    order_class='multileg',
                    legs=legs,
                    tag=self.strategy_id,
                    strategy_params={'short_leg': short_leg['symbol'], 'long_leg': long_leg['symbol']}
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
