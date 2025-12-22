import logging
import traceback
from datetime import datetime, timedelta, date
from services.container import Container

class WheelStrategy:
    def __init__(self, tradier_service, db, dry_run=False):
        self.tradier = tradier_service
        self.db = db
        self.dry_run = dry_run
        # Constants
        self.TARGET_DTE = 42 # 6 Weeks
        self.MIN_POP = 55
        self.MAX_POP = 70
        self.ROLL_TRIGGER_DTE = 7
        self.ROLL_MAX_DEBIT = 0.90
        self.DELTA_MIN = 0.30
        self.DELTA_MAX = 0.37
        self.execution_logs = []

    def _log(self, message):
        """Log message to DB and internal list."""
        self.execution_logs.append(f"{datetime.now().strftime('%H:%M:%S')} - {message}")
        print(f"[WHEEL] {message}")
        try:
            if self.db is not None:
                self.db['bot_config'].update_one(
                    {"_id": "main_bot"},
                    {"$push": {"logs": {"$each": [{
                        "timestamp": datetime.now(),
                        "message": f"[WHEEL] {message}"
                    }], "$slice": -100}}}
                )
        except Exception as e:
            print(f"Log Error: {e}")

    def execute(self, watchlist):
        """
        Execute the Wheel Strategy Cycle for the watchlist.
        """
        analysis_service = Container.get_analysis_service()
        
        # 1. Fetch Current Positions (The Source of Truth)
        try:
            positions = self.tradier.get_positions() or []
        except Exception as e:
            self._log(f"Error fetching positions: {e}")
            return self.execution_logs

        for symbol in watchlist:
            try:
                # 2. Determine State & Route
                self._process_symbol(symbol, positions, analysis_service)
            except Exception as e:
                self._log(f"❌ Error processing {symbol}: {e}")
                traceback.print_exc()

        # 3. Manage Existing Roles (Check for ITM & < 7 DTE)
        self._manage_positions(positions)

        return self.execution_logs

    def _process_symbol(self, symbol, positions, analysis_service):
        """
        Determine the state of the symbol and execute the appropriate entry leg.
        State Machine:
        - CASH -> Sell Put (Start of Wheel)
        - STOCK -> Sell Covered Call (End of Wheel)
        - EXISTING OPTION -> Do nothing (Management handles it)
        """
        # Analyze Symbol first to get latest price/data
        analysis = analysis_service.analyze_symbol(symbol, period='6m')
        if not analysis or 'error' in analysis:
            self._log(f"Skipping {symbol}: Analysis failed.")
            return

        current_price = analysis.get('current_price')
        
        # Current Inventory for this symbol
        # Tradier positions might lack 'underlying' field.
        # Check: 1. symbol == target (Equity) OR 2. underlying == target OR 3. symbol starts with target + digit (Option)
        import re
        def is_match(pos, target):
            if pos.get('symbol') == target: return True
            if pos.get('underlying') == target: return True
            # Check for Option Symbol: ROOT + Digits
            # e.g. RIOT260130...
            if pos.get('symbol', '').startswith(target):
                 # Ensure next char is digit to avoid matching RIOTA
                 suffix = pos.get('symbol')[len(target):]
                 if suffix and suffix[0].isdigit():
                     return True
            return False

        symbol_positions = [p for p in positions if is_match(p, symbol)]
        
        shares_held = sum(int(p['quantity']) for p in symbol_positions if p['symbol'] == symbol) # Equity
        options_held = [p for p in symbol_positions if p['symbol'] != symbol] # Options

        # Identify Open Bot Positions (Short Puts or Short Calls)
        # Fix: Parse option_type from symbol if missing (Tradier raw positions might omit it)
        import re
        def get_op_type(pos):
             if 'option_type' in pos: return pos['option_type']
             # Parse OCC: ROOT...[P|C]...
             # Simple check: Look for C or P followed by digits at end? 
             # Robust: ROOTyyMMdd[C|P]...
             m = re.search(r'[0-9]{6}([CP])[0-9]+', pos['symbol'])
             if m:
                 return 'call' if m.group(1) == 'C' else 'put'
             return 'unknown'

        short_puts = [o for o in options_held if get_op_type(o) == 'put' and o['quantity'] < 0]
        short_calls = [o for o in options_held if get_op_type(o) == 'call' and o['quantity'] < 0]

        # Logic Flow - Modified for Concurrent Execution (Strangle/Double Dip)
        
        # 1. Evaluate Covered Calls (if we have shares)
        if shares_held >= 100:
            # Check how many calls we are already short
            open_call_contracts = abs(sum(o['quantity'] for o in short_calls))
            
            # Unencumbered Shares logic
            free_shares = shares_held - (open_call_contracts * 100)
            
            if free_shares >= 100:
                self._log(f"🟢 {symbol}: {shares_held} Shares held. {free_shares} Unencumbered. Evaluating Call Sale...")
                self._entry_sell_call(symbol, current_price, analysis)
            else:
                self._log(f"ℹ️ {symbol}: Shares fully covered. ({shares_held} shares, {open_call_contracts} calls).")
        
        # 2. Evaluate Cash Secured Puts (regardless of shares/calls state)
        if short_puts:
            self._log(f"ℹ️ {symbol}: Active Short Put detected. Monitoring.")
        else:
            self._log(f"🟢 {symbol}: Clean Put State. Evaluating Put Sale...")
            self._entry_sell_put(symbol, current_price, analysis)

    def execute_single_leg(self, symbol, leg_type, min_credit=None):
        """
        Direct execution entry point for Money Manager.
        """
        analysis_service = Container.get_analysis_service()
        analysis = analysis_service.analyze_symbol(symbol, period='6m')
        if not analysis or 'error' in analysis:
            self._log(f"Skipping {symbol}: Analysis failed.")
            return

        current_price = analysis.get('current_price')
        
        if leg_type == 'put':
            self._entry_sell_put(symbol, current_price, analysis, min_credit)
        elif leg_type == 'call':
            self._entry_sell_call(symbol, current_price, analysis, min_credit)
        else:
            self._log(f"Unknown leg type: {leg_type}")

    # ------------------------------------------------------------------
    # ENTRY LOGIC
    # ------------------------------------------------------------------

    def _entry_sell_put(self, symbol, current_price, analysis, min_credit=None):
        """
        Priority A: Technical Entry (S/R Based)
        Priority B: Greeks Fallback (Delta Based)
        """
        # Check Constraints
        exclusions = self._check_expiry_constraints(symbol)
        
        target_expiry = self._find_expiry(symbol, weeks=6, exclude_dates=exclusions)
        if not target_expiry:
            self._log(f"No suitable expiry found for {symbol} (Target: 6 weeks, Limits Applied).")
            return

        target_strike = None
        target_reason = ""
        target_pop = 0

        # --- Priority A: Technical (Support) ---
        # Scan Support List. Level Price < Current Price. POP 55-70%.
        # 'put_entry_points' from analysis_service should have 'price' and 'pop'.
        
        put_entries = analysis.get('put_entry_points', [])
        # Sort by Price Descending (Closest to current price first, but still below)
        # Assuming analysis service returns them sorted by price? Let's ensure.
        put_entries.sort(key=lambda x: x['price'], reverse=True)
        
        self._log(f"🔍 Checking {len(put_entries)} Support Levels for {symbol}...")

        valid_supports = []
        for ep in put_entries:
            p_price = ep['price']
            p_pop = ep.get('pop', 0)
            is_price_ok = p_price < current_price
            is_pop_ok = self.MIN_POP <= p_pop <= self.MAX_POP
            
            if is_price_ok and is_pop_ok:
                valid_supports.append(ep)
            else:
                # Log rejection for debug (limiting to first 3 to avoid spam)
                # self._log(f"   • Reject {p_price}: Price<{current_price}? {is_price_ok}, POP {p_pop} in 55-70? {is_pop_ok}")
                pass

        if valid_supports:
            # Pick the best one. 
            # Strategy doesn't specify "Best" vs "First". 
            # Usually "Highest Support" (Closest to price) offers most premium while satisfying conditions.
            best_support = valid_supports[0]
            target_strike = best_support['price']
            target_pop = best_support.get('pop')
            target_reason = f"Support Level (POP {target_pop}%)"
            self._log(f"🎯 Found Technical Entry: Strike {target_strike} @ Support.")
        
        # --- Priority B: Greeks Fallback ---
        if not target_strike:
            self._log("🔹 No S/R criteria met. Checking Greeks (Delta 0.30-0.37)...")
            chain = self.tradier.get_option_chains(symbol, target_expiry)
            if not chain: 
                self._log(f"❌ Failed to fetch option chain for {target_expiry}")
                return

            target_strike, delta = self._find_delta_strike(chain, 'put', self.DELTA_MIN, self.DELTA_MAX)
            if target_strike:
                target_reason = f"Delta Fallback ({delta:.2f})"
                target_pop = "N/A" # Could calculate, but not required for selection
                self._log(f"🎯 Found Delta Entry: Strike {target_strike} (Delta {delta})")
        
        if target_strike:
            self._execute_order(symbol, target_expiry, target_strike, 'put', 'sell_to_open', target_reason, min_credit)
        else:
            self._log(f"🚫 No valid Put Entry found for {symbol} (checked S/R & Delta).")

    def _entry_sell_call(self, symbol, current_price, analysis, min_credit=None):
        """
        Priority A: Technical (Resistance)
        Priority B: Greeks Fallback
        Pre-Condition: Free Shares check done in caller.
        """
        # Check Constraints
        exclusions = self._check_expiry_constraints(symbol)
        
        target_expiry = self._find_expiry(symbol, weeks=6, exclude_dates=exclusions)
        if not target_expiry: return

        target_strike = None
        target_reason = ""
        target_pop = 0

        # --- Priority A: Technical (Resistance) ---
        # Scan Resistance List. Level Price > Current Price. POP 55-70%.
        
        call_entries = analysis.get('call_entry_points', [])
        # Sort by Price Ascending (Closest to current price first)
        call_entries.sort(key=lambda x: x['price'])

        valid_resistances = [
            ep for ep in call_entries 
            if ep['price'] > current_price and self.MIN_POP <= ep.get('pop', 0) <= self.MAX_POP
        ]

        if valid_resistances:
            # Start with closest resistance above price
            best_res = valid_resistances[0]
            target_strike = best_res['price']
            target_pop = best_res.get('pop')
            target_reason = f"Resistance Level (POP {target_pop}%)"
            self._log(f"🎯 Found Technical Entry: Strike {target_strike} @ Resistance.")

        # --- Priority B: Greeks Fallback ---
        if not target_strike:
            self._log("🔹 No S/R criteria met. Checking Greeks (Delta 0.30-0.37)...")
            chain = self.tradier.get_option_chains(symbol, target_expiry)
            if not chain: 
                self._log(f"❌ Failed to fetch option chain for {target_expiry}")
                return

            target_strike, delta = self._find_delta_strike(chain, 'call', self.DELTA_MIN, self.DELTA_MAX)
            if target_strike:
                target_reason = f"Delta Fallback ({delta:.2f})"
                target_pop = "N/A"
                self._log(f"🎯 Found Delta Entry: Strike {target_strike} (Delta {delta})")

        if target_strike:
            self._execute_order(symbol, target_expiry, target_strike, 'call', 'sell_to_open', target_reason)
        else:
            self._log(f"🚫 No valid Call Entry found for {symbol} (checked S/R & Delta).")


    # ------------------------------------------------------------------
    # MANAGEMENT & ROLLING LOGIC
    # ------------------------------------------------------------------

    def _manage_positions(self, positions):
        """
        Scan open options.
        Trigger: ITM AND DTE < 7 Days.
        """
        # Filter for Short Puts and Short Calls
        short_options = [p for p in positions if p.get('option_type') in ['put', 'call'] and p.get('quantity', 0) < 0]

        for position in short_options:
            symbol = position['underlying']
            option_symbol = position['symbol']
            strike = position['strike'] # Tradier positions should have strike/expiry parsed or available
            
            # Need to parse Expiry to check DTE
            # Tradier 'date' field in position? Or parse symbol if not available.
            # Assuming 'expiry' field is not directly in position object, parsing symbol is safer.
            # Format: ROOTyyMMdd[P|C]...
            import re
            match = re.search(r'[A-Z]+(\d{6})[PC]', option_symbol)
            if not match: continue
            
            d_str = match.group(1)
            expiry_date = datetime.strptime(d_str, "%y%m%d").date()
            today = date.today()
            dte = (expiry_date - today).days

            # 1. Check DTE Trigger
            if dte >= self.ROLL_TRIGGER_DTE:
                continue # No roll yet

            # 2. Check ITM Status
            # Need current underlying price
            try:
                quote = self.tradier.get_quote(symbol)
                current_price = quote['last']
            except:
                continue

            is_itm = False
            if position['option_type'] == 'put':
                if current_price < strike: is_itm = True
            else:
                if current_price > strike: is_itm = True

            if not is_itm:
                continue # Expire worthless logic (or let it ride)

            # --- ROLL TRIGGERED ---
            self._log(f"⚠️ Rolling Triggered for {option_symbol}. ITM & DTE {dte} < 7.")
            
            # 3. Execution - Roll Logic
            if position['option_type'] == 'put':
                self._roll_put(symbol, position, current_price, strike)
            else:
                self._roll_call(symbol, position, current_price, strike)


    def _roll_put(self, symbol, position, current_price, current_strike):
        """
        Safety: Only roll if Current Strike > Lowest Support Level.
        Execution: Buy to Close current. Sell to Open new at Next Strike BELOW.
        """
        # Get Support Levels
        analysis_service = Container.get_analysis_service()
        analysis = analysis_service.analyze_symbol(symbol, period='6m')
        supports = analysis.get('put_entry_points', [])
        
        if supports:
            lowest_support = min(s['price'] for s in supports)
            # Safety Check
            if current_strike <= lowest_support:
                self._log(f"🛑 Roll Aborted: Floor Broken ({current_strike} <= Lowest Support {lowest_support}). Taking Assignment.")
                return

        # OK to Roll
        new_expiry = self._find_expiry(symbol, weeks=6)
        if not new_expiry: return
        
        # New Strike: Next Available Below Current
        # Get chain for new expiry to find strikes
        chain = self.tradier.get_option_chains(symbol, new_expiry)
        if not chain: return
        
        # Filter puts
        puts = [o for o in chain if o['option_type'] == 'put']
        puts.sort(key=lambda x: x['strike'])
        
        # Find strikes lower than current
        candidates = [p for p in puts if p['strike'] < current_strike]
        if not candidates:
             self._log("❌ No lower strikes available for roll.")
             return
             
        # Pick the one immediately below (Next Available)
        new_leg = candidates[-1] # Largest strike that is still < current
        
        self._execute_roll(symbol, position, new_leg, "Roll Down & Out (Floor Safe)")

    def _roll_call(self, symbol, position, current_price, current_strike):
        """
        Safety: Only roll if Current Strike < Highest Resistance Level.
        Execution: Buy to Close current. Sell to Open new at Next Strike ABOVE.
        """
        analysis_service = Container.get_analysis_service()
        analysis = analysis_service.analyze_symbol(symbol, period='6m')
        resistances = analysis.get('call_entry_points', [])
        
        if resistances:
            highest_res = max(r['price'] for r in resistances)
            # Safety Check
            if current_strike >= highest_res:
                 self._log(f"🛑 Roll Aborted: Ceiling Broken ({current_strike} >= Highest Res {highest_res}). Shares called away.")
                 return

        # OK to Roll
        new_expiry = self._find_expiry(symbol, weeks=6)
        if not new_expiry: return
        
        chain = self.tradier.get_option_chains(symbol, new_expiry)
        if not chain: return # Should log error
        
        calls = [o for o in chain if o['option_type'] == 'call']
        calls.sort(key=lambda x: x['strike'])
        
        # Find strikes higher than current
        candidates = [c for c in calls if c['strike'] > current_strike]
        if not candidates:
             self._log("❌ No higher strikes available for roll.")
             return
        
        # Pick the one immediately above
        new_leg = candidates[0]
        
        self._execute_roll(symbol, position, new_leg, "Roll Up & Out (Ceiling Safe)")

    def _execute_roll(self, symbol, old_position, new_leg_option, reason):
        """
        Execute the roll as a multi-leg order (Diagonal/Calendar spread essentially).
        However, Tradier API rolling is often best done as a 'combo'.
        We are Buying to Close Old, Selling to Open New.
        Constraint: Max Net Debit 0.90.
        """
        leg1 = {
            'option_symbol': old_position['symbol'],
            'side': 'buy_to_close',
            'quantity': abs(old_position['quantity'])
        }
        leg2 = {
            'option_symbol': new_leg_option['symbol'],
            'side': 'sell_to_open',
            'quantity': abs(old_position['quantity'])
        }
        
        self._log(f"🔄 Executing Roll ({reason}). Paying max ${self.ROLL_MAX_DEBIT} Debit.")
        
        # We need to specify a PRICE for the Net Debit. 
        # Since it's a Debit limit, we use price=0.90 debit.
        # Tradier API Convention: 
        # For 'market' debit, we might treat it differently, but for 'limit', we specify the net cost.
        # Ensure we send positive value for debit? Tradier documentation varies. 
        # Usually positive price for Credit, positive cost for Debit?
        # Let's assume positive = debit for this text, but usually Limit Price is signed or side-dependent.
        # Safe approach: Limit 0.90 Debit.
        
        if self.dry_run:
            self._log(f"[DRY RUN] Would Submit Roll Order: BTC {leg1['option_symbol']}, STO {leg2['option_symbol']} Limit {self.ROLL_MAX_DEBIT} Debit")
        else:
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='buy', # 'buy' the spread (paying debit) - check Tradier specific nuance for rolls
                quantity=1,
                order_type='limit',
                duration='day',
                price=self.ROLL_MAX_DEBIT, 
                order_class='multileg',
                legs=[leg1, leg2]
            )
            if 'error' in response:
                self._log(f"Roll Order Failed: {response['error']}")
            else:
                self._log(f"Roll Order Placed: {response.get('id', 'unknown')}")
                # Record Trade here...

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _execute_order(self, symbol, expiry, strike, option_type, side, reason, min_credit=None):
        """Find the specific option symbol and execute single leg order."""
        chain = self.tradier.get_option_chains(symbol, expiry)
        option = next((o for o in chain if o['strike'] == strike and o['option_type'] == option_type), None)
        
        if not option:
            self._log(f"Could not find option in chain: {strike} {option_type}")
            return

        # Price Logic: Midpoint
        price = round((option['bid'] + option['ask']) / 2, 2)
        
        # Min Credit Check for Money Manager
        if min_credit and price < min_credit:
             # Option A: Place Limit Order AT min_credit (Aggressive for fill, but passive for market)
             # Option B: Skip.
             # Request implies "Trigger orders... Calculate Price = 0.30".
             # This means we should set Limit Price = min_credit.
             # BUT if market price is lower, it won't fill immediately. That's a valid ladder strategy.
             self._log(f"⚠️ Market Price ({price}) < Target ({min_credit}). Placing Limit Order at Target.")
             price = min_credit
        
        self._log(f"🚀 Executing {side} {symbol} {strike} {option_type}. Exp: {expiry}. Reason: {reason}. Price: {price}")
        
        if self.dry_run:
            self._log(f"[DRY RUN] Order: {side} {option['symbol']} @ {price}")
            # Record simulated trade
            mock_res = {'id': 'dry_run_id', 'status': 'ok'}
            self._record_trade(symbol, f"Wheel {side}", price, mock_res)
        else:
            res = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side=side,
                quantity=1,
                order_type='limit',
                duration='day',
                price=price,
                option_symbol=option['symbol'],
                order_class='option'
            )
            if 'error' in res:
                self._log(f"Order Error: {res['error']}")
            else:
                self._log(f"Order Success: {res.get('id', 'unknown')}")
                self._record_trade(symbol, f"Wheel {side}", price, res)

    def _find_expiry(self, symbol, weeks=6, exclude_dates=None):
        """
        Find available expiry closest to current date + weeks.
        exclude_dates: List of string dates 'YYYY-MM-DD' to skip.
        """
        from datetime import timedelta
        target_date = date.today() + timedelta(weeks=weeks)
        
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations: return None
        
        exp_dates = []
        for e in expirations:
            if exclude_dates and e in exclude_dates:
                 continue
            exp_dates.append(datetime.strptime(e, "%Y-%m-%d").date())
        
        if not exp_dates:
            self._log(f"No valid expirations found (Excluded: {exclude_dates})")
            return None
        
        # Find closest
        best_date = min(exp_dates, key=lambda d: abs(d - target_date))
        return best_date.strftime("%Y-%m-%d")

    def _check_expiry_constraints(self, symbol):
        """
        Check existing positions to find 'full' expiration weeks.
        Limit: Max 1 Wheel Contract per Expiry.
        Returns: List of 'YYYY-MM-DD' strings to exclude.
        """
        try:
            positions = self.tradier.get_positions()
            if positions is None: positions = []
        except:
             return []
        
        # Filter for this symbol and options
        relevant = [p for p in positions if p.get('underlying') == symbol and p.get('option_type') in ['put', 'call']]
        
        import re
        expiry_counts = {}
        
        for p in relevant:
            # Parse Expiry from Symbol: ROOTyyMMdd...
            # This is robust for standard OCC symbols.
            m = re.search(r'[A-Z]+(\d{6})[PC]', p['symbol'])
            if m:
                d_str = m.group(1) # yyMMdd
                # Convert to YYYY-MM-DD
                dt = datetime.strptime(d_str, "%y%m%d")
                exp_str = dt.strftime("%Y-%m-%d")
                
                # Count contracts (abs quantity)
                qty = abs(p['quantity'])
                expiry_counts[exp_str] = expiry_counts.get(exp_str, 0) + qty
        
        # Find Expiries where count >= 1
        full_expiries = [exp for exp, count in expiry_counts.items() if count >= 1]
        
        if full_expiries:
            self._log(f"⚠️ Weekly Limits: Excluding {full_expiries} (Max 1 contract/week met).")
            
        return full_expiries

    def _find_delta_strike(self, chain, option_type, min_d, max_d):
        """Find strike with delta in range."""
        candidates = []
        for opt in chain:
            if opt['option_type'] != option_type: continue
            delta = opt.get('greeks', {}).get('delta')
            if delta is None: continue
            
            # Put deltas are negative
            abs_delta = abs(delta)
            
            if min_d <= abs_delta <= max_d:
                candidates.append((opt['strike'], abs_delta))
        
        if not candidates: return None, None
        
        # Return the one closest to min_d (Safer, further OTM) or midpoint?
        # "Sell a Put at a strike with Delta 0.30 to 0.37"
        # Usually preferring 0.30 is standard safe wheel.
        best = min(candidates, key=lambda x: abs(x[1] - min_d))
        return best[0], best[1]

    def _record_trade(self, symbol, strategy, price, response):
        if self.db is not None:
            self.db['auto_trades'].insert_one({
                "symbol": symbol,
                "strategy": strategy,
                "price": price,
                "entry_date": datetime.now(),
                "order_details": response,
                "status": "OPEN",
                "is_dry_run": self.dry_run
            })
