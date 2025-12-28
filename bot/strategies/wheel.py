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

    def execute(self, watchlist, config=None):
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
                max_lots = int(config.get('max_wheel_contracts_per_symbol', 1)) if config else 1
                self._log(f"DEBUG: Processing {symbol} with Max Lots: {max_lots}")
                self._process_symbol(symbol, positions, analysis_service, max_lots=max_lots)
            except Exception as e:
                self._log(f"❌ Error processing {symbol}: {e}")
                traceback.print_exc()

        # 3. Manage Existing Roles (Check for ITM & < 7 DTE)
        self._manage_positions(positions)

        return self.execution_logs

    def _process_symbol(self, symbol, positions, analysis_service, max_lots=1):
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
                self._entry_sell_call(symbol, current_price, analysis, max_lots=max_lots)
            else:
                self._log(f"ℹ️ {symbol}: Shares fully covered. ({shares_held} shares, {open_call_contracts} calls).")
        
        # 2. Evaluate Cash Secured Puts (regardless of shares/calls state)
        # Note: Global limit check removed per user request. Limit is enforced per-expiry in _entry_sell_put.
        open_put_contracts = sum(abs(p['quantity']) for p in short_puts)
        self._log(f"🟢 {symbol}: Clean or Partial Put State ({open_put_contracts} active). Evaluating Put Sale...")
        self._entry_sell_put(symbol, current_price, analysis, max_lots=max_lots)

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

    def _entry_sell_put(self, symbol, current_price, analysis, min_credit=None, max_lots=1):
        """
        Priority A: Technical Entry (S/R Based)
        Priority B: Greeks Fallback (Delta Based)
        """
        # Check Constraints
        exclusions = self._check_expiry_constraints(symbol, max_lots=max_lots)
        
        target_expiry = self._find_expiry(symbol, target_dte=42, min_dte=37, max_dte=43, exclude_dates=exclusions)
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

    def _entry_sell_call(self, symbol, current_price, analysis, min_credit=None, max_lots=1):
        """
        Priority A: Technical (Resistance)
        Priority B: Greeks Fallback
        Pre-Condition: Free Shares check done in caller.
        """
        # Check Constraints
        exclusions = self._check_expiry_constraints(symbol, max_lots=max_lots)
        
        target_expiry = self._find_expiry(symbol, target_dte=42, min_dte=37, max_dte=43, exclude_dates=exclusions)
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
        Trigger: ITM AND DTE <= 7 Days.
        """
        import re

        for position in positions:
            # 1. Enrich Data if missing (Tradier raw data might lack keys)
            symbol = position.get('symbol', '')
            underlying = position.get('underlying')
            option_type = position.get('option_type')
            strike = position.get('strike')
            
            # Parsing OCC Symbol: e.g., RIOT251226P00015000
            # Group 1: Underlying (letters)
            # Group 2: Date (6 digits)
            # Group 3: Type (C/P)
            # Group 4: Strike (8 digits, implied decimal at 3 from right)
            match = re.match(r'^([A-Z]+)(\d{6})([CP])(\d{8})$', symbol)
            
            if match:
                if not underlying: underlying = match.group(1)
                if not option_type: option_type = 'call' if match.group(3) == 'C' else 'put'
                if not strike: 
                    # Strike is last 8 digits, divide by 1000
                    strike = float(match.group(4)) / 1000.0
                
                # We can also get expiry date from group 2
                d_str = match.group(2)
                expiry_date = datetime.strptime(d_str, "%y%m%d").date()
            else:
                # Not a standard option symbol or cannot parse
                if not (underlying and option_type and strike):
                    continue
                # Try to get expiry logic from existing symbol parsing if possible
                # Retain old parsing for partial matches if needed? 
                # Better to rely on the robust regex above.
                pass

            # Filter for Short Options (Short Puts or Short Calls)
            if position.get('quantity', 0) >= 0:
                continue
            if option_type not in ['put', 'call']:
                continue

            # Calculate DTE if we parsed expiry
            if 'expiry_date' not in locals():
                 # Handle case where we didn't parse it above (rare if match fails)
                 # Try parsing again just for date if regex missed?
                 m2 = re.search(r'[A-Z]+(\d{6})[PC]', symbol)
                 if m2:
                     expiry_date = datetime.strptime(m2.group(1), "%y%m%d").date()
                 else:
                     continue
            
            # 1. Get Today (Simulation Aware)
            if hasattr(self.tradier, 'current_date') and self.tradier.current_date:
                today = self.tradier.current_date.date()
            else:
                today = date.today()
                
            dte = (expiry_date - today).days

            # 1. Check DTE Trigger
            if dte > self.ROLL_TRIGGER_DTE: # STRICTLY > 7 means 8+. So 7 matches trigger.
                continue 
            
            # ... (rest of loop)

    # ...


    def _execute_order(self, symbol, expiry, strike, option_type, side, reason, min_credit=None):
        """Find the specific option symbol and execute single leg order."""
        chain = self.tradier.get_option_chains(symbol, expiry)
        option = next((o for o in chain if o['strike'] == strike and o['option_type'] == option_type), None)
        
        if not option:
            self._log(f"Could not find option in chain: {strike} {option_type}")
            return

        # Price Logic: Bid - 0.01 (Aggressive)
        price = round(option['bid'] - 0.01, 2)
        
        # Min Value Check (0.30)
        if price < 0.30:
            self._log(f"🚫 Aggressive Entry Aborted: Price {price} < 0.30 Minimum.")
            return

        # Explicit Min Credit Check (if provided by caller, though usually lower priority than the 0.30 hard floor)
        if min_credit and price < min_credit:
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
                # Record Trade here...
                self._record_trade(symbol, f"Wheel {side}", price, res)

    def _find_expiry(self, symbol, target_dte=42, min_dte=None, max_dte=None, exclude_dates=None, method='closest'):
        """
        Find available expiry.
        target_dte: Target days from today (default 42).
        min_dte, max_dte: Optional range filter (inclusive).
        exclude_dates: List of 'YYYY-MM-DD' strings to skip.
        method: 'closest' (default) or 'min' (pick lowest DTE in range).
        """
        from datetime import timedelta
        
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
        
        # Simulation Aware Today
        if hasattr(self.tradier, 'current_date') and self.tradier.current_date:
            today = self.tradier.current_date.date()
        else:
            today = date.today()

        candidates = []
        
        # 1. Filter by Range if specified
        for d in exp_dates:
            if min_dte is not None and max_dte is not None:
                dte = (d - today).days
                if not (min_dte <= dte <= max_dte):
                    continue
            candidates.append(d)
        
        if not candidates:
            rng = f"[{min_dte}, {max_dte}]" if min_dte else "Any"
            self._log(f"No expirations found in DTE range {rng} for {symbol}.")
            return None

        # 2. Select
        if method == 'min':
            # Pick the lowest DTE available
            best_date = min(candidates)
        else:
            # Pick closest to Target
            target_date = today + timedelta(days=target_dte)
            best_date = min(candidates, key=lambda d: abs((d - today).days - target_dte))
            
        return best_date.strftime("%Y-%m-%d")

    def _check_expiry_constraints(self, symbol, max_lots=1):
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
        
        
        import re
        expiry_counts = {}
        
        # Iterate ALL positions and robustly check if they belong to this symbol
        for p in positions:
            sym_raw = p.get('symbol', '')
            
            # Robust Match: START with underlying characters
            # e.g. RIOT250117... or TSLA25...
            # We want to match if it STARTS with "SYMBOL" + "Digit" (Option)
            # OR invalid case where underlying is missing but symbol is correct
            
            # Option 1: Quick strict check
            if not sym_raw.startswith(symbol):
                continue
            
            # Option 2: Ensure the char AFTER symbol is a digit (invulnerable to RIOTA vs RIOT)
            remaining = sym_raw[len(symbol):]
            if not remaining or not remaining[0].isdigit():
                continue
            
            # Now we know it's a derivative of 'symbol' (e.g. RIOT...)
            
            # Parse Expiry: ROOTyyMMdd...
            m = re.search(r'[A-Z]+(\d{6})[PC]', sym_raw)
            if m:
                d_str = m.group(1) # yyMMdd
                try:
                    dt = datetime.strptime(d_str, "%y%m%d")
                    exp_str = dt.strftime("%Y-%m-%d")
                    
                    # Count contracts (abs quantity)
                    qty = abs(p['quantity'])
                    expiry_counts[exp_str] = expiry_counts.get(exp_str, 0) + qty
                except Exception as e:
                    self._log(f"Error parsing date from {sym_raw}: {e}")

        # ------------------------------------------------------------------
        # NEW: Count Pending Orders too (Avoids multi-ordering)
        # ------------------------------------------------------------------
        try:
            orders = self.tradier.get_orders() or []
        except:
            orders = []

        relevant_orders = [
            o for o in orders 
            if o.get('symbol') == symbol 
            and o.get('status') in ['open', 'partially_filled', 'pending']
            and o.get('side') == 'sell_to_open'
            and o.get('class') == 'option'
        ]

        for o in relevant_orders:
            # Parse expiry from option_symbol e.g. ROOTyyMMdd...
            osym = o.get('option_symbol')
            if not osym: continue
            
            m_ord = re.search(r'[A-Z]+(\d{6})[PC]', osym)
            if m_ord:
                d_str = m_ord.group(1)
                dt = datetime.strptime(d_str, "%y%m%d")
                exp_str = dt.strftime("%Y-%m-%d")
                
                qty = int(o.get('quantity', 0))
                # Add to existing count
                expiry_counts[exp_str] = expiry_counts.get(exp_str, 0) + qty
                self._log(f"📝 Pending Order Counted: {qty} for {exp_str}")
        
        # Limit: Max Variable Wheel Contracts per Expiry.
        full_expiries = [exp for exp, count in expiry_counts.items() if count >= max_lots]
        
        if full_expiries:
            self._log(f"⚠️ Weekly Limits: Excluding {full_expiries} (Max {max_lots} contract/week met).")
            
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
