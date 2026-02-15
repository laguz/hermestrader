import logging
import traceback
import re
from datetime import datetime, timedelta, date
from services.container import Container
from bot.strategies.base_strategy import AbstractStrategy
from bot.utils import is_match, get_op_type, get_expiry_str, get_underlying

class WheelStrategy(AbstractStrategy):
    def __init__(self, tradier_service, db, dry_run=False, analysis_service=None):
        super().__init__(tradier_service, db, dry_run, analysis_service)
        # Constants
        self.TARGET_DTE = 42 # 6 Weeks
        self.MIN_POP = 55
        self.MAX_POP = 70
        self.ROLL_TRIGGER_DTE = 7
        self.ROLL_MAX_DEBIT = 0.90
        self.DELTA_MIN = 0.30
        self.DELTA_MAX = 0.37

    def _log(self, message):
        super()._log(message, strategy_name="WHEEL")

    def execute(self, watchlist, config=None):
        """
        Execute the Wheel Strategy Cycle for the watchlist.
        """
        self.config = config or {}
        analysis_service = self.analysis_service or Container.get_analysis_service()
        
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
        self._manage_positions(positions, watchlist=watchlist)

        return self.execution_logs

    def _process_symbol(self, symbol, positions, analysis_service, max_lots=1):
        """
        Determine the state of the symbol and execute the appropriate entry leg.
        """
        # Analyze Symbol first to get latest price/data
        analysis = analysis_service.analyze_symbol(symbol, period='6m')
        if not analysis or 'error' in analysis:
            self._log(f"Skipping {symbol}: Analysis failed.")
            return

        current_price = analysis.get('current_price')
        
        symbol_positions = [p for p in positions if is_match(p, symbol)]
        
        shares_held = sum(int(p['quantity']) for p in symbol_positions if p['symbol'] == symbol) # Equity
        options_held = [p for p in symbol_positions if p['symbol'] != symbol] # Options

        short_puts = [o for o in options_held if get_op_type(o) == 'put' and o['quantity'] < 0]
        short_calls = [o for o in options_held if get_op_type(o) == 'call' and o['quantity'] < 0]

        # 1. Evaluate Covered Calls (if we have shares)
        if shares_held >= 100:
            open_call_contracts = abs(sum(o['quantity'] for o in short_calls))
            free_shares = shares_held - (open_call_contracts * 100)
            
            if free_shares >= 100:
                self._log(f"🟢 {symbol}: {shares_held} Shares held. {free_shares} Unencumbered. Evaluating Call Sale...")
                self._entry_sell_call(symbol, current_price, analysis, max_lots=max_lots)
            else:
                self._log(f"ℹ️ {symbol}: Shares fully covered. ({shares_held} shares, {open_call_contracts} calls).")
        
        # 2. Evaluate Cash Secured Puts
        open_put_contracts = sum(abs(p['quantity']) for p in short_puts)
        if open_put_contracts >= max_lots:
            self._log(f"ℹ️ {symbol}: Max put contracts reached ({open_put_contracts}/{max_lots}). Skipping new entry.")
        else:
            self._log(f"🟢 {symbol}: Put slot available ({open_put_contracts}/{max_lots}). Evaluating Put Sale...")
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
        put_entries = analysis.get('put_entry_points', [])
        put_entries.sort(key=lambda x: x['price'], reverse=True)
        
        self._log(f"🔍 Checking {len(put_entries)} Support Levels for {symbol}...")

        valid_supports = []
        for ep in put_entries:
            p_price = ep['price']
            p_pop = ep.get('pop', 0)
            if p_price < current_price and self.MIN_POP <= p_pop <= self.MAX_POP:
                valid_supports.append(ep)

        if valid_supports:
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
                self._log(f"🎯 Found Delta Entry: Strike {target_strike} (Delta {delta})")
        
        if target_strike:
            self._execute_order(symbol, target_expiry, target_strike, 'put', 'sell_to_open', target_reason, min_credit)
        else:
            self._log(f"🚫 No valid Put Entry found for {symbol} (checked S/R & Delta).")

    def _entry_sell_call(self, symbol, current_price, analysis, min_credit=None, max_lots=1):
        """
        Priority A: Technical (Resistance)
        Priority B: Greeks Fallback
        """
        # Check Constraints
        exclusions = self._check_expiry_constraints(symbol, max_lots=max_lots)
        
        target_expiry = self._find_expiry(symbol, target_dte=42, min_dte=37, max_dte=43, exclude_dates=exclusions)
        if not target_expiry: return

        target_strike = None
        target_reason = ""

        # --- Priority A: Technical (Resistance) ---
        call_entries = analysis.get('call_entry_points', [])
        call_entries.sort(key=lambda x: x['price'])

        valid_resistances = [
            ep for ep in call_entries 
            if ep['price'] > current_price and self.MIN_POP <= ep.get('pop', 0) <= self.MAX_POP
        ]

        if valid_resistances:
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
                self._log(f"🎯 Found Delta Entry: Strike {target_strike} (Delta {delta})")

        if target_strike:
            self._execute_order(symbol, target_expiry, target_strike, 'call', 'sell_to_open', target_reason)
        else:
            self._log(f"🚫 No valid Call Entry found for {symbol} (checked S/R & Delta).")

    def _manage_positions(self, positions, watchlist=None):
        """
        Scan open options and trigger roll if ITM AND DTE <= 7 Days.
        """
        for position in positions:
            symbol = position.get('symbol', '')
            underlying = get_underlying(symbol)
            
            if watchlist is not None and underlying not in watchlist:
                continue

            option_type = get_op_type(position)
            if option_type not in ['put', 'call'] or position.get('quantity', 0) >= 0:
                continue
            
            # Parse Strike and Expiry
            match = re.match(r'^([A-Z]+)(\d{6})[CP](\d{8})$', symbol)
            if not match: continue
            
            strike = float(match.group(3)) / 1000.0
            expiry_str = get_expiry_str(symbol)
            if not expiry_str: continue
            expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
            
            today = self._get_current_date()
            dte = (expiry_date - today).days

            if dte >= self.ROLL_TRIGGER_DTE: 
                continue 
            
            self._log(f"🔍 {symbol} management: DTE {dte} < {self.ROLL_TRIGGER_DTE}. Checking ITM status...")

            try:
                quote = self.tradier.get_quote(underlying)
                if not quote: continue
                
                current_price = float(quote.get('last'))
                is_itm = (option_type == 'put' and current_price < strike) or \
                         (option_type == 'call' and current_price > strike)
                
                if not is_itm:
                    self._log(f"ℹ️ {symbol} is OTM (Price {current_price}, Strike {strike}). Allowing to expire.")
                    continue

                self._log(f"🚨 {symbol} is ITM (Price {current_price}, Strike {strike}). Triggering ROLL.")

                # Execute Roll
                chain = self.tradier.get_option_chains(underlying, expiry_str)
                current_option = next((o for o in chain if o['strike'] == strike and o['option_type'] == option_type), None)
                if not current_option: continue

                close_price = current_option['ask']
                new_expiry = self._find_expiry(underlying, target_dte=42, min_dte=42, max_dte=63)
                if not new_expiry: continue

                target_strike = strike - 1.0
                new_chain = self.tradier.get_option_chains(underlying, new_expiry)
                new_option = next((o for o in new_chain if o['strike'] == target_strike and o['option_type'] == option_type), None)
                
                if not new_option:
                    candidates = [o for o in new_chain if o['option_type'] == option_type]
                    if not candidates: continue
                    new_option = min(candidates, key=lambda x: abs(x['strike'] - target_strike))
                    self._log(f"⚠️ Target strike {target_strike} not available. Snapping to {new_option['strike']}.")

                open_price = round(new_option['bid'] - 0.01, 2)
                net_credit = open_price - close_price
                if net_credit < -self.ROLL_MAX_DEBIT:
                    self._log(f"🚫 Roll Aborted: Net Credit {net_credit:.2f} would exceed max debit {self.ROLL_MAX_DEBIT}.")
                    continue

                self._log(f"🔄 Rolling {symbol} to {new_expiry} Strike {new_option['strike']}. Net: {net_credit:.2f}")

                if net_credit < 0 and not self._is_bp_sufficient(abs(net_credit) * 100):
                     continue

                if self.dry_run:
                    self._log(f"[DRY RUN] Rollover: BTC {symbol} @ {close_price}, STO {new_option['symbol']} @ {open_price}")
                    self._record_trade(underlying, "Wheel Roll BTC", close_price, {'id': 'dry_run_btc'})
                    self._record_trade(underlying, "Wheel Roll STO", open_price, {'id': 'dry_run_sto'})
                else:
                    # BTC
                    btc_res = self.tradier.place_order(
                        account_id=self.tradier.account_id,
                        symbol=underlying,
                        side='buy_to_close',
                        quantity=abs(int(position['quantity'])),
                        order_type='limit',
                        duration='day',
                        price=close_price,
                        option_symbol=symbol,
                        order_class='option',
                        tag="WHEEL"
                    )
                    
                    if 'error' in btc_res:
                        self._log(f"❌ BTC Error: {btc_res['error']}")
                        continue
                    
                    self._record_trade(underlying, "Wheel Roll BTC", close_price, btc_res)

                    # STO
                    sto_res = self.tradier.place_order(
                        account_id=self.tradier.account_id,
                        symbol=underlying,
                        side='sell_to_open',
                        quantity=abs(int(position['quantity'])),
                        order_type='limit',
                        duration='day',
                        price=open_price,
                        option_symbol=new_option['symbol'],
                        order_class='option',
                        tag="WHEEL"
                    )
                    
                    if 'error' in sto_res:
                        self._log(f"❌ STO Error: {sto_res['error']}")
                    else:
                        self._record_trade(underlying, "Wheel Roll STO", open_price, sto_res)

            except Exception as e:
                self._log(f"❌ Error managing {symbol}: {e}")
                traceback.print_exc()

    def _execute_order(self, symbol, expiry, strike, option_type, side, reason, min_credit=None):
        """Find the specific option symbol and execute single leg order."""
        chain = self.tradier.get_option_chains(symbol, expiry)
        option = next((o for o in chain if o['strike'] == strike and o['option_type'] == option_type), None)
        
        if not option:
            candidates = [o for o in chain if o['option_type'] == option_type]
            if candidates:
                option = min(candidates, key=lambda x: abs(x['strike'] - strike))
                self._log(f"⚠️ Exact strike {strike} not found. Snapping to {option['strike']}.")
            else: return

        # Price Logic: Bid - 0.01 (Aggressive)
        price = round(option['bid'] - 0.01, 2)
        if price < 0.30:
            self._log(f"🚫 Aggressive Entry Aborted: Price {price} < 0.30 Minimum.")
            return

        if min_credit and price < min_credit:
             self._log(f"⚠️ Market Price ({price}) < Target ({min_credit}). Placing Limit Order at Target.")
             price = min_credit
        
        self._log(f"🚀 Executing {side} {symbol} {strike} {option_type}. Exp: {expiry}. Reason: {reason}. Price: {price}")
        
        requirement = (strike * 100) if option_type == 'put' and 'sell' in side else 0
        if not self._is_bp_sufficient(requirement):
            return

        if self.dry_run:
            self._log(f"[DRY RUN] Order: {side} {option['symbol']} @ {price}")
            self._record_trade(symbol, f"Wheel {side}", price, {'id': 'dry_run_id'})
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
                order_class='option',
                tag="WHEEL"
            )
            if 'error' in res:
                self._log(f"Order Error: {res['error']}")
            else:
                self._record_trade(symbol, f"Wheel {side}", price, res)

    def _check_expiry_constraints(self, symbol, max_lots=1):
        """
        Check existing positions to find 'full' expiration weeks.
        """
        try:
            positions = self.tradier.get_positions() or []
        except: return []
        
        expiry_counts = {}

        # 1. Tally Positions (Lots) by Expiry
        for p in positions:
            if get_underlying(p['symbol']) != symbol or p['symbol'] == symbol:
                continue

            exp_str = get_expiry_str(p['symbol'])
            if exp_str:
                expiry_counts[exp_str] = expiry_counts.get(exp_str, 0) + abs(p.get('quantity', 1))

        # 2. Count Pending Orders
        try:
            orders = self.tradier.get_orders() or []
        except: orders = []

        pending_statuses = ['open', 'partially_filled', 'pending']
        for o in orders:
            if o.get('status') not in pending_statuses or get_underlying(o.get('symbol')) != symbol:
                continue

            if o.get('side') != 'sell_to_open' or o.get('class') != 'option':
                continue

            osym = o.get('option_symbol')
            exp_str = get_expiry_str(osym)
            if exp_str:
                qty = abs(o.get('remaining_quantity' if o['status'] == 'partially_filled' else 'quantity', 0))
                expiry_counts[exp_str] = expiry_counts.get(exp_str, 0) + qty
                self._log(f"📝 Pending Order detected: {qty} lot(s) for {exp_str} ({osym})")
        
        full_expiries = [exp for exp, count in expiry_counts.items() if count >= max_lots]
        if full_expiries:
            self._log(f"⚠️ Weekly Limits: Excluding {full_expiries} (Max {max_lots} contract/week met).")
            
        return full_expiries
