import logging
import traceback
import re
from datetime import datetime
from bot.strategies.base_strategy import AbstractStrategy
from bot.utils import Colors, is_match, get_op_type, get_expiry_str, get_underlying

class CreditSpreadStrategy(AbstractStrategy):
    def __init__(self, tradier_service, db, dry_run=False, analysis_service=None):
        super().__init__(tradier_service, db, dry_run, analysis_service)
        self.min_confidence_score = 7

    def _log(self, message):
        super()._log(message, strategy_name="CREDIT_SPREADS")

    def execute(self, watchlist, config=None):
        """
        Execute the Credit Spread strategy on the watchlist.
        1. Analyze symbol.
        2. Check for entry signals.
        3. Place order if high confidence.
        """
        # ... logic moved to chunks ...
        # analysis_service is now self.analysis_service
        current_hour = self._get_current_datetime().hour

        # Only trade during market hours (roughly)
        # Assuming UTC-5 (EST)
        # Simple check: pass for now, bot loop controls timing.

        for symbol in watchlist:
            try:
                # 1. Check Global & Per-Symbol Limits
                positions = self.tradier.get_positions() or []
                orders = []
                try:
                    orders = self.tradier.get_orders() or []
                except Exception as e:
                    self._log(f"Error fetching orders for limit check: {e}")
                
                # Global Limits removed as per user request.
                # Relying entirely on per-expiry constraints in _check_expiry_constraints logic later.

                # 2. Analyze
                if self.dry_run:
                    print(f"\n{Colors.HEADER}📦 Analyzing {symbol}...{Colors.ENDC}")
                else:
                    self._log(f"Analyzing {symbol}...")
                analysis = self.analysis_service.analyze_symbol(symbol)
                
                if not analysis or 'error' in analysis:
                    self._log(f"⚠️  Analysis failed for {symbol}: {analysis.get('error')}")
                    continue
                    
                # 3. Execution Logic
                current_price = analysis.get('current_price')
                
                max_lots = config.get('max_credit_spreads_per_symbol', 5) if config else 5

                # Attempt Bull Put Spread (if support exists below price)
                self._place_credit_put_spread(symbol, current_price, analysis, max_lots=max_lots, config=config)
                
                # Attempt Bear Call Spread (if resistance exists above price)
                self._place_credit_call_spread(symbol, current_price, analysis, max_lots=max_lots, config=config)
                    
            except Exception as e:
                self._log(f"❌ Error processing {symbol}: {e}")
                traceback.print_exc()
        
        return self.execution_logs
        
    def execute_spread(self, symbol, spread_type, min_credit=None):
        """
        Direct execution entry point for Money Manager.
        """
        # analysis_service is now self.analysis_service
        analysis = self.analysis_service.analyze_symbol(symbol)
        
        if not analysis or 'error' in analysis:
            self._log(f"⚠️  Analysis failed for {symbol}: {analysis.get('error')}")
            return

        current_price = analysis.get('current_price')

        if spread_type == 'put':
            self._place_credit_put_spread(symbol, current_price, analysis, min_credit)
        elif spread_type == 'call':
            self._place_credit_call_spread(symbol, current_price, analysis, min_credit)
        else:
            self._log(f"Unknown spread type: {spread_type}")

    def manage_positions(self, simulation_mode=False):
        """
        Check open positions for exit conditions.
        Condition: If ITM for 2 days straight, close on next day at 3:00 PM EST.
        """
        # Clear logs for this run if simulation
        if simulation_mode:
            self.execution_logs = []
            
        # 1. Check Time (Only run after 3:00 PM EST)
        # Explicitly check for EST time to ensure 3 PM is 3 PM ET.
        if hasattr(self.tradier, 'current_date') and self.tradier.current_date:
             now_est = self.tradier.current_date
        else:
             import pytz
             est = pytz.timezone('America/New_York')
             now_est = datetime.now(est)
        
        # 3 PM EST = 15:00
        # If simulation_mode is True, ignore time check
        if not simulation_mode and now_est.hour < 15: 
            return # Too early, wait for 3 PM EST
            
        now = now_est # Use EST time for logging/checks

        # 2. Get Open Trades from DB
        open_trades = list(self.db['auto_trades'].find({"status": "OPEN"}))
        if not open_trades: return self.execution_logs if simulation_mode else None

        # 3. Verify with Tradier (Source of Truth)
        try:
            positions = self.tradier.get_positions()
        except Exception as e:
            self._log(f"Error fetching positions for management: {e}")
            return self.execution_logs if simulation_mode else None
            
        # Map of Option Symbol -> Position
        active_option_symbols = {p['symbol']: p for p in positions}

        for trade in open_trades:
            symbol = trade['symbol']
            # Check if this trade is still active in Tradier
            # We track by Short Leg mostly (risk leg)
            short_leg = trade.get('short_leg')
            long_leg = trade.get('long_leg')
            
            if not short_leg or short_leg not in active_option_symbols:
                # Position might be closed manually or expired
                # Mark as CLOSED in DB? Or just skip logic?
                # Safer to maybe mark closed if missing, but let's just log and skip for now
                self._log(f"⚠️ Trade {trade.get('symbol')} ({short_leg}) not found in active positions. Ignoring.")
                continue
                
            # Check Check Frequency (Once per day)
            last_check = trade.get('last_check_date')
            today_str = now.strftime('%Y-%m-%d')
            
            # If we already checked today, check if we need to Execute Close
            if last_check == today_str and not simulation_mode:
                if trade.get('close_on_next_day', False):
                    # It's D-Day (Day 3 or later) and we are past 3 PM.
                    self._execute_close(trade, simulation_mode=simulation_mode)
                continue
                
            # Start of New Daily Check
            
            # 0. Check for Pending Close from Previous Day (HARD CLOSE)
            # Logic: If ITM for 2 consecutive days, we schedule a close for the NEXT trading day at 3 PM EST.
            # This is a strict rule. Even if it goes OTM today, the decision was made yesterday.
            if trade.get('close_on_next_day', False):
                 self._log(f"🚨 HARD CLOSE: Executing scheduled close for {symbol} (ITM > 2 days).")
                 self._execute_close(trade, simulation_mode=simulation_mode)
                 continue

            # Check ITM Status
            # We need quote for underlying to check ITM? Or quote for Option?
            # ITM is defined by Underlying Price vs Strike.
            # Get Underlying Quote
            symbol = trade['symbol']
            try:
                quote = self.tradier.get_quote(symbol)
                current_price = quote.get('last')
            except:
                self._log(f"Could not get quote for {symbol}")
                continue
                
            # Determine Strike from DB or Parse Symbol
            # We didn't store Strike explicitly in _record_trade separate fields, but it is in order_details sometimes.
            # BUT we can parse it from option symbol or look at 'order_details'.
            # Tradier Option Symbol: SYMBOLyyMMdd[P|C]00000000
            # Let's rely on stored "short_leg" symbol to parse logic or assume we calculate it?
            # Parsing is safer.
            
            # Helper to parse strike from symbol?
            # Or just check if Tradier says it is ITM? Tradier positions endpoint usually doesn't say ITM.
            # Let's parse.
            # e.g., TSLA230120P00100000
            # Strike is last 8 digits / 1000.
            try:
                strike_part = short_leg[-8:]
                strike = int(strike_part) / 1000.0
                option_type = 'put' if 'P' in short_leg else 'call'
            except:
                self._log(f"Error parsing leg {short_leg}")
                continue
            
            is_itm = False
            if option_type == 'put':
                 if current_price < strike: is_itm = True
            else:
                 if current_price > strike: is_itm = True
            
            # Update Logic
            updates = {
                "last_check_date": today_str
            }
            
            if is_itm:
                new_days = trade.get('days_itm', 0) + 1
                updates['days_itm'] = new_days
                self._log(f"Trade {symbol} {short_leg} is ITM ({current_price} vs {strike}). Days ITM: {new_days}")
                
                if new_days >= 2:
                    updates['close_on_next_day'] = True
                    self._log(f"🚨 Trade {symbol} ITM for 2 days. Scheduled for close next session.")
            else:
                # Reset if OTM?
                # "Two days straight" implies consecutive. So yes, reset.
                if trade.get('days_itm', 0) > 0:
                    self._log(f"Trade {symbol} back OTM. Resetting counter.")
                updates['days_itm'] = 0
                updates['close_on_next_day'] = False
            
            # Save state
            if not simulation_mode:
                self.db['auto_trades'].update_one(
                    {"_id": trade['_id']},
                    {"$set": updates}
                )
            else:
                self._log(f"[SIMULATION] Would update trade state: {updates}")
            
            # If we just flagged it, we DO NOT close today. "Close on the NEXT trading day".
            # So we wait. (Unless trigger below causes immediate close?)

            # --- NEW PROFIT TAKING LOGIC ---
            # Parse Expiry from Short Leg
            # Symbol Format: ROOTyyMMdd[P|C]... e.g. TSLA230120P...
            import re
            match = re.search(r'[A-Z]+(\d{6})[PC]', short_leg)
            if match:
                date_str = match.group(1) # yyMMdd
                try:
                    expiry_date = datetime.strptime(date_str, '%y%m%d')
                    # Calculate DTE
                    dte = (expiry_date.date() - datetime.now().date()).days
                    
                    # Calculate Profit %
                    # Entry Price (Credit)
                    entry_credit = trade.get('price', 0)
                    
                    # Current Price (Debit to Close)
                    # We need quotes for short and long leg to estimate debit
                    
                    # Only check if we haven't already marked for close
                    if not trade.get('close_on_next_day', False) and entry_credit > 0:
                        try:
                             # Re-fetch quotes just for profit check
                             # Use get_quote(symbol) which handles single or comma-list
                             legs_list = [short_leg, long_leg]
                             legs_str = ",".join(legs_list)
                             try:
                                 q_data = self.tradier.get_quote(legs_str)
                                 # Standardize to list
                                 if isinstance(q_data, dict): legs_quotes = [q_data]
                                 elif isinstance(q_data, list): legs_quotes = q_data
                                 else: legs_quotes = []
                             except:
                                 legs_quotes = []

                             sq = next((q for q in legs_quotes if q['symbol'] == short_leg), None)
                             lq = next((q for q in legs_quotes if q['symbol'] == long_leg), None)
                             
                             if sq and lq:
                                 # Debit to Close = Short Ask - Long Bid
                                 # (Buy back Short at Ask, Sell Long at Bid)
                                 curr_debit = (sq.get('ask', 0) - lq.get('bid', 0))
                                 
                                 # Profit = Entry Credit - Current Debit
                                 profit_val = entry_credit - curr_debit
                                 profit_pct = (profit_val / entry_credit)
                                 
                                 self._log(f"📊 Trade {symbol} ({short_leg}) DTE: {dte}, Profit: {profit_pct*100:.1f}% (Entry: {entry_credit}, Curr: {curr_debit:.2f})")

                                 should_close, reason, target_pct = CreditSpreadStrategy.should_close_early(dte, profit_pct)
                                     
                                 if should_close:
                                     # Calculate Strict Limit Price based on user formula
                                     # Limit = Entry Credit * Target Pct (e.g. 1.00 * 0.50 = 0.50)
                                     # Round DOWN to 2 decimals
                                     import math
                                     target_debit = entry_credit * target_pct
                                     limit_price = math.floor(target_debit * 100) / 100.0
                                     
                                     self._log(f"💰 PROFIT TAKING: {reason}. Closing {symbol} at Limit {limit_price}.")
                                     self._execute_close(trade, limit_price=limit_price, simulation_mode=simulation_mode)
                                     continue # Done with this trade

                        except Exception as e:
                            self._log(f"Error checking profit for {short_leg}: {e}")

                except ValueError:
                    self._log(f"Could not parse date from {short_leg}")

    @staticmethod
    def should_close_early(dte, profit_pct):
        """
        Determine if the trade should be closed early based on DTE and Profit %.
        Returns (bool, reason_string, target_pct)
        User Targets:
        - DTE > 13: 50% Profit (Target Debit = Entry * 0.50)
        - 6 < DTE <= 13: 60% Profit (Target Debit = Entry * 0.40)
        - DTE <= 6: 70% Profit (Target Debit = Entry * 0.30)
        """
        target_pct = None
        
        if dte > 13:
            # Target: Pay 50% of credit
            if profit_pct >= 0.50:
                return True, f"Target Met: DTE {dte} > 13, Profit {profit_pct*100:.1f}% >= 50%", 0.50
                
        elif 6 < dte <= 13:
             # Target: Pay 40% of credit
             if profit_pct >= 0.60:
                return True, f"Target Met: 6 < DTE {dte} <= 13, Profit {profit_pct*100:.1f}% >= 60%", 0.40
        
        elif dte <= 6:
             # Target: Pay 30% of credit
             if profit_pct >= 0.70:
                return True, f"Target Met: DTE {dte} <= 6, Profit {profit_pct*100:.1f}% >= 70%", 0.30
            
        return False, "", None
            
        return False, "", None
            
    def _execute_close(self, trade, limit_price=None, simulation_mode=False):
        """Close the spread position using a LIMIT order."""
        self._log(f"Executing Close for {trade['symbol']}...")
        
        # Build closing order (Buy to Close Short, Sell to Close Long)
        short_leg = trade['short_leg']
        long_leg = trade['long_leg']
        
        legs = [
            {'option_symbol': short_leg, 'side': 'buy_to_close', 'quantity': 1},
            {'option_symbol': long_leg, 'side': 'sell_to_close', 'quantity': 1}
        ]
        
        if limit_price is None:
             # Fallback if limit_price not passed (e.g. ITM close)
             # Use Natural Debit + 5% logic as default safe close
             try:
                # Use get_quote(symbol) which handles single or comma-list
                legs_list = [short_leg, long_leg]
                legs_str = ",".join(legs_list)
                try:
                    q_data = self.tradier.get_quote(legs_str)
                    if isinstance(q_data, dict): quotes = [q_data]
                    elif isinstance(q_data, list): quotes = q_data
                    else: quotes = []
                except:
                    quotes = []
                
                short_q = next((q for q in quotes if q['symbol'] == short_leg), {})
                long_q = next((q for q in quotes if q['symbol'] == long_leg), {})
                
                short_ask = short_q.get('ask', 0)
                long_bid = long_q.get('bid', 0)
                natural_debit = short_ask - long_bid
                if natural_debit < 0: natural_debit = 0
                
                limit_price = round(natural_debit * 1.05, 2)
                self._log(f"📉 Calculated Safe Limit for Close: {limit_price} (Natural: {natural_debit:.2f})")
             except Exception as e:
                self._log(f"Error calculating default limit: {e}. Using 0 (Market Risk).")
                limit_price = 0.0 # Will fail if 'debit' order requires price > 0, or be treated as market?
                # Actually for 'debit' type, 0 might be rejected.
                # Let's hope ITM close passes a price or we have quotes.

        if self.dry_run or simulation_mode:
            self._log(f"[DRY RUN/SIM] Closing {trade['symbol']} spread. Limit: {limit_price}")
            # Mark Closed
            if not simulation_mode:
                self.db['auto_trades'].update_one(
                    {"_id": trade['_id']},
                    {"$set": {"status": "CLOSED", "close_date": datetime.now(), "exit_price": limit_price}}
                )
            else:
                self._log(f"[SIMULATION] Would Mark CLOSED in DB.")
        else:
            # Real execution
            # Order Type: 'debit' usually works for Credit Spreads closing (which is a Debit Spread)
            # Price: Limit Price
            
            # If limit_price is 0, we might need to fallback to 'market' order type?
            # But we want to avoid market.
            # Let's try 'debit' with the calculated limit.
            
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=trade['symbol'],
                side='buy', # Not used for multileg but required arg (Buy to close)
                quantity=1,
                order_type='debit', # Limit Debit
                duration='day',
                price=limit_price, # STRICT LIMIT
                order_class='multileg',
                legs=legs,
                tag="CREDIT_SPREADS"
            )
            
            if 'error' in response:
                self._log(f"Close Order Failed: {response['error']}")
            else:
                self._log(f"Close Order Placed: {response.get('id')}")
                # Mark as CLOSED immediately for Backtest simplicity (Live bot might wait for fill)
                # In Backtest, 'place_order' fills immediately.
                self.db['auto_trades'].update_one(
                    {"_id": trade['_id']},
                    {"$set": {
                        "status": "CLOSED", 
                        "close_date": datetime.now(), 
                        "close_order_id": response.get('id'),
                        "exit_price": limit_price
                    }}
                )

    def _find_delta_strike(self, chain, option_type, min_delta=0.30, max_delta=0.37):
        """Find strike with delta closest to min_delta within range."""
        if not chain: return None
        
        # Filter by type
        options = [o for o in chain if o['option_type'] == option_type]
        if not options: return None
        
        candidates = []
        for opt in options:
            greeks = opt.get('greeks')
            if not greeks: continue
            
            delta = greeks.get('delta')
            if delta is None: continue
            
            # Use absolute delta for puts
            abs_delta = abs(delta)
            
            if min_delta <= abs_delta <= max_delta:
                candidates.append((opt, abs_delta))
                
        if not candidates:
            return None
            
        # Sort by distance to ideal delta (let's say we prefer higher premium so strictly higher delta? 
        # User said "delta .30 to .37". 
        # Let's pick the one closest to 0.30 to be safer (further OTM) or 0.37 for more premium?
        # Usually "sell 30 delta" means around 0.30.
        # Let's pick closest to 0.30 (lower risk)
        
        best = min(candidates, key=lambda x: abs(x[1] - 0.30))
        return best[0]['strike']

    def _find_expiry(self, symbol, target_dte=21, min_dte=16, max_dte=22, exclude_dates=None):
        """
        Find available expiry strictly within min_dte and max_dte.
        Range: [min_dte, max_dte] inclusive.
        User Constraint: Strict 3 Weeks (16-22 Days).
        """
        if exclude_dates is None: exclude_dates = []
        
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations: return None
        
        from datetime import date, timedelta
        if isinstance(expirations[0], str):
            # Convert strings to dates
            exp_dates = []
            for e in expirations:
                if exclude_dates and e in exclude_dates: continue
                exp_dates.append(datetime.strptime(e, "%Y-%m-%d").date())
        else:
             # handle date objects if already parsed
            exp_dates = []
            for e in expirations:
                 d_str = e.strftime("%Y-%m-%d")
                 if exclude_dates and d_str in exclude_dates: continue
                 exp_dates.append(e)

        if not exp_dates:
             self._log(f"No valid expirations found (Excluded: {exclude_dates})")
             return None
            
        today = self._get_current_date()
        if isinstance(today, datetime): today = today.date()
        candidates = []
        
        for d in exp_dates:
            dte = (d - today).days
            if min_dte <= dte <= max_dte:
                candidates.append(d)
                
        if not candidates:
            self._log(f"No expirations found in DTE range [{min_dte}, {max_dte}] for {symbol}.")
            return None

        # Sort by proximity to target_dte
        # Target 21 days (3 weeks)
        target_date = today + timedelta(days=target_dte)
        closest_date = min(candidates, key=lambda d: abs((d - today).days - target_dte))
        
        return closest_date.strftime("%Y-%m-%d")

    def _check_expiry_constraints(self, symbol, is_put, max_lots=5):
        """
        Check existing positions + orders to find 'full' expiration weeks.
        Limit: Max 5 Spreads per Side per Expiry (Lots).
        """
        try:
            positions = self.tradier.get_positions() or []
            orders = self.tradier.get_orders() or []
        except:
             return []
        
        # 1. Tally Positions (Lots) by Expiry
        expiry_counts = {}
        target_type_check = 'put' if is_put else 'call'

        from bot.utils import get_expiry_str
        
        for p in positions:
            if not self._is_short_option(p): continue
            
            p_underlying = self._get_underlying_from_pos(p)
            if p_underlying != symbol: continue
            
            # Check Side (Put vs Call)
            # Use regex if option_type missing
            p_type = p.get('option_type')
            if not p_type:
                 if re.search(r'\d{6}P\d+', p['symbol']): p_type = 'put'
                 elif re.search(r'\d{6}C\d+', p['symbol']): p_type = 'call'
            
            if p_type != target_type_check: continue

            # Count Lots
            qty = abs(p.get('quantity', 1))
            
            exp_str = get_expiry_str(p['symbol'])
            if exp_str:
                expiry_counts[exp_str] = expiry_counts.get(exp_str, 0) + qty

        # 2. Tally Orders (Pending)
        # Avoid double counting: For partially filled orders, we only count 'remaining_quantity'
        # since 'exec_quantity' already shows up in 'positions'.
        pending_statuses = ['open', 'partially_filled', 'pending']
        for o in orders:
            status = o.get('status')
            if status not in pending_statuses: continue
            
            o_sym = o.get('symbol')
            o_class = o.get('class')
            
            # Use robust extraction for underlying
            o_underlying = self._get_underlying_from_pos(o)
            if o_underlying != symbol: continue

            legs = o.get('leg') or o.get('legs', [])
            if isinstance(legs, dict): legs = [legs]

            is_target_spread = False
            short_leg_sym = None
            
            if o_class == 'multileg' and legs:
                for leg in legs:
                    if leg.get('side') == 'sell_to_open':
                        lsym = leg.get('option_symbol', '')
                        # Check type (Put vs Call)
                        if is_put and re.search(r'\d{6}P\d+', lsym): 
                            is_target_spread = True
                            short_leg_sym = lsym
                        elif not is_put and re.search(r'\d{6}C\d+', lsym): 
                            is_target_spread = True
                            short_leg_sym = lsym
            
            elif o_class == 'option':
                lsym = o.get('option_symbol', '')
                if not lsym: continue
                
                if o.get('side') == 'sell_to_open':
                    if is_put and re.search(r'\d{6}P\d+', lsym):
                        is_target_spread = True
                        short_leg_sym = lsym
                    elif not is_put and re.search(r'\d{6}C\d+', lsym):
                        is_target_spread = True
                        short_leg_sym = lsym
            
            if is_target_spread and short_leg_sym:
                # Use remaining_quantity for partially_filled to avoid double counting
                if status == 'partially_filled':
                    qty = abs(o.get('remaining_quantity', 0))
                else:
                    qty = abs(o.get('quantity', 0))
                
                exp_str = get_expiry_str(short_leg_sym)
                if exp_str:
                    expiry_counts[exp_str] = expiry_counts.get(exp_str, 0) + qty
                    self._log(f"📝 Pending Order detected: {qty} lot(s) for {exp_str} ({short_leg_sym}, status: {status})")

        # Limit is variable Lots per Expiry
        full_expiries = [exp for exp, count in expiry_counts.items() if count >= max_lots]
        
        if expiry_counts:
             side = "Put" if is_put else "Call"
             self._log(f"📊 Current Tally for {symbol} {side} by Expiry: {expiry_counts} (Limit: {max_lots})")

        if full_expiries:
            side = "Put" if is_put else "Call"
            self._log(f"⚠️ Weekly Limits: Excluding {full_expiries} for {side} Spreads (Max {max_lots} lots met).")
            
        return full_expiries


    def _place_credit_put_spread(self, symbol, current_price, analysis, min_credit=None, max_lots=5, config=None):
        """
        Sell Put at Support, Buy Put lower (defined risk).
        """
        # 1. Early Constraint Check
        exclusions = self._check_expiry_constraints(symbol, is_put=True, max_lots=max_lots)
        # Note: target_dte here is just for sorting preference within the strict min/max range (16-22)
        expiry = self._find_expiry(symbol, target_dte=21, exclude_dates=exclusions)
        if not expiry: 
             self._log(f"🔸 No expiry found for {symbol}")
             return

        # Get Support Levels
        # AnalysisService returns flattened keys now
        entry_points = analysis.get('put_entry_points', [])
        
        # Find Support Levels LOWER than current price AND with 55 <= POP <= 70
        # entry_points are sorted by price ascending.
        # We want the HIGHEST support level that is strictly LOWER than current price.
        all_points_count = len(entry_points)
        valid_points = [
            ep for ep in entry_points 
            if ep['price'] < current_price and 55 <= ep.get('pop', 0) <= 70
        ]
        
        if not valid_points:
            # Fallback to Delta 0.30-0.37
            self._log(f"🔹 No valid support levels found for {symbol}. Checking Delta 0.30-0.37...")
            
            # Check Constraints (Is Put = True)
            exclusions = self._check_expiry_constraints(symbol, is_put=True, max_lots=max_lots)
            expiry = self._find_expiry(symbol, target_dte=30, exclude_dates=exclusions)
            if not expiry: return

            chain = self.tradier.get_option_chains(symbol, expiry)
            delta_strike = self._find_delta_strike(chain, 'put', min_delta=0.30, max_delta=0.37)
            
            if delta_strike:
                 self._log(f"🔹 Found Delta Strike for Put: {delta_strike}")
                 target_strike = delta_strike
                 pop = "N/A (Delta)"
                 # We need to ensure we don't re-fetch chain redundantly but flow is cleaner if we just set target here
                 # and let the logic below re-fetch or pass chain? 
                 # Logic below calls get_option_chains again. That's fine for now (cache/optimization later).
            else:
                 return

        else:
             # Target = The closest support below price (Last item in sorted list < price)
             target_strike = valid_points[-1]['price']
             pop = valid_points[-1].get('pop', 'N/A')
             
             # Expiry already found above

        # Common Logic starts here
        if not 'expiry' in locals() or not expiry: # expiry might be set in if/else
             self._log(f"🔸 No expiry found for {symbol}")
             return

        width = 1.0 if current_price < 100 else 5.0
        short_put_strike = target_strike
        long_put_strike = short_put_strike - width

        self._log(f"✅ Placing Bull Put Spread on {symbol}")
        self._log(f"   • Exp: {expiry} | Short: {short_put_strike} | Long: {long_put_strike} | POP: {pop}%")
        
        # Get Chain to find Option Symbols
        chain = self.tradier.get_option_chains(symbol, expiry)
        if not chain: return
        
        short_leg = next((o for o in chain if o['strike'] == short_put_strike and o['option_type'] == 'put'), None)
        long_leg = next((o for o in chain if o['strike'] == long_put_strike and o['option_type'] == 'put'), None)
        
        if not short_leg or not long_leg:
            self._log("Could not find option legs.")
            return

        # Calculate Price (Credit)
        # Sell Short, Buy Long. Credit = Short Bid - Long Ask (conservative) or Mid - Mid.
        # Let's try Mid point.
        short_price = (short_leg['bid'] + short_leg['ask']) / 2
        long_price = (long_leg['bid'] + long_leg['ask']) / 2
        net_credit = round(short_price - long_price, 2)
        
        # Credit Threshold Check
        threshold = min_credit if min_credit else 0.80
        
        if net_credit < threshold:
            if min_credit:
                self._log(f"⚠️ Market Credit ({net_credit}) < Target ({min_credit}). Placing Limit Order at Target.")
                net_credit = min_credit
            else:
                self._log(f"Credit too low ({net_credit}) for risk (Min 0.80).")
                return

        # BP Check
        requirement = abs(short_put_strike - long_put_strike) * 100
        if not self._is_bp_sufficient(requirement, config):
            return

        # Place Order
        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': 1},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': 1}
        ]
        
        if self.dry_run:
            self._log(f"[DRY RUN] Simulating Bull Put Spread Order for {symbol} @ {net_credit}")
            response = {'id': 'mock_order_id', 'status': 'ok', 'partner_id': 'mock'}
        else:
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='sell', # Not used for multileg but required arg
                quantity=1,
                order_type='credit',
                duration='day',
                price=net_credit,
                order_class='multileg',
                legs=legs,
                tag="CREDIT_SPREADS"
            )
        
        if 'error' in response:
            self._log(f"Order failed: {response['error']}")
        else:
            self._log(f"Order placed: {response}")
            legs_info = {
                 'short_leg': next((l for l in legs if l['side'] == 'sell_to_open'), {}).get('option_symbol'),
                 'long_leg': next((l for l in legs if l['side'] == 'buy_to_open'), {}).get('option_symbol')
            }
            self._record_trade(symbol, "Bull Put Spread", net_credit, response, legs_info)

    def _place_credit_call_spread(self, symbol, current_price, analysis, min_credit=None, max_lots=5, config=None):
        # Similar logic for Bear Call Spread
        # 1. Early Constraint Check
        exclusions = self._check_expiry_constraints(symbol, is_put=False, max_lots=max_lots)
        expiry = self._find_expiry(symbol, target_dte=21, exclude_dates=exclusions)
        if not expiry:
             self._log(f"🔸 No expiry found for {symbol}")
             return

        # Get Resistance Levels
        entry_points = analysis.get('call_entry_points', [])
        if not entry_points: return

        self._log(f"DEBUG: {symbol} Call Entry Points: {entry_points} | Current Price: {current_price}")

        # Find Resistance Levels HIGHER than current price AND with 55 <= POP <= 70
        # entry_points are sorted by price ascending.
        # We want the LOWEST resistance level that is strictly HIGHER than current price.
        all_points_count = len(entry_points)
        valid_points = [
            ep for ep in entry_points 
            if ep['price'] > current_price and 55 <= ep.get('pop', 0) <= 70
        ]
        
        if not valid_points:
             # Fallback to Delta 0.30-0.37
            self._log(f"🔹 No valid resistance levels found for {symbol}. Checking Delta 0.30-0.37...")
            
            # Check Constraints (Is Put = False)
            exclusions = self._check_expiry_constraints(symbol, is_put=False, max_lots=max_lots)
            expiry = self._find_expiry(symbol, target_dte=30, exclude_dates=exclusions)
            if not expiry: return

            chain = self.tradier.get_option_chains(symbol, expiry)
            delta_strike = self._find_delta_strike(chain, 'call', min_delta=0.30, max_delta=0.37)
            
            if delta_strike:
                 self._log(f"🔹 Found Delta Strike for Call: {delta_strike}")
                 target_strike = delta_strike
                 pop = "N/A (Delta)"
            else:
                 return
        else:
            # Target = The closest resistance above price (First item in sorted list > price)
            target_strike = valid_points[0]['price']
            pop = valid_points[0].get('pop', 'N/A')
            
            # Expiry already found above
        
        # Common Logic
        if not 'expiry' in locals() or not expiry:
             self._log(f"🔸 No expiry found for {symbol}")
             return

        width = 1.0 if current_price < 100 else 5.0
        short_call_strike = target_strike
        long_call_strike = short_call_strike + width
        if not expiry:
             self._log(f"🔸 No expiry found for {symbol}")
             return

        self._log(f"✅ Placing Bear Call Spread on {symbol}")
        self._log(f"   • Exp: {expiry} | Short: {short_call_strike} | Long: {long_call_strike} | POP: {pop}%")
        chain = self.tradier.get_option_chains(symbol, expiry)
        
        short_leg = next((o for o in chain if o['strike'] == short_call_strike and o['option_type'] == 'call'), None)
        long_leg = next((o for o in chain if o['strike'] == long_call_strike and o['option_type'] == 'call'), None)
        
        if not short_leg or not long_leg: return
        
        short_price = (short_leg['bid'] + short_leg['ask']) / 2
        long_price = (long_leg['bid'] + long_leg['ask']) / 2
        net_credit = round(short_price - long_price, 2)
        
        # Credit Threshold Check
        threshold = min_credit if min_credit else 0.80
        
        if net_credit < threshold:
            if min_credit:
                self._log(f"⚠️ Market Credit ({net_credit}) < Target ({min_credit}). Placing Limit Order at Target.")
                net_credit = min_credit
            else:
                self._log(f"Credit too low ({net_credit}).")
                return

        # BP Check
        requirement = abs(short_call_strike - long_call_strike) * 100
        if not self._is_bp_sufficient(requirement, config):
            return

        self._log(f"Placing Bear Call Spread on {symbol} Exp: {expiry} Short: {short_call_strike} Long: {long_call_strike}")

        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': 1},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': 1}
        ]
        
        if self.dry_run:
            self._log(f"[DRY RUN] Simulating Bear Call Spread Order for {symbol} @ {net_credit}")
            response = {'id': 'mock_order_id', 'status': 'ok', 'partner_id': 'mock'}
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
                tag="CREDIT_SPREADS"
            )
        
        if 'error' in response:
            self._log(f"Order failed: {response['error']}")
        else:
             self._log(f"Order placed: {response}")
             legs_info = {
                 'short_leg': next((l for l in legs if l['side'] == 'sell_to_open'), {}).get('option_symbol'),
                 'long_leg': next((l for l in legs if l['side'] == 'buy_to_open'), {}).get('option_symbol')
             }
             self._record_trade(symbol, "Bear Call Spread", net_credit, response, legs_info)
