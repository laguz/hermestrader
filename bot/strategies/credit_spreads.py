import logging
import traceback
import re
import math
import pytz
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
                analysis = self.analysis_service.analyze_symbol(symbol, period='3m')
                
                if not analysis or 'error' in analysis:
                    self._log(f"⚠️  Analysis failed for {symbol}: {analysis.get('error')}")
                    continue
                    
                # 3. Execution Logic
                current_price = analysis.get('current_price')
                
                # Retrieve directional trend (added for Upgrade 1: Trend Filter)
                trend = analysis.get('trend', 'neutral')
                
                # Upgrade 4: Capital-Based Limits 
                # Let's target roughly $500 risk per symbol as a base constraint before checking dynamic width
                # But we'll refine this inside the individual spread generator where width is known.
                # For now, we still calculate 'current_total' lots, but we don't strictly reject on it yet
                # We'll pass `config` down and evaluate true BP risk.
                max_lots_baseline = config.get('max_credit_spreads_per_symbol', 5) if config else 5
                current_total = self._count_total_spreads(symbol)
                
                orders_placed_this_run = 0

                # Attempt Bull Put Spread (if support exists below price)
                # Trend Filter: Only execute if trend is NOT strongly bearish
                if trend != 'bearish':
                    if (current_total + orders_placed_this_run) < max_lots_baseline:
                        if self._place_credit_put_spread(symbol, current_price, analysis, max_lots=max_lots_baseline, config=config):
                            orders_placed_this_run += 1
                else:
                    self._log(f"📉 Trend is Bearish for {symbol}, skipping Bull Put Spread.")
                
                # Attempt Bear Call Spread (if resistance exists above price)
                # Trend Filter: Only execute if trend is NOT strongly bullish
                if trend != 'bullish':
                    if (current_total + orders_placed_this_run) < max_lots_baseline:
                        self._place_credit_call_spread(symbol, current_price, analysis, max_lots=max_lots_baseline, config=config)
                else:
                    self._log(f"📈 Trend is Bullish for {symbol}, skipping Bear Call Spread.")
                    
            except Exception as e:
                self._log(f"❌ Error processing {symbol}: {e}")
                traceback.print_exc()
        
        return self.execution_logs
    def manage_positions(self, simulation_mode=False):
        """
        Check open positions for exit conditions.
        Close when DTE < 3 AND (short leg is ITM OR debit >= 2.5x entry credit).
        """
        if simulation_mode:
            self.execution_logs = []

        # 1. Check Time (Only run after 10:30 AM EST)
        if hasattr(self.tradier, 'current_date') and self.tradier.current_date:
            now_est = self.tradier.current_date
        else:
            est = pytz.timezone('America/New_York')
            now_est = datetime.now(est)

        if not simulation_mode:
            if now_est.hour < 10 or (now_est.hour == 10 and now_est.minute < 30):
                return

        # 2. Get Open Trades from DB
        open_trades = list(self.db['auto_trades'].find({"status": "OPEN"}))
        if not open_trades:
            return self.execution_logs if simulation_mode else None

        # 3. Verify with Tradier (Source of Truth)
        try:
            positions = self.tradier.get_positions()
        except Exception as e:
            self._log(f"Error fetching positions for management: {e}")
            return self.execution_logs if simulation_mode else None

        active_option_symbols = {p['symbol']: p for p in positions}

        for trade in open_trades:
            symbol = trade['symbol']
            short_leg = trade.get('short_leg')
            long_leg = trade.get('long_leg')

            if not short_leg or short_leg not in active_option_symbols:
                self._log(f"⚠️ Trade {symbol} ({short_leg}) not found in active positions. Ignoring.")
                continue

            # --- Parse DTE from short_leg (format: ROOTyyMMdd[P|C]...) ---
            match = re.search(r'[A-Z]+(\d{6})[PC]', short_leg)
            if not match:
                self._log(f"Could not parse expiry from {short_leg}")
                continue
            try:
                expiry_date = datetime.strptime(match.group(1), '%y%m%d')
                dte = (expiry_date.date() - self._get_current_date()).days
            except ValueError:
                self._log(f"Could not parse date from {short_leg}")
                continue

            # --- Primary Gate: only evaluate close when DTE < 3 ---
            if dte >= 3:
                self._log(f"⏳ {symbol} ({short_leg}) DTE: {dte} — holding.")
                continue

            self._log(f"⚠️ {symbol} ({short_leg}) DTE: {dte} < 3 — evaluating close conditions.")

            # --- Get underlying quote for ITM check ---
            try:
                quote = self.tradier.get_quote(symbol)
                current_price = quote.get('last')
            except Exception as e:
                self._log(f"Could not get quote for {symbol}: {e}")
                continue

            # --- Parse strike and option type ---
            try:
                strike = int(short_leg[-8:]) / 1000.0
                option_type = 'put' if 'P' in short_leg else 'call'
            except Exception:
                self._log(f"Error parsing leg {short_leg}")
                continue

            is_itm = (option_type == 'put' and current_price < strike) or \
                     (option_type == 'call' and current_price > strike)

            # --- Check 2.5x debit stop loss ---
            should_stop = False
            curr_debit = 0.0
            entry_credit = trade.get('price', 0)

            if entry_credit > 0 and long_leg:
                try:
                    legs_str = f"{short_leg},{long_leg}"
                    q_data = self.tradier.get_quote(legs_str)
                    if isinstance(q_data, dict):
                        legs_quotes = [q_data]
                    elif isinstance(q_data, list):
                        legs_quotes = q_data
                    else:
                        legs_quotes = []

                    sq = next((q for q in legs_quotes if q['symbol'] == short_leg), None)
                    lq = next((q for q in legs_quotes if q['symbol'] == long_leg), None)

                    if sq and lq:
                        curr_debit = float(sq.get('ask', 0)) - float(lq.get('bid', 0))
                        if curr_debit >= entry_credit * 2.5:
                            should_stop = True
                            self._log(f"🛑 Stop loss: debit {curr_debit:.2f} >= {entry_credit * 2.5:.2f} (2.5× credit)")
                except Exception as e:
                    self._log(f"Error checking debit for {short_leg}: {e}")

            # --- Close if ITM or stop loss triggered ---
            if is_itm or should_stop:
                reason_parts = []
                if is_itm:
                    reason_parts.append(f"ITM ({current_price} vs strike {strike})")
                if should_stop:
                    reason_parts.append(f"2.5× stop (debit {curr_debit:.2f})")
                self._log(f"🚨 Closing {symbol} — DTE {dte} < 3 + {' + '.join(reason_parts)}")
                limit_price = round(curr_debit * 1.05, 2) if curr_debit > 0 else None
                self._execute_close(trade, limit_price=limit_price, simulation_mode=simulation_mode)
            else:
                self._log(f"✅ {symbol} ({short_leg}) DTE {dte} < 3, OTM, debit OK — monitoring.")

        return self.execution_logs if simulation_mode else None


            
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
                natural_debit = float(short_ask) - float(long_bid)
                if natural_debit < 0: natural_debit = 0.0
                
                limit_price = float(math.floor((natural_debit * 1.05) * 100) / 100.0)
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
                tag="CREDITSPREADS"
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

    def _find_expiry(self, symbol, target_dte=11, min_dte=7, max_dte=15, exclude_dates=None):
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
                try:
                    exp_dates.append(datetime.strptime(e, "%Y-%m-%d").date())
                except:
                    continue
        else:
             # handle date objects if already parsed
            exp_dates = []
            for e in expirations:
                 if hasattr(e, 'strftime'):
                     d_str = e.strftime("%Y-%m-%d")
                     if exclude_dates and d_str in exclude_dates: continue
                     if hasattr(e, 'date'):
                         exp_dates.append(e.date())
                     else:
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

    def _count_total_spreads(self, symbol):
        """
        Count TOTAL existing spreads (Positions + Pending Orders) for this symbol.
        Aggregates Puts + Calls across ALL expiries.
        """
        count = 0
        try:
            positions = self.tradier.get_positions() or []
            orders = self.tradier.get_orders() or []
        except:
            return 0
            
        # 1. Positions
        for p in positions:
            if not self._is_short_option(p): continue
            if self._get_underlying_from_pos(p) != symbol: continue
            count += abs(p.get('quantity', 1))
            
        # 2. Orders
        pending_statuses = ['open', 'partially_filled', 'pending']
        for o in orders:
            if o.get('status') not in pending_statuses: continue
            if self._get_underlying_from_pos(o) != symbol: continue
            
            # Check if it's a spread order (sell_to_open)
            legs = o.get('leg') or o.get('legs', [])
            if isinstance(legs, dict): legs = [legs]
            
            is_opening_spread = False
            
            if o.get('class') == 'multileg':
                 for leg in legs:
                     if leg.get('side') == 'sell_to_open':
                         is_opening_spread = True
                         break
            elif o.get('class') == 'option' and o.get('side') == 'sell_to_open':
                 is_opening_spread = True
                 
            if is_opening_spread:
                if o.get('status') == 'partially_filled':
                    count += abs(o.get('remaining_quantity', 0))
                else:
                    count += abs(o.get('quantity', 0))
                    
        return count


    def _get_spread_width(self, current_price):
        """
        Price-tiered spread width:
          < $50   → $0.50 wide
          $50–$150 → $1.00 wide
          > $150   → $2.50 wide (caller falls back to $5.00 if long leg not in chain)
        """
        if current_price < 50:
            return 0.50
        elif current_price <= 150:
            return 1.00
        else:
            return 2.50

    def _place_credit_put_spread(self, symbol, current_price, analysis, min_credit=None, max_lots=5, config=None):
        """
        Sell Put at Support, Buy Put lower (defined risk).
        """
        # 1. Early Constraint Check
        exclusions = self._check_expiry_constraints(symbol, is_put=True, max_lots=max_lots)
        expiry = self._find_expiry(symbol, target_dte=11, min_dte=7, max_dte=15, exclude_dates=exclusions)
        if not expiry: 
             self._log(f"🔸 No expiry found for {symbol}")
             return

        # Get Support Levels
        # AnalysisService returns flattened keys now
        entry_points = analysis.get('put_entry_points', [])
        
        # First support level below price with POP > 75%
        valid_points = [
            ep for ep in entry_points
            if ep['price'] < current_price and ep.get('pop', 0) > 75
        ]

        if not valid_points:
            # Fallback to Delta 0.30-0.37
            self._log(f"🔹 No valid support levels found for {symbol}. Checking Delta 0.30-0.37...")
            
            # Check Constraints (Is Put = True)
            exclusions = self._check_expiry_constraints(symbol, is_put=True, max_lots=max_lots)
            expiry = self._find_expiry(symbol, target_dte=11, min_dte=7, max_dte=15, exclude_dates=exclusions)
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
             # Use first qualifying level (lowest support below price, highest POP safety margin)
             target_strike = valid_points[0]['price']
             pop = valid_points[0].get('pop', 'N/A')
             
             # Expiry already found above

        # Common Logic starts here
        if not 'expiry' in locals() or not expiry: # expiry might be set in if/else
             self._log(f"🔸 No expiry found for {symbol}")
             return

        # Price-tiered spread width
        width = self._get_spread_width(current_price)
        short_put_strike = target_strike
        long_put_strike = short_put_strike - width

        self._log(f"✅ Placing Bull Put Spread on {symbol}")
        self._log(f"   • Exp: {expiry} | Short: {short_put_strike} | Long: {long_put_strike} | POP: {pop}%")
        
        # Get Chain to find Option Symbols
        chain = self.tradier.get_option_chains(symbol, expiry)
        if not chain: return
        
        short_leg = next((o for o in chain if o['strike'] == short_put_strike and o['option_type'] == 'put'), None)
        long_leg = next((o for o in chain if o['strike'] == long_put_strike and o['option_type'] == 'put'), None)

        # For >$150 stocks: fall back from $2.50 to $5.00 wide if long leg not in chain
        if short_leg and not long_leg and current_price > 150 and width == 2.50:
            long_put_strike = short_put_strike - 5.00
            long_leg = next((o for o in chain if o['strike'] == long_put_strike and o['option_type'] == 'put'), None)
            if long_leg:
                width = 5.00
                self._log(f"📌 {symbol}: $2.50 long leg unavailable, using $5.00 wide spread.")

        if not short_leg or not long_leg:
            self._log("Could not find option legs.")
            return False

        # Calculate Price (Credit)
        # Sell Short, Buy Long. Credit = Short Bid - Long Ask (conservative) or Mid - Mid.
        # Let's try Mid point.
        short_price = (short_leg['bid'] + short_leg['ask']) / 2
        long_price = (long_leg['bid'] + long_leg['ask']) / 2
        net_credit = round(short_price - long_price, 2)
        
        # Credit Threshold Check
        # Default to 20% of spread width if min_credit not specified
        # e.g. Width 1.0 -> 0.20, Width 5.0 -> 1.00
        threshold = min_credit if min_credit else (width * 0.20)
        
        if net_credit < threshold:
            if min_credit:
                self._log(f"⚠️ Market Credit ({net_credit}) < Target ({min_credit}). Placing Limit Order at Target.")
                net_credit = min_credit
            else:
                self._log(f"Credit too low ({net_credit}) for risk (Min {threshold:.2f}).")
                return False

        # BP Check & Lot Sizing
        requirement_per_lot = abs(short_put_strike - long_put_strike) * 100
        
        # DYNAMIC BP-BASED SCALING
        available_bp = self._get_available_bp(config)
        
        # Calculate how many lots we can afford with remaining BP
        dynamic_lots = int(available_bp // requirement_per_lot)
        if dynamic_lots < 1:
            self._log(f"Spread requirement (${requirement_per_lot:,.2f}) exceeds available BP (${available_bp:,.2f}). Skipping.")
            return False
            
        # Hard cap the dynamic lots if user manually set max_credit_spreads_per_symbol
        dynamic_lots = min(dynamic_lots, max_lots)

        # Place Order
        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': dynamic_lots},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': dynamic_lots}
        ]
        
        # Final BP sanity check for the Total Requirement before sending order
        total_requirement = requirement_per_lot * dynamic_lots
        if not self._is_bp_sufficient(total_requirement, config):
            return False
        
        if self.dry_run:
            self._log(f"[DRY RUN] Simulating Bull Put Spread Order for {symbol} ({dynamic_lots} lots) @ {net_credit}")
            response = {'id': 'mock_order_id', 'status': 'ok', 'partner_id': 'mock'}
        else:
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='sell', # Not used for multileg but required arg
                quantity=1, # Multileg uses leg-level quantity
                order_type='credit',
                duration='day',
                price=net_credit,
                order_class='multileg',
                legs=legs,
                tag="CREDITSPREADS"
            )
        
        # Order Placed
            if 'error' in response:
                self._log(f"Order failed: {response['error']}")
                return False
            else:
                self._log(f"Order placed: {response}")
                legs_info = {
                     'short_leg': next((l for l in legs if l['side'] == 'sell_to_open'), {}).get('option_symbol'),
                     'long_leg': next((l for l in legs if l['side'] == 'buy_to_open'), {}).get('option_symbol')
                }
                self._record_trade(symbol, "Bull Put Spread", net_credit, response, legs_info)
                return True

    def _place_credit_call_spread(self, symbol, current_price, analysis, min_credit=None, max_lots=5, config=None):
        # Similar logic for Bear Call Spread
        # 1. Early Constraint Check
        exclusions = self._check_expiry_constraints(symbol, is_put=False, max_lots=max_lots)
        expiry = self._find_expiry(symbol, target_dte=11, min_dte=7, max_dte=15, exclude_dates=exclusions)
        if not expiry:
             self._log(f"🔸 No expiry found for {symbol}")
             return

        # Get Resistance Levels
        entry_points = analysis.get('call_entry_points', [])
        if not entry_points: return False

        self._log(f"DEBUG: {symbol} Call Entry Points: {entry_points} | Current Price: {current_price}")

        # First resistance level above price with POP > 75%
        valid_points = [
            ep for ep in entry_points
            if ep['price'] > current_price and ep.get('pop', 0) > 75
        ]

        if not valid_points:
             # Fallback to Delta 0.30-0.37
            self._log(f"🔹 No valid resistance levels found for {symbol}. Checking Delta 0.30-0.37...")
            
            # Check Constraints (Is Put = False)
            exclusions = self._check_expiry_constraints(symbol, is_put=False, max_lots=max_lots)
            expiry = self._find_expiry(symbol, target_dte=11, min_dte=7, max_dte=15, exclude_dates=exclusions)
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

        # Price-tiered spread width
        width = self._get_spread_width(current_price)
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

        # For >$150 stocks: fall back from $2.50 to $5.00 wide if long leg not in chain
        if short_leg and not long_leg and current_price > 150 and width == 2.50:
            long_call_strike = short_call_strike + 5.00
            long_leg = next((o for o in chain if o['strike'] == long_call_strike and o['option_type'] == 'call'), None)
            if long_leg:
                width = 5.00
                self._log(f"📌 {symbol}: $2.50 long leg unavailable, using $5.00 wide spread.")

        if not short_leg or not long_leg:
            self._log(f"Could not find option legs for Call Spread (Short: {short_call_strike}, Long: {long_call_strike})")
            return False
        
        short_price = (short_leg['bid'] + short_leg['ask']) / 2
        long_price = (long_leg['bid'] + long_leg['ask']) / 2
        net_credit = round(short_price - long_price, 2)
        
        # Credit Threshold Check
        # Default to 20% of spread width if min_credit not specified
        threshold = min_credit if min_credit else (width * 0.20)
        
        if net_credit < threshold:
            if min_credit:
                self._log(f"⚠️ Market Credit ({net_credit}) < Target ({min_credit}). Placing Limit Order at Target.")
                net_credit = min_credit
            else:
                self._log(f"Credit too low ({net_credit}) for risk (Min {threshold:.2f}).")
                return False

        # BP Check & Lot Sizing
        requirement_per_lot = abs(short_call_strike - long_call_strike) * 100
        
        # DYNAMIC BP-BASED SCALING
        available_bp = self._get_available_bp(config)
        
        dynamic_lots = int(available_bp // requirement_per_lot)
        if dynamic_lots < 1:
            self._log(f"Spread requirement (${requirement_per_lot:,.2f}) exceeds available BP (${available_bp:,.2f}). Skipping.")
            return False
            
        dynamic_lots = min(dynamic_lots, max_lots)

        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': dynamic_lots},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': dynamic_lots}
        ]

        # Final BP sanction
        total_requirement = requirement_per_lot * dynamic_lots
        if not self._is_bp_sufficient(total_requirement, config):
            return False

        self._log(f"Placing Bear Call Spread on {symbol} Exp: {expiry} Short: {short_call_strike} Long: {long_call_strike} (Lots: {dynamic_lots})")
        
        if self.dry_run:
            self._log(f"[DRY RUN] Simulating Bear Call Spread Order for {symbol} ({dynamic_lots} lots) @ {net_credit}")
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
                tag="CREDITSPREADS"
            )
        
        if 'error' in response:
            self._log(f"Order failed: {response['error']}")
            return False
        else:
             self._log(f"Order placed: {response}")
             legs_info = {
                 'short_leg': next((l for l in legs if l['side'] == 'sell_to_open'), {}).get('option_symbol'),
                 'long_leg': next((l for l in legs if l['side'] == 'buy_to_open'), {}).get('option_symbol')
             }
             self._record_trade(symbol, "Bear Call Spread", net_credit, response, legs_info)
             return True
