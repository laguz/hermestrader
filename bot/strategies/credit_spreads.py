import logging
import traceback
from datetime import datetime


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class CreditSpreadStrategy:
    def __init__(self, tradier_service, db, dry_run=False):
        self.tradier = tradier_service
        self.db = db
        self.dry_run = dry_run
        self.min_confidence_score = 7  # Out of 10
        self.execution_logs = []
        
    def _log(self, message):
        """Log message to DB via BotService mechanism (manually for now)."""
        self.execution_logs.append(f"{datetime.now().strftime('%H:%M:%S')} - {message}")
        
        # Cleaner stdout for dry run
        if self.dry_run:
            if "Analyzing" in message:
                print(f"{Colors.HEADER}   {message}{Colors.ENDC}")
            elif "Placing" in message and "✅" in message:
                print(f"{Colors.OKGREEN}   {message}{Colors.ENDC}")
            elif "Skipping" in message:
                print(f"{Colors.OKCYAN}   {message}{Colors.ENDC}")
            elif "Error" in message or "failed" in message or "❌" in message:
                print(f"{Colors.FAIL}   {message}{Colors.ENDC}")
            elif "•" in message:
                print(f"{Colors.OKGREEN}   {message}{Colors.ENDC}")
            else:
                print(f"   {message}")
        else:
            print(f"[CREDIT_SPREADS] {message}")
            
        try:
            if self.db is not None:
                self.db['bot_config'].update_one(
                    {"_id": "main_bot"},
                    {"$push": {"logs": {"$each": [{
                        "timestamp": datetime.now(),
                        "message": f"[CREDIT_SPREADS] {message}"
                    }], "$slice": -100}}}
                )
        except Exception as e:
            print(f"Log Error: {e}")

    def execute(self, watchlist):
        """
        Execute the Credit Spread strategy on the watchlist.
        1. Analyze symbol.
        2. Check for entry signals.
        3. Place order if high confidence.
        """
        from services.analysis_service import AnalysisService
        from services.container import Container

        analysis_service = Container.get_analysis_service() # We need to get it here to avoid circular dependencies
        current_hour = datetime.now().hour

        # Only trade during market hours (roughly)
        # Assuming UTC-5 (EST)
        # Simple check: pass for now, bot loop controls timing.

        for symbol in watchlist:
            try:
                # 1. Safety Check: Do we already have a position?
                positions = self.tradier.get_positions()
                has_position = any(p.get('symbol') == symbol or p.get('underlying') == symbol for p in positions)
                
                if has_position:
                    self._log(f"⏭️  Skipping {symbol}: Existing position found.")
                    continue

                # 2. Analyze
                if self.dry_run:
                    print(f"\n{Colors.HEADER}📦 Analyzing {symbol}...{Colors.ENDC}")
                else:
                    self._log(f"Analyzing {symbol}...")
                analysis = analysis_service.analyze_symbol(symbol)
                
                if not analysis or 'error' in analysis:
                    self._log(f"⚠️  Analysis failed for {symbol}: {analysis.get('error')}")
                    continue
                    
                # 3. Execution Logic
                current_price = analysis.get('current_price')
                
                # Attempt Bull Put Spread (if support exists below price)
                self._place_credit_put_spread(symbol, current_price, analysis)
                
                # Attempt Bear Call Spread (if resistance exists above price)
                self._place_credit_call_spread(symbol, current_price, analysis)
                    
            except Exception as e:
                self._log(f"❌ Error processing {symbol}: {e}")
                traceback.print_exc()
        
        return self.execution_logs

    def manage_positions(self):
        """
        Check open positions for exit conditions.
        Condition: If ITM for 2 days straight, close on next day at 3:00 PM EST.
        """
        # 1. Check Time (Only run after 3:00 PM EST)
        # Assuming server time is roughly aligned or we check UTC. 
        # EST is UTC-5. 3 PM EST is 20:00 UTC.
        # Let's assume system local time is used for simplicity as per existing logic.
        now = datetime.now()
        if now.hour < 15: 
            return # Too early

        # 2. Get Open Trades from DB
        open_trades = list(self.db['auto_trades'].find({"status": "OPEN"}))
        if not open_trades: return

        # 3. Verify with Tradier (Source of Truth)
        try:
            positions = self.tradier.get_positions()
        except Exception as e:
            self._log(f"Error fetching positions for management: {e}")
            return
            
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
            if last_check == today_str:
                if trade.get('close_on_next_day', False):
                    # It's D-Day (Day 3 or later) and we are past 3 PM.
                    self._execute_close(trade)
                continue
                
            # Start of New Daily Check
            
            # 0. Check for Pending Close from Previous Day
            if trade.get('close_on_next_day', False):
                 self._log(f"🚨 Executing scheduled close for {symbol} (ITM > 2 days).")
                 self._execute_close(trade)
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
            self.db['auto_trades'].update_one(
                {"_id": trade['_id']},
                {"$set": updates}
            )
            
            # If we just flagged it, we DO NOT close today. "Close on the NEXT trading day".
            # So we wait.
            
    def _execute_close(self, trade):
        """Close the spread position."""
        self._log(f"Refuting Close Logic for {trade['symbol']} (ITM Limit Reached)...")
        
        # Build closing order (Buy to Close Short, Sell to Close Long)
        short_leg = trade['short_leg']
        long_leg = trade['long_leg']
        
        legs = [
            {'option_symbol': short_leg, 'side': 'buy_to_close', 'quantity': 1},
            {'option_symbol': long_leg, 'side': 'sell_to_close', 'quantity': 1}
        ]
        
        # We need to pay debit. Get market price?
        # For automation, we might use 'market' order or limit at mid?
        # Tradier 'market' for multileg might be risky or blocked.
        # Let's try to get quotes.
        try:
            quotes = self.tradier.get_quotes([short_leg, long_leg])
            # Calculate Debit: (Short Ask - Long Bid) ? To buy back short and sell long.
            # Short (Buy) -> Ask. Long (Sell) -> Bid.
            # safe assumption?
            short_q = next((q for q in quotes if q['symbol'] == short_leg), {})
            long_q = next((q for q in quotes if q['symbol'] == long_leg), {})
            
            debit = (short_q.get('ask', 0) - long_q.get('bid', 0))
            # Pad it slightly to ensure fill?
            limit_price = round(debit * 1.05, 2) # paying 5% more?
            # Or just use Mid?
            # Let's just log and skip actual execution if risk is high, or use 'market' if permitted.
            # User didn't specify order type. Let's assume Market for "Close immediately at 3 PM".
        except:
             limit_price = 0 # trigger manual review or fail
        
        if self.dry_run:
            self._log(f"[DRY RUN] Closing {trade['symbol']} spread. Debit: ~{limit_price}")
            # Mark Closed
            self.db['auto_trades'].update_one(
                {"_id": trade['_id']},
                {"$set": {"status": "CLOSED_STOP_LOSS", "close_date": datetime.now()}}
            )
        else:
            # Real execution
            # self.tradier.place_order(...) 
            # Placeholder for safety until tested
            self._log(f"Would close {trade['symbol']} now. Implementation pending safe limit logic.")
            pass

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

    def _find_expiry(self, symbol, target_dte=30):
        """Find expiry date closest to target DTE."""
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations: return None
        
        from datetime import date, timedelta
        if isinstance(expirations[0], str):
            # Convert strings to dates
            exp_dates = [datetime.strptime(e, "%Y-%m-%d").date() for e in expirations]
        else:
            exp_dates = list(expirations)
            
        target_date = date.today() + timedelta(days=target_dte)
        
        # Find closest
        closest_date = min(exp_dates, key=lambda d: abs(d - target_date))
        return closest_date.strftime("%Y-%m-%d")

    def _place_credit_put_spread(self, symbol, current_price, analysis):
        """
        Sell Put at Support, Buy Put lower (defined risk).
        """
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
            
            expiry = self._find_expiry(symbol, target_dte=30) # Use 30 DTE for delta usage
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
             expiry = self._find_expiry(symbol, target_dte=21)

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
        
        if net_credit < 0.80:
            self._log(f"Credit too low ({net_credit}) for risk.")
            return

        # Place Order
        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': 1},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': 1}
        ]
        
        # Note: 'price' for credit spread is a Credit. Tradier handles this as positive price for 'credit' strategies usually? 
        # Actually for multileg/combo, user specifies net price.
        # Limit price is required.
        
        if self.dry_run:
            self._log(f"[DRY RUN] Simulating Bull Put Spread Order for {symbol}")
            response = {'id': 'mock_order_id', 'status': 'ok', 'partner_id': 'mock'}
        else:
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='sell', # Not used for multileg but required arg
                quantity=1,
                order_type='limit',
                duration='day',
                price=net_credit,
                order_class='multileg',
                legs=legs
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

    def _place_credit_call_spread(self, symbol, current_price, analysis):
        # Similar logic for Bear Call Spread
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
            
            expiry = self._find_expiry(symbol, target_dte=30)
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
            expiry = self._find_expiry(symbol, target_dte=21)
        
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
        
        if net_credit < 0.80:
             self._log(f"Credit too low ({net_credit}).")
             return

        self._log(f"Placing Bear Call Spread on {symbol} Exp: {expiry} Short: {short_call_strike} Long: {long_call_strike}")

        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': 1},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': 1}
        ]
        
        if self.dry_run:
            self._log(f"[DRY RUN] Simulating Bear Call Spread Order for {symbol}")
            response = {'id': 'mock_order_id', 'status': 'ok', 'partner_id': 'mock'}
        else:
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='sell',
                quantity=1,
                order_type='limit',
                duration='day',
                price=net_credit,
                order_class='multileg',
                legs=legs
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

    def _record_trade(self, symbol, strategy, price, response, legs_info=None):
        if self.db is not None:
            doc = {
                "symbol": symbol,
                "strategy": strategy,
                "price": price,
                "entry_date": datetime.now(),
                "order_details": response,
                "status": "DRY_RUN" if self.dry_run else "OPEN",
                "pnl": 0.0,
                "is_dry_run": self.dry_run,
                "days_itm": 0,
                "close_on_next_day": False,
                "last_check_date": None
            }
            if legs_info:
                doc.update(legs_info)
                
            self.db['auto_trades'].insert_one(doc)
