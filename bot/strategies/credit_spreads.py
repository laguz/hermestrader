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
        
        if net_credit < 0.20:
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
            self._record_trade(symbol, "Bull Put Spread", net_credit, response)

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
        
        if net_credit < 0.20:
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
             self._record_trade(symbol, "Bear Call Spread", net_credit, response)

    def _record_trade(self, symbol, strategy, price, response):
        if self.db is not None:
            self.db['auto_trades'].insert_one({
                "symbol": symbol,
                "strategy": strategy,
                "price": price,
                "entry_date": datetime.now(),
                "order_details": response,
                "order_details": response,
                "status": "DRY_RUN" if self.dry_run else "OPEN",
                "pnl": 0.0,
                "is_dry_run": self.dry_run
            })
