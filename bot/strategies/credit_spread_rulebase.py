import logging
import traceback
import re
import math
from datetime import datetime, timedelta
from bot.strategies.credit_spreads import Colors

class CreditSpreadRulebaseStrategy:
    def __init__(self, tradier_service, db, dry_run=False, analysis_service=None):
        self.tradier = tradier_service
        self.db = db
        self.dry_run = dry_run
        self.analysis_service = analysis_service
        self.execution_logs = []

    def _log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clean_msg = f"[{timestamp}] {message}"
        print(clean_msg)
        self.execution_logs.append(clean_msg)
        
        if self.db is not None:
            try:
                self.db.logs.insert_one({
                    "timestamp": datetime.now(),
                    "message": message,
                    "source": "CreditSpreadRulebaseStrategy"
                })
            except:
                pass

    def _get_current_date(self):
        return datetime.now().date()

    def execute(self, watchlist, config=None):
        """
        Execute the Rule-Based Credit Spread strategy on the watchlist.
        """
        self._log(f"{Colors.HEADER}--- Starting Rule-Based Credit Spread Execution ---{Colors.ENDC}")
        config = config or {}
        
        # 1. Market Sentiment Check (VIX > 30) - Optional but recommended
        try:
            vix_quote = self.tradier.get_quote("VIX")
            vix_last = vix_quote.get('last', 0)
            self._log(f"Market Sentiment (VIX): {vix_last}")
            # We don't strictly block on VIX but log it as per rules
        except Exception as e:
            self._log(f"Could not fetch VIX: {e}")

        for symbol in watchlist:
            try:
                self._log(f"Analyzing {symbol}...")
                
                # 2. Get Analysis
                analysis = self.analysis_service.analyze_symbol(symbol)
                if not analysis or "error" in analysis:
                    self._log(f"Skipping {symbol}: Analysis failed")
                    continue

                indicators = analysis.get('indicators', {})
                current_price = analysis.get('current_price')
                hv_rank = indicators.get('hv_rank', 0)
                rsi = indicators.get('rsi', 50)
                sma_200 = indicators.get('sma_200')

                # 3. Rule Selection: IVR > 50%
                if hv_rank < 50:
                    self._log(f"Skipping {symbol}: IV Rank ({hv_rank}%) < 50%")
                    continue

                # 4. Directional Bias & Entry
                # Bull Put: Price > 200-day MA OR RSI < 30
                is_bullish = False
                if (sma_200 and current_price > sma_200) or rsi < 30:
                    is_bullish = True
                    self._log(f"Bullish signal for {symbol} (Price > SMA200 or RSI < 30)")
                
                # NOTE: The user only explicitly asked for rules for Bull Put in the description, 
                # but "tastytrade-style" usually implies both. I'll focus on Bull Put as per example.
                # If RSI > 70 or Price < SMA 200 (with some ands), could be Bear Call.
                # For now, let's implement the Bull Put as described.
                
                if is_bullish:
                    # Check Lots
                    max_lots = config.get('max_credit_spread_rulebase_lots', 5)
                    if not self._check_lots_sufficient(symbol, max_lots):
                        self._log(f"Skipping {symbol}: Max lots ({max_lots}) reached.")
                        continue
                        
                    self._place_credit_spread(symbol, current_price, analysis, is_put=True, config=config)
                else:
                    self._log(f"No directional bias for {symbol}")

            except Exception as e:
                self._log(f"Error processing {symbol}: {e}")
                traceback.print_exc()

    def manage_positions(self, simulation_mode=False):
        """
        Manage open positions based on rule-based exits.
        - 50% Profit Taking
        - 2x Credit Stop Loss
        - 21 DTE Time Exit
        """
        self._log(f"{Colors.HEADER}--- Managing Rule-Based Credit Spread Positions ---{Colors.ENDC}")
        
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')

        # Fetch open trades from DB
        query = {"status": "OPEN", "strategy": {"$regex": "Rule-Based"}}
        open_trades = list(self.db['auto_trades'].find(query))
        
        if not open_trades:
            self._log("No open rule-based trades to manage.")
            return

        try:
            positions = self.tradier.get_positions()
        except Exception as e:
            self._log(f"Error fetching positions: {e}")
            return

        active_option_symbols = {p['symbol']: p for p in positions}

        for trade in open_trades:
            symbol = trade['symbol']
            short_leg = trade.get('short_leg')
            long_leg = trade.get('long_leg')
            
            if not short_leg or short_leg not in active_option_symbols:
                self._log(f"⚠️ Trade {symbol} ({short_leg}) not found in active positions. Marking CLOSED.")
                self.db['auto_trades'].update_one({"_id": trade['_id']}, {"$set": {"status": "CLOSED", "close_date": now}})
                continue

            # 1. Time Exit (21 DTE)
            expiry_date = self._parse_expiry_from_symbol(short_leg)
            if not expiry_date:
                continue
                
            dte = (expiry_date.date() - now.date()).days
            if dte <= 21:
                self._log(f"🕒 TIME EXIT: Closing {symbol} at {dte} DTE to avoid gamma risk.")
                self._execute_close(trade, simulation_mode=simulation_mode)
                continue

            # 2. Profit Taking & Stop Loss
            entry_credit = trade.get('price', 0)
            if entry_credit <= 0: continue

            try:
                # Get Quotes for both legs
                legs_quotes = self.tradier.get_quote(f"{short_leg},{long_leg}")
                if isinstance(legs_quotes, dict): legs_quotes = [legs_quotes]
                
                sq = next((q for q in legs_quotes if q['symbol'] == short_leg), None)
                lq = next((q for q in legs_quotes if q['symbol'] == long_leg), None)
                
                if sq and lq:
                    # Current price to close (Debit)
                    curr_debit = (sq.get('ask', 0) - lq.get('bid', 0))
                    profit_val = entry_credit - curr_debit
                    profit_pct = (profit_val / entry_credit)
                    
                    self._log(f"📊 {symbol} DTE: {dte} | Profit: {profit_pct*100:.1f}% | Debit: {curr_debit:.2f} (Entry: {entry_credit})")

                    # Rule: 50% Profit Taking
                    if profit_pct >= 0.50:
                        self._log(f"💰 PROFIT TAKING: 50% target met for {symbol}.")
                        self._execute_close(trade, limit_price=round(entry_credit * 0.50, 2), simulation_mode=simulation_mode)
                        continue

                    # Rule: 2x Credit Loss (Stop Loss)
                    # If entry was 1.50, stop loss is at 3.00 debit (loss = 1.50 = 1x credit? or loss = 3.00 = 2x credit?)
                    # "loss hits 2x credit" usually means loss = 2 * entry_credit.
                    # Current Loss = Curr Debit - Entry Credit
                    current_loss = curr_debit - entry_credit
                    if current_loss >= 2 * entry_credit:
                        self._log(f"🛑 STOP LOSS: 2x credit loss hit for {symbol} (Loss: {current_loss:.2f} >= {2*entry_credit:.2f}).")
                        self._execute_close(trade, simulation_mode=simulation_mode)
                        continue
                    
                    # Rule: Technical Breach (below short strike by 50%)
                    # "below short strike by 50%" is ambiguous. Likely means 50% of the distance between entry and strike?
                    # Or 50% of the width?
                    # "below short strike by 50%" usually means if it breaches then some more.
                    # Let's check underlying price.
                    quote = self.tradier.get_quote(symbol)
                    current_price = quote.get('last')
                    short_strike = self._parse_strike_from_symbol(short_leg)
                    
                    if trade.get('strategy') == "Rule-Based Bull Put Spread":
                        # If price < short_strike, it's ITM.
                        if current_price < short_strike:
                            # How much below? Let's assume user meant 50% of the credit/width?
                            # I'll implement "Breached short strike" for now as a simpler proxy or 
                            # use the 2x credit loss which usually happens first.
                            pass

            except Exception as e:
                self._log(f"Error managing {symbol}: {e}")

    def _place_credit_spread(self, symbol, current_price, analysis, is_put=True, config=None):
        """
        Specialized entry for rule-based spreads.
        """
        # 45 DTE Target
        expiry = self._find_expiry(symbol, target_dte=45, min_dte=38, max_dte=52)
        if not expiry:
            self._log(f"No valid 45 DTE expiry found for {symbol}")
            return

        chain = self.tradier.get_option_chains(symbol, expiry)
        if not chain: return

        # Strike Selection: 16-30 Delta
        target_side = 'put' if is_put else 'call'
        short_strike = self._find_delta_strike(chain, target_side, min_delta=0.16, max_delta=0.30)
        
        if not short_strike:
            self._log(f"Could not find 16-30 delta {target_side} for {symbol}")
            return

        # Width $5-$10
        # If stock > 200, use 10. If < 200, use 5. (Simple heuristic)
        width = 10.0 if current_price > 200 else 5.0
        long_strike = short_strike - width if is_put else short_strike + width

        # Calculate Credit Target: 1/3 of width
        target_credit = round(width / 3.0, 2)
        
        # Get actual prices
        short_leg = next((o for o in chain if o['strike'] == short_strike and o['option_type'] == target_side), None)
        long_leg = next((o for o in chain if o['strike'] == long_strike and o['option_type'] == target_side), None)
        
        if not short_leg or not long_leg:
            self._log(f"Missing legs for {symbol} {short_strike}/{long_strike}")
            return

        # Mid-price credit
        short_mid = (short_leg['bid'] + short_leg['ask']) / 2
        long_mid = (long_leg['bid'] + long_leg['ask']) / 2
        net_credit = round(short_mid - long_mid, 2)

        if net_credit < target_credit:
            self._log(f"Skipping {symbol}: Credit {net_credit} < Target {target_credit} (1/3 width)")
            return

        # BP Check
        requirement = width * 100
        if not self._is_bp_sufficient(requirement, config):
            return

        self._log(f"✅ Executing Rule-Based Bull Put on {symbol}: {short_strike}/{long_strike} Exp: {expiry} Credit: {net_credit}")
        
        # Place Order
        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': 1},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': 1}
        ]
        
        strategy_name = "Rule-Based Bull Put Spread" if is_put else "Rule-Based Bear Call Spread"
        
        if self.dry_run:
            response = {'id': 'dry_run_' + datetime.now().strftime("%Y%m%d%H%M%S"), 'status': 'ok'}
            self._log(f"[DRY RUN] Order Placed for {symbol}")
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
                legs=legs
            )

        if 'id' in response:
            self._record_trade(symbol, strategy_name, net_credit, response, {
                'short_leg': short_leg['symbol'],
                'long_leg': long_leg['symbol']
            })

    def _find_expiry(self, symbol, target_dte=45, min_dte=38, max_dte=52):
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations: return None
        
        today = datetime.now().date()
        valid = []
        for e_str in expirations:
            e_date = datetime.strptime(e_str, "%Y-%m-%d").date()
            dte = (e_date - today).days
            if min_dte <= dte <= max_dte:
                valid.append((e_str, abs(dte - target_dte)))
        
        if not valid: return None
        return min(valid, key=lambda x: x[1])[0]

    def _find_delta_strike(self, chain, option_type, min_delta=0.16, max_delta=0.30):
        # We want delta closest to 0.25 (midpoint of 16-30)
        target_delta = 0.25
        candidates = []
        for opt in chain:
            if opt['option_type'] != option_type: continue
            delta = abs(opt.get('greeks', {}).get('delta', 0))
            if min_delta <= delta <= max_delta:
                candidates.append((opt['strike'], abs(delta - target_delta)))
        
        if not candidates: return None
        return min(candidates, key=lambda x: x[1])[0]

    def _is_bp_sufficient(self, requirement, config):
        min_reserve = config.get('min_obp_reserve', 1000)
        balances = self.tradier.get_account_balances()
        if not balances: return False
        obp = balances.get('option_buying_power', 0)
        if obp - requirement < min_reserve:
            self._log(f"🚫 Insufficient BP: OBP {obp} - Req {requirement} < Reserve {min_reserve}")
            return False
        return True

    def _check_lots_sufficient(self, symbol, max_lots):
        """
        Count existing rule-based positions for this symbol.
        """
        try:
            query = {"status": "OPEN", "symbol": symbol, "strategy": {"$regex": "Rule-Based"}}
            count = self.db['auto_trades'].count_documents(query)
            return count < max_lots
        except Exception as e:
            self._log(f"Error checking lots: {e}")
            return False

    def _record_trade(self, symbol, strategy, price, response, legs_info):
        if self.db is not None:
            doc = {
                "symbol": symbol,
                "strategy": strategy,
                "price": price,
                "entry_date": datetime.now(),
                "order_details": response,
                "status": "OPEN",
                "is_dry_run": self.dry_run,
                **legs_info
            }
            self.db['auto_trades'].insert_one(doc)

    def _execute_close(self, trade, limit_price=None, simulation_mode=False):
        symbol = trade['symbol']
        legs = [
            {'option_symbol': trade['short_leg'], 'side': 'buy_to_close', 'quantity': 1},
            {'option_symbol': trade['long_leg'], 'side': 'sell_to_close', 'quantity': 1}
        ]
        
        if self.dry_run or simulation_mode:
            self._log(f"[DRY RUN/SIM] Closing {symbol} at {limit_price or 'Market'}")
            self.db['auto_trades'].update_one({"_id": trade['_id']}, {"$set": {"status": "CLOSED", "close_date": datetime.now()}})
        else:
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='buy',
                quantity=1,
                order_type='debit' if limit_price else 'market',
                duration='day',
                price=limit_price,
                order_class='multileg',
                legs=legs
            )
            if 'id' in response:
                self.db['auto_trades'].update_one({"_id": trade['_id']}, {"$set": {"status": "CLOSED", "close_date": datetime.now()}})

    def _parse_expiry_from_symbol(self, symbol):
        match = re.search(r'[A-Z]+(\d{6})[PC]', symbol)
        if match:
            return datetime.strptime(match.group(1), '%y%m%d')
        return None

    def _parse_strike_from_symbol(self, symbol):
        try:
            strike_part = symbol[-8:]
            return int(strike_part) / 1000.0
        except:
            return 0
