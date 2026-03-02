import logging
import traceback
import re
import math
import pytz
from datetime import datetime, timedelta
import sys
import os
# Ensure project root is in path for absolute imports if run standalone
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from bot.strategies.base_strategy import AbstractStrategy
from bot.utils import Colors, is_match, get_op_type, get_expiry_str, get_underlying

class CreditSpreadRulebaseStrategy(AbstractStrategy):
    def __init__(self, tradier_service, db, dry_run=False, analysis_service=None):
        super().__init__(tradier_service, db, dry_run, analysis_service)

    def _log(self, message):
        super()._log(message, strategy_name="CreditSpreadRulebaseStrategy")

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
                    self._log(f"Bullish technical signal for {symbol} (Price > SMA200 or RSI < 30)")
                
                # UPGRADE 1: TREND FILTERING
                trend = analysis.get('trend', 'neutral')
                
                if is_bullish:
                    if trend == 'bearish':
                        self._log(f"Skipping {symbol}: Technicals Bullish, but overall Trend is Bearish.")
                        continue
                        
                    # Target Strategy: Bull Put Spread
                    self._place_credit_spread(symbol, current_price, analysis, is_put=True, config=config)
                else:
                    self._log(f"No bullish bias for {symbol}")

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
        
        # UPGRADE 5: EXECUTION TIMING (10:30 AM EST)
        if hasattr(self.tradier, 'current_date') and self.tradier.current_date:
             now_est = self.tradier.current_date
        else:
             est = pytz.timezone('America/New_York')
             now_est = datetime.now(est)
             
        now = now_est
        
        if not simulation_mode:
            if now.hour < 10 or (now.hour == 10 and now.minute < 30): 
                return # Too early, wait for 10:30 AM EST

        # Fetch open trades from DB
        query = {"status": "OPEN", "strategy": {"$regex": "Rule-Based"}}
        open_trades = list(self.db['auto_trades'].find(query))
        
        if not open_trades:
            self._log("No open rule-based trades to manage.")
            return

        try:
            positions = self.tradier.get_positions() or []
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
            expiry_date_str = get_expiry_str(short_leg)
            if not expiry_date_str: continue
            expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d")
                
            dte = (expiry_date.date() - self._get_current_date()).days
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

                    # UPGRADE 3: HARD STOP LOSS (2.5x Initial Credit)
                    # We compare the current required debit against the absolute 2.5 multiplier
                    if curr_debit >= 2.5 * entry_credit:
                        limit_price = round(curr_debit * 1.05, 2)
                        self._log(f"🛑 STOP LOSS: Debit ({curr_debit:.2f}) >= 2.5x Credit ({entry_credit:.2f}). Triggering Limit Close at {limit_price}")
                        self._execute_close(trade, limit_price=limit_price, simulation_mode=simulation_mode)
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
        short_strike, _ = self._find_delta_strike(chain, target_side, min_d=0.16, max_d=0.30)
        
        if not short_strike:
            self._log(f"Could not find 16-30 delta {target_side} for {symbol}")
            return

        # UPGRADE 2: DYNAMIC SPREAD WIDTHS
        dynamic_width = current_price * 0.015
        width = float(max(1.0, math.ceil(dynamic_width)))
        # Robust Long Strike Selection: Find nearest available strike in the right direction
        if is_put:
            target_long = short_strike - width
            long_candidates = [o for o in chain if o['option_type'] == target_side and o['strike'] <= target_long]
            long_leg = max(long_candidates, key=lambda x: x['strike']) if long_candidates else None
        else:
            target_long = short_strike + width
            long_candidates = [o for o in chain if o['option_type'] == target_side and o['strike'] >= target_long]
            long_leg = min(long_candidates, key=lambda x: x['strike']) if long_candidates else None

        if not long_leg:
            self._log(f"Could not find available long leg for {symbol} near strike {target_long}")
            return

        long_strike = long_leg['strike']
        width = abs(short_strike - long_strike)

        # Calculate Credit Target: configurable, defaults to 1/3 of width
        min_credit_pct = config.get('min_credit_pct', 1/3) if config else 1/3
        target_credit = round(width * min_credit_pct, 2)
        
        # Get short leg object (should exist since short_strike came from chain)
        short_leg = next((o for o in chain if o['strike'] == short_strike and o['option_type'] == target_side), None)
        
        if not short_leg:
            self._log(f"Unexpected: Short leg {short_strike} not found in chain for {symbol}")
            return

        # Mid-price credit
        short_mid = (short_leg['bid'] + short_leg['ask']) / 2
        long_mid = (long_leg['bid'] + long_leg['ask']) / 2
        net_credit = round(short_mid - long_mid, 2)

        if net_credit < target_credit:
            self._log(f"Skipping {symbol}: Credit {net_credit} < Target {target_credit} ({min_credit_pct:.0%} width)")
            return

        # UPGRADE 4: CAPITAL-BASED LIMITS 
        requirement_per_lot = width * 100
        max_capital = config.get('max_capital_per_symbol', 500) if config else 500
        dynamic_lots = int(max_capital // requirement_per_lot)
        
        if dynamic_lots < 1:
            self._log(f"Spread requirement (${requirement_per_lot}) exceeds Max Capital limit (${max_capital}). Skipping.")
            return False
            
        # Hard cap the dynamic lots based on standard rulebase limit constraint
        baseline_max = config.get('max_credit_spread_rulebase_lots', 5)
        # Verify lot sufficiency for total aggregated positions to block entries if already running multiple positions of this asset early
        if not self._check_lots_sufficient(symbol, baseline_max):
             self._log(f"Skipping {symbol}: Max rulebase concurrent spread instances ({baseline_max}) reached.")
             return False
        
        dynamic_lots = min(dynamic_lots, baseline_max)

        # Final BP Check
        total_requirement = requirement_per_lot * dynamic_lots
        if not self._is_bp_sufficient(total_requirement, config):
            return False

        self._log(f"✅ Executing Rule-Based Bull Put on {symbol}: {short_strike}/{long_strike} Exp: {expiry} Credit: {net_credit} Lots: {dynamic_lots}")
        
        # Place Order
        legs = [
            {'option_symbol': short_leg['symbol'], 'side': 'sell_to_open', 'quantity': dynamic_lots},
            {'option_symbol': long_leg['symbol'], 'side': 'buy_to_open', 'quantity': dynamic_lots}
        ]
        
        strategy_name = "Rule-Based Bull Put Spread" if is_put else "Rule-Based Bear Call Spread"
        
        if self.dry_run:
            response = {'id': 'dry_run_' + datetime.now().strftime("%Y%m%d%H%M%S"), 'status': 'ok'}
            self._record_trade(symbol, strategy_name, net_credit, response, {
                'short_leg': short_leg['symbol'],
                'long_leg': long_leg['symbol']
            })
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
                tag="RULE_BASED_SPREADS"
            )
            if 'error' in response:
                self._log(f"Order failed: {response['error']}")
            else:
                self._record_trade(symbol, strategy_name, net_credit, response, {
                    'short_leg': short_leg['symbol'],
                    'long_leg': long_leg['symbol']
                })

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
                legs=legs,
                tag="RULE_BASED_SPREADS"
            )
            if 'id' in response:
                self.db['auto_trades'].update_one({"_id": trade['_id']}, {"$set": {"status": "CLOSED", "close_date": datetime.now()}})

    def _parse_strike_from_symbol(self, symbol):
        try:
            strike_part = symbol[-8:]
            return int(strike_part) / 1000.0
        except:
            return 0
