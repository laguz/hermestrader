import logging
import traceback
from datetime import datetime

class WheelStrategy:
    def __init__(self, tradier_service, db):
        self.tradier = tradier_service
        self.db = db
        self.min_confidence_score = 4 # Wheel is safer, lower threshold ok
        
    def _log(self, message):
        print(f"[WHEEL] {message}")
        try:
            if self.db:
                self.db['bot_config'].update_one(
                    {"_id": "main_bot"},
                    {"$push": {"logs": {"$each": [{
                        "timestamp": datetime.now(),
                        "message": f"[WHEEL] {message}"
                    }], "$slice": -100}}}
                )
        except: pass

    def execute(self, watchlist):
        """
        Execute Wheel Strategy.
        State Machine:
        1. Check Holdings.
        2. Determine State for each symbol:
           - CASH: No position -> Sell Put (Cash Secured)
           - SHORT_PUT: Put sold -> Monitor (Wait for assignment or expiry)
           - STOCK: Assigned (100 shares+) -> Sell Covered Call
           - COVERED_CALL: Call sold -> Monitor
        """
        from services.container import Container
        analysis_service = Container.get_analysis_service()
        
        positions = self.tradier.get_positions()
        
        for symbol in watchlist:
            try:
                state = self._determine_state(symbol, positions)
                self._log(f"{symbol} State: {state}")
                
                if state == "CASH":
                    self._step_sell_put(symbol, analysis_service)
                elif state == "STOCK":
                    self._step_sell_covered_call(symbol, positions, analysis_service)
                elif state == "SHORT_PUT":
                    self._log(f"{symbol}: Monitoring Short Put.")
                elif state == "COVERED_CALL":
                    self._log(f"{symbol}: Monitoring Covered Call.")
                    
            except Exception as e:
                self._log(f"Error processing {symbol}: {e}")
                traceback.print_exc()

    def _determine_state(self, symbol, positions):
        # Filter positions for this symbol
        related = [p for p in positions if p.get('symbol') == symbol or p.get('underlying') == symbol]
        
        if not related:
            return "CASH"
            
        # Check for Stock
        shares = 0
        options = []
        for p in related:
            if p.get('symbol') == symbol: # Equity position
                 # Tradier equity positions structure might vary, strictly 'symbol' usually matches underlying for stock
                 # but for options, symbol is option symbol.
                 # Actually Tradier 'positions' list items: {'symbol': 'AAPL', 'quantity': 100, ...}
                 # {'symbol': 'AAPL23...', 'underlying': 'AAPL', ...}
                 shares += int(p.get('quantity', 0))
            else:
                options.append(p)
                
        has_short_put = any(o['option_type'] == 'put' and o['quantity'] < 0 for o in options)
        has_short_call = any(o['option_type'] == 'call' and o['quantity'] < 0 for o in options)
        
        if has_short_call:
            return "COVERED_CALL"
        if has_short_put:
            return "SHORT_PUT"
        if shares >= 100:
            return "STOCK"
            
        return "CASH" # Or "PARTIAL_STOCK" / "LONG_STRATEGY" (ignored for now)

    def _step_sell_put(self, symbol, analysis_service):
        """Find meaningful support level and sell put."""
        analysis = analysis_service.analyze_symbol(symbol)
        if not analysis or 'error' in analysis: return
        
        # We want to be somewhat bullish or neutral.
        # Check sentiment/score.
        entry_signal = analysis.get('entry_signal', {})
        score = entry_signal.get('score', 0)
        
        # If extremely bearish, maybe hold off?
        current_price = analysis.get('current_price')
        
        # Find Support
        entry_points = analysis.get('entry_points', {}).get('put_entry_points', [])
        if not entry_points: return
        
        # Select strike: ~30 delta or strong support
        # Let's use the highest support level that is at least 2-3% OTM
        valid_strikes = [ep['price'] for ep in entry_points if ep['price'] < current_price * 0.98]
        if not valid_strikes: return
        
        target_strike = valid_strikes[-1]
        
        expiry = self._find_expiry(symbol)
        if not expiry: return
        
        self._log(f"Attempting to Sell Put on {symbol}. Strike: {target_strike} Exp: {expiry}")
        
        # Get Chain
        chain = self.tradier.get_option_chains(symbol, expiry)
        option = next((o for o in chain if o['strike'] == target_strike and o['option_type'] == 'put'), None)
        
        if option:
            price = (option['bid'] + option['ask']) / 2
            if price < 0.10: 
                self._log("Premium too low, skipping.")
                return
                
            # Place Order
            # Cash Secured Put is Selling to Open (equity/option class)
            response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='sell_to_open',
                quantity=1,
                order_type='limit',
                duration='day',
                price=round(price, 2),
                option_symbol=option['symbol'],
                order_class='option'
            )
            
            if 'error' in response:
                self._log(f"Order failed: {response['error']}")
            else:
                self._log(f"Sold CSP: {response}")
                self._record_trade(symbol, "Wheel: Sell Put", price, response)

    def _step_sell_covered_call(self, symbol, positions, analysis_service):
        """Sell call against shares."""
        analysis = analysis_service.analyze_symbol(symbol)
        current_price = analysis.get('current_price')
        
        # Find Resistance
        entry_points = analysis.get('entry_points', {}).get('call_entry_points', [])
        
        # Target strike above cost basis preferably, but definitely above current price (OTM)
        # For simple logic: target > 1.05 * current_price (5% OTM)
        
        target_strike = current_price * 1.05
        # Refine with resistance if available
        valid_res = [ep['price'] for ep in entry_points if ep['price'] > current_price * 1.03]
        if valid_res:
            target_strike = valid_res[0] # Lowest resistance that is safely OTM
            
        expiry = self._find_expiry(symbol)
        chain = self.tradier.get_option_chains(symbol, expiry)
        option = next((o for o in chain if o['strike'] >= target_strike and o['option_type'] == 'call'), None) # Find closest above target
        
        if option:
             price = (option['bid'] + option['ask']) / 2
             if price < 0.10: return

             self._log(f"Selling Covered Call on {symbol} (Holdings detected). Strike: {option['strike']}")
             
             response = self.tradier.place_order(
                account_id=self.tradier.account_id,
                symbol=symbol,
                side='sell_to_open',
                quantity=1,
                order_type='limit',
                duration='day',
                price=round(price, 2),
                option_symbol=option['symbol'],
                order_class='option'
            )
             if 'error' not in response:
                 self._record_trade(symbol, "Wheel: Sell Call", price, response)

    def _find_expiry(self, symbol):
        # Same helper as credit spreads, prefer 30-45 dte
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations: return None
        from datetime import date, timedelta
        exp_dates = [datetime.strptime(e, "%Y-%m-%d").date() for e in expirations]
        target_date = date.today() + timedelta(days=30)
        closest_date = min(exp_dates, key=lambda d: abs(d - target_date))
        return closest_date.strftime("%Y-%m-%d")

    def _record_trade(self, symbol, strategy, price, response):
        if self.db:
            self.db['auto_trades'].insert_one({
                "symbol": symbol,
                "strategy": strategy,
                "price": price,
                "entry_date": datetime.now(),
                "order_details": response,
                "status": "OPEN",
                "pnl": 0.0
            })
