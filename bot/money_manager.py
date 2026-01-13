from datetime import datetime
import logging
from bot.utils import is_match, get_op_type, get_underlying

class MoneyManager:
    def __init__(self, tradier_service, db, wheel_strategy, credit_spread_strategy):
        self.tradier = tradier_service
        self.db = db
        self.wheel_strategy = wheel_strategy
        self.credit_spread_strategy = credit_spread_strategy
        self.logger = logging.getLogger(__name__)

    def _log(self, message):
        print(f"[MONEY_MANAGER] {message}")
        if self.db is not None:
            try:
                self.db['bot_config'].update_one(
                    {"_id": "main_bot"},
                    {"$push": {"logs": {"$each": [{
                        "timestamp": datetime.now(),
                        "message": f"[MONEY_MANAGER] {message}"
                    }], "$slice": -100}}}
                )
            except Exception as e:
                print(f"Log Error: {e}")

    # ------------------------------------------------------------------
    # 1. THE ACCOUNTANT (Inventory Logic)
    # ------------------------------------------------------------------
    def get_inventory(self, symbol):
        """
        Calculate Current Inventory counts based on Paired Unit rules.
        """
        try:
            positions = self.tradier.get_positions()
        except Exception as e:
            self._log(f"Error fetching positions: {e}")
            return None

        symbol_positions = [p for p in positions if is_match(p, symbol)]
        
        # --- Wheel Inventory ---
        short_puts = [p for p in symbol_positions if get_op_type(p) == 'put' and p.get('quantity', 0) < 0]
        short_calls = [p for p in symbol_positions if get_op_type(p) == 'call' and p.get('quantity', 0) < 0]
        
        count_puts = sum(abs(p['quantity']) for p in short_puts)
        count_calls = sum(abs(p['quantity']) for p in short_calls)
        
        wheel_count = min(count_puts, count_calls)
        
        # --- Credit Spread Inventory ---
        puts = [p for p in symbol_positions if get_op_type(p) == 'put']
        calls = [p for p in symbol_positions if get_op_type(p) == 'call']
        
        count_put_spreads = 0
        count_call_spreads = 0
        
        short_puts_list = sorted([p for p in puts if p['quantity'] < 0], key=lambda x: x['strike'])
        long_puts_list = sorted([p for p in puts if p['quantity'] > 0], key=lambda x: x['strike'])
        
        long_puts_pool = [{'strike': p['strike'], 'qty': p['quantity']} for p in long_puts_list] # Qty is positive
        
        count_put_spreads = 0
        
        for sp in short_puts_list:
            short_qty = abs(sp['quantity'])
            short_strike = sp['strike']
            
            while short_qty > 0:
                match_idx = -1
                best_strike = -1
                
                for idx, lp in enumerate(long_puts_pool):
                    if lp['qty'] > 0 and lp['strike'] < short_strike:
                        if lp['strike'] > best_strike:
                            best_strike = lp['strike']
                            match_idx = idx
                
                if match_idx != -1:
                    available_long = long_puts_pool[match_idx]['qty']
                    matched_amt = min(short_qty, available_long)
                    count_put_spreads += matched_amt
                    short_qty -= matched_amt
                    long_puts_pool[match_idx]['qty'] -= matched_amt
                else:
                    break
        
        short_calls_list = sorted([p for p in calls if p['quantity'] < 0], key=lambda x: x['strike'])
        long_calls_list = sorted([p for p in calls if p['quantity'] > 0], key=lambda x: x['strike'])

        long_calls_pool = [{'strike': p['strike'], 'qty': p['quantity']} for p in long_calls_list]
        
        count_call_spreads = 0
        
        for sc in short_calls_list:
            short_qty = abs(sc['quantity'])
            short_strike = sc['strike']
            
            while short_qty > 0:
                match_idx = -1
                best_strike = 999999
                
                for idx, lc in enumerate(long_calls_pool):
                    if lc['qty'] > 0 and lc['strike'] > short_strike:
                        if lc['strike'] < best_strike:
                            best_strike = lc['strike']
                            match_idx = idx
                            
                if match_idx != -1:
                    available_long = long_calls_pool[match_idx]['qty']
                    matched_amt = min(short_qty, available_long)
                    count_call_spreads += matched_amt
                    short_qty -= matched_amt
                    long_calls_pool[match_idx]['qty'] -= matched_amt
                else:
                    break
        
        spread_count = min(count_put_spreads, count_call_spreads)

        total_short_puts_contracts = sum(abs(p['quantity']) for p in short_puts_list)
        total_short_calls_contracts = sum(abs(p['quantity']) for p in short_calls_list)
        
        wheel_puts = total_short_puts_contracts - count_put_spreads
        wheel_calls = total_short_calls_contracts - count_call_spreads
        
        final_wheel_count = min(wheel_puts, wheel_calls)
        
        return {
            "wheel_count": final_wheel_count,
            "spread_count": spread_count,
            "details": {
                "total_short_puts": total_short_puts_contracts,
                "total_short_calls": total_short_calls_contracts,
                "put_spreads": count_put_spreads,
                "call_spreads": count_call_spreads,
                "wheel_puts": wheel_puts,
                "wheel_calls": wheel_calls
            }
        }

    # ------------------------------------------------------------------
    # 2. THE DISPATCHER (Ladder Logic)
    # ------------------------------------------------------------------
    def process_symbol(self, symbol, target_wheel_qty, target_spread_qty):
        """
        Main execution loop for a symbol.
        """
        inventory = self.get_inventory(symbol)
        if not inventory: return
        
        self._log(f"Inventory for {symbol}: Wheel Units={inventory['wheel_count']}, Spread Units={inventory['spread_count']}")
        
        # 1. Wheel Ladder
        wheel_needed = target_wheel_qty - inventory['wheel_count']
        if wheel_needed > 0:
            self._log(f"🔸 Wheel Shortfall: {wheel_needed} units. Engaging Ladder.")
            self._run_wheel_ladder(symbol, wheel_needed)
        else:
            self._log(f"✅ Wheel Target Met ({inventory['wheel_count']}/{target_wheel_qty})")

        # 2. Spread Ladder
        spreads_needed = target_spread_qty - inventory['spread_count']
        if spreads_needed > 0:
            self._log(f"🔸 Spread Shortfall: {spreads_needed} units. Engaging Ladder.")
            self._run_spread_ladder(symbol, spreads_needed)
        else:
            self._log(f"✅ Spread Target Met ({inventory['spread_count']}/{target_spread_qty})")

    def _run_wheel_ladder(self, symbol, quantity_needed):
        """
        Wheel Ladder: Start $0.30, Step +$0.10.
        Check Resources (Cash/Shares) -> Trigger Put/Call/Both.
        """
        base_credit = 0.30
        step = 0.10
        
        for i in range(quantity_needed):
            target_credit = round(base_credit + (i * step), 2)
            
            has_cash = self._check_cash_availability()
            has_shares = self._check_share_availability(symbol)
            
            if has_cash and has_shares:
                self._log(f"⚡ Ladder Step {i+1} (${target_credit}): Firing BOTH Put & Call.")
                self.wheel_strategy.execute_single_leg(symbol, 'put', min_credit=target_credit)
                self.wheel_strategy.execute_single_leg(symbol, 'call', min_credit=target_credit)
            elif has_cash:
                self._log(f"⚡ Ladder Step {i+1} (${target_credit}): Firing Put Only (No Shares).")
                self.wheel_strategy.execute_single_leg(symbol, 'put', min_credit=target_credit)
            elif has_shares:
                self._log(f"⚡ Ladder Step {i+1} (${target_credit}): Firing Call Only (No Cash).")
                self.wheel_strategy.execute_single_leg(symbol, 'call', min_credit=target_credit)
            else:
                self._log(f"⚠️ Ladder Step {i+1} Skipped: No Resources.")

    def _run_spread_ladder(self, symbol, quantity_needed):
        """
        Spread Ladder: Start $0.80, Step +$0.10.
        Always Trigger BOTH Put Spread and Call Spread.
        """
        base_credit = 0.80
        step = 0.10
        
        for j in range(quantity_needed):
            target_credit = round(base_credit + (j * step), 2)
            
            self._log(f"⚡ Spread Ladder Step {j+1} (${target_credit}): Firing Put & Call Spreads.")
            
            # Fire Trigger A: Call Spread
            self.credit_spread_strategy.execute_spread(symbol, 'call', min_credit=target_credit)
            
            # Fire Trigger B: Put Spread
            self.credit_spread_strategy.execute_spread(symbol, 'put', min_credit=target_credit)

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _check_cash_availability(self):
        """Simple check if we have buying power."""
        balances = self.tradier.get_account_balances()
        if not balances: return False
        return balances.get('option_buying_power', 0) > 1000 # Min threshold?

    def _check_share_availability(self, symbol):
        """Check if we have 100+ unencumbered shares."""
        positions = self.tradier.get_positions()
        shares = sum(p['quantity'] for p in positions if p['symbol'] == symbol)
        
        short_calls = [p for p in positions if get_underlying(p['symbol']) == symbol and p['symbol'] != symbol and get_op_type(p) == 'call' and p['quantity'] < 0]
        encumbered = sum(abs(p['quantity']) for p in short_calls) * 100
        
        return (shares - encumbered) >= 100
