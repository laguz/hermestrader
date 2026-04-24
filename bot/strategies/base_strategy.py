from abc import ABC, abstractmethod
from datetime import datetime, date, timedelta
from bot.utils import Colors, get_expiry_str

class AbstractStrategy(ABC):
    def __init__(self, tradier_service, db, dry_run=False, analysis_service=None, strategy_id=None, trade_manager=None):
        self.tradier = tradier_service
        self.db = db
        self.dry_run = dry_run
        self.execution_logs = []
        self.strategy_id = strategy_id or self.__class__.__name__
        self.trade_manager = trade_manager
        
        if analysis_service:
            self.analysis_service = analysis_service
        else:
            from services.container import Container
            self.analysis_service = Container.get_analysis_service()

    def _log(self, message, strategy_name=None):
        """Standardized logging to DB and stdout."""
        timestamp = self._get_current_datetime()
        log_entry = f"{timestamp.strftime('%H:%M:%S')} - {message}"
        self.execution_logs.append(log_entry)
        
        if strategy_name is None:
            strategy_name = self.__class__.__name__
            
        prefix = f"[{strategy_name.upper()}]"
        
        # UI/Console Logging
        if self.dry_run:
            if any(x in message for x in ["Analyzing", "📦"]):
                print(f"{Colors.HEADER}   {message}{Colors.ENDC}")
            elif "✅" in message or "•" in message:
                print(f"{Colors.OKGREEN}   {message}{Colors.ENDC}")
            elif "Skipping" in message or "🔹" in message:
                print(f"{Colors.OKCYAN}   {message}{Colors.ENDC}")
            elif any(x in message for x in ["Error", "failed", "❌", "🚫", "⚠️"]):
                print(f"{Colors.FAIL}   {message}{Colors.ENDC}")
            else:
                print(f"   {message}")
        else:
            print(f"{prefix} {message}")
            
        # DB Logging
        try:
            if self.db is not None:
                self.db['bot_config'].update_one(
                    {"_id": "main_bot"},
                    {"$push": {"logs": {"$each": [{
                        "timestamp": timestamp,
                        "message": f"{prefix} {message}"
                    }], "$slice": -100}}}
                )
        except Exception as e:
            print(f"Log Error: {e}")

    def _get_current_date(self):
        """Get effective current date (handling simulation/backtest)."""
        if hasattr(self.tradier, 'current_date') and self.tradier.current_date:
            return self.tradier.current_date.date()
        return date.today()

    def _is_short_option(self, pos):
        """Check if a position represents a short option."""
        from bot.utils import get_op_type
        op_type = get_op_type(pos)
        if op_type in ['put', 'call']:
            return pos.get('quantity', 0) < 0
        return False

    def _get_underlying_from_pos(self, pos):
        """Extract underlying symbol from a position object."""
        from bot.utils import get_underlying
        return get_underlying(pos.get('symbol', ''))

    def _get_current_datetime(self):
        """Get effective current datetime (handling simulation/backtest)."""
        if hasattr(self.tradier, 'current_date') and self.tradier.current_date:
             return self.tradier.current_date
        return datetime.now()

    def _get_available_bp(self, config=None):
        """Returns the True Available Option Buying Power (OBP - min_obp_reserve)."""
        if config is None:
             config = getattr(self, 'config', {}) or {}
        min_reserve = config.get('min_obp_reserve', 1000)
        
        balances = self.tradier.get_account_balances()
        if not balances:
             return 0
             
        obp = balances.get('option_buying_power', 0)
        return max(0, obp - min_reserve)

    def _is_bp_sufficient(self, requirement, config=None):
        """Check if Option Buying Power is sufficient after reserve."""
        if config is None:
             config = getattr(self, 'config', {}) or {}
        min_reserve = config.get('min_obp_reserve', 1000)
        
        balances = self.tradier.get_account_balances()
        if not balances:
            self._log("⚠️ Could not fetch balances for BP check. Skipping trade for safety.")
            return False
        
        obp = balances.get('option_buying_power', 0)
        if obp - requirement < min_reserve:
            self._log(f"🚫 Insufficient Buying Power: OBP ${obp:,.2f} - Req ${requirement:,.2f} < Reserve ${min_reserve:,.2f}")
            return False
            
        return True

    def _find_expiry(self, symbol, target_dte=42, min_dte=None, max_dte=None, exclude_dates=None, method='closest'):
        """Unified expiry search logic."""
        expirations = self.tradier.get_option_expirations(symbol)
        if not expirations: return None
        
        exclude_dates = exclude_dates or []
        
        exp_dates = []
        for e in expirations:
            # Handle both string and date objects from TradierService
            d = datetime.strptime(e, "%Y-%m-%d").date() if isinstance(e, str) else e
            if d.strftime("%Y-%m-%d") in exclude_dates:
                 continue
            exp_dates.append(d)
        
        if not exp_dates:
            self._log(f"No valid expirations found (Excluded: {exclude_dates})")
            return None
        
        today = self._get_current_date()
        candidates = []
        
        for d in exp_dates:
            dte = (d - today).days
            if min_dte is not None and max_dte is not None:
                if not (min_dte <= dte <= max_dte):
                    continue
            candidates.append(d)
        
        if not candidates:
            rng = f"[{min_dte}, {max_dte}]" if min_dte is not None else "Any"
            self._log(f"No expirations found in DTE range {rng} for {symbol}.")
            return None

        if method == 'min':
            best_date = min(candidates)
        else:
            target_date = today + timedelta(days=target_dte)
            best_date = min(candidates, key=lambda d: abs((d - today).days - target_dte))
            
        return best_date.strftime("%Y-%m-%d")

    def _find_delta_strike(self, chain, option_type, min_d, max_d, target_d=None):
        """Unified delta-based strike selection."""
        candidates = []
        for opt in chain:
            if opt['option_type'] != option_type: continue
            delta = opt.get('greeks', {}).get('delta')
            if delta is None: continue
            
            abs_delta = abs(delta)
            if min_d <= abs_delta <= max_d:
                candidates.append((opt, abs_delta))
        
        if not candidates: return None, None
        
        # If target_d not provided, default to min_d (standard/safe)
        target = target_d if target_d is not None else min_d
        best = min(candidates, key=lambda x: abs(x[1] - target))
        return best[0]['strike'], best[1]

    def _record_trade(self, symbol, strategy, price, response, extra_fields=None):
        """Record trade to DB."""
        if self.db is not None:
            doc = {
                "symbol": symbol,
                "strategy": self.strategy_id,
                "price": price,
                "timestamp": datetime.now(),
                "order_details": response,
                "status": "DRY_RUN" if self.dry_run else "OPEN",
                "is_dry_run": self.dry_run
            }
            if extra_fields:
                doc.update(extra_fields)
            self.db['active_trades'].insert_one(doc)

    def _close_trade(self, underlying, option_symbol, exit_price, btc_res=None):
        """Find the matching STO trade in the DB and mark it CLOSED with P&L calculated."""
        if self.db is not None:
            # Find the OPEN trade matching underlying
            query = {"symbol": underlying, "status": "OPEN", "strategy": self.strategy_id}
            
            # Prioritize exact match if option_symbol was tracked
            match = self.db['active_trades'].find_one({**query, "option_symbol": option_symbol})
            if not match:
                match = self.db['active_trades'].find_one(query, sort=[("timestamp", 1)])
                
            if match:
                entry_price = match.get('price', 0)
                qty = match.get('quantity', 1)
                pnl = (entry_price - exit_price) * 100 * qty
                
                self.db['active_trades'].update_one(
                    {"_id": match['_id']},
                    {"$set": {
                        "status": "CLOSED",
                        "close_date": datetime.now(),
                        "exit_price": exit_price,
                        "pnl": round(pnl, 2),
                        "close_order_details": btc_res
                    }}
                )
            else:
                self._log(f"⚠️ Could not find OPEN auto_trade for {underlying} to mark CLOSED.")

    def get_open_trades(self):
        """Helper to safely fetch strictly isolated trades bound to this specific script."""
        if self.trade_manager is not None:
            return self.trade_manager.get_my_trades(self.strategy_id, status="OPEN")
        # Fallback if unconfigured
        if self.db is not None:
            return list(self.db['active_trades'].find({"strategy": self.strategy_id, "status": "OPEN"}))
        return []

    def _count_existing_on_expiry(self, symbol, expiry):
        """Count open spread lots for this strategy on a specific expiry chain."""
        open_trades = self.get_open_trades()
        count = 0
        for trade in open_trades:
            if trade.get('symbol') != symbol:
                continue
            short_leg = trade.get('short_leg', '')
            exp_str = get_expiry_str(short_leg) if short_leg else None
            if exp_str == expiry:
                # Get lots from legs_info or quantity
                legs = trade.get('legs_info', [])
                if isinstance(legs, list) and legs:
                    count += abs(legs[0].get('quantity', 1))
                else:
                    count += trade.get('quantity', 1)
        return count

    @abstractmethod
    def execute(self, watchlist, config=None):
        pass
