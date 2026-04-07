import time
import logging
import threading
import traceback
from datetime import datetime, timedelta
from exceptions import AppError
from services.container import Container

# Strategy imports needed for type hinting / proper resolving
from bot.strategies.credit_spreads import CreditSpreadStrategy
from bot.strategies.credit_spreads_7 import CreditSpreads7Strategy
from bot.strategies.credit_spreads_75 import CreditSpreads75Strategy
from bot.strategies.tastytrade45 import TastyTrade45Strategy
from bot.strategies.wheel import WheelStrategy
from bot.portfolio_manager import PortfolioManager

logger = logging.getLogger(__name__)

class BotService:
    _instance = None
    _thread = None
    _stop_event = threading.Event()
    
    # Singleton pattern to ensure only one bot thread
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(BotService, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance

    def __init__(self):
        if self.initialized: return
        self.db = Container.get_db()
        self.tradier = Container.get_tradier_service()
        self.ml_service = None # Lazy load to avoid circular deps if any

        from bot.trade_manager import TradeManager
        self.trade_manager = TradeManager(self.tradier, self.db)
        
        # Initialize Strategies
        self.credit_spread_strategy = CreditSpreadStrategy(self.tradier, self.db, trade_manager=self.trade_manager)
        self.credit_spread_7_strategy = CreditSpreads7Strategy(self.tradier, self.db, trade_manager=self.trade_manager)
        self.credit_spread_75_strategy = CreditSpreads75Strategy(self.tradier, self.db, trade_manager=self.trade_manager)
        self.tastytrade45_strategy = TastyTrade45Strategy(self.tradier, self.db, trade_manager=self.trade_manager)
        self.wheel_strategy = WheelStrategy(self.tradier, self.db, trade_manager=self.trade_manager)
        self.portfolio_manager = PortfolioManager(self.tradier, self.db)
        
        self._init_db_config()
        # Reset state on startup to avoid phantom running state
        self._update_status("STOPPED")
        self.ml_training_in_progress = False
        self.initialized = True
        
        # Resurrect state on app start? 
        # If DB says active, we should theoretically auto-start. 
        # But for safety, we default to stopped on restart unless explicit.

    def _get_ml_service(self):
        """Lazy-load MLService to avoid circular imports."""
        if self.ml_service is None:
            from services.ml_service import MLService
            self.ml_service = MLService(self.tradier)
        return self.ml_service

    def _init_db_config(self):
        """Ensure initial bot configuration exists in DB."""
        if self.db is not None:
             status = self.db['bot_config'].find_one({"_id": "main_bot"})
             
             # prepare default structure
             defaults = {
                 "_id": "main_bot",
                 "status": "STOPPED", # STOPPED, RUNNING, ERROR
                 "last_heartbeat": None,
                 "logs": [],
                 "settings": {
                     "watchlist_credit_spreads": [], # Start empty, user adds via UI
                     "watchlist_credit_spreads_7": [],
                     "watchlist_credit_spreads_75": [],
                     "watchlist_tastytrade45": [],
                     "watchlist_wheel": [],         # Start empty, user adds via UI
                     "max_drawdown": 500,
                     "max_position_size": 1000,
                     "max_credit_spreads_per_symbol": 5,
                     "max_credit_spreads_7_per_symbol": 5,
                     "max_credit_spreads_75_per_symbol": 5,
                     "max_tastytrade45_per_symbol": 5,
                     "max_total_credit_spreads": 10,
                     "max_wheel_contracts_per_symbol": 1
                 }
             }

             if not status:
                 self.db['bot_config'].insert_one(defaults)
             else:
                 # Migration: Ensure new fields exist if old doc exists
                 settings = status.get('settings', {})
                 updates = {}
                 if 'watchlist_credit_spreads' not in settings:
                     updates['settings.watchlist_credit_spreads'] = []
                 if 'watchlist_credit_spreads_7' not in settings:
                     updates['settings.watchlist_credit_spreads_7'] = []
                 if 'watchlist_credit_spreads_75' not in settings:
                     updates['settings.watchlist_credit_spreads_75'] = []
                 if 'watchlist_tastytrade45' not in settings:
                     updates['settings.watchlist_tastytrade45'] = []
                 if 'watchlist_wheel' not in settings:
                    updates['settings.watchlist_wheel'] = []
                 if 'max_wheel_contracts_per_symbol' not in settings:
                     updates['settings.max_wheel_contracts_per_symbol'] = 1
                 if 'max_credit_spreads_7_per_symbol' not in settings:
                     updates['settings.max_credit_spreads_7_per_symbol'] = 5
                 if 'max_credit_spreads_75_per_symbol' not in settings:
                     updates['settings.max_credit_spreads_75_per_symbol'] = 5
                 if 'max_tastytrade45_per_symbol' not in settings:
                     updates['settings.max_tastytrade45_per_symbol'] = 5
                 
                 if updates:
                     self.db['bot_config'].update_one({"_id": "main_bot"}, {"$set": updates})

    def get_status(self):
        if self.db is None: return {"status": "ERROR", "message": "DB Unavailable"}
        return self.db['bot_config'].find_one({"_id": "main_bot"}) or {}

    def start_bot(self):
        if self._thread and self._thread.is_alive():
             return {"message": "Bot is already running."}
        
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        
        self._update_status("RUNNING")
        self._log("Bot started successfully.")
        return {"message": "Bot started."}

    def stop_bot(self):
        # Check if thread is alive
        if not self._thread or not self._thread.is_alive():
            # If thread is dead but DB says otherwise, sync it.
            current = self.get_status().get('status')
            if current != 'STOPPED':
                 self._update_status("STOPPED")
                 self._log("Bot state synchronized (was phantom RUNNING).")
            return {"message": "Bot is not running."}
            
        self._stop_event.set()
        self._update_status("STOPPING")
        self._log("Bot stop signal sent...")
        return {"message": "Bot is stopping..."}

    def _update_status(self, status):
        if self.db is None: return
        self.db['bot_config'].update_one(
            {"_id": "main_bot"},
            {"$set": {"status": status, "last_heartbeat": datetime.now()}}
        )

    def update_watchlist(self, watchlist, list_type="credit_spreads"):
        """Update the watchlist in settings. list_type: 'credit_spreads' or 'wheel'"""
        if self.db is None: return False
        # Validate input (list of strings)
        if not isinstance(watchlist, list):
            return False
        
        # Upper case and dedup, maintaining original order
        raw_list = [str(s).upper().strip() for s in watchlist if s]
        seen = set()
        clean_list = []
        for s in raw_list:
            if s not in seen:
                clean_list.append(s)
                seen.add(s)
        
        # Map frontend type to DB key
        db_key = f"settings.watchlist_{list_type}"
        # Safety check to only allow specific keys
        if list_type not in ['credit_spreads', 'wheel', 'credit_spreads_7', 'credit_spreads_75', 'tastytrade45']:
            self._log(f"Error: Invalid watchlist type {list_type}")
            return None

        self.db['bot_config'].update_one(
            {"_id": "main_bot"},
            {"$set": {db_key: clean_list}}
        )
        self._log(f"Watchlist ({list_type}) updated: {clean_list}")
        return clean_list

    def update_settings(self, settings_update):
        """Generic method to update settings fields."""
        if self.db is None: return False
        if not isinstance(settings_update, dict): return False
        
        # Whitelist allowed keys to prevent overwriting critical internal state
        allowed_keys = [
            'max_drawdown',
            'max_position_size',
            'max_credit_spreads_per_symbol',
            'max_credit_spreads_7_per_symbol',
            'max_credit_spreads_75_per_symbol',
            'max_tastytrade45_per_symbol',
            'max_total_credit_spreads',
            'max_wheel_contracts_per_symbol'
        ]
        
        safe_updates = {}
        for k, v in settings_update.items():
            if k in allowed_keys:
                # Ensure we cast to appropriate types if needed (e.g. int/float)
                # For now assume input is clean or cast safe
                try:
                    if 'max' in k: safe_updates[f"settings.{k}"] = int(v)
                    else: safe_updates[f"settings.{k}"] = v
                except Exception:
                   self._log(f"Invalid value for {k}: {v}")

        if not safe_updates:
            return False

        self.db['bot_config'].update_one(
            {"_id": "main_bot"},
            {"$set": safe_updates}
        )
        self._log(f"Settings updated: {safe_updates}")
        return True

    # ... (logs, status methods unchanged) ...

    def sync_open_positions(self):
        """
        Sync Tradier positions to DB with Lifecycle Tracking.
        Delegated to PortfolioManager.
        """
        return self.portfolio_manager.sync_open_positions(self._log)

    def _backfill_history(self):
        """
        Fetch historical closed positions from Gain/Loss and populate DB if missing.
        Delegated to PortfolioManager.
        """
        self.portfolio_manager._backfill_history(self._log)

    def get_open_positions_pnl(self):
        """
        Calculate P&L for tracked positions.
        Returns: { 'open': [...], 'closed': [...] }
        Delegated to PortfolioManager.
        """
        return self.portfolio_manager.get_open_positions_pnl(self._log)

    def get_trades(self, limit=50):
        """Fetch recent bot trades."""
        if self.db is None: return []
        cursor = self.db['auto_trades'].find().sort("entry_date", -1).limit(limit)
        
        trades = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            trades.append(doc)
        return trades

    def get_unmanaged_orphans(self):
        """Retrieve MANUAL_ORPHAN trades."""
        if not hasattr(self, 'trade_manager'): return []
        orphans = self.trade_manager.get_unmanaged_orphans()
        for o in orphans:
             o['_id'] = str(o['_id'])
        return orphans

    def close_unmanaged_orphan(self, trade_id):
        """Send a market close order natively for an unmanaged orphan, bypassing strategies."""
        from bson.objectid import ObjectId
        if not hasattr(self, 'trade_manager'): return {"error": "Trade Manager offline"}
        if self.db is None: return {"error": "DB Offline"}
        
        try:
             trade = self.db['active_trades'].find_one({"_id": ObjectId(trade_id), "strategy": "MANUAL_ORPHAN", "status": "OPEN"})
             if not trade: 
                 return {"error": "Orphan trade not found or already closed"}
                 
             symbol = trade.get('symbol')
             qty = int(trade.get('quantity', 0))
             
             if qty == 0:
                 self.db['active_trades'].update_one({"_id": ObjectId(trade_id)}, {"$set": {"status": "CLOSED"}})
                 return {"status": "ok", "msg": "Trade verified closed (0 qty)"}
                 
             side = 'sell_to_close' if qty > 0 else 'buy_to_close'
             
             import bot.utils as utils
             underlying = utils.get_underlying(symbol)
             
             if not self.tradier:
                 pass
             else:
                 res = self.tradier.place_order(
                    account_id=self.tradier.account_id,
                    symbol=underlying,
                    side=side,
                    quantity=abs(qty),
                    order_type='market',
                    duration='day',
                    option_symbol=symbol,
                    order_class='option',
                    tag="MANUAL_CLOSE"
                 )
                 if 'error' in res:
                     return {"error": res['error']}
             
             self.trade_manager.mark_trade_closed(ObjectId(trade_id), limit_price=None, response_id=None)
             self._log(f"✅ User manually liquidated ORPHAN trade: {symbol}")
             return {"status": "ok", "msg": f"Closed {symbol} successfully."}
             
        except Exception as e:
             return {"error": str(e)}

    def get_performance_summary(self):
        """Calculate P&L metrics."""
        if self.db is None: return {}
        
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "total_pnl": {"$sum": "$pnl"},
                    "total_trades": {"$sum": 1},
                    "wins": {
                        "$sum": { 
                            "$cond": [ { "$gt": [ "$pnl", 0 ] }, 1, 0 ] 
                        }
                    }
                }
            }
        ]
        
        result = list(self.db['auto_trades'].aggregate(pipeline))
        if not result:
            return {
                "total_pnl": 0.0,
                "total_trades": 0,
                "win_rate": 0.0
            }
            
        stats = result[0]
        total = stats['total_trades']
        wins = stats['wins']
        win_rate = (wins / total * 100) if total > 0 else 0.0
        
        return {
            "total_pnl": stats['total_pnl'],
            "total_trades": total,
            "win_rate": round(win_rate, 2)
        }

    def _log(self, message):
        """Append log to DB."""
        if self.db is None: return
        logger.info(f"[BOT] {message}")
        entry = {
            "timestamp": datetime.now(),
            "message": message
        }
        # Keep last 100 logs
        self.db['bot_config'].update_one(
            {"_id": "main_bot"},
            {"$push": {"logs": {"$each": [entry], "$slice": -100}}}
        )

    def _run_loop(self):
        """Main execution loop."""
        self._log("Entering main loop.")
        
        # TradeManager: Reconcile dynamic orphan tags & register strategy states on boot
        if hasattr(self, 'trade_manager'):
            self.trade_manager.reconcile_orphans(self._log)
            self.trade_manager.register_strategy(self.credit_spread_strategy.strategy_id)
            self.trade_manager.register_strategy(self.credit_spread_7_strategy.strategy_id)
            self.trade_manager.register_strategy(self.credit_spread_75_strategy.strategy_id)
            self.trade_manager.register_strategy(self.tastytrade45_strategy.strategy_id)
            self.trade_manager.register_strategy(self.wheel_strategy.strategy_id)
        
        while not self._stop_event.is_set():
            try:
                # 1. Heartbeat - Only update if not stopping
                if self._stop_event.is_set(): break
                self._update_status("RUNNING")
                
                # 3. Strategy Execution
                config = self.get_status().get('settings', {})
                wl_spreads = config.get('watchlist_credit_spreads', [])
                wl_spreads_7 = config.get('watchlist_credit_spreads_7', [])
                wl_spreads_75 = config.get('watchlist_credit_spreads_75', [])
                wl_tastytrade45 = config.get('watchlist_tastytrade45', [])
                wl_wheel = config.get('watchlist_wheel', [])
                
                # 3. Global Circuit Breaker Check and Strategy Execution
                if self._check_circuit_breaker(config):
                     self._execute_strategies(config)

                # 4. ML Scheduler: Daily Predictions + Biweekly Training
                self._run_ml_scheduler(config)
                    
                # Sleep cycle - responsive wait
                # Wait for 60 seconds (strategies shouldn't run too hot) or until stop event
                if self._stop_event.wait(timeout=60):
                    break
                    
            except Exception as e:
                logger.error(f"Error in bot loop: {e}", exc_info=True)
                self._log(f"Error in loop: {e}")
                # Prevent tight loop on error (wait 10s)
                if self._stop_event.wait(timeout=10):
                    break
        
        self._update_status("STOPPED")
        self._update_status("STOPPED")
        self._log("Bot loop ended.")

    def _check_circuit_breaker(self, config):
        """Returns True if trading is allowed, False if buying power is too low."""
        balances = self.tradier.get_account_balances()
        min_reserve = config.get('global_min_obp_reserve', 0) # Floor set to lowest requirement
        
        if balances:
            obp = balances.get('option_buying_power', 0)
            if obp < min_reserve:
                self._log(f"🛑 CIRCUIT BREAKER: Option Buying Power ${obp:,.2f} is below Minimum Reserve ${min_reserve:,.2f}. Skipping entries.")
                
                # Management ONLY logic
                self._log(f"Running Credit Spread MANAGEMENT (Exits) ONLY...")
                self.credit_spread_strategy.manage_positions()
                self.credit_spread_75_strategy.manage_positions()
                self.tastytrade45_strategy.manage_positions()
                
                return False
            return True
        else:
            acct_id = self.tradier._get_account_id()
            if not acct_id:
                self._log("⚠️ Could not fetch balances: TRADIER_ACCOUNT_ID is missing.")
            else:
                # If ID exists but still no balances, it's likely an API/Auth error
                self._log("⚠️ Could not fetch balances. Check API credentials and connectivity.")
            return False
            
    def _execute_strategies(self, config):
        """Executes all active trading strategies."""
        wl_spreads = config.get('watchlist_credit_spreads', [])
        wl_wheel = config.get('watchlist_wheel', [])

        # Priority 1: Wheel Strategy
        if wl_wheel:
            self._log(f"Running Wheel Strategy on {len(wl_wheel)} symbols...")
            wheel_config = config.copy()
            wheel_config['min_obp_reserve'] = 26000
            self.wheel_strategy.execute(wl_wheel, wheel_config)

        # Priority 2: Credit Spread Strategy
        if wl_spreads:
            self._log(f"Running Credit Spread Strategy on {len(wl_spreads)} symbols...")
            self.credit_spread_strategy.manage_positions()
            cs_config = config.copy()
            cs_config['min_obp_reserve'] = 0
            self.credit_spread_strategy.execute(wl_spreads, cs_config)

        # Priority 3: 7DTE Credit Spread Strategy
        wl_spreads_7 = config.get('watchlist_credit_spreads_7', [])
        if wl_spreads_7:
            self._log(f"Running 7DTE Credit Spread Strategy on {len(wl_spreads_7)} symbols...")
            cs7_config = config.copy()
            cs7_config['min_obp_reserve'] = 0
            cs7_config['max_credit_spreads_per_symbol'] = config.get('max_credit_spreads_7_per_symbol', 5)
            self.credit_spread_7_strategy.execute(wl_spreads_7, cs7_config)

        # Priority 4: 45DTE Credit Spreads Strategy (75 POP)
        wl_spreads_75 = config.get('watchlist_credit_spreads_75', [])
        if wl_spreads_75:
            self._log(f"Managing & Running 45DTE/75POP Credit Spread Strategy on {len(wl_spreads_75)} symbols...")
            self.credit_spread_75_strategy.manage_positions()
            cs75_config = config.copy()
            cs75_config['min_obp_reserve'] = 0
            cs75_config['max_credit_spreads_per_symbol'] = config.get('max_credit_spreads_75_per_symbol', 5)
            self.credit_spread_75_strategy.execute(wl_spreads_75, cs75_config)

        # Priority 5: TastyTrade45
        wl_tastytrade45 = config.get('watchlist_tastytrade45', [])
        if wl_tastytrade45:
            self._log(f"Managing & Running TastyTrade45 Strategy on {len(wl_tastytrade45)} symbols...")
            self.tastytrade45_strategy.manage_positions()
            tt45_config = config.copy()
            tt45_config['min_obp_reserve'] = 0
            tt45_config['max_tastytrade45_per_symbol'] = config.get('max_tastytrade45_per_symbol', 5)
            self.tastytrade45_strategy.execute(wl_tastytrade45, tt45_config)
    def _run_ml_scheduler(self, config):
        """
        Run daily predictions and biweekly training for all watchlist symbols.
        Called from _run_loop after strategy execution.
        """
        today_str = datetime.now().strftime('%Y-%m-%d')

        # Collect unique symbols from ALL watchlists
        all_symbols = set()
        all_symbols.update(config.get('watchlist_credit_spreads', []))
        all_symbols.update(config.get('watchlist_wheel', []))
        all_symbols.update(config.get('watchlist_credit_spreads_7', []))
        all_symbols.update(config.get('watchlist_credit_spreads_75', []))
        all_symbols.update(config.get('watchlist_tastytrade45', []))
        all_symbols = sorted(all_symbols)

        if not all_symbols:
            return

        # Read scheduler state from DB
        bot_doc = self.db['bot_config'].find_one({"_id": "main_bot"}) or {}
        scheduler = bot_doc.get('ml_scheduler', {})
        last_prediction_date = scheduler.get('last_prediction_date', '')
        last_training_date = scheduler.get('last_training_date', '')

        # --- Daily Predictions ---
        if last_prediction_date != today_str:
            self._log(f"🔮 Running daily ML predictions for {len(all_symbols)} symbols...")
            try:
                ml = self._get_ml_service()
                results = ml.run_batch_predictions(all_symbols)
                self._log(f"📊 Predictions done: {results['success']} OK, {results['skipped']} skipped, {results['errors']} errors")

                # Mark as done for today
                self.db['bot_config'].update_one(
                    {"_id": "main_bot"},
                    {"$set": {"ml_scheduler.last_prediction_date": today_str}}
                )
            except Exception as e:
                logger.error(f"ML Prediction scheduler error: {e}", exc_info=True)
                self._log(f"❌ ML Predictions failed: {e}")

        # --- Biweekly Training ---
        should_train = False
        if not last_training_date:
            should_train = True  # Never trained before
        else:
            try:
                last_dt = datetime.strptime(last_training_date, '%Y-%m-%d')
                if (datetime.now() - last_dt).days >= 14:
                    should_train = True
            except ValueError:
                should_train = True  # Invalid date, retrain

        if should_train:
            # Check persistent status in DB
            if scheduler.get('status') == "TRAINING":
                last_attempt = scheduler.get('last_attempt_date', '')
                if last_attempt == today_str:
                    self._log("⚠️ ML Training already in progress or attempted today. Skipping trigger.")
                    return

            self._log(f"🎓 Starting biweekly ML training for {len(all_symbols)} symbols (background thread)...")
            
            # Mark as TRAINING in DB BEFORE spawning
            self.db['bot_config'].update_one(
                {"_id": "main_bot"},
                {"$set": {
                    "ml_scheduler.status": "TRAINING",
                    "ml_scheduler.last_attempt_date": today_str
                }}
            )
            
            def _train_worker():
                try:
                    ml = self._get_ml_service()
                    results = ml.run_batch_training(all_symbols, express=True)
                    self._log(f"🎓 Training done: {results['success']} OK, {results['errors']} errors")

                    # Mark as READY and set last_training_date
                    self.db['bot_config'].update_one(
                        {"_id": "main_bot"},
                        {"$set": {
                            "ml_scheduler.status": "READY",
                            "ml_scheduler.last_training_date": today_str
                        }}
                    )
                except Exception as e:
                    logger.error(f"ML Training scheduler error: {e}", exc_info=True)
                    self._log(f"❌ ML Training failed: {e}")
                    # Mark as FAILED to prevent immediate retry
                    self.db['bot_config'].update_one(
                        {"_id": "main_bot"},
                        {"$set": {"ml_scheduler.status": "FAILED"}}
                    )

            train_thread = threading.Thread(target=_train_worker, daemon=True)
            train_thread.start()

    def run_dry_run(self, data: dict = None) -> dict:
        """
        Execute a single dry-run cycle of all strategies.
        Returns {'status': 'success', 'logs': [...]} or {'error': '...'}.
        """
        data = data or {}
        tradier_service = self.tradier
        db = self.db

        # --- Resolve watchlists from request data, DB config, or defaults ---
        def _resolve_watchlist(request_key: str, db_key: str, defaults: list) -> list:
            wl = data.get(request_key)
            if not wl and request_key == 'credit_spreads_watchlist':
                wl = data.get('watchlist')  # backward compat
            if not wl:
                bot_cfg = db.bot_config.find_one({"_id": "main_bot"}) or {}
                wl = bot_cfg.get('settings', {}).get(db_key, [])
            return wl or defaults

        cs_watchlist = _resolve_watchlist(
            'credit_spreads_watchlist', 'watchlist_credit_spreads',
            ['SPY', 'QQQ', 'IWM', 'TSLA', 'AAPL', 'NVDA', 'AMZN', 'GOOGL', 'MSFT', 'DIA']
        )
        cs7_watchlist = _resolve_watchlist(
            'credit_spreads_7_watchlist', 'watchlist_credit_spreads_7',
            ['SPY', 'QQQ', 'IWM', 'TSLA', 'AAPL', 'NVDA', 'AMZN', 'GOOGL', 'MSFT', 'DIA']
        )
        cs75_watchlist = _resolve_watchlist(
            'credit_spreads_75_watchlist', 'watchlist_credit_spreads_75',
            ['SPY', 'QQQ', 'IWM', 'TSLA', 'AAPL', 'NVDA', 'AMZN', 'GOOGL', 'MSFT', 'DIA']
        )
        tt45_watchlist = _resolve_watchlist(
            'tastytrade45_watchlist', 'watchlist_tastytrade45',
            ['SPY', 'QQQ', 'IWM', 'TSLA', 'AAPL', 'NVDA', 'AMZN', 'GOOGL', 'MSFT', 'DIA']
        )
        wheel_watchlist = _resolve_watchlist(
            'wheel_watchlist', 'watchlist_wheel',
            ['SPY', 'IWM', 'QQQ', 'DIA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
        )

        logger.debug(f"Dry-run watchlists — CS: {cs_watchlist}, Wheel: {wheel_watchlist}")

        # Fetch config
        bot_config = (db.bot_config.find_one({"_id": "main_bot"}) or {}).get('settings', {})
        all_logs: list[str] = []

        # --- Credit Spreads ---
        all_logs.append(f"--- Credit Spread Strategy (Limit: {bot_config.get('max_credit_spreads_per_symbol', 5)}) ---")
        try:
            strategy_cs = CreditSpreadStrategy(tradier_service, db, dry_run=True)
            cs_logs = strategy_cs.execute(cs_watchlist, bot_config)
            all_logs.extend(cs_logs)

            all_logs.append(f"--- 7DTE Credit Spread Strategy ---")
            strategy_cs7 = CreditSpreads7Strategy(tradier_service, db, dry_run=True)
            bot_config_cs7 = bot_config.copy()
            bot_config_cs7['max_credit_spreads_per_symbol'] = bot_config.get('max_credit_spreads_7_per_symbol', 5)
            cs7_logs = strategy_cs7.execute(cs7_watchlist, bot_config_cs7)
            all_logs.extend(cs7_logs)

            all_logs.append(f"--- 45DTE/75POP Credit Spread Strategy ---")
            strategy_cs75 = CreditSpreads75Strategy(tradier_service, db, dry_run=True)
            bot_config_cs75 = bot_config.copy()
            bot_config_cs75['max_credit_spreads_per_symbol'] = bot_config.get('max_credit_spreads_75_per_symbol', 5)
            cs75_logs_management = strategy_cs75.manage_positions(simulation_mode=True)
            if cs75_logs_management: all_logs.extend(cs75_logs_management)
            cs75_logs = strategy_cs75.execute(cs75_watchlist, bot_config_cs75)
            all_logs.extend(cs75_logs)

            all_logs.append(f"--- TastyTrade45 Strategy ---")
            strategy_tt45 = TastyTrade45Strategy(tradier_service, db, dry_run=True)
            bot_config_tt45 = bot_config.copy()
            bot_config_tt45['max_tastytrade45_per_symbol'] = bot_config.get('max_tastytrade45_per_symbol', 5)
            tt45_logs_management = strategy_tt45.manage_positions(simulation_mode=True)
            if tt45_logs_management: all_logs.extend(tt45_logs_management)
            tt45_logs = strategy_tt45.execute(tt45_watchlist, bot_config_tt45)
            all_logs.extend(tt45_logs)

        except Exception as e:
            logger.error(f"Credit Spread dry-run failed: {e}", exc_info=True)
            all_logs.append(f"❌ Credit Spread Strategy Failed: {e}")

        # --- Credit Spread management ---
        all_logs.append("\n--- Checking Open Credit Spreads (Closing Logic) ---")
        try:
            closing_logs = strategy_cs.manage_positions(simulation_mode=True)
            if closing_logs:
                all_logs.extend(closing_logs)
            else:
                all_logs.append("No open positions to manage or no actions needed.")
        except Exception as e:
            logger.error(f"Credit Spread closing-logic dry-run failed: {e}", exc_info=True)
            all_logs.append(f"❌ Closing Logic Check Failed: {e}")

        # --- Wheel ---
        all_logs.append("\n--- Wheel Strategy ---")
        try:
            strategy_wheel = WheelStrategy(tradier_service, db, dry_run=True)
            w_logs = strategy_wheel.execute(wheel_watchlist, bot_config)
            all_logs.extend(w_logs)
        except Exception as e:
            logger.error(f"Wheel dry-run failed: {e}", exc_info=True)
            all_logs.append(f"❌ Wheel Strategy Failed: {e}")

        return {'status': 'success', 'logs': all_logs}
