import time
import threading
import traceback
from datetime import datetime
from services.container import Container
from exceptions import AppError

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
        self.ml_service = None # Lazy load to avoid circular deps if any
        
        # Initialize Strategies
        from bot.strategies.credit_spreads import CreditSpreadStrategy
        from bot.strategies.wheel import WheelStrategy
        self.credit_spread_strategy = CreditSpreadStrategy(self.tradier, self.db)
        self.wheel_strategy = WheelStrategy(self.tradier, self.db)
        
        self._init_db_config()
        # Reset state on startup to avoid phantom running state
        self._update_status("STOPPED")
        self.initialized = True
        
        # Resurrect state on app start? 
        # If DB says active, we should theoretically auto-start. 
        # But for safety, we default to stopped on restart unless explicit.

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
                     "watchlist_wheel": [],         # Start empty, user adds via UI
                     "max_drawdown": 500,
                     "max_position_size": 1000
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
                 if 'watchlist_wheel' not in settings:
                     updates['settings.watchlist_wheel'] = []
                 
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
        
        # Upper case and dedup
        clean_list = list(set([str(s).upper().strip() for s in watchlist if s]))
        
        # Map frontend type to DB key
        db_key = f"settings.watchlist_{list_type}"
        # Safety check to only allow specific keys
        if list_type not in ['credit_spreads', 'wheel']:
            self._log(f"Error: Invalid watchlist type {list_type}")
            return False

        self.db['bot_config'].update_one(
            {"_id": "main_bot"},
            {"$set": {db_key: clean_list}}
        )
        self._log(f"Watchlist ({list_type}) updated: {clean_list}")
        return True

    # ... (logs, status methods unchanged) ...

    def get_trades(self, limit=50):
        """Fetch recent bot trades."""
        if self.db is None: return []
        cursor = self.db['auto_trades'].find().sort("entry_date", -1).limit(limit)
        
        trades = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            trades.append(doc)
        return trades

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
        print(f"[BOT] {message}")
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
        
        while not self._stop_event.is_set():
            try:
                # 1. Heartbeat - Only update if not stopping
                if self._stop_event.is_set(): break
                self._update_status("RUNNING")
                
                # 3. Strategy Execution
                config = self.get_status().get('settings', {})
                wl_spreads = config.get('watchlist_credit_spreads', [])
                wl_wheel = config.get('watchlist_wheel', [])
                
                # Dynamic Import/Reload or just use instance
                # We want to run them if we have symbols
                
                if wl_spreads:
                    self._log(f"Running Credit Spread Strategy on {len(wl_spreads)} symbols...")
                    self.credit_spread_strategy.execute(wl_spreads)
                    
                if wl_wheel:
                    self._log(f"Running Wheel Strategy on {len(wl_wheel)} symbols...")
                    self.wheel_strategy.execute(wl_wheel)
                    
                # Sleep cycle - responsive wait
                # Wait for 60 seconds (strategies shouldn't run too hot) or until stop event
                if self._stop_event.wait(timeout=60):
                    break
                    
            except Exception as e:
                traceback.print_exc()
                self._log(f"Error in loop: {e}")
                # Prevent tight loop on error (wait 10s)
                if self._stop_event.wait(timeout=10):
                    break
        
        self._update_status("STOPPED")
        self._log("Bot loop ended.")
