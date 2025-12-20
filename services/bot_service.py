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
        self._init_db_config()
        self.initialized = True
        
        # Resurrect state on app start? 
        # If DB says active, we should theoretically auto-start. 
        # But for safety, we default to stopped on restart unless explicit.

    def _init_db_config(self):
        """Ensure initial bot configuration exists in DB."""
        if self.db is not None:
             status = self.db['bot_config'].find_one({"_id": "main_bot"})
             if not status:
                 self.db['bot_config'].insert_one({
                     "_id": "main_bot",
                     "status": "STOPPED", # STOPPED, RUNNING, ERROR
                     "last_heartbeat": None,
                     "logs": [],
                     "settings": {
                         "watchlist": ["SPY", "QQQ", "TSLA"],
                         "max_drawdown": 500,
                         "max_position_size": 1000
                     }
                 })

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
        if not self._thread or not self._thread.is_alive():
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
        
        # Lazy load ML Service here
        # (Assuming container handles it)
        # from services.ml_service import MLService
        # self.ml_service = MLService(self.tradier) 
        
        while not self._stop_event.is_set():
            try:
                # 1. Heartbeat
                self._update_status("RUNNING")
                
                # 2. Market Hours Check (Simulated for now)
                # if not self._is_market_open():
                #     self._log("Market closed. Sleeping...")
                #     time.sleep(60)
                #     continue
                
                # 3. Dummy Strategy Logic
                config = self.get_status().get('settings', {})
                watchlist = config.get('watchlist', [])
                
                self._log(f"Scanning watchlist: {watchlist}...")
                
                # TODO: Implement real analysis here
                # for symbol in watchlist:
                #     prediction = self.ml_service.predict_next_day(symbol)
                #     self._log(f"Analyzed {symbol}: {prediction}")
                
                # Sleep cycle (e.g. 10 seconds for demo, 5 mins for real)
                for _ in range(10): 
                    if self._stop_event.is_set(): break
                    time.sleep(1)
                    
            except Exception as e:
                traceback.print_exc()
                self._log(f"Error in loop: {e}")
                time.sleep(10) # Prevent tight loop on error
        
        self._update_status("STOPPED")
        self._log("Bot loop ended.")
