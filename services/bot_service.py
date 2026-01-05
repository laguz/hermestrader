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
                     "max_position_size": 1000,
                     "max_credit_spreads_per_symbol": 5,
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
                 if 'watchlist_wheel' not in settings:
                     updates['settings.watchlist_wheel'] = []
                 if 'max_wheel_contracts_per_symbol' not in settings:
                     updates['settings.max_wheel_contracts_per_symbol'] = 1
                 
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

    def update_settings(self, settings_update):
        """Generic method to update settings fields."""
        if self.db is None: return False
        if not isinstance(settings_update, dict): return False
        
        # Whitelist allowed keys to prevent overwriting critical internal state
        allowed_keys = [
            'max_drawdown', 
            'max_position_size', 
            'max_credit_spreads_per_symbol', 
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
                except:
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
        - Updates existing OPEN positions.
        - Inserts new OPEN positions.
        - Detects CLOSED positions (in DB but not in Tradier).
        - Fetches Gain/Loss for CLOSED positions.
        """
        if self.db is None: return 0
        try:
            # 1. Fetch Current Tradier Positions
            t_positions = self.tradier.get_positions()
            if t_positions is None: t_positions = []
            
            # Key Tradier positions by Symbol + ID (if available, else index?)
            # Tradier ID is unique per position leg.
            t_pos_map = {str(p['id']): p for p in t_positions if 'id' in p}
            # Fallback if ID missing? (Unlikely for Tradier, but robust check)
            
            # 2. Fetch DB 'OPEN' Positions
            db_open = list(self.db['open_positions'].find({"status": "OPEN"}))
            
            synced_count = 0
            now = datetime.now()
            
            # 3. Process Tradier Positions (Upsert OPEN)
            for p in t_positions:
                pid = str(p.get('id'))
                # Prepare document
                doc = p.copy()
                doc['status'] = "OPEN"
                doc['last_updated'] = now
                doc['_id'] = pid # Use Tradier ID as MongoDB _id for uniqueness
                
                # Upsert
                self.db['open_positions'].update_one(
                    {"_id": pid},
                    {"$set": doc},
                    upsert=True
                )
                synced_count += 1
                
            # 4. Detect Closures (In DB OPEN but not in Tradier)
            # Keys in DB matching current Tradier set are kept OPEN (already updated above).
            # Keys NOT in Tradier set are CLOSED.
            
            t_ids = set(str(p.get('id')) for p in t_positions if 'id' in p)
            
            for db_p in db_open:
                db_id = str(db_p.get('_id'))
                if db_id not in t_ids:
                    # MARK AS CLOSED
                    self._log(f"Position Closed: {db_p.get('symbol')} ({db_id})")
                    
                    # Fetch Gain/Loss to get Exit details
                    # We look for a recent closed position for this symbol/qty
                    # Since we don't have the exact close transaction ID easily mapping to position ID from GainLoss endpoint,
                    # We heuristic match: Symbol + Qty + Date ~= Now?
                    # GainLoss endpoint returns 'close_date'.
                    
                    symbol = db_p.get('symbol')
                    qty = db_p.get('quantity') # This is the position qty (e.g. -1 for short).
                    # Close Qty in GainLoss is positive? Or matching?
                    # Tradier GainLoss 'quantity' usually matches order size (positive).
                    
                    # Try to fetch recent gainloss for this symbol
                    gl_data = self.tradier.get_gainloss(limit=20, symbol=symbol)
                    
                    # Find match: 
                    # For a closed position, we expect an entry in gainloss with 'close_date' very recent.
                    # And 'open_date' matching our position 'date_acquired'.
                    
                    matched_gl = None
                    pos_acquired = db_p.get('date_acquired')
                    # Tradier date format: 2025-12-15T18:45:55.340Z
                    # GainLoss open_date: 2025-12-15T18:45:55.000Z (might vary slightly on millis)
                    
                    for gl in gl_data:
                        # Simple Heuristic: Match Open Date (as best as possible)
                        # Or just take the most recent for this symbol if confident?
                        # Let's try Open Date prefix match (YYYY-MM-DDTHH:MM)
                        gl_open = gl.get('open_date', '')
                        if pos_acquired and gl_open and pos_acquired[:16] == gl_open[:16]:
                            matched_gl = gl
                            break
                    
                    exit_price = 0
                    realized_pnl = 0
                    exit_date = now
                    
                    if matched_gl:
                        exit_price = matched_gl.get('close_price')
                        realized_pnl = matched_gl.get('gain_loss')
                        exit_date = matched_gl.get('close_date')
                        self._log(f"Found GL match for {symbol}: P&L {realized_pnl}")
                    else:
                         self._log(f"Warning: No GL match found for {symbol}. using defaults.")
                    
                    # Update DB
                    self.db['open_positions'].update_one(
                        {"_id": db_id},
                        {"$set": {
                            "status": "CLOSED",
                            "last_updated": now,
                            "exit_price": exit_price,
                            "realized_pnl": realized_pnl,
                            "exit_date": exit_date
                        }}
                    )

            # 5. Backfill History (Gain/Loss)
            # Fetch last 100 closed positions to ensure we catch anything missed or pre-existing
            self._backfill_history()

            return synced_count
        except Exception as e:
            self._log(f"Error syncing positions: {e}")
            traceback.print_exc()
            return 0

    def _backfill_history(self):
        """
        Fetch historical closed positions from Gain/Loss and populate DB if missing.
        """
        try:
            # Fetch recent history
            history = self.tradier.get_gainloss(limit=50) 
            if isinstance(history, dict) and 'error' in history:
                self._log(f"Error backfilling history: {history['error']}")
                return

            if not history: return

            count = 0
            for item in history:
                # Create a composite ID for deduplication
                # GainLoss doesn't have a stable ID, so we use: Symbol + OpenDate + CloseDate + Qty
                sym = item.get('symbol')
                o_date = item.get('open_date')
                c_date = item.get('close_date')
                qty = item.get('quantity')
                
                # Generate determinist ID
                # Simple string hash for now
                import hashlib
                raw_id = f"{sym}_{o_date}_{c_date}_{qty}"
                doc_id = hashlib.md5(raw_id.encode()).hexdigest()
                
                # Check if exists
                if self.db['open_positions'].find_one({"_id": doc_id}):
                    continue
                
                # Also check if we have a "CLOSED" position that looks like this but has a different ID (e.g. Tradier position ID)
                # If we synced it as open and then closed it, it has a Tradier Position ID.
                # We want to avoid duplicating it as a "Historical" entry.
                # Heuristic: Check if any CLOSED position matches Symbol + Exit Date + Qty?
                # Matching strictly on timestamp might be hard due to slight diffs.
                # But `item['close_date']` from GainLoss is usually the exact timestamp used.
                
                # If we implemented step 4 (Detect Closures) correctly, it updates 'exit_date' from gainloss.
                # So we can search for that.
                
                exists = self.db['open_positions'].find_one({
                    "symbol": sym,
                    "status": "CLOSED",
                    "exit_date": c_date
                })
                if exists: continue

                # Insert new historical record
                cost = item.get('cost', 0)
                proceeds = item.get('proceeds', 0)
                
                # Infer type
                is_option = any(c.isdigit() for c in sym)
                multiplier = 100 if is_option else 1
                
                # Calculate entry/exit prices approx
                # If Qty < 0 (Short?): Cost is Buy (Exit), Proceeds is Sell (Entry)
                # If Qty > 0 (Long): Cost is Buy (Entry), Proceeds is Sell (Exit)
                
                # Data from debug: quantity: -1.0, cost: 103.0, proceeds: 80.0
                # implied Short.
                
                entry_p = 0
                exit_p = 0
                
                abs_qty = abs(qty) if qty != 0 else 1
                
                if qty < 0:
                    # Short Position
                    # Entry was Sell (Proceeds)
                    entry_p = proceeds / (abs_qty * multiplier)
                    # Exit was Buy (Cost)
                    exit_p = cost / (abs_qty * multiplier)
                else:
                    # Long Position
                    # Entry was Buy (Cost)
                    entry_p = cost / (abs_qty * multiplier)
                    # Exit was Sell (Proceeds)
                    exit_p = proceeds / (abs_qty * multiplier)

                doc = {
                    "_id": doc_id,
                    "symbol": sym,
                    "quantity": qty,
                    "status": "CLOSED",
                    "date_acquired": o_date,
                    "exit_date": c_date,
                    "cost_basis": cost if qty > 0 else proceeds, # For P&L calc, usually we want entry cost
                    # Actually, our P&L table expects 'cost_basis' to mean 'Initial Value'.
                    # For Short, initial value is Proceeds (Credit).
                    # For Long, initial value is Cost (Debit).
                    
                    "entry_price": entry_p,
                    "exit_price": exit_p,
                    "realized_pnl": item.get('gain_loss'),
                    "last_updated": datetime.now(),
                    "type": "Option" if is_option else "Stock"
                }
                
                # Adjust cost_basis field for Short consistency if needed
                if qty < 0:
                     doc['cost_basis'] = -proceeds # Negative for credit?
                     # In existing logic: "divider = abs(cost_basis)"
                     # If we use negative for credit, it works.
                else:
                     doc['cost_basis'] = cost

                self.db['open_positions'].insert_one(doc)
                count += 1
                
            if count > 0:
                self._log(f"Backfilled {count} historical positions.")

        except Exception as e:
            self._log(f"Backfill error: {e}")
            traceback.print_exc()

    def get_open_positions_pnl(self):
        """
        Calculate P&L for tracked positions.
        Returns: { 'open': [...], 'closed': [...] }
        """
        if self.db is None: return {'open': [], 'closed': []}
        
        all_positions = list(self.db['open_positions'].find())
        if not all_positions: return {'open': [], 'closed': []}
        
        open_pos = [p for p in all_positions if p.get('status', 'OPEN') == 'OPEN']
        closed_pos = [p for p in all_positions if p.get('status') == 'CLOSED']
        
        # --- PROCESS OPEN POSITIONS (Real-time P&L) ---
        open_results = []
        if open_pos:
            symbols_to_fetch = list(set([p['symbol'] for p in open_pos]))
            symbols_str = ",".join(symbols_to_fetch)
            
            quotes_map = {}
            if symbols_str:
                try:
                    q_data = self.tradier.get_quote(symbols_str)
                    if isinstance(q_data, dict): q_list = [q_data]
                    elif isinstance(q_data, list): q_list = q_data
                    else: q_list = []
                    for q in q_list:
                        if 'symbol' in q: quotes_map[q['symbol']] = q
                except Exception as e:
                    self._log(f"Error fetching quotes for P&L: {e}")
            
            for p in open_pos:
                sym = p['symbol']
                qty = p['quantity']
                cost_basis = p.get('cost_basis', 0.0)
                quote = quotes_map.get(sym, {})
                current_price = quote.get('last') or quote.get('close') or 0.0
                close_price = quote.get('prevclose') or quote.get('close') or 0.0
                
                is_option = any(c.isdigit() for c in sym)
                multiplier = 100 if is_option else 1
                market_value = current_price * qty * multiplier
                pnl = market_value - cost_basis
                divider = abs(cost_basis) if cost_basis != 0 else 1.0
                pnl_pct = (pnl / divider) * 100
                
                open_results.append({
                    "symbol": sym,
                    "quantity": qty,
                    "type": "Option" if is_option else "Stock",
                    "entry_price": cost_basis / (qty * multiplier) if qty != 0 else 0,
                    "current_price": current_price,
                    "close_price": close_price,
                    "cost_basis": cost_basis,
                    "market_value": market_value,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                    "last_updated": p.get('last_updated')
                })

        # --- PROCESS CLOSED POSITIONS (Historical P&L) ---
        closed_results = []
        for p in closed_pos:
            sym = p['symbol']
            qty = p['quantity']
            is_option = any(c.isdigit() for c in sym)
            cost_basis = p.get('cost_basis', 0.0)
            realized_pnl = p.get('realized_pnl', 0.0)
            
            # P&L % for closed
            divider = abs(cost_basis) if cost_basis != 0 else 1.0
            pnl_pct = (realized_pnl / divider) * 100
            
            closed_results.append({
                "symbol": sym,
                "quantity": qty,
                "type": "Option" if is_option else "Stock",
                "entry_price": p.get('cost_basis', 0) / (qty * (100 if is_option else 1)) if qty != 0 else 0,
                "exit_price": p.get('exit_price', 0.0),
                "cost_basis": cost_basis,
                "pnl": realized_pnl,
                "pnl_pct": pnl_pct,
                "exit_date": p.get('exit_date')
            })

        return {'open': open_results, 'closed': closed_results}

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
                
                # 3. Global Circuit Breaker: Buying Power Check
                balances = self.tradier.get_account_balances()
                min_reserve = config.get('min_obp_reserve', 1000)
                
                if balances:
                    obp = balances.get('option_buying_power', 0)
                    if obp < min_reserve:
                        self._log(f"🛑 CIRCUIT BREAKER: Option Buying Power ${obp:,.2f} is below Minimum Reserve ${min_reserve:,.2f}. Skipping all trading activity.")
                        # Still manage positions? 
                        # Closing positions usually increases BP. 
                        # Let's allowed management (exits/rolls) but maybe skip entries?
                        # Actually management might involve rolls which require BP.
                        # For now, let's skip entries but allow management.
                        
                        self._log(f"Running Credit Spread MANAGEMENT (Exits) ONLY...")
                        self.credit_spread_strategy.manage_positions()
                        
                        # Wheel management often involves rolls (new entries), so it's riskier.
                        # But it also involves checking for ITM.
                        # Let's skip Wheel for now if BP is CRITICALLY low.
                        
                        if self._stop_event.wait(timeout=60):
                            break
                        continue
                else:
                    self._log("⚠️ Could not fetch balances. Skipping strategy cycle for safety.")
                    if self._stop_event.wait(timeout=60):
                        break
                    continue

                if wl_spreads:
                    self._log(f"Running Credit Spread Strategy on {len(wl_spreads)} symbols...")
                    # 1. Manage Existing
                    self.credit_spread_strategy.manage_positions()
                    # 2. Execute New
                    self.credit_spread_strategy.execute(wl_spreads, config)
                    
                if wl_wheel:
                    self._log(f"Running Wheel Strategy on {len(wl_wheel)} symbols...")
                    self.wheel_strategy.execute(wl_wheel, config)
                    
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
