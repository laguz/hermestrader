import traceback
import threading
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class TradeManager:
    """
    Centralized router between raw Tradier accounts and individual quantitative strategies.
    Ensures safe thread-locked DB reads, prevents strategy cross-contamination via tagging,
    and handles automated flagging of MANUAL_ORPHAN unmanaged positions.
    """
    
    def __init__(self, tradier, db):
        self.tradier = tradier
        self.db = db
        self.lock = threading.Lock()
        
    def register_strategy(self, strategy_id):
        """
        Creates or updates a metadata document inside the `strategies` MongoDB collection.
        Tracks active load and aggregate win/loss statistics.
        """
        if self.db is None: return
        with self.lock:
            try:
                self.db['strategies'].update_one(
                    {"_id": strategy_id},
                    {"$setOnInsert": {
                        "strategy_id": strategy_id,
                        "created_at": datetime.now(),
                        "wins": 0,
                        "losses": 0,
                        "status": "ACTIVE"
                    }},
                    upsert=True
                )
            except Exception as e:
                logger.error(f"Error registering strategy {strategy_id}: {e}")

    def get_my_trades(self, strategy_id, status="OPEN"):
        """
        Exclusive strategy filter. Only yields positions bearing the specific strategy_id.
        Blocks 'MANUAL_ORPHAN' states inherently.
        """
        if self.db is None: return []
        with self.lock:
            try:
                # We pull from both `auto_trades` (legacy compatibility mapping if active) and `active_trades` ideally.
                # The prompt dictated replacing auto_trades entirely, so we strictly query active_trades.
                return list(self.db['active_trades'].find({
                    "strategy": strategy_id,
                    "status": status
                }))
            except Exception as e:
                logger.error(f"Error fetching trades for {strategy_id}: {e}")
                return []
                
    def get_unmanaged_orphans(self):
        """
        Retrieves all live trades flagged as MANUAL_ORPHAN for UI display & manual fixing.
        """
        if self.db is None: return []
        with self.lock:
            try:
                return list(self.db['active_trades'].find({"strategy": "MANUAL_ORPHAN", "status": "OPEN"}))
            except Exception as e:
                logger.error(f"Error fetching orphans: {e}")
                return []

    def execute_strategy_order(self, strategy_id, symbol, order_class, legs, price, side, quantity=1, tag=None, strategy_params=None):
        """
        Wraps tradier.place_order to strictly enforce tracking metadata isolation. 
        Logs execution to `active_trades` directly (replacing legacy tracking logs).
        """
        if not tag: 
            tag = strategy_id

        # 1. Place order on Tradier natively
        response = self.tradier.place_order(
            account_id=self.tradier.account_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type='credit' if price and price > 0 else 'market' if not price else 'debit',
            duration='day',
            price=price,
            order_class=order_class,
            legs=legs,
            tag=tag
        )
        
        # 2. Persist the state log safely
        if 'error' not in response and self.db is not None:
            with self.lock:
                try:
                    trade_record = {
                        "symbol": symbol,
                        "strategy": strategy_id,
                        "status": "OPEN",
                        "price": price,
                        "quantity": quantity,
                        "order_id": response.get('id'),
                        "timestamp": datetime.now(),
                        "strategy_params": strategy_params or {},
                        # In multileg strategies, legs is a list of dictionaries normally. 
                        # Legacy compatibility usually wrote short_leg and long_leg explicitly.
                        "legs_info": legs
                    }
                    # Map legacy quick-keys if passed via strategy_params
                    if strategy_params:
                        if 'short_leg' in strategy_params: trade_record['short_leg'] = strategy_params['short_leg']
                        if 'long_leg' in strategy_params: trade_record['long_leg'] = strategy_params['long_leg']
                        
                    self.db['active_trades'].insert_one(trade_record)
                except Exception as e:
                    logger.error(f"Failed logging trade to active_trades for {strategy_id}: {e}")
                    
        return response
        
    def reconcile_orphans(self, log_func=logger.info):
        """
        Validates live Tradier positions against locked `active_trades`.
        Tags untracked elements purely as 'MANUAL_ORPHAN'.
        """
        if self.db is None: return
        log_func("🔄 Reconciling active portfolio positions against strategy DB...")
        
        with self.lock:
            try:
                t_positions = self.tradier.get_positions()
                if not t_positions: return
                
                # Fetch all active system trades tracked dynamically
                active_db_trades = list(self.db['active_trades'].find({"status": "OPEN"}))
                
                # Legacy mapping bridge for strategies not fully written over yet
                legacy_db_trades = list(self.db['auto_trades'].find({"status": "OPEN"}))
                active_db_trades.extend(legacy_db_trades)
                
                db_tracked_legs = set()
                for trade in active_db_trades:
                    # Strategy might define legacy "short_leg" "long_leg" directly
                    if 'short_leg' in trade: db_tracked_legs.add(trade['short_leg'])
                    if 'long_leg' in trade: db_tracked_legs.add(trade['long_leg'])
                    
                    # Or nested inside legs_info list
                    if 'legs_info' in trade and isinstance(trade['legs_info'], list):
                        for l in trade['legs_info']:
                            if 'option_symbol' in l:
                                db_tracked_legs.add(l['option_symbol'])
                                
                    # Naked equity entries directly as symbol
                    # If it's a wheel trade tracking a core equity, the trade 'symbol' itself is tracked 
                    if 'strategy' in trade and 'wheel' in trade['strategy'].lower():
                        if 'symbol' in trade and sum(c.isdigit() for c in trade['symbol']) < 6: # Not an option string heuristically
                             db_tracked_legs.add(trade['symbol'])

                orphan_count = 0
                for p in t_positions:
                    p_symbol = p.get('symbol') 
                    
                    if p_symbol not in db_tracked_legs:
                        # Evaluate if already marked as orphan
                        existing_orphan = self.db['active_trades'].find_one({
                            "strategy": "MANUAL_ORPHAN",
                            "status": "OPEN",
                            "symbol": p_symbol 
                        })
                        
                        if not existing_orphan:
                            self.db['active_trades'].insert_one({
                                "strategy": "MANUAL_ORPHAN",
                                "status": "OPEN",
                                "symbol": p_symbol,
                                "quantity": p.get('quantity', 0),
                                "cost_basis": p.get('cost_basis', 0),
                                "timestamp": datetime.now(),
                                "raw_tradier_data": p
                            })
                            orphan_count += 1
                            log_func(f"⚠️ MANUAL_ORPHAN detected & isolated: {p_symbol}")
                            
            except Exception as e:
                log_func(f"Error during Orphan Reconciliation: {e}")
                traceback.print_exc()

    def mark_trade_closed(self, trade_id, limit_price=None, response_id=None, collection='active_trades'):
         if self.db is None: return
         with self.lock:
             try:
                 self.db[collection].update_one(
                    {"_id": trade_id},
                    {"$set": {
                        "status": "CLOSED", 
                        "close_date": datetime.now(), 
                        "close_order_id": response_id,
                        "exit_price": limit_price
                    }}
                )
             except Exception as e:
                  logger.error(f"Error marking trade {trade_id} closed in {collection}: {e}")
