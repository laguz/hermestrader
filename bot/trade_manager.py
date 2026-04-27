import traceback
import threading
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

@dataclass
class TradeAction:
    strategy_id: str
    symbol: str
    order_class: str
    legs: List[Dict[str, Any]]
    price: Optional[float]
    side: str
    quantity: int = 1
    tag: Optional[str] = None
    strategy_params: Optional[Dict[str, Any]] = None

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
                
    def _get_tracked_symbols(self):
        """
        Build a set of all option/equity symbols currently tracked by bot strategies.
        Used to diff against live Tradier positions to find orphans.
        """
        db_tracked_legs = set()
        if self.db is None:
            return db_tracked_legs

        active_db_trades = list(self.db['active_trades'].find({"status": "OPEN", "strategy": {"$ne": "MANUAL_ORPHAN"}}))
        legacy_db_trades = list(self.db['auto_trades'].find({"status": "OPEN"}))
        active_db_trades.extend(legacy_db_trades)

        for trade in active_db_trades:
            if 'short_leg' in trade: db_tracked_legs.add(trade['short_leg'])
            if 'long_leg' in trade: db_tracked_legs.add(trade['long_leg'])

            if 'legs_info' in trade and isinstance(trade['legs_info'], list):
                for l in trade['legs_info']:
                    if 'option_symbol' in l:
                        db_tracked_legs.add(l['option_symbol'])

            if 'strategy' in trade and 'wheel' in trade['strategy'].lower():
                if 'symbol' in trade and sum(c.isdigit() for c in trade['symbol']) < 6:
                    db_tracked_legs.add(trade['symbol'])

        return db_tracked_legs

    def get_unmanaged_orphans(self):
        """
        Compute orphans LIVE by diffing Tradier positions against tracked DB trades.
        No database persistence — orphans disappear as soon as positions are closed on Tradier.
        """
        try:
            t_positions = self.tradier.get_positions()
            if not t_positions:
                return []

            db_tracked = self._get_tracked_symbols()

            orphans = []
            for p in t_positions:
                p_symbol = p.get('symbol')
                if p_symbol and p_symbol not in db_tracked:
                    orphans.append({
                        'symbol': p_symbol,
                        'quantity': p.get('quantity', 0),
                        'cost_basis': p.get('cost_basis', 0),
                        'date_acquired': p.get('date_acquired', ''),
                    })
            return orphans
        except Exception as e:
            logger.error(f"Error computing live orphans: {e}")
            return []

    def execute_strategy_order(self, action: TradeAction):
        """
        Wraps tradier.place_order to strictly enforce tracking metadata isolation. 
        Logs execution to `active_trades` directly (replacing legacy tracking logs).
        """
        if not action.tag:
            action.tag = action.strategy_id

        # 1. Determine order type and extract option symbol if needed
        order_type = 'market'
        option_symbol = None

        if not action.price:
            order_type = 'market'
        elif action.order_class in ['equity', 'option']:
            order_type = 'limit'
            if action.order_class == 'option' and action.legs:
                option_symbol = action.legs[0].get('option_symbol')
        else:
            # For multileg, use credit/debit based on side
            order_type = 'credit' if 'sell' in action.side.lower() else 'debit'

        # 2. Place order on Tradier natively
        response = self.tradier.place_order(
            account_id=self.tradier.account_id,
            symbol=action.symbol,
            side=action.side,
            quantity=action.quantity,
            order_type=order_type,
            duration='day',
            price=action.price,
            option_symbol=option_symbol,
            order_class=action.order_class,
            legs=action.legs,
            tag=action.tag
        )
        
        # 2. Persist the state log safely
        if 'error' not in response and self.db is not None:
            with self.lock:
                try:
                    trade_record = {
                        "symbol": action.symbol,
                        "strategy": action.strategy_id,
                        "status": "OPEN",
                        "price": action.price,
                        "quantity": action.quantity,
                        "order_id": response.get('id'),
                        "timestamp": datetime.now(),
                        "strategy_params": action.strategy_params or {},
                        # In multileg strategies, legs is a list of dictionaries normally. 
                        # Legacy compatibility usually wrote short_leg and long_leg explicitly.
                        "legs_info": action.legs
                    }
                    # Map legacy quick-keys if passed via strategy_params
                    if action.strategy_params:
                        if 'short_leg' in action.strategy_params: trade_record['short_leg'] = action.strategy_params['short_leg']
                        if 'long_leg' in action.strategy_params: trade_record['long_leg'] = action.strategy_params['long_leg']
                        
                    self.db['active_trades'].insert_one(trade_record)
                except Exception as e:
                    logger.error(f"Failed logging trade to active_trades for {action.strategy_id}: {e}")
                    
        return response
        
    def reconcile_orphans(self, log_func=logger.info):
        """
        Log-only reconciliation: reports untracked positions without writing to DB.
        Orphans are computed live via get_unmanaged_orphans().
        """
        log_func("🔄 Reconciling active portfolio positions against strategy DB...")
        
        try:
            orphans = self.get_unmanaged_orphans()
            if orphans:
                for o in orphans:
                    log_func(f"⚠️ Untracked position detected: {o['symbol']} (qty: {o['quantity']})")
                log_func(f"Found {len(orphans)} untracked position(s). View them in the Orphans panel.")
            else:
                log_func("✅ All positions are tracked by strategies.")
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
