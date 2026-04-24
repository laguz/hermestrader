import logging
import hashlib
import traceback
from datetime import datetime

logger = logging.getLogger(__name__)

class PortfolioManager:
    """
    Handles tracking and synchronizing of open/closed positions 
    with the Tradier API and the local MongoDB database.
    """
    
    def __init__(self, tradier, db):
        self.tradier = tradier
        self.db = db

    def sync_open_positions(self, log_func=logger.info):
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
            
            # 2. Fetch DB 'OPEN' Positions
            db_open = list(self.db['open_positions'].find({"status": "OPEN"}))
            
            return self._sync_state(t_positions, db_open, log_func)
        except Exception as e:
            log_func(f"Error syncing positions: {e}")
            traceback.print_exc()
            return 0

    def _sync_state(self, t_positions, db_open, log_func):
        """
        Internal method to synchronize state between fetched positions and database.
        """
        now = datetime.now()

        # 3. Process Tradier Positions (Upsert OPEN)
        synced_count = self._upsert_open_positions(t_positions, now)

        # 4. Detect Closures (In DB OPEN but not in Tradier)
        t_ids = set(str(p.get('id')) for p in t_positions if 'id' in p)
        self._detect_and_handle_closures(db_open, t_ids, now, log_func)

        # 5. Backfill History (Gain/Loss)
        # Fetch last 100 closed positions to ensure we catch anything missed or pre-existing
        self._backfill_history(log_func)

        return synced_count

    def _upsert_open_positions(self, t_positions, now):
        """
        Helper method to upsert OPEN positions from Tradier to the DB.
        """
        synced_count = 0
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
        return synced_count

    def _detect_and_handle_closures(self, db_open, t_ids, now, log_func):
        """
        Helper method to detect positions that are OPEN in the DB but missing from Tradier,
        marking them as CLOSED and fetching gain/loss details.
        """
        # Keys in DB matching current Tradier set are kept OPEN (already updated above).
        # Keys NOT in Tradier set are CLOSED.
        for db_p in db_open:
            db_id = str(db_p.get('_id'))
            if db_id not in t_ids:
                # MARK AS CLOSED
                log_func(f"Position Closed: {db_p.get('symbol')} ({db_id})")

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
                    log_func(f"Found GL match for {symbol}: P&L {realized_pnl}")
                else:
                     log_func(f"Warning: No GL match found for {symbol}. using defaults.")

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

    def _backfill_history(self, log_func=logger.info):
        """
        Fetch historical closed positions from Gain/Loss and populate DB if missing.
        """
        try:
            # Fetch recent history
            history = self.tradier.get_gainloss(limit=50) 
            if isinstance(history, dict) and 'error' in history:
                log_func(f"Error backfilling history: {history['error']}")
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
                raw_id = f"{sym}_{o_date}_{c_date}_{qty}"
                doc_id = hashlib.sha256(raw_id.encode()).hexdigest()
                
                # Check if exists
                if self.db['open_positions'].find_one({"_id": doc_id}):
                    continue
                
                # Also check if we have a "CLOSED" position that looks like this but has a different ID
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
                entry_p = 0
                exit_p = 0
                
                abs_qty = abs(qty) if qty != 0 else 1
                
                if qty < 0:
                    # Short Position
                    entry_p = proceeds / (abs_qty * multiplier)
                    exit_p = cost / (abs_qty * multiplier)
                else:
                    # Long Position
                    entry_p = cost / (abs_qty * multiplier)
                    exit_p = proceeds / (abs_qty * multiplier)

                doc = {
                    "_id": doc_id,
                    "symbol": sym,
                    "quantity": qty,
                    "status": "CLOSED",
                    "date_acquired": o_date,
                    "exit_date": c_date,
                    "cost_basis": cost if qty > 0 else proceeds, # For P&L calc, usually we want entry cost
                    "entry_price": entry_p,
                    "exit_price": exit_p,
                    "realized_pnl": item.get('gain_loss'),
                    "last_updated": datetime.now(),
                    "type": "Option" if is_option else "Stock"
                }
                
                # Adjust cost_basis field for Short consistency if needed
                if qty < 0:
                     doc['cost_basis'] = -proceeds # Negative for credit?
                else:
                     doc['cost_basis'] = cost

                self.db['open_positions'].insert_one(doc)
                count += 1
                
            if count > 0:
                log_func(f"Backfilled {count} historical positions.")

        except Exception as e:
            log_func(f"Backfill error: {e}")
            traceback.print_exc()

    def get_open_positions_pnl(self, log_func=logger.info):
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
                    log_func(f"Error fetching quotes for P&L: {e}")
            
            for p in open_pos:
                sym = p['symbol']
                qty = p['quantity']
                cost_basis = p.get('cost_basis')
                if cost_basis is None: cost_basis = 0.0
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
            cost_basis = p.get('cost_basis')
            if cost_basis is None: cost_basis = 0.0
            realized_pnl = p.get('realized_pnl')
            if realized_pnl is None: realized_pnl = 0.0
            
            pnl_pct = 0.0
            if cost_basis != 0:
                divider = abs(cost_basis)
                if divider != 0:
                    pnl_pct = (realized_pnl / divider) * 100
                    
            closed_results.append({
                "symbol": sym,
                "quantity": qty,
                "type": p.get('type', 'Option'),
                "entry_price": p.get('entry_price', 0),
                "exit_price": p.get('exit_price', 0),
                "cost_basis": cost_basis,
                "pnl": realized_pnl,
                "pnl_pct": pnl_pct,
                "date_acquired": p.get('date_acquired'),
                "exit_date": p.get('exit_date')
            })

        def safe_date_sort(x):
            d = x.get('exit_date')
            if isinstance(d, datetime):
                return d.isoformat()
            return str(d or '')

        return {
            'open': open_results,
            'closed': sorted(closed_results, key=safe_date_sort, reverse=True)
        }
