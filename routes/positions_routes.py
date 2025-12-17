from flask import Blueprint, jsonify
from services.container import Container

positions_bp = Blueprint('positions', __name__)

@positions_bp.route('/api/positions')
def get_positions():
    tradier = Container.get_tradier_service()
    positions = tradier.get_positions()
    
    # Enrichment: Fetch quotes for all symbols to get current price
    if positions:
        # Extract symbols. Handle list of dicts.
        symbols = [p.get('symbol') for p in positions if p.get('symbol')]
        if symbols:
            # Tradier allows comma separated symbols
            symbols_str = ",".join(symbols)
            quotes_data = tradier.get_quote(symbols_str)
            
            # Quotes might be a single dict (one quote) or list of dicts.
            # Convert to dict map for easy lookup
            quote_map = {}
            if isinstance(quotes_data, dict):
                quote_map[quotes_data.get('symbol')] = quotes_data
            elif isinstance(quotes_data, list):
                for q in quotes_data:
                    quote_map[q.get('symbol')] = q
            
            # Merge data
            for p in positions:
                sym = p.get('symbol')
                if sym in quote_map:
                    p['current_price'] = quote_map[sym].get('last')
                    
    return jsonify(positions)
