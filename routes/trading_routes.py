from flask import Blueprint, render_template, request, jsonify
from services.container import Container

trading_bp = Blueprint('trading', __name__)

@trading_bp.route('/manual_orders')
def manual_orders():
    """Render the manual orders page."""
    return render_template('manual_orders.html')

@trading_bp.route('/automated_trading')
def automated_trading():
    """Render the automated trading page."""
    return render_template('automated_trading.html')

@trading_bp.route('/api/orders', methods=['POST'])
def place_order():
    """Place a manual order using Tradier Service."""
    try:
        data = request.json
        tradier_service = Container.get_tradier_service()
        
        # Extract params
        symbol = data.get('symbol')
        side = data.get('side')
        quantity = data.get('quantity')
        order_type = data.get('type')
        duration = data.get('duration', 'day')
        price = data.get('price')
        stop = data.get('stop')
        option_symbol = data.get('option_symbol')
        order_class = data.get('class', 'equity')
        legs = data.get('legs')
        
        # Helper to get account ID (should ideally be from context/user but using env/service default for single user app)
        account_id = tradier_service.account_id
        
        # Validation
        if not symbol or not order_type:
             return jsonify({'error': 'Missing required fields'}), 400
             
        if order_class == 'multileg':
            if not legs or not isinstance(legs, list):
                return jsonify({'error': 'Legs required for multileg order'}), 400
        else:
            if not side or not quantity:
                return jsonify({'error': 'Side and Quantity required for equity/option orders'}), 400

        result = tradier_service.place_order(
            account_id=account_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=order_type,
            duration=duration,
            price=price,
            stop=stop,
            option_symbol=option_symbol,
            order_class=order_class,
            legs=legs
        )
        
        if 'error' in result:
             return jsonify(result), 400
             
        return jsonify(result)

    except Exception as e:
        print(f"Order Placement Error: {e}")
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/options/expirations', methods=['GET'])
def get_expirations():
    """Get option expirations for a symbol."""
    symbol = request.args.get('symbol')
    if not symbol:
        return jsonify({'error': 'Symbol required'}), 400
        
    try:
        tradier_service = Container.get_tradier_service()
        expirations = tradier_service.get_option_expirations(symbol)
        return jsonify({'expirations': expirations})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/options/chain', methods=['GET'])
def get_option_chain():
    """Get option chain for a symbol and expiration."""
    symbol = request.args.get('symbol')
    expiration = request.args.get('expiration')
    
    if not symbol or not expiration:
        return jsonify({'error': 'Symbol and expiration required'}), 400
        
    try:
        tradier_service = Container.get_tradier_service()
        chain = tradier_service.get_option_chains(symbol, expiration)
        # Tradier returns a list of dicts. We can just return it.
        # Each item has: symbol (OCC), strike, type, last, bid, ask, etc.
        return jsonify({'chain': chain})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/quotes', methods=['GET'])
def get_quote():
    """Get quote for a symbol."""
    symbol = request.args.get('symbol')
    if not symbol:
         return jsonify({'error': 'Symbol required'}), 400
         
    try:
        tradier_service = Container.get_tradier_service()
        quote = tradier_service.get_quote(symbol)
        return jsonify({'quote': quote})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ---------------------------------------------------------------------
# Bot Control Routes
# ---------------------------------------------------------------------

@trading_bp.route('/api/bot/status', methods=['GET'])
def bot_status():
    try:
        service = Container.get_bot_service()
        status = service.get_status()
        # Ensure _id is str if present
        if '_id' in status: status['_id'] = str(status['_id'])
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/start', methods=['POST'])
def start_bot():
    try:
        service = Container.get_bot_service()
        result = service.start_bot()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/stop', methods=['POST'])
def stop_bot():
    try:
        service = Container.get_bot_service()
        result = service.stop_bot()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/watchlist', methods=['POST'])
def update_watchlist():
    try:
        data = request.json
        watchlist = data.get('watchlist')
        list_type = data.get('type', 'credit_spreads')
        
        if watchlist is None:
             return jsonify({'error': 'watchlist is required'}), 400
             
        service = Container.get_bot_service()
        success = service.update_watchlist(watchlist, list_type)
        if success:
            return jsonify({"message": f"Watchlist ({list_type}) updated", "watchlist": watchlist})
        else:
            return jsonify({'error': 'Failed to update watchlist'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/dry_run', methods=['POST'])
def run_dry_run():
    try:
        tradier_service = Container.get_tradier_service()
        db = Container.get_db()
        from bot.strategies.credit_spreads import CreditSpreadStrategy
        
        # Determine watchlist
        # Try to get from request or DB, else default
        data = request.json or {}
        watchlist = data.get('watchlist')
        
        if not watchlist:
             # Fetch from DB if available
             bot_config = db.bot_config.find_one({"_id": "main_bot"})
             if bot_config and 'watchlist' in bot_config:
                 # Check nested structure or flat
                 # Current UI saves to settings.watchlist_credit_spreads
                 settings = bot_config.get('settings', {})
                 watchlist = settings.get('watchlist_credit_spreads', [])
                 
        if not watchlist:
            watchlist = ['SPY', 'QQQ', 'IWM', 'TSLA', 'AAPL', 'NVDA']

        strategy = CreditSpreadStrategy(tradier_service, db, dry_run=True)
        logs = strategy.execute(watchlist)
        
        return jsonify({'status': 'success', 'logs': logs})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
@trading_bp.route('/bot_performance')
def bot_performance():
    """Render the bot performance page."""
    return render_template('bot_performance.html')

@trading_bp.route('/api/bot/performance', methods=['GET'])
def get_bot_performance():
    try:
        service = Container.get_bot_service()
        stats = service.get_performance_summary()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/trades', methods=['GET'])
def get_bot_trades():
    try:
        service = Container.get_bot_service()
        trades = service.get_trades()
        return jsonify({'trades': trades})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
