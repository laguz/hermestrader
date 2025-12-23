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
        from bot.strategies.wheel import WheelStrategy
        
        # Determine watchlists
        data = request.json or {}
        
        # 1. Credit Spreads Watchlist
        cs_watchlist = data.get('credit_spreads_watchlist')
        if not cs_watchlist:
             # Backward compatibility: 'watchlist' field
             cs_watchlist = data.get('watchlist')
             
        if not cs_watchlist:
             # Fetch from DB
             bot_config = db.bot_config.find_one({"_id": "main_bot"})
             if bot_config:
                 settings = bot_config.get('settings', {})
                 cs_watchlist = settings.get('watchlist_credit_spreads', [])

        if not cs_watchlist:
            cs_watchlist = ['SPY', 'QQQ', 'IWM', 'TSLA', 'AAPL', 'NVDA', 'AMZN', 'GOOGL', 'MSFT', 'DIA']

        # 2. Wheel Watchlist
        wheel_watchlist = data.get('wheel_watchlist')
        if not wheel_watchlist:
             bot_config = db.bot_config.find_one({"_id": "main_bot"})
             if bot_config:
                 settings = bot_config.get('settings', {})
                 wheel_watchlist = settings.get('watchlist_wheel', [])
                 
        if not wheel_watchlist:
            wheel_watchlist = ['SPY', 'IWM', 'QQQ', 'DIA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
            
        print(f"DEBUG: CS Watchlist ({len(cs_watchlist)}): {cs_watchlist}")
        print(f"DEBUG: Wheel Watchlist ({len(wheel_watchlist)}): {wheel_watchlist}")

        all_logs = []

        # Execute Credit Spreads
        all_logs.append("--- Credit Spread Strategy ---")
        try:
            strategy_cs = CreditSpreadStrategy(tradier_service, db, dry_run=True)
            cs_logs = strategy_cs.execute(cs_watchlist)
            all_logs.extend(cs_logs)
        except Exception as e:
            error_msg = f"❌ Credit Spread Strategy Failed: {str(e)}"
            all_logs.append(error_msg)
            print(error_msg)
            traceback.print_exc()

        # Execute Wheel
        all_logs.append("\n--- Wheel Strategy ---")
        try:
            strategy_wheel = WheelStrategy(tradier_service, db, dry_run=True)
            w_logs = strategy_wheel.execute(wheel_watchlist)
            all_logs.extend(w_logs)
        except Exception as e:
            error_msg = f"❌ Wheel Strategy Failed: {str(e)}"
            all_logs.append(error_msg)
            print(error_msg)
            traceback.print_exc()
        
        return jsonify({'status': 'success', 'logs': all_logs})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
@trading_bp.route('/bot_performance', strict_slashes=False)
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

@trading_bp.route('/api/bot/sync_positions', methods=['POST'])
def sync_positions():
    try:
        service = Container.get_bot_service()
        count = service.sync_open_positions()
        return jsonify({'status': 'success', 'count': count, 'message': f"Synced {count} positions."})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/pnl')
def pnl_page():
    return render_template('pnl.html')

@trading_bp.route('/api/pnl', methods=['GET'])
def get_pnl_data():
    try:
        service = Container.get_bot_service()
        data = service.get_open_positions_pnl()
        return jsonify({'pnl_data': data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
