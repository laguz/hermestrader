from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from services.container import Container

trading_bp = Blueprint('trading', __name__)

@trading_bp.route('/manual_orders')
@login_required
def manual_orders():
    """Render the manual orders page."""
    return render_template('manual_orders.html')

@trading_bp.route('/automated_trading')
@login_required
def automated_trading():
    """Render the automated trading page."""
    return render_template('automated_trading.html')

@trading_bp.route('/api/orders', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
def start_bot():
    try:
        service = Container.get_bot_service()
        result = service.start_bot()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/stop', methods=['POST'])
@login_required
def stop_bot():
    try:
        service = Container.get_bot_service()
        result = service.stop_bot()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/trading_mode', methods=['GET', 'POST'])
@login_required
def trading_mode():
    """Get or set the current trading mode (paper or live)."""
    try:
        service = Container.get_bot_service()

        if request.method == 'GET':
            mode = service.get_trading_mode()
            return jsonify({'mode': mode})

        # POST
        data = request.json or {}
        mode = data.get('mode')
        if mode not in ('paper', 'live'):
            return jsonify({'error': 'mode must be "paper" or "live"'}), 400

        result = service.set_trading_mode(mode)
        if 'error' in result:
            return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/watchlist', methods=['POST'])
@login_required
def update_watchlist():
    try:
        data = request.json
        watchlist = data.get('watchlist')
        list_type = data.get('type', 'credit_spreads_7')
        
        if watchlist is None:
             return jsonify({'error': 'watchlist is required'}), 400
             
        service = Container.get_bot_service()
        updated_list = service.update_watchlist(watchlist, list_type)
        if updated_list is not None:
            return jsonify({"message": f"Watchlist ({list_type}) updated", "watchlist": updated_list})
        else:
            return jsonify({'error': 'Failed to update watchlist'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/settings', methods=['POST'])
@login_required
def update_bot_settings():
    try:
        data = request.json
        if not data:
            return jsonify({'error': 'settings data required'}), 400
            
        service = Container.get_bot_service()
        success = service.update_settings(data)
        if success:
            return jsonify({"message": "Settings updated"})
        else:
            return jsonify({'error': 'Failed to update settings (invalid keys or db error)'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/dry_run', methods=['POST'])
@login_required
def run_dry_run():
    try:
        service = Container.get_bot_service()
        data = request.json or {}
        result = service.run_dry_run(data)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/bot_performance', strict_slashes=False)
@login_required
def bot_performance():
    """Render the bot performance page."""
    return render_template('bot_performance.html')

@trading_bp.route('/api/bot/performance', methods=['GET'])
@login_required
def get_bot_performance():
    try:
        service = Container.get_bot_service()
        stats = service.get_performance_summary()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/trades', methods=['GET'])
@login_required
def get_bot_trades():
    try:
        service = Container.get_bot_service()
        trades = service.get_trades()
        return jsonify({'trades': trades})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/orphans', methods=['GET'])
@login_required
def get_bot_orphans():
    try:
        service = Container.get_bot_service()
        orphans = service.get_unmanaged_orphans()
        return jsonify({'orphans': orphans})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/orphans/close', methods=['POST'])
@login_required
def close_bot_orphan():
    try:
        data = request.json or {}
        symbol = data.get('symbol')
        quantity = data.get('quantity')
        if not symbol: return jsonify({'error': 'symbol required'}), 400
        if quantity is None: return jsonify({'error': 'quantity required'}), 400
        service = Container.get_bot_service()
        result = service.close_unmanaged_orphan(symbol, quantity)
        if 'error' in result:
             return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/api/bot/sync_positions', methods=['POST'])
@login_required
def sync_positions():
    try:
        service = Container.get_bot_service()
        count = service.sync_open_positions()
        return jsonify({'status': 'success', 'count': count, 'message': f"Synced {count} positions."})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/pnl')
@login_required
def pnl_page():
    return render_template('pnl.html')

@trading_bp.route('/api/pnl', methods=['GET'])
@login_required
def get_pnl_data():
    try:
        service = Container.get_bot_service()
        data = service.get_open_positions_pnl()
        return jsonify({'pnl_data': data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
