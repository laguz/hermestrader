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
