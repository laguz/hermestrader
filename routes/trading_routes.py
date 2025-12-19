from flask import Blueprint, render_template, request, jsonify
from services.container import Container

trading_bp = Blueprint('trading', __name__)

@trading_bp.route('/manual_orders')
def manual_orders():
    """Render the manual orders page."""
    return render_template('manual_orders.html')

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
        
        # Helper to get account ID (should ideally be from context/user but using env/service default for single user app)
        account_id = tradier_service.account_id
        
        if not symbol or not side or not quantity or not order_type:
             return jsonify({'error': 'Missing required fields'}), 400

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
            order_class=order_class
        )
        
        if 'error' in result:
             return jsonify(result), 400
             
        return jsonify(result)

    except Exception as e:
        print(f"Order Placement Error: {e}")
        return jsonify({'error': str(e)}), 500
