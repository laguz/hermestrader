from flask import Blueprint, jsonify
from services.container import Container

market_bp = Blueprint('market', __name__)

@market_bp.route('/api/market/status', methods=['GET'])
def market_status():
    """Get current market status (open/closed) and time."""
    try:
        tradier_service = Container.get_tradier_service()
        clock = tradier_service.get_clock()
        
        if clock:
            return jsonify({'clock': clock})
        else:
            return jsonify({'error': 'Failed to fetch market clock'}), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500
