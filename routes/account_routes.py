from flask import Blueprint, jsonify
from flask_login import login_required
from services.container import Container

account_bp = Blueprint('account', __name__)

@account_bp.route('/api/account', methods=['GET'])
@login_required
def get_account_overview():
    try:
        tradier = Container.get_tradier_service()
        if not tradier.access_token:
            return jsonify({
                'success': False, 
                'message': 'Tradier Vault is locked.',
                'vault_locked': True
            }), 401
            
        balances = tradier.get_account_balances()
        if not balances:
            return jsonify({"error": "Failed to fetch account balances"}), 500
        return jsonify(balances)
    except Exception as e:
        print(f"Account API Error: {e}")
        return jsonify({"error": str(e)}), 500
