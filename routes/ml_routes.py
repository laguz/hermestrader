from flask import Blueprint, jsonify, request
from services.container import Container
from services.ml_service import MLService

ml_bp = Blueprint('ml', __name__)

@ml_bp.route('/api/train', methods=['POST'])
def train_model():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON or missing body"}), 400
            
        symbol = data.get('symbol', 'TSLA')
        model_type = data.get('model_type', 'rf')
        
        tradier = Container.get_tradier_service()
        ml_service = MLService(tradier)
    
        result = ml_service.train_model(symbol, model_type=model_type)
        if "error" in result:
             return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        print(f"Train Error: {e}")
        return jsonify({"error": str(e)}), 500

@ml_bp.route('/api/predict', methods=['POST'])
def predict_price():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON or missing body"}), 400

        symbol = data.get('symbol', 'TSLA')
        model_type = data.get('model_type', 'rf')
        
        tradier = Container.get_tradier_service()
        ml_service = MLService(tradier)
    
        result = ml_service.predict_next_day(symbol, model_type=model_type)
        if "error" in result:
             return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        print(f"Predict Error: {e}")
        return jsonify({"error": str(e)}), 500

@ml_bp.route('/api/evaluate', methods=['POST'])
def evaluate_model():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON or missing body"}), 400

        symbol = data.get('symbol', 'TSLA')
        model_type = data.get('model_type', 'rf')
        
        tradier = Container.get_tradier_service()
        ml_service = MLService(tradier)
    
        result = ml_service.evaluate_model(symbol, model_type=model_type)
        if "error" in result:
             return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        print(f"Evaluate Error: {e}")
        return jsonify({"error": str(e)}), 500

@ml_bp.route('/api/history', defaults={'symbol': None}, methods=['GET'])
@ml_bp.route('/api/history/<symbol>', methods=['GET'])
def get_prediction_history(symbol):
    try:
        tradier = Container.get_tradier_service()
        ml_service = MLService(tradier)
    
        history = ml_service.get_prediction_history(symbol)
        return jsonify(history)
    except Exception as e:
        print(f"History Error: {e}")
        return jsonify({"error": str(e)}), 500
@ml_bp.route('/api/history/refresh', methods=['POST'])
def refresh_history():
    try:
        tradier = Container.get_tradier_service()
        ml_service = MLService(tradier)
        result = ml_service.refresh_prediction_actuals()
        return jsonify(result)
    except Exception as e:
        print(f"Refresh Error: {e}")
        return jsonify({"error": str(e)}), 500
