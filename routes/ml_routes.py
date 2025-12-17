from flask import Blueprint, jsonify, request
from services.container import Container
from services.ml_service import MLService

ml_bp = Blueprint('ml', __name__)

@ml_bp.route('/api/train', methods=['POST'])
def train_model():
    data = request.json
    symbol = data.get('symbol', 'SPY')
    model_type = data.get('model_type', 'rf')
    
    tradier = Container.get_tradier_service()
    ml_service = MLService(tradier)
    
    try:
        result = ml_service.train_model(symbol, model_type=model_type)
        if "error" in result:
             return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        print(f"Train Error: {e}")
        return jsonify({"error": str(e)}), 500

@ml_bp.route('/api/predict', methods=['POST'])
def predict_price():
    data = request.json
    symbol = data.get('symbol', 'SPY')
    model_type = data.get('model_type', 'rf')
    
    tradier = Container.get_tradier_service()
    ml_service = MLService(tradier)
    
    try:
        result = ml_service.predict_next_day(symbol, model_type=model_type)
        if "error" in result:
             return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        print(f"Predict Error: {e}")
        return jsonify({"error": str(e)}), 500

@ml_bp.route('/api/evaluate', methods=['POST'])
def evaluate_model():
    data = request.json
    symbol = data.get('symbol', 'SPY')
    model_type = data.get('model_type', 'rf')
    
    tradier = Container.get_tradier_service()
    ml_service = MLService(tradier)
    
    try:
        result = ml_service.evaluate_model(symbol, model_type=model_type)
        if "error" in result:
             return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        print(f"Evaluate Error: {e}")
        return jsonify({"error": str(e)}), 500
