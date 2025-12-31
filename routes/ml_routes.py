from flask import Blueprint, jsonify, request
from flask_login import login_required
from services.container import Container
from services.ml_service import MLService
from exceptions import ValidationError

ml_bp = Blueprint('ml', __name__)

@ml_bp.route('/api/train', methods=['POST'])
@login_required
def train_model():
    data = request.get_json()
    if not data:
        raise ValidationError("Invalid JSON or missing body")
        
    symbol = data.get('symbol', 'TSLA')
    model_type = data.get('model_type', 'rf')
    
    tradier = Container.get_tradier_service()
    ml_service = MLService(tradier)

    result = ml_service.train_model(symbol, model_type=model_type)
    return jsonify(result)

@ml_bp.route('/api/predict', methods=['POST'])
@login_required
def predict_price():
    data = request.get_json()
    if not data:
        raise ValidationError("Invalid JSON or missing body")

    symbol = data.get('symbol', 'TSLA')
    model_type = data.get('model_type', 'rf')
    
    tradier = Container.get_tradier_service()
    ml_service = MLService(tradier)

    result = ml_service.predict_next_day(symbol, model_type=model_type)
    return jsonify(result)

@ml_bp.route('/api/evaluate', methods=['POST'])
@login_required
def evaluate_model():
    data = request.get_json()
    if not data:
        raise ValidationError("Invalid JSON or missing body")

    symbol = data.get('symbol', 'TSLA')
    model_type = data.get('model_type', 'rf')
    
    tradier = Container.get_tradier_service()
    ml_service = MLService(tradier)

    result = ml_service.evaluate_model(symbol, model_type=model_type)
    return jsonify(result)

@ml_bp.route('/api/history', defaults={'symbol': None}, methods=['GET'])
@ml_bp.route('/api/history/<symbol>', methods=['GET'])
@login_required
def get_prediction_history(symbol):
    tradier = Container.get_tradier_service()
    ml_service = MLService(tradier)

    days = request.args.get('days', 3)
    history = ml_service.get_prediction_history(symbol, days=days)
    return jsonify(history)

@ml_bp.route('/api/history/refresh', methods=['POST'])
@login_required
def refresh_history():
    tradier = Container.get_tradier_service()
    ml_service = MLService(tradier)
    result = ml_service.refresh_prediction_actuals()
    return jsonify(result)
