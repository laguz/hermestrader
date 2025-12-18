from flask import Blueprint, render_template, jsonify, request
from services.container import Container
from services.analysis_service import AnalysisService

analysis_bp = Blueprint('analysis', __name__)

@analysis_bp.route('/entry-point', methods=['GET'])
def entry_point_page():
    return render_template('entry_point.html')

@analysis_bp.route('/api/analysis/<symbol>', methods=['GET'])
def analyze_symbol(symbol):
    print(f"Received analysis request for: {symbol}")
    try:
        tradier = Container.get_tradier_service()
        ml_service = Container.get_ml_service()
        service = AnalysisService(tradier, ml_service)
        result = service.analyze_symbol(symbol)
        
        if "error" in result:
             return jsonify(result), 404
             
        return jsonify(result)
    except Exception as e:
        print(f"Analysis Error: {e}")
        return jsonify({"error": str(e)}), 500
