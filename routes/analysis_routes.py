from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required
from services.container import Container
from services.analysis_service import AnalysisService

analysis_bp = Blueprint('analysis', __name__)

@analysis_bp.route('/entry-point', methods=['GET'])
@login_required
def entry_point_page():
    return render_template('entry_point.html')

@analysis_bp.route('/api/analysis/<symbol>', methods=['GET'])
@login_required
def analyze_symbol(symbol):
    print(f"Received analysis request for: {symbol}")
    period = request.args.get('period', '1y')
    try:
        tradier = Container.get_tradier_service()
        ml_service = Container.get_ml_service()
        db = Container.get_db()
        service = AnalysisService(tradier, ml_service, db)
        result = service.analyze_symbol(symbol, period=period)
        
        if "error" in result:
             return jsonify(result), 404
             
        return jsonify(result)
    except Exception as e:
        print(f"Analysis Error: {e}")
        return jsonify({"error": str(e)}), 500
