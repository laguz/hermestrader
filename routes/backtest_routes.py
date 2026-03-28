import logging
from flask import Blueprint, jsonify, request
from flask_login import login_required
from services.container import Container
from services.backtest_service import BacktestService
from datetime import datetime

logger = logging.getLogger(__name__)
backtest_bp = Blueprint('backtest', __name__)

@backtest_bp.route('/api/status')
@login_required
def status():
    service = Container.get_tradier_service()
    connected = service.check_connection()
    return jsonify({
        "status": "healthy",
        "tradier_connected": connected
    })

@backtest_bp.route('/api/backtest', methods=['POST'])
@login_required
def run_backtest():
    data = request.json
    logger.debug(f"Received backtest request: {data}")
    symbol = data.get('symbol')
    strategy = data.get('strategy')
    start_date = data.get('start_date')
    end_date = data.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    tradier = Container.get_tradier_service()
    backtester = BacktestService(tradier)
    
    try:
        result = backtester.run_backtest(symbol, strategy, start_date, end_date)
        logger.info(f"Backtest complete. Metrics: {result.get('metrics')}")
    except Exception as e:
        logger.error(f"Exception during backtest execution: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    
    if "error" in result:
        return jsonify(result), 400
        
    return jsonify(result)

