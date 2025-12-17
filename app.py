from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv
from services.tradier_service import TradierService
from datetime import datetime

load_dotenv()

app = Flask(__name__)
tradier_service = None

def get_tradier_service():
    global tradier_service
    if not tradier_service:
        tradier_service = TradierService()
    return tradier_service

@app.route('/')
def index():
    return render_template('index.html')

from services.backtest_service import BacktestService

@app.route('/api/status')
def status():
    service = get_tradier_service()
    connected = service.check_connection()
    return jsonify({
        "status": "healthy",
        "tradier_connected": connected
    })

@app.route('/api/backtest', methods=['POST'])
def run_backtest():
    data = request.json
    print(f"DEBUG: Received backtest request: {data}")
    symbol = data.get('symbol')
    strategy = data.get('strategy')
    start_date = data.get('start_date')
    # Default end date to today if not provided
    end_date = data.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    tradier = get_tradier_service()
    backtester = BacktestService(tradier)
    
    # Map frontend strategy names to backend types if needed
    try:
        result = backtester.run_backtest(symbol, strategy, start_date, end_date)
        # Ensure result is JSON serializable (handle NaNs just in case)
        # Simple/naive NaN check not efficient for large data but safe for this MVP
        import json
        import math
        # We assume result is dict.
        print(f"DEBUG: Backtest complete. Metrics: {result.get('metrics')}")
    except Exception as e:
        print(f"DEBUG: Exception during backtest execution: {e}")
        return jsonify({"error": str(e)}), 500
    
    if "error" in result:
        return jsonify(result), 400
        
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True, port=8080)
