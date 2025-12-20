from flask import Flask
from dotenv import load_dotenv

# Import Blueprints
from routes.main_routes import main_bp
from routes.positions_routes import positions_bp
from routes.backtest_routes import backtest_bp
from routes.ml_routes import ml_bp
from routes.account_routes import account_bp
from routes.trading_routes import trading_bp

load_dotenv()

app = Flask(__name__)

# Register Blueprints
app.register_blueprint(main_bp)
app.register_blueprint(positions_bp)
app.register_blueprint(backtest_bp)
app.register_blueprint(ml_bp)
app.register_blueprint(account_bp)
app.register_blueprint(trading_bp)

from routes.market_routes import market_bp
app.register_blueprint(market_bp)

from routes.analysis_routes import analysis_bp
app.register_blueprint(analysis_bp)

from flask import jsonify
from exceptions import AppError

@app.errorhandler(AppError)
def handle_app_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.errorhandler(Exception)
def handle_generic_error(error):
    app.logger.error(f"Unhandled Exception: {error}", exc_info=True)
    return jsonify({"error": "An internal error occurred"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=8080)
