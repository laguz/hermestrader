from flask import Flask
import os
from dotenv import load_dotenv
from flask_login import LoginManager
from services.container import Container

# Import Blueprints
from routes.main_routes import main_bp
from routes.positions_routes import positions_bp
from routes.backtest_routes import backtest_bp
from routes.ml_routes import ml_bp
from routes.account_routes import account_bp
from routes.trading_routes import trading_bp
from routes.auth_routes import auth_bp


load_dotenv()
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY') or 'super_secret_key_change_me'

# Init Login Manager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'

@login_manager.user_loader
def load_user(user_id):
    return Container.get_auth_service().load_user(user_id)

# Register Blueprints
app.register_blueprint(auth_bp)
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

from routes.rule1_routes import rule1_bp
app.register_blueprint(rule1_bp)

from flask import jsonify
from exceptions import AppError

@app.errorhandler(AppError)
def handle_app_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

from werkzeug.exceptions import HTTPException
from flask import request

@app.errorhandler(Exception)
def handle_generic_error(error):
    # Pass through HTTP errors (like 404) to let Flask handle them normally
    if isinstance(error, HTTPException):
        return error

    app.logger.error(f"Unhandled Exception at {request.url}: {error}", exc_info=True)
    return jsonify({"error": "An internal error occurred"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
