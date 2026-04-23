import os
from dotenv import load_dotenv
load_dotenv()

import logging
from flask import Flask, jsonify, request
from flask_login import LoginManager
from werkzeug.exceptions import HTTPException
from services.container import Container
from exceptions import AppError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def create_app():
    """Application factory pattern."""
    app = Flask(__name__)
    
    secret_key = os.getenv('FLASK_SECRET_KEY')
    if not secret_key:
        raise RuntimeError("FLASK_SECRET_KEY is required but missing from environment variables.")
        
    app.config['SECRET_KEY'] = secret_key
    app.config['SESSION_COOKIE_SAMESITE'] = 'Strict'
    app.config['SESSION_COOKIE_SECURE'] = True # Strongly recommended if behind HTTPS

    _init_login_manager(app)
    _register_blueprints(app)
    _register_error_handlers(app)

    @app.before_request
    def require_login():
        from flask_login import current_user
        if not current_user.is_authenticated:
            # Allow static files and auth routes
            if request.endpoint and request.endpoint != 'static' and not request.endpoint.startswith('auth.'):
                return app.login_manager.unauthorized()

    return app


def _init_login_manager(app):
    """Initialize Flask-Login."""
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'

    @login_manager.user_loader
    def load_user(user_id):
        return Container.get_auth_service().load_user(user_id)


def _register_blueprints(app):
    """Register all route blueprints."""
    from routes.main_routes import main_bp
    from routes.positions_routes import positions_bp
    from routes.backtest_routes import backtest_bp
    from routes.ml_routes import ml_bp
    from routes.account_routes import account_bp
    from routes.trading_routes import trading_bp
    from routes.auth_routes import auth_bp
    from routes.market_routes import market_bp
    from routes.analysis_routes import analysis_bp
    from routes.rule1_routes import rule1_bp

    for bp in [auth_bp, main_bp, positions_bp, backtest_bp, ml_bp,
               account_bp, trading_bp, market_bp, analysis_bp, rule1_bp]:
        app.register_blueprint(bp)


def _register_error_handlers(app):
    """Register global error handlers."""
    @app.errorhandler(AppError)
    def handle_app_error(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    @app.errorhandler(Exception)
    def handle_generic_error(error):
        if isinstance(error, HTTPException):
            return error
        logger.error(f"Unhandled Exception at {request.url}: {error}", exc_info=True)
        return jsonify({
            "error": "An internal error occurred",
            "message": f"Server Error: {str(error)}"
        }), 500


app = create_app()

if __name__ == '__main__':
    is_dev = os.getenv('FLASK_ENV') == 'development'
    app.run(debug=is_dev, host='0.0.0.0', port=8080)
