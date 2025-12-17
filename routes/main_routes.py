from flask import Blueprint, render_template

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def dashboard():
    return render_template('dashboard.html')

@main_bp.route('/backtester')
def backtester():
    return render_template('index.html')

@main_bp.route('/ai')
def ai_prediction():
    return render_template('ai_prediction.html')

@main_bp.route('/evaluation')
def evaluation():
    return render_template('evaluation.html')
