from flask import Blueprint, render_template
from flask_login import login_required

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
@login_required
def dashboard():
    return render_template('dashboard.html')

@main_bp.route('/backtester')
@login_required
def backtester():
    return render_template('index.html')

@main_bp.route('/ai')
@login_required
def ai_prediction():
    return render_template('ai_prediction.html')

@main_bp.route('/evaluation')
@login_required
def evaluation():
    return render_template('evaluation.html')
