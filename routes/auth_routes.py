from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user
from services.container import Container

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
        
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        auth_service = Container.get_auth_service()
        user = auth_service.login(username, password)
        
        if user:
            login_user(user)
            # Check if bot needs to be notified or started?
            # Ideally the bot service picks up the key from TradierService which was updated by AuthService.
            return redirect(url_for('main.dashboard'))
        else:
            flash('Login failed. Check username and password.', 'error')
            
    return render_template('login.html')

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    # Only allow registration if no users exist (Personal App Mode)
    # OR if authenticated admin allows it (not implemented).
    # For now: Check DB count.
    
    db = Container.get_db()
    if db is not None:
        if db['users'].count_documents({}) > 0:
            if not current_user.is_authenticated:
                flash("Registration disabled. Please Login.", "warning")
                return redirect(url_for('auth.login'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        key = request.form.get('tradier_key')
        account_id = request.form.get('account_id')
        
        if not username or not password or not key or not account_id:
            flash('All fields are required.', 'error')
            return redirect(url_for('auth.register'))
            
        auth_service = Container.get_auth_service()
        user = auth_service.create_user(username, password, key, account_id)
        
        if user:
            login_user(user)
            return redirect(url_for('main.dashboard'))
        else:
            flash('Username already exists.', 'error')
            
    return render_template('register.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))
