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

@auth_bp.route('/login/nostr', methods=['POST'])
def login_nostr():
    data = request.json
    event = data.get('event')
    
    if not event:
        return {'success': False, 'message': 'Missing event data'}, 400
        
    auth_service = Container.get_auth_service()
    user, nostr_manager = auth_service.login_with_nostr(event)
    
    if user:
        login_user(user)
        return {
            'success': True, 
            'vault_locked': True if nostr_manager else False,
            'vault_metadata': nostr_manager
        }
    else:
        return {'success': False, 'message': 'Nostr login failed or user not found'}, 401

@auth_bp.route('/api/auth/unlock', methods=['POST'])
@login_required
def unlock_vault():
    data = request.json
    decrypted_dek = data.get('dek')
    
    if not decrypted_dek:
        return {'success': False, 'message': 'Missing decrypted DEK'}, 400
        
    auth_service = Container.get_auth_service()
    success = auth_service.unlock_vault_with_dek(current_user, decrypted_dek)
    
    if success:
        return {'success': True}
    else:
        return {'success': False, 'message': 'Vault unlock failed (Invalid DEK)'}, 400

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    # Only allow registration if no users exist (Personal App Mode)
    # OR if authenticated admin allows it (not implemented).
    # For now: Check DB count.
    

    
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

@auth_bp.route('/register/nostr', methods=['POST'])
def register_nostr():
    data = request.json
    event = data.get('event')
    username = data.get('username')
    tradier_key = data.get('tradier_key')
    account_id = data.get('account_id')
    
    if not event or not tradier_key or not account_id:
        return {'success': False, 'message': 'Missing data'}, 400
        
    pubkey = event.get('pubkey')
    if not pubkey:
        return {'success': False, 'message': 'Invalid event'}, 400
        
    auth_service = Container.get_auth_service()
    
    # We should verify sig here ideally.
    
    user = auth_service.create_user_with_nostr(username, pubkey, tradier_key, account_id)
    
    if user:
        login_user(user)
        return {'success': True}
    else:
        return {'success': False, 'message': 'Registration failed (User might exist)'}, 400

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))
