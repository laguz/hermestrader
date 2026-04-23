from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user
from services.container import Container
from services.auth_service import UserRegistration

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
            return redirect(url_for('main.dashboard'))
        else:
            flash('Invalid username or password.', 'error')
            
    return render_template('login.html')

@auth_bp.route('/login/nostr', methods=['POST'])
def login_nostr():
    data = request.json
    event = data.get('event')
    
    if not event or not isinstance(event, dict):
        return {'success': False, 'message': 'Missing or invalid event data'}, 400
        
    pubkey = event.get('pubkey')
    if not pubkey or not isinstance(pubkey, str):
        return {'success': False, 'message': 'Invalid pubkey format'}, 400
        
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
    auth_service = Container.get_auth_service()
    
    if auth_service.db is not None:
        user_count = auth_service.db['users'].count_documents({})
        if user_count > 0:
            flash('Registration is disabled. Only one account may exist.', 'error')
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
        live_key = request.form.get('live_tradier_key')
        live_account_id = request.form.get('live_account_id')
        reg = UserRegistration(
            username=username,
            password=password,
            tradier_key=key,
            account_id=account_id,
            live_tradier_key=live_key,
            live_account_id=live_account_id
        )
        user = auth_service.create_user(reg)
        
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
    
    if not event or not isinstance(event, dict) or not tradier_key or not account_id:
        return {'success': False, 'message': 'Missing data'}, 400
        
    pubkey = event.get('pubkey')
    if not pubkey or not isinstance(pubkey, str):
        return {'success': False, 'message': 'Invalid event pubkey'}, 400
        
    # Verify NIP-98/Auth event signature
    from nostr_sdk import Event
    try:
        import json
        event_obj = Event.from_json(json.dumps(event))
        if not event_obj.verify():
            return {'success': False, 'message': 'Invalid signature'}, 401
    except Exception as e:
        return {'success': False, 'message': f'Signature verification failed: {str(e)}'}, 401
        
    auth_service = Container.get_auth_service()
    
    if auth_service.db is not None:
        user_count = auth_service.db['users'].count_documents({})
        if user_count > 0:
             return {'success': False, 'message': 'Registration is disabled.'}, 403
    
    live_key = data.get('live_tradier_key')
    live_account_id = data.get('live_account_id')
    reg = UserRegistration(
        username=username,
        nostr_pubkey=pubkey,
        tradier_key=tradier_key,
        account_id=account_id,
        live_tradier_key=live_key,
        live_account_id=live_account_id
    )
    user = auth_service.create_user_with_nostr(reg)
    
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

@auth_bp.route('/settings')
@login_required
def settings():
    auth_service = Container.get_auth_service()
    endpoints = auth_service.get_endpoints(current_user.id)
    relays = auth_service.get_nostr_relays(current_user.id)
    return render_template('settings.html', endpoints=endpoints, relays=relays)

@auth_bp.route('/config')
@login_required
def config_page():
    auth_service = Container.get_auth_service()
    endpoints = auth_service.get_endpoints(current_user.id)
    relays = auth_service.get_nostr_relays(current_user.id)
    
    # Fetch decrypted credentials from session
    creds = {
        "paper_key": auth_service.get_api_key(mode='paper'),
        "paper_account": auth_service.get_account_id(mode='paper'),
        "live_key": auth_service.get_api_key(mode='live'),
        "live_account": auth_service.get_account_id(mode='live')
    }
    
    return render_template('config.html', endpoints=endpoints, relays=relays, creds=creds)

@auth_bp.route('/api/auth/update_credentials', methods=['POST'])
@login_required
def update_credentials():
    data = request.json
    password = data.get('password')
    paper_key = data.get('paper_key')
    paper_account = data.get('paper_account')
    live_key = data.get('live_key')
    live_account = data.get('live_account')
    paper_endpoint = data.get('paper_endpoint', 'https://sandbox.tradier.com/v1')
    live_endpoint = data.get('live_endpoint', 'https://api.tradier.com/v1')

    if not paper_key or not paper_account:
        return {'success': False, 'message': 'Paper Key and Paper Account are required'}, 400

    auth_service = Container.get_auth_service()
    success = auth_service.update_vault_credentials(
        current_user.id, password,
        paper_key, paper_account,
        live_key, live_account,
        paper_endpoint, live_endpoint
    )

    if success:
        return {'success': True}
    else:
        return {'success': False, 'message': 'Vault is locked or session expired. Please log out and log in again.'}, 401

@auth_bp.route('/api/auth/test_connection', methods=['POST'])
@login_required
def test_connection():
    data = request.json
    key = data.get('key')
    account_id = data.get('account_id')
    endpoint = data.get('endpoint')
    
    if not key or not account_id or not endpoint:
        return {'success': False, 'message': 'Missing required fields'}, 400
        
    auth_service = Container.get_auth_service()
    is_valid = auth_service.test_credentials(key, account_id, endpoint)
    
    if is_valid:
        return {'success': True, 'message': 'Connection successful!'}
    else:
        return {'success': False, 'message': 'Connection failed. Please check your credentials and endpoint.'}

@auth_bp.route('/api/auth/relays', methods=['GET', 'POST'])
@login_required
def manage_relays():
    auth_service = Container.get_auth_service()
    if request.method == 'GET':
        relays = auth_service.get_nostr_relays(current_user.id)
        return {'relays': relays}
    
    # POST
    data = request.json
    relays = data.get('relays')
    if not isinstance(relays, list):
        return {'success': False, 'message': 'Invalid relays format'}, 400
        
    success = auth_service.update_nostr_relays(current_user.id, relays)
    return {'success': success}
