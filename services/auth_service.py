import logging
import os
logger = logging.getLogger(__name__)
import base64
from datetime import datetime
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from services.container import Container
from exceptions import AppError
from pymongo.errors import PyMongoError

# Ephemeral in-memory key generated strictly once per application boot.
_EPHEMERAL_KEY = Fernet.generate_key()
_F_EPHEMERAL = Fernet(_EPHEMERAL_KEY)

class User(UserMixin):
    def __init__(self, user_doc):
        self.id = str(user_doc.get('_id', ''))
        self.username = user_doc.get('username', '')
        self.password_hash = user_doc.get('password_hash', '')
        self.vault = user_doc.get('vault', {})
        self.nostr_pubkey = user_doc.get('nostr_pubkey', '')

    def get_id(self):
        return self.id

from nostr_sdk import Keys, PublicKey, SecretKey, nip04_encrypt, init_logger, LogLevel

class AuthService:
    def __init__(self):
        self.db = Container.get_db()
        # Removed _unlocked_tradier_key to prevent singleton data leakage
        
        # Initialize logger for nostr-sdk if needed, but only once
        try:
            init_logger(LogLevel.INFO)
        except:
            pass # Already initialized

    def load_user(self, user_id):
        if self.db is None: return None
        from bson.objectid import ObjectId
        try:
            doc = self.db['users'].find_one({"_id": ObjectId(user_id)})
            return User(doc) if doc else None
        except:
            return None

    def _derive_key(self, password, salt):
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=480000,
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode()))

    def create_user(self, username, password, tradier_key, account_id, live_tradier_key=None, live_account_id=None, paper_endpoint='https://sandbox.tradier.com/v1', live_endpoint='https://api.tradier.com/v1'):
        """Create a new user with Multi-Key Vault architecture (DEK)."""
        if self.db is None: return None
        
        from pymongo.errors import DuplicateKeyError
        try:
            self.db['system_locks'].insert_one({"_id": "single_user_lock", "claimed_by": username})
        except DuplicateKeyError:
            logger.warning("Registration locked natively: single user lock already exists.")
            return None
            
        if self.db['users'].find_one({"username": username}): return None

        # 1. Generate DEK (Data Encryption Key)
        dek = Fernet.generate_key()
        f_dek = Fernet(dek)

        # 2. Encrypt Secrets with DEK
        enc_tradier_key = f_dek.encrypt(tradier_key.encode()).decode('utf-8')
        enc_account_id = f_dek.encrypt(account_id.encode()).decode('utf-8')
        enc_live_key = f_dek.encrypt(live_tradier_key.encode()).decode('utf-8') if live_tradier_key else None
        enc_live_account = f_dek.encrypt(live_account_id.encode()).decode('utf-8') if live_account_id else None

        # 3. Encrypt DEK with Password
        salt = os.urandom(16)
        password_derived_key = self._derive_key(password, salt)
        f_pwd = Fernet(password_derived_key)
        enc_dek_by_password = f_pwd.encrypt(dek).decode('utf-8')

        user_doc = {
            "username": username,
            "password_hash": generate_password_hash(password, method='pbkdf2:sha256'),
            "vault": {
                "version": 2, # DEK architecture
                "encrypted_tradier_key": enc_tradier_key,
                "encrypted_account_id": enc_account_id,
                "dek_managers": {
                    "password": {
                        "salt": base64.b64encode(salt).decode('utf-8'),
                        "encrypted_dek": enc_dek_by_password
                    }
                }
            },
            "created_at": datetime.now()
        }

        res = self.db['users'].insert_one(user_doc)
        user_doc['_id'] = res.inserted_id
        
        self._unlock_session(tradier_key, account_id, live_tradier_key, live_account_id)
        return User(user_doc)

    def create_user_with_nostr(self, username, nostr_pubkey, tradier_key, account_id, live_tradier_key=None, live_account_id=None, paper_endpoint='https://sandbox.tradier.com/v1', live_endpoint='https://api.tradier.com/v1'):
        """Create a new user with Nostr DEK manager."""
        if self.db is None: return None
        
        from pymongo.errors import DuplicateKeyError
        try:
            self.db['system_locks'].insert_one({"_id": "single_user_lock", "claimed_by": username})
        except DuplicateKeyError:
            logger.warning("Registration locked natively: single user lock already exists.")
            return None
            
        if self.db['users'].find_one({"username": username}): return None
        if self.db['users'].find_one({"nostr_pubkey": nostr_pubkey}): return None

        # 1. Generate DEK
        dek = Fernet.generate_key()
        f_dek = Fernet(dek)

        # 2. Encrypt Secrets with DEK
        enc_tradier_key = f_dek.encrypt(tradier_key.encode()).decode('utf-8')
        enc_account_id = f_dek.encrypt(account_id.encode()).decode('utf-8')
        enc_live_key = f_dek.encrypt(live_tradier_key.encode()).decode('utf-8') if live_tradier_key else None
        enc_live_account = f_dek.encrypt(live_account_id.encode()).decode('utf-8') if live_account_id else None

        # 3. Encrypt DEK with Nostr (NIP-04)
        # Server generates ephemeral keys to encrypt TO the user
        server_keys = Keys.generate()
        server_priv = server_keys.secret_key()
        server_pub = server_keys.public_key()
        
        user_pub_obj = PublicKey.parse(nostr_pubkey)
        
        # Encrypt the DEK (bytes -> string -> encrypt)
        # Note: Fernet key 'dek' is bytes. Convert to base64 string first to ensure safe transmission/decryption
        dek_str = dek.decode('utf-8')
        encrypted_dek_blob = nip04_encrypt(server_priv, user_pub_obj, dek_str)
        
        user_doc = {
            "username": username,
            "password_hash": "", # No password
            "nostr_pubkey": nostr_pubkey,
            "vault": {
                "version": 2,
                "encrypted_tradier_key": enc_tradier_key,
                "encrypted_account_id": enc_account_id,
                "dek_managers": {
                    "nostr": {
                        "encrypted_dek": encrypted_dek_blob,
                        "sender_pubkey": server_pub.to_hex() # Client needs this to decrypt
                    }
                }
            },
            "created_at": datetime.now()
        }
        
        res = self.db['users'].insert_one(user_doc)
        user_doc['_id'] = res.inserted_id
        
        self._unlock_session(tradier_key, account_id, live_tradier_key, live_account_id)
        return User(user_doc)

    def login(self, username, password):
        """Login with Password and auto-migrate vault if needed."""
        if self.db is None: return None
        
        try:
            user_doc = self.db['users'].find_one({"username": username})
        except PyMongoError as e:
            logger.error(f"Database error during login: {e}")
            raise AppError("Database connection failed. Please ensure MongoDB is running.", status_code=503)

        if not user_doc: return None
        
        if not check_password_hash(user_doc.get('password_hash', ''), password):
            return None

        return self._attempt_vault_unlock(user_doc, password=password)

    def login_with_nostr(self, event):
        """
        Verify NIP-98/Auth event and return vault metadata if available.
        """
        if self.db is None: return None, None
        
        import json
        from nostr_sdk import Event

        try:
            event_obj = Event.from_json(json.dumps(event))
            if not event_obj.verify():
                logger.warning("Nostr login failed: Invalid signature")
                return None, None
        except Exception as e:
            logger.warning(f"Nostr login failed: Signature verification error: {str(e)}")
            return None, None

        pubkey = event.get('pubkey')
        
        try:
            user_doc = self.db['users'].find_one({"nostr_pubkey": pubkey})
        except PyMongoError as e:
            logger.error(f"Database error during Nostr login: {e}")
            raise AppError("Database connection failed. Please ensure MongoDB is running.", status_code=503)

        if not user_doc:
            return None, None 

        vault = user_doc.get('vault', {})
        nostr_manager = vault.get('dek_managers', {}).get('nostr', {})
        
        return User(user_doc), nostr_manager

    def unlock_vault_with_dek(self, user, decrypted_dek):
        """Unlock vault and initialize session with provided DEK."""
        if self.db is None: return False
        
        from bson.objectid import ObjectId
        user_doc = self.db['users'].find_one({"_id": ObjectId(user.id)})
        if not user_doc: return False
        
        # Attempt to unlock session
        success_user = self._attempt_vault_unlock(user_doc, decrypted_dek=decrypted_dek)
        return success_user is not None

    def _attempt_vault_unlock(self, user_doc, password=None, decrypted_dek=None):
        try:
            vault = user_doc.get('vault', {})
            version = vault.get('version', 1)
            
            tradier_key = None
            account_id = None
            live_tradier_key = None
            live_account_id = None
            
            if version == 1 and password:
                # --- LEGACY VAULT ---
                salt_b64 = vault.get('salt')
                enc_key = vault.get('tradier_api_key')
                enc_acc_id = vault.get('tradier_account_id')
                
                if salt_b64 and enc_key:
                    salt = base64.b64decode(salt_b64)
                    key = self._derive_key(password, salt)
                    f = Fernet(key)
                    tradier_key = f.decrypt(enc_key.encode()).decode('utf-8')
                    if enc_acc_id:
                        account_id = f.decrypt(enc_acc_id.encode()).decode('utf-8') 
                    self._migrate_to_dek(user_doc['_id'], password, tradier_key, account_id)
                    
            elif version == 2:
                # --- NEW VAULT (DEK) ---
                dek = None
                
                # If we have password, try password manager
                if password:
                    managers = vault.get('dek_managers', {})
                    pwd_manager = managers.get('password')
                    if pwd_manager:
                        salt_b64 = pwd_manager.get('salt')
                        enc_dek_blob = pwd_manager.get('encrypted_dek')
                        salt = base64.b64decode(salt_b64)
                        key = self._derive_key(password, salt)
                        f_pwd = Fernet(key)
                        dek = f_pwd.decrypt(enc_dek_blob.encode())

                # If we were given the DEK directly (e.g. from Nostr flow later)
                if decrypted_dek:
                    dek = decrypted_dek.encode('utf-8') if isinstance(decrypted_dek, str) else decrypted_dek
                
                if dek:
                    f_dek = Fernet(dek)
                    enc_key = vault.get('encrypted_tradier_key')
                    enc_acc = vault.get('encrypted_account_id')
                    
                    tradier_key = f_dek.decrypt(enc_key.encode()).decode('utf-8')
                    if enc_acc:
                        account_id = f_dek.decrypt(enc_acc.encode()).decode('utf-8')

            if tradier_key:
                self._unlock_session(tradier_key, account_id, live_tradier_key, live_account_id)
                return User(user_doc)
                
        except Exception as e:
            logger.error(f"Login/Unlock Error: {e}")
            return None
        
        # If we failed to unlock but password was correct (hash check passed before), 
        # we might still want to return User but in "Locked" state? 
        # For now, we return User (so they are logged in) but session key is None.
        return User(user_doc)

    def _unlock_session(self, key, account_id, live_key=None, live_account_id=None):
        from flask import session, has_request_context
        if has_request_context():
            enc_key = _F_EPHEMERAL.encrypt(key.encode()).decode('utf-8')
            session['tradier_key'] = enc_key
            if account_id:
                enc_acct = _F_EPHEMERAL.encrypt(account_id.encode()).decode('utf-8')
                session['account_id'] = enc_acct
            if live_key:
                session['live_tradier_key'] = _F_EPHEMERAL.encrypt(live_key.encode()).decode('utf-8')
            if live_account_id:
                session['live_account_id'] = _F_EPHEMERAL.encrypt(live_account_id.encode()).decode('utf-8')
        
        # We still update the tradier service, but the tradier service itself 
        # needs to be modified to pull from session instead of storing it on tracking instances.
        ts = Container.get_tradier_service()
        ts.update_access_token(key)
        if account_id: 
            ts.update_account_id(account_id)

    def _migrate_to_dek(self, user_id, password, tradier_key, account_id):
        """Migrate legacy vault to DEK vault."""
        logger.info(f"Migrating user {user_id} to Vault V2...")
        
        # Generate DEK
        dek = Fernet.generate_key()
        f_dek = Fernet(dek)
        
        # Encrypt Payload with DEK
        enc_tradier_key = f_dek.encrypt(tradier_key.encode()).decode('utf-8')
        enc_account_id = f_dek.encrypt(account_id.encode()).decode('utf-8') if account_id else None
        
        # Encrypt DEK with Password
        salt = os.urandom(16)
        pwd_key = self._derive_key(password, salt)
        f_pwd = Fernet(pwd_key)
        enc_dek = f_pwd.encrypt(dek).decode('utf-8')
        
        new_vault = {
            "version": 2,
            "encrypted_tradier_key": enc_tradier_key,
            "encrypted_account_id": enc_account_id,
            "dek_managers": {
                "password": {
                    "salt": base64.b64encode(salt).decode('utf-8'),
                    "encrypted_dek": enc_dek
                }
            }
        }
        
        self.db['users'].update_one({"_id": user_id}, {"$set": {"vault": new_vault}})
        logger.info("Migration complete.")

    def get_api_key(self, mode='paper'):
        from flask import session, has_request_context
        if has_request_context():
            key_name = 'tradier_key' if mode == 'paper' else 'live_tradier_key'
            enc_key = session.get(key_name)
            if enc_key:
                try:
                    return _F_EPHEMERAL.decrypt(enc_key.encode()).decode('utf-8')
                except Exception as e:
                    logger.error("Failed to decrypt ephemeral session key.")
                    return None
        return None

    def get_account_id(self, mode='paper'):
        from flask import session, has_request_context
        if has_request_context():
            key_name = 'account_id' if mode == 'paper' else 'live_account_id'
            enc_acct = session.get(key_name)
            if enc_acct:
                try:
                    return _F_EPHEMERAL.decrypt(enc_acct.encode()).decode('utf-8')
                except Exception as e:
                    logger.error("Failed to decrypt ephemeral account id.")
                    return None
        return None

    def update_vault_credentials(self, user_id, password, paper_key, paper_account, live_key, live_account, paper_endpoint, live_endpoint):
        if self.db is None: return False
        import base64
        from bson.objectid import ObjectId

        user_doc = self.db['users'].find_one({"_id": ObjectId(user_id)})
        if not user_doc: return False

        vault = user_doc.get('vault', {})
        managers = vault.get('dek_managers', {})
        pwd_manager = managers.get('password')

        dek = None
        if pwd_manager:
            salt_b64 = pwd_manager.get('salt')
            enc_dek_blob = pwd_manager.get('encrypted_dek')
            salt = base64.b64decode(salt_b64)
            key = self._derive_key(password, salt)
            f_pwd = Fernet(key)
            try:
                dek = f_pwd.decrypt(enc_dek_blob.encode())
            except Exception:
                return False # wrong password
        else:
            return False

        f_dek = Fernet(dek)
        enc_paper_key = f_dek.encrypt(paper_key.encode()).decode('utf-8')
        enc_paper_acc = f_dek.encrypt(paper_account.encode()).decode('utf-8') if paper_account else None
        enc_live_key = f_dek.encrypt(live_key.encode()).decode('utf-8') if live_key else None
        enc_live_acc = f_dek.encrypt(live_account.encode()).decode('utf-8') if live_account else None

        self.db['users'].update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {
                "vault.encrypted_tradier_key": enc_paper_key,
                "vault.encrypted_account_id": enc_paper_acc,
                "vault.encrypted_live_tradier_key": enc_live_key,
                "vault.encrypted_live_account_id": enc_live_acc,
                "endpoints": {
                    "paper": paper_endpoint,
                    "live": live_endpoint
                }
            }}
        )

        self._unlock_session(paper_key, paper_account, live_key, live_account)
        return True

    def get_endpoints(self, user_id):
        if self.db is None: return {"paper": "https://sandbox.tradier.com/v1", "live": "https://api.tradier.com/v1"}
        from bson.objectid import ObjectId
        user_doc = self.db['users'].find_one({"_id": ObjectId(user_id)})
        if user_doc and "endpoints" in user_doc:
            return user_doc["endpoints"]
        return {"paper": "https://sandbox.tradier.com/v1", "live": "https://api.tradier.com/v1"}

    def test_credentials(self, key, account_id, endpoint):
        """Test Tradier credentials by initializing a temporary service and checking connection."""
        from services.tradier_service import TradierService
        try:
            # We bypass the singleton and environment variables by passing explicit args
            test_service = TradierService(access_token=key, account_id=account_id, endpoint=endpoint)
            # The TradierService.check_connection() calls get_quote('SPY')
            return test_service.check_connection()
        except Exception as e:
            logger.error(f"Credential test failed: {e}")
            return False

    def get_nostr_relays(self, user_id):
        if self.db is None: return ['wss://relay.primal.net', 'wss://relay.damus.io', 'wss://nos.lol']
        from bson.objectid import ObjectId
        user_doc = self.db['users'].find_one({"_id": ObjectId(user_id)})
        return user_doc.get("nostr_relays", ['wss://relay.primal.net', 'wss://relay.damus.io', 'wss://nos.lol'])

    def update_nostr_relays(self, user_id, relays):
        if self.db is None: return False
        from bson.objectid import ObjectId
        self.db['users'].update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"nostr_relays": relays}}
        )
        return True
