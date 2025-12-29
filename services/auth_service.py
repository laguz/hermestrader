import os
import base64
from datetime import datetime
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from services.container import Container

class User(UserMixin):
    def __init__(self, user_doc):
        self.id = str(user_doc['_id'])
        self.username = user_doc['username']
        self.password_hash = user_doc['password_hash']
        self.vault = user_doc.get('vault', {})

    def get_id(self):
        return self.id

class AuthService:
    def __init__(self):
        self.db = Container.get_db()
        self._unlocked_tradier_key = None # In-memory storage for the session
        
    def load_user(self, user_id):
        if self.db is None: return None
        from bson.objectid import ObjectId
        try:
            doc = self.db['users'].find_one({"_id": ObjectId(user_id)})
            return User(doc) if doc else None
        except:
            return None

    def create_user(self, username, password, tradier_key, account_id):
        """
        Create a new user and encrypt the Tradier Key and Account ID into their vault.
        Returns: User object or None if exists.
        """
        if self.db is None: return None
        
        if self.db['users'].find_one({"username": username}):
            return None

        # 1. Hash Password for Auth
        pw_hash = generate_password_hash(password)
        
        # 2. Derive Key for Vault Encryption
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=480000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        f = Fernet(key)
        
        # 3. Encrypt the Secrets
        encrypted_key = f.encrypt(tradier_key.encode()).decode('utf-8')
        encrypted_account_id = f.encrypt(account_id.encode()).decode('utf-8')
        
        user_doc = {
            "username": username,
            "password_hash": pw_hash,
            "vault": {
                "tradier_api_key": encrypted_key,
                "tradier_account_id": encrypted_account_id,
                "salt": base64.b64encode(salt).decode('utf-8')
            },
            "created_at": datetime.now()
        }
        
        res = self.db['users'].insert_one(user_doc)
        user_doc['_id'] = res.inserted_id
        
        # Auto-unlock
        self._unlocked_tradier_key = tradier_key
        # We also need to update Account ID context immediately
        ts = Container.get_tradier_service()
        ts.update_access_token(tradier_key)
        ts.update_account_id(account_id)
        
        return User(user_doc)

    def login(self, username, password):
        """
        Verify credentials AND attempt to unlock vault.
        """
        if self.db is None: return None
        
        user_doc = self.db['users'].find_one({"username": username})
        if not user_doc: return None
        
        if not check_password_hash(user_doc['password_hash'], password):
            return None
            
        # Attempt to unlock vault
        try:
            vault = user_doc.get('vault', {})
            enc_key = vault.get('tradier_api_key')
            salt_b64 = vault.get('salt')
            
            if enc_key and salt_b64:
                salt = base64.b64decode(salt_b64)
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=32,
                    salt=salt,
                    iterations=480000,
                )
                key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
                f = Fernet(key)
                
                decrypted_key = f.decrypt(enc_key.encode()).decode('utf-8')
                self._unlocked_tradier_key = decrypted_key
                
                ts = Container.get_tradier_service()
                ts.update_access_token(self._unlocked_tradier_key)
                
                # Decrypt Account ID if present
                enc_acc_id = vault.get('tradier_account_id')
                if enc_acc_id:
                    decrypted_acc_id = f.decrypt(enc_acc_id.encode()).decode('utf-8')
                    ts.update_account_id(decrypted_acc_id)
                
        except Exception as e:
            print(f"Vault Unlock Failed: {e}")
            # We might still allow login, but Bot won't work?
            # Or fail login? strict: fail login.
            return None
            
        return User(user_doc)
        
    def get_api_key(self):
        return self._unlocked_tradier_key
