import logging
import os

logger = logging.getLogger(__name__)
import base64
import hashlib
import time
import struct
from urllib.parse import parse_qs
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.exceptions import InvalidSignature
from services.container import Container

class SQRLService:
    def __init__(self):
        self.secret_key = os.environ.get('SECRET_KEY', 'dev_secret_key').encode()
        # In-memory store for nuts and sessions (use Redis in prod)
        self._nuts = {} # nut -> {ip: ..., created: ...}
        self._sessions = {} # nut -> {user_id: ..., status: ...}

    def generate_nut(self, ip_address):
        """
        Generate a secure, stateful nonce (Nut).
        Structure: <timestamp><hmac> to force statelessness? 
        Or just random + store for this MVP.
        Let's use Random + Store for simplicity and security against replay.
        """
        # 128-bit random nonce
        nonce = os.urandom(16)
        nut = base64.urlsafe_b64encode(nonce).decode('utf-8').rstrip('=')
        
        self._nuts[nut] = {
            'ip': ip_address,
            'created': time.time()
        }
        return nut

    def validate_nut(self, nut, ip_address):
        """Validate if nut exists and matches IP (optional constraint)."""
        if nut not in self._nuts:
            return False
            
        data = self._nuts[nut]
        # Basic expiration (e.g. 5 minutes)
        if time.time() - data['created'] > 300:
            del self._nuts[nut]
            return False
            
        # Optional: IP check (might be tricky if phone is on 4G and server on WiFi)
        # Check if same IP? SQRL standard discusses this. 
        # For local dev, we might relax it.
        return True

    def verify_signature(self, message, signature, public_key_b64):
        """Verify Ed25519 signature."""
        try:
            # Add padding back if needed
            pub_key_bytes = base64.urlsafe_b64decode(public_key_b64 + '==' * (-len(public_key_b64) % 4))
            sig_bytes = base64.urlsafe_b64decode(signature + '==' * (-len(signature) % 4))
            
            pub_key = Ed25519PublicKey.from_public_bytes(pub_key_bytes)
            pub_key.verify(sig_bytes, message)
            return True
        except (InvalidSignature, ValueError) as e:
            logger.error(f"Sig Verify Failed: {e}")
            return False

    def handle_request(self, client_param, server_param, ids_param, pds_param, urs_param):
        """
        Handle SQRL Client Request.
        params are the base64url encoded strings sent by client.
        """
        try:
            # 1. Decode Client Param
            client_decoded = base64.urlsafe_b64decode(client_param + '==' * (-len(client_param) % 4)).decode('utf-8')
            client_data = {}
            for line in client_decoded.split('\n'): # standard says \r\n
                if '=' in line:
                    k, v = line.split('=', 1)
                    client_data[k] = v
            
            cmd = client_data.get('cmd')
            idk = client_data.get('idk') # Identity Key
            
            # 2. Validation
            # Verify server param (nut) matches what we sent
            server_decoded = base64.urlsafe_b64decode(server_param + '==' * (-len(server_param) % 4)).decode('utf-8')
            # server string usually includes "sqrl://domain/path?nut=..."
            # We just need to extract nut.
            # Ideally verify the whole string matches what we signed/sent? 
            # For this MVP, let's extract nut from query string in server_decoded
            
            qs = parse_qs(server_decoded.split('?')[1])
            nut = qs.get('nut', [''])[0]
            
            if not self.validate_nut(nut, None): # IP check skipped for mobile->desktop dev
                return self._response_error(404) # Nut not found/expired

            # 3. Verify Signature (ids)
            # Message = client_param + server_param
            message = (client_param + server_param).encode()
            if not self.verify_signature(message, ids_param, idk):
                return self._response_error(400) # Bad Sig

            # 4. Handle Commands
            if cmd == 'query':
                return self._handle_query(nut, idk)
            elif cmd == 'ident':
                return self._handle_ident(nut, idk, client_data)
            else:
                return self._response_error(400) # Unknown cmd

        except Exception as e:
            logger.error(f"SQRL Handle Error: {e}")
            return self._response_error(500)

    def _handle_query(self, nut, idk):
        # Check if user exists with this IDK
        db = Container.get_db()
        user = db['users'].find_one({"sqrl_idk": idk})
        
        flags = "0" # Minimal
        
        # Responses codes (TIF): 
        # 1 = ID match found
        # 4 = IP matched (skip)
        tif = 0
        if user:
            tif |= 1 # Current ID Match
            
        # Reply
        # server param must be echoed/updated? 
        # For query, we confirm we see them. 
        # "suk" (server user key) needed? Only if we implement full protocol.
        
        return self._build_response(nut, tif)

    def _handle_ident(self, nut, idk, client_data):
        # Log the user in or register
        db = Container.get_db()
        user = db['users'].find_one({"sqrl_idk": idk})
        
        if user:
            # Login Existing
            self._sessions[nut] = {'user_id': str(user['_id']), 'status': 'authenticated'}
            tif = 1 # ID Match
        else:
            # Register New? 
            # For this app, maybe we auto-bind if user is logged in on desktop?
            # Or create new empty user?
            # Let's create new user if allowed, OR fail if we require invitation.
            # create stub user
            user_doc = {
                "username": f"SQRL_{idk[:8]}",
                "sqrl_idk": idk,
                "created_at": datetime.now(),
                "vault": {} 
            }
            res = db['users'].insert_one(user_doc)
            self._sessions[nut] = {'user_id': str(res.inserted_id), 'status': 'authenticated'}
            tif = 1 | 2 # ID Match | ID Created? (No, bit 1 is Previous ID match. Bit 2 is IP match?)
            # TIF: 0x01 = Current ID Match. 
            
        return self._build_response(nut, tif)

    def _build_response(self, nut, tif):
        # Construct Server Response
        # ver=1
        # nut=<new_nut> (rolling nut for next request)
        # tif=<hex>
        # qry=/sqrl?nut=...
        
        new_nut = self.generate_nut('0.0.0.0')
        # Link new nut to old session if needed?
        # For simple 1-request login (ident), we might not need chain.
        
        raw_response = f"ver=1\r\nnut={new_nut}\r\ntif={hex(tif)[2:].upper()}\r\nqry=/sqrl?nut={new_nut}\r\n"
        encoded = base64.urlsafe_b64encode(raw_response.encode()).decode('utf-8').rstrip('=')
        return encoded

    def _response_error(self, code):
        # SQRL error handling is specific but HTTP code 200 usually?
        # We'll just return text.
        return f"error={code}"

    def check_session(self, nut):
        return self._sessions.get(nut)
        
from datetime import datetime
