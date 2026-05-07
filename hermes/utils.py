"""Centralised utility functions for HermesTrader."""
from __future__ import annotations

import os
import logging
from cryptography.fernet import Fernet

logger = logging.getLogger("hermes.utils")

ENCRYPTION_KEY = os.environ.get("HERMES_ENCRYPTION_KEY")
ENCRYPTED_PREFIX = "enc:"

def _get_fernet() -> Fernet | None:
    if not ENCRYPTION_KEY:
        return None
    try:
        return Fernet(ENCRYPTION_KEY.encode())
    except Exception as exc:
        logger.warning("Invalid HERMES_ENCRYPTION_KEY: %s", exc)
        return None

def encrypt_value(value: str) -> str:
    """Encrypt a string value using Fernet if HERMES_ENCRYPTION_KEY is set.

    Returns the encrypted value with an 'enc:' prefix, or the original
    value if encryption is disabled.
    """
    if not value:
        return value

    f = _get_fernet()
    if not f:
        return value

    try:
        encrypted = f.encrypt(value.encode()).decode()
        return f"{ENCRYPTED_PREFIX}{encrypted}"
    except Exception as exc:
        logger.warning("Encryption failed: %s", exc)
        return value

def decrypt_value(value: str) -> str:
    """Decrypt a string value if it has the 'enc:' prefix.

    Returns the decrypted value, or the original value if it's not
    prefixed or if decryption fails.
    """
    if not value or not value.startswith(ENCRYPTED_PREFIX):
        return value

    f = _get_fernet()
    if not f:
        logger.warning("Value is encrypted but HERMES_ENCRYPTION_KEY is missing")
        return value

    try:
        token = value[len(ENCRYPTED_PREFIX):]
        return f.decrypt(token.encode()).decode()
    except Exception as exc:
        logger.warning("Decryption failed: %s", exc)
        return value
