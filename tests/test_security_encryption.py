import os
import unittest
from unittest.mock import patch
from cryptography.fernet import Fernet
from hermes.utils import encrypt_value, decrypt_value

class TestSecurityEncryption(unittest.TestCase):
    def setUp(self):
        # A valid Fernet key for testing
        self.test_key = Fernet.generate_key().decode()

    def test_encryption_decryption_with_key(self):
        """Test that encryption and decryption work correctly when a key is present."""
        with patch.dict(os.environ, {"HERMES_ENCRYPTION_KEY": self.test_key}):
            # We need to reload or re-evaluate the ENCRYPTION_KEY in hermes.utils
            # Since it's evaluated at module load, we patch it directly for the test
            with patch("hermes.utils.ENCRYPTION_KEY", self.test_key):
                original_value = "my-secret-api-key"
                encrypted = encrypt_value(original_value)

                self.assertTrue(encrypted.startswith("enc:"))
                self.assertNotEqual(encrypted, f"enc:{original_value}")

                decrypted = decrypt_value(encrypted)
                self.assertEqual(decrypted, original_value)

    def test_backward_compatibility(self):
        """Test that non-prefixed values are returned as-is (plaintext)."""
        with patch.dict(os.environ, {"HERMES_ENCRYPTION_KEY": self.test_key}):
            with patch("hermes.utils.ENCRYPTION_KEY", self.test_key):
                plaintext = "already-in-db-as-plaintext"
                decrypted = decrypt_value(plaintext)
                self.assertEqual(decrypted, plaintext)

    def test_no_key_behavior(self):
        """Test that functions fall back to plaintext when no key is present."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("hermes.utils.ENCRYPTION_KEY", None):
                secret = "some-secret"
                encrypted = encrypt_value(secret)
                self.assertEqual(encrypted, secret)

                decrypted = decrypt_value(encrypted)
                self.assertEqual(decrypted, secret)

    def test_decryption_failure_returns_original(self):
        """Test that if decryption fails, the original prefixed value is returned."""
        with patch.dict(os.environ, {"HERMES_ENCRYPTION_KEY": self.test_key}):
            with patch("hermes.utils.ENCRYPTION_KEY", self.test_key):
                invalid_encrypted = "enc:not-a-valid-token"
                decrypted = decrypt_value(invalid_encrypted)
                self.assertEqual(decrypted, invalid_encrypted)

if __name__ == "__main__":
    unittest.main()
