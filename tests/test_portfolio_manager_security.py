import unittest
from unittest.mock import MagicMock, patch
from bot.portfolio_manager import PortfolioManager
import hashlib

class TestPortfolioManagerSecurity(unittest.TestCase):
    def test_portfolio_manager_uses_sha256(self):
        """
        Verify that _backfill_history uses SHA-256 (64 hex characters) instead of MD5 (32 hex characters)
        for generating document IDs.
        """
        mock_tradier = MagicMock()
        mock_db = MagicMock()
        mock_open_positions = MagicMock()

        # Configure DB mock to return our mocked collection
        mock_db.__getitem__.return_value = mock_open_positions

        # Ensure that no existing docs are found, so insert_one will be called
        mock_open_positions.find_one.return_value = None

        # Mock the return value of tradier.get_gainloss
        mock_history = [
            {
                'symbol': 'AAPL',
                'open_date': '2023-10-01T00:00:00Z',
                'close_date': '2023-10-05T00:00:00Z',
                'quantity': 10,
                'cost': 1500,
                'proceeds': 1600,
                'gain_loss': 100
            }
        ]
        mock_tradier.get_gainloss.return_value = mock_history

        pm = PortfolioManager(tradier=mock_tradier, db=mock_db)

        # Run the backfill
        pm._backfill_history(log_func=lambda x: None)

        # Ensure insert_one was called
        self.assertTrue(mock_open_positions.insert_one.called, "insert_one should be called")

        # Get the arguments it was called with
        args, kwargs = mock_open_positions.insert_one.call_args
        inserted_doc = args[0]

        # Verify the length of _id is 64 (SHA-256) and not 32 (MD5)
        doc_id = inserted_doc.get('_id')
        self.assertIsNotNone(doc_id)
        self.assertEqual(len(doc_id), 64, f"Expected _id length to be 64 for SHA-256, but got {len(doc_id)}")

        # Verify it matches the expected SHA-256 hash
        expected_raw_id = "AAPL_2023-10-01T00:00:00Z_2023-10-05T00:00:00Z_10"
        expected_hash = hashlib.sha256(expected_raw_id.encode()).hexdigest()
        self.assertEqual(doc_id, expected_hash, "The _id does not match the expected SHA-256 hash.")
