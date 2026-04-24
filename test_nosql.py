from unittest.mock import patch, MagicMock
from exceptions import ValidationError
from services.ml_service import MLService
import datetime
import pytest
import pymongo

class MockTradier:
    pass

def test_nosql_injection():
    # If a payload like {"$ne": ""} gets passed as the symbol, it shouldn't execute
    # if we validate symbol properly.

    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_db.__getitem__.return_value = mock_collection
    mock_collection.find.return_value.sort.return_value = []

    with patch('services.container.Container.get_db') as mock_get_db:
        mock_get_db.return_value = mock_db
        service = MLService(MockTradier())

        try:
            # Let's bypass _validate_symbol for a second to see what happens
            original_validate = service._validate_symbol
            service._validate_symbol = lambda x: x

            payload = {"$ne": "TSLA"}

            # Call a function that uses the DB
            service._fetch_and_prepare_training_data(payload)
            print("VULNERABLE: executed find with dict payload:", mock_collection.find.call_args)

            # Restore and test the fix
            service._validate_symbol = original_validate
            try:
                service._fetch_and_prepare_training_data(payload)
                print("FAILED: validation didn't catch it")
            except ValidationError as e:
                print("SECURE: Validation caught it:", e)

        except Exception as e:
            print("Exception:", e)

if __name__ == "__main__":
    test_nosql_injection()
