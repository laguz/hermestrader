from services.tradier_service import TradierService
from pymongo import MongoClient
import os

class Container:
    _tradier_service = None
    _mongo_client = None
    _db = None

    @classmethod
    def get_tradier_service(cls):
        if not cls._tradier_service:
            cls._tradier_service = TradierService()
        return cls._tradier_service

    @classmethod
    def get_mongo_client(cls):
        if not cls._mongo_client:
            mongo_uri = os.getenv('MONGODB_URI_LOCAL')
            if not mongo_uri:
                # Fallback or Error? Ideally log warning.
                print("WARNING: MONGODB_URI_LOCAL not set. MongoDB features will fail.")
                return None
            cls._mongo_client = MongoClient(mongo_uri)
        return cls._mongo_client

    @classmethod
    def get_db(cls):
        if not cls._db:
            client = cls.get_mongo_client()
            if client:
                # Use a default database name 'investment_db' or parse from URI if needed.
                # Usually URI might have path /dbname? but often we pick one.
                cls._db = client['investment_db']
        return cls._db

