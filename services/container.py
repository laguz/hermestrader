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
            cls._mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        return cls._mongo_client

    @classmethod
    def get_db(cls):
        if cls._db is None:
            client = cls.get_mongo_client()
            if client:
                cls._db = client['investment_db']
        return cls._db

    @classmethod
    def get_ml_service(cls):
        from services.ml_service import MLService
        return MLService(cls.get_tradier_service())

