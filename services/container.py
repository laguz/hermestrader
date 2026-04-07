import os
import logging
import certifi
from pymongo import MongoClient
from services.tradier_service import TradierService

logger = logging.getLogger(__name__)

class Container:
    _tradier_service = None
    _mongo_client = None
    _db = None
    _auth_service = None
    _ml_service = None
    _analysis_service = None
    _bot_service = None

    @classmethod
    def get_tradier_service(cls) -> TradierService:
        if not cls._tradier_service:
            cls._tradier_service = TradierService()
        return cls._tradier_service

    @classmethod
    def get_mongo_client(cls):
        if not cls._mongo_client:
            mongo_uri = os.getenv('MONGODB_URI')
            if not mongo_uri:
                mongo_uri = os.getenv('MONGODB_URI_LOCAL')
            
            if not mongo_uri:
                logger.warning("MONGODB_URI not set. MongoDB features will fail.")
                return None
            kwargs = {'serverSelectionTimeoutMS': 2000}
            if 'localhost' not in mongo_uri and '127.0.0.1' not in mongo_uri and 'mongodb' not in mongo_uri:
                kwargs['tlsCAFile'] = certifi.where()
            
            cls._mongo_client = MongoClient(mongo_uri, **kwargs)
        return cls._mongo_client

    @classmethod
    def get_db(cls):
        if cls._db is None:
            client = cls.get_mongo_client()
            if client:
                cls._db = client['investment_db']
        return cls._db

    @classmethod
    def get_auth_service(cls):
        if not cls._auth_service:
            from services.auth_service import AuthService
            cls._auth_service = AuthService()
        return cls._auth_service

    @classmethod
    def get_ml_service(cls):
        if not cls._ml_service:
            from services.ml_service import MLService
            cls._ml_service = MLService(cls.get_tradier_service())
        return cls._ml_service

    @classmethod
    def get_bot_service(cls):
        if not cls._bot_service:
            from services.bot_service import BotService
            cls._bot_service = BotService()
        return cls._bot_service

    @classmethod
    def get_analysis_service(cls):
        if not cls._analysis_service:
            from services.analysis_service import AnalysisService
            cls._analysis_service = AnalysisService(
                cls.get_tradier_service(), cls.get_ml_service()
            )
        return cls._analysis_service
