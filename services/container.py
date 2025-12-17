from services.tradier_service import TradierService

class Container:
    _tradier_service = None

    @classmethod
    def get_tradier_service(cls):
        if not cls._tradier_service:
            cls._tradier_service = TradierService()
        return cls._tradier_service
