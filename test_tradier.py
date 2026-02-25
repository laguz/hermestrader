from services.container import Container
import os
from dotenv import load_dotenv
load_dotenv()
try:
    tradier = Container.get_tradier_service()
    quote = tradier.get_quote('AAPL')
    print("Tradier quote:", quote)
except Exception as e:
    print("Error:", e)
