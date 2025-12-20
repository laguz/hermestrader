from services.container import Container
from dotenv import load_dotenv
import os

load_dotenv()

try:
    db = Container.get_db()
    if db is None:
        print("ERROR: Container.get_db() returned None")
    else:
        coll = db['auto_trades']
        count = coll.count_documents({})
        print(f"Collection 'auto_trades' has {count} documents.")
        
        if count > 0:
            doc = coll.find_one()
            print("Sample Document:")
            print(doc)
            
except Exception as e:
    print(f"Exception: {e}")
