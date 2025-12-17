import sys
import os

sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

from services.container import Container

def verify_data():
    db = Container.get_db()
    if db is None:
        print("Error: No DB")
        return

    collection = db['market_data']
    count = collection.count_documents({"symbol": "SPY"})
    print(f"Total SPY records in MongoDB: {count}")
    
    latest = collection.find_one({"symbol": "SPY"}, sort=[("date", -1)])
    if latest:
        print(f"Latest record: {latest['date']} - Close: {latest['close']}")
    
    oldest = collection.find_one({"symbol": "SPY"}, sort=[("date", 1)])
    if oldest:
        print(f"Oldest record: {oldest['date']} - Close: {oldest['close']}")

if __name__ == "__main__":
    verify_data()
