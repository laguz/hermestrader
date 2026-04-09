import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_logs():
    mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    client = MongoClient(mongo_uri)
    db = client['investment_db']
    config = db['bot_config'].find_one({"_id": "main_bot"})
    
    if not config or 'logs' not in config:
        print("No logs found in DB.")
        return

    print("--- RECENT BOT LOGS ---")
    for log in config['logs']:
        # Filter for TASTYTRADE45 or any relevant messages
        if "TASTYTRADE45" in log.get('message', ''):
             print(f"{log.get('timestamp')} - {log.get('message')}")
        # Also print any general analyzing messages to see what's being processed
        elif "Analyzing" in log.get('message', ''):
             print(f"{log.get('timestamp')} - {log.get('message')}")

if __name__ == "__main__":
    get_logs()
