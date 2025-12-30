from services.container import Container
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# We need to manually initialize if we don't use Container full setup or just connect directly
mongo_uri = os.getenv('MONGODB_URI')
if not mongo_uri:
    print("MONGODB_URI not found")
else:
    client = MongoClient(mongo_uri)
    db = client['investment_db']
    users = list(db.users.find({}, {'username': 1, 'vault': 1}))
    print(f"Found {len(users)} users.")
    for u in users:
        print(f"Username: {u.get('username')}")
        vault = u.get('vault', {})
        print(f"  Vault keys: {list(vault.keys())}")
