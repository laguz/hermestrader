from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
client = MongoClient(uri)
db = client['investment_db']

new_key = "7xVW1wTgL4DwxD4dGFDZieCIl1ta"
new_acc = "VA60129978"

result = db.users.update_many(
    {}, 
    {"$set": {"tradier_key": new_key, "account_id": new_acc}}
)

print(f"Updated {result.modified_count} users with the new Tradier Sandbox credentials.")
