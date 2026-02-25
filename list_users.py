from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
uri = os.getenv('MONGODB_URI', "mongodb://localhost:27017/")
client = MongoClient(uri)
db = client['investment_db']
users = list(db.users.find({}, {"_id": 0, "username": 1, "email": 1}))
print("Users:", users)
