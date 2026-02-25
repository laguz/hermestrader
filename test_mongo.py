import os
from dotenv import load_dotenv
load_dotenv('.env')
uri = os.getenv("MONGODB_URI")
if uri: uri = uri.strip('"').strip("'")
print("URI:", uri)

from pymongo import MongoClient
client = MongoClient(uri)
db = client['investment_db']

print("auto_trades count:", db.auto_trades.count_documents({}))
for x in db.auto_trades.find().sort("entry_date", -1).limit(5):
    print(x)
