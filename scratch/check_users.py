import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from services.container import Container

db = Container.get_db()
if db is None:
    print("Database not connected.")
    sys.exit(1)

users = list(db.users.find())
print(f"Found {len(users)} users.")
for u in users:
    print(f"Username: {u.get('username')}, Nostr Pubkey: {u.get('nostr_pubkey')}")
