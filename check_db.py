import os
import sys

sys.path.insert(0, os.path.abspath('.'))

from services.container import Container

db = Container.get_db()
recent_trades = list(db.auto_trades.find().sort('entry_date', -1).limit(5))
print(f"Found {len(recent_trades)} recent trades.")
for t in recent_trades:
    print(t.get('entry_date'), t.get('symbol'), t.get('strategy'), t.get('price'))

bot_status = db.bot_config.find_one({'_id': 'main_bot'})
print('--- RECENT LOGS ---')
for log in bot_status.get('logs', [])[-15:]:
    print(log.get('timestamp'), log.get('message'))
