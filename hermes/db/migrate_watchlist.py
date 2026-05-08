
from hermes.db.models import HermesDB
import os

DSN = os.environ.get("DATABASE_URL", "postgresql://hermes:hermes@localhost:5432/hermes")
db = HermesDB(DSN)

with db.engine.connect() as conn:
    print("Adding target_lots to strategy_watchlists...")
    try:
        conn.execute("ALTER TABLE strategy_watchlists ADD COLUMN target_lots INTEGER")
        conn.commit()
        print("Success.")
    except Exception as e:
        print(f"Error (probably already exists): {e}")
