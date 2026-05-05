
import psycopg2
import os

DSN = os.environ.get("HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes")
# Convert DSN to psycopg2 format if needed
if DSN.startswith("postgresql+psycopg://"):
    DSN = DSN.replace("postgresql+psycopg://", "postgresql://")

print(f"Connecting to {DSN}...")
try:
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    print("Checking if target_lots exists...")
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='strategy_watchlists' AND column_name='target_lots'")
    if not cur.fetchone():
        print("Adding target_lots column...")
        cur.execute("ALTER TABLE strategy_watchlists ADD COLUMN target_lots INTEGER")
        print("Success.")
    else:
        print("Column already exists.")
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
