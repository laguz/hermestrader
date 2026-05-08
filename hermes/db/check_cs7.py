
import psycopg
import os

DSN = os.environ.get("HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes")
# Psycopg3 likes postgresql:// or psycopg://
if DSN.startswith("postgresql+psycopg://"):
    DSN = DSN.replace("postgresql+psycopg://", "postgresql://")

print(f"Connecting to {DSN}...")
try:
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            print("Fetching CS7 Watchlist with Lots...")
            cur.execute("SELECT symbol, target_lots FROM strategy_watchlists WHERE strategy_id = 'CS7'")
            rows = cur.fetchall()
            if not rows:
                print("CS7 Watchlist is empty in the database.")
            else:
                for sym, lots in rows:
                    print(f"SYMBOL: {sym} | LOTS: {lots if lots is not None else '10 (Default)'}")
            
            print("\nFetching Global Settings...")
            cur.execute("SELECT value FROM system_settings WHERE key = 'cs7_target_lots'")
            row = cur.fetchone()
            print(f"GLOBAL CS7 TARGET: {row[0] if row else '10'}")
except Exception as e:
    print(f"Error: {e}")
