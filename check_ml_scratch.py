import os
import sys
from sqlalchemy import create_engine, text

dsn = os.environ.get("HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes")
engine = create_engine(dsn)

def check_status():
    with engine.connect() as conn:
        print("--- System Settings ---")
        res = conn.execute(text("SELECT key, value FROM system_settings WHERE key LIKE 'ml_%'"))
        for row in res:
            print(f"{row[0]}: {row[1]}")
        
        print("\n--- Recent Predictions ---")
        res = conn.execute(text("SELECT symbol, predicted_return, ts FROM predictions ORDER BY ts DESC LIMIT 5"))
        for row in res:
            print(f"{row[0]}: {row[1]} at {row[2]}")

        print("\n--- Bar Counts ---")
        res = conn.execute(text("SELECT symbol, COUNT(*) FROM bars_daily GROUP BY symbol"))
        for row in res:
            print(f"{row[0]}: {row[1]} daily bars")

if __name__ == "__main__":
    try:
        check_status()
    except Exception as e:
        print(f"Error: {e}")
