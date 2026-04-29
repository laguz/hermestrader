import os
import psycopg
from datetime import datetime

def get_latest_hermes_logs():
    dsn = os.environ.get("HERMES_DSN", "postgresql://hermes:hermes@localhost:5432/hermes")
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ts, strategy_id, level, message 
                    FROM bot_logs 
                    ORDER BY ts DESC 
                    LIMIT 50
                """)
                rows = cur.fetchall()
                print(f"--- Recent Hermes Bot Logs ({len(rows)} found) ---")
                for row in reversed(rows):
                    print(f"[{row[0]}] [{row[1]}] {row[2]}: {row[3]}")
    except Exception as e:
        print(f"Error connecting to Postgres: {e}")

if __name__ == "__main__":
    get_latest_hermes_logs()
