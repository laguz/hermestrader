from services.container import Container
import pprint

def connect_and_check():
    db = Container.get_db()
    
    print("--- Checking Bot Config ---")
    doc = db.bot_config.find_one({"_id": "main_bot"})
    if not doc:
        print("No main_bot config found!")
        return
        
    settings = doc.get('settings', {})
    print("\nSettings:")
    pprint.pprint(settings)
    
    print("\n--- Verifying Value Types ---")
    cs_max = settings.get('max_credit_spreads_per_symbol')
    wh_max = settings.get('max_wheel_contracts_per_symbol')
    
    print(f"max_credit_spreads_per_symbol: {cs_max} (Type: {type(cs_max)})")
    print(f"max_wheel_contracts_per_symbol: {wh_max} (Type: {type(wh_max)})")

if __name__ == "__main__":
    connect_and_check()
