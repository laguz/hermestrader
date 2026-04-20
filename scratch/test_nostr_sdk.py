try:
    from nostr_sdk import Event, Keys, PublicKey
    print("nostr_sdk imported successfully")
    
    # Test creating an event from JSON
    import json
    event_dict = {
        "id": "0000000000000000000000000000000000000000000000000000000000000000",
        "pubkey": "0000000000000000000000000000000000000000000000000000000000000000",
        "created_at": 1600000000,
        "kind": 1,
        "tags": [],
        "content": "test",
        "sig": "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    }
    try:
        event = Event.from_json(json.dumps(event_dict))
        print("Event.from_json worked")
    except Exception as e:
        print(f"Event.from_json failed: {e}")

except ImportError as e:
    print(f"ImportError: {e}")
except Exception as e:
    print(f"Error: {e}")
