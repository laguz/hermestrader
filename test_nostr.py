from nostr_sdk import Event, EventBuilder, Keys, Kind

keys = Keys.generate()
builder = EventBuilder(Kind(22242), "Login")
event = builder.sign_with_keys(keys)
event_json = event.as_json()
print("Valid Event JSON:")
print(event_json)

# Now test parsing
parsed = Event.from_json(event_json)
print("Parsed ID:", parsed.id().to_hex())
