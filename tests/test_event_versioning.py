from __future__ import annotations

from typing import Any, Dict

from hermes.db.events import BaseEvent, deserialize_event, EVENT_TYPE_TO_CLASS


class DummyVersionedEvent(BaseEvent):
    """Test event to verify version upgrades."""
    val: int
    tag: str = "default"
    event_version: int = 2

    @classmethod
    def upgrade_payload(cls, payload: Dict[str, Any], from_version: int) -> Dict[str, Any]:
        if from_version < 2:
            # Upgrade logic: map old key 'old_val' to 'val', and add a tag if missing
            if "old_val" in payload:
                payload["val"] = payload.pop("old_val")
            if "tag" not in payload:
                payload["tag"] = "upgraded"
            payload["event_version"] = 2
        return payload


def test_event_versioning_serialization():
    """Verify that serializing versioned events includes the event_version field."""
    # When instantiated, it uses the class default version (2)
    ev = DummyVersionedEvent(val=42)
    assert ev.event_version == 2
    
    # model_dump should include event_version
    dumped = ev.model_dump()
    assert dumped["event_version"] == 2
    assert dumped["val"] == 42
    assert dumped["tag"] == "default"


def test_event_versioning_upgrade():
    """Verify that deserialize_event successfully runs upgrade_payload on old versions."""
    # Register the dummy event temporarily
    original_classes = EVENT_TYPE_TO_CLASS.copy()
    try:
        EVENT_TYPE_TO_CLASS["DUMMY_VERSIONED"] = DummyVersionedEvent
        
        # Test 1: Deserializing version 1 payload (simulating old DB record)
        # It has 'old_val' instead of 'val', and lacks 'tag' and 'event_version'.
        old_payload = {"old_val": 100}
        
        ev = deserialize_event("DUMMY_VERSIONED", old_payload, event_id=99)
        
        assert ev is not None
        assert isinstance(ev, DummyVersionedEvent)
        assert ev.event_id == 99
        assert ev.val == 100
        assert ev.tag == "upgraded"
        assert ev.event_version == 2
        
        # Test 2: Deserializing already updated version 2 payload
        new_payload = {"val": 200, "tag": "custom", "event_version": 2}
        ev2 = deserialize_event("DUMMY_VERSIONED", new_payload)
        assert ev2 is not None
        assert ev2.val == 200
        assert ev2.tag == "custom"
        assert ev2.event_version == 2
        
    finally:
        # Restore registry
        EVENT_TYPE_TO_CLASS.clear()
        EVENT_TYPE_TO_CLASS.update(original_classes)
