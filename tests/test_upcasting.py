"""Test upcasting immutability guarantee."""

import pytest
import json
import asyncpg
from datetime import datetime

from src.core.event_store import EventStore
from src.models.events import BaseEvent, StoredEvent


class CreditAnalysisCompletedV1(BaseEvent):
    """V1 of CreditAnalysisCompleted - no model_version or confidence_score."""
    event_type = "CreditAnalysisCompleted"
    event_version = 1
    
    def __init__(self, application_id: str, agent_id: str, session_id: str,
                 risk_tier: str, recommended_limit_usd: float,
                 analysis_duration_ms: int, input_data_hash: str):
        super().__init__()
        self.application_id = application_id
        self.agent_id = agent_id
        self.session_id = session_id
        self.risk_tier = risk_tier
        self.recommended_limit_usd = recommended_limit_usd
        self.analysis_duration_ms = analysis_duration_ms
        self.input_data_hash = input_data_hash


@pytest.mark.asyncio
async def test_upcasting_immutability(postgres_pool):
    """
    CRITICAL TEST: Verify that upcasting NEVER modifies stored events.
    
    This test ensures the core guarantee of event sourcing:
    - Past events are immutable
    - Schema evolution happens at read time only
    - The database is never updated after initial write
    """
    store = EventStore(postgres_pool)
    stream_id = "loan-upcast-test"
    
    # STEP 1: Store a v1 event directly
    v1_event = CreditAnalysisCompletedV1(
        application_id="test-789",
        agent_id="agent-1",
        session_id="session-123",
        risk_tier="MEDIUM",
        recommended_limit_usd=50000,
        analysis_duration_ms=1500,
        input_data_hash="abc123def456"
    )
    
    await store.append(
        stream_id=stream_id,
        events=[v1_event],
        expected_version=-1
    )
    
    # STEP 2: Get the raw stored payload from database
    async with postgres_pool.acquire() as conn:
        raw_row = await conn.fetchrow("""
            SELECT event_id, event_version, payload, recorded_at
            FROM events
            WHERE stream_id = $1
        """, stream_id)
        
        raw_payload = raw_row['payload']
        raw_version = raw_row['event_version']
        recorded_at = raw_row['recorded_at']
        
        print(f"\n🔍 Raw stored event - Version: {raw_version}")
        print(f"🔍 Raw payload: {raw_payload}")
        
        # Verify it's stored as v1
        assert raw_version == 1
        assert "model_version" not in raw_payload
        assert "confidence_score" not in raw_payload
    
    # STEP 3: Load through EventStore (should be upcasted to v2)
    events = await store.load_stream(stream_id)
    loaded = events[0]
    
    print(f"\n🔄 Loaded event - Version: {loaded.event_version}")
    print(f"🔄 Upcasted payload: {loaded.payload}")
    
    # Verify it's upcasted to v2
    assert loaded.event_version == 2
    assert "model_version" in loaded.payload
    assert "confidence_score" in loaded.payload
    assert "regulatory_basis" in loaded.payload
    
    # Check inference strategy
    assert loaded.payload["model_version"] in ["legacy-pre-2026", "v1.0-initial"]
    assert loaded.payload["confidence_score"] is None
    assert isinstance(loaded.payload["regulatory_basis"], list)
    
    # STEP 4: Verify raw stored payload is UNCHANGED
    async with postgres_pool.acquire() as conn:
        new_raw = await conn.fetchrow("""
            SELECT event_version, payload
            FROM events
            WHERE stream_id = $1
        """, stream_id)
        
        print(f"\n✅ Raw stored after load - Version: {new_raw['event_version']}")
        print(f"✅ Raw payload unchanged: {new_raw['payload']}")
        
        # The stored event is STILL v1!
        assert new_raw['event_version'] == 1
        assert "model_version" not in new_raw['payload']
        assert "confidence_score" not in new_raw['payload']
        assert new_raw['payload'] == raw_payload  # Exactly the same!
    
    print("\n🎉 Immutability test passed!")
    print("   - Event stored as v1")
    print("   - Loaded as v2 via upcasting")
    print("   - Raw storage UNCHANGED")