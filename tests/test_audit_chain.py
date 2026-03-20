"""Test cryptographic audit chain."""

import pytest
from datetime import datetime

from src.core.event_store import EventStore
from src.integrity.audit_chain import AuditChain
from src.models.events import ApplicationSubmitted


@pytest.mark.asyncio
async def test_audit_chain_tamper_detection(postgres_pool):
    """Test that tampering with events breaks the hash chain."""
    store = EventStore(postgres_pool)
    audit = AuditChain(store)
    
    # Create some events
    app_id = "test-audit-123"
    
    for i in range(3):
        event = ApplicationSubmitted(
            application_id=app_id,
            applicant_id=f"applicant-{i}",
            requested_amount_usd=10000 * (i+1),
            loan_purpose="test",
            submission_channel="api",
            submitted_at=datetime.utcnow().isoformat()
        )
        await store.append(
            stream_id=f"loan-{app_id}",
            events=[event],
            expected_version=i if i > 0 else -1
        )
    
    # Run integrity check
    result = await audit.run_integrity_check("loan", app_id)
    assert result.chain_valid is True
    assert result.tamper_detected is False
    assert result.events_verified == 3
    
    # Tamper with an event (simulate by directly updating database)
    async with postgres_pool.acquire() as conn:
        await conn.execute("""
            UPDATE events 
            SET payload = jsonb_set(payload, '{requested_amount_usd}', '999999')
            WHERE stream_id = $1 AND stream_position = 1
        """, f"loan-{app_id}")
    
    # Run another integrity check - should detect tampering
    result2 = await audit.run_integrity_check("loan", app_id)
    assert result2.tamper_detected is True
    assert result2.chain_valid is False