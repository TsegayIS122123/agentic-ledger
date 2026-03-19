"""Test optimistic concurrency control with the double-decision scenario."""

import asyncio
import pytest
import asyncpg
from datetime import datetime

from src.core.event_store import EventStore
from src.core.exceptions import OptimisticConcurrencyError
from src.models.events import BaseEvent


class CreditAnalysisCompleted(BaseEvent):
    """Test event for credit analysis."""
    event_type = "CreditAnalysisCompleted"
    event_version = 1
    
    def __init__(self, application_id: str, risk_tier: str):
        super().__init__()
        self.application_id = application_id
        self.risk_tier = risk_tier


@pytest.mark.asyncio
async def test_double_decision_concurrency(postgres_pool):
    """
    The DOUBLE-DECISION TEST - Most critical test in Phase 1.
    
    Scenario:
    - Stream loan-123 has 3 events (version = 3)
    - Two agents both read version 3
    - Both try to append with expected_version=3
    - Exactly ONE must succeed, ONE must get OptimisticConcurrencyError
    
    This simulates two fraud detection agents simultaneously flagging
    the same application in the Apex Financial Services scenario.
    """
    # Setup: Create a stream with 3 events
    store = EventStore(postgres_pool)
    stream_id = "loan-double-decision-test"
    
    # Add 3 initial events to get to version 3
    for i in range(3):
        await store.append(
            stream_id=stream_id,
            events=[CreditAnalysisCompleted(f"app-{i}", "MEDIUM")],
            expected_version=i if i > 0 else -1
        )
    
    # Verify initial version is 3
    version = await store.stream_version(stream_id)
    assert version == 3, f"Expected version 3, got {version}"
    
    # Create two concurrent tasks that both try to append at version 3
    async def agent_a_append():
        """Agent A (credit analysis) tries to append."""
        event = CreditAnalysisCompleted("app-123", "HIGH")
        try:
            new_version = await store.append(
                stream_id=stream_id,
                events=[event],
                expected_version=3,
                correlation_id="test-correlation-a",
                causation_id="test-causation-a"
            )
            return {"success": True, "version": new_version, "agent": "A"}
        except OptimisticConcurrencyError as e:
            return {"success": False, "error": e, "agent": "A"}
    
    async def agent_b_append():
        """Agent B (fraud detection) tries to append."""
        event = CreditAnalysisCompleted("app-123", "LOW")
        try:
            new_version = await store.append(
                stream_id=stream_id,
                events=[event],
                expected_version=3,
                correlation_id="test-correlation-b",
                causation_id="test-causation-b"
            )
            return {"success": True, "version": new_version, "agent": "B"}
        except OptimisticConcurrencyError as e:
            return {"success": False, "error": e, "agent": "B"}
    
    # Run both agents concurrently
    results = await asyncio.gather(
        agent_a_append(),
        agent_b_append()
    )
    
    # Analyze results
    successes = [r for r in results if r["success"]]
    errors = [r for r in results if not r["success"]]
    
    # ASSERTION A: Exactly one succeeds
    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}"
    # ASSERTION B: Exactly one gets error
    assert len(errors) == 1, f"Expected 1 error, got {len(errors)}"
    
    # Get the winning agent
    winner = successes[0]
    loser = errors[0]
    
    print(f"\n✅ WINNER: Agent {winner['agent']} succeeded at version {winner['version']}")
    print(f"✅ LOSER: Agent {loser['agent']} got OptimisticConcurrencyError")
    
    # ASSERTION C: Winning event has stream_position = 4
    assert winner['version'] == 4, f"Winner version should be 4, got {winner['version']}"
    
    # Verify final stream version is 4 (only one new event added)
    final_version = await store.stream_version(stream_id)
    # ASSERTION D: Total events = 4 (3 original + 1 new)
    assert final_version == 4, f"Final version should be 4, got {final_version}"
    
    # Verify the losing agent got the right error type
    assert isinstance(loser['error'], OptimisticConcurrencyError)
    assert loser['error'].stream_id == stream_id
    assert loser['error'].expected_version == 3
    assert loser['error'].actual_version == 4  # After winner committed
    
    # Load the stream and verify order
    events = await store.load_stream(stream_id)
    assert len(events) == 4
    
    # The winning event should be at position 4
    last_event = events[-1]
    assert last_event.stream_position == 4
    
    # Verify correlation_ids are preserved
    if winner['agent'] == "A":
        assert last_event.metadata.get("correlation_id") == "test-correlation-a"
    else:
        assert last_event.metadata.get("correlation_id") == "test-correlation-b"
    
    print("\n🎉 DOUBLE-DECISION TEST PASSED!")
    print("   - Two concurrent agents attempted to append")
    print("   - Exactly one succeeded, one got OptimisticConcurrencyError")
    print("   - Stream version correctly advanced to 4")
    print("   - Event order preserved")


@pytest.mark.asyncio
async def test_retry_after_concurrency_error(postgres_pool):
    """
    Test that the losing agent can reload and retry successfully.
    
    This is what should happen in production:
    1. Agent gets OptimisticConcurrencyError
    2. Agent reloads stream to see what happened
    3. Agent decides whether to retry or abort
    """
    store = EventStore(postgres_pool)
    stream_id = "loan-retry-test"
    
    # Setup stream with initial events
    for i in range(2):
        await store.append(
            stream_id=stream_id,
            events=[CreditAnalysisCompleted(f"app-{i}", "MEDIUM")],
            expected_version=i if i > 0 else -1
        )
    
    # First agent succeeds
    async def winning_agent():
        event = CreditAnalysisCompleted("app-123", "HIGH")
        return await store.append(
            stream_id=stream_id,
            events=[event],
            expected_version=2,
            correlation_id="winning-agent"
        )
    
    # Second agent will lose, then retry
    async def losing_agent_with_retry():
        try:
            # First attempt (will lose)
            event = CreditAnalysisCompleted("app-123", "LOW")
            await store.append(
                stream_id=stream_id,
                events=[event],
                expected_version=2,
                correlation_id="losing-agent-first"
            )
        except OptimisticConcurrencyError as e:
            print(f"\n🔄 Agent lost: {e}")
            
            # RELOAD the stream to see what happened
            events = await store.load_stream(stream_id)
            assert len(events) == 3  # Original 2 + winner's event
            
            # Check if our action is still needed
            last_event = events[-1]
            if last_event.payload.get("risk_tier") != "LOW":
                # Our analysis still relevant - retry with new version
                print("🔄 Retrying with new version...")
                event = CreditAnalysisCompleted("app-123", "LOW")
                new_version = await store.append(
                    stream_id=stream_id,
                    events=[event],
                    expected_version=3,  # Now version is 3
                    correlation_id="losing-agent-retry"
                )
                return {"success": True, "version": new_version}
        
        return {"success": False}
    
    # Run both
    winner_task = asyncio.create_task(winning_agent())
    loser_task = asyncio.create_task(losing_agent_with_retry())
    
    winner_version, loser_result = await asyncio.gather(winner_task, loser_task)
    
    assert winner_version == 3
    assert loser_result["success"] is True
    assert loser_result["version"] == 4
    
    # Final stream should have 4 events
    final_version = await store.stream_version(stream_id)
    assert final_version == 4
    
    print("\n🎉 RETRY TEST PASSED!")
    print("   - Losing agent reloaded, retried, and succeeded")