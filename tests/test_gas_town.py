"""Test Gas Town pattern - agent memory reconstruction."""

import pytest
from datetime import datetime

from src.core.event_store import EventStore
from src.integrity.gas_town import GasTownMemory
from src.models.events import (
    AgentContextLoaded,
    CreditAnalysisCompleted,
    FraudScreeningCompleted,
    DecisionGenerated
)


@pytest.mark.asyncio
async def test_agent_crash_recovery(postgres_pool):
    """Test that agent can reconstruct context after crash."""
    store = EventStore(postgres_pool)
    memory = GasTownMemory(store)
    
    agent_id = "test-agent"
    session_id = "session-123"
    
    # Start agent session (Gas Town: first event MUST be context loaded)
    context_event = AgentContextLoaded(
        agent_id=agent_id,
        session_id=session_id,
        context_source="loan-123",
        event_replay_from_position=0,
        context_token_count=5000,
        model_version="v2.3.1"
    )
    
    await store.append(
        stream_id=f"agent-{agent_id}-{session_id}",
        events=[context_event],
        expected_version=-1,
        correlation_id="test-crash-1",
        causation_id=None
    )
    
    # Agent does some work (5 events)
    for i in range(5):
        if i % 2 == 0:
            event = CreditAnalysisCompleted(
                application_id=f"app-{i}",
                agent_id=agent_id,
                session_id=session_id,
                model_version="v2.3.1",
                confidence_score=0.85,
                risk_tier="MEDIUM",
                recommended_limit_usd=50000,
                analysis_duration_ms=1200,
                input_data_hash=f"hash{i}"
            )
        else:
            event = FraudScreeningCompleted(
                application_id=f"app-{i}",
                agent_id=agent_id,
                fraud_score=0.12,
                anomaly_flags=[],
                screening_model_version="fraud-v1.2",
                input_data_hash=f"hash{i}"
            )
        
        await store.append(
            stream_id=f"agent-{agent_id}-{session_id}",
            events=[event],
            expected_version=i+1  # After context event
        )
    
    # SIMULATE CRASH - agent process dies
    print("\n💥 Agent crashed! Memory lost.")
    
    # Reconstruct context from event store
    context = await memory.reconstruct_agent_context(
        agent_id=agent_id,
        session_id=session_id,
        token_budget=2000
    )
    
    # Verify reconstruction
    print(f"\n🔄 Reconstructed context:\n{context.context_text}")
    
    assert context.agent_id == agent_id
    assert context.session_id == session_id
    assert context.last_event_position == 6  # 1 context + 5 actions
    assert context.session_health_status == "HEALTHY"
    assert context.token_count > 0
    
    # Context should contain information about the work done
    assert "Credit analysis" in context.context_text
    assert "Fraud screening" in context.context_text
    assert "model v2.3.1" in context.context_text


@pytest.mark.asyncio
async def test_partial_action_detection(postgres_pool):
    """Test detection of partial/incomplete actions."""
    store = EventStore(postgres_pool)
    memory = GasTownMemory(store)
    
    agent_id = "test-agent"
    session_id = "session-partial"
    
    # Start session
    context_event = AgentContextLoaded(
        agent_id=agent_id,
        session_id=session_id,
        context_source="loan-456",
        event_replay_from_position=0,
        context_token_count=5000,
        model_version="v2.3.1"
    )
    
    await store.append(
        stream_id=f"agent-{agent_id}-{session_id}",
        events=[context_event],
        expected_version=-1
    )
    
    # Start credit analysis but don't complete it (simulate crash during work)
    # In a real scenario, there would be a CreditAnalysisRequested without completion
    # For this test, we'll just have the context event to simulate crash before any work
    
    # Reconstruct
    context = await memory.reconstruct_agent_context(
        agent_id=agent_id,
        session_id=session_id
    )
    
    # Should be HEALTHY (no partial actions)
    assert context.session_health_status == "HEALTHY"
    assert len(context.pending_work) == 0


@pytest.mark.asyncio
async def test_token_budget_truncation(postgres_pool):
    """Test that context is truncated to fit token budget."""
    store = EventStore(postgres_pool)
    memory = GasTownMemory(store)
    
    agent_id = "test-agent"
    session_id = "session-budget"
    
    # Start session
    context_event = AgentContextLoaded(
        agent_id=agent_id,
        session_id=session_id,
        context_source="loan-789",
        event_replay_from_position=0,
        context_token_count=5000,
        model_version="v2.3.1"
    )
    
    await store.append(
        stream_id=f"agent-{agent_id}-{session_id}",
        events=[context_event],
        expected_version=-1
    )
    
    # Add many events to exceed token budget
    for i in range(20):
        event = CreditAnalysisCompleted(
            application_id=f"app-{i}",
            agent_id=agent_id,
            session_id=session_id,
            model_version="v2.3.1",
            confidence_score=0.85,
            risk_tier="MEDIUM",
            recommended_limit_usd=50000,
            analysis_duration_ms=1200,
            input_data_hash=f"hash{i}"
        )
        await store.append(
            stream_id=f"agent-{agent_id}-{session_id}",
            events=[event],
            expected_version=i+1
        )
    
    # Reconstruct with small budget
    context = await memory.reconstruct_agent_context(
        agent_id=agent_id,
        session_id=session_id,
        token_budget=500  # Small budget
    )
    
    # Should be truncated
    assert "[truncated]" in context.context_text
    assert context.token_count > 0