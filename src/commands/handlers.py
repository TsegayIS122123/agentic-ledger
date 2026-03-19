"""Command handlers - implement the command pattern."""

from dataclasses import dataclass
from typing import List, Optional
import hashlib
import json

from src.core.event_store import EventStore
from src.core.exceptions import DomainError, OptimisticConcurrencyError
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.agent_session import AgentSessionAggregate
from src.models.events import (
    CreditAnalysisCompleted, AgentContextLoaded, 
    ApplicationSubmitted, DecisionGenerated
)


@dataclass
class SubmitApplicationCommand:
    """Command to submit a new loan application."""
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    submission_channel: str
    submitted_at: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class CreditAnalysisCompletedCommand:
    """Command to record credit analysis results."""
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: float
    duration_ms: int
    input_data: dict
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class StartAgentSessionCommand:
    """Command to start a new agent session."""
    agent_id: str
    session_id: str
    context_source: str
    event_replay_from_position: int
    context_token_count: int
    model_version: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


def hash_inputs(data: dict) -> str:
    """Create SHA-256 hash of input data for audit trail."""
    return hashlib.sha256(
        json.dumps(data, sort_keys=True).encode()
    ).hexdigest()


async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store: EventStore
) -> None:
    """
    Handle application submission.
    
    Creates a new loan application stream with first event.
    """
    # Check if stream already exists
    try:
        version = await store.stream_version(f"loan-{cmd.application_id}")
        if version > 0:
            raise DomainError(
                f"Application {cmd.application_id} already exists",
                aggregate_type="LoanApplication",
                stream_id=f"loan-{cmd.application_id}"
            )
    except:
        pass  # Stream doesn't exist - good!
    
    # Create event
    event = ApplicationSubmitted(
        application_id=cmd.application_id,
        applicant_id=cmd.applicant_id,
        requested_amount_usd=cmd.requested_amount_usd,
        loan_purpose=cmd.loan_purpose,
        submission_channel=cmd.submission_channel,
        submitted_at=cmd.submitted_at
    )
    
    # Append to store
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event],
        expected_version=-1,  # New stream
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store: EventStore
) -> None:
    """
    Handle starting a new agent session.
    
    Gas Town pattern: This MUST be the first event in any agent session.
    """
    # Check if session already exists
    stream_id = f"agent-{cmd.agent_id}-{cmd.session_id}"
    try:
        version = await store.stream_version(stream_id)
        if version > 0:
            raise DomainError(
                f"Session {cmd.session_id} already exists",
                aggregate_type="AgentSession",
                stream_id=stream_id
            )
    except:
        pass  # Stream doesn't exist - good!
    
    # Create event
    event = AgentContextLoaded(
        agent_id=cmd.agent_id,
        session_id=cmd.session_id,
        context_source=cmd.context_source,
        event_replay_from_position=cmd.event_replay_from_position,
        context_token_count=cmd.context_token_count,
        model_version=cmd.model_version
    )
    
    # Append to store
    await store.append(
        stream_id=stream_id,
        events=[event],
        expected_version=-1,  # New stream
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store: EventStore
) -> None:
    """
    Handle credit analysis completion.
    
    This is the critical command that demonstrates:
    - Loading multiple aggregates
    - Business rule validation
    - Event creation and storage
    """
    # STEP 1: LOAD aggregates
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)
    
    # STEP 2: VALIDATE all business rules
    # Loan rules
    app.assert_awaiting_credit_analysis()
    app.assert_no_double_analysis()
    
    # Agent rules (Gas Town pattern)
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)
    
    # STEP 3: CREATE events (pure domain logic, no I/O)
    new_events = [
        CreditAnalysisCompleted(
            application_id=cmd.application_id,
            agent_id=cmd.agent_id,
            session_id=cmd.session_id,
            model_version=cmd.model_version,
            confidence_score=cmd.confidence_score,
            risk_tier=cmd.risk_tier,
            recommended_limit_usd=cmd.recommended_limit_usd,
            analysis_duration_ms=cmd.duration_ms,
            input_data_hash=hash_inputs(cmd.input_data)
        )
    ]
    
    # STEP 4: APPEND atomically with optimistic concurrency
    try:
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,  # Critical for concurrency!
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id
        )
    except OptimisticConcurrencyError:
        # Someone else modified the stream - reload and retry
        # In production, you'd have retry logic here
        raise