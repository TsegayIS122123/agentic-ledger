"""Command handlers - implement the command pattern."""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import hashlib
import json
from datetime import datetime  

from src.core.event_store import EventStore
from src.core.exceptions import DomainError, OptimisticConcurrencyError
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.agent_session import AgentSessionAggregate
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    AgentContextLoaded,
    DecisionGenerated,
    FraudScreeningCompleted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined
)


# ========== COMMAND CLASSES ==========

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


@dataclass
class FraudScreeningCompletedCommand:
    """Command to record fraud screening results."""
    application_id: str
    agent_id: str
    fraud_score: float
    anomaly_flags: List[str]
    model_version: str
    input_data: dict
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class ComplianceCheckCommand:
    """Command to record a compliance check result."""
    application_id: str
    rule_id: str
    rule_version: str
    passed: bool
    evidence_hash: str
    regulation_set: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class DecisionGeneratedCommand:
    """Command to generate a decision."""
    application_id: str
    orchestrator_agent_id: str
    recommendation: str
    confidence_score: float
    contributing_agent_sessions: List[str]
    decision_basis_summary: str
    model_versions: Dict[str, str]
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class HumanReviewCompletedCommand:
    """Command to record human review."""
    application_id: str
    reviewer_id: str
    override: bool
    final_decision: str
    override_reason: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


# ========== HELPER FUNCTION ==========

def hash_inputs(data: dict) -> str:
    """Create SHA-256 hash of input data for audit trail."""
    return hashlib.sha256(
        json.dumps(data, sort_keys=True).encode()
    ).hexdigest()


# ========== COMMAND HANDLERS ==========

async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store: EventStore
) -> None:
    """Handle application submission."""
    try:
        version = await store.stream_version(f"loan-{cmd.application_id}")
        if version > 0:
            raise DomainError(
                message=f"Application {cmd.application_id} already exists",
                aggregate_type="LoanApplication",
                stream_id=f"loan-{cmd.application_id}"
            )
    except:
        pass
    
    event = ApplicationSubmitted(
        application_id=cmd.application_id,
        applicant_id=cmd.applicant_id,
        requested_amount_usd=cmd.requested_amount_usd,
        loan_purpose=cmd.loan_purpose,
        submission_channel=cmd.submission_channel,
        submitted_at=cmd.submitted_at
    )
    
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store: EventStore
) -> None:
    """Handle starting a new agent session."""
    stream_id = f"agent-{cmd.agent_id}-{cmd.session_id}"
    try:
        version = await store.stream_version(stream_id)
        if version > 0:
            raise DomainError(
                message=f"Session {cmd.session_id} already exists",
                aggregate_type="AgentSession",
                stream_id=stream_id
            )
    except:
        pass
    
    event = AgentContextLoaded(
        agent_id=cmd.agent_id,
        session_id=cmd.session_id,
        context_source=cmd.context_source,
        event_replay_from_position=cmd.event_replay_from_position,
        context_token_count=cmd.context_token_count,
        model_version=cmd.model_version
    )
    
    await store.append(
        stream_id=stream_id,
        events=[event],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store: EventStore
) -> int:
    """Handle credit analysis completion."""
    # Load aggregates
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)
    
    # Validate
    app.assert_awaiting_credit_analysis()
    app.assert_no_double_analysis()
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)
    
    # Create event
    event = CreditAnalysisCompleted(
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
    
    # Append
    new_version = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )
    return new_version


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand,
    store: EventStore
) -> int:
    """Handle fraud screening completion."""
    # Load aggregates
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)
    
    # Validate
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)
    
    # Create event
    event = FraudScreeningCompleted(
        application_id=cmd.application_id,
        agent_id=cmd.agent_id,
        fraud_score=cmd.fraud_score,
        anomaly_flags=cmd.anomaly_flags,
        screening_model_version=cmd.model_version,
        input_data_hash=hash_inputs(cmd.input_data)
    )
    
    # Append
    new_version = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )
    return new_version


async def handle_compliance_check(
    cmd: ComplianceCheckCommand,
    store: EventStore
) -> str:
    """Handle compliance check."""
    # Create event
    if cmd.passed:
        event = ComplianceRulePassed(
            application_id=cmd.application_id,
            rule_id=cmd.rule_id,
            rule_version=cmd.rule_version,
            evaluation_timestamp=datetime.utcnow().isoformat(),
            evidence_hash=cmd.evidence_hash
        )
    else:
        event = ComplianceRuleFailed(
            application_id=cmd.application_id,
            rule_id=cmd.rule_id,
            rule_version=cmd.rule_version,
            failure_reason=cmd.evidence_hash,
            remediation_required=True
        )
    
    # Get current version
    stream_id = f"compliance-{cmd.application_id}"
    try:
        version = await store.stream_version(stream_id)
    except:
        version = 0
    
    # Append
    await store.append(
        stream_id=stream_id,
        events=[event],
        expected_version=version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )
    return f"{cmd.rule_id}-{cmd.application_id}"


async def handle_generate_decision(
    cmd: DecisionGeneratedCommand,
    store: EventStore
) -> int:
    """Handle decision generation."""
    # Load aggregate
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    
    # Enforce confidence floor
    recommendation = cmd.recommendation
    if cmd.confidence_score < 0.6 and recommendation != "REFER":
        recommendation = "REFER"
    
    # Create event
    event = DecisionGenerated(
        application_id=cmd.application_id,
        orchestrator_agent_id=cmd.orchestrator_agent_id,
        recommendation=recommendation,
        confidence_score=cmd.confidence_score,
        contributing_agent_sessions=cmd.contributing_agent_sessions,
        decision_basis_summary=cmd.decision_basis_summary,
        model_versions=cmd.model_versions
    )
    
    # Append
    new_version = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )
    return new_version


async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand,
    store: EventStore
) -> int:
    """Handle human review completion."""
    # Load aggregate
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    
    # Validate
    if cmd.override and not cmd.override_reason:
        raise DomainError(
            message="override_reason required when override=True",
            aggregate_type="LoanApplication",
            stream_id=f"loan-{cmd.application_id}"
        )
    
    # Create event
    event = HumanReviewCompleted(
        application_id=cmd.application_id,
        reviewer_id=cmd.reviewer_id,
        override=cmd.override,
        final_decision=cmd.final_decision,
        override_reason=cmd.override_reason
    )
    
    # Append
    new_version = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )
    
    # If final decision, also append approval or decline
    if cmd.final_decision == "APPROVE":
        approval = ApplicationApproved(
            application_id=cmd.application_id,
            approved_amount_usd=app.requested_amount,
            interest_rate=5.5,
            conditions=[],
            approved_by=cmd.reviewer_id,
            effective_date=datetime.utcnow().isoformat()
        )
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=[approval],
            expected_version=new_version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id
        )
        return new_version + 1
    else:
        decline = ApplicationDeclined(
            application_id=cmd.application_id,
            decline_reasons=["Not approved by human reviewer"],
            declined_by=cmd.reviewer_id,
            adverse_action_notice_required=True
        )
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=[decline],
            expected_version=new_version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id
        )
        return new_version + 1