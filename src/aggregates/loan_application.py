"""LoanApplication aggregate - enforces all business rules for a loan."""

from typing import Optional, List, Set
from src.core.event_store import EventStore
from src.models.events import StoredEvent
from src.core.exceptions import DomainError  # Import DomainError
from src.aggregates.state import ApplicationState, RiskTier


class LoanApplicationAggregate:
    """LoanApplication aggregate - consistency boundary for a single loan."""
    
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.version = 0
        self.state = None
        self.applicant_id = None
        self.requested_amount = None
        self.risk_tier = None
        self.credit_analysis_completed = False
        self.compliance_checks_passed = set()
        self.decision = None
        self.decision_confidence = None
        
    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        """Reconstruct aggregate by replaying all events."""
        events = await store.load_stream(f"loan-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
            agg.version = event.stream_position
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply an event to update aggregate state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
    
    # ========== EVENT HANDLERS ==========
    
    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = event.payload["applicant_id"]
        self.requested_amount = event.payload["requested_amount_usd"]
        
    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        self._assert_valid_transition(ApplicationState.AWAITING_ANALYSIS)
        self.state = ApplicationState.AWAITING_ANALYSIS
        
    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self._assert_valid_transition(ApplicationState.ANALYSIS_COMPLETE)
        self.state = ApplicationState.ANALYSIS_COMPLETE
        self.risk_tier = event.payload["risk_tier"]
        self.credit_analysis_completed = True
        
    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        rule_id = event.payload["rule_id"]
        self.compliance_checks_passed.add(rule_id)
        
    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        self._assert_valid_transition(ApplicationState.PENDING_DECISION)
        self.state = ApplicationState.PENDING_DECISION
        self.decision = event.payload["recommendation"]
        self.decision_confidence = event.payload["confidence_score"]
        
    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        if event.payload["final_decision"] == "APPROVE":
            self._assert_valid_transition(ApplicationState.FINAL_APPROVED)
            self.state = ApplicationState.FINAL_APPROVED
        else:
            self._assert_valid_transition(ApplicationState.FINAL_DECLINED)
            self.state = ApplicationState.FINAL_DECLINED
            
    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        self._assert_valid_transition(ApplicationState.FINAL_APPROVED)
        self.state = ApplicationState.FINAL_APPROVED
        
    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        self._assert_valid_transition(ApplicationState.FINAL_DECLINED)
        self.state = ApplicationState.FINAL_DECLINED
    
    # ========== BUSINESS RULE VALIDATION ==========
    
    def _assert_valid_transition(self, new_state: ApplicationState) -> None:
        """Enforce state machine rules."""
        valid_transitions = {
            None: [ApplicationState.SUBMITTED],
            ApplicationState.SUBMITTED: [ApplicationState.AWAITING_ANALYSIS],
            ApplicationState.AWAITING_ANALYSIS: [ApplicationState.ANALYSIS_COMPLETE],
            ApplicationState.ANALYSIS_COMPLETE: [ApplicationState.COMPLIANCE_REVIEW],
            ApplicationState.COMPLIANCE_REVIEW: [ApplicationState.PENDING_DECISION],
            ApplicationState.PENDING_DECISION: [
                ApplicationState.FINAL_APPROVED, 
                ApplicationState.FINAL_DECLINED,
                ApplicationState.APPROVED_PENDING_HUMAN,
                ApplicationState.DECLINED_PENDING_HUMAN
            ],
            ApplicationState.APPROVED_PENDING_HUMAN: [ApplicationState.FINAL_APPROVED],
            ApplicationState.DECLINED_PENDING_HUMAN: [ApplicationState.FINAL_DECLINED],
        }
        
        if new_state not in valid_transitions.get(self.state, []):
            raise DomainError(
                message=f"Invalid transition from {self.state} to {new_state}",
                aggregate_type="LoanApplication",
                stream_id=f"loan-{self.application_id}"
            )
    
    def assert_awaiting_credit_analysis(self) -> None:
        """Rule: Can only do credit analysis when in AWAITING_ANALYSIS state."""
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            raise DomainError(
                message=f"Cannot complete credit analysis in state {self.state}",
                aggregate_type="LoanApplication",
                stream_id=f"loan-{self.application_id}"
            )
    
    def assert_no_double_analysis(self) -> None:
        """Rule: Cannot do credit analysis twice."""
        if self.credit_analysis_completed:
            raise DomainError(
                message="Credit analysis already completed for this application",
                aggregate_type="LoanApplication",
                stream_id=f"loan-{self.application_id}"
            )
    
    def assert_compliance_complete(self, required_checks: List[str]) -> None:
        """Rule: Cannot approve without all compliance checks passed."""
        missing = set(required_checks) - self.compliance_checks_passed
        if missing:
            raise DomainError(
                message=f"Missing compliance checks: {missing}",
                aggregate_type="LoanApplication",
                stream_id=f"loan-{self.application_id}"
            )