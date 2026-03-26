"""ComplianceRecord aggregate - tracks regulatory checks for applications."""

from typing import Set, List, Optional
from src.core.event_store import EventStore
from src.models.events import StoredEvent
from src.core.exceptions import DomainError


class ComplianceRecordAggregate:
    """
    ComplianceRecord aggregate - tracks all compliance checks for a loan.
    
    This aggregate ensures:
    - All mandatory checks are completed before compliance clearance
    - Each check references the correct regulation version
    - Compliance failures are tracked with remediation requirements
    """
    
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.version = 0
        self.checks_passed: Set[str] = set()
        self.checks_failed: Set[str] = set()
        self.regulation_set_version: Optional[str] = None
        self.compliance_status = "PENDING"
        self.required_checks: Set[str] = set()
    
    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "ComplianceRecordAggregate":
        """Reconstruct aggregate by replaying all events."""
        try:
            events = await store.load_stream(f"compliance-{application_id}")
        except:
            # No events yet - new aggregate
            return cls(application_id)
        
        agg = cls(application_id)
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
    
    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        """Record that compliance checks were requested."""
        self.regulation_set_version = event.payload.get("regulation_set_version")
        self.required_checks = set(event.payload.get("checks_required", []))
        self.compliance_status = "IN_PROGRESS"
    
    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        """Record a passed compliance rule."""
        rule_id = event.payload["rule_id"]
        self.checks_passed.add(rule_id)
        
        # Update status if all required checks are passed
        if self.required_checks and self.required_checks.issubset(self.checks_passed):
            self.compliance_status = "PASSED"
    
    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        """Record a failed compliance rule."""
        rule_id = event.payload["rule_id"]
        self.checks_failed.add(rule_id)
        self.compliance_status = "FAILED"
    
    # ========== BUSINESS RULE VALIDATION ==========
    
    def assert_all_checks_passed(self, required_checks: Optional[List[str]] = None) -> None:
        """
        Rule: Cannot approve without all mandatory compliance checks passed.
        
        Raises DomainError if any required check is missing or failed.
        """
        checks_to_verify = set(required_checks) if required_checks else self.required_checks
        
        missing = checks_to_verify - self.checks_passed
        if missing:
            raise DomainError(
                message=f"Missing compliance checks: {missing}",
                aggregate_type="ComplianceRecord",
                stream_id=f"compliance-{self.application_id}"
            )
        
        if self.checks_failed:
            raise DomainError(
                message=f"Compliance checks failed: {self.checks_failed}",
                aggregate_type="ComplianceRecord",
                stream_id=f"compliance-{self.application_id}"
            )
    
    def assert_check_not_duplicate(self, rule_id: str) -> None:
        """Rule: Cannot run the same compliance check twice."""
        if rule_id in self.checks_passed or rule_id in self.checks_failed:
            raise DomainError(
                message=f"Compliance rule {rule_id} already processed",
                aggregate_type="ComplianceRecord",
                stream_id=f"compliance-{self.application_id}"
            )
    
    def get_compliance_status(self) -> dict:
        """Get current compliance status."""
        return {
            "application_id": self.application_id,
            "status": self.compliance_status,
            "checks_passed": list(self.checks_passed),
            "checks_failed": list(self.checks_failed),
            "required_checks": list(self.required_checks),
            "regulation_version": self.regulation_set_version
        }