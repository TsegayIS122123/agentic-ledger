"""AuditLedger aggregate - cross-cutting audit trail with causal chains."""

from typing import List, Dict, Any, Optional
from src.core.event_store import EventStore
from src.models.events import StoredEvent
from src.core.exceptions import DomainError


class AuditLedgerAggregate:
    """
    AuditLedger aggregate - maintains cross-cutting audit trail.
    
    This aggregate:
    - Tracks all events across streams for a business entity
    - Maintains causal chains via correlation_id and causation_id
    - Enforces append-only semantics
    - Provides integrity verification
    """
    
    def __init__(self, entity_type: str, entity_id: str):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.version = 0
        self.events: List[StoredEvent] = []
        self.causal_chain: Dict[str, List[str]] = {}  # event_id -> list of child events
        self.last_integrity_hash: Optional[str] = None
    
    @classmethod
    async def load(cls, store: EventStore, entity_type: str, entity_id: str) -> "AuditLedgerAggregate":
        """Reconstruct aggregate by replaying all events."""
        stream_id = f"audit-{entity_type}-{entity_id}"
        try:
            events = await store.load_stream(stream_id)
        except:
            # No events yet - new aggregate
            return cls(entity_type, entity_id)
        
        agg = cls(entity_type, entity_id)
        for event in events:
            agg._apply(event)
            agg.version = event.stream_position
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply an event to update aggregate state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        else:
            # Store all events for audit trail
            self.events.append(event)
    
    # ========== EVENT HANDLERS ==========
    
    def _on_AuditIntegrityCheckRun(self, event: StoredEvent) -> None:
        """Record an integrity check run."""
        self.last_integrity_hash = event.payload.get("integrity_hash")
        self.events.append(event)
    
    def _on_any_event(self, event: StoredEvent) -> None:
        """Track causal relationships for any event."""
        # Build causal chain
        causation_id = event.metadata.get("causation_id")
        if causation_id:
            if causation_id not in self.causal_chain:
                self.causal_chain[causation_id] = []
            self.causal_chain[causation_id].append(str(event.event_id))
        
        # Store the event
        self.events.append(event)
    
    # ========== BUSINESS RULE VALIDATION ==========
    
    def assert_append_only(self, event: StoredEvent) -> None:
        """
        Rule: No events may be removed or modified.
        
        This is enforced by the event store itself, but we validate
        that we're only adding new events.
        """
        # Check if this event ID already exists
        for existing in self.events:
            if existing.event_id == event.event_id:
                raise DomainError(
                    message=f"Event {event.event_id} already exists in audit trail",
                    aggregate_type="AuditLedger",
                    stream_id=f"audit-{self.entity_type}-{self.entity_id}"
                )
    
    def assert_causal_chain_valid(self, event: StoredEvent) -> None:
        """
        Rule: Causation_id must reference an existing event.
        
        Ensures causal integrity across the audit trail.
        """
        causation_id = event.metadata.get("causation_id")
        if causation_id:
            # Check if the referenced event exists
            found = False
            for existing in self.events:
                if str(existing.event_id) == causation_id:
                    found = True
                    break
            if not found:
                raise DomainError(
                    message=f"Causation_id {causation_id} references non-existent event",
                    aggregate_type="AuditLedger",
                    stream_id=f"audit-{self.entity_type}-{self.entity_id}"
                )
    
    def get_audit_trail(self) -> Dict[str, Any]:
        """Get the complete audit trail for this entity."""
        return {
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "total_events": len(self.events),
            "events": [
                {
                    "event_id": str(e.event_id),
                    "event_type": e.event_type,
                    "stream_id": e.stream_id,
                    "stream_position": e.stream_position,
                    "global_position": e.global_position,
                    "payload": e.payload,
                    "metadata": e.metadata,
                    "recorded_at": e.recorded_at.isoformat()
                }
                for e in self.events
            ],
            "causal_chain": self.causal_chain,
            "last_integrity_hash": self.last_integrity_hash
        }
    
    def get_causal_chain(self, event_id: str) -> List[str]:
        """Get all events caused by a specific event."""
        return self.causal_chain.get(event_id, [])