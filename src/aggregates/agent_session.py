"""AgentSession aggregate - tracks one AI agent's work session."""

from typing import Optional, Set
from src.core.event_store import EventStore
from src.models.events import StoredEvent
from src.core.exceptions import DomainError  # Import DomainError


class AgentSessionAggregate:
    """AgentSession aggregate - consistency boundary for one agent session."""
    
    def __init__(self, agent_id: str, session_id: str):
        self.agent_id = agent_id
        self.session_id = session_id
        self.version = 0
        self.context_loaded = False
        self.model_version = None
        self.context_source = None
        self.token_count = 0
        self.completed_actions = set()
        self.last_action_position = 0
        
    @classmethod
    async def load(cls, store: EventStore, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        """Reconstruct agent session by replaying all events."""
        events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in events:
            agg._apply(event)
            agg.version = event.stream_position
            agg.last_action_position = event.stream_position
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply event to update state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
    
    # ========== EVENT HANDLERS ==========
    
    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        """Handle AgentContextLoaded - MUST be first event."""
        self.context_loaded = True
        self.model_version = event.payload["model_version"]
        self.context_source = event.payload["context_source"]
        self.token_count = event.payload["context_token_count"]
        
    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        """Handle CreditAnalysisCompleted."""
        self.completed_actions.add(f"credit_analysis_{event.payload['application_id']}")
        
    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        """Handle FraudScreeningCompleted."""
        self.completed_actions.add(f"fraud_screening_{event.payload['application_id']}")
    
    # ========== BUSINESS RULE VALIDATION ==========
    
    def assert_context_loaded(self) -> None:
        """Gas Town pattern: Cannot make decisions without context loaded."""
        if not self.context_loaded:
            raise DomainError(
                message=f"Agent session {self.agent_id}/{self.session_id} has no context loaded",
                aggregate_type="AgentSession",
                stream_id=f"agent-{self.agent_id}-{self.session_id}"
            )
    
    def assert_model_version_current(self, expected_version: str) -> None:
        """Rule: Agent must use the model version it was loaded with."""
        if self.model_version != expected_version:
            raise DomainError(
                message=f"Expected model version {self.model_version}, got {expected_version}",
                aggregate_type="AgentSession",
                stream_id=f"agent-{self.agent_id}-{self.session_id}"
            )