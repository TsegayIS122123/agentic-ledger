"""Gas Town pattern - agent memory reconstruction from event stream."""

from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from src.core.event_store import EventStore
from src.models.events import StoredEvent


@dataclass
class AgentContext:
    """Reconstructed agent context after crash."""
    agent_id: str
    session_id: str
    context_text: str
    last_event_position: int
    pending_work: List[Dict[str, Any]] = field(default_factory=list)
    session_health_status: str = "HEALTHY"
    token_count: int = 0
    reconstructed_at: datetime = field(default_factory=datetime.utcnow)


class GasTownMemory:
    """
    Gas Town pattern - persistent agent memory via event replay.
    
    An AI agent that crashes can reconstruct its exact context
    by replaying its event stream from the event store.
    """
    
    def __init__(self, store: EventStore):
        self.store = store
    
    async def reconstruct_agent_context(
        self,
        agent_id: str,
        session_id: str,
        token_budget: int = 8000,
    ) -> AgentContext:
        """
        Reconstruct agent context from event stream after crash.
        """
        stream_id = f"agent-{agent_id}-{session_id}"
        
        try:
            events = await self.store.load_stream(stream_id)
        except:
            return AgentContext(
                agent_id=agent_id,
                session_id=session_id,
                context_text="New session - no history",
                last_event_position=0,
                pending_work=[],
                session_health_status="NEW"
            )
        
        if not events:
            return AgentContext(
                agent_id=agent_id,
                session_id=session_id,
                context_text="Empty session",
                last_event_position=0,
                pending_work=[],
                session_health_status="EMPTY"
            )
        
        # Analyze events
        context_parts = []
        pending_work = []
        token_estimate = 0
        health_status = "HEALTHY"
        
        # Find the last event
        last_event = events[-1]
        last_position = last_event.stream_position
        
        # Check for partial/incomplete actions
        if self._is_partial_action(last_event):
            health_status = "NEEDS_RECONCILIATION"
            pending_work.append(self._extract_pending_work(last_event))
        
        # Process events for summarization
        # Keep last 3 events verbatim
        verbatim_events = events[-3:] if len(events) >= 3 else events
        
        # Summarize older events
        if len(events) > 3:
            old_events = events[:-3]
            summary = self._summarize_events(old_events)
            context_parts.append(summary)
            token_estimate += len(summary) // 4
        
        # Add verbatim recent events with proper event text
        for event in verbatim_events:
            event_text = self._event_to_text(event)
            context_parts.append(event_text)
            token_estimate += len(event_text) // 4
        
        # Add session metadata
        context_parts.append(f"\nSession: {agent_id}/{session_id}")
        context_parts.append(f"Total events: {len(events)}")
        context_parts.append(f"Last position: {last_position}")
        context_parts.append(f"Status: {health_status}")
        
        if pending_work:
            context_parts.append("\nPending work:")
            for work in pending_work:
                context_parts.append(f"  - {work['description']}")
        
        context_text = "\n".join(context_parts)
        
        # Truncate if over token budget
        if token_estimate > token_budget:
            context_text = self._truncate_to_budget(context_text, token_budget)
        
        return AgentContext(
            agent_id=agent_id,
            session_id=session_id,
            context_text=context_text,
            last_event_position=last_position,
            pending_work=pending_work,
            session_health_status=health_status,
            token_count=token_estimate
        )
    
    def _is_partial_action(self, event: StoredEvent) -> bool:
        """Check if an event represents a partial/incomplete action."""
        # Check if event type indicates a request without completion
        partial_types = ["CreditAnalysisRequested"]
        return event.event_type in partial_types
    
    def _extract_pending_work(self, event: StoredEvent) -> Dict[str, Any]:
        """Extract pending work information from an event."""
        payload = event.payload
        return {
            "type": event.event_type,
            "application_id": payload.get("application_id", "unknown"),
            "description": f"{event.event_type} for {payload.get('application_id', 'unknown')}"
        }
    
    def _summarize_events(self, events: List[StoredEvent]) -> str:
        """Summarize a list of old events."""
        if not events:
            return ""
        
        # Group by event type
        counts = {}
        for event in events:
            event_name = event.event_type
            counts[event_name] = counts.get(event_name, 0) + 1
        
        summary = f"[Summary: {len(events)} earlier events - "
        summary += ", ".join([f"{count} × {etype}" for etype, count in counts.items()])
        summary += "]"
        
        return summary
    
    def _event_to_text(self, event: StoredEvent) -> str:
        """Convert an event to human-readable text with proper display names."""
        payload = event.payload
        
        # Map event types to readable descriptions
        if event.event_type == "AgentContextLoaded":
            return f"🟢 Session started with model {payload.get('model_version', 'unknown')}, context from {payload.get('context_source', 'unknown')}"
        
        elif event.event_type == "CreditAnalysisCompleted":
            return f"📊 Credit analysis: risk_tier={payload.get('risk_tier', 'unknown')}, confidence={payload.get('confidence_score', 'N/A')}"
        
        elif event.event_type == "FraudScreeningCompleted":
            return f"🔍 Fraud screening: score={payload.get('fraud_score', 'N/A')}"
        
        elif event.event_type == "DecisionGenerated":
            return f"🎯 Decision: {payload.get('recommendation', 'unknown')} (confidence: {payload.get('confidence_score', 'N/A')})"
        
        else:
            # Generic fallback
            event_name = event.event_type
            key_fields = []
            if 'application_id' in payload:
                key_fields.append(f"app={payload['application_id']}")
            if 'agent_id' in payload:
                key_fields.append(f"agent={payload['agent_id']}")
            if 'risk_tier' in payload:
                key_fields.append(f"risk={payload['risk_tier']}")
            
            if key_fields:
                return f"📝 {event_name}: {', '.join(key_fields)}"
            return f"📝 {event_name}"
    
    def _truncate_to_budget(self, text: str, budget: int) -> str:
        """Truncate text to fit within token budget."""
        # Rough approximation: 1 token ≈ 4 characters
        max_chars = budget * 4
        
        if len(text) <= max_chars:
            return text
        
        # Keep first and last parts
        keep_chars = max_chars
        first_part = text[:keep_chars // 2]
        last_part = text[-(keep_chars // 2):]
        
        return f"{first_part}\n... [truncated to fit token budget] ...\n{last_part}"