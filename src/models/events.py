"""Event models for the Agentic Ledger."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, List
from uuid import UUID, uuid4


@dataclass
class BaseEvent:
    """
    Base class for all domain events.
    
    All events in the system inherit from this class.
    Events are facts that have happened - named in past tense.
    """
    # Metadata fields (populated by EventStore)
    event_id: Optional[UUID] = None
    stream_id: Optional[str] = None
    stream_position: Optional[int] = None
    global_position: Optional[int] = None
    recorded_at: Optional[datetime] = None
    
    # To be overridden by subclasses
    event_type: str = "BaseEvent"
    event_version: int = 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for storage."""
        # Start with all fields that aren't metadata
        result = {
            k: v for k, v in self.__dict__.items() 
            if k not in ['event_id', 'stream_id', 'stream_position', 
                        'global_position', 'recorded_at']
        }
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseEvent':
        """Create event from dictionary."""
        return cls(**data)


@dataclass
class StoredEvent:
    """
    An event as stored in the database.
    
    Wraps the domain event with storage metadata.
    This is what EventStore returns from load operations.
    """
    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    recorded_at: datetime
    
    domain_event: Optional[Any] = None
    
    @classmethod
    def from_row(cls, row: dict) -> 'StoredEvent':
        """Create from database row."""
        # Parse JSON fields if they're strings
        payload = row['payload']
        if isinstance(payload, str):
            payload = json.loads(payload)
            
        metadata = row['metadata']
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
            
        return cls(
            event_id=row['event_id'],
            stream_id=row['stream_id'],
            stream_position=row['stream_position'],
            global_position=row['global_position'],
            event_type=row['event_type'],
            event_version=row['event_version'],
            payload=payload,
            metadata=metadata,
            recorded_at=row['recorded_at']
        )


@dataclass
class StreamMetadata:
    """Metadata for a stream."""
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: Optional[datetime]
    metadata: Dict[str, Any]


class ApplicationSubmitted(BaseEvent):
    event_type = "ApplicationSubmitted"
    event_version = 1
    
    def __init__(self, application_id: str, applicant_id: str, requested_amount_usd: float,
                 loan_purpose: str, submission_channel: str, submitted_at: str):
        super().__init__()
        self.application_id = application_id
        self.applicant_id = applicant_id
        self.requested_amount_usd = requested_amount_usd
        self.loan_purpose = loan_purpose
        self.submission_channel = submission_channel
        self.submitted_at = submitted_at


class CreditAnalysisRequested(BaseEvent):
    event_type = "CreditAnalysisRequested"
    event_version = 1
    
    def __init__(self, application_id: str, assigned_agent_id: str, requested_at: str, priority: int):
        super().__init__()
        self.application_id = application_id
        self.assigned_agent_id = assigned_agent_id
        self.requested_at = requested_at
        self.priority = priority


class CreditAnalysisCompleted(BaseEvent):
    event_type = "CreditAnalysisCompleted"
    event_version = 2
    
    def __init__(self, application_id: str, agent_id: str, session_id: str,
                 model_version: str, confidence_score: float, risk_tier: str,
                 recommended_limit_usd: float, analysis_duration_ms: int, input_data_hash: str):
        super().__init__()
        self.application_id = application_id
        self.agent_id = agent_id
        self.session_id = session_id
        self.model_version = model_version
        self.confidence_score = confidence_score
        self.risk_tier = risk_tier
        self.recommended_limit_usd = recommended_limit_usd
        self.analysis_duration_ms = analysis_duration_ms
        self.input_data_hash = input_data_hash


class AgentContextLoaded(BaseEvent):
    event_type = "AgentContextLoaded"
    event_version = 1
    
    def __init__(self, agent_id: str, session_id: str, context_source: str,
                 event_replay_from_position: int, context_token_count: int, model_version: str):
        super().__init__()
        self.agent_id = agent_id
        self.session_id = session_id
        self.context_source = context_source
        self.event_replay_from_position = event_replay_from_position
        self.context_token_count = context_token_count
        self.model_version = model_version