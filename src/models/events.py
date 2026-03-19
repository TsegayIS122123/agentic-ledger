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