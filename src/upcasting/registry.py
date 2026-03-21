"""Upcaster Registry for schema evolution."""

import logging
from typing import Dict, Callable, Tuple, Any
from dataclasses import replace

from src.models.events import StoredEvent

logger = logging.getLogger(__name__)


class UpcasterRegistry:
    """
    Registry for event upcasters.
    
    Upcasters transform old event schemas to new versions at read time,
    without modifying the stored events.
    """
    
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable[[dict], dict]] = {}
    
    def register(self, event_type: str, from_version: int):
        """Decorator to register an upcaster function."""
        def decorator(fn: Callable[[dict], dict]) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            logger.debug(f"Registered upcaster for {event_type} v{from_version}")
            return fn
        return decorator
    
    def upcast(self, event: StoredEvent) -> StoredEvent:
        """
        Apply all registered upcasters for this event type in version order.
        """
        current = event
        v = event.event_version
        
        # Apply upcasters in sequence
        while (event.event_type, v) in self._upcasters:
            upcaster = self._upcasters[(event.event_type, v)]
            new_payload = upcaster(current.payload)
            
            # Create new StoredEvent with updated payload and version
            current = StoredEvent(
                event_id=current.event_id,
                stream_id=current.stream_id,
                stream_position=current.stream_position,
                global_position=current.global_position,
                event_type=current.event_type,
                event_version=v + 1,
                payload=new_payload,
                metadata=current.metadata,
                recorded_at=current.recorded_at
            )
            v += 1
        
        return current
    
    def has_upcaster(self, event_type: str, version: int) -> bool:
        """Check if an upcaster exists."""
        return (event_type, version) in self._upcasters