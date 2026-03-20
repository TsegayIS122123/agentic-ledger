"""Upcaster Registry for schema evolution."""

import logging
from typing import Dict, Callable, Tuple, Any
from dataclasses import dataclass, replace

from src.models.events import StoredEvent

logger = logging.getLogger(__name__)


class UpcasterRegistry:
    """
    Registry for event upcasters.
    
    Upcasters transform old event schemas to new versions at read time,
    without modifying the stored events. This maintains immutability
    while allowing schema evolution.
    
    The registry applies upcasters in version order automatically:
    v1 → v2 → v3 → ...
    """
    
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable[[dict], dict]] = {}
    
    def register(self, event_type: str, from_version: int):
        """
        Decorator to register an upcaster function.
        
        Usage:
            @registry.register("CreditAnalysisCompleted", from_version=1)
            def upcast_v1_to_v2(payload):
                # transform payload
                return new_payload
        """
        def decorator(fn: Callable[[dict], dict]) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            logger.debug(f"Registered upcaster for {event_type} v{from_version}")
            return fn
        return decorator
    
    def upcast(self, event: StoredEvent) -> StoredEvent:
        """
        Apply all registered upcasters for this event type in version order.
        
        This is called transparently during event loading - callers never
        need to invoke upcasters manually.
        """
        current = event
        v = event.event_version
        
        # Apply upcasters in sequence until no more are registered
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
                event_version=v + 1,  # Increment version
                payload=new_payload,
                metadata=current.metadata,
                recorded_at=current.recorded_at
            )
            v += 1
            logger.debug(f"Upcasted {event.event_type} to v{v}")
        
        return current
    
    def has_upcaster(self, event_type: str, version: int) -> bool:
        """Check if an upcaster exists for this event type and version."""
        return (event_type, version) in self._upcasters