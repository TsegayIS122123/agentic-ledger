"""Custom exceptions for the Agentic Ledger."""

from typing import Any, Optional


class AgenticLedgerError(Exception):
    """Base exception for all ledger errors."""
    pass


class OptimisticConcurrencyError(AgenticLedgerError):
    """
    Raised when an append operation fails due to concurrent modification.
    
    Two agents tried to append to the same stream with the same expected_version.
    The caller must reload the stream and retry.
    """
    
    def __init__(
        self,
        stream_id: str,
        expected_version: int,
        actual_version: int,
        message: Optional[str] = None
    ):
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        self.message = message or (
            f"Stream {stream_id} version mismatch: "
            f"expected {expected_version}, actual {actual_version}"
        )
        super().__init__(self.message)
        
    def to_dict(self) -> dict[str, Any]:
        """Convert to structured error for LLM consumption."""
        return {
            "error_type": "OptimisticConcurrencyError",
            "stream_id": self.stream_id,
            "expected_version": self.expected_version,
            "actual_version": self.actual_version,
            "message": self.message,
            "suggested_action": "reload_stream_and_retry"
        }


class DomainError(AgenticLedgerError):
    """
    Raised when a business rule is violated.
    
    The command attempted an operation that violates aggregate invariants.
    """
    
    def __init__(
        self,
        message: str,
        aggregate_type: str,
        stream_id: str,
        current_state: Optional[dict] = None
    ):
        self.aggregate_type = aggregate_type
        self.stream_id = stream_id
        self.current_state = current_state
        super().__init__(message)
        
    def to_dict(self) -> dict[str, Any]:
        """Convert to structured error for LLM consumption."""
        return {
            "error_type": "DomainError",
            "aggregate_type": self.aggregate_type,
            "stream_id": self.stream_id,
            "message": str(self),
            "suggested_action": "check_business_rules_and_retry"
        }


class StreamNotFoundError(AgenticLedgerError):
    """Raised when attempting to load a non-existent stream."""
    
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream not found: {stream_id}")


class InvalidEventError(AgenticLedgerError):
    """Raised when an event fails validation."""
    
    def __init__(self, message: str, event_type: str, validation_errors: dict):
        self.event_type = event_type
        self.validation_errors = validation_errors
        super().__init__(f"Invalid event {event_type}: {message}")


class UpcasterNotFoundError(AgenticLedgerError):
    """Raised when no upcaster is registered for an event version."""
    
    def __init__(self, event_type: str, version: int):
        self.event_type = event_type
        self.version = version
        super().__init__(
            f"No upcaster registered for {event_type} v{version}"
        )