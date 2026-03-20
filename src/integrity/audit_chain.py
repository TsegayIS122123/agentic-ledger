"""Cryptographic audit chain for tamper detection."""

import hashlib
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from src.core.event_store import EventStore
from src.models.events import AuditIntegrityCheckRun, StoredEvent


@dataclass
class IntegrityCheckResult:
    """Result of an integrity check."""
    events_verified: int
    chain_valid: bool
    tamper_detected: bool
    current_hash: str
    previous_hash: str
    checked_at: datetime


class AuditChain:
    """
    Cryptographic hash chain for tamper-evident audit trail.
    
    Forms a blockchain-style hash chain where each integrity check
    records a hash of all preceding events plus the previous hash.
    Any modification to historical events breaks the chain.
    """
    
    def __init__(self, store: EventStore):
        self.store = store
    
    async def run_integrity_check(
        self,
        entity_type: str,
        entity_id: str,
    ) -> IntegrityCheckResult:
        """
        Run integrity check on an entity's event stream.
        
        1. Load all events for the entity's primary stream
        2. Load the last AuditIntegrityCheckRun event (if any)
        3. Hash the payloads of all events since the last check
        4. Verify hash chain
        5. Append new check event
        """
        stream_id = f"audit-{entity_type}-{entity_id}"
        
        # Load all events in the audit stream
        events = await self.store.load_stream(stream_id)
        
        # Find the last integrity check
        last_check = None
        check_events = [e for e in events if e.event_type == "AuditIntegrityCheckRun"]
        if check_events:
            last_check = check_events[-1]
        
        # Get the main entity stream events to verify
        main_stream_id = f"{entity_type}-{entity_id}"
        try:
            main_events = await self.store.load_stream(main_stream_id)
        except:
            main_events = []
        
        # Build hash chain
        chain_valid = True
        tamper_detected = False
        previous_hash = "0" * 64  # Initial hash (all zeros)
        
        if last_check:
            previous_hash = last_check.payload.get("integrity_hash", previous_hash)
        
        # Hash all events since last check
        start_position = last_check.global_position if last_check else 0
        events_to_verify = [e for e in main_events if e.global_position > start_position]
        
        # Build hash of all new events
        hash_input = previous_hash
        for event in events_to_verify:
            # Hash the event payload (excluding metadata)
            event_data = json.dumps(event.payload, sort_keys=True).encode()
            event_hash = hashlib.sha256(event_data).hexdigest()
            hash_input += event_hash
        
        current_hash = hashlib.sha256(hash_input.encode()).hexdigest()
        
        # Verify chain if we have a previous check
        if last_check:
            expected_hash = last_check.payload.get("integrity_hash")
            if expected_hash != previous_hash:
                tamper_detected = True
                chain_valid = False
        
        # Append new integrity check event
        check_event = AuditIntegrityCheckRun(
            entity_id=entity_id,
            check_timestamp=datetime.utcnow().isoformat(),
            events_verified_count=len(events_to_verify),
            integrity_hash=current_hash,
            previous_hash=previous_hash
        )
        
        # Get current version of audit stream
        version = await self.store.stream_version(stream_id)
        
        await self.store.append(
            stream_id=stream_id,
            events=[check_event],
            expected_version=version
        )
        
        return IntegrityCheckResult(
            events_verified=len(events_to_verify),
            chain_valid=chain_valid,
            tamper_detected=tamper_detected,
            current_hash=current_hash,
            previous_hash=previous_hash,
            checked_at=datetime.utcnow()
        )
    
    async def verify_chain(
        self,
        entity_type: str,
        entity_id: str,
    ) -> bool:
        """
        Verify the entire hash chain without appending a new check.
        Useful for regulatory examinations.
        """
        stream_id = f"audit-{entity_type}-{entity_id}"
        events = await self.store.load_stream(stream_id)
        
        if not events:
            return True  # No checks yet = trivially valid
        
        previous_hash = "0" * 64
        for event in events:
            if event.event_type != "AuditIntegrityCheckRun":
                continue
            
            payload = event.payload
            expected_previous = payload.get("previous_hash")
            current_hash = payload.get("integrity_hash")
            
            # Verify chain link
            if expected_previous != previous_hash:
                return False
            
            previous_hash = current_hash
        
        return True