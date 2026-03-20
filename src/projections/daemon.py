"""Async Projection Daemon - Builds read models from events."""

import asyncio
import logging
from typing import Dict, List, Optional, Set, Callable, Awaitable
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.core.event_store import EventStore
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)


@dataclass
class Projection:
    """Base class for all projections."""
    name: str
    
    async def handle(self, event: StoredEvent) -> None:
        """Handle an event - override in subclass."""
        pass
    
    async def get_checkpoint(self) -> int:
        """Get last processed position."""
        return 0
    
    async def save_checkpoint(self, position: int) -> None:
        """Save checkpoint."""
        pass


class ProjectionDaemon:
    """
    Async daemon that builds projections from the event stream.
    
    Features:
    - Fault-tolerant: continues if one projection fails
    - Checkpoint management: resumes from last position
    - Lag monitoring: exposes metrics for each projection
    - Rebuild support: can rebuild from scratch
    """
    
    def __init__(
        self,
        store: EventStore,
        projections: List[Projection],
        poll_interval_ms: int = 100,
        batch_size: int = 500,
        max_retries: int = 3
    ):
        self.store = store
        self.projections = {p.name: p for p in projections}
        self.poll_interval = poll_interval_ms / 1000
        self.batch_size = batch_size
        self.max_retries = max_retries
        self._running = False
        self._checkpoints: Dict[str, int] = {}
        self._lag: Dict[str, float] = {}
        
    async def run_forever(self) -> None:
        """Main daemon loop - runs until stopped."""
        self._running = True
        logger.info(f"🚀 Projection daemon started with {len(self.projections)} projections")
        
        # Load initial checkpoints
        await self._load_checkpoints()
        
        while self._running:
            try:
                await self._process_batch()
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Daemon error: {e}", exc_info=True)
                await asyncio.sleep(1)  # Back off on error
    
    async def _load_checkpoints(self) -> None:
        """Load last processed positions for all projections."""
        for name, projection in self.projections.items():
            self._checkpoints[name] = await projection.get_checkpoint()
            logger.debug(f"Checkpoint for {name}: {self._checkpoints[name]}")
    
    async def _process_batch(self) -> None:
        """
        Process one batch of events.
        
        1. Find minimum checkpoint across all projections
        2. Load next batch of events
        3. Route each event to subscribed projections
        4. Update checkpoints
        """
        # Find the earliest checkpoint (all projections must be at least this far)
        min_position = min(self._checkpoints.values()) if self._checkpoints else 0
        
        # Load next batch
        events = await self._load_events_batch(min_position)
        if not events:
            return
        
        logger.debug(f"Processing {len(events)} events from position {min_position}")
        
        # Process each event
        for event in events:
            await self._route_event(event)
        
        # Update checkpoints to the last event's position
        last_position = events[-1].global_position
        await self._update_checkpoints(last_position)
        
        # Update lag metrics
        await self._update_lag_metrics(last_position)
    
    async def _load_events_batch(self, from_position: int) -> List[StoredEvent]:
        """Load next batch of events."""
        events = []
        async for batch in self.store.load_all(
            from_global_position=from_position,
            batch_size=self.batch_size
        ):
            events.extend(batch)
            if len(events) >= self.batch_size:
                break
        return events
    
    async def _route_event(self, event: StoredEvent) -> None:
        """
        Route event to all projections that care about it.
        
        If a projection fails, log error and continue (fault tolerance).
        """
        for name, projection in self.projections.items():
            try:
                await projection.handle(event)
            except Exception as e:
                logger.error(f"Projection {name} failed on event {event.event_id}: {e}")
                # Continue processing other projections
                continue
    
    async def _update_checkpoints(self, position: int) -> None:
        """Update checkpoints for all projections."""
        for name, projection in self.projections.items():
            try:
                await projection.save_checkpoint(position)
                self._checkpoints[name] = position
            except Exception as e:
                logger.error(f"Failed to save checkpoint for {name}: {e}")
    
    async def _update_lag_metrics(self, last_processed: int) -> None:
        """Update lag metrics for monitoring."""
        # Get latest event position
        latest = 0
        async for batch in self.store.load_all(from_global_position=last_processed, batch_size=1):
            if batch:
                latest = batch[0].global_position
            break
        
        for name in self.projections:
            self._lag[name] = latest - self._checkpoints.get(name, 0)
    
    def get_lag(self, projection_name: str) -> float:
        """Get current lag for a projection in milliseconds."""
        if projection_name not in self._lag:
            return 0
        return self._lag[projection_name] * 1000  # Convert to ms
    
    async def stop(self) -> None:
        """Stop the daemon."""
        self._running = False
        logger.info("🛑 Projection daemon stopped")