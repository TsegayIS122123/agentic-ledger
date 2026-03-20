"""ApplicationSummary projection - current state of all loans."""

import asyncpg
from datetime import datetime
from typing import Optional
from src.projections.daemon import Projection
from src.models.events import StoredEvent


class ApplicationSummaryProjection(Projection):
    """
    Maintains current state of all loan applications.
    
    One row per application, updated as events arrive.
    SLO: <500ms lag
    """
    
    def __init__(self, pool: asyncpg.Pool):
        self.name = "application_summary"
        self.pool = pool
    
    async def handle(self, event: StoredEvent) -> None:
        """Update application summary based on event."""
        print(f"\n🔵 ApplicationSummary handling event: {event.event_type}")
        print(f"   Payload: {event.payload}")
        
        if event.event_type == "ApplicationSubmitted":
            await self._handle_submitted(event)
        else:
            print(f"   ⚠️ Unhandled event type: {event.event_type}")
    
    async def _handle_submitted(self, event: StoredEvent) -> None:
        """Insert new row for submitted application."""
        print(f"   📝 Inserting application: {event.payload['application_id']}")
        
        try:
            result = await self.pool.execute("""
                INSERT INTO application_summary (
                    application_id, state, applicant_id, requested_amount_usd,
                    last_event_type, last_event_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (application_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    applicant_id = EXCLUDED.applicant_id,
                    requested_amount_usd = EXCLUDED.requested_amount_usd,
                    last_event_type = EXCLUDED.last_event_type,
                    last_event_at = EXCLUDED.last_event_at
            """,
                event.payload["application_id"],
                "SUBMITTED",
                event.payload["applicant_id"],
                event.payload["requested_amount_usd"],
                event.event_type,
                event.recorded_at
            )
            print(f"   ✅ Insert result: {result}")
        except Exception as e:
            print(f"   ❌ Insert error: {e}")
            raise
    
    async def get_checkpoint(self) -> int:
        """Get last processed position."""
        row = await self.pool.fetchrow("""
            SELECT last_position FROM projection_checkpoints
            WHERE projection_name = 'application_summary'
        """)
        return row['last_position'] if row else 0
    
    async def save_checkpoint(self, position: int) -> None:
        """Save checkpoint."""
        await self.pool.execute("""
            INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
            VALUES ('application_summary', $1, NOW())
            ON CONFLICT (projection_name) DO UPDATE SET
                last_position = EXCLUDED.last_position,
                updated_at = NOW()
        """, position)