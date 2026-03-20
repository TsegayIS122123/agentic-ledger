"""ComplianceAuditView - regulatory read model with temporal queries."""

import asyncpg
from datetime import datetime
from typing import Optional, Dict, Any, List
from src.projections.daemon import Projection
from src.models.events import StoredEvent
import logging

logger = logging.getLogger(__name__)


class ComplianceAuditProjection(Projection):
    """
    Regulatory compliance view with temporal query support.
    
    Features:
    - Complete compliance history
    - Time-travel queries (state at any timestamp)
    - Rebuildable from scratch
    - Snapshot strategy for performance
    """
    
    def __init__(self, pool: asyncpg.Pool, snapshot_interval: int = 100):
        self.name = "compliance_audit"
        self.pool = pool
        self.snapshot_interval = snapshot_interval
        self.event_count = 0
    
    async def handle(self, event: StoredEvent) -> None:
        """Process compliance-related events."""
        
        if event.event_type in ["ComplianceRulePassed", "ComplianceRuleFailed"]:
            await self._record_compliance_check(event)
            self.event_count += 1
            
            # Take snapshot periodically
            if self.event_count % self.snapshot_interval == 0:
                await self._take_snapshot(event)
    
    async def _record_compliance_check(self, event: StoredEvent) -> None:
        """Record a compliance check result."""
        passed = event.event_type == "ComplianceRulePassed"
        
        await self.pool.execute("""
            INSERT INTO compliance_audit_view (
                application_id, check_timestamp, rule_id, rule_version,
                passed, evidence_hash, regulation_set
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
            event.payload["application_id"],
            event.recorded_at,
            event.payload["rule_id"],
            event.payload.get("rule_version", "1.0"),
            passed,
            event.payload.get("evidence_hash", ""),
            event.payload.get("regulation_set", "BASEL_III")
        )
    
    async def _take_snapshot(self, event: StoredEvent) -> None:
        """Take a snapshot for faster temporal queries."""
        # Get all compliance records for this application
        rows = await self.pool.fetch("""
            SELECT * FROM compliance_audit_view
            WHERE application_id = $1
            ORDER BY check_timestamp
        """, event.payload["application_id"])
        
        # Store snapshot
        await self.pool.execute("""
            INSERT INTO compliance_snapshots (
                application_id, snapshot_timestamp, snapshot_data, event_position
            ) VALUES ($1, $2, $3::jsonb, $4)
        """,
            event.payload["application_id"],
            event.recorded_at,
            {"checks": [dict(r) for r in rows]},
            event.global_position
        )
    
    async def get_current_compliance(self, application_id: str) -> Dict[str, Any]:
        """Get current compliance state for an application."""
        rows = await self.pool.fetch("""
            SELECT * FROM compliance_audit_view
            WHERE application_id = $1
            ORDER BY check_timestamp DESC
        """, application_id)
        
        return {
            "application_id": application_id,
            "checks": [dict(r) for r in rows],
            "all_passed": all(r['passed'] for r in rows) if rows else False,
            "checked_at": datetime.utcnow().isoformat()
        }
    
    async def get_compliance_at(
        self, 
        application_id: str, 
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Get compliance state as it existed at a specific moment.
        
        Uses snapshots for efficiency, then replays events after snapshot.
        """
        # Find the closest snapshot before timestamp
        snapshot = await self.pool.fetchrow("""
            SELECT * FROM compliance_snapshots
            WHERE application_id = $1 AND snapshot_timestamp <= $2
            ORDER BY snapshot_timestamp DESC
            LIMIT 1
        """, application_id, timestamp)
        
        if snapshot:
            # Start from snapshot
            base_state = snapshot['snapshot_data']
            from_time = snapshot['snapshot_timestamp']
        else:
            # No snapshot - start from beginning
            base_state = {"checks": []}
            from_time = datetime.min
        
        # Get events after snapshot up to target timestamp
        rows = await self.pool.fetch("""
            SELECT * FROM compliance_audit_view
            WHERE application_id = $1 
              AND check_timestamp > $2
              AND check_timestamp <= $3
            ORDER BY check_timestamp
        """, application_id, from_time, timestamp)
        
        # Combine snapshot with events
        checks = base_state.get("checks", [])
        for row in rows:
            checks.append(dict(row))
        
        return {
            "application_id": application_id,
            "as_of": timestamp.isoformat(),
            "checks": checks,
            "from_snapshot": bool(snapshot)
        }
    
    async def rebuild_from_scratch(self) -> None:
        """Rebuild entire projection by replaying all events."""
        logger.info("🔄 Rebuilding compliance audit from scratch...")
        
        # Truncate existing tables
        await self.pool.execute("TRUNCATE compliance_audit_view, compliance_snapshots")
        
        # Reset checkpoint
        await self.save_checkpoint(0)
        self.event_count = 0
        
        logger.info("✅ Rebuild complete")
    
    async def get_checkpoint(self) -> int:
        """Get last processed position."""
        row = await self.pool.fetchrow("""
            SELECT last_position FROM projection_checkpoints
            WHERE projection_name = 'compliance_audit'
        """)
        return row['last_position'] if row else 0
    
    async def save_checkpoint(self, position: int) -> None:
        """Save checkpoint."""
        await self.pool.execute("""
            INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
            VALUES ('compliance_audit', $1, NOW())
            ON CONFLICT (projection_name) DO UPDATE SET
                last_position = EXCLUDED.last_position,
                updated_at = NOW()
        """, position)
    
    async def get_projection_lag(self) -> float:
        """Get current lag in milliseconds."""
        # Get latest event position
        latest = await self.pool.fetchval("SELECT MAX(global_position) FROM events")
        checkpoint = await self.get_checkpoint()
        
        return (latest - checkpoint) * 1000  # Approximate ms