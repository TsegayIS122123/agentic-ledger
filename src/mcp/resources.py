"""MCP Resources - Query side for AI agents."""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from src.core.event_store import EventStore
from src.core.exceptions import StreamNotFoundError
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.integrity.gas_town import GasTownMemory

logger = logging.getLogger(__name__)


class MCPResources:
    """
    MCP Resources - Query interface for AI agents.
    
    All resources read from projections (not event store) for performance.
    Exceptions: audit-trail and sessions are direct stream loads (justified).
    """
    
    def __init__(self, store: EventStore, pool):
        self.store = store
        self.pool = pool
        self.app_summary = ApplicationSummaryProjection(pool)
        self.agent_perf = AgentPerformanceProjection(pool)
        self.compliance = ComplianceAuditProjection(pool)
        self.gas_town = GasTownMemory(store)
    
    # ========== RESOURCE 1: ledger://applications/{id} ==========
    async def get_application_summary(self, app_id: str) -> Dict[str, Any]:
        """
        Get current application state.
        
        SLO: p99 < 50ms
        Source: ApplicationSummary projection
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM application_summary
                WHERE application_id = $1
            """, app_id)
            
            if not row:
                return {"error": "Application not found"}
            return dict(row)
    
    # ========== RESOURCE 2: ledger://applications/{id}/compliance ==========
    async def get_compliance_view(self, app_id: str, as_of: Optional[str] = None) -> Dict[str, Any]:
        """
        Get compliance view with optional temporal query.
        
        SLO: p99 < 200ms
        Source: ComplianceAuditView projection
        Supports: ?as_of=timestamp for time travel
        """
        if as_of:
            try:
                timestamp = datetime.fromisoformat(as_of.replace('Z', '+00:00'))
                return await self.compliance.get_compliance_at(app_id, timestamp)
            except ValueError:
                return {"error": "Invalid timestamp format. Use ISO format: 2026-03-22T10:00:00Z"}
        return await self.compliance.get_current_compliance(app_id)
    
    # ========== RESOURCE 3: ledger://applications/{id}/audit-trail ==========
    async def get_audit_trail(
        self, 
        app_id: str, 
        from_pos: Optional[int] = None, 
        to_pos: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get complete audit trail (direct stream load - justified exception).
        
        SLO: p99 < 500ms
        Justification: Audit trails need complete history, not just current state.
        """
        try:
            events = await self.store.load_stream(f"loan-{app_id}", 
                                                   from_position=from_pos or 0,
                                                   to_position=to_pos)
            
            return {
                "application_id": app_id,
                "events": [
                    {
                        "type": e.event_type,
                        "position": e.stream_position,
                        "global_position": e.global_position,
                        "payload": e.payload,
                        "metadata": e.metadata,
                        "recorded_at": e.recorded_at.isoformat()
                    }
                    for e in events
                ],
                "total_events": len(events)
            }
        except StreamNotFoundError:
            return {"error": "Application not found"}
    
    # ========== RESOURCE 4: ledger://agents/{id}/performance ==========
    async def get_agent_performance(self, agent_id: str) -> Dict[str, Any]:
        """
        Get agent performance metrics by model version.
        
        SLO: p99 < 50ms
        Source: AgentPerformanceLedger projection
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM agent_performance_ledger
                WHERE agent_id = $1
                ORDER BY model_version
            """, agent_id)
            
            return {
                "agent_id": agent_id,
                "versions": [dict(r) for r in rows],
                "total_versions": len(rows)
            }
    
    # ========== RESOURCE 5: ledger://agents/{id}/sessions/{session_id} ==========
    async def get_agent_session(self, agent_id: str, session_id: str) -> Dict[str, Any]:
        """
        Get full agent session history (direct stream load).
        
        SLO: p99 < 300ms
        Justification: Agents need complete context for crash recovery.
        """
        try:
            events = await self.store.load_stream(f"agent-{agent_id}-{session_id}")
            context = await self.gas_town.reconstruct_agent_context(agent_id, session_id)
            
            return {
                "agent_id": agent_id,
                "session_id": session_id,
                "events": [
                    {
                        "type": e.event_type,
                        "position": e.stream_position,
                        "payload": e.payload,
                        "recorded_at": e.recorded_at.isoformat()
                    }
                    for e in events
                ],
                "reconstructed_context": context.context_text,
                "health_status": context.session_health_status,
                "total_events": len(events)
            }
        except StreamNotFoundError:
            return {"error": "Session not found"}
    
    # ========== RESOURCE 6: ledger://ledger/health ==========
    async def get_health(self) -> Dict[str, Any]:
        """
        Get system health metrics (watchdog endpoint).
        
        SLO: p99 < 10ms
        Source: ProjectionDaemon.get_all_lags()
        """
        async with self.pool.acquire() as conn:
            # Get projection lags
            rows = await conn.fetch("""
                SELECT 
                    pc.projection_name,
                    pc.last_position as checkpoint,
                    (SELECT MAX(global_position) FROM events) as latest,
                    (SELECT MAX(global_position) FROM events) - pc.last_position as lag
                FROM projection_checkpoints pc
            """)
            
            lags = {}
            for row in rows:
                lags[row['projection_name']] = {
                    "lag_events": row['lag'],
                    "lag_ms": row['lag'] * 1000 if row['lag'] else 0,
                    "checkpoint": row['checkpoint']
                }
            
            # Check SLO compliance
            slo_status = {}
            for name, data in lags.items():
                if name == "application_summary":
                    slo_status[name] = "OK" if data["lag_ms"] < 500 else "VIOLATION"
                elif name == "compliance_audit":
                    slo_status[name] = "OK" if data["lag_ms"] < 2000 else "VIOLATION"
                else:
                    slo_status[name] = "OK" if data["lag_ms"] < 5000 else "WARNING"
            
            return {
                "status": "healthy" if all(v == "OK" for v in slo_status.values()) else "degraded",
                "projection_lags_ms": {k: v["lag_ms"] for k, v in lags.items()},
                "slo_status": slo_status,
                "timestamp": datetime.utcnow().isoformat()
            }