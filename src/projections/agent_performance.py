"""AgentPerformanceLedger - tracks AI agent performance metrics."""

import asyncpg
from src.projections.daemon import Projection
from src.models.events import StoredEvent


class AgentPerformanceProjection(Projection):
    """
    Aggregates performance metrics per agent model version.
    
    Answers: "Is v2.3 making worse decisions than v2.2?"
    """
    
    def __init__(self, pool: asyncpg.Pool):
        self.name = "agent_performance"
        self.pool = pool
    
    async def handle(self, event: StoredEvent) -> None:
        """Update agent performance metrics."""
        
        if event.event_type == "CreditAnalysisCompleted":
            await self._update_analysis_metrics(event)
            
        elif event.event_type == "DecisionGenerated":
            await self._update_decision_metrics(event)
            
        elif event.event_type == "HumanReviewCompleted" and event.payload.get("override"):
            await self._update_override_metrics(event)
    
    async def _update_analysis_metrics(self, event: StoredEvent) -> None:
        """Update metrics for credit analysis."""
        agent_id = event.payload["agent_id"]
        model_version = event.payload["model_version"]
        
        await self.pool.execute("""
            INSERT INTO agent_performance_ledger (
                agent_id, model_version,
                analyses_completed, avg_confidence_score, avg_duration_ms,
                first_seen_at, last_seen_at
            ) VALUES ($1, $2, 1, $3, $4, NOW(), NOW())
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                analyses_completed = agent_performance_ledger.analyses_completed + 1,
                avg_confidence_score = (
                    (agent_performance_ledger.avg_confidence_score * 
                     agent_performance_ledger.analyses_completed + $3) /
                    (agent_performance_ledger.analyses_completed + 1)
                ),
                avg_duration_ms = (
                    (agent_performance_ledger.avg_duration_ms * 
                     agent_performance_ledger.analyses_completed + $4) /
                    (agent_performance_ledger.analyses_completed + 1)
                ),
                last_seen_at = NOW()
        """,
            agent_id,
            model_version,
            event.payload["confidence_score"],
            event.payload["analysis_duration_ms"]
        )
    
    async def _update_decision_metrics(self, event: StoredEvent) -> None:
        """Update metrics for decisions."""
        # This is simplified - in reality you'd track per agent
        recommendation = event.payload["recommendation"]
        
        for session_id in event.payload["contributing_agent_sessions"]:
            # Parse agent_id from session_id (format: agent-{id}-{session})
            parts = session_id.split('-')
            if len(parts) >= 2:
                agent_id = f"{parts[0]}-{parts[1]}"
                
                # Get model version from agent session (simplified)
                await self.pool.execute("""
                    UPDATE agent_performance_ledger
                    SET decisions_generated = decisions_generated + 1,
                        approve_rate = CASE WHEN $2 = 'APPROVE' 
                            THEN (approve_rate * decisions_generated + 1) / (decisions_generated + 1)
                            ELSE approve_rate * decisions_generated / (decisions_generated + 1)
                        END,
                        decline_rate = CASE WHEN $2 = 'DECLINE'
                            THEN (decline_rate * decisions_generated + 1) / (decisions_generated + 1)
                            ELSE decline_rate * decisions_generated / (decisions_generated + 1)
                        END,
                        refer_rate = CASE WHEN $2 = 'REFER'
                            THEN (refer_rate * decisions_generated + 1) / (decisions_generated + 1)
                            ELSE refer_rate * decisions_generated / (decisions_generated + 1)
                        END
                    WHERE agent_id = $1
                """, agent_id, recommendation)
    
    async def _update_override_metrics(self, event: StoredEvent) -> None:
        """Update override rates."""
        # Simplified - in reality you'd track per agent
        pass
    
    async def get_checkpoint(self) -> int:
        """Get last processed position."""
        row = await self.pool.fetchrow("""
            SELECT last_position FROM projection_checkpoints
            WHERE projection_name = 'agent_performance'
        """)
        return row['last_position'] if row else 0
    
    async def save_checkpoint(self, position: int) -> None:
        """Save checkpoint."""
        await self.pool.execute("""
            INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
            VALUES ('agent_performance', $1, NOW())
            ON CONFLICT (projection_name) DO UPDATE SET
                last_position = EXCLUDED.last_position,
                updated_at = NOW()
        """, position)