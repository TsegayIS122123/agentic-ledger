#!/usr/bin/env python
"""Run the projection daemon."""

import asyncio
import asyncpg
import logging
from src.core.event_store import EventStore
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.utils.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()


async def main():
    """Start the projection daemon."""
    logger.info("🚀 Starting projection daemon...")
    
    # Connect to database
    pool = await asyncpg.create_pool(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB,
        min_size=1,
        max_size=5
    )
    
    # Create event store
    store = EventStore(pool)
    
    # Create projections
    projections = [
        ApplicationSummaryProjection(pool),
        AgentPerformanceProjection(pool),
        ComplianceAuditProjection(pool, snapshot_interval=100)
    ]
    
    # Create and start daemon
    daemon = ProjectionDaemon(
        store=store,
        projections=projections,
        poll_interval_ms=settings.PROJECTION_POLL_INTERVAL_MS,
        batch_size=settings.PROJECTION_BATCH_SIZE,
        max_retries=settings.PROJECTION_MAX_RETRIES
    )
    
    try:
        await daemon.run_forever()
    except KeyboardInterrupt:
        logger.info("🛑 Stopping daemon...")
        await daemon.stop()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())