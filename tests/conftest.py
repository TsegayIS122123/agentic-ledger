"""Pytest fixtures for testing."""

import pytest
import asyncpg
import asyncio
import os
from typing import AsyncGenerator


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for tests."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def postgres_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """Create PostgreSQL connection pool for testing."""
    
    db_config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "database": "agentic_ledger_test",
        "min_size": 1,
        "max_size": 5,
    }
    
    pool = await asyncpg.create_pool(**db_config)
    
    # Clean all tables before each test
    async with pool.acquire() as conn:
        await conn.execute("""
            TRUNCATE 
                events, 
                event_streams, 
                projection_checkpoints, 
                outbox,
                application_summary,
                compliance_audit_view,
                compliance_snapshots,
                agent_performance_ledger
            CASCADE
        """)
        
        # Reset sequences
        await conn.execute("ALTER SEQUENCE events_global_position_seq RESTART WITH 1")
    
    yield pool
    
    await pool.close()