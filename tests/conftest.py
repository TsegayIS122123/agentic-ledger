"""Pytest fixtures for testing."""

import pytest
import asyncpg
import asyncio
import os
from typing import AsyncGenerator, Dict, Any


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
        "database": os.getenv("POSTGRES_DB", "agentic_ledger_test"),
        "min_size": 1,
        "max_size": 5,
    }
    
    # Create pool
    pool = await asyncpg.create_pool(**db_config)
    
    # Clean tables before each test
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE events, event_streams, projection_checkpoints, outbox CASCADE")
    
    yield pool
    
    await pool.close()