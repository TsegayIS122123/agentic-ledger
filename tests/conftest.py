"""Pytest fixtures for testing."""

import pytest
import asyncpg
import asyncio
import os


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def postgres_pool():
    pool = await asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database="agentic_ledger_test",
        min_size=1,
        max_size=5
    )
    
    # Clean tables
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE events, event_streams, projection_checkpoints CASCADE")
    
    yield pool
    await pool.close()