"""Single test file that verifies database connection."""

import pytest
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

# Mark all tests as asyncio
pytestmark = pytest.mark.asyncio


async def test_database_connection():
    """Test that we can connect to the database."""
    
    # Get database URL from environment or use default
    db_config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "database": os.getenv("POSTGRES_DB", "agentic_ledger"),
    }
    
    # Try to connect
    try:
        conn = await asyncpg.connect(**db_config)
        
        # Run a simple query
        result = await conn.fetchval("SELECT 1 + 1")
        assert result == 2
        
        # Check if we have our events table
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        print(f"\n✅ Connected to database: {db_config['database']}")
        print(f"📊 Found {len(tables)} tables")
        
        await conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Database connection failed: {e}")
        raise