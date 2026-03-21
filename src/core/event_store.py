"""Event Store implementation for Agentic Ledger."""

import json
import logging
from typing import List, Optional, AsyncIterator, Dict, Any, AsyncGenerator
from uuid import UUID
from datetime import datetime

import asyncpg

from src.models.events import BaseEvent, StoredEvent, StreamMetadata
from src.core.exceptions import OptimisticConcurrencyError, StreamNotFoundError

logger = logging.getLogger(__name__)


class EventStore:
    """Event Store implementation using PostgreSQL."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def append(
        self,
        stream_id: str,
        events: List[BaseEvent],
        expected_version: int,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> int:
        """Atomically append events to a stream."""
        if not events:
            raise ValueError("Cannot append empty events list")

        # Build metadata
        base_metadata = {}
        if correlation_id:
            base_metadata["correlation_id"] = correlation_id
        if causation_id:
            base_metadata["causation_id"] = causation_id

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Lock and get current version
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1 FOR UPDATE",
                    stream_id
                )

                current_version = row['current_version'] if row else 0

                # Check concurrency
                if row is None:
                    if expected_version != -1:
                        raise OptimisticConcurrencyError(
                            stream_id=stream_id,
                            expected_version=expected_version,
                            actual_version=0,
                            message=f"Stream {stream_id} does not exist"
                        )
                elif expected_version != current_version:
                    raise OptimisticConcurrencyError(
                        stream_id=stream_id,
                        expected_version=expected_version,
                        actual_version=current_version,
                        message=f"Stream {stream_id} version mismatch"
                    )

                # Insert events
                for i, event in enumerate(events):
                    position = current_version + i + 1

                    # Get event type from the event object
                    event_type = event.event_type

                    # Convert event to dict
                    event_dict = event.to_dict()

                    await conn.execute("""
                        INSERT INTO events (
                            stream_id, stream_position, event_type,
                            event_version, payload, metadata
                        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
                    """,
                        stream_id,
                        position,
                        event_type,
                        event.event_version,
                        json.dumps(event_dict),
                        json.dumps(base_metadata)
                    )

                # Update stream
                new_version = current_version + len(events)

                if row is None:
                    await conn.execute("""
                        INSERT INTO event_streams (stream_id, aggregate_type, current_version)
                        VALUES ($1, $2, $3)
                    """, stream_id, stream_id.split('-')[0], new_version)
                else:
                    await conn.execute("""
                        UPDATE event_streams SET current_version = $1 WHERE stream_id = $2
                    """, new_version, stream_id)

                return new_version

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: Optional[int] = None,
    ) -> List[StoredEvent]:
        """Load all events for a stream."""
        query = """
            SELECT
                event_id, stream_id, stream_position, global_position,
                event_type, event_version, payload, metadata, recorded_at
            FROM events
            WHERE stream_id = $1
        """
        params = [stream_id]

        if from_position > 0:
            query += " AND stream_position >= $2"
            params.append(from_position)

        if to_position is not None:
            query += f" AND stream_position <= ${len(params) + 1}"
            params.append(to_position)

        query += " ORDER BY stream_position ASC"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

            if not rows and from_position == 0:
                exists = await conn.fetchval(
                    "SELECT 1 FROM event_streams WHERE stream_id = $1",
                    stream_id
                )
                if not exists:
                    raise StreamNotFoundError(f"Stream {stream_id} not found")

            return [StoredEvent.from_row(dict(row)) for row in rows]

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: Optional[List[str]] = None,
        batch_size: int = 500,
    ) -> AsyncGenerator[List[StoredEvent], None]:
        """Load events in global order."""
        query = """
            SELECT
                event_id, stream_id, stream_position, global_position,
                event_type, event_version, payload, metadata, recorded_at
            FROM events
            WHERE global_position > $1
        """
        params = [from_global_position]

        if event_types:
            placeholders = [f"${i+2}" for i in range(len(event_types))]
            query += f" AND event_type IN ({','.join(placeholders)})"
            params.extend(event_types)

        query += " ORDER BY global_position ASC LIMIT $1000"

        async with self.pool.acquire() as conn:
            offset = 0
            while True:
                current_query = query + f" OFFSET {offset}"
                current_params = params + [batch_size]

                rows = await conn.fetch(current_query, *current_params)

                if not rows:
                    break

                yield [StoredEvent.from_row(dict(row)) for row in rows]

                offset += len(rows)
                if len(rows) < batch_size:
                    break

    async def stream_version(self, stream_id: str) -> int:
        """Get current version of a stream."""
        async with self.pool.acquire() as conn:
            version = await conn.fetchval(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id
            )
            return version or 0

    async def archive_stream(self, stream_id: str) -> None:
        """Mark a stream as archived."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE event_streams
                SET archived_at = NOW()
                WHERE stream_id = $1 AND archived_at IS NULL
            """, stream_id)

            if result == "UPDATE 0":
                raise StreamNotFoundError(f"Stream {stream_id} not found")

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """Get metadata for a stream."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT stream_id, aggregate_type, current_version,
                       created_at, archived_at, metadata
                FROM event_streams
                WHERE stream_id = $1
            """, stream_id)

            if not row:
                raise StreamNotFoundError(f"Stream {stream_id} not found")

            return StreamMetadata(
                stream_id=row['stream_id'],
                aggregate_type=row['aggregate_type'],
                current_version=row['current_version'],
                created_at=row['created_at'],
                archived_at=row['archived_at'],
                metadata=row['metadata']
            )