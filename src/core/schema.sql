-- =====================================================
-- PHASE 1: EVENT STORE CORE SCHEMA
-- TRP1 Week 5 - Agentic Ledger
-- Author: Tsegay
-- =====================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- TABLE: events
-- Purpose: Immutable append-only log of all domain events
-- This is the SOURCE OF TRUTH for the entire system
-- =====================================================
CREATE TABLE events (
    -- Primary key: Uniquely identifies each event globally
    -- UUID avoids sequential leaks and enables sharding
    event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Stream ID: Identifies which aggregate this event belongs to
    -- Format: "loan-{uuid}" or "agent-{id}-{session}"
    -- TEXT allows human-readable format, no length issues
    stream_id        TEXT NOT NULL,
    
    -- Stream position: Version number WITHIN the stream
    -- Starts at 1, increments by 1, no gaps
    -- Used for optimistic concurrency control
    stream_position  BIGINT NOT NULL,
    
    -- Global position: Total order across ALL streams
    -- GENERATED ALWAYS prevents application from setting it
    -- Identity = auto-incrementing bigint
    global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Event type: Domain event name (e.g., "ApplicationSubmitted")
    -- Used for routing to upcasters and projections
    event_type       TEXT NOT NULL,
    
    -- Event version: Schema version of this event's payload
    -- SMALLINT is sufficient (max 32767 versions)
    -- Default 1 for all new events
    event_version    SMALLINT NOT NULL DEFAULT 1,
    
    -- Payload: The actual event data in JSON format
    -- JSONB is binary JSON, allows indexing, preserves ordering
    -- Stores ONLY domain data, no metadata
    payload          JSONB NOT NULL,
    
    -- Metadata: Cross-cutting concerns (correlation_id, causation_id)
    -- Separated from payload to avoid polluting domain schema
    -- Default empty object, never NULL
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    -- Recorded at: Physical write time
    -- clock_timestamp() = actual time, not transaction start time
    -- Critical for temporal queries and auditing
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    
    -- Unique constraint: Enforces no gaps/duplicates in stream
    -- This is what makes optimistic concurrency work!
    -- Two inserts with same (stream_id, stream_position) impossible
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- Index for loading aggregate streams efficiently
-- Covers both stream_id and ordering in one index
CREATE INDEX idx_events_stream_id ON events (stream_id, stream_position);

-- Index for projection daemon (global order replay)
-- Critical for async projections to resume from checkpoint
CREATE INDEX idx_events_global_pos ON events (global_position);

-- Index for filtering by event type (e.g., replay only CreditAnalysisCompleted)
-- Useful for projections that only care about specific events
CREATE INDEX idx_events_type ON events (event_type);

-- Index for time-range queries (regulatory examinations)
-- "Show me all events between Date X and Date Y"
CREATE INDEX idx_events_recorded ON events (recorded_at);

-- BRIN index for very large tables (millions+ events)
-- Much smaller than B-tree, perfect for append-only time-ordered data
CREATE INDEX idx_events_recorded_brin ON events USING BRIN (recorded_at);


-- =====================================================
-- TABLE: event_streams
-- Purpose: Stream metadata and current version for O(1) concurrency checks
-- Without this table, concurrency check would need MAX(stream_position)
-- which becomes slower as stream grows
-- =====================================================
CREATE TABLE event_streams (
    -- Primary key: Same as events.stream_id
    -- Ensures one row per stream
    stream_id        TEXT PRIMARY KEY,
    
    -- Aggregate type: The type of aggregate ("loan", "agent", "compliance")
    -- Enables queries like "find all active loan streams"
    aggregate_type   TEXT NOT NULL,
    
    -- Current version: Latest stream_position for this stream
    -- Updated atomically with every append
    -- Enables O(1) concurrency check without scanning events table
    current_version  BIGINT NOT NULL DEFAULT 0,
    
    -- Created at: When the first event was appended
    -- Useful for stream age analytics and cleanup policies
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Archived at: When stream was archived (NULL = active)
    -- Archived streams reject new appends
    -- Enables cold storage strategies
    archived_at      TIMESTAMPTZ,
    
    -- Metadata: Stream-level configuration
    -- e.g., retention policy, encryption keys, owner team
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Index for finding active streams by type
CREATE INDEX idx_streams_type ON event_streams (aggregate_type, archived_at);


-- =====================================================
-- TABLE: projection_checkpoints
-- Purpose: Track last processed global_position for each projection
-- Enables async daemon to resume after crash without full replay
-- =====================================================
CREATE TABLE projection_checkpoints (
    -- Projection name: Unique identifier (e.g., "application_summary")
    projection_name  TEXT PRIMARY KEY,
    
    -- Last position: Last global_position successfully processed
    -- Daemon queries: SELECT * FROM events WHERE global_position > last_position
    last_position    BIGINT NOT NULL DEFAULT 0,
    
    -- Updated at: When checkpoint was last updated
    -- Used for lag monitoring: NOW() - updated_at = approximate lag
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- =====================================================
-- TABLE: outbox
-- Purpose: Reliable event publishing to external systems
-- Solves the dual-write problem: event store + message bus
-- Written in SAME transaction as events table
-- =====================================================
CREATE TABLE outbox (
    -- Primary key: Unique identifier for outbox entry
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Event ID: References the source event
    -- Foreign key ensures outbox entry cannot exist without event
    event_id         UUID NOT NULL REFERENCES events(event_id),
    
    -- Destination: Target system (e.g., "kafka://loan-events")
    -- Enables routing to different buses
    destination      TEXT NOT NULL,
    
    -- Payload: Message to publish (may differ from event payload)
    -- e.g., stripped of internal fields, transformed for external
    payload          JSONB NOT NULL,
    
    -- Created at: When outbox entry was created
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Published at: NULL = not yet published, timestamp = published
    -- Publisher polls WHERE published_at IS NULL
    published_at     TIMESTAMPTZ,
    
    -- Attempts: Number of publish attempts
    -- Used for retry logic and dead-lettering
    attempts         SMALLINT NOT NULL DEFAULT 0
);

-- Index for efficient polling of unpublished messages
CREATE INDEX idx_outbox_unpublished ON outbox (published_at, created_at) 
    WHERE published_at IS NULL;


-- =====================================================
-- HELPER FUNCTION: append_events (optional but recommended)
-- Atomic append with optimistic concurrency check
-- Can be called from Python or used directly in tests
-- =====================================================
CREATE OR REPLACE FUNCTION append_events(
    p_stream_id TEXT,
    p_expected_version BIGINT,
    p_events JSONB,
    p_metadata JSONB DEFAULT '{}'::jsonb
) RETURNS TABLE(
    new_version BIGINT,
    event_ids UUID[]
) LANGUAGE plpgsql AS $$
DECLARE
    v_current_version BIGINT;
    v_new_version BIGINT;
    v_event_ids UUID[];
BEGIN
    -- Lock the stream row to prevent concurrent modifications
    SELECT current_version INTO v_current_version
    FROM event_streams
    WHERE stream_id = p_stream_id
    FOR UPDATE;
    
    -- Handle new stream creation
    IF v_current_version IS NULL THEN
        IF p_expected_version != -1 THEN
            RAISE EXCEPTION 'OptimisticConcurrencyError: Stream does not exist'
            USING HINT = 'expected_version=-1 for new streams';
        END IF;
        v_current_version := 0;
        
        -- Insert new stream record
        INSERT INTO event_streams (stream_id, aggregate_type, current_version)
        VALUES (p_stream_id, split_part(p_stream_id, '-', 1), 0);
    ELSE
        -- Check expected version against current version
        IF p_expected_version != v_current_version THEN
            RAISE EXCEPTION 'OptimisticConcurrencyError: Expected version %, actual version %',
                p_expected_version, v_current_version
            USING HINT = 'Reload stream and retry';
        END IF;
    END IF;
    
    -- Insert events (one or more) atomically
    WITH inserted AS (
        INSERT INTO events (
            stream_id, stream_position, event_type, event_version,
            payload, metadata
        )
        SELECT
            p_stream_id,
            v_current_version + ROW_NUMBER() OVER (ORDER BY ordinal),
            ev->>'event_type',
            (ev->>'event_version')::SMALLINT,
            ev->'payload',
            p_metadata || COALESCE(ev->'metadata', '{}'::jsonb)
        FROM jsonb_array_elements(p_events) WITH ORDINALITY AS t(ev, ordinal)
        RETURNING event_id, stream_position
    )
    SELECT array_agg(event_id) INTO v_event_ids FROM inserted;
    
    -- Update stream version
    v_new_version := v_current_version + jsonb_array_length(p_events);
    UPDATE event_streams
    SET current_version = v_new_version
    WHERE stream_id = p_stream_id;
    
    RETURN QUERY SELECT v_new_version, v_event_ids;
END;
$$;


-- =====================================================
-- TRIGGER: Notify on new events (for real-time projections)
-- PostgreSQL LISTEN/NOTIFY enables push-based updates
-- More efficient than polling
-- =====================================================
CREATE OR REPLACE FUNCTION notify_event_appended()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'event_appended',
        json_build_object(
            'stream_id', NEW.stream_id,
            'global_position', NEW.global_position,
            'event_type', NEW.event_type
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_event_appended
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_event_appended();