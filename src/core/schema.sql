-- Core Event Store Schema for Agentic Ledger
-- Based on TRP1 Week 5 specifications

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- Table: events
-- The immutable source of truth - append-only event log
-- =====================================================
CREATE TABLE events (
    event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id        TEXT NOT NULL,
    stream_position  BIGINT NOT NULL,
    global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
    event_type       TEXT NOT NULL,
    event_version    SMALLINT NOT NULL DEFAULT 1,
    payload          JSONB NOT NULL,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    
    -- Unique constraint for optimistic concurrency
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- Indexes for performance
CREATE INDEX idx_events_stream_id ON events (stream_id, stream_position);
CREATE INDEX idx_events_global_pos ON events (global_position);
CREATE INDEX idx_events_type ON events (event_type);
CREATE INDEX idx_events_recorded ON events (recorded_at);

-- BRIN index for time-range queries on large tables
CREATE INDEX idx_events_recorded_brin ON events USING BRIN (recorded_at);

-- =====================================================
-- Table: event_streams
-- Stream metadata and current version for O(1) concurrency checks
-- =====================================================
CREATE TABLE event_streams (
    stream_id        TEXT PRIMARY KEY,
    aggregate_type   TEXT NOT NULL,
    current_version  BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at      TIMESTAMPTZ,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_streams_type ON event_streams (aggregate_type, archived_at);

-- =====================================================
-- Table: projection_checkpoints
-- Daemon state tracking for async projections
-- =====================================================
CREATE TABLE projection_checkpoints (
    projection_name  TEXT PRIMARY KEY,
    last_position    BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- Table: outbox
-- Reliable event publishing to external systems
-- =====================================================
CREATE TABLE outbox (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID NOT NULL REFERENCES events(event_id),
    destination      TEXT NOT NULL,
    payload          JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at     TIMESTAMPTZ,
    attempts         SMALLINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_outbox_unpublished ON outbox (published_at, created_at) 
    WHERE published_at IS NULL;

-- =====================================================
-- Table: snapshots
-- Performance optimization for high-volume streams
-- =====================================================
CREATE TABLE snapshots (
    snapshot_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id        TEXT NOT NULL REFERENCES event_streams(stream_id),
    stream_position  BIGINT NOT NULL,
    aggregate_type   TEXT NOT NULL,
    snapshot_version INT NOT NULL,
    state            JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_snapshots_stream ON snapshots (stream_id, stream_position DESC);

-- =====================================================
-- Helper Functions
-- =====================================================

-- Function to append events atomically with optimistic concurrency
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
    -- Lock the stream row
    SELECT current_version INTO v_current_version
    FROM event_streams
    WHERE stream_id = p_stream_id
    FOR UPDATE;
    
    -- Check expected version (handle new streams)
    IF v_current_version IS NULL THEN
        IF p_expected_version != -1 THEN
            RAISE EXCEPTION 'OptimisticConcurrencyError: Stream does not exist'
            USING HINT = 'expected_version=-1 for new streams';
        END IF;
        v_current_version := 0;
        
        -- Insert new stream
        INSERT INTO event_streams (stream_id, aggregate_type, current_version)
        VALUES (p_stream_id, split_part(p_stream_id, '-', 1), 0);
    ELSE
        IF p_expected_version != v_current_version THEN
            RAISE EXCEPTION 'OptimisticConcurrencyError: Expected version %, actual version %',
                p_expected_version, v_current_version
            USING HINT = 'Reload stream and retry';
        END IF;
    END IF;
    
    -- Insert events
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
    SET current_version = v_new_version,
        updated_at = NOW()
    WHERE stream_id = p_stream_id;
    
    RETURN QUERY SELECT v_new_version, v_event_ids;
END;
$$;

-- Function to notify listeners of new events
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

-- Trigger for notifications
CREATE TRIGGER trigger_event_appended
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_event_appended();