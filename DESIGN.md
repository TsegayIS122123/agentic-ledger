# DESIGN.md - Agentic Ledger Architecture Decisions

## Phase 1: Event Store Core - Schema Justifications

### Table: `events`

| Column | Justification |
|--------|---------------|
| `event_id UUID` | Globally unique identifier for each event. UUID avoids sequential leaks and enables future sharding. |
| `stream_id TEXT` | Identifies which aggregate instance this event belongs to. TEXT allows human-readable format like "loan-123". |
| `stream_position BIGINT` | Version number within the stream. Combined with stream_id for optimistic concurrency. BIGINT supports streams with billions of events. |
| `global_position BIGINT GENERATED ALWAYS AS IDENTITY` | Total order across all streams. GENERATED ALWAYS prevents application from tampering with global order. |
| `event_type TEXT` | Domain event name for routing to upcasters and projections. |
| `event_version SMALLINT` | Schema version for upcasting. SMALLINT is sufficient (max 32767 versions). |
| `payload JSONB` | The actual event data. JSONB enables indexing and efficient queries. |
| `metadata JSONB` | Cross-cutting concerns (correlation_id, causation_id). Separated to avoid polluting domain schema. |
| `recorded_at TIMESTAMPTZ` | Physical write time using clock_timestamp() for accuracy in temporal queries. |

**Constraint `uq_stream_position`**: Enforces no gaps/duplicates in streams. This is the foundation of optimistic concurrency.

### Table: `event_streams`

| Column | Justification |
|--------|---------------|
| `stream_id TEXT PRIMARY KEY` | Same as events.stream_id. One row per stream. |
| `aggregate_type TEXT` | Enables queries like "find all active loan streams". |
| `current_version BIGINT` | Enables O(1) concurrency check without scanning events table. |
| `created_at TIMESTAMPTZ` | Tracks when stream was created for analytics. |
| `archived_at TIMESTAMPTZ` | NULL = active. Enables cold storage strategies. |
| `metadata JSONB` | Stream-level configuration (retention, encryption, owner). |

### Table: `projection_checkpoints`

| Column | Justification |
|--------|---------------|
| `projection_name TEXT PRIMARY KEY` | Unique identifier for each projection. |
| `last_position BIGINT` | Last processed global_position. Enables resume after crash. |
| `updated_at TIMESTAMPTZ` | Used for lag monitoring: NOW() - updated_at. |

### Table: `outbox`

| Column | Justification |
|--------|---------------|
| `id UUID PRIMARY KEY` | Unique identifier for outbox entry. |
| `event_id UUID REFERENCES events` | Ensures outbox entry cannot exist without event. |
| `destination TEXT` | Target system for routing. |
| `payload JSONB` | Message to publish (may differ from event). |
| `created_at TIMESTAMPTZ` | For ordering and cleanup. |
| `published_at TIMESTAMPTZ` | NULL = not yet published. Enables polling. |
| `attempts SMALLINT` | For retry logic and dead-lettering.

## Phase 4: Upcasting Inference Strategies

### CreditAnalysisCompleted v1 → v2

| Field | Inference Strategy | Justification |
|-------|-------------------|---------------|
| `model_version` | Infer from recorded_at year | Pre-2026 events used rule-based system, not ML. Sentinels are accurate categorical facts. |
| `confidence_score` | NULL | This data was not collected. Fabrication would create false audit trail - worse than null. |
| `regulatory_basis` | Look up from regulation changes | Regulation changes are public record. Inference is deterministic and verifiable. |

### DecisionGenerated v1 → v2

| Field | Inference Strategy | Performance Impact |
|-------|-------------------|-------------------|
| `model_versions` | Load each AgentSession to get model_version | Each upcast may require N additional queries. Acceptable because old events are upcasted once and cached. |

### Immutability Guarantee

The system NEVER modifies stored events. Upcasting happens entirely in memory at read time, preserving the core event sourcing guarantee. |