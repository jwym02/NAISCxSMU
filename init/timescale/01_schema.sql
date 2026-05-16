-- ════════════════════════════════════════════════════════════
--  TimescaleDB Schema — Smart Tool Log Parser
--  Auto-runs on first container start via
--  /docker-entrypoint-initdb.d/
-- ════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ─────────────────────────────────────────
-- ENUMS
-- ─────────────────────────────────────────
DO $$ BEGIN CREATE TYPE severity_level   AS ENUM ('CRITICAL', 'ERROR', 'WARN', 'INFO', 'DEBUG');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE urgency_priority AS ENUM ('P0', 'P1', 'P2', 'DEADLETTER');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE log_format_type  AS ENUM ('JSON', 'XML', 'CSV', 'SYSLOG', 'KEY_VALUE', 'TEXT', 'BINARY', 'UNKNOWN');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE path_type        AS ENUM ('HOT', 'COLD', 'DEADLETTER');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ─────────────────────────────────────────
-- LOG EVENTS (core hypertable)
-- Written to by:
--   app-consumer-hot  (P0/P1 events)
--   app-consumer-cold (P2 events, batched)
--   app-consumer-deadletter (minimal failure record)
-- Queried by:
--   app-query (NL2SQL)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS log_events (
    id                      UUID DEFAULT gen_random_uuid(),
    event_time              TIMESTAMPTZ NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Source
    tool_id                 TEXT NOT NULL,
    vendor_id               TEXT,
    machine_type            TEXT,
    wafer_id                TEXT,
    recipe_name             TEXT,
    source_file             TEXT,

    -- Classification
    log_format              log_format_type  NOT NULL DEFAULT 'UNKNOWN',
    severity                severity_level   NOT NULL DEFAULT 'INFO',
    priority                urgency_priority NOT NULL DEFAULT 'P2',
    path                    path_type        NOT NULL DEFAULT 'COLD',

    -- Event details
    event_type              TEXT,
    fault_code              TEXT,
    process_step            TEXT,

    -- Normalized sensor readings (canonical units)
    temperature_k           DOUBLE PRECISION,
    pressure_pa             DOUBLE PRECISION,
    flow_slm                DOUBLE PRECISION,
    rf_power_w              DOUBLE PRECISION,

    -- Payloads
    raw_payload             JSONB,
    normalized_payload      JSONB,

    -- AI confidence (0.0 → 1.0)
    -- Only events >= 0.85 reach here via Kafka.
    -- Events < 0.85 go to DynamoDB human-review-queue instead.
    parse_confidence        FLOAT CHECK (parse_confidence BETWEEN 0 AND 1),
    normalize_confidence    FLOAT CHECK (normalize_confidence BETWEEN 0 AND 1),

    -- Tracking
    job_id                  UUID,
    reviewed_by_human       BOOLEAN DEFAULT FALSE,

    PRIMARY KEY (id, event_time)
);

SELECT create_hypertable(
    'log_events', 'event_time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────────
-- PARSE JOBS
-- One row per uploaded log file.
-- Updated as the file moves through
-- each stage of the pipeline.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS parse_jobs (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    file_name               TEXT NOT NULL,
    file_size_bytes         BIGINT,
    minio_raw_key           TEXT,
    detected_format         log_format_type,

    -- Status progression:
    -- PENDING → STORED → DEDUP_CHECK → DETECTING → EXTRACTING
    -- → NORMALIZING → ROUTING → COMPLETE | FAILED | DUPLICATE
    status                  TEXT NOT NULL DEFAULT 'PENDING',

    dedup_key               TEXT,
    is_duplicate            BOOLEAN DEFAULT FALSE,

    total_events            INTEGER DEFAULT 0,
    parsed_events           INTEGER DEFAULT 0,
    hot_events              INTEGER DEFAULT 0,
    cold_events             INTEGER DEFAULT 0,
    review_events           INTEGER DEFAULT 0,
    deadletter_events       INTEGER DEFAULT 0,

    detection_ms            INTEGER,
    extraction_ms           INTEGER,
    normalization_ms        INTEGER,
    routing_ms              INTEGER,

    error_message           TEXT,
    error_stage             TEXT
);

-- ─────────────────────────────────────────
-- INDEXES
-- ─────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_events_tool_time
    ON log_events (tool_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_events_severity_time
    ON log_events (severity, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_events_priority_time
    ON log_events (priority, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_events_hot_path
    ON log_events (event_time DESC)
    WHERE path = 'HOT';

CREATE INDEX IF NOT EXISTS idx_events_normalized_gin
    ON log_events USING gin (normalized_payload);

CREATE INDEX IF NOT EXISTS idx_events_job_id
    ON log_events (job_id);

-- ─────────────────────────────────────────
-- CONTINUOUS AGGREGATES
-- Pre-computed rollups used by NL2SQL
-- ─────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS events_5min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', event_time)    AS bucket,
    tool_id,
    severity,
    COUNT(*)                                AS event_count,
    AVG(normalize_confidence)               AS avg_norm_conf,
    AVG(temperature_k)                      AS avg_temp_k,
    AVG(pressure_pa)                        AS avg_pressure_pa,
    AVG(rf_power_w)                         AS avg_rf_power_w
FROM log_events
GROUP BY bucket, tool_id, severity
WITH NO DATA;

SELECT add_continuous_aggregate_policy('events_5min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS events_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', event_time)       AS bucket,
    tool_id,
    severity,
    COUNT(*)                                AS event_count,
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN severity = 'ERROR'    THEN 1 ELSE 0 END) AS error_count,
    AVG(temperature_k)                      AS avg_temp_k,
    AVG(pressure_pa)                        AS avg_pressure_pa
FROM log_events
GROUP BY bucket, tool_id, severity
WITH NO DATA;

SELECT add_continuous_aggregate_policy('events_1hour',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ─────────────────────────────────────────
-- COMPRESSION + RETENTION
-- ─────────────────────────────────────────
ALTER TABLE log_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'tool_id, severity'
);

SELECT add_compression_policy('log_events', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('log_events', INTERVAL '90 days', if_not_exists => TRUE);