"""
Database module for TimescaleDB connection and schema management.

Architecture:
  raw_logs (hypertable) ──── normalized_events (hypertable)
       ↓                              ↓
  30-day retention          7-day retention
  (full file backup)        (AI analysis output)
       ↓                              ↓
  event_routing ────────── review_queue_status
  (Kafka topic tracking)    (Review status tracking)
"""

import os
import asyncio
import logging
from typing import Optional, List, Dict, Any
import asyncpg
from datetime import datetime, timedelta

# Database configuration
TIMESCALE_HOST = os.getenv("TIMESCALE_HOST", "timescaledb")
TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT", 5432))
TIMESCALE_DB = os.getenv("TIMESCALE_DB", "logparser_db")
TIMESCALE_USER = os.getenv("TIMESCALE_USER", "logparser")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "logparser_secret")
TIMESCALE_POOL_SIZE = int(os.getenv("TIMESCALE_POOL_SIZE", 5))

logger = logging.getLogger(__name__)

# Global connection pool
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Get or create the database connection pool."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=TIMESCALE_HOST,
            port=TIMESCALE_PORT,
            database=TIMESCALE_DB,
            user=TIMESCALE_USER,
            password=TIMESCALE_PASSWORD,
            min_size=2,
            max_size=TIMESCALE_POOL_SIZE,
            command_timeout=60,
        )
        logger.info(f"Created database pool: {TIMESCALE_HOST}:{TIMESCALE_PORT}/{TIMESCALE_DB}")
    return _pool


async def close_pool():
    """Close the database connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Closed database pool")


async def init_schema():
    """Initialize database schema. Run once on startup."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Enable TimescaleDB extension
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        logger.info("TimescaleDB extension enabled")

        # Create raw_logs hypertable (30-day retention)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_logs (
                job_id UUID,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                file_name TEXT NOT NULL,
                file_format TEXT NOT NULL,
                raw_content TEXT NOT NULL,
                file_hash VARCHAR(64),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_id, timestamp)
            );
        """)
        logger.info("Created raw_logs table")

        # Convert raw_logs to hypertable if not already
        await conn.execute("""
            SELECT create_hypertable('raw_logs', 'timestamp', if_not_exists => TRUE)
        """)
        logger.info("raw_logs converted to hypertable")

        # Set retention policy: 30 days
        await conn.execute("""
            SELECT add_retention_policy('raw_logs', INTERVAL '30 days', if_not_exists => TRUE)
        """)
        logger.info("raw_logs retention policy: 30 days")

        # Create normalized_events hypertable (7-day retention)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS normalized_events (
                job_id UUID,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                ai_category TEXT,
                ai_root_cause TEXT,
                ai_recommended_action TEXT,
                confidence_score FLOAT,
                requires_review BOOLEAN DEFAULT FALSE,
                review_reason TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_id, timestamp)
            );
        """)
        logger.info("Created normalized_events table")

        await conn.execute("""
            ALTER TABLE normalized_events
            ADD COLUMN IF NOT EXISTS search_text TEXT
        """)
        logger.info("Ensured normalized_events.search_text column")

        # Convert normalized_events to hypertable if not already
        await conn.execute("""
            SELECT create_hypertable('normalized_events', 'timestamp', if_not_exists => TRUE)
        """)
        logger.info("normalized_events converted to hypertable")

        # Set retention policy: 7 days
        await conn.execute("""
            SELECT add_retention_policy('normalized_events', INTERVAL '7 days', if_not_exists => TRUE)
        """)
        logger.info("normalized_events retention policy: 7 days")

        # Create event_routing table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS event_routing (
                id BIGSERIAL PRIMARY KEY,
                job_id UUID NOT NULL UNIQUE,
                kafka_topic TEXT NOT NULL,
                routed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Created event_routing table")

        # Create review_queue_status table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS review_queue_status (
                id BIGSERIAL PRIMARY KEY,
                job_id UUID NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                reviewer_notes TEXT,
                reviewed_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Created review_queue_status table")

        # Create indexes for common queries
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_logs_timestamp ON raw_logs (timestamp DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_logs_file_hash ON raw_logs (file_hash)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_timestamp ON normalized_events (timestamp DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_source ON normalized_events (source)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_severity ON normalized_events (severity)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_category ON normalized_events (ai_category)")
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_normalized_search_text_gin
            ON normalized_events USING GIN (
                to_tsvector('simple', COALESCE(search_text, ''))
            )
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_event_routing_topic ON event_routing (kafka_topic)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_review_status ON review_queue_status (status)")
        logger.info("Created indexes")

        # Create continuous aggregates for analytics
        await conn.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS events_by_hour AS
            SELECT
                time_bucket('1 hour', timestamp) as bucket,
                ai_category,
                severity,
                COUNT(*) as event_count
            FROM normalized_events
            GROUP BY bucket, ai_category, severity
            WITH DATA;
        """)
        logger.info("Created continuous aggregate: events_by_hour")

        await conn.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS events_by_machine_daily AS
            SELECT
                time_bucket('1 day', timestamp) as bucket,
                source,
                ai_category,
                severity,
                COUNT(*) as event_count,
                AVG(confidence_score) as avg_confidence
            FROM normalized_events
            GROUP BY bucket, source, ai_category, severity
            WITH DATA;
        """)
        logger.info("Created continuous aggregate: events_by_machine_daily")


# ── Write Operations ──────────────────────────────────────────────


async def insert_raw_log(
    job_id: str,
    timestamp: datetime,
    file_name: str,
    file_format: str,
    raw_content: str,
    file_hash: str,
) -> bool:
    """Insert a raw log entry."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO raw_logs (job_id, timestamp, file_name, file_format, raw_content, file_hash)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, job_id, timestamp, file_name, file_format, raw_content, file_hash)
        logger.info(f"Inserted raw log: job_id={job_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to insert raw log: {e}")
        return False


async def insert_normalized_event(
    job_id: str,
    timestamp: datetime,
    source: str,
    event_type: str,
    severity: str,
    message: str,
    ai_category: str,
    ai_root_cause: str,
    ai_recommended_action: str,
    confidence_score: float,
    requires_review: bool,
    review_reason: Optional[str] = None,
    search_text: Optional[str] = None,
) -> bool:
    """Insert a normalized event."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO normalized_events (
                    job_id, timestamp, source, event_type, severity, message,
                    ai_category, ai_root_cause, ai_recommended_action,
                    confidence_score, requires_review, review_reason, search_text
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """,
            job_id, timestamp, source, event_type, severity, message,
            ai_category, ai_root_cause, ai_recommended_action,
            confidence_score, requires_review, review_reason, search_text
            )
        logger.info(f"Inserted normalized event: job_id={job_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to insert normalized event: {e}")
        return False


async def insert_event_routing(job_id: str, kafka_topic: str) -> bool:
    """Record the Kafka topic an event was routed to."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO event_routing (job_id, kafka_topic)
                VALUES ($1, $2)
            """, job_id, kafka_topic)
        logger.info(f"Recorded routing: job_id={job_id}, topic={kafka_topic}")
        return True
    except Exception as e:
        logger.error(f"Failed to insert event routing: {e}")
        return False


async def insert_review_queue_item(job_id: str) -> bool:
    """Add an item to the review queue."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO review_queue_status (job_id, status)
                VALUES ($1, 'pending')
            """, job_id)
        logger.info(f"Added to review queue: job_id={job_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to insert review queue item: {e}")
        return False


async def update_review_status(job_id: str, status: str, notes: Optional[str] = None) -> bool:
    """Update review queue status (approved/rejected/pending)."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE review_queue_status
                SET status = $1, reviewer_notes = $2, reviewed_at = CURRENT_TIMESTAMP
                WHERE job_id = $3
            """, status, notes, job_id)
        logger.info(f"Updated review status: job_id={job_id}, status={status}")
        return True
    except Exception as e:
        logger.error(f"Failed to update review status: {e}")
        return False


# ── Read Operations ──────────────────────────────────────────────


async def get_normalized_events_by_machine(
    source: str,
    start_time: datetime,
    end_time: datetime,
) -> List[Dict[str, Any]]:
    """Get all normalized events for a machine within a time range."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM normalized_events
                WHERE source = $1 AND timestamp BETWEEN $2 AND $3
                ORDER BY timestamp DESC
            """, source, start_time, end_time)
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to query events by machine: {e}")
        return []


async def get_events_by_category_and_severity(
    start_time: datetime,
    end_time: datetime,
) -> List[Dict[str, Any]]:
    """Get event counts by category and severity for trend analysis."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    ai_category,
                    severity,
                    COUNT(*) as count
                FROM normalized_events
                WHERE timestamp BETWEEN $1 AND $2
                GROUP BY ai_category, severity
                ORDER BY count DESC
            """, start_time, end_time)
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to query events by category/severity: {e}")
        return []


async def get_hourly_analytics(
    start_time: datetime,
    end_time: datetime,
) -> List[Dict[str, Any]]:
    """Get hourly event counts from continuous aggregate."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM events_by_hour
                WHERE bucket BETWEEN $1 AND $2
                ORDER BY bucket DESC
            """, start_time, end_time)
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to query hourly analytics: {e}")
        return []


async def get_machine_health(
    source: str,
    start_time: datetime,
    end_time: datetime,
) -> List[Dict[str, Any]]:
    """Get machine health metrics (daily aggregates)."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM events_by_machine_daily
                WHERE source = $1 AND bucket BETWEEN $2 AND $3
                ORDER BY bucket DESC
            """, source, start_time, end_time)
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to query machine health: {e}")
        return []


async def get_review_queue_pending() -> List[Dict[str, Any]]:
    """Get all pending review queue items."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT rqs.*, ne.* FROM review_queue_status rqs
                JOIN normalized_events ne ON rqs.job_id = ne.job_id
                WHERE rqs.status = 'pending'
                ORDER BY ne.timestamp DESC
            """)
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to query review queue: {e}")
        return []


async def get_event_with_routing(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a normalized event with its routing info."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT ne.*, er.kafka_topic, rqs.status as review_status
                FROM normalized_events ne
                LEFT JOIN event_routing er ON ne.job_id = er.job_id
                LEFT JOIN review_queue_status rqs ON ne.job_id = rqs.job_id
                WHERE ne.job_id = $1
            """, job_id)
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"Failed to query event with routing: {e}")
        return None