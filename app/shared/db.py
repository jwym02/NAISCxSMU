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
                novelty_score FLOAT,
                requires_review BOOLEAN DEFAULT FALSE,
                review_reason TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_id, timestamp)
            );
        """)
        logger.info("Created normalized_events table")

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

        # Create trend_alerts table (temporal anomaly detection results)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trend_alerts (
                id BIGSERIAL PRIMARY KEY,
                machine TEXT NOT NULL,
                pattern TEXT NOT NULL,
                predicted_severity TEXT NOT NULL,
                estimated_time_to_critical TEXT,
                recommended_action TEXT,
                confidence FLOAT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trend_alerts_machine ON trend_alerts (machine)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trend_alerts_created ON trend_alerts (created_at DESC)")
        logger.info("Created trend_alerts table")

        # Create categories table (reviewer-managed list of valid event categories)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                name TEXT PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Seed the built-in categories so the dropdown is always populated
        await conn.execute("""
            INSERT INTO categories (name) VALUES
                ('thermal'), ('mechanical'), ('electrical'), ('gas_leak'),
                ('contamination'), ('process_drift'), ('safety'), ('software'),
                ('maintenance'), ('unknown')
            ON CONFLICT (name) DO NOTHING;
        """)
        logger.info("Created and seeded categories table")

        # Idempotent column migration — adds novelty_score if this is an existing DB
        await conn.execute("""
            ALTER TABLE normalized_events ADD COLUMN IF NOT EXISTS novelty_score FLOAT;
        """)
        logger.info("Ensured novelty_score column on normalized_events")

        # Create indexes for common queries
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_logs_timestamp ON raw_logs (timestamp DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_logs_file_hash ON raw_logs (file_hash)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_timestamp ON normalized_events (timestamp DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_source ON normalized_events (source)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_severity ON normalized_events (severity)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_category ON normalized_events (ai_category)")
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
                ON CONFLICT (job_id, timestamp) DO NOTHING
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
    novelty_score: Optional[float] = None,
) -> bool:
    """Insert a normalized event."""
    logger.info(
        "▶▶▶ [DB] insert_normalized_event called  job_id=%s  source=%s  severity=%s  "
        "requires_review=%s  confidence=%.2f  novelty=%.4f",
        job_id, source, severity, requires_review, confidence_score, novelty_score or 0.0,
    )
    try:
        pool = await get_pool()
    except Exception as e:
        logger.error("▶▶▶ [DB] get_pool() FAILED: %s", e)
        return False
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO normalized_events (
                    job_id, timestamp, source, event_type, severity, message,
                    ai_category, ai_root_cause, ai_recommended_action,
                    confidence_score, novelty_score, requires_review, review_reason
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (job_id, timestamp) DO NOTHING
            """,
            job_id, timestamp, source, event_type, severity, message,
            ai_category, ai_root_cause, ai_recommended_action,
            confidence_score, novelty_score, requires_review, review_reason,
            )
        logger.info("▶▶▶ [DB] insert_normalized_event OK  job_id=%s", job_id)
        return True
    except Exception as e:
        logger.error("▶▶▶ [DB] insert_normalized_event FAILED  job_id=%s  error=%s", job_id, e)
        return False


async def insert_event_routing(job_id: str, kafka_topic: str) -> bool:
    """Record the Kafka topic an event was routed to."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO event_routing (job_id, kafka_topic)
                VALUES ($1, $2)
                ON CONFLICT (job_id) DO NOTHING
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
                ON CONFLICT (job_id) DO NOTHING
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
    """
    Get all pending review queue items.

    Drives from normalized_events (requires_review = true) so that events
    needing review are always surfaced even if the review_queue_status row
    was never inserted.  Events already approved or rejected are excluded.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    ne.job_id,
                    ne.timestamp,
                    ne.source,
                    ne.event_type,
                    ne.severity,
                    ne.message,
                    ne.ai_category,
                    ne.ai_root_cause,
                    ne.ai_recommended_action,
                    ne.confidence_score,
                    ne.requires_review,
                    ne.review_reason,
                    ne.created_at,
                    COALESCE(rqs.status, 'pending')   AS review_status,
                    rqs.reviewer_notes,
                    rqs.reviewed_at
                FROM normalized_events ne
                LEFT JOIN review_queue_status rqs ON rqs.job_id = ne.job_id
                WHERE ne.requires_review = true
                  AND (rqs.status IS NULL OR rqs.status = 'pending')
                ORDER BY ne.timestamp DESC
            """)
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to query review queue: {e}")
        return []


async def get_event_stats() -> Dict[str, Any]:
    """Get aggregate counts for the dashboard summary panel."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM normalized_events")
            # Count events that still need human review — same query logic as
            # get_review_queue_pending() so badge and drawer are always in sync.
            in_review = await conn.fetchval("""
                SELECT COUNT(*)
                FROM normalized_events ne
                LEFT JOIN review_queue_status rqs ON rqs.job_id = ne.job_id
                WHERE ne.requires_review = true
                  AND (rqs.status IS NULL OR rqs.status = 'pending')
            """)
            # Jobs with no corresponding normalized event (processing failures)
            failures = await conn.fetchval("""
                SELECT COUNT(*) FROM raw_logs r
                WHERE NOT EXISTS (
                    SELECT 1 FROM normalized_events ne WHERE ne.job_id = r.job_id
                )
            """)
            jobs_today = await conn.fetchval("""
                SELECT COUNT(*) FROM raw_logs
                WHERE created_at >= CURRENT_DATE
            """)
            severity_rows = await conn.fetch("""
                SELECT UPPER(severity) AS severity, COUNT(*) AS count
                FROM normalized_events
                GROUP BY UPPER(severity)
            """)
        severity_breakdown = {row["severity"]: int(row["count"]) for row in severity_rows}
        return {
            "events_processed": int(total or 0),
            "events_in_review": int(in_review or 0),
            "errors": int(failures or 0),
            "jobs_today": int(jobs_today or 0),
            "severity_breakdown": severity_breakdown,
        }
    except Exception as e:
        logger.error(f"Failed to get event stats: {e}")
        return {
            "events_processed": 0,
            "events_in_review": 0,
            "errors": 0,
            "jobs_today": 0,
            "severity_breakdown": {},
        }


async def get_events_timeseries(hours: int = 12) -> List[Dict[str, Any]]:
    """Get hourly event counts grouped by severity for the last N hours."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    time_bucket('1 hour', timestamp) AS hour,
                    UPPER(severity) AS severity,
                    COUNT(*) AS count
                FROM normalized_events
                WHERE timestamp >= NOW() - ($1::int * INTERVAL '1 hour')
                GROUP BY time_bucket('1 hour', timestamp), UPPER(severity)
                ORDER BY hour ASC
            """, hours)
        return [
            {
                "hour": row["hour"].isoformat(),
                "severity": row["severity"],
                "count": int(row["count"]),
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Failed to get events timeseries: {e}")
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


# ── Category Registry ─────────────────────────────────────────────────────────

async def get_all_categories() -> List[str]:
    """Return all known event categories, sorted alphabetically."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT name FROM categories ORDER BY name")
        return [row["name"] for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch categories: {e}")
        return []


async def insert_category(name: str) -> bool:
    """
    Insert a new category. Returns True if created, False if it already existed.
    The caller is responsible for validating the name before calling this.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            result = await conn.execute(
                "INSERT INTO categories (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
                name,
            )
        return result == "INSERT 0 1"
    except Exception as e:
        logger.error(f"Failed to insert category '{name}': {e}")
        return False


# ── Trend Alerts ──────────────────────────────────────────────────────────────

async def insert_trend_alert(alert: dict) -> bool:
    """Persist a temporal anomaly alert returned by detect_temporal_anomaly()."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO trend_alerts
                    (machine, pattern, predicted_severity, estimated_time_to_critical,
                     recommended_action, confidence)
                VALUES ($1, $2, $3, $4, $5, $6)
            """,
            alert.get("machine", "unknown"),
            alert.get("pattern", ""),
            alert.get("predicted_severity", "warning"),
            alert.get("estimated_time_to_critical", "N/A"),
            alert.get("recommended_action", ""),
            float(alert.get("confidence", 0.0)),
            )
        logger.info("Inserted trend alert for machine=%s", alert.get("machine"))
        return True
    except Exception as e:
        logger.error("Failed to insert trend alert: %s", e)
        return False


async def get_trend_alerts(limit: int = 50) -> List[Dict[str, Any]]:
    """Return the most recent trend alerts, newest first."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, machine, pattern, predicted_severity,
                       estimated_time_to_critical, recommended_action,
                       confidence, created_at
                FROM trend_alerts
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)
        return [
            {
                **dict(row),
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            }
            for row in rows
        ]
    except Exception as e:
        logger.error("Failed to fetch trend alerts: %s", e)
        return []