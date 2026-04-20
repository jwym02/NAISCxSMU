"""
Dead Letter Consumer — Audit & Spike Detection for Unclassifiable Events
=========================================================================
Consumes: logs.deadletter

Events land here when the router determines they cannot be reliably acted on:
  • confidence < 0.3        — AI had almost no idea what the event was
  • category == "unknown"   — unclassifiable format, AND severity == "error"

This consumer does three things per event:

  1. AUDIT ROW       Insert into normalized_events with requires_review=True
                     and a review_reason explaining why it was dead-lettered.
                     Keeps all events — even failures — queryable from one table.

  2. REVIEW RECORD   Insert into review_queue_status as 'pending' so that the
                     pipeline API's /queue endpoint surfaces it for human triage.

  3. SPIKE DETECTION Track a rolling window of dead-letter timestamps.
                     If SPIKE_THRESHOLD events arrive within SPIKE_WINDOW_SECONDS,
                     log a loud WARNING. This is the earliest signal that a new
                     machine vendor or log format has arrived that the normaliser
                     cannot yet handle.

No batching — dead-letter events are rare by design and must be audited
immediately. Each event is written in its own DB round-trip.

Entry:  python -m app.consumers.dead_letter
Port:   8085  (health endpoint; add port mapping to docker-compose if needed)
"""

import os
import json
import gzip
import logging
import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer

from app.shared.db import get_pool, init_schema, close_pool
from app.shared.kafka_client import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC_DEADLETTER",      "logs.deadletter")
KAFKA_GROUP      = os.getenv("KAFKA_GROUP_ID_DEADLETTER",   "consumer-group-deadletter")
SPIKE_THRESHOLD  = int(os.getenv("DL_SPIKE_THRESHOLD",      "10"))   # events
SPIKE_WINDOW_SECS= int(os.getenv("DL_SPIKE_WINDOW_SECONDS", "300"))  # 5 min

# ── State ──────────────────────────────────────────────────────────────────────
# Rolling window: deque of UTC timestamps for recent dead-letter events.
# We keep at most SPIKE_THRESHOLD entries — older ones are dropped during
# each check, so memory is bounded regardless of throughput.
_spike_window: deque = deque()
total_received: int  = 0
total_written:  int  = 0

app = FastAPI(title="Dead Letter Consumer")
kafka_consumer: Optional[AIOKafkaConsumer] = None
_consumer_task: Optional[asyncio.Task] = None  # held for cancellation on shutdown


# ── Helpers ────────────────────────────────────────────────────────────────────

def _deserialize(data: bytes) -> dict:
    """Try gzip decompression first, fall back to plain UTF-8 JSON."""
    try:
        return json.loads(gzip.decompress(data).decode("utf-8"))
    except (OSError, EOFError):
        try:
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            logger.error("Failed to deserialize dead-letter message: %s", e)
            return {}


def _classify_reason(event: dict) -> str:
    """
    Build a human-readable explanation of why this event was dead-lettered.
    Used as the review_reason stored in the DB.
    """
    ai         = event.get("ai_normalized", {})
    confidence = float(ai.get("confidence", 0.0))
    category   = (ai.get("category") or "unknown").lower()
    severity   = event.get("severity", "").lower()

    if confidence < 0.3:
        return (
            f"AI confidence too low ({confidence:.0%}) — "
            "model could not reliably classify this event"
        )
    if category == "unknown" and severity == "error":
        return (
            "Unclassifiable error: AI returned category='unknown' "
            f"with severity='{severity}' — likely an unseen log format"
        )
    # Fallback reason (event routed here for another reason, e.g. future rule)
    return (
        f"Dead-lettered: category='{category}', "
        f"confidence={confidence:.0%}, severity='{severity}'"
    )


def _check_spike(now: datetime) -> None:
    """
    Add the current timestamp to the rolling window and emit a WARNING
    if SPIKE_THRESHOLD events have arrived within SPIKE_WINDOW_SECS.
    Called after every dead-letter event is received.
    """
    _spike_window.append(now)

    # Drop timestamps outside the rolling window
    cutoff = now.timestamp() - SPIKE_WINDOW_SECS
    while _spike_window and _spike_window[0].timestamp() < cutoff:
        _spike_window.popleft()

    count = len(_spike_window)
    if count >= SPIKE_THRESHOLD:
        logger.warning(
            "⚠️  DEAD-LETTER SPIKE DETECTED — %d events in the last %ds "
            "(threshold: %d). Possible new unrecognised log format arriving. "
            "Check the human review queue immediately.",
            count, SPIKE_WINDOW_SECS, SPIKE_THRESHOLD,
        )


# ── Core: handle a single dead-letter event ────────────────────────────────────

async def handle_event(event: dict) -> bool:
    """
    Write one dead-letter event to TimescaleDB:

      Step 1 — Insert into normalized_events (requires_review=True).
      Step 2 — Insert into review_queue_status (status='pending').

    Both inserts run inside a single connection acquired from the pool.
    Uses ON CONFLICT DO NOTHING so Kafka re-delivery is safe.

    Returns True on success, False on failure.
    """
    global total_written

    job_id = event.get("job_id", "")
    if not job_id:
        logger.warning("Dead-letter event missing job_id — skipping: %s", event)
        return False

    # Parse timestamp
    ts_raw = event.get("timestamp", "")
    try:
        timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    except Exception:
        timestamp = datetime.now(timezone.utc)

    ai     = event.get("ai_normalized", {})
    reason = _classify_reason(event)

    # Log full context so the reason is always visible in container logs,
    # even before anyone opens the review queue UI.
    logger.warning(
        "DEAD LETTER — job=%s source=%s severity=%s "
        "category=%s confidence=%.0f%% | %s",
        job_id[:8],
        event.get("source", "unknown"),
        event.get("severity", "?"),
        ai.get("category", "unknown"),
        float(ai.get("confidence", 0.0)) * 100,
        reason,
    )

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:

            # Step 1: audit row in normalized_events
            await conn.execute(
                """
                INSERT INTO normalized_events (
                    job_id, timestamp, source, event_type, severity, message,
                    ai_category, ai_root_cause, ai_recommended_action,
                    confidence_score, requires_review, review_reason
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (job_id, timestamp) DO NOTHING
                """,
                job_id,
                timestamp,
                event.get("source",     "unknown"),
                event.get("event_type", "unknown"),
                event.get("severity",   "UNKNOWN"),
                event.get("message",    ""),
                ai.get("category",           "unknown"),
                ai.get("root_cause",         ""),
                ai.get("recommended_action", ""),
                float(ai.get("confidence",   0.0)),
                True,    # requires_review — always True for dead letters
                reason,
            )

            # Step 2: pending entry in the human review queue
            await conn.execute(
                """
                INSERT INTO review_queue_status (job_id, status)
                VALUES ($1, 'pending')
                ON CONFLICT (job_id) DO NOTHING
                """,
                job_id,
            )

        total_written += 1
        logger.info(
            "Dead-letter audited: job=%s (total written: %d)",
            job_id[:8], total_written,
        )
        return True

    except Exception as e:
        logger.error(
            "Failed to write dead-letter event job=%s: %s", job_id[:8], e
        )
        return False


# ── Kafka consumer loop ────────────────────────────────────────────────────────

async def consume_messages():
    """
    Continuously consume from logs.deadletter.
    Each message is handled immediately — no buffering.
    """
    global kafka_consumer, total_received

    try:
        kafka_consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=_deserialize,
        )

        await kafka_consumer.start()
        logger.info(
            "Dead-letter consumer started — topic=%s, "
            "spike_threshold=%d events / %ds window",
            KAFKA_TOPIC, SPIKE_THRESHOLD, SPIKE_WINDOW_SECS,
        )

        try:
            async for message in kafka_consumer:
                event = message.value
                if not event:
                    continue

                total_received += 1
                now = datetime.now(timezone.utc)

                # Spike detection runs before the DB write — we want the warning
                # even if the write subsequently fails.
                _check_spike(now)

                await handle_event(event)

        finally:
            await kafka_consumer.stop()

    except asyncio.CancelledError:
        logger.info("consume_messages task cancelled")
    except Exception as e:
        logger.error("Dead-letter consumer error: %s", e)
        await asyncio.sleep(5)


# ── FastAPI lifecycle ──────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logging.basicConfig(level=logging.INFO)

    # init_schema() is idempotent — creates tables only if they don't exist.
    await init_schema()

    global _consumer_task
    _consumer_task = asyncio.create_task(consume_messages(), name="deadletter-consumer")
    logger.info("Dead-letter consumer ready")


@app.on_event("shutdown")
async def shutdown():
    global kafka_consumer

    logger.info("Dead-letter consumer shutting down…")

    # Cancel the background task before stopping Kafka.
    if _consumer_task and not _consumer_task.done():
        _consumer_task.cancel()
        await asyncio.gather(_consumer_task, return_exceptions=True)
        logger.info("Consumer task cancelled")

    if kafka_consumer:
        await kafka_consumer.stop()
        logger.info("Kafka consumer stopped")

    await close_pool()
    logger.info("Dead-letter consumer shut down cleanly")


# ── Health endpoint ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """
    Live stats for monitoring dead-letter volume.

    total_received          — events consumed from Kafka since startup
    total_written           — events successfully written to TimescaleDB
    spike_window_count      — events in the current rolling spike window
    spike_threshold         — event count that triggers a spike warning
    spike_window_seconds    — duration of the rolling window
    spike_active            — True if currently above the spike threshold
    """
    now     = datetime.now(timezone.utc)
    cutoff  = now.timestamp() - SPIKE_WINDOW_SECS

    # Count events within the current window (deque may contain stale entries
    # if no events have arrived recently, so recount here)
    recent = sum(1 for t in _spike_window if t.timestamp() >= cutoff)

    return {
        "status":               "ok",
        "service":              "consumer-deadletter",
        "timestamp":            now.isoformat(),
        "total_received":       total_received,
        "total_written":        total_written,
        "spike_window_count":   recent,
        "spike_threshold":      SPIKE_THRESHOLD,
        "spike_window_seconds": SPIKE_WINDOW_SECS,
        "spike_active":         recent >= SPIKE_THRESHOLD,
    }


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8085)
