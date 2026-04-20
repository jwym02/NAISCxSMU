"""
Cold Path Consumer — Batch Processing of P2 (Non-Urgent) Events
================================================================
Consumes: logs.p2
Strategy: accumulate events in memory, then bulk INSERT into TimescaleDB
          when EITHER of two triggers fires:

    Trigger 1 — Buffer full:   buffer reaches COLD_BATCH_SIZE (default 100)
    Trigger 2 — Time window:   COLD_BATCH_INTERVAL_SECONDS have elapsed
                                since the last flush (default 300 s / 5 min)

Two concurrent asyncio tasks share one buffer protected by a single Lock:

    Task A  consume_messages()  ← Kafka loop; appends events; flush on full
    Task B  timer_loop()        ← wakes every 30 s; flush if window elapsed

Bulk insert uses asyncpg executemany() — one round-trip for up to 100 rows.
ON CONFLICT DO NOTHING handles Kafka at-least-once re-delivery safely.

Entry:  python -m app.consumers.cold_path
Port:   8084  (health endpoint only — add port mapping to docker-compose if needed)
"""

import os
import json
import gzip
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer

from app.shared.db import get_pool, init_schema, close_pool
from app.shared.kafka_client import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC_COLD",             "logs.p2")
KAFKA_GROUP      = os.getenv("KAFKA_GROUP_ID_COLD",          "consumer-group-cold")
BATCH_SIZE       = int(os.getenv("COLD_BATCH_SIZE",          "100"))
BATCH_INTERVAL   = int(os.getenv("COLD_BATCH_INTERVAL_SECONDS", "300"))  # 5 min
TIMER_CHECK_SECS = 30   # how often the timer task wakes to check the clock

# ── Shared mutable state ───────────────────────────────────────────────────────
# ALL access to buffer and last_flush_time must hold buffer_lock.
buffer:          list          = []
buffer_lock:     asyncio.Lock  = asyncio.Lock()
last_flush_time: datetime      = datetime.now(timezone.utc)
total_flushed:   int           = 0

# ── FastAPI app & Kafka handle ─────────────────────────────────────────────────
app = FastAPI(title="Cold Path Consumer")
kafka_consumer: Optional[AIOKafkaConsumer] = None
_tasks: list = []  # holds task handles so they can be cancelled on shutdown


# ── Helpers ────────────────────────────────────────────────────────────────────

def _deserialize(data: bytes) -> dict:
    """
    Deserialize a Kafka message value.
    Tries gzip first (aiokafka decompresses at protocol level; this is a
    safety net), then falls back to plain UTF-8 JSON.
    """
    try:
        return json.loads(gzip.decompress(data).decode("utf-8"))
    except (OSError, EOFError):
        try:
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to deserialize message: {e}")
            return {}


def _event_to_row(event: dict) -> Optional[tuple]:
    """
    Convert a Kafka event dict into a parameter tuple for the normalized_events
    INSERT, in this exact column order:

      job_id, timestamp, source, event_type, severity, message,
      ai_category, ai_root_cause, ai_recommended_action,
      confidence_score, requires_review, review_reason

    The ai_* fields live inside the nested 'ai_normalized' dict — same shape
    that hot_path uses.  Returns None for events too malformed to store.
    """
    try:
        job_id = event.get("job_id", "")
        if not job_id:
            logger.warning("Skipping event with missing job_id")
            return None

        ts_raw = event.get("timestamp", "")
        try:
            timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except Exception:
            timestamp = datetime.now(timezone.utc)

        ai = event.get("ai_normalized", {})

        return (
            job_id,
            timestamp,
            event.get("source",     "unknown"),
            event.get("event_type", "unknown"),
            event.get("severity",   "INFO"),
            event.get("message",    ""),
            ai.get("category",           "unknown"),
            ai.get("root_cause",         ""),
            ai.get("recommended_action", ""),
            float(ai.get("confidence",   0.0)),
            False,   # requires_review — P2 events cleared the confidence threshold
            None,    # review_reason
        )
    except Exception as e:
        logger.warning(f"Skipping malformed event ({e}): {event}")
        return None


# ── Core: flush the buffer ─────────────────────────────────────────────────────

async def flush_buffer() -> int:
    """
    Bulk INSERT all buffered events into TimescaleDB, then reset the buffer
    and the last_flush_time clock.

    MUST be called while buffer_lock is already held by the caller — this
    prevents the consumer loop and timer loop from flushing simultaneously.

    Returns the number of rows successfully written (0 on failure or empty).
    """
    global buffer, last_flush_time, total_flushed

    if not buffer:
        last_flush_time = datetime.now(timezone.utc)
        return 0

    # Snapshot and immediately clear the buffer.
    # New events can be appended again as soon as we release the lock,
    # even while the DB write is in progress.
    snapshot        = buffer[:]
    buffer          = []
    last_flush_time = datetime.now(timezone.utc)

    # Build valid row tuples; silently drop malformed events.
    rows  = [_event_to_row(e) for e in snapshot]
    rows  = [r for r in rows if r is not None]
    count = len(rows)

    if count == 0:
        logger.warning("Flush: all %d events were malformed — nothing written", len(snapshot))
        return 0

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO normalized_events (
                    job_id, timestamp, source, event_type, severity, message,
                    ai_category, ai_root_cause, ai_recommended_action,
                    confidence_score, requires_review, review_reason
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (job_id, timestamp) DO NOTHING
                """,
                rows,
            )

        total_flushed += count
        logger.info(
            "Flushed %d P2 events to TimescaleDB (lifetime total: %d)",
            count, total_flushed,
        )
        return count

    except Exception as e:
        logger.error("Bulk insert failed (%d events): %s", count, e)
        # Put the snapshot back at the front of the buffer so it will be
        # retried on the next flush rather than silently dropped.
        buffer = snapshot + buffer
        return 0


# ── Task A: Kafka consumer loop ────────────────────────────────────────────────

async def consume_messages():
    """
    Continuously consume from logs.p2.

    Each message is appended to the shared buffer inside buffer_lock.
    The flush check also happens inside the same lock acquisition so there
    is no window for the timer task to flush a partially-updated buffer.
    """
    global kafka_consumer

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
            "Cold consumer started — topic=%s, batch_size=%d, interval=%ds",
            KAFKA_TOPIC, BATCH_SIZE, BATCH_INTERVAL,
        )

        try:
            async for message in kafka_consumer:
                event = message.value
                if not event:
                    continue

                async with buffer_lock:
                    buffer.append(event)
                    current = len(buffer)

                    logger.debug(
                        "Buffered [%d/%d] source=%s job=%s…",
                        current, BATCH_SIZE,
                        event.get("source", "?"),
                        str(event.get("job_id", "?"))[:8],
                    )

                    # Trigger 1: buffer reached batch size — flush immediately,
                    # still inside the lock so timer_loop cannot interleave.
                    if current >= BATCH_SIZE:
                        logger.info(
                            "Buffer full (%d/%d) — flushing now", current, BATCH_SIZE
                        )
                        await flush_buffer()

        finally:
            await kafka_consumer.stop()

    except asyncio.CancelledError:
        logger.info("consume_messages task cancelled")
    except Exception as e:
        logger.error("Kafka consumer error: %s", e)
        await asyncio.sleep(5)   # back-off before Docker restarts the container


# ── Task B: Timer loop ─────────────────────────────────────────────────────────

async def timer_loop():
    """
    Background task that checks every TIMER_CHECK_SECS seconds whether the
    time-window flush threshold (BATCH_INTERVAL) has elapsed.

    If time has elapsed AND the buffer is non-empty, it triggers a flush.
    This ensures that in quiet periods (fewer than BATCH_SIZE events arriving
    within 5 minutes) data still reaches the DB within a predictable window.
    """
    logger.info(
        "Timer loop started — checking every %ds, flush window=%ds",
        TIMER_CHECK_SECS, BATCH_INTERVAL,
    )

    while True:
        try:
            await asyncio.sleep(TIMER_CHECK_SECS)

            now     = datetime.now(timezone.utc)
            elapsed = (now - last_flush_time).total_seconds()

            if elapsed >= BATCH_INTERVAL:
                async with buffer_lock:
                    pending = len(buffer)
                    if pending > 0:
                        # Trigger 2: window elapsed with data waiting
                        logger.info(
                            "Time window elapsed (%.0fs >= %ds) — flushing %d event(s)",
                            elapsed, BATCH_INTERVAL, pending,
                        )
                        await flush_buffer()
                    else:
                        # Window elapsed but buffer is empty; just reset the clock.
                        # We hold the lock briefly to keep last_flush_time consistent.
                        globals()["last_flush_time"] = now
                        logger.debug("Time window elapsed, buffer empty — clock reset")

        except asyncio.CancelledError:
            logger.info("timer_loop task cancelled")
            break
        except Exception as e:
            # Log and continue — a timer error must not kill the consumer
            logger.error("Timer loop error (continuing): %s", e)


# ── FastAPI lifecycle ──────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logging.basicConfig(level=logging.INFO)

    # init_schema() is idempotent — safe to call on every startup.
    # It creates tables/hypertables only if they don't already exist.
    await init_schema()

    _tasks.append(asyncio.create_task(consume_messages(), name="cold-consumer"))
    _tasks.append(asyncio.create_task(timer_loop(),       name="cold-timer"))

    logger.info("Cold path consumer ready")


@app.on_event("shutdown")
async def shutdown():
    """
    Graceful shutdown sequence:
      1. Stop the Kafka consumer (no more messages arrive)
      2. Final flush of whatever remains in the buffer
      3. Close the DB connection pool
    """
    global kafka_consumer

    logger.info("Cold path consumer shutting down…")

    # Cancel background tasks first so they stop producing work.
    for task in _tasks:
        task.cancel()
    if _tasks:
        await asyncio.gather(*_tasks, return_exceptions=True)
        logger.info("Background tasks cancelled")

    # Stop Kafka so no new events arrive during the final flush.
    if kafka_consumer:
        await kafka_consumer.stop()
        logger.info("Kafka consumer stopped")

    async with buffer_lock:
        remaining = len(buffer)
        if remaining > 0:
            logger.info("Shutdown flush: %d events still buffered", remaining)
            await flush_buffer()
        else:
            logger.info("Shutdown: buffer empty, nothing to flush")

    await close_pool()
    logger.info("Cold path consumer shut down cleanly")


# ── Health endpoint ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """
    Live stats for monitoring batch behaviour.

    buffer_size              — events currently waiting in memory
    batch_size               — maximum before an immediate flush
    batch_interval_seconds   — time-window flush threshold
    seconds_since_last_flush — age of the oldest un-flushed data
    next_forced_flush_in     — seconds until time-window trigger fires
    total_flushed            — cumulative events written since startup
    """
    now     = datetime.now(timezone.utc)
    elapsed = (now - last_flush_time).total_seconds()

    return {
        "status":                   "ok",
        "service":                  "consumer-cold",
        "timestamp":                now.isoformat(),
        "buffer_size":              len(buffer),
        "batch_size":               BATCH_SIZE,
        "batch_interval_seconds":   BATCH_INTERVAL,
        "seconds_since_last_flush": round(elapsed, 1),
        "next_forced_flush_in":     max(0.0, round(BATCH_INTERVAL - elapsed, 1)),
        "total_flushed":            total_flushed,
    }


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8084)
