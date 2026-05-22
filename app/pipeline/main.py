"""
Pipeline Service - Log Processing Orchestration

Orchestrates the complete log processing pipeline:
  1. Ingest: Upload files and store in MinIO
  2. Parse: Detect format and extract structured records
  3. Normalize: AI categorization, severity detection, confidence scoring
  4. Route: Distribute to Kafka topics based on severity
  5. Persist: Store to TimescaleDB for analytics and review

Architecture:
  Upload → MinIO → Parse → Normalize → Route → Kafka + TimescaleDB
"""

import os
import logging
from typing import Optional
from datetime import datetime, timezone
from uuid import uuid4
import asyncio

from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel

from app.pipeline.ingest import ingest_log
from app.pipeline.parser import parse_log
from app.pipeline.normalizer import normalize_log
from app.pipeline.router import route_event
from app.pipeline.routes import router
from app.shared.db import (
    get_pool, close_pool, init_schema,
    insert_raw_log, insert_normalized_event, insert_event_routing,
    insert_review_queue_item, insert_trend_alert,
)
from app.shared.kafka_client import get_kafka_client, close_kafka_client, EventPriority

logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Pipeline Service")

# Mount routes
app.include_router(router)


class ProcessResult(BaseModel):
    """Result of pipeline processing."""
    job_id: str
    file_name: str
    file_format: str
    status: str  # success, partial_success, failed
    events_processed: int
    events_routed: dict  # {topic: count, ...}
    events_in_review: int
    errors: list
    timestamp: str


async def process_log_file(file_data: bytes, file_name: str, file_format: Optional[str] = None, job_id: Optional[str] = None) -> ProcessResult:
    """
    Execute the complete pipeline for a log file.

    Args:
        file_data: File content as bytes
        file_name: Original filename
        file_format: Optional format hint (JSON, CSV, XML, LOG, etc.)
        job_id: Optional pre-generated job ID (caller may need it before processing starts)

    Returns:
        ProcessResult with pipeline execution details
    """
    job_id = job_id or str(uuid4())
    start_time = datetime.now(timezone.utc)
    errors = []

    try:
        logger.info("▶▶▶ [PIPELINE] START  job_id=%s  file=%s  format=%s  bytes=%d",
                    job_id, file_name, file_format, len(file_data))
        
        # STEP 1: INGEST
        # Store file in MinIO and check for duplicates.
        # ingest_log() returns a plain tuple: (job_id, file_key, is_duplicate)
        logger.info("▶▶▶ [PIPELINE:1] INGEST starting …")
        try:
            _ingest_job_id, minio_key, is_duplicate = await ingest_log(file_data, file_name, file_format, job_id=job_id)
            if is_duplicate:
                logger.warning("▶▶▶ [PIPELINE:1] duplicate file detected: %s", file_name)
                errors.append("File is duplicate (content already processed)")

            logger.info("▶▶▶ [PIPELINE:1] INGEST OK  minio_key=%s  duplicate=%s",
                        minio_key, is_duplicate)

            # Record file metadata in raw_logs so the review UI can retrieve the
            # original file from MinIO by looking up file_name for this job_id.
            try:
                import hashlib
                file_hash = hashlib.sha256(file_data).hexdigest()
                await insert_raw_log(
                    job_id=job_id,
                    timestamp=start_time,
                    file_name=file_name,
                    file_format=file_format or "unknown",
                    raw_content="",
                    file_hash=file_hash,
                )
            except Exception as _e:
                logger.warning("▶▶▶ [PIPELINE:1] raw_logs insert failed (non-critical): %s", _e)

        except Exception as e:
            logger.exception("▶▶▶ [PIPELINE:1] INGEST FAILED: %s", e)
            errors.append(f"Ingest error: {str(e)}")
            raise
        
        # STEP 2: PARSE
        # Detect format and extract structured records
        logger.info("▶▶▶ [PIPELINE:2] PARSE starting …")
        try:
            parse_result = parse_log(file_data, file_format)
            detected_format = parse_result.get("detected_format", file_format or "UNKNOWN")
            records = parse_result.get("records", [])
            parse_errors = parse_result.get("parse_errors", [])

            if parse_errors:
                errors.extend([f"Parse warning: {err}" for err in parse_errors[:3]])

            logger.info("▶▶▶ [PIPELINE:2] PARSE OK  detected_format=%s  records=%d  parse_errors=%d",
                        detected_format, len(records), len(parse_errors))
            if not records:
                logger.warning("▶▶▶ [PIPELINE:2] PARSE returned 0 records — nothing to normalize")

        except Exception as e:
            logger.exception("▶▶▶ [PIPELINE:2] PARSE FAILED: %s", e)
            errors.append(f"Parse error: {str(e)}")
            raise
        
        # STEP 3: NORMALIZE
        # AI categorization, severity detection, confidence scoring
        logger.info("▶▶▶ [PIPELINE:3] NORMALIZE starting on %d record(s) …", len(records))
        try:
            normalize_result = await normalize_log(records)
            normalized_events = normalize_result.get("normalized_records", [])
            review_queue_items = normalize_result.get("review_queue_items", [])
            trend_alerts_detected = normalize_result.get("trend_alerts", [])

            logger.info("▶▶▶ [PIPELINE:3] NORMALIZE OK  normalized=%d  review_queue=%d",
                        len(normalized_events), len(review_queue_items))
            for i, ev in enumerate(normalized_events):
                ai = ev.get("ai_normalized", {})
                logger.info(
                    "▶▶▶ [PIPELINE:3]   event[%d]  source=%s  severity=%s  "
                    "category=%s  confidence=%.2f  requires_review=%s",
                    i, ev.get("source"), ev.get("severity"),
                    ai.get("category"), float(ai.get("confidence", 0)),
                    ev.get("requires_review"),
                )

        except Exception as e:
            logger.exception("▶▶▶ [PIPELINE:3] NORMALIZE FAILED: %s", e)
            errors.append(f"Normalize error: {str(e)}")
            raise
        
        # STEP 4: ROUTE
        # Distribute to Kafka topics based on severity
        logger.info("▶▶▶ [PIPELINE:4] ROUTE starting on %d event(s) …", len(normalized_events))
        try:
            routed_counts = {}
            routing_results = []

            for event in normalized_events:
                route_result = route_event(event, job_id)
                routing_results.append((event, route_result))

                topic = route_result.get("topic")
                if topic:
                    routed_counts[topic] = routed_counts.get(topic, 0) + 1

            logger.info(
                "▶▶▶ [PIPELINE:4] ROUTE OK  p0=%d  p1=%d  p2=%d  dl=%d  review=%d",
                routed_counts.get("logs.p0", 0), routed_counts.get("logs.p1", 0),
                routed_counts.get("logs.p2", 0), routed_counts.get("logs.deadletter", 0),
                len(normalized_events) - sum(routed_counts.values()),
            )

        except Exception as e:
            logger.exception("▶▶▶ [PIPELINE:4] ROUTE FAILED: %s", e)
            errors.append(f"Routing error: {str(e)}")
            raise
        
        # STEP 4.5: SEND TO KAFKA
        # Send routed events to appropriate Kafka topics
        try:
            kafka_client = await get_kafka_client()
            kafka_sent_count = 0
            
            for event, route_result in routing_results:
                topic = route_result.get("topic")
                
                if not topic:
                    continue  # Review queue items or failed routing
                
                try:
                    # Map topic string to EventPriority enum
                    priority = EventPriority(topic)
                    source = event.get("source", "unknown")
                    event_to_send = {**event, "job_id": job_id}
                    await kafka_client.send_event(priority, event_to_send, key=source)
                    kafka_sent_count += 1
                except (ValueError, Exception) as e:
                    logger.warning(f"Failed to send event to Kafka topic {topic}: {e}")
            
            logger.info(f"Kafka: Sent {kafka_sent_count} events to topics")
            
        except Exception as e:
            logger.warning(f"Kafka send failed (non-critical): {e}")
            # Don't raise - Kafka unavailability shouldn't block the pipeline
        
        # STEP 5: PERSIST
        # Save to TimescaleDB for analytics and review
        logger.info("▶▶▶ [PIPELINE:5] PERSIST starting — job_id=%s  events=%d  review=%d",
                    job_id, len(normalized_events), len(review_queue_items))
        try:
            now = datetime.now(timezone.utc)
            ne_ok = 0
            ne_fail = 0

            # Insert normalized events
            for i, event in enumerate(normalized_events):
                try:
                    ai = event.get("ai_normalized", {})
                    ok = await insert_normalized_event(
                        job_id=job_id,
                        timestamp=now,
                        source=event.get("source", "unknown"),
                        event_type=event.get("event_type", "unknown"),
                        severity=event.get("severity", "INFO"),
                        message=event.get("message", ""),
                        ai_category=ai.get("category", ""),
                        ai_root_cause=ai.get("root_cause", ""),
                        ai_recommended_action=ai.get("recommended_action", ""),
                        confidence_score=float(ai.get("confidence", 0.0)),
                        requires_review=event.get("requires_review", False),
                        review_reason=event.get("review_reason") or "",
                        novelty_score=event.get("novelty_score"),
                    )
                    if ok:
                        ne_ok += 1
                        logger.info("▶▶▶ [PIPELINE:5]   normalized_events INSERT OK  idx=%d  job_id=%s", i, job_id)
                    else:
                        ne_fail += 1
                        logger.error("▶▶▶ [PIPELINE:5]   normalized_events INSERT FAILED (returned False)  idx=%d  job_id=%s", i, job_id)
                except Exception as e:
                    ne_fail += 1
                    logger.exception("▶▶▶ [PIPELINE:5]   normalized_events INSERT EXCEPTION  idx=%d: %s", i, e)

            logger.info("▶▶▶ [PIPELINE:5] normalized_events done  ok=%d  failed=%d", ne_ok, ne_fail)

            # Insert event routing
            for topic, count in routed_counts.items():
                try:
                    await insert_event_routing(job_id=job_id, kafka_topic=topic)
                    logger.info("▶▶▶ [PIPELINE:5]   event_routing INSERT OK  topic=%s", topic)
                except Exception as e:
                    logger.exception("▶▶▶ [PIPELINE:5]   event_routing INSERT EXCEPTION  topic=%s: %s", topic, e)

            # Insert review queue items
            rq_ok = 0
            for i, review_item in enumerate(review_queue_items):
                try:
                    await insert_review_queue_item(job_id=job_id)
                    rq_ok += 1
                    logger.info("▶▶▶ [PIPELINE:5]   review_queue INSERT OK  idx=%d  job_id=%s", i, job_id)
                except Exception as e:
                    logger.exception("▶▶▶ [PIPELINE:5]   review_queue INSERT EXCEPTION  idx=%d: %s", i, e)

            logger.info("▶▶▶ [PIPELINE:5] PERSIST done  ne_ok=%d  ne_fail=%d  rq_ok=%d",
                        ne_ok, ne_fail, rq_ok)

            # Persist temporal anomaly alerts
            for alert in trend_alerts_detected:
                try:
                    await insert_trend_alert(alert)
                except Exception as e:
                    logger.warning("▶▶▶ [PIPELINE:5] trend_alert INSERT failed: %s", e)

        except Exception as e:
            logger.exception("▶▶▶ [PIPELINE:5] PERSIST outer EXCEPTION: %s", e)
            errors.append(f"Database error: {str(e)}")
            # Don't raise — database errors shouldn't block the pipeline
        
        # SUCCESS
        status = "partial_success" if errors else "success"
        logger.info(f"Pipeline COMPLETE: job_id={job_id}, status={status}")
        
        return ProcessResult(
            job_id=job_id,
            file_name=file_name,
            file_format=detected_format,
            status=status,
            events_processed=len(normalized_events),
            events_routed=routed_counts,
            events_in_review=len(review_queue_items),
            errors=errors,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        
    except Exception as e:
        logger.error(f"Pipeline FAILED: job_id={job_id}, error={e}")
        return ProcessResult(
            job_id=job_id,
            file_name=file_name,
            file_format=file_format or "UNKNOWN",
            status="failed",
            events_processed=0,
            events_routed={},
            events_in_review=0,
            errors=errors + [str(e)],
            timestamp=datetime.now(timezone.utc).isoformat()
        )


@app.post("/process", response_model=ProcessResult)
async def process(file: UploadFile = File(...), format: Optional[str] = None):
    """
    Upload and process a log file through the complete pipeline.
    
    Args:
        file: Log file (JSON, CSV, XML, LOG, TXT, etc.)
        format: Optional format hint
    
    Returns:
        ProcessResult with pipeline execution details
    """
    try:
        # Read file
        file_data = await file.read()
        
        if not file_data:
            raise HTTPException(status_code=400, detail="File is empty")
        
        # Process through pipeline
        result = await process_log_file(file_data, file.filename, format)
        
        # Return appropriate status code
        status_code = 200 if result.status == "success" else (206 if result.status == "partial_success" else 400)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload handler error: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "pipeline",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/status/{job_id}")
async def status(job_id: str):
    """
    Get pipeline execution status for a job.
    
    Note: In production, this would query a job status table.
    For now, we return a placeholder.
    """
    return {
        "job_id": job_id,
        "status": "Processing status tracking not yet implemented",
        "note": "Implement job tracking table for persistent status queries"
    }


async def startup():
    """Initialize database schema on startup."""
    try:
        await init_schema()
        logger.info("Database schema initialized")
    except Exception as e:
        logger.error(f"Failed to initialize schema: {e}")
        # Continue - schema may already exist


async def shutdown():
    """Cleanup on shutdown."""
    await close_pool()
    await close_kafka_client()
    logger.info("Database pool and Kafka client closed")


# Register startup/shutdown events
app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
