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
import time
from collections import deque
from typing import Any, Dict, List, Optional

from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.pipeline.ingest import ingest_log
from app.pipeline.parser import parse_log
from app.pipeline.normalizer import normalize_log
from app.pipeline.router import route_event
from app.shared.text_tokens import (
    TOKENIZER_VERSION,
    build_event_search_text,
    estimate_token_count,
)
from app.shared.db import (
    get_pool, close_pool, init_schema,
    insert_raw_log, insert_normalized_event, insert_event_routing,
    insert_review_queue_item
)
from app.shared.kafka_client import get_kafka_client, close_kafka_client, EventPriority

logger = logging.getLogger(__name__)

PREVIEW_MAX_BYTES = int(os.getenv("PREVIEW_MAX_BYTES", "524288"))
PREVIEW_MAX_RECORDS = int(os.getenv("PREVIEW_MAX_RECORDS", "500"))
PREVIEW_RPM = int(os.getenv("PREVIEW_RPM", "120"))
_preview_hits: deque[float] = deque()

# FastAPI app
app = FastAPI(title="Pipeline Service")

_cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    records_preview: Optional[List[Dict[str, Any]]] = None


class ParsePreviewIn(BaseModel):
    text: str
    format: Optional[str] = None


class ParsePreviewOut(BaseModel):
    detected_format: str
    record_count: int
    records: List[Dict[str, Any]]
    parse_errors: List[Any]
    input_char_count: int
    input_approx_tokens: float
    tokenizer_version: str


def _preview_rate_limit() -> None:
    now = time.time()
    while _preview_hits and _preview_hits[0] < now - 60:
        _preview_hits.popleft()
    if len(_preview_hits) >= PREVIEW_RPM:
        raise HTTPException(
            status_code=429,
            detail="Preview rate limit exceeded; try again in a minute",
        )
    _preview_hits.append(now)


def _compact_event_for_preview(ev: Dict[str, Any]) -> Dict[str, Any]:
    ai = ev.get("ai_normalized") or {}
    msg = ev.get("message") or ""
    if len(msg) > 2000:
        msg = msg[:2000] + "…"
    out: Dict[str, Any] = {
        "timestamp": ev.get("timestamp"),
        "source": ev.get("source"),
        "event_type": ev.get("event_type"),
        "severity": ev.get("severity"),
        "message": msg,
        "ai_category": ai.get("category"),
        "confidence": ai.get("confidence"),
    }
    if ev.get("line_no") is not None:
        out["line_no"] = ev.get("line_no")
    return out


async def process_log_file(file_data: bytes, file_name: str, file_format: Optional[str] = None) -> ProcessResult:
    """
    Execute the complete pipeline for a log file.
    
    Args:
        file_data: File content as bytes
        file_name: Original filename
        file_format: Optional format hint (JSON, CSV, XML, LOG, etc.)
    
    Returns:
        ProcessResult with pipeline execution details
    """
    job_id = str(uuid4())
    start_time = datetime.now(timezone.utc)
    errors = []
    
    try:
        logger.info(f"Pipeline START: job_id={job_id}, file={file_name}")
        
        # STEP 1: INGEST
        # Store file in MinIO and check for duplicates
        try:
            ingest_result = ingest_log(file_data, file_name, file_format)
            if ingest_result.get("is_duplicate"):
                logger.warning(f"Duplicate file detected: {file_name}")
                errors.append("File is duplicate (content already processed)")

            minio_key = ingest_result.get("file_key")
            logger.info(f"Ingested: {file_name} → MinIO key={minio_key}")
            
        except Exception as e:
            logger.error(f"Ingest failed: {e}")
            errors.append(f"Ingest error: {str(e)}")
            raise
        
        # STEP 2: PARSE
        # Detect format and extract structured records
        try:
            parse_result = parse_log(file_data, file_format)
            detected_format = parse_result.get("detected_format", file_format or "UNKNOWN")
            records = parse_result.get("records", [])
            parse_errors = parse_result.get("parse_errors", [])
            
            if parse_errors:
                errors.extend([f"Parse warning: {err}" for err in parse_errors[:3]])
            
            logger.info(f"Parsed: {len(records)} records from {detected_format} file")
            
        except Exception as e:
            logger.error(f"Parse failed: {e}")
            errors.append(f"Parse error: {str(e)}")
            raise
        
        # STEP 3: NORMALIZE
        # AI categorization, severity detection, confidence scoring
        try:
            normalize_result = normalize_log(records)
            normalized_events = normalize_result.get("normalized_records", [])   # key is "normalized_records"
            review_queue_items = normalize_result.get("review_queue_items", [])
            
            logger.info(f"Normalized: {len(normalized_events)} events, {len(review_queue_items)} need review")
            
        except Exception as e:
            logger.error(f"Normalize failed: {e}")
            errors.append(f"Normalize error: {str(e)}")
            raise
        
        # STEP 4: ROUTE
        # Distribute to Kafka topics based on severity
        try:
            routed_counts = {}
            routing_results = []
            
            for event in normalized_events:
                route_result = route_event(event, job_id)
                routing_results.append((event, route_result))  # Store event with routing result
                
                topic = route_result.get("topic")
                if topic:
                    routed_counts[topic] = routed_counts.get(topic, 0) + 1
            
            logger.info(f"Routed: P0={routed_counts.get('logs.p0', 0)}, "
                       f"P1={routed_counts.get('logs.p1', 0)}, "
                       f"P2={routed_counts.get('logs.p2', 0)}, "
                       f"Deadletter={routed_counts.get('logs.deadletter', 0)}")
            
        except Exception as e:
            logger.error(f"Routing failed: {e}")
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
                    await kafka_client.send_event(priority, event, key=source)
                    kafka_sent_count += 1
                except (ValueError, Exception) as e:
                    logger.warning(f"Failed to send event to Kafka topic {topic}: {e}")
            
            logger.info(f"Kafka: Sent {kafka_sent_count} events to topics")
            
        except Exception as e:
            logger.warning(f"Kafka send failed (non-critical): {e}")
            # Don't raise - Kafka unavailability shouldn't block the pipeline
        
        # STEP 5: PERSIST
        # Save to TimescaleDB for analytics and review
        try:
            now = datetime.now(timezone.utc)
            
            # Insert normalized events
            for event in normalized_events:
                try:
                    ai = event.get("ai_normalized", {})
                    st = build_event_search_text(
                        event.get("source", "unknown"),
                        event.get("event_type", "unknown"),
                        event.get("message", ""),
                        str(ai.get("category", "") or ""),
                    )
                    await insert_normalized_event(
                        job_id=job_id,                                      # use pipeline's job_id
                        timestamp=now,
                        source=event.get("source", "unknown"),
                        event_type=event.get("event_type", "unknown"),
                        severity=event.get("severity", "INFO"),
                        message=event.get("message", ""),
                        ai_category=ai.get("category", ""),                 # nested under ai_normalized
                        ai_root_cause=ai.get("root_cause", ""),
                        ai_recommended_action=ai.get("recommended_action", ""),
                        confidence_score=float(ai.get("confidence", 0.0)),
                        requires_review=event.get("requires_review", False),
                        review_reason=event.get("review_reason") or "",
                        search_text=st or None,
                    )
                except Exception as e:
                    logger.warning(f"Failed to insert event: {e}")

            # Insert event routing
            for topic, count in routed_counts.items():
                try:
                    await insert_event_routing(
                        job_id=job_id,                                      # use pipeline's job_id
                        kafka_topic=topic
                    )
                except Exception as e:
                    logger.warning(f"Failed to record routing: {e}")

            # Insert review queue items
            for review_item in review_queue_items:
                try:
                    await insert_review_queue_item(
                        job_id=job_id                                       # use pipeline's job_id
                    )
                except Exception as e:
                    logger.warning(f"Failed to add review item: {e}")
            
            logger.info(f"Persisted: {len(normalized_events)} events, {len(review_queue_items)} review items")
            
        except Exception as e:
            logger.error(f"Persistence failed: {e}")
            errors.append(f"Database error: {str(e)}")
            # Don't raise - database errors shouldn't block the pipeline
        
        # SUCCESS
        status = "partial_success" if errors else "success"
        logger.info(f"Pipeline COMPLETE: job_id={job_id}, status={status}")

        preview_cap = int(os.getenv("PROCESS_RECORDS_PREVIEW_MAX", "25"))
        records_preview = [
            _compact_event_for_preview(e)
            for e in normalized_events[:preview_cap]
        ]
        
        return ProcessResult(
            job_id=job_id,
            file_name=file_name,
            file_format=detected_format,
            status=status,
            events_processed=len(normalized_events),
            events_routed=routed_counts,
            events_in_review=len(review_queue_items),
            errors=errors,
            timestamp=datetime.now(timezone.utc).isoformat(),
            records_preview=records_preview,
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
            timestamp=datetime.now(timezone.utc).isoformat(),
            records_preview=None,
        )


@app.post("/preview", response_model=ParsePreviewOut)
async def parse_preview(body: ParsePreviewIn):
    """
    Parse-only: no MinIO, Kafka, or normalization. Rate-limited; size-capped.
    """
    _preview_rate_limit()
    raw = body.text or ""
    data = raw.encode("utf-8")
    if len(data) > PREVIEW_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Preview body exceeds limit ({PREVIEW_MAX_BYTES} bytes)",
        )
    out = parse_log(data, body.format or "")
    recs = out["records"][:PREVIEW_MAX_RECORDS]
    return ParsePreviewOut(
        detected_format=out["detected_format"],
        record_count=len(out["records"]),
        records=recs,
        parse_errors=out["parse_errors"],
        input_char_count=len(raw),
        input_approx_tokens=estimate_token_count(raw),
        tokenizer_version=TOKENIZER_VERSION,
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
