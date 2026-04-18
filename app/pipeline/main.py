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
from app.shared.db import (
    get_pool, close_pool, init_schema,
    insert_raw_log, insert_normalized_event, insert_event_routing,
    insert_review_queue_item
)
from app.shared.kafka_client import get_kafka_client, close_kafka_client, EventPriority

logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Pipeline Service")


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
                        review_reason=event.get("review_reason") or ""
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
