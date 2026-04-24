"""
HTTP API for the Pipeline service.
All routes are registered on a FastAPI APIRouter and mounted in main.py.
"""

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import datetime, timezone
import logging

from pydantic import BaseModel
from app.shared.kafka_client import get_kafka_client
from app.shared.dynamo import update_feedback_rule
from app.shared.db import (
    get_review_queue_pending,
    get_event_with_routing,
    update_review_status,
    get_pool,
    get_event_stats,
    get_events_timeseries,
)

log = logging.getLogger(__name__)


class ReviewRequest(BaseModel):
    decision: str
    notes: Optional[str] = None
    category: Optional[str] = None   # AI-assigned (or corrected) category — used for DynamoDB feedback


def _flatten_event(item: dict) -> dict:
    """Flatten nested ai_normalized structure to root level."""
    flattened = {k: v for k, v in item.items() if k != "ai_normalized"}

    ai_data = item.get("ai_normalized", {})
    if isinstance(ai_data, dict):
        flattened["ai_category"] = ai_data.get("category", "unknown")
        flattened["ai_root_cause"] = ai_data.get("root_cause", "unknown")
        flattened["ai_recommended_action"] = ai_data.get("recommended_action", "")
        flattened["confidence_score"] = ai_data.get("confidence", 0.0)
    else:
        # Fields are already flat (asyncpg records from normalized_events)
        flattened["ai_category"] = item.get("ai_category", "unknown")
        flattened["ai_root_cause"] = item.get("ai_root_cause", "unknown")
        flattened["ai_recommended_action"] = item.get("ai_recommended_action", "")
        flattened["confidence_score"] = item.get("confidence_score", 0.0)

    return flattened


router = APIRouter()


# ── ENDPOINT 1: POST /logs/upload ─────────────────────────────────────────────

@router.post("/logs/upload")
async def upload_log(
    file: UploadFile = File(...),
    file_format: Optional[str] = Form(None),
):
    """Accept a log file, run the full processing pipeline, return a job_id."""
    log.info("▶▶▶ [UPLOAD] handler entered — file=%s  format=%s", file.filename, file_format)
    try:
        contents = await file.read()
        log.info("▶▶▶ [UPLOAD] read %d bytes from %s", len(contents), file.filename)

        if not contents:
            log.warning("▶▶▶ [UPLOAD] file is empty — rejecting")
            raise HTTPException(status_code=400, detail="File is empty")

        # Lazy import avoids the circular dependency:
        #   main.py  (imports router from routes.py)  →  routes.py  →  main.py
        log.info("▶▶▶ [UPLOAD] importing process_log_file …")
        from app.pipeline.main import process_log_file
        log.info("▶▶▶ [UPLOAD] calling process_log_file …")

        result = await process_log_file(contents, file.filename, file_format)

        log.info(
            "▶▶▶ [UPLOAD] process_log_file returned — job_id=%s  status=%s  "
            "events_processed=%s  errors=%s",
            result.job_id, result.status, result.events_processed, result.errors,
        )

        if result.status == "failed":
            log.error("▶▶▶ [UPLOAD] pipeline FAILED — returning 500")
            raise HTTPException(
                status_code=500,
                detail=f"Processing failed: {result.errors}",
            )

        log.info("▶▶▶ [UPLOAD] returning job_id=%s to client", result.job_id)
        return {
            "job_id": result.job_id,
            "status": "accepted",
            "file_name": file.filename,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        log.exception("▶▶▶ [UPLOAD] unhandled exception: %s", e)
        raise HTTPException(status_code=500, detail="Error uploading log file")


# ── ENDPOINT 2: GET /jobs/{job_id} ────────────────────────────────────────────

@router.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Return current status and results for a given job."""
    # FIX: get_pool() is async — must be awaited; also added raw_logs query for file_name
    try:
        pool = await get_pool()
        if not pool:
            raise HTTPException(status_code=503, detail="Database connection not available")

        event_info = await get_event_with_routing(job_id)
        if not event_info:
            raise HTTPException(status_code=404, detail="Job ID not found")

        event_info = _flatten_event(event_info)

        # get_event_with_routing doesn't join raw_logs, so query it separately for file_name
        file_name = None
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT file_name FROM raw_logs WHERE job_id = $1 LIMIT 1", job_id
            )
            if row:
                file_name = row["file_name"]

        if isinstance(event_info.get("timestamp"), datetime):
            event_info["timestamp"] = event_info["timestamp"].isoformat()

        return {
            "job_id": job_id,
            "file_name": file_name,
            "kafka_topic": event_info.get("kafka_topic"),
            "review_status": event_info.get("review_status"),
            "timestamp": event_info.get("timestamp"),
            "source": event_info.get("source"),
            "severity": event_info.get("severity"),
            "ai_category": event_info.get("ai_category"),
            "ai_root_cause": event_info.get("ai_root_cause"),
            "ai_recommended_action": event_info.get("ai_recommended_action"),
            "confidence_score": event_info.get("confidence_score"),
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching job details")


# ── ENDPOINT 3: GET /queue ────────────────────────────────────────────────────

@router.get("/queue")
async def get_review_queue():
    """List all pending review queue items with their associated events."""
    try:
        pending_items = await get_review_queue_pending()
        flattened_items = []

        for item in pending_items:
            item = _flatten_event(item)
            if isinstance(item.get("timestamp"), datetime):
                item["timestamp"] = item["timestamp"].isoformat()
            if isinstance(item.get("created_at"), datetime):
                item["created_at"] = item["created_at"].isoformat()
            flattened_items.append(item)

        return {
            "total_items": len(flattened_items),
            "items": flattened_items,
        }

    except Exception as e:
        logging.error(f"Error fetching review queue: {e}")
        raise HTTPException(status_code=500, detail="Error fetching review queue")


# ── ENDPOINT 4: POST /queue/{job_id}/review ───────────────────────────────────

@router.post("/queue/{job_id}/review")
async def review_queue_item(job_id: str, review: ReviewRequest):
    """Accept a human decision (approved/rejected) on a queued item."""
    try:
        if review.decision not in ["approved", "rejected"]:
            raise HTTPException(status_code=400, detail="decision must be 'approved' or 'rejected'")

        is_updated = await update_review_status(job_id, review.decision, review.notes)

        # ── DynamoDB FEEDBACK LOOP ─────────────────────────────────────────────
        # Regardless of whether the DB row was updated (idempotent re-reviews are
        # fine to re-record), persist the human decision back to DynamoDB so that
        # the normalizer's confidence_boost adjusts on the next pipeline run.
        event_for_feedback = await get_event_with_routing(job_id)
        if event_for_feedback:
            event_for_feedback = _flatten_event(event_for_feedback)
            source   = event_for_feedback.get("source", "unknown")
            # Prefer the category sent by the reviewer (corrected label); fall back
            # to whatever AI assigned when the event was first processed.
            category = review.category or event_for_feedback.get("ai_category", "unknown")
            approved = review.decision == "approved"
            update_feedback_rule(source, category, approved)

        # ── KAFKA FORWARD (approved events only) ──────────────────────────────
        if is_updated and review.decision == "approved" and event_for_feedback:
            severity = event_for_feedback.get("severity", "ERROR").upper()
            kafka_client = await get_kafka_client()
            if kafka_client:
                if severity == "CRITICAL":
                    await kafka_client.send_p0_event(event_for_feedback, key=event_for_feedback.get("source", "unknown"))
                else:
                    await kafka_client.send_p1_event(event_for_feedback, key=event_for_feedback.get("source", "unknown"))

        return {
            "job_id": job_id,
            "decision": review.decision,
            "message": f"Review status updated to {review.decision}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error processing review for {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Error processing review")


# ── ENDPOINT 5: GET /stats ────────────────────────────────────────────────────

@router.get("/stats")
async def get_stats():
    """Return aggregate dashboard statistics (events processed, in review, errors, severity breakdown)."""
    try:
        stats = await get_event_stats()
        return stats
    except Exception as e:
        logging.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Error fetching stats")


# ── ENDPOINT 6: GET /events/timeseries ────────────────────────────────────────

@router.get("/events/timeseries")
async def get_timeseries(hours: int = 12):
    """Return hourly event counts grouped by severity for the last N hours (1–168)."""
    try:
        if not (1 <= hours <= 168):
            raise HTTPException(status_code=400, detail="hours must be between 1 and 168")
        data = await get_events_timeseries(hours)
        return {"hours": hours, "data": data}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching timeseries: {e}")
        raise HTTPException(status_code=500, detail="Error fetching timeseries")


# ── ENDPOINT 7: GET /health ───────────────────────────────────────────────────

@router.get("/health")
async def health():
    """Report connectivity to every downstream service."""
    # FIX: await get_pool() and await get_kafka_client() (both are async)
    # FIX: removed close_kafka_client() — health checks must not tear down shared resources
    # FIX: degraded case returns JSONResponse with status_code=503, not a plain 200 dict

    services = {}

    # Check TimescaleDB
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        services["timescaledb"] = "connected"
    except Exception as e:
        logging.error("TimescaleDB health check failed: %s", e)
        services["timescaledb"] = f"error: {e}"

    # Check Kafka
    try:
        kafka_client = await get_kafka_client()
        if kafka_client and kafka_client.producer:
            services["kafka"] = "connected"
        else:
            services["kafka"] = "error: producer not initialised"
    except Exception as e:
        logging.error("Kafka health check failed: %s", e)
        services["kafka"] = f"error: {e}"

    all_ok = all(v == "connected" for v in services.values())
    payload = {
        "status": "ok" if all_ok else "degraded",
        "service": "pipeline",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": services,
    }

    if all_ok:
        return payload
    return JSONResponse(status_code=503, content=payload)
