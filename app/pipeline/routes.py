# All HTTP endpoints:
#   POST /logs/upload
#   GET  /jobs/{id}
#   GET  /queue
#   POST /queue/{id}/review
#   GET  /health

"""
INSTRUCTIONS FOR FastAPI ROUTES
=================================

This file implements the HTTP API for the Pipeline service.
It orchestrates the entire log processing pipeline via REST endpoints.

SERVICES USED:
  - from app.pipeline import ingest_log, parse_log, normalize_log
  - from app.shared.kafka_client: KafkaProducer
  - from app.shared.db: Database connection pool
  - FastAPI framework

ENDPOINT 1: POST /logs/upload
==============================
PURPOSE: Receive a log file from user, start processing pipeline

ACCEPTS:
  - multipart/form-data with:
    * file: The log file (binary)
    * Optional: file_format (hint: "json", "xml", "csv", "log", "txt")

RETURNS:
  - 200 OK with:
    {
      "job_id": "uuid",
      "status": "processing",
      "message": "File received and queued for processing",
      "file_name": "original_filename.log"
    }

IMPLEMENTATION:
  1. Extract file from request.files
  2. Call ingest_log(file_data, file_name, file_format)
  3. Get back: job_id, file_key, is_duplicate, status
  4. If is_duplicate: return 409 Conflict status
  5. If new file:
     a. Store job metadata to TimescaleDB (job_id, file_name, status, timestamp)
     b. Publish message to Kafka topic "log-ingest" with:
        - job_id, file_key, file_format, raw_file_metadata
     c. Return 200 with job_id
  6. Error handling:
     - 400 Bad Request if no file provided
     - 413 Payload Too Large if file exceeds size limit
     - 500 Internal Server Error if MinIO or Kafka fails

ENDPOINT 2: GET /jobs/{id}
===========================
PURPOSE: Check the status of a processing job

RETURNS:
  - 200 OK with:
    {
      "job_id": "uuid",
      "file_name": "original_filename.log",
      "status": "processing|completed|failed|reviewing",
      "created_at": "2024-04-17T10:30:00Z",
      "progress": {
        "step": "normalizing",                    # ingest, parsing, normalizing, routing
        "step_progress": 75                       # percent complete
      },
      "results": {
        "records_processed": 1250,
        "records_routed_hot": 45,                 # P0/P1 events
        "records_routed_cold": 1200,              # P2+ events
        "records_in_review": 5                    # Low confidence items
      },
      "error": None or "error message if failed"
    }

IMPLEMENTATION:
  1. Query TimescaleDB job table for job_id
  2. If not found: return 404 Not Found
  3. If found: return current status and metadata from database
  4. Status should be updated by Kafka consumers as they process
  5. Error handling:
     - 404 Not Found if job_id doesn't exist
     - 500 if database query fails

ENDPOINT 3: GET /queue
======================
PURPOSE: List all items in the low-confidence review queue

RETURNS:
  - 200 OK with:
    {
      "total_items": 5,
      "items": [
        {
          "id": "queue_item_id",
          "job_id": "job_uuid",
          "timestamp": "2024-04-17T10:30:00Z",
          "original_event": {...},
          "ai_suggestion": {...},
          "confidence": 0.45,
          "reason": "Low confidence in categorization",
          "created_at": "2024-04-17T10:31:00Z"
        }
      ]
    }

IMPLEMENTATION:
  1. Query DynamoDB "review-queue" table (from app.shared.dynamo_client)
  2. Sort by created_at descending (newest first)
  3. Limit to last 100 items (configurable)
  4. For each item: format response with all required fields
  5. Error handling:
     - 500 if DynamoDB query fails

ENDPOINT 4: POST /queue/{id}/review
====================================
PURPOSE: Human approves or rejects a review queue item

ACCEPTS:
  - JSON body:
    {
      "decision": "approved|rejected",
      "notes": "reason for decision (optional)"
    }

RETURNS:
  - 200 OK with:
    {
      "id": "queue_item_id",
      "decision": "approved",
      "message": "Item approved and sent to hot path"
    }

IMPLEMENTATION:
  1. Look up review queue item in DynamoDB by id
  2. If not found: return 404
  3. If found:
     a. Update item status: decision = "approved" or "rejected"
     b. Add human notes to record
     c. If approved: publish to Kafka hot/cold topic based on severity
     d. Delete from review queue (or mark as resolved)
  4. Return confirmation
  5. Error handling:
     - 404 if queue item not found
     - 400 if invalid decision value
     - 500 if Kafka publish fails

ENDPOINT 5: GET /health
========================
PURPOSE: Health check for orchestration/monitoring

RETURNS:
  - 200 OK with:
    {
      "status": "healthy",
      "services": {
        "kafka": "connected",
        "timescaledb": "connected",
        "minio": "connected",
        "redis": "connected"
      },
      "timestamp": "2024-04-17T10:30:00Z"
    }

IMPLEMENTATION:
  1. Check connection to each service:
     - Kafka: Try to list topics
     - TimescaleDB: Run SELECT 1
     - MinIO: List buckets
     - Redis: PING command
  2. If all connected: return 200 healthy
  3. If any service down: return 503 Service Unavailable
  4. Include service status details in response

ERROR HANDLING GUIDELINES:
  - Always return JSON responses
  - Include error details in error responses
  - Log all errors server-side for debugging
  - Validate input (file size, format, etc.) before processing
  - Rate limit requests if needed (use decorator)
"""