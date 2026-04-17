# Step 1+2: 
# receive file upload, store to MinIO, Redis dedup check
import uuid
import hashlib
import io
import logging
from app.shared.minio_client import minio_client
from app.shared.redis_client import redis_client          
"""
INSTRUCTIONS FOR ingest_log() FUNCTION
========================================

PURPOSE:
  Handle the first two steps of the log processing pipeline:
  1. Receive a log file from a user upload
  2. Store it to MinIO (object storage)
  3. Check Redis to see if we've already processed this exact file

INPUTS:
  - file_data: The raw bytes of the uploaded file
  - file_name: String with the original filename (e.g., "machine_001_2024_04_17.log")
  - file_format: String indicating file type (e.g., "json", "xml", "csv", "log", "txt")

OUTPUTS:
  - Return a dictionary with:
    {
      "job_id": "unique_identifier",          # Generate a UUID for tracking this job
      "file_key": "path/in/minio",            # The MinIO bucket key where file was stored
      "is_duplicate": True/False,             # Whether Redis found this file before
      "status": "new" or "duplicate"          # Status of the job
    }

STEPS TO IMPLEMENT:
  1. Generate a unique job_id (use uuid.uuid4())
  2. Create a file path in MinIO format: "raw_logs/{job_id}/{file_name}"
  3. Upload file_data to MinIO using the minio_client (from app.shared.minio_client)
  4. Create a dedup key from the file (hash the file content or use filename + size)
  5. Check if this key exists in Redis using redis_client (from app.shared.redis_client)
     - If Redis key EXISTS: mark as duplicate, return early with is_duplicate=True
     - If Redis key NOT FOUND: this is a new file, set the key in Redis
  6. Return the result dictionary with all required fields

SERVICES TO USE:
  - app.shared.minio_client: MinIO S3-compatible storage client
  - app.shared.redis_client: Redis caching for duplicate detection

ERROR HANDLING:
  - If MinIO upload fails: raise an exception with descriptive message
  - If Redis connection fails: still allow upload but log warning about dedup check
  - If file_data is empty: reject with validation error

NOTES:
  - Job ID should be returned so the user can track progress via GET /jobs/{id}
  - Duplicates should be rejected early to avoid reprocessing the same file
  - MinIO bucket names should be "raw-logs" (already created by init container)
"""

def ingest_log(file_data, file_name, file_format):
    if not file_data:
        raise ValueError("Cannot ingest empty file")
    
    unique_job_id = str(uuid.uuid4())
    raw_file_key = f"raw_logs/{unique_job_id}/{file_name}"
    # Upload to MinIO
    try:
        minio_client.put_object(
            bucket_name = "raw-logs",
            object_name = raw_file_key,
            data = io.BytesIO(file_data),
            length = len(file_data)
        )
    except Exception as e:
        raise Exception(f"Failed to upload file to MinIO: {str(e)}")
    
    # hash this file
    file_hash = hashlib.sha256(file_data).hexdigest()
    is_duplicate = False
    try:
      # Check Redis for dedup key, if exists, then this is a dupe request
      is_duplicate = bool(redis_client.exists(f"dedup:{file_hash}"))
      if not is_duplicate: 
          redis_client.set(f"dedup:{file_hash}", unique_job_id, ex=86400)
    except Exception as e:
        logging.warning(f"Redis dedup check failed, proceeding without it: {e}")

    return {
        "job_id": str(unique_job_id),
        "file_key": raw_file_key,
        "is_duplicate": is_duplicate,
        "status": "duplicate" if is_duplicate else "new"
    }   
  