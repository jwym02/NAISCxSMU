# Step 1+2:
# Receive file upload, store to MinIO, run deduplication check via dedup.py

import uuid
import io
import logging

from app.shared.minio_client import minio_client
from app.pipeline.dedup import check_duplicate, DedupResult

logger = logging.getLogger(__name__)


async def ingest_log(file_data: bytes, file_name: str, file_format: str, job_id: str = None):
    """
    Handle the first two steps of the log processing pipeline:
      1. Validate and store the raw file in MinIO.
      2. Run a content-based duplicate check via dedup.check_duplicate().

    Args:
        file_data:   Raw bytes of the uploaded file.
        file_name:   Original filename (e.g. "machine_001_2024-04-17.log").
        file_format: Format hint (e.g. "json", "csv", "xml", "log").
        job_id:      Optional caller-supplied UUID; a new one is generated if omitted.

    Returns:
        Tuple of (job_id: str, file_key: str, is_duplicate: bool)

    Raises:
        ValueError:  If file_data is empty.
        Exception:   If the MinIO upload fails.
    """
    if not file_data:
        raise ValueError("Cannot ingest empty file")

    unique_job_id = job_id or str(uuid.uuid4())
    raw_file_key  = f"raw_logs/{unique_job_id}/{file_name}"

    # ── Step 1: Upload to MinIO ────────────────────────────────────────────────
    try:
        minio_client.put_object(
            bucket_name="raw-logs",
            object_name=raw_file_key,
            data=io.BytesIO(file_data),
            length=len(file_data),
        )
        logger.info("Uploaded %s → MinIO key=%s", file_name, raw_file_key)
    except Exception as exc:
        raise Exception(f"Failed to upload file to MinIO: {exc}") from exc

    # ── Step 2: Deduplication check ────────────────────────────────────────────
    dedup_result: DedupResult = await check_duplicate(file_data, job_id=unique_job_id)

    if dedup_result.is_duplicate:
        logger.warning(
            "Duplicate file rejected — file=%s hash=%s… original_job=%s",
            file_name, dedup_result.file_hash[:16], dedup_result.job_id,
        )

    return unique_job_id, raw_file_key, dedup_result.is_duplicate
