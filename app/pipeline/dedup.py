"""
app/pipeline/dedup.py — Reusable file deduplication utilities.

Deduplication is content-based: a SHA-256 hash of the raw file bytes is stored
in Redis with a 24-hour TTL.  If the same bytes arrive again within that window
the file is flagged as a duplicate and the pipeline skips reprocessing.

Redis unavailability is treated as non-fatal: all functions log a warning and
return an optimistic (non-duplicate) result so the pipeline keeps running.

Public API
----------
Models   : DedupResult, BatchDedupResult
Functions: calculate_file_hash, check_duplicate, mark_as_processed,
           get_dedup_info, batch_check_duplicates, cleanup_expired_dedup_keys
"""

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel
import redis.asyncio as aioredis

from app.shared.redis_client import redis_client as _sync_redis_client

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

DEDUP_HASH_ALGORITHM = "sha256"
DEDUP_KEY_PREFIX     = "dedup:"
DEDUP_TTL_SECONDS    = 86_400   # 24 hours
BATCH_CHECK_SIZE     = 100      # max files per bulk operation

# ── Async Redis client ─────────────────────────────────────────────────────────
# redis.asyncio shares the same URL convention as the sync client.

import os
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

_async_redis: Optional[aioredis.Redis] = None


async def _get_async_redis() -> aioredis.Redis:
    """Return (and lazily create) the module-level async Redis client."""
    global _async_redis
    if _async_redis is None:
        _async_redis = aioredis.from_url(_REDIS_URL, decode_responses=True)
    return _async_redis


# ── Pydantic models ────────────────────────────────────────────────────────────

class DedupResult(BaseModel):
    """Result of a single-file duplicate check."""

    file_hash:    str            # SHA-256 hex digest of the file content
    is_duplicate: bool           # True  → file was seen before
    first_seen:   Optional[str]  # ISO-8601 timestamp of first occurrence
    job_id:       Optional[str]  # job_id that first processed this hash


class BatchDedupResult(BaseModel):
    """Aggregated result of a bulk duplicate check."""

    total_checked:    int
    duplicates_found: int
    new_files:        int
    results:          List[DedupResult]


# ── Core helpers ───────────────────────────────────────────────────────────────

def calculate_file_hash(file_data: bytes) -> str:
    """
    Return the SHA-256 hex digest of *file_data*.

    Deterministic: identical bytes always produce the same hash.

    Args:
        file_data: Raw bytes of the file.

    Returns:
        Lowercase hex string (64 characters).

    Raises:
        ValueError: If *file_data* is empty.
        Exception:  If hashing fails for any other reason.
    """
    if not file_data:
        raise ValueError("Cannot hash empty file data")

    try:
        digest = hashlib.new(DEDUP_HASH_ALGORITHM, file_data).hexdigest()
        logger.debug("Computed %s hash: %s…", DEDUP_HASH_ALGORITHM, digest[:16])
        return digest
    except Exception as exc:
        logger.error("Hash calculation failed: %s", exc)
        raise


# ── Async core functions ───────────────────────────────────────────────────────

async def check_duplicate(
    file_data: bytes,
    job_id: Optional[str] = None,
) -> DedupResult:
    """
    Check whether *file_data* has been processed before.

    If the hash is not found in Redis the file is marked as processed
    immediately so concurrent uploads of the same file are handled safely.

    Args:
        file_data: Raw bytes of the file to check.
        job_id:    job_id to store when marking a new file as processed.
                   Pass the job_id you generated for this upload.

    Returns:
        DedupResult — inspect ``.is_duplicate`` to decide whether to continue.

    Note:
        Redis failures are non-fatal. On error this function logs a warning
        and returns ``is_duplicate=False`` so the pipeline is not blocked.
    """
    file_hash = calculate_file_hash(file_data)
    redis_key = f"{DEDUP_KEY_PREFIX}{file_hash}"
    now_iso   = datetime.now(timezone.utc).isoformat()

    try:
        r = await _get_async_redis()
        existing = await r.get(redis_key)

        if existing is not None:
            # Redis stores  "job_id|first_seen"  or just  "job_id"  (legacy)
            parts         = existing.split("|", 1)
            stored_job_id = parts[0]
            first_seen    = parts[1] if len(parts) > 1 else None

            logger.info(
                "Duplicate detected — hash=%s… original_job=%s",
                file_hash[:16], stored_job_id,
            )
            return DedupResult(
                file_hash=file_hash,
                is_duplicate=True,
                first_seen=first_seen,
                job_id=stored_job_id,
            )

        # New file — mark it before returning so concurrent requests see it.
        await mark_as_processed(file_hash, job_id or "", DEDUP_TTL_SECONDS)
        logger.info("New file registered — hash=%s…", file_hash[:16])

        return DedupResult(
            file_hash=file_hash,
            is_duplicate=False,
            first_seen=now_iso,
            job_id=job_id,
        )

    except Exception as exc:
        logger.warning(
            "Redis dedup check failed (optimistic pass-through): %s", exc
        )
        return DedupResult(
            file_hash=file_hash,
            is_duplicate=False,
            first_seen=now_iso,
            job_id=job_id,
        )


async def mark_as_processed(
    file_hash: str,
    job_id: str,
    ttl_seconds: int = DEDUP_TTL_SECONDS,
) -> bool:
    """
    Store *file_hash* → *job_id* in Redis with an expiry.

    Idempotent: calling this multiple times with the same hash is safe —
    it will refresh the TTL on subsequent calls.

    Args:
        file_hash:   SHA-256 hex digest (from ``calculate_file_hash``).
        job_id:      job_id to associate with this hash.
        ttl_seconds: How long to keep the key (default 24 h).

    Returns:
        True on success, False if Redis is unavailable.
    """
    redis_key = f"{DEDUP_KEY_PREFIX}{file_hash}"
    value     = f"{job_id}|{datetime.now(timezone.utc).isoformat()}"

    try:
        r = await _get_async_redis()
        await r.set(redis_key, value, ex=ttl_seconds)
        logger.debug("Marked hash=%s… job_id=%s ttl=%ds",
                     file_hash[:16], job_id, ttl_seconds)
        return True
    except Exception as exc:
        logger.error("mark_as_processed failed for hash=%s…: %s",
                     file_hash[:16], exc)
        return False


async def get_dedup_info(file_hash: str) -> Optional[Dict]:
    """
    Return stored dedup metadata for *file_hash*, or None if not found.

    Args:
        file_hash: SHA-256 hex digest to look up.

    Returns:
        Dict with keys ``job_id``, ``first_seen``, ``ttl_remaining`` (seconds),
        or None if the hash is unknown / Redis is unavailable.
    """
    redis_key = f"{DEDUP_KEY_PREFIX}{file_hash}"

    try:
        r = await _get_async_redis()
        value, ttl = await asyncio.gather(
            r.get(redis_key),
            r.ttl(redis_key),
        )

        if value is None:
            return None

        parts      = value.split("|", 1)
        job_id     = parts[0]
        first_seen = parts[1] if len(parts) > 1 else None

        return {
            "job_id":        job_id,
            "first_seen":    first_seen,
            "ttl_remaining": ttl,   # -2 = key does not exist, -1 = no expiry
        }

    except Exception as exc:
        logger.warning("get_dedup_info failed for hash=%s…: %s",
                       file_hash[:16], exc)
        return None


async def batch_check_duplicates(
    files: List[Tuple[bytes, str]],
) -> BatchDedupResult:
    """
    Check and mark a list of files in one operation.

    Processes files in chunks of ``BATCH_CHECK_SIZE`` to avoid overwhelming
    Redis with a single pipeline flush.

    Args:
        files: List of ``(file_data, job_id)`` tuples.

    Returns:
        BatchDedupResult with per-file DedupResult entries and summary counts.
    """
    all_results: List[DedupResult] = []
    duplicates  = 0

    for i in range(0, len(files), BATCH_CHECK_SIZE):
        chunk = files[i : i + BATCH_CHECK_SIZE]
        chunk_results = await asyncio.gather(
            *[check_duplicate(data, job_id) for data, job_id in chunk],
            return_exceptions=True,
        )

        for result in chunk_results:
            if isinstance(result, Exception):
                logger.error("Batch dedup entry failed: %s", result)
                continue
            all_results.append(result)
            if result.is_duplicate:
                duplicates += 1

    new_files = len(all_results) - duplicates
    logger.info(
        "Batch dedup complete — checked=%d  duplicates=%d  new=%d",
        len(all_results), duplicates, new_files,
    )

    return BatchDedupResult(
        total_checked=len(all_results),
        duplicates_found=duplicates,
        new_files=new_files,
        results=all_results,
    )


async def cleanup_expired_dedup_keys() -> int:
    """
    Advisory cleanup of dedup keys.

    Redis expires keys automatically via TTL, so this function is a no-op in
    normal operation.  It is provided as a hook for manual maintenance tasks
    (e.g., clearing the dedup namespace during testing).

    Returns:
        Count of ``dedup:*`` keys that currently exist (informational).
    """
    try:
        r = await _get_async_redis()
        keys = await r.keys(f"{DEDUP_KEY_PREFIX}*")
        count = len(keys)
        logger.info("Dedup namespace contains %d active keys (TTL-managed by Redis)", count)
        return count
    except Exception as exc:
        logger.warning("cleanup_expired_dedup_keys failed: %s", exc)
        return 0


# ── Public exports ─────────────────────────────────────────────────────────────

__all__ = [
    # Models
    "DedupResult",
    "BatchDedupResult",
    # Functions
    "calculate_file_hash",
    "check_duplicate",
    "mark_as_processed",
    "get_dedup_info",
    "batch_check_duplicates",
    "cleanup_expired_dedup_keys",
]
