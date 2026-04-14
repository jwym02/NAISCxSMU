# app/shared/__init__.py
# Exposes all shared infrastructure clients from a single import point.
# Every module imports from here — no module reaches into shared's
# internal files directly.
#
# Usage from any module:
#   from app.shared import db, redis, dynamo, minio, kafka

from app.shared.db import get_db_pool, get_db_connection
from app.shared.redis_client import get_redis, check_dedup, set_dedup_key
from app.shared.dynamo import get_dynamo, get_normalization_rules, put_review_item
from app.shared.minio_client import get_minio, upload_raw_log, download_raw_log
from app.shared.kafka_client import get_producer, get_consumer

__all__ = [
    # Database
    "get_db_pool",
    "get_db_connection",
    # Redis
    "get_redis",
    "check_dedup",
    "set_dedup_key",
    # DynamoDB
    "get_dynamo",
    "get_normalization_rules",
    "put_review_item",
    # MinIO
    "get_minio",
    "upload_raw_log",
    "download_raw_log",
    # Kafka
    "get_producer",
    "get_consumer",
]