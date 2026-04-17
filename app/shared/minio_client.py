import os
from minio import Minio

_endpoint = os.getenv("S3_ENDPOINT", "localhost:9000") \
    .replace("http://", "").replace("https://", "")

minio_client = Minio(
    endpoint=_endpoint,
    access_key=os.getenv("S3_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("S3_SECRET_KEY", "minioadmin123"),
    secure=False   # no HTTPS in the local dev stack
)