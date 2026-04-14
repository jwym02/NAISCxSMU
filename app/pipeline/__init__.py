# app/pipeline/__init__.py
# Exposes the pipeline's key public functions.
# Other modules (e.g. tests) import the pipeline steps
# from here rather than from internal files.
#
# The pipeline runs in this order:
#   ingest → parser → normalizer → router
#
# Usage:
#   from app.pipeline import ingest_log, parse_log, normalize_log, route_event

from app.pipeline.ingest import ingest_log
from app.pipeline.parser import parse_log
from app.pipeline.normalizer import normalize_log
from app.pipeline.router import route_event

__all__ = [
    "ingest_log",    # Step 1+2: store to MinIO, Redis dedup check
    "parse_log",     # Step 3+4: detect format, extract fields
    "normalize_log", # Step 5:   AI normalize, confidence score
    "route_event",   # Step 6:   urgency classify, produce to Kafka
]