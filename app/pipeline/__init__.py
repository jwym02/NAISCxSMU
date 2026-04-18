# app.pipeline — lazy exports so `from app.pipeline.parser import parse_log`
# does not pull in MinIO/Kafka until those symbols are used.

from typing import Any

__all__ = [
    "ingest_log",
    "parse_log",
    "normalize_log",
    "route_event",
]


def __getattr__(name: str) -> Any:
    if name == "ingest_log":
        from app.pipeline.ingest import ingest_log as _ingest_log

        return _ingest_log
    if name == "parse_log":
        from app.pipeline.parser import parse_log as _parse_log

        return _parse_log
    if name == "normalize_log":
        from app.pipeline.normalizer import normalize_log as _normalize_log

        return _normalize_log
    if name == "route_event":
        from app.pipeline.router import route_event as _route_event

        return _route_event
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
