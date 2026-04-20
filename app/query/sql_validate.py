"""Post-generation checks for NL→SQL: single statement + allowed tables (incl. subqueries)."""
from __future__ import annotations

import re
from typing import Set

import sqlglot
from fastapi import HTTPException
from sqlglot import exp

ALLOWED_TABLES = frozenset(
    {
        "normalized_events",
        "raw_logs",
        "event_routing",
        "review_queue_status",
        "events_by_hour",
        "events_by_machine_daily",
    }
)
_ALLOWED_LOWER = {t.lower() for t in ALLOWED_TABLES}

ALLOWED_COLUMNS_BY_TABLE: dict[str, frozenset[str]] = {
    "normalized_events": frozenset(
        {
            "job_id",
            "timestamp",
            "source",
            "event_type",
            "severity",
            "message",
            "ai_category",
            "ai_root_cause",
            "ai_recommended_action",
            "confidence_score",
            "requires_review",
            "review_reason",
            "search_text",
            "created_at",
        }
    ),
    "raw_logs": frozenset(
        {
            "job_id",
            "timestamp",
            "file_name",
            "file_format",
            "raw_content",
            "file_hash",
            "created_at",
        }
    ),
    "event_routing": frozenset({"id", "job_id", "kafka_topic", "routed_at"}),
    "review_queue_status": frozenset(
        {"id", "job_id", "status", "reviewer_notes", "reviewed_at", "created_at"}
    ),
    "events_by_hour": frozenset({"bucket", "ai_category", "severity", "event_count"}),
    "events_by_machine_daily": frozenset(
        {"bucket", "source", "ai_category", "severity", "event_count", "avg_confidence"}
    ),
}


def extract_table_names_from_sql(sql: str) -> Set[str]:
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception as e:
        raise ValueError(f"SQL parse error: {e}") from e
    names: Set[str] = set()
    for t in tree.find_all(exp.Table):
        n = t.name
        if n:
            names.add(n)
    return names


def validate_generated_sql(sql: str) -> None:
    s = (sql or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="Generated SQL is empty")

    core = s.rstrip().rstrip(";")
    if ";" in core:
        raise HTTPException(
            status_code=400,
            detail="Generated SQL must be a single statement (no embedded semicolons)",
        )

    sql_upper = s.upper()
    if any(k in sql_upper for k in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "TRUNCATE ")):
        raise HTTPException(
            status_code=400,
            detail="Generated SQL must be read-only (SELECT only)",
        )
    if "SELECT" not in sql_upper:
        raise HTTPException(status_code=400, detail="Generated query must be a SELECT statement")

    try:
        names = extract_table_names_from_sql(s)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    for name in names:
        base = name.split(".")[-1]
        if base.lower() not in _ALLOWED_LOWER:
            raise HTTPException(
                status_code=400,
                detail=f"Generated SQL references disallowed relation: {name}",
            )

    for m in re.finditer(r"\bnormalized_events\.(\w+)\b", s, re.I):
        col = m.group(1).lower()
        allowed = ALLOWED_COLUMNS_BY_TABLE.get("normalized_events", frozenset())
        if col not in {c.lower() for c in allowed}:
            raise HTTPException(
                status_code=400,
                detail=f"Disallowed column on normalized_events: {m.group(1)}",
            )
