"""Post-generation checks for NL→SQL: single statement + allowed base tables."""
from __future__ import annotations

import re

from fastapi import HTTPException

# Hypertables / tables / continuous aggregates the NL layer may reference.
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

_SKIP_IDENT = frozenset({"select", "with", "lateral", "unnest", "values"})


def validate_generated_sql(sql: str) -> None:
    """
    Reject multi-statement batches and FROM/JOIN targets outside the allowlist.
    Best-effort parse; intended to block obvious overreach from the LLM.
    """
    s = (sql or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="Generated SQL is empty")

    core = s.rstrip().rstrip(";")
    if ";" in core:
        raise HTTPException(
            status_code=400,
            detail="Generated SQL must be a single statement (no embedded semicolons)",
        )

    for m in re.finditer(
        r"(?is)\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        sql,
    ):
        name = m.group(1)
        if name.lower() in _SKIP_IDENT:
            continue
        if name.lower() not in _ALLOWED_LOWER:
            raise HTTPException(
                status_code=400,
                detail=f"Generated SQL references disallowed relation: {name}",
            )
