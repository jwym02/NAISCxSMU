"""
NL2SQL Query Service

Converts natural language queries to SQL and executes them against TimescaleDB.
Integrates with OpenRouter API for LLM-based SQL generation.

Architecture:
  User Query (NL) → OpenRouter LLM → Generated SQL → TimescaleDB → Results
"""

import os
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone
import httpx

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.query.sql_validate import validate_generated_sql
from app.shared.db import get_pool, close_pool
from app.shared.text_tokens import (
    MAX_USER_PROMPT_CHARS,
    normalize_prompt,
    tokenize_for_match,
)

# Configuration
OPENROUTER_API_KEY = os.getenv("AI_KEY")
OPENROUTER_URL = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")
NL2SQL_MODEL = os.getenv("NL2SQL_MODEL", os.getenv("AI_MODEL", "nvidia/nemotron-nano-9b-v2"))
NL2SQL_TEMPERATURE = float(os.getenv("NL2SQL_TEMPERATURE", 0.3))

logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Query Service")

_cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    """Natural language query request."""
    query: str
    limit: Optional[int] = 100
    time_range_hours: Optional[int] = 24


class QueryResponse(BaseModel):
    """Query execution response."""
    original_query: str
    generated_sql: str
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: float


# Database schema documentation for LLM context
DATABASE_SCHEMA = """
## TimescaleDB Schema

### Hypertables (partitioned by timestamp, auto-retention)

**raw_logs** (30-day retention)
- job_id: UUID (unique identifier for this log file)
- timestamp: TIMESTAMP WITH TIME ZONE (when processed)
- file_name: TEXT (original filename)
- file_format: TEXT (JSON, CSV, XML, LOG, TXT)
- raw_content: TEXT (full file content)
- file_hash: VARCHAR(64) (SHA256 hash)
- created_at: TIMESTAMP WITH TIME ZONE

**normalized_events** (7-day retention)
- job_id: UUID (links to raw_logs)
- timestamp: TIMESTAMP WITH TIME ZONE (when normalized)
- source: TEXT (machine/device identifier, e.g., 'machine_001')
- event_type: TEXT (error, warning, info, thermal, pressure, etc.)
- severity: TEXT (CRITICAL, ERROR, WARNING, INFO, DEBUG)
- message: TEXT (event message)
- ai_category: TEXT (AI-determined category: system, thermal, mechanical, electrical, software, safety, etc.)
- ai_root_cause: TEXT (AI analysis of root cause)
- ai_recommended_action: TEXT (AI-suggested remediation)
- confidence_score: FLOAT (0.0-1.0, higher = more confident)
- requires_review: BOOLEAN (TRUE if low confidence)
- review_reason: TEXT (reason for review queue)
- search_text: TEXT (normalized tokens for search / full-text; use for keyword filters)
- created_at: TIMESTAMP WITH TIME ZONE

### Regular Tables

**event_routing**
- id: BIGSERIAL (primary key)
- job_id: UUID (which raw_log was routed)
- kafka_topic: TEXT (logs.p0, logs.p1, logs.p2, logs.deadletter)
- routed_at: TIMESTAMP WITH TIME ZONE

**review_queue_status**
- id: BIGSERIAL (primary key)
- job_id: UUID
- status: TEXT (pending, approved, rejected)
- reviewer_notes: TEXT
- reviewed_at: TIMESTAMP WITH TIME ZONE
- created_at: TIMESTAMP WITH TIME ZONE

### Continuous Aggregates (materialized views)

**events_by_hour**
- time: TIMESTAMP (hourly bucket)
- ai_category: TEXT
- severity: TEXT
- event_count: BIGINT

**events_by_machine_daily**
- date: DATE (daily bucket)
- source: TEXT (machine name)
- event_count: BIGINT
- avg_confidence: FLOAT

## Common Queries

1. Recent errors by machine:
   SELECT source, COUNT(*) as count, AVG(confidence_score) as avg_confidence
   FROM normalized_events
   WHERE severity = 'ERROR' AND timestamp > NOW() - INTERVAL '24 hours'
   GROUP BY source;

2. Events requiring review:
   SELECT ne.*, rq.status
   FROM normalized_events ne
   JOIN review_queue_status rq ON ne.job_id = rq.job_id
   WHERE rq.status = 'pending'
   ORDER BY ne.timestamp DESC;

3. Confidence score distribution:
   SELECT ai_category, severity, AVG(confidence_score) as avg_confidence
   FROM normalized_events
   WHERE timestamp > NOW() - INTERVAL '7 days'
   GROUP BY ai_category, severity;

4. Thermal events (semiconductor manufacturing):
   SELECT * FROM normalized_events
   WHERE ai_category = 'thermal' AND severity IN ('CRITICAL', 'ERROR')
   AND timestamp > NOW() - INTERVAL '24 hours'
   ORDER BY timestamp DESC;
"""


async def generate_sql(nl_query: str, limit: int = 100, time_range_hours: int = 24) -> str:
    """
    Convert natural language query to SQL using OpenRouter LLM.
    
    Args:
        nl_query: Natural language query
        limit: Max rows to return
        time_range_hours: Hours to look back
    
    Returns:
        Generated SQL query string
        
    Raises:
        HTTPException: If LLM call fails
    """
    if not OPENROUTER_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="AI_KEY environment variable not set"
        )
    
    prompt = f"""You are an expert SQL developer for TimescaleDB (PostgreSQL with TimescaleDB extensions).

{DATABASE_SCHEMA}

Convert the following natural language query to SQL. Return ONLY the SQL query, no explanation.
The SQL should be safe, efficient, and return meaningful results.
Use LIMIT {100} by default unless specified otherwise.
Always use timestamps for time-based queries.
Return only SELECT queries (no INSERT, UPDATE, DELETE).

Natural Language Query: {nl_query}

SQL Query:"""

    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(
                f"{OPENROUTER_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://logparser.local",
                },
                json={
                    "model": NL2SQL_MODEL,
                    "temperature": NL2SQL_TEMPERATURE,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": 500,
                },
                timeout=30.0
            )
            
            if response.status_code != 200:
                logger.error(f"OpenRouter API error: {response.text}")
                raise HTTPException(
                    status_code=500,
                    detail=f"LLM API error: {response.status_code}"
                )
            
            result = response.json()
            logger.info(f"OpenRouter full response: {result}")
            
            # Handle None content gracefully
            try:
                sql_query = result['choices'][0]['message']['content']
                if not sql_query or not sql_query.strip():
                    logger.warning("AI returned empty response, using fallback query")
                    # Generic fallback for any query
                    sql_query = f"SELECT * FROM normalized_events WHERE timestamp > NOW() - INTERVAL '{time_range_hours} hours' ORDER BY timestamp DESC LIMIT {limit}"
                else:
                    sql_query = sql_query.strip()
            except (KeyError, TypeError, IndexError) as e:
                logger.warning(f"Failed to parse AI response, using fallback: {e}")
                # Generic fallback for any query
                sql_query = f"SELECT * FROM normalized_events WHERE timestamp > NOW() - INTERVAL '{time_range_hours} hours' ORDER BY timestamp DESC LIMIT {limit}"
            
            # Basic safety checks
            sql_upper = sql_query.upper()
            if any(keyword in sql_upper for keyword in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER']):
                raise HTTPException(
                    status_code=400,
                    detail="Generated query contains unsafe operations (INSERT/UPDATE/DELETE/DROP/ALTER)"
                )
            
            if 'SELECT' not in sql_upper:
                raise HTTPException(
                    status_code=400,
                    detail="Generated query must be a SELECT statement"
                )

            validate_generated_sql(sql_query)
            
            logger.info(f"Generated SQL: {sql_query[:200]}...")
            return sql_query
            
    except httpx.RequestError as e:
        logger.error(f"HTTP request failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to connect to LLM service: {str(e)}"
        )


async def execute_query(sql_query: str) -> List[Dict[str, Any]]:
    """
    Execute SQL query against TimescaleDB.
    
    Args:
        sql_query: SQL query to execute
    
    Returns:
        List of result rows as dictionaries
        
    Raises:
        HTTPException: If query execution fails
    """
    try:
        logger.info(f"Executing SQL: {sql_query}")
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql_query)
            
            # Convert rows to list of dicts
            result = [dict(row) for row in rows]
            logger.info(f"Query returned {len(result)} rows")
            
            # Handle datetime serialization
            for row in result:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()
            
            return result
            
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        logger.error(f"Failed SQL was: {sql_query}")
        raise HTTPException(
            status_code=400,
            detail=f"Query execution error: {str(e)}"
        )


@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """
    Execute a natural language query against the database.
    
    Steps:
    1. Convert NL query to SQL using OpenRouter LLM
    2. Validate generated SQL (safety checks)
    3. Execute query against TimescaleDB
    4. Return results
    """
    import time
    start_time = time.time()

    raw_q = (request.query or "").strip()
    if not raw_q:
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    if len(raw_q) > MAX_USER_PROMPT_CHARS:
        raise HTTPException(
            status_code=400,
            detail=f"Query exceeds maximum length ({MAX_USER_PROMPT_CHARS} characters)",
        )
    nq = normalize_prompt(raw_q)
    logger.info(
        "NL query chars=%s distinct_tokens=%s",
        len(nq),
        len(set(tokenize_for_match(nq))),
    )

    sql_query = await generate_sql(nq, request.limit, request.time_range_hours)

    rows = await execute_query(sql_query)
    
    # Calculate execution time
    execution_time_ms = (time.time() - start_time) * 1000
    
    return QueryResponse(
        original_query=raw_q,
        generated_sql=sql_query,
        rows=rows,
        row_count=len(rows),
        execution_time_ms=execution_time_ms
    )


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "query",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/schema")
async def schema():
    """Return database schema documentation."""
    return {
        "schema": DATABASE_SCHEMA,
        "models": {
            "nl2sql_model": NL2SQL_MODEL,
            "temperature": NL2SQL_TEMPERATURE
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
