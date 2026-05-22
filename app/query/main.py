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
import asyncio

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.shared.db import get_pool, close_pool

# Configuration
OPENROUTER_API_KEY = os.getenv("AI_KEY")
OPENROUTER_URL = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")
NL2SQL_MODEL = os.getenv("NL2SQL_MODEL", os.getenv("AI_MODEL", "nvidia/nemotron-nano-9b-v2"))
NL2SQL_TEMPERATURE = float(os.getenv("NL2SQL_TEMPERATURE", 0.3))

logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Query Service")


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


def _sanitize_sql(sql: str) -> str:
    """
    Patch common LLM hallucinations before sending SQL to PostgreSQL.

    Smaller models frequently truncate built-in function names or wrap the
    output in markdown fences.  We fix the known patterns here so that a
    slightly-wrong LLM response still executes rather than blowing up with
    a confusing "column does not exist" error.
    """
    import re

    # 1. Strip markdown code fences  (```sql ... ``` or ``` ... ```)
    sql = re.sub(r"```(?:sql)?\s*", "", sql, flags=re.IGNORECASE).strip("`").strip()

    # 2. Fix truncated / hallucinated PostgreSQL function names
    #    Pattern: whole-word match so we don't clobber e.g. "upper_limit"
    truncations = {
        r"\bupp\s*\("    : "UPPER(",   # upp(  → UPPER(
        r"\buppr\s*\("   : "UPPER(",   # uppr( → UPPER(
        r"\blow\s*\("    : "LOWER(",   # low(  → LOWER(
        r"\blowr\s*\("   : "LOWER(",   # lowr( → LOWER(
        r"\blen\s*\("    : "LENGTH(",  # len(  → LENGTH(
        r"\bsubstr\s*\(" : "SUBSTRING(", # sometimes emitted without the full name
        r"\bconcat_ws\b" : "CONCAT",   # not supported in all PG contexts
        r"\bnow\(\s*\)"  : "NOW()",    # normalise spacing
        r"\bcount\(\s*1\s*\)" : "COUNT(*)",  # COUNT(1) → COUNT(*)
    }
    for pattern, replacement in truncations.items():
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    # 3. Ensure severity comparisons are always upper-cased
    #    e.g.  severity = 'critical'  →  UPPER(severity) = 'CRITICAL'
    def _fix_severity_literal(m):
        val = m.group(1).upper()
        return f"UPPER(severity) = '{val}'"
    sql = re.sub(
        r"\bseverity\s*=\s*'(critical|error|warning|info|debug)'",
        _fix_severity_literal,
        sql,
        flags=re.IGNORECASE,
    )

    # 4. Remove any trailing semicolon (asyncpg doesn't need it and some
    #    drivers reject multi-statement strings)
    sql = sql.rstrip("; \t\n")

    return sql


def _generate_smart_fallback_sql(nl_query: str, time_range_hours: int = 24, limit: int = 100) -> str:
    """
    Generate SQL by parsing keywords from natural language query.
    Fallback when LLM is unavailable.
    """
    logger.info(f"[fallback_sql] Generating SQL from NL query: {nl_query}")
    query_lower = nl_query.lower()
    
    # Parse natural language time ranges
    if "today" in query_lower:
        time_range_hours = 24
        logger.info(f"[fallback_sql] Detected 'today' → 24 hours")
    elif "past 7 days" in query_lower or "7 days" in query_lower:
        time_range_hours = 7 * 24  # 168 hours
        logger.info(f"[fallback_sql] Detected '7 days' → 168 hours")
    elif "past 5 days" in query_lower or "5 days" in query_lower:
        time_range_hours = 5 * 24  # 120 hours
        logger.info(f"[fallback_sql] Detected '5 days' → 120 hours")
    elif "past 30 days" in query_lower or "month" in query_lower:
        time_range_hours = 30 * 24  # 720 hours
        logger.info(f"[fallback_sql] Detected 'month' → 720 hours")
    elif "week" in query_lower:
        time_range_hours = 7 * 24  # 168 hours
        logger.info(f"[fallback_sql] Detected 'week' → 168 hours")
    else:
        logger.info(f"[fallback_sql] Using default time range: {time_range_hours} hours")
    
    # Build WHERE clause with proper parentheses
    where_parts = [f"(timestamp > NOW() - INTERVAL '{time_range_hours} hours')"]
    
    # Check for severity keywords (use UPPER to match both cases)
    severity_filters = []
    if any(word in query_lower for word in ['critical', 'severity = critical']):
        severity_filters.append("'CRITICAL'")
        logger.info(f"[fallback_sql] Detected 'CRITICAL' keyword")
    if any(word in query_lower for word in ['error', 'errors']):
        severity_filters.append("'ERROR'")
        logger.info(f"[fallback_sql] Detected 'ERROR' keyword")
    if any(word in query_lower for word in ['warning']):
        severity_filters.append("'WARNING'")
        logger.info(f"[fallback_sql] Detected 'WARNING' keyword")
    if any(word in query_lower for word in ['info']):
        severity_filters.append("'INFO'")
        logger.info(f"[fallback_sql] Detected 'INFO' keyword")
    
    logger.info(f"[fallback_sql] Severity filters: {severity_filters}")
    
    if severity_filters:
        # Use UPPER to handle case-insensitive matching
        severity_clause = f"UPPER(severity) IN ({', '.join(severity_filters)})"
        where_parts.append(severity_clause)
    
    base_where = " AND ".join(where_parts)
    logger.info(f"[fallback_sql] Final WHERE clause: {base_where}")
    logger.info(f"[fallback_sql] Time range: {time_range_hours} hours")
    
    # Check for keywords that suggest aggregation or showing all events
    # "show me all" = detail records (NOT aggregated)
    # "count/summary/trend" = aggregated data
    wants_aggregation = any(word in query_lower for word in ['count', 'how many', 'total', 'summary', 'by hour', 'by day', 'trend', 'distribution', 'breakdown'])
    wants_all_events = any(word in query_lower for word in ['show me all', 'list', 'get all', 'fetch', 'retrieve'])
    
    logger.info(f"[fallback_sql] wants_aggregation: {wants_aggregation}, wants_all_events: {wants_all_events}")
    
    # If asking for aggregation/analysis, return aggregated results by hour and severity for charting
    # "show me all X" means return the actual records, not a summary
    if wants_aggregation and not wants_all_events:
        # Return aggregated results by hour and severity for charting
        sql = f"""
SELECT 
  time_bucket('1 hour', timestamp) as hour,
  UPPER(severity) as severity,
  COUNT(*) as event_count
FROM normalized_events
WHERE {base_where}
GROUP BY time_bucket('1 hour', timestamp), UPPER(severity)
ORDER BY hour DESC
        """.strip()
        logger.info(f"[fallback_sql] Generated aggregated query (for charting/analysis)")
        return sql
    
    # Default: return all matching events with relevant columns
    # Select key columns that are most useful for reviewing events
    sql = f"""
SELECT 
  timestamp,
  source,
  severity,
  event_type,
  message,
  ai_category,
  ai_root_cause,
  ai_recommended_action,
  confidence_score
FROM normalized_events
WHERE {base_where}
ORDER BY timestamp DESC
LIMIT {limit}
    """.strip()
    logger.info(f"[fallback_sql] Generated detailed events query with {limit} limit")
    return sql


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

CRITICAL COLUMN MAPPINGS:
- Use 'severity' column (not 'event_type') for filtering by log level: CRITICAL, ERROR, WARNING, INFO
- Use 'event_type' column only for specific event types like: maintenance, thermal, temperature_reading, etc.
- Always use UPPER(severity) for case-insensitive matching of severity levels
- 'info' queries should filter: UPPER(severity) IN ('INFO')
- 'error' queries should filter: UPPER(severity) IN ('ERROR') 
- 'critical' queries should filter: UPPER(severity) IN ('CRITICAL')

Convert the following natural language query to SQL. Return ONLY the SQL query, no explanation.
The SQL should be safe, efficient, and return meaningful results.
Use LIMIT {limit} by default unless specified otherwise.
Always use timestamps for time-based queries.
Return only SELECT queries (no INSERT, UPDATE, DELETE).

Natural Language Query: {nl_query}

SQL Query:"""

    try:
        logger.info(f"[generate_sql] Calling OpenRouter API with model: {NL2SQL_MODEL}")
        logger.info(f"[generate_sql] API Key configured: {bool(OPENROUTER_API_KEY)}")
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
            
            logger.info(f"[generate_sql] Response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"[generate_sql] OpenRouter API error: {response.text}")
                logger.warning(f"[generate_sql] Status code {response.status_code}, falling back to smart fallback")
                return _generate_smart_fallback_sql(nl_query, time_range_hours, limit)
            
            result = response.json()
            logger.info(f"[generate_sql] Response keys: {result.keys()}")
            logger.info(f"[generate_sql] Full response: {json.dumps(result, indent=2)[:500]}")
            
            # Handle None content gracefully
            try:
                sql_query = result['choices'][0]['message']['content']
                logger.info(f"[generate_sql] LLM returned: {sql_query[:100] if sql_query else '(empty)'}")
                
                if not sql_query or not sql_query.strip():
                    logger.warning(f"[generate_sql] AI returned empty response, using smart fallback query")
                    sql_query = _generate_smart_fallback_sql(nl_query, time_range_hours, limit)
                else:
                    sql_query = sql_query.strip()
                    sql_query = _sanitize_sql(sql_query)

                    # Validate LLM query - check for common mistakes
                    # If query mentions 'info'/'error'/'critical' but filters by event_type, fall back
                    query_lower = nl_query.lower()
                    sql_lower = sql_query.lower()
                    
                    is_severity_query = any(word in query_lower for word in ['critical', 'error', 'errors', 'warning', 'info'])
                    filters_by_event_type_only = 'event_type' in sql_lower and 'severity' not in sql_lower
                    
                    if is_severity_query and filters_by_event_type_only:
                        logger.warning(f"[generate_sql] LLM generated incorrect query - filtered by event_type instead of severity, falling back")
                        sql_query = _generate_smart_fallback_sql(nl_query, time_range_hours, limit)
                    else:
                        logger.info(f"[generate_sql] ✓ Using LLM-generated SQL")
            except (KeyError, TypeError, IndexError) as e:
                logger.warning(f"[generate_sql] Failed to parse AI response: {e}, using smart fallback")
                sql_query = _generate_smart_fallback_sql(nl_query, time_range_hours, limit)
            
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
            
            logger.info(f"[generate_sql] Final SQL: {sql_query[:200]}...")
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
        logger.info(f"[execute_query] Starting SQL execution...")
        logger.info(f"[execute_query] SQL: {sql_query[:200]}{'...' if len(sql_query) > 200 else ''}")
        pool = await get_pool()
        async with pool.acquire() as conn:
            logger.info(f"[execute_query] Acquired database connection")
            rows = await conn.fetch(sql_query)
            logger.info(f"[execute_query] Raw rows returned: {len(rows)}")
            
            # Convert rows to list of dicts
            result = [dict(row) for row in rows]
            logger.info(f"[execute_query] Converted to dicts. Result count: {len(result)}")
            
            if result:
                logger.info(f"[execute_query] First row keys: {list(result[0].keys())}")
                logger.info(f"[execute_query] First row: {result[0]}")
            
            # Handle datetime serialization
            for row in result:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()
            
            logger.info(f"[execute_query] ✓ Query execution succeeded. Total rows: {len(result)}")
            return result
            
    except Exception as e:
        logger.error(f"[execute_query] ✗ Query execution failed: {e}", exc_info=True)
        logger.error(f"[execute_query] Failed SQL was: {sql_query}")
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
    
    logger.info(f"=== QUERY START ===")
    logger.info(f"Natural language query: {request.query}")
    logger.info(f"Limit: {request.limit}, Time range: {request.time_range_hours} hours")
    
    # Generate SQL from natural language
    logger.info(f"Generating SQL from natural language...")
    sql_query = await generate_sql(request.query, request.limit, request.time_range_hours)
    logger.info(f"Generated SQL:\n{sql_query}")
    
    # Execute query — retry with smart fallback if LLM SQL fails at runtime
    logger.info(f"Executing query...")
    try:
        rows = await execute_query(sql_query)
    except HTTPException as exc:
        logger.warning(f"[query] LLM SQL failed at execution ({exc.detail}), retrying with smart fallback")
        sql_query = _generate_smart_fallback_sql(request.query, request.time_range_hours, request.limit)
        logger.info(f"[query] Fallback SQL: {sql_query}")
        rows = await execute_query(sql_query)
    logger.info(f"✓ Query completed. Rows returned: {len(rows)}")
    
    # Calculate execution time
    execution_time_ms = (time.time() - start_time) * 1000
    logger.info(f"✓ Total execution time: {execution_time_ms:.1f}ms")
    
    if not rows:
        logger.warning(f"⚠ Query returned 0 rows!")
    else:
        logger.info(f"First row sample: {rows[0]}")
    
    logger.info(f"=== QUERY END ===\n")
    
    return QueryResponse(
        original_query=request.query,
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
