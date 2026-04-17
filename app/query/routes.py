# POST /query (NL → SQL → result)
# GET  /health

"""
INSTRUCTIONS FOR FastAPI ROUTES (Query Service)
=================================================

This file implements the natural language query interface.
Users ask questions about logs in plain English, and the service converts
that to SQL queries and returns structured results.

SERVICES USED:
  - app.shared.db: TimescaleDB connection pool
  - OpenRouter API: LLM for converting NL to SQL
  - FastAPI framework

ENDPOINT 1: POST /query
=======================
PURPOSE: Accept natural language question, convert to SQL, return results

ACCEPTS:
  - JSON body:
    {
      "question": "How many temperature warnings occurred in machine_001 today?",
      "limit": 100,                              # Optional: max rows (default 100)
      "time_range": "24h" or "7d" or "30d"      # Optional: query time window
    }

RETURNS:
  - 200 OK with:
    {
      "question": "How many temperature warnings...",
      "sql_query": "SELECT COUNT(*) FROM log_events WHERE...",
      "results": [                                # Rows as list of dicts
        {
          "machine": "machine_001",
          "event_type": "temperature_warning",
          "timestamp": "2024-04-17T10:30:00Z",
          "severity": "warning",
          "count": 1
        }
      ],
      "result_count": 1,
      "query_time_ms": 234
    }

IMPLEMENTATION:
  1. Validate input:
     - question must not be empty
     - limit must be <= 1000 (prevent huge result sets)
     - time_range must be valid format

  2. Convert natural language to SQL:
     a. Call OpenRouter API with prompt:
        "Convert this natural language query to SQL for a PostgreSQL database.
         Tables available:
         - log_events (id, timestamp, source, event_type, severity, message, category, root_cause, confidence)
         - machines (id, name, type, location, status)
         
         Natural language question: {question}
         
         Requirements:
         - Only return SELECT queries (no INSERT, UPDATE, DELETE)
         - Include WHERE clause for time_range (if provided)
         - Use exact table and column names
         - Return only the SQL query without explanation"
     
     b. Extract SQL from LLM response (may be wrapped in markdown code blocks)
     c. Validate SQL (no dangerous operations, etc.)

  3. Execute SQL query:
     a. Set query timeout (30 seconds max)
     b. Execute using db pool connection
     c. Fetch results
     d. Measure query execution time

  4. Format results:
     a. Convert database rows to list of dicts
     b. Include question, SQL query, results, count, execution time
     c. Limit results to limit parameter

  5. Return 200 with results

  ERROR HANDLING:
    - 400 Bad Request if question is empty or invalid
    - 400 if LLM returns invalid SQL
    - 408 Request Timeout if query takes > 30 seconds
    - 500 if database query fails
    - Include error message explaining what went wrong

SAFETY CONSIDERATIONS:
  - Validate/sanitize LLM SQL output before execution
  - Block dangerous operations (DROP, ALTER, INSERT, UPDATE, DELETE)
  - Use prepared statements/parameterized queries (prevent SQL injection)
  - Set query timeout to prevent runaway queries
  - Log all queries for audit trail
  - Never return sensitive system information in errors

RESULT FORMATTING:
  - All timestamps as ISO 8601 format
  - Numbers as numbers (not strings)
  - NULL values as null
  - Large result sets truncated to limit parameter

ENDPOINT 2: GET /health
========================
PURPOSE: Health check for orchestration/monitoring

RETURNS:
  - 200 OK with:
    {
      "status": "healthy",
      "services": {
        "timescaledb": "connected",
        "openrouter_api": "connected"
      },
      "timestamp": "2024-04-17T10:30:00Z"
    }

IMPLEMENTATION:
  1. Check TimescaleDB:
     - Execute SELECT 1
     - If succeeds: "connected"
     - If fails: "disconnected" and return 503

  2. Check OpenRouter API:
     - Make test API call (small prompt)
     - If succeeds: "connected"
     - If fails: "disconnected" (non-fatal, still return 200 but note in services)

  3. Return status

QUERY EXAMPLES TO SUPPORT:
  These are example user queries that should work:
  - "Count logs by severity"
  - "Show me P0 events from machine_001 in the last 24 hours"
  - "What's the most common error type?"
  - "List all thermal events with timestamp and machine ID"
  - "How many logs per machine in the last week?"
  - "Show events with confidence < 0.5 that need review"

NL-TO-SQL TIPS:
  - "P0 events" = severity = 'critical'
  - "P1 events" = severity = 'error'
  - "last 24 hours" = WHERE timestamp > now() - interval '24 hours'
  - "machine_001" = source = 'machine_001'
  - "thermal events" = category = 'thermal_event'
  - "confidence < 0.5" = confidence < 0.5
"""