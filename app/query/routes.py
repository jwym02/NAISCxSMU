"""
Query Service Routes

All routes are implemented in app.query.main and exposed through the FastAPI app.
This file serves as documentation for the API contract.
"""

from app.query.main import app

# Routes exported from main.py:
#
# POST /query
#   - Request: QueryRequest (query: str, limit: int, time_range_hours: int)
#   - Response: QueryResponse (original_query, generated_sql, rows, row_count, execution_time_ms)
#   - Purpose: Convert NL to SQL, execute, return results
#
# GET /health
#   - Response: {status, service, timestamp}
#   - Purpose: Health check for orchestration
#
# GET /schema
#   - Response: {schema (string), models (dict)}
#   - Purpose: Return database schema documentation for reference


__all__ = ['app']
