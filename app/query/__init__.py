# app/query/__init__.py
# Exposes the NL2SQL query service.

from app.query.main import app, generate_sql, execute_query, query

__all__ = [
    "app",           # FastAPI application instance
    "generate_sql",  # Convert natural language to SQL
    "execute_query", # Execute SQL and return results
    "query",         # Main query endpoint (POST /query)
]