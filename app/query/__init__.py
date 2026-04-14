# app/query/__init__.py
# Exposes the NL2SQL query function.
# The pipeline never imports from here —
# this module is only used by app-query's own routes.
#
# Usage:
#   from app.query import run_nl_query

from app.query.routes import run_nl_query

__all__ = [
    "run_nl_query",  # Accepts natural language string, returns query result
]