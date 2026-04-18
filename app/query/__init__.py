# app.query — lazy exports so importing submodules (e.g. sql_validate) does not load FastAPI + DB.

from typing import Any

__all__ = [
    "app",
    "generate_sql",
    "execute_query",
    "query",
]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from app.query import main as _main

        return getattr(_main, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
