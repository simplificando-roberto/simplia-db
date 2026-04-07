"""Search-path helpers for multi-schema PostgreSQL databases."""

from __future__ import annotations

import re
from typing import Union

import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine


def normalize_search_path(value: str | None, *, default: str = "public") -> str:
    """Parse, deduplicate, and validate a search-path string.

    Accepts comma- or space-separated schema names, deduplicates while
    preserving order, and returns a cleaned comma-separated string.
    """
    if not value or not value.strip():
        return default
    parts: list[str] = []
    seen: set[str] = set()
    for item in re.split(r"[\s,]+", value.strip()):
        item = item.strip().strip('"')
        if item and item not in seen:
            parts.append(item)
            seen.add(item)
    return ", ".join(parts) if parts else default


def _quote_identifier(identifier: str) -> str:
    """Quote a schema identifier for safe use in SET statements."""
    return '"' + identifier.replace('"', '""') + '"'


def _build_set_search_path_sql(search_path: str) -> str:
    """Build a ``SET search_path TO ...`` SQL string."""
    parts = [item.strip() for item in re.split(r"[\s,]+", search_path) if item.strip()]
    if not parts:
        return ""
    return "SET search_path TO " + ", ".join(_quote_identifier(p) for p in parts)


def install_search_path_listener(
    engine: Union[Engine, AsyncEngine],
    search_path: str,
) -> None:
    """Attach a ``connect`` event listener that SETs search_path on every new connection.

    This is the recommended approach for both pooler (Supavisor/PgBouncer) and
    direct connections.  Pooler connections in transaction mode reject
    ``server_settings`` startup parameters, so we always use a post-connect listener.

    Works with both asyncpg (``driver_connection.execute``) and
    psycopg2 (``cursor.execute``).
    """
    search_path = normalize_search_path(search_path)
    set_sql = _build_set_search_path_sql(search_path)
    if not set_sql:
        return

    # Resolve to sync engine for event listener attachment
    sync_engine = engine.sync_engine if isinstance(engine, AsyncEngine) else engine

    @sa.event.listens_for(sync_engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(set_sql)
        finally:
            cursor.close()
