"""Build connect_args dicts for asyncpg and psycopg2 engines."""

from __future__ import annotations

from pathlib import Path
from typing import Union

from simplia_db._ssl import build_ssl_context
from simplia_db._url import is_pooler_url


def build_asyncpg_connect_args(
    database_url: str,
    *,
    application_name: str = "simplia_app",
    ssl_mode: str | None = None,
    ca_cert_path: Union[str, Path, None] = None,
    auto_ssl: bool = True,
    command_timeout: float | None = None,
    connect_timeout: float = 10.0,
) -> dict:
    """Build ``connect_args`` dict for an asyncpg-backed SQLAlchemy engine.

    Handles:
    - ``statement_cache_size=0`` always (safe for both pooler and direct)
    - ``prepared_statement_cache_size=0`` for pooler URLs
    - ``prepared_statement_name_func`` to avoid name collisions on poolers
    - SSL context auto-detection for Supabase hosts
    - Application name for connection monitoring
    """
    args: dict = {
        "statement_cache_size": 0,
        "timeout": connect_timeout,
    }

    # Server settings (application_name only -- search_path via event listener)
    server_settings: dict[str, str] = {}
    if application_name:
        server_settings["application_name"] = application_name
    if server_settings:
        args["server_settings"] = server_settings

    # Command timeout for queries
    if command_timeout is not None:
        args["command_timeout"] = command_timeout

    # Pooler-specific: disable prepared statement caching
    if is_pooler_url(database_url):
        args["prepared_statement_cache_size"] = 0
        args["prepared_statement_name_func"] = lambda: ""

    # SSL
    ssl_setting = build_ssl_context(
        database_url,
        ssl_mode=ssl_mode,
        ca_cert_path=ca_cert_path,
        auto_detect=auto_ssl,
    )
    if ssl_setting is not None:
        args["ssl"] = ssl_setting

    return args


def _is_psycopg3_url(database_url: str) -> bool:
    """Detect whether the SQLAlchemy URL targets psycopg3 (``postgresql+psycopg://``).

    psycopg2 uses plain ``postgresql://`` (no driver suffix).
    """
    return database_url.lstrip().startswith("postgresql+psycopg://")


def build_psycopg2_connect_args(
    database_url: str,
    *,
    application_name: str = "simplia_app",
    connect_timeout: int = 10,
) -> dict:
    """Build ``connect_args`` dict for a sync SQLAlchemy engine (psycopg2 or psycopg3).

    Despite the legacy name, this also handles psycopg3 (``postgresql+psycopg://``)
    URLs. When the driver is psycopg3 AND the URL is a Supabase pooler URL
    (transaction-mode PgBouncer/Supavisor), ``prepare_threshold=None`` is added
    to disable client-side prepared-statement caching. Without this, psycopg3
    auto-creates prepared statements with reusable names like ``_pg3_0`` that
    collide on the pooler backend across reused connections, raising
    ``DuplicatePreparedStatement``.

    psycopg2 has no equivalent auto-prepare and is unaffected.
    """
    args: dict = {
        "connect_timeout": connect_timeout,
    }
    if application_name:
        args["application_name"] = application_name

    # psycopg3 + pooler: disable auto-prepare to avoid name collisions on the pooler.
    if _is_psycopg3_url(database_url) and is_pooler_url(database_url):
        args["prepare_threshold"] = None

    return args


# Public alias with a name that reflects the function's actual scope (sync drivers,
# not just psycopg2). The legacy name is kept exported for backward compatibility.
build_sync_connect_args = build_psycopg2_connect_args
