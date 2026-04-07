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


def build_psycopg2_connect_args(
    database_url: str,
    *,
    application_name: str = "simplia_app",
    connect_timeout: int = 10,
) -> dict:
    """Build ``connect_args`` dict for a psycopg2-backed sync SQLAlchemy engine."""
    args: dict = {
        "connect_timeout": connect_timeout,
    }
    if application_name:
        args["application_name"] = application_name
    return args
