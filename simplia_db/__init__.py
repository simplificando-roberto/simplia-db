"""simplia-db: Shared database resilience layer for Simplia ecosystem."""

from simplia_db._url import (
    is_pooler_url,
    is_supabase_host,
    mask_url,
    normalize_async_url,
    normalize_sync_url,
)
from simplia_db._ssl import build_ssl_context
from simplia_db._search_path import install_search_path_listener, normalize_search_path
from simplia_db._connect_args import build_asyncpg_connect_args, build_psycopg2_connect_args
from simplia_db.engine import create_resilient_engine, create_resilient_sync_engine
from simplia_db.session import ResilientAsyncSession, create_session_factory, is_retryable_disconnect
from simplia_db.startup import init_db_with_retry, check_connection, dispose_engine, background_recovery
from simplia_db.health import check_db_health

__all__ = [
    "is_pooler_url",
    "is_supabase_host",
    "mask_url",
    "normalize_async_url",
    "normalize_sync_url",
    "build_ssl_context",
    "install_search_path_listener",
    "normalize_search_path",
    "build_asyncpg_connect_args",
    "build_psycopg2_connect_args",
    "create_resilient_engine",
    "create_resilient_sync_engine",
    "ResilientAsyncSession",
    "create_session_factory",
    "is_retryable_disconnect",
    "init_db_with_retry",
    "check_connection",
    "dispose_engine",
    "background_recovery",
    "check_db_health",
]
