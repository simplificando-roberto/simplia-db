"""Resilient SQLAlchemy engine factories for Supabase PostgreSQL."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable, Union

from sqlalchemy import create_engine, Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool, QueuePool

from simplia_db._connect_args import build_asyncpg_connect_args, build_psycopg2_connect_args
from simplia_db._search_path import install_search_path_listener
from simplia_db._url import is_pooler_url, mask_url, normalize_async_url, normalize_sync_url

logger = logging.getLogger("simplia_db")


def create_resilient_engine(
    database_url: str,
    *,
    application_name: str = "simplia_app",
    search_path: str | None = None,
    echo: bool = False,
    # Pool settings (ignored when pooler URL -> NullPool)
    pool_size: int = 4,
    max_overflow: int = 2,
    pool_timeout: int = 30,
    pool_recycle: int = 900,
    # SSL
    ssl_mode: str | None = None,
    ca_cert_path: Union[str, Path, None] = None,
    auto_ssl: bool = True,
    # Timeouts
    connect_timeout: float = 10.0,
    command_timeout: float | None = None,
    # JSON handling (avoids asyncpg codec introspection failures)
    json_serializer: Callable | None = None,
    json_deserializer: Callable | None = None,
) -> AsyncEngine:
    """Create an async SQLAlchemy engine with Supabase-resilient defaults.

    Automatically:
    - Detects pooler URLs -> NullPool + disabled statement caching
    - Detects Supabase hosts -> SSL context
    - Sets ``pool_pre_ping=True``
    - Installs search_path via event listener (pooler-safe)
    - Configures json_serializer/deserializer to bypass asyncpg codec
      introspection that can fail on transient connection issues
    """
    url = normalize_async_url(database_url)
    use_nullpool = is_pooler_url(url)

    connect_args = build_asyncpg_connect_args(
        url,
        application_name=application_name,
        ssl_mode=ssl_mode,
        ca_cert_path=ca_cert_path,
        auto_ssl=auto_ssl,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
    )

    kwargs: dict = {
        "echo": echo,
        "pool_pre_ping": True,
        "connect_args": connect_args,
        "json_serializer": json_serializer or json.dumps,
        "json_deserializer": json_deserializer or json.loads,
    }

    if use_nullpool:
        kwargs["poolclass"] = NullPool
        logger.debug(
            "Using NullPool for pooler URL: %s",
            mask_url(url),
        )
    else:
        # Do NOT pass poolclass=QueuePool for async engines -- SQLAlchemy
        # auto-selects AsyncAdaptedQueuePool.  Passing the sync QueuePool
        # raises: "Pool class QueuePool cannot be used with asyncio engine".
        kwargs["pool_size"] = max(1, pool_size)
        kwargs["max_overflow"] = max(0, max_overflow)
        kwargs["pool_timeout"] = pool_timeout
        kwargs["pool_recycle"] = pool_recycle
        logger.debug(
            "Using default async pool (size=%d, overflow=%d, recycle=%ds) for: %s",
            pool_size,
            max_overflow,
            pool_recycle,
            mask_url(url),
        )

    engine = create_async_engine(url, **kwargs)

    # Install search_path via event listener (safe for pooler and direct)
    if search_path:
        install_search_path_listener(engine, search_path)

    return engine


def create_resilient_sync_engine(
    database_url: str,
    *,
    application_name: str = "simplia_app",
    search_path: str | None = None,
    echo: bool = False,
    pool_size: int = 4,
    max_overflow: int = 2,
    pool_timeout: int = 30,
    pool_recycle: int = 900,
    connect_timeout: int = 10,
) -> Engine:
    """Create a sync SQLAlchemy engine (for psycopg2 apps, Celery workers, etc.)."""
    url = normalize_sync_url(database_url)
    use_nullpool = is_pooler_url(url)

    connect_args = build_psycopg2_connect_args(
        url,
        application_name=application_name,
        connect_timeout=connect_timeout,
    )

    kwargs: dict = {
        "echo": echo,
        "pool_pre_ping": True,
        "connect_args": connect_args,
    }

    if use_nullpool:
        kwargs["poolclass"] = NullPool
    else:
        kwargs["poolclass"] = QueuePool
        kwargs["pool_size"] = max(1, pool_size)
        kwargs["max_overflow"] = max(0, max_overflow)
        kwargs["pool_timeout"] = pool_timeout
        kwargs["pool_recycle"] = pool_recycle
        kwargs["pool_use_lifo"] = True

    engine = create_engine(url, **kwargs)

    if search_path:
        install_search_path_listener(engine, search_path)

    return engine
