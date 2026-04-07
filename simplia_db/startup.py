"""Startup retry, connection verification, and background recovery utilities."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger("simplia_db")


async def check_connection(engine: AsyncEngine) -> bool:
    """Test database connectivity with a ``SELECT 1`` query.

    Returns True on success, False on any error.
    """
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.debug("check_connection failed: %s", exc)
        return False


async def init_db_with_retry(
    engine: AsyncEngine,
    *,
    max_retries: int = 5,
    retry_delay: float = 2.0,
    backoff_factor: float = 2.0,
    on_failure: object = None,
) -> bool:
    """Test database connectivity with exponential backoff.

    Does NOT crash the app -- the caller decides how to handle failure.

    Parameters
    ----------
    engine:
        The async engine to test.
    max_retries:
        Maximum number of connection attempts.
    retry_delay:
        Initial delay between retries in seconds.
    backoff_factor:
        Multiplier for the delay after each failed attempt.
    on_failure:
        Optional callable ``(attempt: int, exception: Exception) -> None``
        invoked after each failed attempt.

    Returns
    -------
    True if connection succeeded, False if all retries were exhausted.
    """
    delay = retry_delay

    for attempt in range(1, max_retries + 1):
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info("Database connection OK (attempt %d/%d)", attempt, max_retries)
            return True
        except Exception as exc:
            logger.warning(
                "Database connection failed (attempt %d/%d): %s: %s",
                attempt,
                max_retries,
                type(exc).__name__,
                exc,
            )
            if callable(on_failure):
                try:
                    on_failure(attempt, exc)
                except Exception:
                    pass

            if attempt < max_retries:
                logger.info("Retrying in %.1fs...", delay)
                await asyncio.sleep(delay)
                delay *= backoff_factor

    logger.error("Database connection failed after %d attempts", max_retries)
    return False


async def dispose_engine(engine: AsyncEngine) -> None:
    """Dispose all pooled connections, forcing fresh ones on next use.

    For NullPool this is a no-op (no connections to clear).
    For QueuePool this drops all idle connections from the pool.
    Safe to call at any time -- in-flight queries finish normally.
    """
    try:
        await engine.dispose()
        logger.info("Engine disposed: all pooled connections cleared")
    except Exception as exc:
        logger.warning("Engine dispose failed: %s", exc)


async def background_recovery(
    engine: AsyncEngine,
    on_recovered: Callable[[], Awaitable[Any]],
    *,
    check_interval: float = 30.0,
    max_interval: float = 300.0,
    backoff_factor: float = 1.5,
) -> None:
    """Background loop that waits for the DB to recover, then runs on_recovered.

    Designed to be launched as an ``asyncio.Task`` from the app lifespan
    when the DB was unavailable at startup. It polls the database at
    increasing intervals until connectivity is restored, then:

    1. Disposes the engine (clears any broken pooled connections)
    2. Calls ``on_recovered()`` (typically ``init_db()`` or equivalent)
    3. Returns (task finishes)

    Parameters
    ----------
    engine:
        The async engine to monitor.
    on_recovered:
        Async callable invoked once when the DB becomes reachable.
        Typically the app's ``init_db()`` function.
    check_interval:
        Initial polling interval in seconds (default 30s).
    max_interval:
        Maximum polling interval in seconds (default 5min).
    backoff_factor:
        Multiplier for the interval after each failed check.

    Usage in lifespan::

        from simplia_db import init_db_with_retry, background_recovery

        async def lifespan(app):
            db_ok = await init_db_with_retry(engine)
            if not db_ok:
                app.state.recovery_task = asyncio.create_task(
                    background_recovery(engine, on_recovered=init_db)
                )
            yield
            # Cancel on shutdown
            task = getattr(app.state, "recovery_task", None)
            if task and not task.done():
                task.cancel()
    """
    interval = check_interval
    logger.warning(
        "DB recovery loop started (check every %.0fs, max %.0fs)",
        check_interval,
        max_interval,
    )

    while True:
        await asyncio.sleep(interval)
        if await check_connection(engine):
            logger.info("DB recovery: connection restored, running on_recovered...")
            try:
                await dispose_engine(engine)
                await on_recovered()
                logger.info("DB recovery: on_recovered completed successfully")
            except Exception as exc:
                logger.error(
                    "DB recovery: on_recovered failed: %s: %s",
                    type(exc).__name__,
                    exc,
                )
            return
        else:
            logger.debug("DB recovery: still unavailable, next check in %.0fs", interval)
            interval = min(interval * backoff_factor, max_interval)
