"""Database health check utilities."""

from __future__ import annotations

import logging
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger("simplia_db")


async def check_db_health(engine: AsyncEngine) -> dict:
    """Async health check: ``SELECT 1`` with timing and pool metrics.

    Returns a dict with:
    - ``status``: ``"healthy"``, ``"degraded"``, or ``"unhealthy"``
    - ``response_time_ms``: round-trip time in milliseconds
    - ``pool``: pool utilization metrics (if available)
    - ``error``: error message (only when unhealthy)
    """
    start = time.monotonic()
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        elapsed_ms = (time.monotonic() - start) * 1000

        # Pool metrics (QueuePool only; NullPool returns empty)
        pool_info = {}
        pool = engine.pool
        if hasattr(pool, "size"):
            pool_info = {
                "size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
            }

        status = "healthy" if elapsed_ms < 5000 else "degraded"
        return {
            "status": status,
            "response_time_ms": round(elapsed_ms, 2),
            "pool": pool_info,
        }
    except Exception as exc:
        elapsed_ms = (time.monotonic() - start) * 1000
        return {
            "status": "unhealthy",
            "response_time_ms": round(elapsed_ms, 2),
            "error": f"{type(exc).__name__}: {exc}",
            "pool": {},
        }
