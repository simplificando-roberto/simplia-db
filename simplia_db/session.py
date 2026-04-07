"""Resilient async session with automatic retry on transient disconnects."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.sql import ClauseElement

logger = logging.getLogger("simplia_db")

# Error messages that indicate a transient disconnect safe to retry
_RETRYABLE_MESSAGES = (
    "connection was closed",
    "ssl connection has been closed",
    "connection refused",
    "connection reset",
    "broken pipe",
    "server closed the connection unexpectedly",
    "terminating connection due to administrator command",
    "could not connect to server",
    "remaining connection slots are reserved",
)


def is_retryable_disconnect(exc: Exception) -> bool:
    """Check if an exception represents a transient disconnect safe to retry."""
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True
    msg = str(exc).lower()
    return any(pattern in msg for pattern in _RETRYABLE_MESSAGES)


def _is_read_only_statement(statement: Any) -> bool:
    """Best-effort check for read-only (SELECT) statements."""
    if isinstance(statement, str):
        return statement.strip().upper().startswith("SELECT")
    if isinstance(statement, ClauseElement):
        return not getattr(statement, "is_dml", False)
    return False


class ResilientAsyncSession(AsyncSession):
    """AsyncSession with one automatic retry on transient read-query disconnects.

    Only retries read-only (SELECT) statements to avoid double-mutation.
    Write operations raise immediately on disconnect.
    """

    async def execute(self, statement: Any, params: Any = None, **kwargs: Any) -> Any:
        for attempt in range(2):
            try:
                return await super().execute(statement, params=params, **kwargs)
            except DBAPIError as exc:
                can_retry = (
                    attempt == 0
                    and _is_read_only_statement(statement)
                    and is_retryable_disconnect(exc)
                )
                if not can_retry:
                    raise
                logger.warning(
                    "Transient DB disconnect on read query; retrying once.",
                    exc_info=True,
                )
                await self.rollback()
        # Unreachable, but satisfies type checkers
        raise RuntimeError("Retry loop exhausted")


def create_session_factory(
    engine: AsyncEngine,
    *,
    resilient: bool = True,
    expire_on_commit: bool = False,
    autoflush: bool = False,
) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory.

    Parameters
    ----------
    engine:
        The async engine to bind sessions to.
    resilient:
        When True (default), use ``ResilientAsyncSession`` which retries
        read-only queries once on transient disconnects.
    expire_on_commit:
        SQLAlchemy expire_on_commit setting.
    autoflush:
        SQLAlchemy autoflush setting.
    """
    return async_sessionmaker(
        engine,
        class_=ResilientAsyncSession if resilient else AsyncSession,
        expire_on_commit=expire_on_commit,
        autoflush=autoflush,
    )
