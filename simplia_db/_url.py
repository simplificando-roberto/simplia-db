"""URL detection, normalization, and masking for Supabase/PostgreSQL connections."""

from __future__ import annotations

import re
from urllib.parse import urlparse


def is_supabase_host(url: str) -> bool:
    """Detect any Supabase host (pooler or direct)."""
    try:
        host = (urlparse(url).hostname or "").lower()
        return (
            host.endswith(".supabase.com")
            or host.endswith(".supabase.co")
            or host.endswith(".supabase.io")
            or "supabase" in host
        )
    except Exception:
        return False


def is_pooler_url(url: str) -> bool:
    """Detect Supabase Supavisor/PgBouncer pooler URLs.

    Returns True for:
    - *.pooler.supabase.com hostname
    - Port 6543 (Supabase transaction-mode pooler)
    """
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        port = int(parsed.port or 0)
        return host.endswith(".pooler.supabase.com") or port == 6543
    except Exception:
        return False


def normalize_async_url(url: str) -> str:
    """Ensure URL uses ``postgresql+asyncpg://`` scheme.

    Handles common scheme variants: ``postgres://``, ``postgresql://``.
    """
    url = url.strip()
    url = re.sub(r"^postgres://", "postgresql+asyncpg://", url)
    url = re.sub(r"^postgresql://", "postgresql+asyncpg://", url)
    return url


def normalize_sync_url(url: str, *, driver: str | None = None) -> str:
    """Ensure URL uses the correct sync driver scheme.

    Parameters
    ----------
    driver:
        Explicit driver to use: ``"psycopg2"`` or ``"psycopg"`` (v3).
        When None (default), auto-detects: tries psycopg2 first, then psycopg3.
    """
    url = url.strip()
    url = re.sub(r"^postgres://", "postgresql://", url)
    url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    url = url.replace("sqlite+aiosqlite://", "sqlite://", 1)

    if not url.startswith("postgresql://"):
        return url

    if driver == "psycopg":
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if driver == "psycopg2":
        return url  # plain postgresql:// defaults to psycopg2

    # Auto-detect: prefer psycopg2 (most common), fall back to psycopg3
    try:
        import psycopg2  # noqa: F401
        return url
    except ImportError:
        pass
    try:
        import psycopg  # noqa: F401
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    except ImportError:
        pass
    return url


def mask_url(url: str) -> str:
    """Return URL with password masked for safe logging."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            masked = url.replace(f":{parsed.password}@", ":***@", 1)
            return masked
    except Exception:
        pass
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)
