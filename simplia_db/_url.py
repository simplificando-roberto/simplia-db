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


def normalize_sync_url(url: str) -> str:
    """Ensure URL uses plain ``postgresql://`` scheme (for psycopg2/sync engines)."""
    url = url.strip()
    url = re.sub(r"^postgres://", "postgresql://", url)
    url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    url = url.replace("sqlite+aiosqlite://", "sqlite://", 1)
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
