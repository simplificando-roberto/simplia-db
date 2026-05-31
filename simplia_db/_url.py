"""URL detection, normalization, and masking for Supabase/PostgreSQL connections."""

from __future__ import annotations

import os
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, urlparse


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


_GENERIC_TRANSACTION_POOLER_PORTS = {6432}


def is_pooler_url(url: str) -> bool:
    """Detect transaction-mode pooler URLs (Supabase Supavisor OR PgBouncer).

    Returns True for:
    - ``*.pooler.supabase.com`` hostname
    - Port 6543 (Supabase Supavisor transaction pooler)
    - Port 6432 (self-hosted PgBouncer, e.g. the Hetzner primary)
    - When env ``DB_FORCE_TRANSACTION_POOLER`` is truthy (non-standard ports)

    Against any of these the client must disable prepared-statement caching and
    use unique prepared-statement names, because the physical backend connection
    can change between transactions.
    """
    if _env_flag("DB_FORCE_TRANSACTION_POOLER"):
        return True
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        port = int(parsed.port or 0)
        if host.endswith(".pooler.supabase.com") or port == 6543:
            return True
        return port in _GENERIC_TRANSACTION_POOLER_PORTS
    except Exception:
        return False


def _env_flag(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


# libpq-style query params that asyncpg.connect() does NOT accept as kwargs.
# They must be stripped from the URL and translated (sslmode -> ssl context).
_ASYNCPG_INCOMPATIBLE_QUERY_KEYS = {
    "sslmode",
    "sslrootcert",
    "sslcert",
    "sslkey",
    "options",
    "target_session_attrs",
}


def extract_ssl_mode(url: str) -> str | None:
    """Return the ``sslmode`` value from a URL query string, if present."""
    try:
        for key, value in parse_qsl(urlsplit(url).query, keep_blank_values=True):
            if key == "sslmode":
                return (value or "").strip() or None
    except Exception:
        pass
    return None


def strip_asyncpg_incompatible_params(url: str) -> str:
    """Remove libpq-only query params (sslmode, options, ...) from a URL.

    asyncpg rejects these as connect kwargs; SSL is applied separately via an
    ssl context. Safe no-op when there is no query string.
    """
    try:
        parts = urlsplit(url)
        if not parts.query:
            return url
        kept = [
            (k, v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k not in _ASYNCPG_INCOMPATIBLE_QUERY_KEYS
        ]
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))
    except Exception:
        return url


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
