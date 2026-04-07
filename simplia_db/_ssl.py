"""SSL context builder for Supabase and managed PostgreSQL connections."""

from __future__ import annotations

import ssl
from pathlib import Path
from typing import Union

from simplia_db._url import is_supabase_host


def build_ssl_context(
    database_url: str,
    *,
    ssl_mode: str | None = None,
    ca_cert_path: Union[str, Path, None] = None,
    auto_detect: bool = True,
) -> Union[ssl.SSLContext, bool, None]:
    """Build an SSL setting appropriate for the driver and host.

    Parameters
    ----------
    database_url:
        The database URL (used for auto-detection of Supabase hosts).
    ssl_mode:
        Explicit SSL mode: ``"disable"``, ``"require"``, ``"verify-ca"``,
        ``"verify-full"``.  When set, overrides auto-detection.
    ca_cert_path:
        Path to a CA certificate file for ``verify-ca`` / ``verify-full``.
    auto_detect:
        When True (default), automatically enable SSL for Supabase hosts
        with permissive verification (CERT_NONE).

    Returns
    -------
    - ``ssl.SSLContext`` for asyncpg ``connect_args["ssl"]``
    - ``True`` for simple SSL-on
    - ``None`` when SSL should not be configured
    """
    if ssl_mode == "disable":
        return None

    if ssl_mode in ("verify-ca", "verify-full"):
        ctx = ssl.create_default_context()
        if ca_cert_path:
            ctx.load_verify_locations(str(ca_cert_path))
        if ssl_mode == "verify-full":
            ctx.check_hostname = True
        else:
            ctx.check_hostname = False
        return ctx

    if ssl_mode == "require":
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # Auto-detect: enable permissive SSL for Supabase hosts
    if auto_detect and is_supabase_host(database_url):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    return None
