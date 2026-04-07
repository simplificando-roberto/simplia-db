"""Tests for simplia_db._url module."""

import pytest
from simplia_db._url import is_pooler_url, is_supabase_host, mask_url, normalize_async_url, normalize_sync_url


class TestIsPoolerUrl:
    def test_pooler_hostname(self):
        url = "postgresql://user:pass@aws-1-eu-west-1.pooler.supabase.com:6543/postgres"
        assert is_pooler_url(url) is True

    def test_pooler_port_6543(self):
        url = "postgresql://user:pass@somehost.example.com:6543/mydb"
        assert is_pooler_url(url) is True

    def test_direct_supabase(self):
        url = "postgresql://user:pass@db.abc123.supabase.co:5432/postgres"
        assert is_pooler_url(url) is False

    def test_localhost(self):
        url = "postgresql://user:pass@localhost:5432/mydb"
        assert is_pooler_url(url) is False

    def test_empty_string(self):
        assert is_pooler_url("") is False

    def test_invalid_url(self):
        assert is_pooler_url("not-a-url") is False


class TestIsSupabaseHost:
    def test_supabase_co(self):
        url = "postgresql://user:pass@db.abc123.supabase.co:5432/postgres"
        assert is_supabase_host(url) is True

    def test_supabase_com(self):
        url = "postgresql://user:pass@aws-1.pooler.supabase.com:6543/postgres"
        assert is_supabase_host(url) is True

    def test_supabase_io(self):
        url = "postgresql://user:pass@something.supabase.io:5432/db"
        assert is_supabase_host(url) is True

    def test_non_supabase(self):
        url = "postgresql://user:pass@db.railway.app:5432/mydb"
        assert is_supabase_host(url) is False

    def test_localhost(self):
        assert is_supabase_host("postgresql://user:pass@localhost/db") is False


class TestNormalizeAsyncUrl:
    def test_postgres_scheme(self):
        assert normalize_async_url("postgres://u:p@h/db") == "postgresql+asyncpg://u:p@h/db"

    def test_postgresql_scheme(self):
        assert normalize_async_url("postgresql://u:p@h/db") == "postgresql+asyncpg://u:p@h/db"

    def test_already_asyncpg(self):
        url = "postgresql+asyncpg://u:p@h/db"
        assert normalize_async_url(url) == url

    def test_strips_whitespace(self):
        assert normalize_async_url("  postgres://u:p@h/db  ") == "postgresql+asyncpg://u:p@h/db"


class TestNormalizeSyncUrl:
    def test_removes_asyncpg(self):
        assert normalize_sync_url("postgresql+asyncpg://u:p@h/db") == "postgresql://u:p@h/db"

    def test_postgres_scheme(self):
        assert normalize_sync_url("postgres://u:p@h/db") == "postgresql://u:p@h/db"

    def test_sqlite_aiosqlite(self):
        assert normalize_sync_url("sqlite+aiosqlite:///test.db") == "sqlite:///test.db"


class TestMaskUrl:
    def test_masks_password(self):
        url = "postgresql://admin:secretpass@host:5432/db"
        masked = mask_url(url)
        assert "secretpass" not in masked
        assert "***" in masked
        assert "admin" in masked

    def test_no_password(self):
        url = "postgresql://host:5432/db"
        masked = mask_url(url)
        assert "host" in masked

    def test_special_chars_in_password(self):
        url = "postgresql://user:p%40ss%23word@host/db"
        masked = mask_url(url)
        assert "p%40ss%23word" not in masked
