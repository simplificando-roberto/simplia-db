"""Tests for simplia_db._connect_args module.

Focus: the psycopg3-vs-psycopg2 + pooler-vs-direct matrix that drives
whether ``prepare_threshold=None`` must be passed to avoid
``DuplicatePreparedStatement`` errors against Supabase's transaction-mode
pooler.
"""

from __future__ import annotations

import pytest

from simplia_db._connect_args import (
    build_asyncpg_connect_args,
    build_psycopg2_connect_args,
    build_sync_connect_args,
    _is_psycopg3_url,
)


POOLER_PSYCOPG3 = "postgresql+psycopg://user:pass@aws-1-eu-west-1.pooler.supabase.com:6543/postgres"
POOLER_PSYCOPG2 = "postgresql://user:pass@aws-1-eu-west-1.pooler.supabase.com:6543/postgres"
DIRECT_PSYCOPG3 = "postgresql+psycopg://user:pass@db.abc123.supabase.co:5432/postgres"
DIRECT_PSYCOPG2 = "postgresql://user:pass@db.abc123.supabase.co:5432/postgres"
LOCAL_PSYCOPG2 = "postgresql://user:pass@localhost:5432/mydb"
LOCAL_PSYCOPG3 = "postgresql+psycopg://user:pass@localhost:5432/mydb"


class TestIsPsycopg3Url:
    def test_psycopg3_pooler(self):
        assert _is_psycopg3_url(POOLER_PSYCOPG3) is True

    def test_psycopg3_direct(self):
        assert _is_psycopg3_url(DIRECT_PSYCOPG3) is True

    def test_psycopg3_local(self):
        assert _is_psycopg3_url(LOCAL_PSYCOPG3) is True

    def test_psycopg2_pooler(self):
        assert _is_psycopg3_url(POOLER_PSYCOPG2) is False

    def test_psycopg2_direct(self):
        assert _is_psycopg3_url(DIRECT_PSYCOPG2) is False

    def test_async_url_not_sync_psycopg3(self):
        # An asyncpg URL should not be flagged as psycopg3 even with port 6543.
        url = "postgresql+asyncpg://u:p@host.pooler.supabase.com:6543/db"
        assert _is_psycopg3_url(url) is False

    def test_leading_whitespace(self):
        assert _is_psycopg3_url("   " + POOLER_PSYCOPG3) is True

    def test_empty(self):
        assert _is_psycopg3_url("") is False


class TestBuildSyncConnectArgsPsycopg3Pooler:
    """The bug we are fixing: psycopg3 + pooler MUST disable auto-prepare."""

    def test_psycopg3_pooler_sets_prepare_threshold_none(self):
        args = build_sync_connect_args(POOLER_PSYCOPG3)
        assert "prepare_threshold" in args
        assert args["prepare_threshold"] is None

    def test_legacy_alias_same_behavior(self):
        legacy = build_psycopg2_connect_args(POOLER_PSYCOPG3)
        new = build_sync_connect_args(POOLER_PSYCOPG3)
        assert legacy == new


class TestBuildSyncConnectArgsOtherCombos:
    """psycopg2 has no auto-prepare; psycopg3-direct does not collide on a backend pooler."""

    def test_psycopg2_pooler_no_prepare_threshold(self):
        args = build_psycopg2_connect_args(POOLER_PSYCOPG2)
        assert "prepare_threshold" not in args

    def test_psycopg2_direct_no_prepare_threshold(self):
        args = build_psycopg2_connect_args(DIRECT_PSYCOPG2)
        assert "prepare_threshold" not in args

    def test_psycopg3_direct_no_prepare_threshold(self):
        # Direct connection: no shared backend, prepared statement names do not
        # collide. Leaving prepare_threshold at its default keeps performance
        # benefit of auto-prepare.
        args = build_psycopg2_connect_args(DIRECT_PSYCOPG3)
        assert "prepare_threshold" not in args

    def test_localhost_psycopg2(self):
        args = build_psycopg2_connect_args(LOCAL_PSYCOPG2)
        assert "prepare_threshold" not in args

    def test_localhost_psycopg3(self):
        args = build_psycopg2_connect_args(LOCAL_PSYCOPG3)
        assert "prepare_threshold" not in args


class TestBuildSyncConnectArgsCommonFields:
    def test_application_name_default(self):
        args = build_psycopg2_connect_args(DIRECT_PSYCOPG2)
        assert args["application_name"] == "simplia_app"

    def test_application_name_override(self):
        args = build_psycopg2_connect_args(
            DIRECT_PSYCOPG2, application_name="insaidrv3_celery"
        )
        assert args["application_name"] == "insaidrv3_celery"

    def test_connect_timeout_default(self):
        args = build_psycopg2_connect_args(DIRECT_PSYCOPG2)
        assert args["connect_timeout"] == 10

    def test_connect_timeout_override(self):
        args = build_psycopg2_connect_args(DIRECT_PSYCOPG2, connect_timeout=30)
        assert args["connect_timeout"] == 30


class TestBuildAsyncpgUnaffected:
    """The async path was already correct — verify the patch did not regress it."""

    def test_pooler_disables_prepared_statement_cache(self):
        url = "postgresql+asyncpg://u:p@aws-1.pooler.supabase.com:6543/db"
        args = build_asyncpg_connect_args(url, auto_ssl=False)
        assert args["statement_cache_size"] == 0
        assert args["prepared_statement_cache_size"] == 0
        assert callable(args["prepared_statement_name_func"])
        assert args["prepared_statement_name_func"]() == ""

    def test_direct_keeps_default_prepared_statement_cache(self):
        url = "postgresql+asyncpg://u:p@db.abc.supabase.co:5432/db"
        args = build_asyncpg_connect_args(url, auto_ssl=False)
        assert args["statement_cache_size"] == 0
        assert "prepared_statement_cache_size" not in args
        assert "prepared_statement_name_func" not in args
