"""Explicit audit database configuration: validation, redaction, selection,
fail-closed gate. No live database is used (mocked / dependency-overridden).
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from main import app
from app.config import settings
from app.deps import get_db, get_worker_token, close_db_pool
import app.deps as deps
from app.audit_db import (
    MODE_AUDIT_EXPLICIT,
    MODE_AUDIT_UNCONFIGURED,
    MODE_DEFAULT_SUPABASE,
    MODE_HISTORY_WARMUP_EXPLICIT,
    MODE_HISTORY_WARMUP_UNCONFIGURED,
    AuditDatabaseError,
    audit_dsn_diagnostic,
    get_connection_mode,
    history_warmup_database_configured,
    select_connection_plan,
    validate_audit_database_url,
)

VALID = ("postgresql://smart_scanner_audit_reader:p%40ss@"
         "aws-0-eu-central-1.pooler.supabase.com:5432/postgres?sslmode=require")
WARMER_DSN = ("postgresql://smart_scanner_history_warmer:w%40rm@"
              "127.0.0.1:5432/warmupdb?sslmode=disable")


@pytest.fixture(autouse=True)
def _restore_settings():
    saved = (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL,
             settings.AUDIT_EXPECTED_DB_ROLE,
             settings.MAINTENANCE_ONLY_MODE,
             settings.HISTORY_WARMUP_ONLY_MODE,
             settings.HISTORY_WARMUP_DATABASE_URL)
    yield
    (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL,
     settings.AUDIT_EXPECTED_DB_ROLE,
     settings.MAINTENANCE_ONLY_MODE,
     settings.HISTORY_WARMUP_ONLY_MODE,
     settings.HISTORY_WARMUP_DATABASE_URL) = saved


class TestUrlValidation:
    def test_valid_url_parsed(self):
        info = validate_audit_database_url(VALID)
        assert info["user"] == "smart_scanner_audit_reader"
        assert info["port"] == 5432
        assert info["dbname"] == "postgres"
        assert info["sslmode"] == "require"

    @pytest.mark.parametrize("url", [
        "mysql://u:p@h:5432/d",
        "postgresql://:p@h:5432/d",
        "postgresql://u@h:5432/d",
        "postgresql://u:p@h/d",
        "postgresql://u:p@h:5432/",
        "postgresql://u:p@h:5432/d#frag",
        "postgresql://u:p@h:5432/d?foo=1",
        "",
    ])
    def test_invalid_urls_rejected_and_redacted(self, url):
        with pytest.raises(AuditDatabaseError) as exc:
            validate_audit_database_url(url)
        msg = str(exc.value)
        # never leak the password / full DSN
        assert "secret" not in msg.lower() or "secret" in "AUDIT_DATABASE_URL".lower()
        assert ":p@" not in msg and "p%40ss" not in msg and url not in msg or url == ""

    def test_diagnostic_hides_host_and_password(self):
        settings.AUDIT_DATABASE_URL = VALID
        diag = audit_dsn_diagnostic()
        assert diag == {
            "configured": True, "valid": True, "host_present": True,
            "port": 5432, "database_present": True,
            "user": "smart_scanner_audit_reader", "ssl_mode": "require",
        }
        # no host value, no password anywhere
        blob = str(diag).lower()
        assert "pooler.supabase.com" not in blob and "p%40ss" not in blob

    def test_diagnostic_unconfigured(self):
        settings.AUDIT_DATABASE_URL = ""
        assert audit_dsn_diagnostic() == {"configured": False}


class TestConnectionSelection:
    def test_mode_default_when_audit_off(self):
        settings.AUDIT_ONLY_MODE = False
        assert get_connection_mode() == MODE_DEFAULT_SUPABASE
        mode, candidates, kwargs = select_connection_plan()
        assert mode == MODE_DEFAULT_SUPABASE
        assert len(candidates) == 3          # legacy Supabase-derived candidates
        assert "statement_cache_size" not in kwargs

    def test_audit_mode_without_url_fails_closed(self):
        settings.AUDIT_ONLY_MODE = True
        settings.AUDIT_DATABASE_URL = ""
        assert get_connection_mode() == MODE_AUDIT_UNCONFIGURED
        with pytest.raises(AuditDatabaseError, match="audit database not configured"):
            select_connection_plan()

    def test_audit_mode_with_url_uses_only_explicit_dsn(self):
        settings.AUDIT_ONLY_MODE = True
        settings.AUDIT_DATABASE_URL = VALID
        assert get_connection_mode() == MODE_AUDIT_EXPLICIT
        mode, candidates, kwargs = select_connection_plan()
        assert mode == MODE_AUDIT_EXPLICIT
        assert candidates == [("audit_explicit", VALID)]   # no legacy fallback
        assert kwargs["statement_cache_size"] == 0

    def test_close_db_pool_resets_state(self):
        # no-op safe when pool is None
        asyncio.run(close_db_pool())
        assert deps._db_pool is None


class TestHistoryWarmupConnectionSelection:
    """HISTORY_WARMUP_ONLY_MODE gets its OWN explicit DSN + role — it must never
    fall back to the Supabase-derived default (which would target the shared
    store as the default identity). Fail closed when no warmup DSN is set."""

    def test_warmup_mode_without_url_fails_closed(self):
        settings.AUDIT_ONLY_MODE = False
        settings.MAINTENANCE_ONLY_MODE = False
        settings.HISTORY_WARMUP_ONLY_MODE = True
        settings.HISTORY_WARMUP_DATABASE_URL = ""
        assert history_warmup_database_configured() is False
        assert get_connection_mode() == MODE_HISTORY_WARMUP_UNCONFIGURED
        with pytest.raises(AuditDatabaseError,
                           match="history-warmup database not configured"):
            select_connection_plan()

    def test_warmup_mode_with_url_uses_only_explicit_dsn(self):
        settings.AUDIT_ONLY_MODE = False
        settings.MAINTENANCE_ONLY_MODE = False
        settings.HISTORY_WARMUP_ONLY_MODE = True
        settings.HISTORY_WARMUP_DATABASE_URL = WARMER_DSN
        assert history_warmup_database_configured() is True
        assert get_connection_mode() == MODE_HISTORY_WARMUP_EXPLICIT
        mode, candidates, kwargs = select_connection_plan()
        assert mode == MODE_HISTORY_WARMUP_EXPLICIT
        assert candidates == [("history_warmup_explicit", WARMER_DSN)]  # no fallback
        assert len(candidates) == 1
        assert kwargs["statement_cache_size"] == 0   # write-capable, pooler-safe

    def test_warmup_mode_precedes_default_supabase_path(self):
        # Even with valid Supabase settings present, warmup mode never returns
        # the 3 legacy Supabase-derived candidates.
        settings.AUDIT_ONLY_MODE = False
        settings.MAINTENANCE_ONLY_MODE = False
        settings.HISTORY_WARMUP_ONLY_MODE = True
        settings.HISTORY_WARMUP_DATABASE_URL = WARMER_DSN
        mode, candidates, _ = select_connection_plan()
        assert mode == MODE_HISTORY_WARMUP_EXPLICIT
        assert all(lbl == "history_warmup_explicit" for lbl, _ in candidates)


class TestGetDbFailClosed:
    @pytest.fixture
    def client(self):
        app.dependency_overrides[get_worker_token] = lambda: "t"
        try:
            yield TestClient(app, raise_server_exceptions=False)
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)

    def test_access_check_503_when_audit_unconfigured(self, client, monkeypatch):
        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
        monkeypatch.setattr(settings, "AUDIT_DATABASE_URL", "")
        asyncio.run(close_db_pool())
        resp = client.get("/api/admin/shadow-cohort/access-check")
        assert resp.status_code == 503
        body = resp.text.lower()
        assert "not configured" in body
        assert "postgresql://" not in body and "password" not in body

    def test_closeout_503_when_audit_unconfigured(self, client, monkeypatch):
        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
        monkeypatch.setattr(settings, "AUDIT_DATABASE_URL", "")
        asyncio.run(close_db_pool())
        resp = client.get(
            "/api/admin/shadow-cohort/closeout",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
        )
        assert resp.status_code == 503


class TestCloseoutFailClosedGate:
    """A configured-but-not-ready identity makes closeout refuse (409)."""

    @pytest.fixture
    def client(self):
        app.dependency_overrides[get_worker_token] = lambda: "t"
        try:
            yield TestClient(app, raise_server_exceptions=False)
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)

    def test_closeout_refuses_when_not_ready(self, client, monkeypatch):
        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
        monkeypatch.setattr(settings, "AUDIT_DATABASE_URL", VALID)
        monkeypatch.setattr(settings, "AUDIT_EXPECTED_DB_ROLE",
                            "smart_scanner_audit_reader")

        # Fake connection: a broad, writable identity -> access-check not ready.
        from test_audit_access_check import _FakeConn

        async def fake_db():
            yield _FakeConn(writes=True)

        app.dependency_overrides[get_db] = fake_db
        resp = client.get(
            "/api/admin/shadow-cohort/closeout",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
        )
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["error"] == "closeout_not_ready"
        assert detail["reasons"]
        # no DSN / credential leak
        assert "postgresql://" not in resp.text and "password" not in resp.text.lower()
