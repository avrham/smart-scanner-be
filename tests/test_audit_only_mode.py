"""AUDIT_ONLY_MODE route gate + startup guard.

Proves that when audit-only mode is on the app exposes ONLY the read-only
allowlist (revision/liveness + the two shadow-cohort audit routes), blocks every
mutation/provider/docs route with a stable 404 even with a valid worker token,
keeps worker-token auth on the allowed admin audit routes, and that
AUDIT_ONLY_MODE=true + ENABLE_SCHEDULER=true fails startup without starting the
scheduler. Default (false) leaves everything unchanged.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from main import app
from app.config import Settings, settings
from app.deps import get_db, get_worker_token
from app.audit_mode import (
    AUDIT_ONLY_ALLOWLIST,
    is_audit_route_allowed,
)


class TestConfigDefault:
    def test_audit_only_mode_defaults_false(self):
        assert Settings.model_fields["AUDIT_ONLY_MODE"].default is False
        assert settings.AUDIT_ONLY_MODE is False  # unchanged in the test env


class TestAllowlistPure:
    def test_allowed_read_routes(self):
        for path in AUDIT_ONLY_ALLOWLIST:
            assert is_audit_route_allowed("GET", path) is True
            assert is_audit_route_allowed("HEAD", path) is True

    def test_mutation_method_blocked_even_on_allowed_path(self):
        # A POST to an allowlisted path is still rejected (read-only methods).
        assert is_audit_route_allowed("POST", "/api/admin/shadow-cohort/closeout") is False

    def test_unlisted_path_blocked(self):
        assert is_audit_route_allowed("GET", "/api/admin/shadow/outcomes/calculate") is False
        assert is_audit_route_allowed("GET", "/docs") is False
        assert is_audit_route_allowed("GET", "/api/patterns") is False


@pytest.fixture
def audit_client(monkeypatch):
    # Audit-only ON; worker token bypassed so we can prove the ROUTE gate is
    # independent of auth. get_db never reached for blocked routes.
    monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
    app.dependency_overrides[get_worker_token] = lambda: "t"
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.pop(get_worker_token, None)
        app.dependency_overrides.pop(get_db, None)


class TestAuditModeAllows:
    def test_version_and_root_allowed(self, audit_client):
        assert audit_client.get("/version").status_code == 200
        assert audit_client.get("/api/version").status_code == 200
        assert audit_client.get("/").status_code == 200

    def test_closeout_reaches_handler(self, audit_client):
        # Not 404: the gate passes it through to its own handler (which then
        # applies its cohort-selector validation → 422 without a selector).
        resp = audit_client.get("/api/admin/shadow-cohort/closeout")
        assert resp.status_code != 404

    def test_access_check_reaches_handler(self, audit_client):
        async def fake_db():
            yield _CleanConn()

        app.dependency_overrides[get_db] = fake_db
        resp = audit_client.get("/api/admin/shadow-cohort/access-check")
        assert resp.status_code == 200


class TestAuditModeBlocks:
    def test_blocks_outcome_calculation(self, audit_client):
        assert audit_client.post(
            "/api/admin/shadow/outcomes/calculate", json={"pending": True}
        ).status_code == 404
        assert audit_client.post(
            "/api/admin/outcomes/calculate", json={}
        ).status_code == 404

    def test_blocks_campaign_create_and_resume(self, audit_client):
        # create and resume are the SAME POST endpoint — both blocked.
        assert audit_client.post(
            "/api/admin/shadow-campaigns", json={}
        ).status_code == 404

    def test_blocks_provider_and_universe_refresh(self, audit_client):
        for path in ("/api/admin/universe/sync", "/api/admin/universe/enrich",
                     "/api/admin/tickers/refresh", "/api/admin/market/daily-sync",
                     "/api/admin/scan/start"):
            assert audit_client.post(path, json={}).status_code == 404

    def test_blocks_docs_and_openapi(self, audit_client):
        assert audit_client.get("/docs").status_code == 404
        assert audit_client.get("/redoc").status_code == 404
        assert audit_client.get("/openapi.json").status_code == 404

    def test_blocked_body_does_not_leak_allowlist(self, audit_client):
        body = audit_client.get("/api/admin/shadow-metrics").text
        assert body == '{"detail":"Not Found"}'
        for p in AUDIT_ONLY_ALLOWLIST:
            assert p not in body

    def test_valid_token_does_not_bypass_gate(self, audit_client):
        # Even a valid worker token cannot reach a blocked mutation route.
        resp = audit_client.post(
            "/api/admin/shadow/outcomes/calculate",
            headers={"X-Worker-Token": "t"}, json={"pending": True},
        )
        assert resp.status_code == 404


class TestAuditModeStillEnforcesAuth:
    def test_allowed_admin_route_still_requires_token(self, monkeypatch):
        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "unit-token")
        c = TestClient(app, raise_server_exceptions=False)
        # allowed path, but missing token → 401 (auth still enforced)
        assert c.get("/api/admin/shadow-cohort/closeout").status_code == 401
        assert c.get(
            "/api/admin/shadow-cohort/access-check",
            headers={"X-Worker-Token": "wrong"},
        ).status_code == 401


class TestNormalModeUnchanged:
    def test_docs_available_when_audit_off(self, monkeypatch):
        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", False)
        c = TestClient(app, raise_server_exceptions=False)
        assert c.get("/docs").status_code == 200
        assert c.get("/openapi.json").status_code == 200


class TestStartupGuard:
    def test_audit_plus_scheduler_fails_startup(self, monkeypatch):
        import main as main_mod

        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
        monkeypatch.setattr(settings, "ENABLE_SCHEDULER", True)
        monkeypatch.setattr(main_mod, "setup_logging", lambda: None)
        started = []
        monkeypatch.setattr(main_mod, "start_scheduler",
                            lambda: started.append(True))
        with pytest.raises(RuntimeError, match="AUDIT_ONLY_MODE"):
            with TestClient(app):
                pass
        assert started == []  # scheduler never started

    def test_audit_mode_never_starts_scheduler(self, monkeypatch):
        import main as main_mod

        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
        monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
        monkeypatch.setattr(main_mod, "setup_logging", lambda: None)
        started = []
        monkeypatch.setattr(main_mod, "start_scheduler",
                            lambda: started.append(True))
        with TestClient(app):
            pass
        assert started == []


class _CleanConn:
    """Minimal read-only fake for the access-check happy path."""

    def transaction(self, readonly=False):
        return _Txn()

    async def fetchval(self, q, *a):
        if "current_user" in q:
            return "smart_scanner_audit_reader"
        if "SHOW" in q:
            return "on"
        if "to_regclass" in q:
            return a[0]
        return None

    async def fetchrow(self, q, *a):
        if "rolsuper" in q:
            return {"rolsuper": False, "rolcreaterole": False,
                    "rolcreatedb": False, "rolreplication": False,
                    "rolbypassrls": False}
        return {"can_select": True, "can_insert": False, "can_update": False,
                "can_delete": False, "can_truncate": False, "can_trigger": False}


class _Txn:
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
