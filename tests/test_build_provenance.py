"""Deployment build provenance (Deployment Readiness - Build Provenance).

Proves the read-only /version endpoint reports the embedded source revision
honestly (unknown when absent, never a misleading value), does not touch the
database or a provider, and keeps /health backward compatible. No live
provider or database is used.
"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from main import app
from app.config import Settings, settings
from app.deps import get_db
import app.build_info as build_info


FULL_SHA = "f6a6bd5652470f6e96e0a02432e454afe0ceb851"


@pytest.fixture
def client():
    yield TestClient(app, raise_server_exceptions=False)


class TestBuildInfoPure:
    def test_valid_full_sha_normalized_and_shortened(self):
        assert build_info.normalize_git_sha(FULL_SHA.upper()) == FULL_SHA
        assert build_info.short_git_sha(FULL_SHA) == "f6a6bd5"

    def test_unknown_and_malformed_are_not_trusted(self):
        for bad in (None, "", "unknown", "latest", "not-a-sha",
                    "main", "deadbeefzz", "  "):
            assert build_info.normalize_git_sha(bad) == "unknown"
            assert build_info.short_git_sha(bad) == "unknown"

    def test_short_sha_accepts_already_short_valid_sha(self):
        assert build_info.short_git_sha("f6a6bd5") == "f6a6bd5"

    def test_provenance_payload_shape(self):
        prov = build_info.build_provenance()
        assert set(prov) == {
            "service", "application_version", "git_sha", "git_sha_short",
            "build_time", "environment", "release",
        }
        assert prov["service"] == "smart-scanner-be"

    def test_startup_log_fields_have_no_secrets(self):
        fields = build_info.startup_log_fields()
        assert set(fields) == {
            "service", "application_version", "git_sha", "environment",
            "release",
        }
        blob = " ".join(str(v) for v in fields.values()).lower()
        for secret in ("token", "password", "supabase", "api_key", "://",
                       "secret", "massive_api", "fmp_api"):
            assert secret not in blob


class TestConfig:
    def test_deployment_metadata_defaults(self):
        fields = Settings.model_fields
        assert fields["APP_GIT_SHA"].default == "unknown"
        assert fields["APP_BUILD_TIME"].default == "unknown"
        assert fields["APP_ENVIRONMENT"].default == "local"
        assert fields["APP_RELEASE"].default == "unknown"

    def test_app_starts_without_build_metadata(self):
        # Defaults must be safe: importing the app + reading provenance never
        # requires the deployment metadata to be supplied.
        prov = build_info.build_provenance()
        assert prov["application_version"] == "1.1.0"
        # git_sha is either a real embedded sha or the honest default.
        assert prov["git_sha"] == build_info.normalize_git_sha(prov["git_sha"])


class TestVersionEndpoint:
    def test_reports_configured_metadata(self, client, monkeypatch):
        monkeypatch.setattr(settings, "APP_GIT_SHA", FULL_SHA)
        monkeypatch.setattr(settings, "APP_BUILD_TIME", "2026-07-26T00:00:00Z")
        monkeypatch.setattr(settings, "APP_ENVIRONMENT", "staging")
        monkeypatch.setattr(settings, "APP_RELEASE", "smart-scanner-be-f6a6bd5")
        resp = client.get("/version")
        assert resp.status_code == 200
        body = resp.json()
        assert body["git_sha"] == FULL_SHA
        assert body["git_sha_short"] == "f6a6bd5"
        assert body["build_time"] == "2026-07-26T00:00:00Z"
        assert body["environment"] == "staging"
        assert body["release"] == "smart-scanner-be-f6a6bd5"
        assert body["application_version"] == "1.1.0"

    def test_unknown_defaults_reported_honestly(self, client, monkeypatch):
        monkeypatch.setattr(settings, "APP_GIT_SHA", "unknown")
        monkeypatch.setattr(settings, "APP_BUILD_TIME", "unknown")
        monkeypatch.setattr(settings, "APP_RELEASE", "unknown")
        body = client.get("/version").json()
        assert body["git_sha"] == "unknown"
        assert body["git_sha_short"] == "unknown"
        assert body["build_time"] == "unknown"

    def test_malformed_sha_not_presented_as_revision(self, client, monkeypatch):
        monkeypatch.setattr(settings, "APP_GIT_SHA", "latest")
        body = client.get("/version").json()
        assert body["git_sha"] == "unknown"
        assert body["git_sha_short"] == "unknown"

    def test_api_alias_matches(self, client, monkeypatch):
        monkeypatch.setattr(settings, "APP_GIT_SHA", FULL_SHA)
        assert client.get("/api/version").json() == client.get("/version").json()

    def test_does_not_leak_secrets(self, client, monkeypatch):
        monkeypatch.setattr(settings, "APP_GIT_SHA", FULL_SHA)
        body = client.get("/version").json()
        blob = str(body).lower()
        for secret in ("token", "password", "supabase", "api_key",
                       "worker", "secret", "://"):
            assert secret not in blob

    def test_endpoint_does_not_access_database(self, monkeypatch):
        # /version has no DB dependency: it still works when get_db would fail.
        def _boom():
            raise AssertionError("/version must not touch the database")

        app.dependency_overrides[get_db] = _boom
        try:
            resp = TestClient(app, raise_server_exceptions=False).get("/version")
            assert resp.status_code == 200
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_endpoint_does_not_construct_provider(self, client, monkeypatch):
        # No provider factory is ever imported/called by /version.
        import app.providers as providers

        def _boom(*a, **k):
            raise AssertionError("/version must not construct a provider")

        monkeypatch.setattr(providers, "get_market_data_provider", _boom)
        assert client.get("/version").status_code == 200


class TestHealthBackwardCompatibility:
    def test_health_routes_still_registered(self):
        paths = {getattr(r, "path", None) for r in app.routes}
        assert "/health" in paths and "/api/health" in paths
        assert "/version" in paths and "/api/version" in paths

    def test_version_endpoint_is_public(self, client):
        # /version is operational metadata and requires no worker token; the
        # protected admin surface is unaffected (covered by admin tests).
        assert client.get("/version").status_code == 200


class TestStartupLoggingWiring:
    def test_lifespan_logs_revision_without_secrets(self, monkeypatch, caplog):
        # Drive the real lifespan safely: no scheduler side effects, and keep
        # caplog's handler attached (setup_logging would otherwise reset the
        # root handlers — we are testing the provenance line, not log config).
        import main as main_mod

        monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
        monkeypatch.setattr(settings, "APP_GIT_SHA", FULL_SHA)
        monkeypatch.setattr(main_mod, "setup_logging", lambda: None)
        with caplog.at_level(logging.INFO):
            with TestClient(app):
                pass
        start_records = [
            r for r in caplog.records
            if "Starting Smart Scanner Backend" in r.getMessage()
        ]
        assert start_records, "startup log line not emitted"
        record = start_records[0]
        extra = getattr(record, "extra_data", {})
        assert extra.get("git_sha") == FULL_SHA
        assert extra.get("service") == "smart-scanner-be"
        # never a secret
        assert "password" not in str(extra).lower()
        assert "token" not in str(extra).lower()
