"""Read-only admin wiring for the campaign audit + cohort closeout endpoints.

Proves both endpoints are read-only (no provider client constructed, nothing
written), 404 on an unknown campaign, and that expected-symbol membership flows
through. Maturation is NOT re-implemented here: it stays on the existing
POST /api/admin/shadow/outcomes/calculate endpoint.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from main import app
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod

from test_wyckoff_v2_9f_cohorts import evidence_record


@pytest.fixture
def client():
    # Bypass worker-token auth and the real DB connection deterministically,
    # regardless of the local REQUIRE_WORKER_TOKEN / Supabase settings (these
    # endpoints ARE token-protected in production; boundaries are covered
    # elsewhere). All persistence reads are monkeypatched per test.
    app.dependency_overrides[get_worker_token] = lambda: "test-token"
    app.dependency_overrides[get_db] = lambda: object()
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.pop(get_worker_token, None)
        app.dependency_overrides.pop(get_db, None)


def _run(requested: List[str], status: str = "completed") -> Dict[str, Any]:
    return {
        "run_id": "r1", "experiment_code": "wyckoff_v2_vs_baseline",
        "status": status, "requested_symbols": requested,
        "rejected_symbols": {}, "error_code": None,
        "pair_count": len(requested),
        "campaign": {"campaign_id": "camp-1", "as_of_date": "2026-07-24",
                     "chunk_index": 0, "chunk_count": 1},
    }


class TestCampaignAuditApi:
    def _patch(self, monkeypatch, *, runs, records):
        async def fake_runs(*, campaign_id=None, limit=100):
            return runs

        async def fake_evals(**kwargs):
            fake_evals.kwargs = kwargs
            return records

        monkeypatch.setattr(
            "app.workers.shadow.persistence.fetch_shadow_campaign_runs",
            fake_runs,
        )
        monkeypatch.setattr(
            "app.workers.shadow.persistence.fetch_strategy_shadow_evaluations",
            fake_evals,
        )
        # Guard: the audit must NEVER construct a provider client.
        def boom():
            raise AssertionError("audit constructed a provider client")

        monkeypatch.setattr(admin_mod, "get_market_data_provider", boom)
        return fake_evals

    def test_valid_campaign_audit(self, client, monkeypatch):
        records = [
            evidence_record(symbol="AAAX", snapshot="2026-07-24"),
            evidence_record(symbol="BBBX", snapshot="2026-07-24"),
        ]
        self._patch(monkeypatch, runs=[_run(["AAAX", "BBBX"])], records=records)
        resp = client.get("/api/admin/shadow-campaigns/camp-1/audit")
        assert resp.status_code == 200
        body = resp.json()
        assert body["verdict"] == "valid"
        assert body["trigger_confirmed_count"] == 0
        assert body["campaign_id"] == "camp-1"

    def test_unknown_campaign_404(self, client, monkeypatch):
        self._patch(monkeypatch, runs=[], records=[])
        resp = client.get("/api/admin/shadow-campaigns/nope/audit")
        assert resp.status_code == 404

    def test_expected_symbols_query_flows(self, client, monkeypatch):
        records = [evidence_record(symbol="AAAX", snapshot="2026-07-24")]
        # No persisted symbols → explicit expected list drives membership.
        runs = [_run([])]
        self._patch(monkeypatch, runs=runs, records=records)
        resp = client.get(
            "/api/admin/shadow-campaigns/camp-1/audit",
            params={"expected_symbols": "AAAX"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["membership_source"] == "explicit_expected_symbols"
        assert body["verdict"] == "valid"

    def test_limit_bounds(self, client, monkeypatch):
        self._patch(monkeypatch, runs=[_run(["AAAX"])], records=[])
        resp = client.get(
            "/api/admin/shadow-campaigns/camp-1/audit",
            params={"limit": 99999},
        )
        assert resp.status_code == 422

    def test_exact_comma_separated_expected_symbols_command(self, client, monkeypatch):
        # Proves the EXACT runbook form: one comma-separated `expected_symbols`
        # query value (as produced from the frozen file), covered at the HTTP
        # boundary. 50 symbols, no persisted set → explicit membership drives.
        symbols = [f"SYM{i:02d}" for i in range(50)]
        records = [
            evidence_record(symbol=s, snapshot="2026-07-24") for s in symbols
        ]
        self._patch(monkeypatch, runs=[_run([])], records=records)
        resp = client.get(
            "/api/admin/shadow-campaigns/camp-1/audit",
            params={"expected_symbols": ",".join(symbols)},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["membership_source"] == "explicit_expected_symbols"
        assert body["expected_symbol_count"] == 50
        assert body["unique_symbol_count"] == 50
        assert body["missing_symbols"] == []
        assert body["verdict"] == "valid"


class TestCohortCloseoutApi:
    def _patch(self, monkeypatch, *, records, outcome_rows):
        async def fake_records(filters):
            return records

        async def fake_rows(filters):
            return outcome_rows

        async def fake_runs(*, campaign_id=None, limit=100):
            return []

        async def fake_discovery(db, code):
            return None

        async def fake_latest():
            return None  # no local calendar → eligibility unknown, honest

        monkeypatch.setattr(
            "app.workers.shadow.evidence_review.fetch_evidence_records",
            fake_records,
        )
        monkeypatch.setattr(admin_mod, "_evidence_outcome_rows", fake_rows)
        monkeypatch.setattr(
            "app.workers.shadow.persistence.fetch_shadow_campaign_runs",
            fake_runs,
        )
        monkeypatch.setattr(
            "app.workers.strategies.discovery.discover_strategy",
            fake_discovery,
        )
        monkeypatch.setattr(
            admin_mod.market_store, "get_latest_daily_bar_date", fake_latest,
        )

        def boom():
            raise AssertionError("closeout constructed a provider client")

        monkeypatch.setattr(admin_mod, "get_market_data_provider", boom)

    def test_read_only_closeout(self, client, monkeypatch):
        records = [
            evidence_record(symbol="AAAX", snapshot="2026-05-04",
                            has_outcome=True, outcome_status="complete"),
        ]
        self._patch(monkeypatch, records=records, outcome_rows=[])
        resp = client.get(
            "/api/admin/shadow-cohort/closeout",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["closeout_contract_version"] == "shadow_cohort_closeout.v1"
        assert body["total_evaluations"] == 1
        assert "eligibility" in body
        assert "quality_audit" in body

    def test_requires_cohort_selector(self, client, monkeypatch):
        # A bare call (only the default pattern_code) must NOT scan all shadow
        # history — it returns a safe validation error instead.
        self._patch(monkeypatch, records=[], outcome_rows=[])
        resp = client.get("/api/admin/shadow-cohort/closeout")
        assert resp.status_code == 422
        assert "cohort selector" in resp.json()["detail"]

    def test_selector_accepts_campaign_id(self, client, monkeypatch):
        self._patch(monkeypatch, records=[], outcome_rows=[])
        resp = client.get(
            "/api/admin/shadow-cohort/closeout",
            params={"campaign_id": "camp-1"},
        )
        assert resp.status_code == 200

    def test_zero_records_is_not_an_error(self, client, monkeypatch):
        # A valid selector with no matching records is a clean empty report,
        # clearly distinct from the invalid-selector 422.
        self._patch(monkeypatch, records=[], outcome_rows=[])
        resp = client.get(
            "/api/admin/shadow-cohort/closeout",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_evaluations"] == 0
        assert body["total_outcome_rows"] == 0


class TestAuthEnforced:
    """Token enforcement ON for the two audit endpoints (REQUIRE_WORKER_TOKEN)."""

    @pytest.fixture
    def client(self, monkeypatch):
        from app.config import settings

        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "unit-test-token")
        app.dependency_overrides[get_db] = lambda: object()

        async def fake_runs(*, campaign_id=None, limit=100):
            return [_run(["AAAX"])] if campaign_id == "camp-1" else []

        async def fake_evals(**kwargs):
            return []

        async def fake_records(filters):
            return []

        async def fake_rows(filters):
            return []

        async def fake_discovery(db, code):
            return None

        async def fake_latest():
            return None

        monkeypatch.setattr(
            "app.workers.shadow.persistence.fetch_shadow_campaign_runs",
            fake_runs,
        )
        monkeypatch.setattr(
            "app.workers.shadow.persistence.fetch_strategy_shadow_evaluations",
            fake_evals,
        )
        monkeypatch.setattr(
            "app.workers.shadow.evidence_review.fetch_evidence_records",
            fake_records,
        )
        monkeypatch.setattr(admin_mod, "_evidence_outcome_rows", fake_rows)
        monkeypatch.setattr(
            "app.workers.strategies.discovery.discover_strategy", fake_discovery
        )
        monkeypatch.setattr(
            admin_mod.market_store, "get_latest_daily_bar_date", fake_latest
        )
        try:
            yield TestClient(app, raise_server_exceptions=False)
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_campaign_audit_missing_token_rejected(self, client):
        assert client.get(
            "/api/admin/shadow-campaigns/camp-1/audit"
        ).status_code == 401

    def test_campaign_audit_invalid_token_rejected(self, client):
        assert client.get(
            "/api/admin/shadow-campaigns/camp-1/audit",
            headers={"X-Worker-Token": "wrong"},
        ).status_code == 401

    def test_campaign_audit_valid_token_reaches(self, client):
        resp = client.get(
            "/api/admin/shadow-campaigns/camp-1/audit",
            headers={"X-Worker-Token": "unit-test-token"},
        )
        assert resp.status_code == 200

    def test_closeout_missing_token_rejected(self, client):
        assert client.get(
            "/api/admin/shadow-cohort/closeout",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
        ).status_code == 401

    def test_closeout_valid_token_reaches(self, client):
        resp = client.get(
            "/api/admin/shadow-cohort/closeout",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
            headers={"X-Worker-Token": "unit-test-token"},
        )
        assert resp.status_code == 200
