"""Phase 9F6: worker-token-protected shadow-evidence admin APIs."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.deps import get_db
from main import app

from test_wyckoff_v2_9c3_discovery import _v2_configured_db
from test_wyckoff_v2_9f_cohorts import evidence_record, trigger_record
from test_wyckoff_v2_9f_outcome_evidence import outcome_row


class _BombProvider:
    def __call__(self):
        raise AssertionError("provider constructed by a read-only endpoint")


def _patch_reads(
    monkeypatch,
    records: Optional[List[Dict[str, Any]]] = None,
    rows: Optional[List[Dict[str, Any]]] = None,
    campaign_runs: Optional[List[Dict[str, Any]]] = None,
):
    """Monkeypatch every persistence read the evidence endpoints use and
    bomb any provider construction."""
    from app.routers import admin as admin_mod
    from app.workers.shadow import persistence as pers
    from app.workers.shadow.outcomes import persistence as opers

    captured: Dict[str, Any] = {"evaluations": [], "outcomes": []}

    async def fake_evaluations(**kwargs):
        captured["evaluations"].append(kwargs)
        return list(records or [])

    async def fake_outcomes(**kwargs):
        captured["outcomes"].append(kwargs)
        return list(rows or [])

    async def fake_campaign_runs(**kwargs):
        return list(campaign_runs or [])

    monkeypatch.setattr(
        pers, "fetch_strategy_shadow_evaluations", fake_evaluations
    )
    monkeypatch.setattr(pers, "fetch_shadow_campaign_runs", fake_campaign_runs)
    monkeypatch.setattr(opers, "fetch_pair_outcomes", fake_outcomes)
    monkeypatch.setattr(
        admin_mod, "get_market_data_provider", _BombProvider()
    )
    return captured


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", False)
    db = _v2_configured_db()

    async def _override():
        yield db

    app.dependency_overrides[get_db] = _override
    try:
        yield TestClient(app, raise_server_exceptions=False), db
    finally:
        app.dependency_overrides.pop(get_db, None)


EVIDENCE_PATHS = (
    "/api/admin/shadow-evidence/cohorts",
    "/api/admin/shadow-evidence/failures",
    "/api/admin/shadow-evidence/outcomes",
    "/api/admin/shadow-evidence/quality",
    "/api/admin/shadow-evidence/readiness",
    "/api/admin/shadow-evidence/export",
)


class TestWorkerTokenProtection:
    @pytest.mark.parametrize("path", EVIDENCE_PATHS)
    def test_unauthorized_get_rejected(self, monkeypatch, path):
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "test-worker-token")
        client = TestClient(app, raise_server_exceptions=False)
        assert client.get(path).status_code == 401

    def test_unauthorized_plan_rejected(self, monkeypatch):
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "test-worker-token")
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post("/api/admin/shadow-campaign-plan", json={
            "candidate_symbols": ["AAAX"],
            "as_of_sessions": ["2026-07-22"],
            "max_symbols_per_campaign": 5,
        })
        assert resp.status_code == 401

    def test_valid_token_passes(self, monkeypatch):
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "test-worker-token")
        _patch_reads(monkeypatch)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get(
            "/api/admin/shadow-evidence/cohorts",
            headers={"X-Worker-Token": "test-worker-token"},
        )
        assert resp.status_code == 200


class TestEvidenceReads:
    def test_cohorts_endpoint(self, client, monkeypatch):
        http, db = client
        _patch_reads(monkeypatch, records=[
            evidence_record(trigger=trigger_record("confirmed", price=50.0)),
        ])
        resp = http.get("/api/admin/shadow-evidence/cohorts")
        assert resp.status_code == 200
        body = resp.json()
        assert body["contract_version"] == "shadow_evidence_cohorts.v1"
        assert body["evaluated_count"] == 1
        assert body["cohorts"]["trigger_confirmed"]["record_count"] == 1
        assert body["filters"]["strategy_code"] == "wyckoff_mtf_v2"

    def test_failures_endpoint(self, client, monkeypatch):
        http, db = client
        _patch_reads(monkeypatch, records=[
            evidence_record(
                verdict="AVOID",
                rejection_reason="no_valid_selected_range",
                trigger=None, setup_state="invalid",
            ),
        ])
        resp = http.get("/api/admin/shadow-evidence/failures")
        body = resp.json()
        assert body["rejection_reason_distribution"] == {
            "no_valid_selected_range": 1
        }
        assert body["trigger_state_distribution"] == {"not_evaluated": 1}

    def test_outcomes_endpoint_reports_missing_separately(
        self, client, monkeypatch
    ):
        http, db = client
        _patch_reads(
            monkeypatch,
            records=[
                evidence_record(has_outcome=False),
                evidence_record(symbol="BBBX", has_outcome=True,
                                outcome_status="complete"),
            ],
            rows=[outcome_row(ret_1d=2.0, status="complete")],
        )
        resp = http.get("/api/admin/shadow-evidence/outcomes")
        body = resp.json()
        assert body["contract_version"] == "shadow_outcome_evidence.v1"
        assert body["total_outcome_rows"] == 1
        assert body["missing_outcome_count"] == 1

    def test_quality_endpoint_uses_discovery_read_only(
        self, client, monkeypatch
    ):
        http, db = client
        _patch_reads(monkeypatch, records=[
            evidence_record(trigger=trigger_record("confirmed", price=None)),
        ])
        resp = http.get("/api/admin/shadow-evidence/quality")
        body = resp.json()
        assert body["contract_version"] == "shadow_evidence_quality.v1"
        codes = {i["code"] for i in body["issues"]}
        assert "confirmed_trigger_missing_price" in codes
        # The configured fake DB has the v2 row -> no blocking config issue.
        assert "missing_db_pattern_row" not in codes
        assert db.writes == []

    def test_readiness_endpoint_is_advisory_only(self, client, monkeypatch):
        http, db = client
        _patch_reads(monkeypatch, records=[])
        resp = http.get("/api/admin/shadow-evidence/readiness")
        body = resp.json()
        assert body["policy_version"] == "wyckoff_v2_rollout_readiness.v1"
        assert body["advisory_status"] == "not_ready"
        assert body["rollout_mutation_performed"] is False
        assert body["rollout_defaults"]["allow_enter"] is False
        assert "enabled" not in body["advisory_status_vocabulary"]
        assert db.writes == []

    def test_readiness_override_bounds(self, client, monkeypatch):
        http, db = client
        _patch_reads(monkeypatch, records=[])
        assert http.get(
            "/api/admin/shadow-evidence/readiness?min_evaluated=1"
        ).status_code == 422
        assert http.get(
            "/api/admin/shadow-evidence/readiness?target_horizon=40D"
        ).status_code == 422
        resp = http.get(
            "/api/admin/shadow-evidence/readiness?min_evaluated=25"
        )
        assert resp.json()["thresholds"]["min_evaluated"] == 25

    def test_export_endpoint(self, client, monkeypatch):
        http, db = client
        _patch_reads(monkeypatch, records=[
            evidence_record(has_outcome=True, outcome_status="complete"),
        ])
        resp = http.get("/api/admin/shadow-evidence/export")
        body = resp.json()
        assert body["evidence"]["export_contract_version"] == (
            "shadow_evidence_export.v1"
        )
        assert body["content_sha256"]
        assert body["generated_at"]
        assert body["evidence"]["rollout_defaults"]["allow_enter"] is False
        assert db.writes == []

    def test_export_reference_bounds(self, client, monkeypatch):
        http, db = client
        _patch_reads(monkeypatch, records=[])
        assert http.get(
            "/api/admin/shadow-evidence/export?max_record_references=0"
        ).status_code == 422
        assert http.get(
            "/api/admin/shadow-evidence/export?max_record_references=201"
        ).status_code == 422


class TestSafeFailureModes:
    def test_unknown_strategy_code_returns_typed_empty(self, client, monkeypatch):
        http, db = client
        captured = _patch_reads(monkeypatch, records=[])
        resp = http.get(
            "/api/admin/shadow-evidence/cohorts?pattern_code=no_such_strategy"
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["evaluated_count"] == 0
        assert body["cohorts"]["evaluated"]["record_count"] == 0
        assert captured["evaluations"][0]["strategy_code"] == (
            "no_such_strategy"
        )

    def test_unknown_filter_vocabulary_fails_safely(self, client, monkeypatch):
        http, db = client
        _patch_reads(monkeypatch)
        assert http.get(
            "/api/admin/shadow-evidence/cohorts?trigger_state=bogus"
        ).status_code == 422
        assert http.get(
            "/api/admin/shadow-evidence/cohorts?limit=99999"
        ).status_code == 422
        assert http.get(
            "/api/admin/shadow-evidence/cohorts?min_snapshot_date=July"
        ).status_code == 422

    def test_filters_are_passed_to_persistence(self, client, monkeypatch):
        http, db = client
        captured = _patch_reads(monkeypatch)
        http.get(
            "/api/admin/shadow-evidence/cohorts"
            "?symbol=aaax&campaign_id=camp-1&min_snapshot_date=2026-07-01"
            "&strategy_version=wyckoff_mtf.v2&limit=50"
        )
        kwargs = captured["evaluations"][0]
        assert kwargs["symbol"] == "AAAX"
        assert kwargs["campaign_id"] == "camp-1"
        assert str(kwargs["min_snapshot_date"]) == "2026-07-01"
        assert kwargs["limit"] == 50

    def test_campaign_plan_endpoint_never_executes(self, client, monkeypatch):
        http, db = client
        from app.routers import admin as admin_mod
        import app.workers.shadow.campaigns as campaigns_mod

        def _bomb(*args, **kwargs):
            raise AssertionError("plan endpoint executed a campaign")

        monkeypatch.setattr(campaigns_mod, "run_shadow_campaign", _bomb)
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", _BombProvider()
        )
        resp = http.post("/api/admin/shadow-campaign-plan", json={
            "candidate_symbols": ["BBBX", "AAAX"],
            "as_of_sessions": ["2026-07-22"],
            "max_symbols_per_campaign": 25,
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["executed"] is False
        assert body["expected_campaign_count"] == 1
        assert db.writes == []

    def test_campaign_plan_validation(self, client, monkeypatch):
        http, db = client
        _patch_reads(monkeypatch)
        assert http.post("/api/admin/shadow-campaign-plan", json={
            "candidate_symbols": ["AAAX"],
            "as_of_sessions": ["2026-07-22"],
        }).status_code == 422
        assert http.post("/api/admin/shadow-campaign-plan", json={
            "experiment_code": "nope",
            "candidate_symbols": ["AAAX"],
            "as_of_sessions": ["2026-07-22"],
            "max_symbols_per_campaign": 5,
        }).status_code == 422


class TestNoPublicSurfaceChange:
    def test_evidence_routes_are_admin_only(self):
        paths = {route.path for route in app.routes}
        for path in EVIDENCE_PATHS:
            assert path in paths
        assert "/api/admin/shadow-campaign-plan" in paths
        for path in paths:
            if "evidence" in path or "campaign-plan" in path:
                assert path.startswith("/api/admin/"), path

    def test_public_routers_untouched(self):
        import subprocess
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            ["git", "diff", "--", "app/routers/public.py",
             "app/routers/shadow.py", "app/routers/outcomes.py"],
            cwd=root, capture_output=True, text=True, check=False,
        )
        assert result.stdout.strip() == ""
