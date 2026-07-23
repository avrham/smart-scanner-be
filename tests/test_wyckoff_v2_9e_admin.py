"""Phase 9E8: worker-token-protected campaign admin APIs."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from main import app


CAMPAIGN_ID = str(uuid.uuid4())
RUN_ID = str(uuid.uuid4())


class _BombProvider:
    def __call__(self):
        raise AssertionError("provider constructed by a read-only endpoint")


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", False)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def auth_required(monkeypatch):
    monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
    monkeypatch.setattr(settings, "WORKER_TOKEN", "test-worker-token")
    return TestClient(app, raise_server_exceptions=False)


def _campaign_run_row(**overrides) -> Dict[str, Any]:
    row = {
        "run_id": RUN_ID,
        "experiment_code": "wyckoff_v2_vs_baseline",
        "experiment_version": "wyckoff_v2_shadow.v2",
        "status": "completed",
        "provider": "massive",
        "requested_symbols": ["AAAX", "BBBX"],
        "requested_limit": 2,
        "started_at": datetime(2026, 7, 22, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 7, 22, 0, 5, tzinfo=timezone.utc),
        "error_code": None,
        "campaign": {
            "campaign_contract_version": "shadow_campaign.v1",
            "campaign_id": CAMPAIGN_ID,
            "experiment_code": "wyckoff_v2_vs_baseline",
            "chunk_index": 0,
            "chunk_count": 1,
            "as_of_date": "2026-07-22",
            "requested_count": 2,
            "max_symbols": 25,
        },
        "pair_count": 1,
        "pairs_created": 1,
        "pairs_deduplicated": 0,
        "rejected_symbols": {"fetch_error": ["BBBX"]},
        "created_at": None,
    }
    row.update(overrides)
    return row


class TestWorkerTokenProtection:
    @pytest.mark.parametrize("method,path,body", [
        ("post", "/api/admin/shadow-campaigns",
         {"experiment_code": "wyckoff_v2_vs_baseline",
          "symbols": ["AAAX"], "max_symbols": 5}),
        ("get", "/api/admin/shadow-campaigns", None),
        ("get", f"/api/admin/shadow-campaigns/{CAMPAIGN_ID}", None),
    ])
    def test_unauthorized_rejected(self, auth_required, method, path, body):
        resp = getattr(auth_required, method)(
            path, **({"json": body} if body is not None else {})
        )
        assert resp.status_code == 401

    def test_valid_token_passes(self, auth_required, monkeypatch):
        from app.workers.shadow import persistence as pers

        async def fake_runs(**kwargs):
            return [_campaign_run_row()]

        monkeypatch.setattr(pers, "fetch_shadow_campaign_runs", fake_runs)
        resp = auth_required.get(
            "/api/admin/shadow-campaigns",
            headers={"X-Worker-Token": "test-worker-token"},
        )
        assert resp.status_code == 200


class TestCampaignCreation:
    def _patch(self, monkeypatch):
        from app.routers import admin as admin_mod
        import app.workers.shadow.campaigns as campaigns_mod

        calls: List[Dict[str, Any]] = []

        async def fake_run_campaign(provider, plan, **kwargs):
            calls.append({"plan": plan})
            return {
                "campaign_contract_version": plan["campaign_contract_version"],
                "campaign_id": plan["campaign_id"],
                "experiment_code": plan["experiment_code"],
                "experiment_version": plan["experiment_version"],
                "as_of_date": plan["as_of_date"],
                "status": "completed",
                "requested_count": plan["requested_count"],
                "chunk_count": plan["chunk_count"],
                "failed_chunk_count": 0,
                "evaluated_count": plan["requested_count"],
                "rejected_count": 0,
                "unresolved_count": 0,
                "runs": [],
                "symbol_statuses": {},
            }

        class _Provider:
            name = "fake_provider"

        monkeypatch.setattr(
            campaigns_mod, "run_shadow_campaign", fake_run_campaign
        )
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", lambda: _Provider()
        )
        return calls

    def test_bounded_campaign_accepted(self, client, monkeypatch):
        calls = self._patch(monkeypatch)
        resp = client.post("/api/admin/shadow-campaigns", json={
            "experiment_code": "wyckoff_v2_vs_baseline",
            "symbols": ["bbbx", "AAAX"],
            "max_symbols": 25,
            "as_of_date": "2026-07-22",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "completed"
        assert body["as_of_date"] == "2026-07-22"
        assert calls[0]["plan"]["symbols"] == ["AAAX", "BBBX"]

    def test_missing_bound_rejected(self, client, monkeypatch):
        calls = self._patch(monkeypatch)
        resp = client.post("/api/admin/shadow-campaigns", json={
            "experiment_code": "wyckoff_v2_vs_baseline",
            "symbols": ["AAAX"],
        })
        assert resp.status_code == 422
        assert "max_symbols" in resp.json()["detail"]
        assert calls == []

    def test_oversized_request_rejected(self, client, monkeypatch):
        calls = self._patch(monkeypatch)
        resp = client.post("/api/admin/shadow-campaigns", json={
            "experiment_code": "wyckoff_v2_vs_baseline",
            "symbols": [f"S{i}" for i in range(30)],
            "max_symbols": 10,
        })
        assert resp.status_code == 422
        assert calls == []

    def test_unknown_experiment_rejected(self, client, monkeypatch):
        calls = self._patch(monkeypatch)
        resp = client.post("/api/admin/shadow-campaigns", json={
            "experiment_code": "nope",
            "symbols": ["AAAX"],
            "max_symbols": 5,
        })
        assert resp.status_code == 422
        assert calls == []

    def test_background_mode_returns_campaign_id(self, client, monkeypatch):
        self._patch(monkeypatch)
        resp = client.post("/api/admin/shadow-campaigns", json={
            "experiment_code": "wyckoff_v2_vs_baseline",
            "symbols": ["AAAX"],
            "max_symbols": 5,
            "run_in_background": True,
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["message"] == "Shadow campaign enqueued"
        assert uuid.UUID(body["campaign_id"])


class TestCampaignReads:
    def test_list_groups_by_campaign(self, client, monkeypatch):
        from app.routers import admin as admin_mod
        from app.workers.shadow import persistence as pers

        other = str(uuid.uuid4())

        async def fake_runs(**kwargs):
            assert kwargs == {"limit": 100}
            return [
                _campaign_run_row(),
                _campaign_run_row(
                    run_id=str(uuid.uuid4()),
                    campaign={
                        "campaign_contract_version": "shadow_campaign.v1",
                        "campaign_id": other,
                        "chunk_index": 0, "chunk_count": 2,
                        "as_of_date": None,
                        "requested_count": 40, "max_symbols": 50,
                    },
                    status="failed",
                ),
            ]

        monkeypatch.setattr(pers, "fetch_shadow_campaign_runs", fake_runs)
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", _BombProvider()
        )
        resp = client.get("/api/admin/shadow-campaigns")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 2
        first = body["campaigns"][0]
        assert first["campaign_id"] == CAMPAIGN_ID
        assert first["run_statuses"] == {"completed": 1}
        assert body["campaigns"][1]["run_statuses"] == {"failed": 1}

    def test_list_limit_bounds(self, client):
        assert client.get(
            "/api/admin/shadow-campaigns?limit=0"
        ).status_code == 422
        assert client.get(
            "/api/admin/shadow-campaigns?limit=501"
        ).status_code == 422

    def test_detail_assembles_statuses_and_coverage(self, client, monkeypatch):
        from app.routers import admin as admin_mod
        from app.workers.shadow import persistence as pers

        async def fake_runs(**kwargs):
            assert kwargs["campaign_id"] == CAMPAIGN_ID
            return [_campaign_run_row()]

        async def fake_pairs(**kwargs):
            assert kwargs == {"run_id": RUN_ID, "limit": 100}
            return [{
                "pair_id": str(uuid.uuid4()),
                "symbol": "AAAX",
                "control": {"verdict": "AVOID"},
                "candidate": {"verdict": "WATCH"},
                "disagreement_category": "control_avoid_candidate_watch",
            }]

        async def fake_coverage(run_ids):
            assert run_ids == [RUN_ID]
            return {
                "pair_count": 1,
                "with_outcome_count": 0,
                "missing_outcome_count": 1,
                "outcome_status_distribution": {},
            }

        monkeypatch.setattr(pers, "fetch_shadow_campaign_runs", fake_runs)
        monkeypatch.setattr(pers, "fetch_shadow_pairs", fake_pairs)
        monkeypatch.setattr(
            pers, "fetch_campaign_outcome_coverage", fake_coverage
        )
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", _BombProvider()
        )
        resp = client.get(f"/api/admin/shadow-campaigns/{CAMPAIGN_ID}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["campaign_id"] == CAMPAIGN_ID
        assert body["symbol_statuses"]["AAAX"]["status"] == "evaluated"
        assert body["symbol_statuses"]["BBBX"] == {
            "status": "rejected",
            "reason_code": "fetch_error",
            "run_id": RUN_ID,
        }
        # Missing outcomes stay MISSING — a typed state, never zero returns.
        assert body["outcome_coverage"]["missing_outcome_count"] == 1
        assert body["runs"][0]["chunk_index"] == 0

    def test_detail_404_when_unknown(self, client, monkeypatch):
        from app.workers.shadow import persistence as pers

        async def fake_runs(**kwargs):
            return []

        monkeypatch.setattr(pers, "fetch_shadow_campaign_runs", fake_runs)
        resp = client.get(f"/api/admin/shadow-campaigns/{uuid.uuid4()}")
        assert resp.status_code == 404

    def test_detail_pair_limit_bounds(self, client):
        assert client.get(
            f"/api/admin/shadow-campaigns/{CAMPAIGN_ID}?pair_limit=0"
        ).status_code == 422
        assert client.get(
            f"/api/admin/shadow-campaigns/{CAMPAIGN_ID}?pair_limit=501"
        ).status_code == 422


class TestPublicSurfaceUnchanged:
    def test_campaign_routes_are_admin_only(self):
        paths = {route.path for route in app.routes}
        assert "/api/admin/shadow-campaigns" in paths
        assert "/api/admin/shadow-campaigns/{campaign_id}" in paths
        for path in paths:
            if "campaign" in path:
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
