"""Phase 9D6: worker-token-protected shadow admin APIs."""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from main import app


RUN_ID = str(uuid.uuid4())


class _BombProvider:
    """get_market_data_provider stand-in that fails the test if constructed."""

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


def _fake_run_row(**overrides) -> Dict[str, Any]:
    row = {
        "run_id": RUN_ID,
        "experiment_code": "wyckoff_v2_vs_baseline",
        "experiment_version": "wyckoff_v2_shadow.v1",
        "status": "completed",
        "provider": "massive",
        "requested_symbols": ["LONGX"],
        "requested_limit": 1,
        "started_at": None,
        "finished_at": None,
        "error_code": None,
        "created_at": None,
    }
    row.update(overrides)
    return row


class TestWorkerTokenProtection:
    @pytest.mark.parametrize("method,path,body", [
        ("post", "/api/admin/strategies/wyckoff_mtf_v2/shadow-run",
         {"symbols": ["LONGX"]}),
        ("get", "/api/admin/shadow-runs", None),
        ("get", f"/api/admin/shadow-runs/{RUN_ID}", None),
        ("get", "/api/admin/shadow-metrics?pattern_code=wyckoff_mtf_v2", None),
        ("get", "/api/admin/shadow-comparison", None),
    ])
    def test_unauthorized_requests_rejected(self, auth_required, method, path, body):
        resp = getattr(auth_required, method)(
            path, **({"json": body} if body is not None else {})
        )
        assert resp.status_code == 401

    def test_authorized_request_passes(self, auth_required, monkeypatch):
        from app.workers.shadow import persistence as pers

        async def fake_runs(**kwargs):
            return [_fake_run_row()]

        monkeypatch.setattr(pers, "fetch_shadow_runs", fake_runs)
        resp = auth_required.get(
            "/api/admin/shadow-runs",
            headers={"X-Worker-Token": "test-worker-token"},
        )
        assert resp.status_code == 200


class TestShadowRunTrigger:
    def _patch_runner(self, monkeypatch):
        from app.routers import admin as admin_mod
        from app.workers.shadow import runner as shadow_runner

        calls: List[Dict[str, Any]] = []

        async def fake_run(provider, symbols, *, run_id=None, now_utc=None,
                           experiment=None):
            calls.append({
                "symbols": symbols,
                "run_id": run_id,
                "experiment": experiment,
            })
            return {
                "run_id": run_id, "status": "completed",
                "telemetry": {"pair_count": len(symbols)},
                "pairs": [],
            }

        class _Provider:
            name = "fake_provider"

        monkeypatch.setattr(shadow_runner, "run_shadow_comparison", fake_run)
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", lambda: _Provider()
        )
        return calls

    def test_wyckoff_candidate_resolves_wyckoff_experiment(
        self, client, monkeypatch
    ):
        calls = self._patch_runner(monkeypatch)
        resp = client.post(
            "/api/admin/strategies/wyckoff_mtf_v2/shadow-run",
            json={"symbols": ["longx", "LONGX", "aaa"]},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["experiment_code"] == "wyckoff_v2_vs_baseline"
        assert body["candidate_pattern_code"] == "wyckoff_mtf_v2"
        assert body["control_pattern_code"] == "sma150_bounce"
        assert calls[0]["symbols"] == ["LONGX", "AAA"]
        assert calls[0]["experiment"].experiment_code == (
            "wyckoff_v2_vs_baseline"
        )

    def test_sma150_v3_candidate_resolves_legacy_experiment(
        self, client, monkeypatch
    ):
        calls = self._patch_runner(monkeypatch)
        resp = client.post(
            "/api/admin/strategies/sma150_bounce_v3/shadow-run",
            json={"symbols": ["JBLX"]},
        )
        assert resp.status_code == 200
        assert resp.json()["experiment_code"] == "sma150_v2_vs_v3"

    def test_unknown_strategy_404(self, client, monkeypatch):
        calls = self._patch_runner(monkeypatch)
        resp = client.post(
            "/api/admin/strategies/no_such/shadow-run",
            json={"symbols": ["LONGX"]},
        )
        assert resp.status_code == 404
        assert calls == []

    def test_registered_strategy_without_experiment_422(
        self, client, monkeypatch
    ):
        calls = self._patch_runner(monkeypatch)
        resp = client.post(
            "/api/admin/strategies/wyckoff_mtf/shadow-run",
            json={"symbols": ["LONGX"]},
        )
        assert resp.status_code == 422
        assert calls == []

    def test_symbol_bounds_enforced(self, client, monkeypatch):
        calls = self._patch_runner(monkeypatch)
        resp = client.post(
            "/api/admin/strategies/wyckoff_mtf_v2/shadow-run",
            json={"symbols": [f"S{i}" for i in range(26)]},
        )
        assert resp.status_code == 422
        resp = client.post(
            "/api/admin/strategies/wyckoff_mtf_v2/shadow-run",
            json={"symbols": []},
        )
        assert resp.status_code == 422
        assert calls == []

    def test_background_mode_returns_run_id(self, client, monkeypatch):
        self._patch_runner(monkeypatch)
        resp = client.post(
            "/api/admin/strategies/wyckoff_mtf_v2/shadow-run",
            json={"symbols": ["LONGX"], "run_in_background": True},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["message"] == "Shadow run enqueued"
        assert uuid.UUID(body["run_id"])


class TestShadowRunReads:
    def test_list_filters_and_bounds(self, client, monkeypatch):
        from app.routers import admin as admin_mod
        from app.workers.shadow import persistence as pers

        captured: Dict[str, Any] = {}

        async def fake_runs(**kwargs):
            captured.update(kwargs)
            return [_fake_run_row()]

        monkeypatch.setattr(pers, "fetch_shadow_runs", fake_runs)
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", _BombProvider()
        )

        resp = client.get(
            "/api/admin/shadow-runs"
            "?pattern_code=wyckoff_mtf_v2&status=completed&limit=10"
        )
        assert resp.status_code == 200
        assert resp.json()["count"] == 1
        assert captured == {
            "experiment_code": "wyckoff_v2_vs_baseline",
            "status": "completed",
            "limit": 10,
        }

    def test_list_validates_status_and_limit(self, client):
        assert client.get(
            "/api/admin/shadow-runs?status=bogus"
        ).status_code == 422
        assert client.get(
            "/api/admin/shadow-runs?limit=0"
        ).status_code == 422
        assert client.get(
            "/api/admin/shadow-runs?limit=201"
        ).status_code == 422

    def test_list_unknown_pattern_code_422(self, client):
        resp = client.get("/api/admin/shadow-runs?pattern_code=sma150_bounce")
        assert resp.status_code == 422

    def test_detail_merges_bounded_pairs(self, client, monkeypatch):
        from app.workers.shadow import persistence as pers

        async def fake_run(run_id):
            return _fake_run_row(run_id=str(run_id))

        async def fake_pairs(**kwargs):
            assert kwargs["limit"] == 100
            return [{"pair_id": str(uuid.uuid4()), "symbol": "LONGX"}]

        monkeypatch.setattr(pers, "fetch_shadow_run", fake_run)
        monkeypatch.setattr(pers, "fetch_shadow_pairs", fake_pairs)
        resp = client.get(f"/api/admin/shadow-runs/{RUN_ID}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["experiment_code"] == "wyckoff_v2_vs_baseline"
        assert body["pair_count"] == 1

    def test_detail_404s(self, client, monkeypatch):
        from app.workers.shadow import persistence as pers

        async def fake_run(run_id):
            return None

        monkeypatch.setattr(pers, "fetch_shadow_run", fake_run)
        assert client.get(
            "/api/admin/shadow-runs/not-a-uuid"
        ).status_code == 404
        assert client.get(
            f"/api/admin/shadow-runs/{uuid.uuid4()}"
        ).status_code == 404


class TestShadowMetricsEndpoint:
    def test_metrics_aggregates_strategy_records(self, client, monkeypatch):
        from app.routers import admin as admin_mod
        from app.workers.shadow import persistence as pers

        captured: Dict[str, Any] = {}

        async def fake_records(**kwargs):
            captured.update(kwargs)
            return [
                {
                    "strategy_code": "wyckoff_mtf_v2",
                    "strategy_version": "wyckoff_mtf.v2",
                    "decision_policy_version": "wyckoff_mtf.policy.v1",
                    "config_hash": "cfg",
                    "experiment_code": "wyckoff_v2_vs_baseline",
                    "experiment_version": "wyckoff_v2_shadow.v1",
                    "verdict": "WATCH",
                    "score": 0.5,
                    "rejection_reason": None,
                    "policy": {
                        "enter_eligible_without_rollout_gate": True,
                        "allow_enter": False,
                        "waiting_reasons": ["enter_disabled_shadow_only"],
                    },
                    "readiness_status": "ready",
                    "evidence_categories": ["structure"],
                    "has_outcome": False,
                    "outcome_status": None,
                },
            ]

        monkeypatch.setattr(
            pers, "fetch_strategy_shadow_evaluations", fake_records
        )
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", _BombProvider()
        )
        resp = client.get(
            "/api/admin/shadow-metrics?pattern_code=wyckoff_mtf_v2&limit=50"
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["strategy_code"] == "wyckoff_mtf_v2"
        # Phase 9E5 bumped the contract additively (trigger/4H evidence).
        assert body["metrics_contract_version"] == "strategy_shadow_metrics.v2"
        assert body["evaluated_count"] == 1
        group = body["groups"][0]
        assert group["rollout_blocked_count"] == 1
        assert group["pre_rollout_enter_candidate_count"] == 1
        assert group["missing_outcome_count"] == 1
        assert captured["strategy_code"] == "wyckoff_mtf_v2"
        assert captured["limit"] == 50

    def test_metrics_limit_bounds(self, client):
        assert client.get(
            "/api/admin/shadow-metrics?pattern_code=wyckoff_mtf_v2&limit=0"
        ).status_code == 422
        assert client.get(
            "/api/admin/shadow-metrics?pattern_code=wyckoff_mtf_v2&limit=2001"
        ).status_code == 422


class TestShadowComparisonEndpoint:
    def test_comparison_reuses_existing_metrics_contract(
        self, client, monkeypatch
    ):
        from app.routers import admin as admin_mod
        from app.workers.shadow.outcomes import persistence as opers

        captured: Dict[str, Any] = {}

        async def fake_outcomes(**kwargs):
            captured.update(kwargs)
            return []

        monkeypatch.setattr(opers, "fetch_pair_outcomes", fake_outcomes)
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", _BombProvider()
        )
        resp = client.get(
            "/api/admin/shadow-comparison?pattern_code=wyckoff_mtf_v2"
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["metrics_contract_version"] == (
            "shadow_pair_resolution_metrics.v1"
        )
        assert body["candidate_strategy_code"] == "wyckoff_mtf_v2"
        assert body["total_outcomes"] == 0
        assert body["groups"] == []
        assert captured["candidate_strategy_code"] == "wyckoff_mtf_v2"

    def test_comparison_validates_inputs(self, client):
        assert client.get(
            "/api/admin/shadow-comparison?outcome_status=bogus"
        ).status_code == 422
        assert client.get(
            "/api/admin/shadow-comparison?limit=0"
        ).status_code == 422
        assert client.get(
            "/api/admin/shadow-comparison?limit=5001"
        ).status_code == 422


class TestNoPublicSurfaceChange:
    def test_new_routes_are_admin_only(self):
        paths = {route.path for route in app.routes}
        assert "/api/admin/strategies/{pattern_code}/dry-run" in paths
        assert "/api/admin/strategies/{pattern_code}/shadow-run" in paths
        assert "/api/admin/shadow-runs" in paths
        assert "/api/admin/shadow-metrics" in paths
        assert "/api/admin/shadow-comparison" in paths
        # No new public (non-admin) route was introduced for any of these.
        for suffix in ("dry-run", "shadow-run", "shadow-runs",
                       "shadow-metrics", "shadow-comparison"):
            for path in paths:
                if suffix in path:
                    assert path.startswith("/api/admin/"), path

    def test_public_pattern_listing_untouched(self):
        import subprocess
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            ["git", "diff", "--", "app/routers/public.py",
             "app/routers/shadow.py", "app/routers/outcomes.py"],
            cwd=root, capture_output=True, text=True, check=False,
        )
        assert result.stdout.strip() == ""
