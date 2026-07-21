"""Phase 8.1B2: admin calculate + shadow outcome read APIs."""

import json
import uuid
from datetime import date

import pytest
from fastapi.testclient import TestClient

from main import app
import app.routers.admin as admin_mod
import app.routers.shadow as shadow_router_mod


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


class TestAdminOutcomeCalculateApi:
    def _patch(self, monkeypatch, summary=None):
        recorded = {}

        async def fake_run(provider, **kwargs):
            recorded.update(kwargs)
            recorded["provider"] = provider
            return summary or {
                "outcome_run_id": kwargs.get("outcome_run_id"),
                "status": "completed",
                "telemetry": {"pairs_selected": 0, "calculated": 0},
                "pairs": [],
            }

        monkeypatch.setattr(
            "app.workers.shadow.outcomes.service.run_shadow_outcome_calculation",
            fake_run,
        )
        # Endpoint imports the symbol at call time from the service module.
        import app.workers.shadow.outcomes.service as svc
        monkeypatch.setattr(svc, "run_shadow_outcome_calculation", fake_run)
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider",
            lambda: type("P", (), {"name": "massive"})(),
        )
        return recorded

    def test_requires_bounded_selector(self, client, monkeypatch):
        self._patch(monkeypatch)
        resp = client.post("/api/admin/shadow/outcomes/calculate", json={})
        assert resp.status_code == 422

    def test_limit_default_and_hard_cap(self, client, monkeypatch):
        recorded = self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/outcomes/calculate",
            json={"pending": True},
        )
        assert resp.status_code == 200
        assert recorded["limit"] == 50

        resp = client.post(
            "/api/admin/shadow/outcomes/calculate",
            json={"pending": True, "limit": 201},
        )
        assert resp.status_code == 422

    def test_malformed_uuid_rejection(self, client, monkeypatch):
        self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/outcomes/calculate",
            json={"pair_ids": ["not-uuid"]},
        )
        assert resp.status_code == 422

    def test_selectors_and_compose(self, client, monkeypatch):
        recorded = self._patch(monkeypatch)
        pid = str(uuid.uuid4())
        rid = str(uuid.uuid4())
        resp = client.post(
            "/api/admin/shadow/outcomes/calculate",
            json={
                "pair_ids": [pid],
                "symbols": ["dhr"],
                "run_id": rid,
                "pending": True,
                "include_recalc": True,
                "limit": 10,
            },
        )
        assert resp.status_code == 200
        assert recorded["pair_ids"] == [pid]
        assert recorded["symbols"] == ["DHR"]
        assert recorded["run_id"] == rid
        assert recorded["pending"] is True
        assert recorded["include_recalc"] is True
        assert recorded["limit"] == 10

    def test_sync_execution(self, client, monkeypatch):
        recorded = self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/outcomes/calculate",
            json={"symbols": ["DHR"], "run_in_background": False},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "completed"
        assert "outcome_run_id" in body
        assert recorded.get("outcome_run_id") == body["outcome_run_id"]

    def test_background_enqueue_behavior(self, client, monkeypatch):
        self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/outcomes/calculate",
            json={"symbols": ["DHR"], "run_in_background": True},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "outcome_run_id" in body
        assert "enqueued" in body["message"].lower()
        assert "resum" not in json.dumps(body).lower()


class TestOutcomeReadApis:
    def test_list_filters_and_compose(self, client, monkeypatch):
        captured = {}

        async def fake_fetch(**kwargs):
            captured.update(kwargs)
            return [{
                "pair": {"pair_id": "p", "symbol": "DHR"},
                "control": {"verdict": "ENTER"},
                "candidate": {"verdict": "WATCH"},
                "disagreement_category": "v2_enter_v3_watch",
                "outcome": {"outcome_status": "partial", "returns": {}},
            }]

        monkeypatch.setattr(shadow_router_mod, "fetch_pair_outcomes", fake_fetch)
        rid = str(uuid.uuid4())
        pid = str(uuid.uuid4())
        resp = client.get("/api/shadow/outcomes", params={
            "pair_id": pid,
            "symbol": "DHR",
            "run_id": rid,
            "outcome_status": "partial",
            "forward_provider": "massive",
            "control_verdict": "enter",
            "candidate_verdict": "watch",
            "disagreement_category": "v2_enter_v3_watch",
            "control_strategy_version": "sma150.v2",
            "candidate_strategy_version": "sma150.v3",
            "control_decision_policy_version": "pol-a",
            "candidate_decision_policy_version": "pol-b",
            "control_config_hash": "c1",
            "candidate_config_hash": "c2",
            "min_snapshot_date": "2026-07-01",
            "max_snapshot_date": "2026-07-20",
            "limit": 25,
        })
        assert resp.status_code == 200
        assert captured["pair_id"] == pid
        assert captured["symbol"] == "DHR"
        assert captured["run_id"] == rid
        assert captured["outcome_status"] == "partial"
        assert captured["forward_provider"] == "massive"
        assert captured["control_verdict"] == "ENTER"
        assert captured["candidate_verdict"] == "WATCH"
        assert captured["disagreement_category_filter"] == "v2_enter_v3_watch"
        assert captured["limit"] == 25
        body = resp.json()
        assert body["count"] == 1
        assert "frame_snapshot" not in json.dumps(body)

    def test_list_excludes_full_frame_snapshot(self, client, monkeypatch):
        async def fake_fetch(**kwargs):
            return [{
                "pair": {"pair_id": "p", "symbol": "DHR", "frame_bar_count": 500},
                "control": {},
                "candidate": {},
                "outcome": {},
            }]

        monkeypatch.setattr(shadow_router_mod, "fetch_pair_outcomes", fake_fetch)
        body = client.get("/api/shadow/outcomes").json()
        assert "frame_snapshot" not in json.dumps(body)

    def test_detail_returns_outcome_and_frozen_decisions(self, client, monkeypatch):
        pid = str(uuid.uuid4())

        async def fake_detail(pair_id):
            return {
                "pair_id": pair_id,
                "symbol": "DHR",
                "frame_summary": {
                    "frame_hash": "fh",
                    "frame_bar_count": 500,
                },
                "evaluations": {
                    "control": {"verdict": "ENTER", "strategy_version": "sma150.v2"},
                    "candidate": {"verdict": "WATCH", "strategy_version": "sma150.v3"},
                },
                "disagreement_category": "v2_enter_v3_watch",
                "outcome": {
                    "outcome_status": "partial",
                    "reference_price": 100.0,
                    "revision_notes": [],
                },
            }

        monkeypatch.setattr(
            shadow_router_mod, "fetch_pair_outcome_detail", fake_detail
        )
        resp = client.get(f"/api/shadow/outcomes/{pid}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["evaluations"]["control"]["verdict"] == "ENTER"
        assert body["outcome"]["reference_price"] == 100.0
        assert "frame_summary" in body
        assert "frame_snapshot" not in body

    def test_detail_404(self, client, monkeypatch):
        async def fake_detail(pair_id):
            return None

        monkeypatch.setattr(
            shadow_router_mod, "fetch_pair_outcome_detail", fake_detail
        )
        assert client.get(
            f"/api/shadow/outcomes/{uuid.uuid4()}"
        ).status_code == 404

    def test_metrics_endpoint_has_no_superiority_language(self, client, monkeypatch):
        async def fake_fetch(**kwargs):
            return [{
                "pair": {
                    "experiment_code": "sma150_v2_vs_v3",
                    "experiment_version": "sma150_shadow.v1",
                },
                "control": {
                    "strategy_code": "sma150",
                    "strategy_version": "sma150.v2",
                    "decision_policy_version": "p1",
                    "config_hash": "c1",
                    "verdict": "ENTER",
                },
                "candidate": {
                    "strategy_code": "sma150",
                    "strategy_version": "sma150.v3",
                    "decision_policy_version": "p2",
                    "config_hash": "c2",
                    "verdict": "WATCH",
                },
                "disagreement_category": "v2_enter_v3_watch",
                "outcome": {
                    "calculation_version": "outcome.v1",
                    "outcome_coverage_version": "shadow_pair_outcomes.v1",
                    "forward_frame_version": "shadow_forward_bars.v1",
                    "forward_provider": "massive",
                    "outcome_status": "partial",
                    "returns": {
                        "1D": 2.0, "3D": None, "5D": None,
                        "10D": None, "20D": None,
                    },
                    "max_favorable_excursion": 3.0,
                    "max_adverse_excursion": -1.0,
                    "benchmark_returns": {},
                },
            }]

        monkeypatch.setattr(shadow_router_mod, "fetch_pair_outcomes", fake_fetch)
        resp = client.get("/api/shadow/outcomes/metrics")
        assert resp.status_code == 200
        body = resp.json()
        text = json.dumps(body).lower()
        for forbidden in (
            "winner", "better", "improvement", "regression",
            "promote", "disable", "win_rate",
        ):
            assert forbidden not in text
        assert body["metrics_contract_version"] == (
            "shadow_pair_resolution_metrics.v1"
        )
        assert body["groups"]
        assert "positive_return_rate" in body["groups"][0]["by_window"][0]
