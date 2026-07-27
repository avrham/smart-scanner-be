"""Shadow pair lineage audit — pure classifier, assembly, and HTTP boundary.

Proves the lineage classifier is deterministic over the closed vocabulary, that
build_pair_lineage joins pair/run/evaluation/run-pairs/telemetry correctly and
bounds/redacts raw telemetry, and that the read-only endpoint requires a worker
token, caps at 20 explicit pair IDs, rejects empty/malformed input, returns a
bounded not-found for unknown pairs, is permitted in audit-only mode, is
fail-closed when access is not ready, and constructs no provider.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from main import app
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod
from app.workers.shadow.pair_lineage import (
    CLASS_INCORRECTLY_TAGGED,
    CLASS_LEGACY_PRE_CAMPAIGN,
    CLASS_LEGIT_MISSING_TELEMETRY,
    CLASS_MANUAL_NON_CAMPAIGN,
    CLASS_ORPHAN_INCONSISTENT,
    CLASS_UNVERIFIABLE,
    MAX_LINEAGE_PAIR_IDS,
    PAIR_LINEAGE_CONTRACT_VERSION,
    RES_BACKFILL,
    RES_EXCLUDE_RETAIN,
    RES_RETAIN_LEGACY,
    build_pair_lineage,
    classify_pair_lineage,
)


def _run(rid, *, status="completed", exp="wyckoff_v2_vs_baseline",
         campaign=None, created="2026-07-24T10:00:00+00:00"):
    return {
        "run_id": rid, "run_status": status, "run_experiment_code": exp,
        "run_created_at": created,
        "campaign_telemetry_present": bool(campaign),
        "campaign_id": campaign,
    }


def _view(runs, *, evals=1, exp="wyckoff_v2_vs_baseline", exists=True):
    return {
        "pair_id": "p", "pair_exists": exists, "experiment_code": exp,
        "runs": runs, "evaluations": [{"i": i} for i in range(evals)],
    }


class TestClassifierDeterministic:
    def test_manual_non_campaign(self):
        v = classify_pair_lineage(_view([_run("r1")]))
        assert v["classification"] == CLASS_MANUAL_NON_CAMPAIGN
        assert v["recommended_resolution"] == RES_EXCLUDE_RETAIN
        assert v["deterministic_campaign_assignable"] is False

    def test_legacy_pre_campaign(self):
        v = classify_pair_lineage(_view([_run("r1", created="2026-07-23T17:00:00+00:00")]))
        assert v["classification"] == CLASS_LEGACY_PRE_CAMPAIGN
        assert v["recommended_resolution"] == RES_RETAIN_LEGACY

    def test_legit_missing_telemetry_with_single_campaign(self):
        v = classify_pair_lineage(_view([_run("r1"), _run("r2", campaign="camp-1")]))
        assert v["classification"] == CLASS_LEGIT_MISSING_TELEMETRY
        assert v["recommended_resolution"] == RES_BACKFILL
        assert v["deterministic_campaign_assignable"] is True
        assert v["assigned_campaign_id"] == "camp-1"
        assert v["backfill_uniqueness_guard"]

    def test_multiple_campaigns_is_unverifiable(self):
        v = classify_pair_lineage(_view([
            _run("r1", campaign="camp-1"), _run("r2", campaign="camp-2")]))
        assert v["classification"] == CLASS_UNVERIFIABLE

    def test_incorrectly_tagged_on_experiment_conflict(self):
        v = classify_pair_lineage(_view([_run("r1", exp="other_exp")]))
        assert v["classification"] == CLASS_INCORRECTLY_TAGGED

    def test_orphan_no_run(self):
        assert classify_pair_lineage(_view([]))["classification"] == CLASS_ORPHAN_INCONSISTENT

    def test_orphan_no_eval(self):
        v = classify_pair_lineage(_view([_run("r1")], evals=0))
        assert v["classification"] == CLASS_ORPHAN_INCONSISTENT

    def test_unverifiable_when_not_completed(self):
        v = classify_pair_lineage(_view([_run("r1", status="failed")]))
        assert v["classification"] == CLASS_UNVERIFIABLE

    def test_missing_pair_is_orphan(self):
        v = classify_pair_lineage(_view([], exists=False))
        assert v["classification"] == CLASS_ORPHAN_INCONSISTENT

    def test_determinism_repeatable(self):
        a = classify_pair_lineage(_view([_run("r1")]))
        b = classify_pair_lineage(_view([_run("r1")]))
        assert a == b


# --------------------------------------------------------------------------- #
# build_pair_lineage assembly (joins + telemetry redaction).
# --------------------------------------------------------------------------- #
def _raw(pair_id, *, run_id, exp="wyckoff_v2_vs_baseline", telemetry=None,
         status="completed", created="2026-07-24T10:00:00+00:00",
         sibling_symbols=("AAPL", "MSFT")):
    pair_rows = [{
        "id": pair_id, "symbol": "AAPL", "snapshot_date": "2026-07-23",
        "created_at": created, "experiment_code": exp,
        "experiment_version": "wyckoff_v2_shadow.v2",
        "origin_run_id": run_id, "provider": "massive",
    }]
    evals = [{
        "id": f"eval-{pair_id}", "pair_id": pair_id, "arm_code": "candidate_wyckoff_v2",
        "strategy_code": "wyckoff_mtf_v2", "strategy_version": "wyckoff_mtf.v2",
        "decision_policy_version": "wyckoff_mtf.policy.v1", "config_hash": "cfg1",
        "verdict": "AVOID", "created_at": created,
    }]
    links = [{"run_id": run_id, "pair_id": pair_id, "created_new_pair": True,
              "linked_at": created}]
    runs = {run_id: {
        "id": run_id, "experiment_code": exp, "experiment_version": "wyckoff_v2_shadow.v2",
        "status": status, "provider": "massive",
        "requested_symbols": list(sibling_symbols), "requested_limit": 25,
        "started_at": created, "finished_at": created, "created_at": created,
        "error_code": None, "telemetry": telemetry,
    }}
    siblings = [{"run_id": run_id, "pair_id": f"sib-{s}", "symbol": s,
                 "snapshot_date": "2026-07-23", "eval_count": 2}
                for s in sibling_symbols]
    return {"pair_rows": pair_rows, "evals": evals, "links": links,
            "outcomes": {pair_id: None}, "runs": runs, "siblings": siblings}


class TestAssembly:
    def test_joins_and_manual_classification(self):
        out = build_pair_lineage(_raw("p1", run_id="r1"), ["p1"])
        assert out["contract_version"] == PAIR_LINEAGE_CONTRACT_VERSION
        assert out["found_pair_count"] == 1
        p = out["pairs"][0]
        assert p["found"] is True
        assert p["classification"] == CLASS_MANUAL_NON_CAMPAIGN
        run = p["lineage"]["runs"][0]
        assert run["run_pair_count"] == 2
        assert run["run_sibling_symbol_count"] == 2
        assert run["campaign_telemetry_present"] is False
        assert p["lineage"]["evaluations"][0]["arm_code"] == "candidate_wyckoff_v2"

    def test_telemetry_is_bounded_and_redacted(self):
        # A giant unrelated blob must NOT be echoed; only keys + campaign block.
        telem = {"campaign": {"campaign_id": "camp-9", "chunk_index": 0,
                              "chunk_count": 1, "as_of_date": "2026-07-23"},
                 "huge_unrelated": {"x": "y" * 100000},
                 "pair_count": 25}
        out = build_pair_lineage(
            _raw("p1", run_id="r1", telemetry=telem), ["p1"])
        run = out["pairs"][0]["lineage"]["runs"][0]
        assert run["campaign_telemetry_present"] is True
        assert run["campaign_id"] == "camp-9"
        assert set(run["telemetry_keys"]) == {"campaign", "huge_unrelated", "pair_count"}
        # the giant blob value is never present anywhere in the serialized run
        import json
        assert "y" * 1000 not in json.dumps(run)

    def test_campaign_linked_pair_is_backfillable(self):
        telem = {"campaign": {"campaign_id": "camp-1", "chunk_index": 2,
                              "chunk_count": 5, "as_of_date": "2026-07-23"}}
        out = build_pair_lineage(_raw("p1", run_id="r1", telemetry=telem), ["p1"])
        p = out["pairs"][0]
        assert p["classification"] == CLASS_LEGIT_MISSING_TELEMETRY
        assert p["assigned_campaign_id"] == "camp-1"

    def test_unknown_pair_bounded_not_found(self):
        out = build_pair_lineage(
            {"pair_rows": [], "evals": [], "links": [], "outcomes": {},
             "runs": {}, "siblings": []},
            ["ffffffff-ffff-ffff-ffff-ffffffffffff"])
        assert out["found_pair_count"] == 0
        p = out["pairs"][0]
        assert p["found"] is False and p["lineage"] is None
        assert p["classification"] == CLASS_ORPHAN_INCONSISTENT


# --------------------------------------------------------------------------- #
# HTTP boundary.
# --------------------------------------------------------------------------- #
class _Boom:
    def __call__(self, *a, **k):
        raise AssertionError("lineage endpoint constructed a provider client")


class _FakeConn:
    async def fetch(self, *a, **k):
        return []


AAPL = "62438e8b-9a38-4b70-8c57-d0219c41771c"
MSFT = "fb01b6d2-ca2b-48f4-8d90-ac32cf65e587"


def _patch_read(monkeypatch, raw):
    async def fake_read(conn, pair_ids):
        fake_read.pair_ids = pair_ids
        return raw
    monkeypatch.setattr(
        "app.workers.shadow.pair_lineage.read_pair_lineage", fake_read)
    monkeypatch.setattr(admin_mod, "get_market_data_provider", _Boom())
    return fake_read


@pytest.fixture
def client():
    app.dependency_overrides[get_worker_token] = lambda: "test-token"
    app.dependency_overrides[get_db] = lambda: _FakeConn()
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.pop(get_worker_token, None)
        app.dependency_overrides.pop(get_db, None)


class TestHttpBoundary:
    def test_empty_request_422(self, client, monkeypatch):
        _patch_read(monkeypatch, _raw("x", run_id="r1"))
        assert client.get("/api/admin/shadow-cohort/pair-lineage").status_code == 422

    def test_malformed_uuid_422(self, client, monkeypatch):
        _patch_read(monkeypatch, _raw("x", run_id="r1"))
        r = client.get("/api/admin/shadow-cohort/pair-lineage",
                       params={"pair_ids": "not-a-uuid"})
        assert r.status_code == 422

    def test_too_many_ids_422(self, client, monkeypatch):
        _patch_read(monkeypatch, _raw("x", run_id="r1"))
        ids = ",".join(
            f"{i:08d}-0000-0000-0000-000000000000" for i in range(MAX_LINEAGE_PAIR_IDS + 1))
        r = client.get("/api/admin/shadow-cohort/pair-lineage", params={"pair_ids": ids})
        assert r.status_code == 422

    def test_unknown_pair_returns_bounded_not_found(self, client, monkeypatch):
        _patch_read(monkeypatch, {"pair_rows": [], "evals": [], "links": [],
                                  "outcomes": {}, "runs": {}, "siblings": []})
        r = client.get("/api/admin/shadow-cohort/pair-lineage",
                       params={"pair_ids": AAPL})
        assert r.status_code == 200
        body = r.json()
        assert body["found_pair_count"] == 0
        assert body["pairs"][0]["found"] is False

    def test_two_pairs_ok_no_provider(self, client, monkeypatch):
        raw = _raw(AAPL, run_id="r1")
        # add MSFT pair to raw
        raw2 = _raw(MSFT, run_id="r2")
        merged = {
            "pair_rows": raw["pair_rows"] + raw2["pair_rows"],
            "evals": raw["evals"] + raw2["evals"],
            "links": raw["links"] + raw2["links"],
            "outcomes": {**raw["outcomes"], **raw2["outcomes"]},
            "runs": {**raw["runs"], **raw2["runs"]},
            "siblings": raw["siblings"] + raw2["siblings"],
        }
        _patch_read(monkeypatch, merged)
        r = client.get("/api/admin/shadow-cohort/pair-lineage",
                       params={"pair_ids": f"{AAPL},{MSFT}"})
        assert r.status_code == 200
        assert r.json()["found_pair_count"] == 2


class TestAuthAndAuditMode:
    @pytest.fixture
    def auth_client(self, monkeypatch):
        from app.config import settings
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "unit-test-token")
        app.dependency_overrides[get_db] = lambda: _FakeConn()
        _patch_read(monkeypatch, {"pair_rows": [], "evals": [], "links": [],
                                  "outcomes": {}, "runs": {}, "siblings": []})
        try:
            yield TestClient(app, raise_server_exceptions=False)
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_missing_token_rejected(self, auth_client):
        assert auth_client.get("/api/admin/shadow-cohort/pair-lineage",
                               params={"pair_ids": AAPL}).status_code == 401

    def test_valid_token_reaches(self, auth_client):
        assert auth_client.get("/api/admin/shadow-cohort/pair-lineage",
                               params={"pair_ids": AAPL},
                               headers={"X-Worker-Token": "unit-test-token"}
                               ).status_code == 200

    def test_audit_only_allowlisted(self):
        from app.audit_mode import is_audit_route_allowed
        assert is_audit_route_allowed("GET", "/api/admin/shadow-cohort/pair-lineage") is True
        assert is_audit_route_allowed("POST", "/api/admin/shadow-cohort/pair-lineage") is False

    def test_audit_only_fail_closed_when_not_ready(self, monkeypatch):
        from app.config import settings
        monkeypatch.setattr(admin_mod.settings, "AUDIT_ONLY_MODE", True)
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _FakeConn()
        _patch_read(monkeypatch, {"pair_rows": [], "evals": [], "links": [],
                                  "outcomes": {}, "runs": {}, "siblings": []})

        async def not_ready(db):
            return {"ready_for_closeout_audit": False,
                    "reasons": ["rls_select_policy_missing:[...]"],
                    "database_connection_mode": "audit_explicit"}
        monkeypatch.setattr(admin_mod, "_run_configured_access_check", not_ready)
        try:
            client = TestClient(app, raise_server_exceptions=False)
            r = client.get("/api/admin/shadow-cohort/pair-lineage",
                           params={"pair_ids": AAPL})
            assert r.status_code == 409
            assert r.json()["detail"]["error"] == "pair_lineage_not_ready"
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)
