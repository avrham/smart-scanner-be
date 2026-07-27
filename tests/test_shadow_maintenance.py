"""Shadow Outcome Maintenance Environment — mode, gate, access-check, preflight,
execute validation, manifest lock, batch slicing, retry separation, concurrency,
idempotency, provider isolation, auth, and audit/plan regressions.

No Supabase or Massive calls: DB is a fake connection, the provider is never
constructed on the exercised paths, and the calc service is stubbed.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from main import app
from app.config import settings
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod
from app.maintenance_mode import is_maintenance_route_allowed
from app.maintenance_access import evaluate_maintenance_access
from app.maintenance_execute import (
    MAINTENANCE_ADVISORY_LOCK_KEY,
    expected_batch_slice,
    validate_normal,
    validate_retry,
)


# --------------------------------------------------------------------------- #
# Route allowlist (pure).
# --------------------------------------------------------------------------- #
class TestRouteAllowlist:
    def test_execute_is_post_only(self):
        p = "/api/admin/shadow-maintenance/outcomes/execute"
        assert is_maintenance_route_allowed("POST", p) is True
        assert is_maintenance_route_allowed("GET", p) is False

    def test_read_routes_get_only(self):
        for p in ("/version", "/api/admin/shadow-maintenance/access-check",
                  "/api/admin/shadow-maintenance/preflight"):
            assert is_maintenance_route_allowed("GET", p) is True
            assert is_maintenance_route_allowed("POST", p) is False

    def test_broad_routes_blocked(self):
        for p in ("/api/admin/shadow/outcomes/calculate",
                  "/api/admin/shadow-campaigns", "/api/admin/scan/start",
                  "/api/admin/tickers/refresh", "/api/admin/universe/sync",
                  "/docs", "/openapi.json", "/redoc",
                  "/api/admin/shadow-cohort/maturation-plan"):
            assert is_maintenance_route_allowed("POST", p) is False
            assert is_maintenance_route_allowed("GET", p) is False


# --------------------------------------------------------------------------- #
# Access-check verdict (pure).
# --------------------------------------------------------------------------- #
def _probe(rel, *, exists=True, select=True, insert=False, update=False,
           delete=False, truncate=False, trigger=False, rls=True,
           full_row=True, has_ins_pol=False, has_upd_pol=False):
    return {"relation": rel, "exists": exists, "can_select": select,
            "can_insert": insert, "can_update": update, "can_delete": delete,
            "can_truncate": truncate, "can_trigger": trigger, "rls_enabled": rls,
            "applicable_select_policies": (
                [{"policyname": "p", "command": "SELECT", "permissive": "PERMISSIVE",
                  "unconditional_true": True}] if full_row else []),
            "has_insert_policy": has_ins_pol, "has_update_policy": has_upd_pol}


READ = ["public.strategy_shadow_evaluations", "public.strategy_shadow_pairs",
        "public.strategy_shadow_run_pairs", "public.strategy_shadow_runs",
        "public.daily_bars", "public.patterns", "public.pattern_configs"]
WRITE = ["public.strategy_shadow_pair_outcomes", "public.strategy_shadow_outcome_runs"]


def _ready_probes():
    probes = [_probe(r) for r in READ]
    for w in WRITE:
        probes.append(_probe(w, insert=True, update=True,
                             has_ins_pol=True, has_upd_pol=True))
    return probes


def _evaluate(probes, **over):
    kw = dict(database_identity="smart_scanner_outcome_maintainer",
              role_attributes={"rolsuper": False, "rolcreatedb": False,
                               "rolcreaterole": False, "rolreplication": False,
                               "rolbypassrls": False},
              relation_probes=probes,
              expected_role="smart_scanner_outcome_maintainer",
              connection_mode="maintenance_explicit", provider="massive",
              provider_credential_configured=True, scheduler_enabled=False,
              maintenance_only_mode=True, max_batch_size=10, mutation_route_count=1)
    kw.update(over)
    return evaluate_maintenance_access(**kw)


class TestAccessCheckVerdict:
    def test_ready_when_all_good(self):
        r = _evaluate(_ready_probes())
        assert r["ready_for_maintenance_execution"] is True, r["reasons"]
        assert r["reasons"] == []

    def test_forbidden_write_on_read_relation_blocks(self):
        probes = _ready_probes()
        probes[0]["can_insert"] = True  # a read relation must not be writable
        r = _evaluate(probes)
        assert r["ready_for_maintenance_execution"] is False
        assert any("unexpected_write" in x for x in r["reasons"])

    def test_delete_privilege_blocks(self):
        probes = _ready_probes()
        probes[-1]["can_delete"] = True
        r = _evaluate(probes)
        assert any("unexpected_write" in x for x in r["reasons"])

    def test_missing_write_grant_blocks(self):
        probes = _ready_probes()
        probes[-1]["can_update"] = False
        r = _evaluate(probes)
        assert any("missing_write_privilege" in x for x in r["reasons"])

    def test_missing_write_rls_policy_blocks(self):
        probes = _ready_probes()
        probes[-2]["has_insert_policy"] = False  # outcome table (first WRITE)
        r = _evaluate(probes)
        assert any("missing_write_rls_policy" in x for x in r["reasons"])

    def test_identity_mismatch_blocks(self):
        assert any("identity_mismatch" in x for x in
                   _evaluate(_ready_probes(), database_identity="postgres")["reasons"])

    def test_config_reasons(self):
        assert any("scheduler_enabled" in x for x in
                   _evaluate(_ready_probes(), scheduler_enabled=True)["reasons"])
        assert any("provider_not_massive" in x for x in
                   _evaluate(_ready_probes(), provider="fmp")["reasons"])
        assert any("provider_credential_missing" in x for x in
                   _evaluate(_ready_probes(), provider_credential_configured=False)["reasons"])
        assert any("maintenance_only_mode_disabled" in x for x in
                   _evaluate(_ready_probes(), maintenance_only_mode=False)["reasons"])
        assert any("max_batch_size" in x for x in
                   _evaluate(_ready_probes(), max_batch_size=50)["reasons"])


# --------------------------------------------------------------------------- #
# Execute validation (pure) — manifest lock, exact slice, retry.
# --------------------------------------------------------------------------- #
EXP = "wyckoff_v2_vs_baseline"


def _plan(n=25, *, safe=True, manifest_hash="sha256:H", retry_hash="sha256:R"):
    entries = [{"pair_id": f"{i:03d}", "snapshot_date": "2026-06-01", "symbol": "S",
                "strategy_code": "wyckoff_mtf_v2", "experiment_code": EXP,
                "campaign_membership": "verifiable", "eligibility_class": "eligible"}
               for i in range(n)]
    return {"cohort_scope": "campaign", "applied_filters": {"experiment_code": EXP},
            "manifest_hash": manifest_hash, "manifest_total": n,
            "campaign_eligible_unmatured_count": n, "eligible_manifest": entries,
            "excluded_non_campaign_evidence": {"records": []},
            "retry_plan": {"retry_plan_hash": retry_hash, "entries": [
                {"pair_id": "RX", "retryable": True,
                 "current_error_code": "forward_fetch_error",
                 "requires_include_recalc": True, "campaign_membership": "verifiable"}]},
            "planning": {"safe_to_execute": safe, "blocking_reasons": []}}


def _normal_req(plan, batch_index, **over):
    entries = plan["eligible_manifest"]
    slice_ids = expected_batch_slice(entries, batch_index, 10)
    req = {"contract_version": "shadow_maintenance_execute.v1", "mode": "normal",
           "experiment_code": EXP, "cohort_scope": "campaign",
           "manifest_hash": plan["manifest_hash"], "batch_index": batch_index,
           "pair_ids": slice_ids, "limit": len(slice_ids)}
    req.update(over)
    return req


def _vn(plan, req):
    return validate_normal(plan, req, allowed_experiment=EXP,
                           allowed_scope="campaign", max_batch_size=10)


class TestExecuteValidation:
    def test_first_and_last_batch_ok(self):
        plan = _plan(25)
        assert _vn(plan, _normal_req(plan, 0))["ok"] is True
        v = _vn(plan, _normal_req(plan, 2))  # final batch of 5
        assert v["ok"] is True and len(v["validated_pair_ids"]) == 5

    def test_manifest_hash_lock(self):
        plan = _plan(25)
        assert _vn(plan, _normal_req(plan, 0, manifest_hash="sha256:OLD"))["reason"] == "manifest_hash_mismatch"

    def test_wrong_slice_rejected(self):
        plan = _plan(25)
        req = _normal_req(plan, 0)
        req["pair_ids"] = [f"{i:03d}" for i in range(1, 11)]  # shifted slice
        assert _vn(plan, req)["reason"] == "pair_ids_not_expected_batch_slice"

    def test_arbitrary_subset_rejected(self):
        plan = _plan(25)
        req = _normal_req(plan, 0)
        req["pair_ids"] = ["000", "005", "009"]
        req["limit"] = 3
        assert _vn(plan, req)["ok"] is False

    def test_unsafe_plan_blocks(self):
        plan = _plan(25, safe=False)
        assert "plan_not_safe" in _vn(plan, _normal_req(plan, 0))["reason"]

    def test_forbidden_fields_rejected(self):
        plan = _plan(25)
        for f in ("pending", "symbols", "run_id", "campaign_id",
                  "run_in_background", "include_recalc"):
            req = _normal_req(plan, 0)
            req[f] = True
            assert "forbidden_request_fields" in _vn(plan, req)["reason"]

    def test_limit_must_equal_pair_count(self):
        plan = _plan(25)
        req = _normal_req(plan, 0)
        req["limit"] = 9
        assert _vn(plan, req)["reason"] == "limit_must_equal_pair_count"

    def test_retry_ok_sets_include_recalc(self):
        plan = _plan(25)
        req = {"contract_version": "shadow_maintenance_execute.v1", "mode": "retry",
               "experiment_code": EXP, "cohort_scope": "campaign",
               "retry_plan_hash": "sha256:R", "pair_ids": ["RX"], "limit": 1}
        v = validate_retry(plan, req, allowed_experiment=EXP, allowed_scope="campaign")
        assert v["ok"] is True and v["include_recalc"] is True

    def test_retry_hash_lock_and_wrong_pair(self):
        plan = _plan(25)
        base = {"contract_version": "shadow_maintenance_execute.v1", "mode": "retry",
                "experiment_code": EXP, "cohort_scope": "campaign",
                "retry_plan_hash": "sha256:R", "pair_ids": ["RX"], "limit": 1}
        bad_hash = dict(base, retry_plan_hash="sha256:X")
        assert validate_retry(plan, bad_hash, allowed_experiment=EXP,
                              allowed_scope="campaign")["reason"] == "retry_plan_hash_mismatch"
        wrong = dict(base, pair_ids=["000"])
        assert validate_retry(plan, wrong, allowed_experiment=EXP,
                              allowed_scope="campaign")["reason"] == "pair_not_in_retry_plan"


# --------------------------------------------------------------------------- #
# HTTP: maintenance endpoints (fake DB, stubbed plan/service).
# --------------------------------------------------------------------------- #
class _Boom:
    def __call__(self, *a, **k):
        raise AssertionError("maintenance path constructed a provider client")


class _FakeConn:
    def __init__(self, *, lock=True, statuses=None):
        self._lock = lock
        self._statuses = statuses or {}

    async def fetchval(self, sql, *args):
        if "pg_try_advisory_lock" in sql:
            return self._lock
        if "pg_advisory_unlock" in sql:
            return True
        return None

    async def fetch(self, sql, *args):
        if "outcome_status" in sql:
            ids = args[0] if args else []
            return [{"pair_id": p, "outcome_status": self._statuses.get(p),
                     "error_code": None} for p in ids]
        return []


@pytest.fixture
def maint_client(monkeypatch):
    monkeypatch.setattr(admin_mod.settings, "MAINTENANCE_ONLY_MODE", True)
    monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", True)
    app.dependency_overrides[get_worker_token] = lambda: "t"
    app.dependency_overrides[get_db] = lambda: _FakeConn()
    monkeypatch.setattr(admin_mod, "get_market_data_provider", _Boom())
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.pop(get_worker_token, None)
        app.dependency_overrides.pop(get_db, None)


class TestPreflightHttp:
    def test_preflight_reports_plan(self, maint_client, monkeypatch):
        plan = _plan(327)
        # add fields preflight reads
        plan["manifest_hash_version"] = "shadow_maturation_manifest_hash.v2"
        plan["manifest_ordering"] = "snapshot_date_asc_symbol_asc_pair_id_asc"
        plan["experiment_eligible_unmatured_count"] = 329
        plan["excluded_non_campaign_eligible_count"] = 2
        plan["retryable_failure_count"] = 1
        plan["terminal_failure_count"] = 0
        plan["membership"] = {"campaign_membership_unverifiable_count": 0}
        plan["planning"].update({"recommended_batch_size": 10,
                                 "recommended_batch_count": 33,
                                 "full_batch_count": 32, "final_batch_size": 7})

        async def fake_plan(db, *, experiment_code, cohort_scope):
            return plan
        monkeypatch.setattr(admin_mod, "_recompute_maintenance_plan", fake_plan)
        r = maint_client.get("/api/admin/shadow-maintenance/preflight")
        assert r.status_code == 200
        b = r.json()
        assert b["manifest_count"] == 327
        assert b["safe_to_execute"] is True
        assert b["excluded_non_campaign_count"] == 2
        assert b["retryable_failure_count"] == 1
        assert b["execution_available"] is True


class TestExecuteHttp:
    def _patch_plan(self, monkeypatch, plan):
        async def fake_plan(db, *, experiment_code, cohort_scope):
            return plan
        monkeypatch.setattr(admin_mod, "_recompute_maintenance_plan", fake_plan)

    def test_validation_failure_422_before_lock(self, maint_client, monkeypatch):
        plan = _plan(25)
        self._patch_plan(monkeypatch, plan)
        req = _normal_req(plan, 0, manifest_hash="sha256:OLD")
        r = maint_client.post("/api/admin/shadow-maintenance/outcomes/execute", json=req)
        assert r.status_code == 422
        assert r.json()["detail"]["reason"] == "manifest_hash_mismatch"

    def test_concurrency_409_when_lock_held(self, monkeypatch):
        plan = _plan(25)
        monkeypatch.setattr(admin_mod.settings, "MAINTENANCE_ONLY_MODE", True)
        monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", True)
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _FakeConn(lock=False)
        monkeypatch.setattr(admin_mod, "get_market_data_provider", _Boom())

        async def fake_plan(db, *, experiment_code, cohort_scope):
            return plan
        monkeypatch.setattr(admin_mod, "_recompute_maintenance_plan", fake_plan)
        try:
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute",
                json=_normal_req(plan, 0))
            assert r.status_code == 409
            assert r.json()["detail"]["error"] == "maintenance_execution_in_progress"
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)

    def test_already_complete_short_circuits_without_provider(self, monkeypatch):
        plan = _plan(25)
        batch = expected_batch_slice(plan["eligible_manifest"], 0, 10)
        statuses = {p: "complete" for p in batch}
        monkeypatch.setattr(admin_mod.settings, "MAINTENANCE_ONLY_MODE", True)
        monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", True)
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _FakeConn(statuses=statuses)
        monkeypatch.setattr(admin_mod, "get_market_data_provider", _Boom())

        async def fake_plan(db, *, experiment_code, cohort_scope):
            return plan
        monkeypatch.setattr(admin_mod, "_recompute_maintenance_plan", fake_plan)
        try:
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute",
                json=_normal_req(plan, 0))
            assert r.status_code == 200
            assert r.json()["status"] == "already_complete"  # _Boom never raised
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)

    def test_stale_partial_batch_409(self, monkeypatch):
        plan = _plan(25)
        batch = expected_batch_slice(plan["eligible_manifest"], 0, 10)
        statuses = {batch[0]: "complete"}  # one complete, rest pending
        monkeypatch.setattr(admin_mod.settings, "MAINTENANCE_ONLY_MODE", True)
        monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", True)
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _FakeConn(statuses=statuses)
        monkeypatch.setattr(admin_mod, "get_market_data_provider", _Boom())

        async def fake_plan(db, *, experiment_code, cohort_scope):
            return plan
        monkeypatch.setattr(admin_mod, "_recompute_maintenance_plan", fake_plan)
        try:
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute",
                json=_normal_req(plan, 0))
            assert r.status_code == 409
            assert r.json()["detail"]["error"] == "stale_partial_batch"
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)


class TestMaintenanceGuardsAndAuth:
    def test_endpoints_404_when_not_maintenance_mode(self):
        # default (maintenance off): the in-handler guard returns 404.
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _FakeConn()
        try:
            c = TestClient(app, raise_server_exceptions=False)
            assert c.get("/api/admin/shadow-maintenance/access-check").status_code == 404
            assert c.get("/api/admin/shadow-maintenance/preflight").status_code == 404
            assert c.post("/api/admin/shadow-maintenance/outcomes/execute",
                          json={}).status_code == 404
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)

    def test_access_check_wiring(self, maint_client, monkeypatch):
        async def fake_ac(db, **kw):
            fake_ac.kw = kw
            return {"ready_for_maintenance_execution": True, "reasons": [],
                    "database_identity": "smart_scanner_outcome_maintainer"}
        monkeypatch.setattr(
            "app.maintenance_access.run_maintenance_access_check", fake_ac)
        r = maint_client.get("/api/admin/shadow-maintenance/access-check")
        assert r.status_code == 200
        assert r.json()["ready_for_maintenance_execution"] is True
        assert fake_ac.kw["mutation_route_count"] == 1

    def test_startup_guard_mutual_exclusion(self):
        from app.config import Settings
        assert Settings.model_fields["MAINTENANCE_ONLY_MODE"].default is False
