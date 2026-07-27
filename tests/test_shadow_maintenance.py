"""Shadow Outcome Maintenance Progression — v2 protocol.

Covers the stable cohort lock + dynamic remaining manifest + deterministic
next-batch progression, v2 execute validation, replay/idempotency, concurrency
double-check, retry gating, access-check locked-hash checks, route allowlist,
auth, and audit-mode guards. No Supabase/Massive: DB is a fake connection, the
provider is never constructed on the exercised paths, the calc service is never
reached (already_complete short-circuits), and plans are built from synthetic
records via the real builder.
"""

from __future__ import annotations

from datetime import date
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
    EXECUTE_CONTRACT_VERSION, validate_normal, validate_retry,
)
from app.workers.shadow.maturation_plan import build_maturation_plan

EXP = "wyckoff_v2_vs_baseline"
CAL = [date(2026, 6, d) for d in range(10, 30)]
LATEST = date(2026, 6, 29)
AF = {"strategy_code": "wyckoff_mtf_v2", "experiment_code": EXP}


def _blk(cid):
    return {"campaign_id": cid, "experiment_code": EXP, "as_of_date": "2026-06-01"}


def _rec(pid, symbol, snap, *, status=None, blocks=None):
    b = [_blk("camp-1")] if blocks is None else blocks
    return {"evaluation_id": f"e-{pid}", "pair_id": pid, "run_id": "run-A",
            "symbol": symbol, "snapshot_date": snap, "strategy_code": "wyckoff_mtf_v2",
            "strategy_version": "wyckoff_mtf.v2", "experiment_code": EXP,
            "experiment_version": "wyckoff_v2_shadow.v2",
            "decision_policy_version": "pol.v1", "config_hash": "cfg1",
            "outcome_status": status, "has_outcome": status is not None,
            "campaign_ids": [x["campaign_id"] for x in b], "campaign_blocks": b,
            "created_at": "2026-06-16T00:00:00+00:00"}


def _outcome(pid, symbol, snap, status, error=None):
    return {"pair": {"pair_id": pid, "symbol": symbol, "snapshot_date": snap},
            "outcome": {"outcome_status": status, "error_code": error, "returns": {}}}


def _build_plan(n_eligible=25, n_complete=3, *, batch_size=10, retryable=True):
    records = [_rec(f"e{i:03d}", f"S{i:03d}", "2026-06-12") for i in range(n_eligible)]
    records += [_rec(f"c{i:03d}", f"C{i:03d}", "2026-06-12", status="complete")
                for i in range(n_complete)]
    rows = [_outcome(f"c{i:03d}", f"C{i:03d}", "2026-06-12", "complete") for i in range(n_complete)]
    if retryable:
        records.append(_rec("rX", "AAPL", "2026-06-10", status="error"))
        rows.append(_outcome("rX", "AAPL", "2026-06-10", "error", error="forward_fetch_error"))
    return build_maturation_plan(records, rows, cohort_scope="campaign",
                                 applied_filters=AF, session_dates=CAL,
                                 latest_completed_session=LATEST, batch_size=batch_size)


def _v2_normal_req(plan, **over):
    nb = plan["next_batch"]
    req = {"contract_version": EXECUTE_CONTRACT_VERSION, "mode": "normal",
           "experiment_code": EXP, "cohort_scope": "campaign",
           "cohort_lock_hash": plan["cohort_lock_hash"],
           "remaining_manifest_hash": plan["remaining_manifest_hash"],
           "next_batch_hash": nb["next_batch_hash"], "pair_ids": nb["pair_ids"],
           "limit": nb["pair_count"]}
    req.update(over)
    return req


def _vn(plan, req, locked=None):
    return validate_normal(plan, req, allowed_experiment=EXP, allowed_scope="campaign",
                           max_batch_size=10, locked_cohort_hash=locked or plan["cohort_lock_hash"])


# --------------------------------------------------------------------------- #
class TestRouteAllowlist:
    def test_execute_post_only_and_broad_blocked(self):
        assert is_maintenance_route_allowed("POST", "/api/admin/shadow-maintenance/outcomes/execute")
        assert not is_maintenance_route_allowed("GET", "/api/admin/shadow-maintenance/outcomes/execute")
        for p in ("/api/admin/shadow/outcomes/calculate", "/api/admin/shadow-campaigns",
                  "/docs", "/openapi.json", "/api/admin/shadow-cohort/maturation-plan"):
            assert not is_maintenance_route_allowed("POST", p)
            assert not is_maintenance_route_allowed("GET", p)


READ = ["public.strategy_shadow_evaluations", "public.strategy_shadow_pairs",
        "public.strategy_shadow_run_pairs", "public.strategy_shadow_runs",
        "public.daily_bars", "public.patterns", "public.pattern_configs"]
WRITE = ["public.strategy_shadow_pair_outcomes", "public.strategy_shadow_outcome_runs"]


def _probe(rel, **o):
    d = {"relation": rel, "exists": True, "can_select": True, "can_insert": False,
         "can_update": False, "can_delete": False, "can_truncate": False,
         "can_trigger": False, "rls_enabled": True,
         "applicable_select_policies": [{"policyname": "p", "command": "SELECT",
                                         "permissive": "PERMISSIVE", "unconditional_true": True}],
         "has_insert_policy": False, "has_update_policy": False}
    d.update(o)
    return d


def _ready_probes():
    return ([_probe(r) for r in READ]
            + [_probe(w, can_insert=True, can_update=True, has_insert_policy=True,
                      has_update_policy=True) for w in WRITE])


def _evaluate(**over):
    kw = dict(database_identity="smart_scanner_outcome_maintainer",
              role_attributes={a: False for a in ("rolsuper", "rolcreatedb",
                               "rolcreaterole", "rolreplication", "rolbypassrls")},
              relation_probes=_ready_probes(),
              expected_role="smart_scanner_outcome_maintainer",
              connection_mode="maintenance_explicit", provider="massive",
              provider_credential_configured=True, scheduler_enabled=False,
              maintenance_only_mode=True, max_batch_size=10, mutation_route_count=1,
              locked_cohort_hash="sha256:LOCK", current_cohort_lock_hash="sha256:LOCK",
              cohort_pair_count=502)
    kw.update(over)
    return evaluate_maintenance_access(**kw)


class TestAccessCheckLockedHash:
    def test_ready_when_lock_matches(self):
        r = _evaluate()
        assert r["ready_for_maintenance_execution"] is True, r["reasons"]
        assert r["locked_cohort_hash_matches"] is True
        assert r["cohort_pair_count"] == 502

    def test_not_configured_blocks(self):
        r = _evaluate(locked_cohort_hash=None)
        assert r["ready_for_maintenance_execution"] is False
        assert "locked_cohort_hash_not_configured" in r["reasons"]

    def test_drift_blocks(self):
        r = _evaluate(current_cohort_lock_hash="sha256:OTHER")
        assert r["ready_for_maintenance_execution"] is False
        assert "cohort_lock_drift" in r["reasons"]

    def test_unverifiable_blocks(self):
        r = _evaluate(current_cohort_lock_hash=None)
        assert "cohort_lock_unverifiable" in r["reasons"]


class TestRegressionFixedHashFlaw:
    """Step 2: prove the ORIGINAL fixed-hash / fixed-batch-index protocol cannot
    progress past the first successful batch, and that the v2 protocol can."""

    def _mature_first_batch(self, plan, n_eligible=12):
        first = set(plan["next_batch"]["pair_ids"])
        recs = [_rec(f"e{i:03d}", f"S{i:03d}", "2026-06-12",
                     status=("complete" if f"e{i:03d}" in first else None))
                for i in range(n_eligible)]
        recs.append(_rec("rX", "AAPL", "2026-06-10", status="error"))
        rows = [_outcome(p, "S", "2026-06-12", "complete") for p in first]
        rows.append(_outcome("rX", "AAPL", "2026-06-10", "error", error="forward_fetch_error"))
        return build_maturation_plan(recs, rows, cohort_scope="campaign", applied_filters=AF,
                                     session_dates=CAL, latest_completed_session=LATEST, batch_size=10)

    def test_fixed_remaining_hash_cannot_progress_but_v2_can(self):
        plan1 = _build_plan(12, 0)          # H1 = remaining hash of 12 eligible
        H1 = plan1["remaining_manifest_hash"]
        assert plan1["remaining_pair_count"] == 12
        # execute batch 0 (10 pairs) -> those become complete
        plan2 = self._mature_first_batch(plan1, 12)
        assert plan2["remaining_pair_count"] == 2          # 12 - 10
        H2 = plan2["remaining_manifest_hash"]
        assert H2 != H1                                    # the dynamic hash changed
        # OLD protocol: the operator still holds H1 for "batch 1" -> REJECTED
        stale = _v2_normal_req(plan1)                      # carries H1 + old slice
        v_stale = _vn(plan2, stale, locked=plan2["cohort_lock_hash"])
        assert v_stale["ok"] is False
        assert v_stale["reason"] in ("remaining_manifest_hash_mismatch",
                                     "next_batch_hash_mismatch")
        # NEW protocol: a fresh preflight yields the current next batch -> OK
        fresh = _v2_normal_req(plan2)
        v_fresh = _vn(plan2, fresh, locked=plan2["cohort_lock_hash"])
        assert v_fresh["ok"] is True
        # and the stable cohort lock never moved across the batch
        assert plan2["cohort_lock_hash"] == plan1["cohort_lock_hash"]


class TestProgressionValidation:
    def test_next_batch_ok(self):
        plan = _build_plan(25)
        v = _vn(plan, _v2_normal_req(plan))
        assert v["ok"] is True and len(v["validated_pair_ids"]) == 10

    def test_cohort_lock_drift_when_locked_differs(self):
        plan = _build_plan(25)
        v = _vn(plan, _v2_normal_req(plan), locked="sha256:STALE")
        assert v["reason"] == "cohort_lock_drift"

    def test_remaining_hash_mismatch(self):
        plan = _build_plan(25)
        v = _vn(plan, _v2_normal_req(plan, remaining_manifest_hash="sha256:OLD"))
        assert v["reason"] == "remaining_manifest_hash_mismatch"

    def test_next_batch_hash_mismatch(self):
        plan = _build_plan(25)
        v = _vn(plan, _v2_normal_req(plan, next_batch_hash="sha256:OLD"))
        assert v["reason"] == "next_batch_hash_mismatch"

    def test_wrong_pair_ids_rejected(self):
        plan = _build_plan(25)
        req = _v2_normal_req(plan)
        req["pair_ids"] = list(reversed(req["pair_ids"]))
        v = _vn(plan, req)
        assert v["reason"] == "next_batch_hash_mismatch" or v["reason"] == "pair_ids_not_expected_next_batch"

    def test_v1_batch_index_rejected(self):
        plan = _build_plan(25)
        req = _v2_normal_req(plan)
        req["batch_index"] = 0  # deprecated v1 field
        assert "forbidden_request_fields" in _vn(plan, req)["reason"]

    def test_forbidden_fields_rejected(self):
        plan = _build_plan(25)
        for f in ("pending", "symbols", "run_id", "include_recalc", "manifest_hash"):
            req = _v2_normal_req(plan)
            req[f] = True
            assert "forbidden_request_fields" in _vn(plan, req)["reason"]

    def test_progression_shrinks_remaining_lock_stable(self):
        plan = _build_plan(25)
        assert plan["cohort_pair_count"] == 29  # 25 eligible + 3 complete + 1 retryable
        assert plan["remaining_pair_count"] == 25
        assert plan["normal_execution_complete"] is False
        first = set(plan["next_batch"]["pair_ids"])
        # mature the first batch
        recs = [_rec(f"e{i:03d}", f"S{i:03d}", "2026-06-12",
                     status=("complete" if f"e{i:03d}" in first else None))
                for i in range(25)]
        recs += [_rec(f"c{i:03d}", f"C{i:03d}", "2026-06-12", status="complete") for i in range(3)]
        recs.append(_rec("rX", "AAPL", "2026-06-10", status="error"))
        rows = [_outcome(f"c{i:03d}", f"C{i:03d}", "2026-06-12", "complete") for i in range(3)]
        rows += [_outcome(pid, "S", "2026-06-12", "complete") for pid in first]
        rows.append(_outcome("rX", "AAPL", "2026-06-10", "error", error="forward_fetch_error"))
        plan2 = build_maturation_plan(recs, rows, cohort_scope="campaign", applied_filters=AF,
                                      session_dates=CAL, latest_completed_session=LATEST, batch_size=10)
        assert plan2["cohort_lock_hash"] == plan["cohort_lock_hash"]     # STABLE
        assert plan2["cohort_pair_count"] == plan["cohort_pair_count"]
        assert plan2["remaining_pair_count"] == 15
        assert plan2["remaining_manifest_hash"] != plan["remaining_manifest_hash"]
        assert plan2["next_batch"]["next_batch_hash"] != plan["next_batch"]["next_batch_hash"]

    def test_normal_complete_and_next_unavailable(self):
        plan = _build_plan(0, 3)  # no eligible left
        assert plan["remaining_pair_count"] == 0
        assert plan["normal_execution_complete"] is True
        assert plan["next_batch"]["available"] is False

    def test_retry_gated_until_normal_complete(self):
        plan = _build_plan(25)  # normal not complete
        req = {"contract_version": EXECUTE_CONTRACT_VERSION, "mode": "retry",
               "experiment_code": EXP, "cohort_scope": "campaign",
               "cohort_lock_hash": plan["cohort_lock_hash"],
               "retry_plan_hash": plan["retry_plan"]["retry_plan_hash"],
               "pair_ids": ["rX"], "limit": 1}
        v = validate_retry(plan, req, allowed_experiment=EXP, allowed_scope="campaign",
                           locked_cohort_hash=plan["cohort_lock_hash"])
        assert v["reason"] == "normal_execution_not_complete"

    def test_retry_ok_after_normal_complete(self):
        plan = _build_plan(0, 3)  # normal complete, retryable present
        req = {"contract_version": EXECUTE_CONTRACT_VERSION, "mode": "retry",
               "experiment_code": EXP, "cohort_scope": "campaign",
               "cohort_lock_hash": plan["cohort_lock_hash"],
               "retry_plan_hash": plan["retry_plan"]["retry_plan_hash"],
               "pair_ids": ["rX"], "limit": 1}
        v = validate_retry(plan, req, allowed_experiment=EXP, allowed_scope="campaign",
                           locked_cohort_hash=plan["cohort_lock_hash"])
        assert v["ok"] is True and v["include_recalc"] is True


# --------------------------------------------------------------------------- #
# HTTP.
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
        return None

    async def fetch(self, sql, *args):
        if "outcome_status" in sql:
            ids = args[0] if args else []
            return [{"pair_id": p, "outcome_status": self._statuses.get(p),
                     "error_code": None} for p in ids]
        return []


def _maint_on(monkeypatch, plan, *, locked=None, conn=None):
    monkeypatch.setattr(admin_mod.settings, "MAINTENANCE_ONLY_MODE", True)
    monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", True)
    monkeypatch.setattr(admin_mod.settings, "MAINTENANCE_LOCKED_COHORT_HASH",
                        locked if locked is not None else plan["cohort_lock_hash"])
    app.dependency_overrides[get_worker_token] = lambda: "t"
    app.dependency_overrides[get_db] = lambda: (conn or _FakeConn())
    monkeypatch.setattr(admin_mod, "get_market_data_provider", _Boom())

    async def fake_plan(db, *, experiment_code, cohort_scope, batch_size=None):
        return plan
    monkeypatch.setattr(admin_mod, "_recompute_maintenance_plan", fake_plan)


def _teardown():
    app.dependency_overrides.pop(get_worker_token, None)
    app.dependency_overrides.pop(get_db, None)


class TestPreflightHttp:
    def test_preflight_exposes_stable_and_dynamic(self, monkeypatch):
        plan = _build_plan(25)
        _maint_on(monkeypatch, plan)
        try:
            b = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-maintenance/preflight").json()
            assert b["cohort_lock_hash"] == plan["cohort_lock_hash"]
            assert b["cohort_pair_count"] == plan["cohort_pair_count"]
            assert b["remaining_manifest_hash"] == plan["remaining_manifest_hash"]
            assert b["remaining_pair_count"] == 25
            assert b["locked_cohort_hash_matches"] is True
            assert b["next_batch"]["available"] is True
            assert b["normal_execution_complete"] is False
            assert b["execution_available"] is True
        finally:
            _teardown()


class TestExecuteHttp:
    def test_valid_v1_style_rejected(self, monkeypatch):
        plan = _build_plan(25)
        _maint_on(monkeypatch, plan)
        try:
            req = _v2_normal_req(plan)
            req["contract_version"] = "shadow_maintenance_execute.v1"
            req["batch_index"] = 0
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute", json=req)
            assert r.status_code == 422
        finally:
            _teardown()

    def test_arbitrary_pairs_rejected(self, monkeypatch):
        plan = _build_plan(25)
        _maint_on(monkeypatch, plan)
        try:
            req = _v2_normal_req(plan)
            req["pair_ids"] = ["deadbeef"]
            req["limit"] = 1
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute", json=req)
            assert r.status_code == 422
        finally:
            _teardown()

    def test_concurrency_409(self, monkeypatch):
        plan = _build_plan(25)
        _maint_on(monkeypatch, plan, conn=_FakeConn(lock=False))
        try:
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute", json=_v2_normal_req(plan))
            assert r.status_code == 409
            assert r.json()["detail"]["error"] == "maintenance_execution_in_progress"
        finally:
            _teardown()

    def test_already_applied_replay_without_provider(self, monkeypatch):
        plan = _build_plan(25)
        batch = plan["next_batch"]["pair_ids"]
        # replay: remaining moved on (client sends stale next-batch), but its
        # pairs are now all complete and cohort lock still matches.
        moved = _build_plan(25)
        # force remaining hash to look "moved" by marking batch complete in the plan the server sees
        recs = [_rec(f"e{i:03d}", f"S{i:03d}", "2026-06-12",
                     status=("complete" if f"e{i:03d}" in set(batch) else None)) for i in range(25)]
        recs += [_rec(f"c{i:03d}", f"C{i:03d}", "2026-06-12", status="complete") for i in range(3)]
        recs.append(_rec("rX", "AAPL", "2026-06-10", status="error"))
        rows = [_outcome(f"c{i:03d}", f"C{i:03d}", "2026-06-12", "complete") for i in range(3)]
        rows += [_outcome(p, "S", "2026-06-12", "complete") for p in batch]
        rows.append(_outcome("rX", "AAPL", "2026-06-10", "error", error="forward_fetch_error"))
        server_plan = build_maturation_plan(recs, rows, cohort_scope="campaign", applied_filters=AF,
                                            session_dates=CAL, latest_completed_session=LATEST, batch_size=10)
        conn = _FakeConn(statuses={p: "complete" for p in batch})
        _maint_on(monkeypatch, server_plan, locked=server_plan["cohort_lock_hash"], conn=conn)
        try:
            req = _v2_normal_req(plan)  # stale (old remaining/next-batch hashes)
            req["cohort_lock_hash"] = server_plan["cohort_lock_hash"]  # lock unchanged
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute", json=req)
            assert r.status_code == 200, r.json()
            assert r.json()["status"] == "already_applied"  # _Boom never raised
        finally:
            _teardown()


class TestGuardsAndAuth:
    def test_endpoints_404_when_not_maintenance_mode(self):
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _FakeConn()
        try:
            c = TestClient(app, raise_server_exceptions=False)
            assert c.get("/api/admin/shadow-maintenance/access-check").status_code == 404
            assert c.get("/api/admin/shadow-maintenance/preflight").status_code == 404
            assert c.post("/api/admin/shadow-maintenance/outcomes/execute", json={}).status_code == 404
        finally:
            _teardown()

    def test_access_check_wiring_passes_locked_hash(self, monkeypatch):
        plan = _build_plan(25)
        _maint_on(monkeypatch, plan)

        async def fake_ac(db, **kw):
            fake_ac.kw = kw
            return {"ready_for_maintenance_execution": True, "reasons": []}
        monkeypatch.setattr("app.maintenance_access.run_maintenance_access_check", fake_ac)
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-maintenance/access-check")
            assert r.status_code == 200
            assert fake_ac.kw["locked_cohort_hash"] == plan["cohort_lock_hash"]
            assert fake_ac.kw["current_cohort_lock_hash"] == plan["cohort_lock_hash"]
        finally:
            _teardown()

    def test_config_default_false(self):
        from app.config import Settings
        assert Settings.model_fields["MAINTENANCE_ONLY_MODE"].default is False
        assert Settings.model_fields["MAINTENANCE_LOCKED_COHORT_HASH"].default == ""
