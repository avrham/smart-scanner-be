"""Server-enforced provider cooldown between maintenance batches.

Reproduces the observed live failure (batch A consumes the Massive Basic minute
budget; batch B four seconds later is throttled to three retryable errors) and
proves the guard rejects B BEFORE provider construction, that the cooldown is
derived from a persisted run (survives restart), that a genuine idempotent
replay is never blocked, and that the pure computation is deterministic under a
fixed clock. No Supabase / Massive: DB is a fake connection and any provider
construction on a blocked path raises.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from main import app
from app.config import settings
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod
from app.maintenance_cooldown import (
    COOLDOWN_BLOCKING_REASON,
    COOLDOWN_UNDER_LOCK_REASON,
    DEFAULT_MIN_BATCH_INTERVAL_SECONDS,
    MASSIVE_BASIC_FLOOR_SECONDS,
    MAX_MIN_BATCH_INTERVAL_SECONDS,
    compute_cooldown,
    reference_timestamp,
    resolve_min_interval_seconds,
    retry_after_seconds,
)

# Reuse the established maintenance test harness (plan builder, request builder,
# maintenance-mode wiring, provider _Boom guard, fake connection).
from test_shadow_maintenance import (  # noqa: E402
    EXP,
    EXECUTE_CONTRACT_VERSION,
    _FakeConn,
    _build_plan,
    _maint_on,
    _teardown,
    _v2_normal_req,
)


T0 = datetime(2026, 7, 28, 12, 0, 0, tzinfo=timezone.utc)


def _run(status="completed", *, finished=None, started=None, created=None,
         updated=None, run_id="00000000-0000-0000-0000-0000000000aa"):
    return {"id": run_id, "status": status, "finished_at": finished,
            "started_at": started, "created_at": created, "updated_at": updated}


# --------------------------------------------------------------------------- #
# Pure computation (fixed clock).
# --------------------------------------------------------------------------- #
class TestComputeCooldownPure:
    def test_no_previous_run_allows(self):
        c = compute_cooldown(None, min_interval_seconds=75, now=T0)
        assert c["cooldown_required"] is False
        assert c["execution_allowed_by_cooldown"] is True
        assert c["cooldown_remaining_seconds"] == 0
        assert c["last_execution_run_id"] is None
        assert c["next_execution_not_before"] is None

    def test_completed_run_inside_window_blocks(self):
        run = _run("completed", finished=T0 - timedelta(seconds=4))
        c = compute_cooldown(run, min_interval_seconds=75, now=T0)
        assert c["cooldown_required"] is True
        assert c["execution_allowed_by_cooldown"] is False
        assert c["cooldown_remaining_seconds"] == 71
        assert c["last_execution_status"] == "completed"
        assert c["last_execution_timestamp_source"] == "finished_at"

    def test_error_dominated_failed_run_still_blocks(self):
        # A 429-dominated batch finalizes 'failed' (or 'completed' with error
        # pairs); either way its run row establishes cooldown.
        run = _run("failed", finished=T0 - timedelta(seconds=10))
        c = compute_cooldown(run, min_interval_seconds=75, now=T0)
        assert c["execution_allowed_by_cooldown"] is False
        assert c["cooldown_remaining_seconds"] == 65

    def test_elapsed_window_allows_and_zero(self):
        run = _run("completed", finished=T0 - timedelta(seconds=80))
        c = compute_cooldown(run, min_interval_seconds=75, now=T0)
        assert c["execution_allowed_by_cooldown"] is True
        assert c["cooldown_remaining_seconds"] == 0

    def test_exactly_at_boundary_allows(self):
        run = _run("completed", finished=T0 - timedelta(seconds=75))
        c = compute_cooldown(run, min_interval_seconds=75, now=T0)
        assert c["execution_allowed_by_cooldown"] is True
        assert c["cooldown_remaining_seconds"] == 0

    def test_timestamp_precedence_falls_back(self):
        # finished_at absent -> updated_at used (documented precedence).
        run = _run("running", finished=None, updated=T0 - timedelta(seconds=5),
                   started=T0 - timedelta(seconds=9), created=T0 - timedelta(seconds=9))
        ref, source = reference_timestamp(run)
        assert source == "updated_at"
        c = compute_cooldown(run, min_interval_seconds=75, now=T0)
        assert c["last_execution_timestamp_source"] == "updated_at"
        assert c["execution_allowed_by_cooldown"] is False

    def test_naive_timestamp_treated_as_utc(self):
        run = _run("completed", finished=datetime(2026, 7, 28, 11, 59, 56))  # naive
        c = compute_cooldown(run, min_interval_seconds=75, now=T0)
        assert c["cooldown_remaining_seconds"] == 71

    def test_malformed_run_without_timestamp_blocks_conservatively(self):
        # Identifiable maintenance run but no usable timestamp: NOT permission to
        # run — hold for the full interval rather than crash or allow.
        run = {"id": "x", "status": "completed", "finished_at": None,
               "started_at": None, "created_at": None, "updated_at": None}
        c = compute_cooldown(run, min_interval_seconds=75, now=T0)
        assert c["cooldown_required"] is True
        assert c["execution_allowed_by_cooldown"] is False
        assert c["cooldown_remaining_seconds"] == 75

    def test_zero_interval_disables(self):
        run = _run("completed", finished=T0 - timedelta(seconds=1))
        c = compute_cooldown(run, min_interval_seconds=0, now=T0)
        assert c["execution_allowed_by_cooldown"] is True
        assert c["cooldown_required"] is False

    def test_retry_after_is_ceiled_and_at_least_one(self):
        run = _run("completed", finished=T0 - timedelta(seconds=74, milliseconds=100))
        c = compute_cooldown(run, min_interval_seconds=75, now=T0)
        # ~0.9s remaining -> ceil -> 1
        assert retry_after_seconds(c) == 1


class TestResolveInterval:
    def test_default(self):
        assert DEFAULT_MIN_BATCH_INTERVAL_SECONDS == 75

    def test_massive_maintenance_floor_60(self):
        assert resolve_min_interval_seconds(
            30, maintenance_only_mode=True, provider="massive") == 60

    def test_default_passthrough(self):
        assert resolve_min_interval_seconds(
            75, maintenance_only_mode=True, provider="massive") == 75

    def test_clamped_to_max(self):
        assert resolve_min_interval_seconds(
            100000, maintenance_only_mode=True, provider="massive"
        ) == MAX_MIN_BATCH_INTERVAL_SECONDS

    def test_floor_not_applied_outside_maintenance(self):
        assert resolve_min_interval_seconds(
            10, maintenance_only_mode=False, provider="massive") == 10

    def test_malformed_config_uses_default(self):
        assert resolve_min_interval_seconds(
            None, maintenance_only_mode=True, provider="massive") == 75


# --------------------------------------------------------------------------- #
# HTTP preflight pacing surface.
# --------------------------------------------------------------------------- #
class TestPreflightPacing:
    def test_preflight_exposes_pacing_and_allows_when_no_run(self, monkeypatch):
        plan = _build_plan(25)
        _maint_on(monkeypatch, plan, conn=_FakeConn(latest_run=None))
        try:
            b = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-maintenance/preflight").json()
            assert b["min_batch_interval_seconds"] == 75
            assert b["execution_allowed_by_cooldown"] is True
            assert b["cooldown_remaining_seconds"] == 0
            assert b["execution_available"] is True
            assert COOLDOWN_BLOCKING_REASON not in b["blocking_reasons"]
        finally:
            _teardown()

    def test_preflight_cooldown_active_blocks_execution_not_safety(self, monkeypatch):
        plan = _build_plan(25)
        recent = _run("completed",
                      finished=datetime.now(timezone.utc) - timedelta(seconds=5))
        _maint_on(monkeypatch, plan, conn=_FakeConn(latest_run=recent))
        try:
            b = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-maintenance/preflight").json()
            assert b["safe_to_execute"] is True          # cohort/manifest safe
            assert b["execution_available"] is False      # temporarily unavailable
            assert b["execution_allowed_by_cooldown"] is False
            assert b["cooldown_remaining_seconds"] > 0
            assert COOLDOWN_BLOCKING_REASON in b["blocking_reasons"]
            assert b["next_execution_not_before"] is not None
        finally:
            _teardown()


# --------------------------------------------------------------------------- #
# HTTP execute enforcement.
# --------------------------------------------------------------------------- #
class TestExecuteCooldown:
    def test_execute_blocked_before_provider_with_retry_after(self, monkeypatch):
        plan = _build_plan(25)
        recent = _run("completed",
                      finished=datetime.now(timezone.utc) - timedelta(seconds=4))
        conn = _FakeConn(latest_run=recent)
        _maint_on(monkeypatch, plan, conn=conn)  # provider is _Boom (must not run)
        try:
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute",
                json=_v2_normal_req(plan))
            assert r.status_code == 409
            assert r.json()["detail"]["error"] == COOLDOWN_BLOCKING_REASON
            assert int(r.headers["Retry-After"]) >= 1
            assert r.json()["detail"]["cooldown_remaining_seconds"] > 0
            # no provider constructed, no outcome-run row created
            assert conn.inserted_runs == []
        finally:
            _teardown()

    def test_back_to_back_regression(self, monkeypatch):
        """batch A succeeds; ~4s later batch B is rejected before provider
        construction; no error outcomes / runs written for B; after 75s B is
        accepted."""
        plan = _build_plan(25)

        # ---- batch A: no prior run -> executes via a stub provider/service. ---
        calls = {"provider": 0, "service": 0}

        class _Provider:
            name = "massive"

            def __init__(self):
                calls["provider"] += 1

        async def _service(provider, *, pair_ids=None, symbols=None, run_id=None,
                           pending=False, limit=None, include_recalc=False,
                           outcome_run_id=None, now_utc=None):
            calls["service"] += 1
            return {"status": "completed", "outcome_run_id": outcome_run_id}

        monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", True)
        monkeypatch.setattr(admin_mod.settings, "MAINTENANCE_ONLY_MODE", True)
        monkeypatch.setattr(admin_mod.settings, "MAINTENANCE_LOCKED_COHORT_HASH",
                            plan["cohort_lock_hash"])
        app.dependency_overrides[get_worker_token] = lambda: "t"
        conn_a = _FakeConn(latest_run=None)
        app.dependency_overrides[get_db] = lambda: conn_a
        monkeypatch.setattr(admin_mod, "get_market_data_provider", lambda: _Provider())
        monkeypatch.setattr(
            "app.workers.shadow.outcomes.service.run_shadow_outcome_calculation", _service)

        async def fake_plan(db, *, experiment_code, cohort_scope, batch_size=None):
            return plan
        monkeypatch.setattr(admin_mod, "_recompute_maintenance_plan", fake_plan)
        try:
            ra = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute",
                json=_v2_normal_req(plan))
            assert ra.status_code == 200, ra.json()
            assert ra.json()["status"] == "executed"
            assert calls["provider"] == 1 and calls["service"] == 1
            assert len(conn_a.inserted_runs) == 1  # maintenance run pre-created

            # ---- batch B ~4s later: a maintenance run finished ~4s ago. --------
            recent = _run("completed",
                          finished=datetime.now(timezone.utc) - timedelta(seconds=4))
            conn_b = _FakeConn(latest_run=recent)
            app.dependency_overrides[get_db] = lambda: conn_b
            before = dict(calls)
            rb = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute",
                json=_v2_normal_req(plan))
            assert rb.status_code == 409
            assert rb.json()["detail"]["error"] == COOLDOWN_BLOCKING_REASON
            # provider calls after early batch B == 0; no run inserted for B
            assert calls["provider"] == before["provider"]
            assert calls["service"] == before["service"]
            assert conn_b.inserted_runs == []

            # ---- after 75s: window cleared -> accepted. ------------------------
            elapsed = _run("completed",
                           finished=datetime.now(timezone.utc) - timedelta(seconds=80))
            conn_c = _FakeConn(latest_run=elapsed)
            app.dependency_overrides[get_db] = lambda: conn_c
            rc = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute",
                json=_v2_normal_req(plan))
            assert rc.status_code == 200, rc.json()
            assert rc.json()["status"] == "executed"
            assert calls["provider"] == before["provider"] + 1
            assert len(conn_c.inserted_runs) == 1
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)

    def test_cooldown_rechecked_under_advisory_lock(self, monkeypatch):
        """Initial gate passes (no run yet), but a maintenance run appears while
        waiting for the lock -> rejected under lock before provider."""
        plan = _build_plan(25)
        recent = _run("completed",
                      finished=datetime.now(timezone.utc) - timedelta(seconds=3))

        class _SeqConn(_FakeConn):
            def __init__(self):
                super().__init__(latest_run=None)
                self._calls = 0

            async def fetchrow(self, sql, *args):
                if "strategy_shadow_outcome_runs" in sql and "requested_selector" in sql:
                    self._calls += 1
                    # 1st (initial gate): none; 2nd (under lock): a recent run.
                    return None if self._calls == 1 else recent
                return None

        conn = _SeqConn()
        _maint_on(monkeypatch, plan, conn=conn)  # provider _Boom must not run
        try:
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute",
                json=_v2_normal_req(plan))
            assert r.status_code == 409
            assert r.json()["detail"]["error"] == COOLDOWN_UNDER_LOCK_REASON
            assert conn.inserted_runs == []
        finally:
            _teardown()

    def test_idempotent_replay_bypasses_cooldown(self, monkeypatch):
        """A stale replay whose pairs are already complete returns already_applied
        even while a cooldown is active — never blocked, never a provider call."""
        from test_shadow_maintenance import _rec, _outcome, AF, CAL, LATEST
        from app.workers.shadow.maturation_plan import build_maturation_plan

        stale = _build_plan(25)
        batch = stale["next_batch"]["pair_ids"]
        recs = [_rec(f"e{i:03d}", f"S{i:03d}", "2026-06-12",
                     status=("complete" if f"e{i:03d}" in set(batch) else None))
                for i in range(25)]
        recs += [_rec(f"c{i:03d}", f"C{i:03d}", "2026-06-12", status="complete")
                 for i in range(3)]
        recs.append(_rec("rX", "AAPL", "2026-06-10", status="error"))
        rows = [_outcome(f"c{i:03d}", f"C{i:03d}", "2026-06-12", "complete") for i in range(3)]
        rows += [_outcome(p, "S", "2026-06-12", "complete") for p in batch]
        rows.append(_outcome("rX", "AAPL", "2026-06-10", "error", error="forward_fetch_error"))
        server_plan = build_maturation_plan(recs, rows, cohort_scope="campaign",
                                            applied_filters=AF, session_dates=CAL,
                                            latest_completed_session=LATEST, batch_size=10)
        recent = _run("completed",
                      finished=datetime.now(timezone.utc) - timedelta(seconds=2))
        conn = _FakeConn(statuses={p: "complete" for p in batch}, latest_run=recent)
        _maint_on(monkeypatch, server_plan, locked=server_plan["cohort_lock_hash"], conn=conn)
        try:
            req = _v2_normal_req(stale)  # stale hashes
            req["cohort_lock_hash"] = server_plan["cohort_lock_hash"]
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute", json=req)
            assert r.status_code == 200, r.json()
            assert r.json()["status"] == "already_applied"  # _Boom never raised
            assert conn.inserted_runs == []
        finally:
            _teardown()

    def test_retry_mode_uses_same_cooldown(self, monkeypatch):
        plan = _build_plan(0, 3)  # normal complete, retryable rX present
        recent = _run("completed",
                      finished=datetime.now(timezone.utc) - timedelta(seconds=5))
        conn = _FakeConn(latest_run=recent)
        _maint_on(monkeypatch, plan, conn=conn)  # provider _Boom must not run
        try:
            req = {"contract_version": EXECUTE_CONTRACT_VERSION, "mode": "retry",
                   "experiment_code": EXP, "cohort_scope": "campaign",
                   "cohort_lock_hash": plan["cohort_lock_hash"],
                   "retry_plan_hash": plan["retry_plan"]["retry_plan_hash"],
                   "pair_ids": ["rX"], "limit": 1}
            r = TestClient(app, raise_server_exceptions=False).post(
                "/api/admin/shadow-maintenance/outcomes/execute", json=req)
            assert r.status_code == 409
            assert r.json()["detail"]["error"] == COOLDOWN_BLOCKING_REASON
            assert conn.inserted_runs == []
        finally:
            _teardown()


class TestAccessCheckPacing:
    def test_access_check_reports_interval_and_source(self, monkeypatch):
        from app.maintenance_access import evaluate_maintenance_access
        out = evaluate_maintenance_access(
            database_identity="smart_scanner_outcome_maintainer",
            role_attributes={}, relation_probes=[], expected_role=None,
            connection_mode="maintenance_explicit", provider="massive",
            provider_credential_configured=True, scheduler_enabled=False,
            maintenance_only_mode=True, max_batch_size=3, mutation_route_count=1,
            min_batch_interval_seconds=75)
        assert out["min_batch_interval_seconds"] == 75
        assert out["cooldown_persistence_source"] == "strategy_shadow_outcome_runs"
