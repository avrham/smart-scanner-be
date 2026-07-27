"""Bounded maturation-plan builder + HTTP boundary (shadow_maturation_plan.v1).

Proves the PURE plan builder is deterministic, complete and fail-closed, and
that the read-only endpoint is worker-token protected, audit-only permitted,
selector-required, bounded, deterministically paginated, hash-stable and never
constructs a provider or issues a mutation. Maturation is NOT re-implemented:
it stays on POST /api/admin/shadow/outcomes/calculate.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from main import app
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod
from app.workers.shadow.maturation_plan import (
    DUP_BENIGN_CROSS_CAMPAIGN,
    DUP_IDENTITY_MISMATCH,
    DUP_WITHIN_SAME_CAMPAIGN,
    DUP_WITHIN_SAME_RUN,
    MATURATION_PLAN_CONTRACT_VERSION,
    build_maturation_plan,
    classify_duplicate_group,
    compute_manifest_hash,
)


# --------------------------------------------------------------------------- #
# Fixtures: a trading calendar and synthetic evaluation/outcome records.
# --------------------------------------------------------------------------- #
CAL = [date(2026, 6, d) for d in range(10, 30)]  # 2026-06-10 .. 06-29
LATEST = date(2026, 6, 29)
AF = {"strategy_code": "wyckoff_mtf_v2", "experiment_code": "wyckoff_v2_vs_baseline"}


def rec(
    pid: str,
    symbol: str,
    snapshot: str,
    *,
    status: Optional[str] = None,
    run: str = "run-A",
    campaigns=("camp-1",),
    strategy_version: str = "wyckoff_mtf.v2",
    experiment_version: str = "wyckoff_v2_shadow.v2",
    experiment_code: str = "wyckoff_v2_vs_baseline",
    strategy_code: str = "wyckoff_mtf_v2",
    config_hash: str = "cfg1",
    policy: str = "pol.v1",
) -> Dict[str, Any]:
    return {
        "evaluation_id": f"e-{pid}",
        "pair_id": pid,
        "run_id": run,
        "symbol": symbol,
        "snapshot_date": snapshot,
        "strategy_code": strategy_code,
        "strategy_version": strategy_version,
        "experiment_code": experiment_code,
        "experiment_version": experiment_version,
        "decision_policy_version": policy,
        "config_hash": config_hash,
        "outcome_status": status,
        "has_outcome": status is not None,
        "campaign_ids": list(campaigns),
        "created_at": "2026-06-16T00:00:00+00:00",
    }


def outcome(pid: str, symbol: str, snapshot: str, status: str, error=None, returns=None):
    return {
        "pair": {"pair_id": pid, "symbol": symbol, "snapshot_date": snapshot},
        "outcome": {"outcome_status": status, "error_code": error,
                    "returns": returns or {}},
    }


def _plan(records, outcome_rows=None, **kw):
    return build_maturation_plan(
        records, outcome_rows or [], applied_filters=AF,
        session_dates=CAL, latest_completed_session=LATEST, **kw,
    )


class TestManifestCompletenessAndSeparation:
    def test_eligible_manifest_and_retry_are_separate(self):
        records = [
            rec("p1", "AAPL", "2026-06-12"),                       # eligible
            rec("p2", "MSFT", "2026-06-12", status="complete"),    # matured
            rec("perr", "AAPL", "2026-06-10", status="error"),     # retryable
        ]
        rows = [
            outcome("perr", "AAPL", "2026-06-10", "error", error="forward_fetch_error"),
            outcome("p2", "MSFT", "2026-06-12", "complete", returns={"1D": 0.1}),
        ]
        plan = _plan(records, rows)
        assert plan["contract_version"] == MATURATION_PLAN_CONTRACT_VERSION
        assert plan["manifest_total"] == 1
        assert plan["eligible_unmatured_count"] == 1
        elig_ids = {e["pair_id"] for e in plan["eligible_manifest"]}
        retry_ids = {e["pair_id"] for e in plan["retry_plan"]["entries"]}
        assert elig_ids == {"p1"}
        assert retry_ids == {"perr"}
        assert not (elig_ids & retry_ids)  # the failure is NEVER in the manifest
        entry = plan["retry_plan"]["entries"][0]
        assert entry["current_error_code"] == "forward_fetch_error"
        assert entry["retryable"] is True
        assert entry["requires_include_recalc"] is True
        assert plan["planning"]["safe_to_execute"] is True

    def test_manifest_count_equals_authoritative_eligible(self):
        records = [rec(f"p{i}", "AAA", "2026-06-12") for i in range(7)]
        plan = _plan(records)
        assert plan["manifest_total"] == 7
        assert plan["eligible_unmatured_count"] == 7
        assert plan["eligibility"]["counts"]["eligible"] == 7

    def test_not_yet_eligible_excluded_from_manifest(self):
        # snapshot == latest completed session ⇒ 0 forward sessions ⇒ not_yet.
        records = [rec("pn", "AAA", "2026-06-29"), rec("pe", "AAA", "2026-06-12")]
        plan = _plan(records)
        assert plan["not_yet_eligible_count"] == 1
        assert {e["pair_id"] for e in plan["eligible_manifest"]} == {"pe"}


class TestDeterministicOrderingAndPagination:
    def _records(self, n=25):
        out = []
        for i in range(n):
            day = 12 + (i % 5)
            out.append(rec(f"p{i:03d}", f"S{i:02d}", f"2026-06-{day:02d}"))
        return out

    def test_ordering_snapshot_symbol_pair(self):
        plan = _plan(self._records())
        entries = plan["eligible_manifest"]
        keys = [(e["snapshot_date"], e["symbol"], e["pair_id"]) for e in entries]
        assert keys == sorted(keys)

    def test_pages_combine_without_dupe_or_gap(self):
        records = self._records(25)
        seen: List[str] = []
        offset = 0
        while True:
            plan = _plan(records, page_limit=10, page_offset=offset)
            seen.extend(e["pair_id"] for e in plan["eligible_manifest"])
            if not plan["has_more"]:
                break
            offset = plan["next_offset"]
        assert len(seen) == 25
        assert len(set(seen)) == 25  # no duplicates, no omissions

    def test_total_is_page_independent(self):
        records = self._records(25)
        for size in (1, 5, 10, 500):
            plan = _plan(records, page_limit=size, page_offset=0)
            assert plan["manifest_total"] == 25


class TestManifestHash:
    def test_hash_independent_of_page_size(self):
        records = [rec(f"p{i}", "AAA", "2026-06-12") for i in range(9)]
        full = _plan(records, page_limit=500)
        paged = _plan(records, page_limit=2, page_offset=4)
        assert full["manifest_hash"] == paged["manifest_hash"]

    def test_changing_pair_id_changes_hash(self):
        a = _plan([rec("p1", "AAA", "2026-06-12")])
        b = _plan([rec("pX", "AAA", "2026-06-12")])
        assert a["manifest_hash"] != b["manifest_hash"]

    def test_changing_cohort_identity_changes_hash(self):
        entries = [{"pair_id": "p1", "snapshot_date": "2026-06-12",
                    "strategy_code": "wyckoff_mtf_v2", "strategy_version": "v2",
                    "experiment_code": "wyckoff_v2_vs_baseline",
                    "experiment_version": "ev"}]
        h1 = compute_manifest_hash(
            {"strategy_code": "wyckoff_mtf_v2", "experiment_code": "wyckoff_v2_vs_baseline"},
            entries)
        h2 = compute_manifest_hash(
            {"strategy_code": "wyckoff_mtf_v2", "experiment_code": "other_experiment"},
            entries)
        assert h1 != h2

    def test_ordering_does_not_change_hash(self):
        r = [rec("p1", "AAA", "2026-06-12"), rec("p2", "BBB", "2026-06-13")]
        assert _plan(r)["manifest_hash"] == _plan(list(reversed(r)))["manifest_hash"]


class TestDuplicateClassification:
    def test_benign_cross_campaign(self):
        g = [rec("p1", "AAPL", "2026-06-12", run="rA", campaigns=("camp-1",)),
             rec("p2", "AAPL", "2026-06-12", run="rB", campaigns=("camp-2",))]
        assert classify_duplicate_group(g) == DUP_BENIGN_CROSS_CAMPAIGN

    def test_same_campaign(self):
        g = [rec("p1", "AAPL", "2026-06-12", run="rA", campaigns=("camp-1",)),
             rec("p2", "AAPL", "2026-06-12", run="rB", campaigns=("camp-1",))]
        assert classify_duplicate_group(g) == DUP_WITHIN_SAME_CAMPAIGN

    def test_same_run(self):
        g = [rec("p1", "AAPL", "2026-06-12", run="rA", campaigns=("camp-1",)),
             rec("p2", "AAPL", "2026-06-12", run="rA", campaigns=("camp-2",))]
        assert classify_duplicate_group(g) == DUP_WITHIN_SAME_RUN

    def test_identity_mismatch(self):
        g = [rec("p1", "AAPL", "2026-06-12", run="rA", campaigns=("camp-1",), config_hash="c1"),
             rec("p2", "AAPL", "2026-06-12", run="rB", campaigns=("camp-2",), config_hash="c2")]
        assert classify_duplicate_group(g) == DUP_IDENTITY_MISMATCH

    def test_benign_does_not_block_but_same_campaign_does(self):
        benign = _plan([
            rec("p1", "AAPL", "2026-06-12", run="rA", campaigns=("camp-1",)),
            rec("p2", "AAPL", "2026-06-12", run="rB", campaigns=("camp-2",)),
        ])
        assert benign["planning"]["safe_to_execute"] is True
        assert "blocking_duplicate_group" not in benign["planning"]["blocking_reasons"]

        same = _plan([
            rec("p1", "AAPL", "2026-06-12", run="rA", campaigns=("camp-1",)),
            rec("p2", "AAPL", "2026-06-12", run="rB", campaigns=("camp-1",)),
        ])
        assert same["planning"]["safe_to_execute"] is False
        assert "blocking_duplicate_group" in same["planning"]["blocking_reasons"]


class TestSafetyGate:
    def test_membership_unverifiable_blocks(self):
        plan = _plan([rec("p1", "AAA", "2026-06-12", campaigns=())])
        assert plan["membership"]["membership_unverifiable_count"] == 1
        assert plan["planning"]["safe_to_execute"] is False
        assert "membership_unverifiable" in plan["planning"]["blocking_reasons"]

    def test_non_uniform_strategy_version_blocks(self):
        plan = _plan([
            rec("p1", "AAA", "2026-06-12", strategy_version="wyckoff_mtf.v2"),
            rec("p2", "BBB", "2026-06-12", strategy_version="wyckoff_mtf.v1"),
        ])
        assert plan["planning"]["safe_to_execute"] is False
        assert "non_uniform_strategy_version" in plan["planning"]["blocking_reasons"]

    def test_terminal_failure_blocks_and_is_in_retry_plan(self):
        records = [
            rec("p1", "AAA", "2026-06-12"),
            rec("pt", "BBB", "2026-06-12", status="error"),
        ]
        rows = [outcome("pt", "BBB", "2026-06-12", "error",
                        error="reference_revision_detected")]
        plan = _plan(records, rows)
        assert plan["terminal_failure_count"] == 1
        assert plan["planning"]["safe_to_execute"] is False
        assert "terminal_failures_present" in plan["planning"]["blocking_reasons"]
        term = [e for e in plan["retry_plan"]["entries"] if e["pair_id"] == "pt"][0]
        assert term["retryable"] is False
        assert "pt" not in {e["pair_id"] for e in plan["eligible_manifest"]}

    def test_truncated_read_blocks(self):
        plan = _plan([rec("p1", "AAA", "2026-06-12")], records_possibly_truncated=True)
        assert plan["planning"]["safe_to_execute"] is False
        assert "cohort_exceeds_bounded_read" in plan["planning"]["blocking_reasons"]

    def test_batch_math_matches_manifest_total(self):
        plan = _plan([rec(f"p{i}", "AAA", "2026-06-12") for i in range(23)])
        pl = plan["planning"]
        size, count, final = (pl["recommended_batch_size"],
                              pl["recommended_batch_count"], pl["final_batch_size"])
        assert size * (count - 1) + final == plan["manifest_total"]


# --------------------------------------------------------------------------- #
# HTTP boundary.
# --------------------------------------------------------------------------- #
class _Boom:
    def __call__(self, *a, **k):
        raise AssertionError("maturation plan constructed a provider client")


class _FakeConn:
    """Minimal async connection: origin-run recovery returns nothing (run_id
    is then None, which the plan tolerates — membership uses campaign_ids)."""

    async def fetch(self, *a, **k):
        return []


def _patch_reads(monkeypatch, *, records, outcome_rows, latest=LATEST, calendar=CAL):
    async def fake_records(filters):
        fake_records.filters = filters
        return records

    async def fake_rows(filters):
        return outcome_rows

    async def fake_latest():
        return latest

    async def fake_bars(symbol, start, end):
        return [{"trading_date": d} for d in calendar]

    monkeypatch.setattr(
        "app.workers.shadow.evidence_review.fetch_evidence_records", fake_records)
    monkeypatch.setattr(admin_mod, "_evidence_outcome_rows", fake_rows)
    monkeypatch.setattr(admin_mod.market_store, "get_latest_daily_bar_date", fake_latest)
    monkeypatch.setattr(admin_mod.market_store, "get_local_daily_bars_range", fake_bars)
    monkeypatch.setattr(admin_mod, "get_market_data_provider", _Boom())
    return fake_records


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
    def test_missing_selector_422(self, client, monkeypatch):
        _patch_reads(monkeypatch, records=[], outcome_rows=[])
        resp = client.get("/api/admin/shadow-cohort/maturation-plan")
        assert resp.status_code == 422
        assert "cohort selector" in resp.json()["detail"]

    def test_unknown_experiment_is_empty_plan_not_all_history(self, client, monkeypatch):
        _patch_reads(monkeypatch, records=[], outcome_rows=[])
        resp = client.get("/api/admin/shadow-cohort/maturation-plan",
                          params={"experiment_code": "does_not_exist"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["manifest_total"] == 0
        assert body["eligible_manifest"] == []
        assert body["has_more"] is False

    def test_result_is_bounded_and_paginated(self, client, monkeypatch):
        records = [rec(f"p{i:03d}", f"S{i:03d}", "2026-06-12") for i in range(12)]
        _patch_reads(monkeypatch, records=records, outcome_rows=[])
        r1 = client.get("/api/admin/shadow-cohort/maturation-plan",
                        params={"experiment_code": "wyckoff_v2_vs_baseline",
                                "limit": 5, "offset": 0}).json()
        assert r1["returned_count"] == 5 and r1["manifest_total"] == 12
        assert r1["has_more"] is True and r1["next_offset"] == 5
        r3 = client.get("/api/admin/shadow-cohort/maturation-plan",
                        params={"experiment_code": "wyckoff_v2_vs_baseline",
                                "limit": 5, "offset": 10}).json()
        assert r3["returned_count"] == 2 and r3["has_more"] is False

    def test_limit_over_max_422(self, client, monkeypatch):
        _patch_reads(monkeypatch, records=[], outcome_rows=[])
        resp = client.get("/api/admin/shadow-cohort/maturation-plan",
                          params={"experiment_code": "x", "limit": 100000})
        assert resp.status_code == 422

    def test_hash_stable_and_count_correct_across_page_sizes(self, client, monkeypatch):
        records = [rec(f"p{i:03d}", f"S{i:03d}", "2026-06-12") for i in range(9)]
        _patch_reads(monkeypatch, records=records, outcome_rows=[])
        hashes = set()
        for size in (1, 3, 9, 500):
            body = client.get("/api/admin/shadow-cohort/maturation-plan",
                              params={"experiment_code": "wyckoff_v2_vs_baseline",
                                      "limit": size}).json()
            assert body["manifest_total"] == 9
            hashes.add(body["manifest_hash"])
        assert len(hashes) == 1

    def test_no_provider_constructed(self, client, monkeypatch):
        # _Boom raises if get_market_data_provider is ever called.
        records = [rec("p1", "AAA", "2026-06-12")]
        _patch_reads(monkeypatch, records=records, outcome_rows=[])
        resp = client.get("/api/admin/shadow-cohort/maturation-plan",
                          params={"experiment_code": "wyckoff_v2_vs_baseline"})
        assert resp.status_code == 200


class TestAuthAndAuditMode:
    @pytest.fixture
    def auth_client(self, monkeypatch):
        from app.config import settings

        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "unit-test-token")
        app.dependency_overrides[get_db] = lambda: _FakeConn()
        _patch_reads(monkeypatch, records=[], outcome_rows=[])
        try:
            yield TestClient(app, raise_server_exceptions=False)
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_missing_token_rejected(self, auth_client):
        assert auth_client.get(
            "/api/admin/shadow-cohort/maturation-plan",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
        ).status_code == 401

    def test_invalid_token_rejected(self, auth_client):
        assert auth_client.get(
            "/api/admin/shadow-cohort/maturation-plan",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
            headers={"X-Worker-Token": "wrong"},
        ).status_code == 401

    def test_valid_token_reaches(self, auth_client):
        assert auth_client.get(
            "/api/admin/shadow-cohort/maturation-plan",
            params={"experiment_code": "wyckoff_v2_vs_baseline"},
            headers={"X-Worker-Token": "unit-test-token"},
        ).status_code == 200

    def test_audit_only_mode_permits_endpoint(self, monkeypatch):
        from app.config import settings
        from app.audit_mode import is_audit_route_allowed

        assert is_audit_route_allowed(
            "GET", "/api/admin/shadow-cohort/maturation-plan") is True
        # POST to the same path stays blocked (read-only method gate).
        assert is_audit_route_allowed(
            "POST", "/api/admin/shadow-cohort/maturation-plan") is False

        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _FakeConn()
        _patch_reads(monkeypatch, records=[], outcome_rows=[])

        # Gate must not 404 this route (its handler then applies its own gates).
        monkeypatch.setattr(admin_mod.settings, "AUDIT_ONLY_MODE", True)

        async def ready_check(db):
            return {"ready_for_closeout_audit": True, "reasons": [],
                    "database_connection_mode": "audit_explicit"}

        monkeypatch.setattr(admin_mod, "_run_configured_access_check", ready_check)
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/api/admin/shadow-cohort/maturation-plan",
                              params={"experiment_code": "wyckoff_v2_vs_baseline"})
            assert resp.status_code == 200
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)

    def test_audit_only_fail_closed_when_not_ready(self, monkeypatch):
        from app.config import settings

        monkeypatch.setattr(admin_mod.settings, "AUDIT_ONLY_MODE", True)
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _FakeConn()
        _patch_reads(monkeypatch, records=[], outcome_rows=[])

        async def not_ready(db):
            return {"ready_for_closeout_audit": False,
                    "reasons": ["rls_select_policy_missing:[...]"],
                    "database_connection_mode": "audit_explicit"}

        monkeypatch.setattr(admin_mod, "_run_configured_access_check", not_ready)
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/api/admin/shadow-cohort/maturation-plan",
                              params={"experiment_code": "wyckoff_v2_vs_baseline"})
            assert resp.status_code == 409
            assert resp.json()["detail"]["error"] == "maturation_plan_not_ready"
        finally:
            app.dependency_overrides.pop(get_worker_token, None)
            app.dependency_overrides.pop(get_db, None)
