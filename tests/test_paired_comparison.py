"""Paired candidate-vs-control analytical surface — pure builders + HTTP + allowlist.

No Supabase / Massive: DB is a fake connection, readers are monkeypatched to
return synthetic records. Proves reconciliation, symmetry, duplicate/missing arm
detection, null-return handling, population filters, pagination, the min-sample
inferential gate, the audit-only allowlist, auth, and no-secret output.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from main import app
from app.config import settings
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod
from app.audit_mode import is_audit_route_allowed
from app.paired_comparison import (
    MIN_INFERENTIAL_SAMPLE,
    build_paired_comparison,
    build_paired_metrics,
    reconcile_pairs,
    sign_test,
    wilcoxon_signed_rank,
    paired_t_test,
    bootstrap_ci,
)

EXP = "wyckoff_v2_vs_baseline"


def cand(pid, *, verdict="AVOID", readiness="insufficient_history", setup=False,
         trigger_state=None, eligible=None, allow_enter=False, score=None,
         fourh_state=None, outcome_status="complete", symbol="AAA", snap="2026-06-11"):
    policy = {"setup_state": "valid" if setup else "no_setup", "allow_enter": allow_enter}
    if eligible is not None:
        policy["enter_eligible_without_rollout_gate"] = eligible
    return {
        "pair_id": pid, "arm_code": "candidate_wyckoff_v2",
        "strategy_code": "wyckoff_mtf_v2", "strategy_version": "wyckoff_mtf.v2",
        "verdict": verdict, "score": score, "readiness_status": readiness,
        "policy": policy,
        "four_hour_trigger": ({"state": trigger_state} if trigger_state else None),
        "four_hour_frame_meta": ({"state": fourh_state} if fourh_state else None),
        "symbol": symbol, "snapshot_date": snap, "campaign_ids": ["camp-1"],
        "outcome_status": outcome_status,
    }


def ctrl(pid, *, verdict="AVOID", score=None, symbol="AAA", snap="2026-06-11",
         outcome_status="complete"):
    return {
        "pair_id": pid, "arm_code": "control_baseline",
        "strategy_code": "sma150_bounce", "strategy_version": "sma150.v2",
        "verdict": verdict, "score": score, "readiness_status": None,
        "policy": {}, "four_hour_trigger": None, "four_hour_frame_meta": None,
        "symbol": symbol, "snapshot_date": snap, "campaign_ids": ["camp-1"],
        "outcome_status": outcome_status,
    }


def oc(pid, *, rets=None, status="complete", symbol="AAA", snap="2026-06-11"):
    r = rets or {"1d": None, "3d": None, "5d": None, "10d": None, "20d": None}
    return {
        "pair": {"pair_id": pid, "symbol": symbol, "snapshot_date": snap,
                 "frame_hash": "fh", "experiment_code": EXP,
                 "experiment_version": "v", "timeframe": "1d",
                 "provider": "massive", "frame_bar_count": 1},
        "control": {"arm_code": "control_baseline", "strategy_code": "sma150_bounce",
                    "verdict": "AVOID"},
        "candidate": {"arm_code": "candidate_wyckoff_v2",
                      "strategy_code": "wyckoff_mtf_v2", "verdict": "AVOID"},
        "agreement": True, "disagreement_category": "agree_avoid",
        "outcome": {"returns": r, "max_favorable_excursion": None,
                    "max_adverse_excursion": None, "benchmark_returns": None,
                    "outcome_status": status, "error_code": None},
        "relative_returns": {},
    }


# --------------------------------------------------------------------------- #
class TestReconciliation:
    def test_valid_paired(self):
        c = [cand("p1"), cand("p2")]
        k = [ctrl("p1"), ctrl("p2")]
        o = [oc("p1"), oc("p2")]
        r = reconcile_pairs(c, k, o)
        assert r["raw_pair_rows"] == 2
        assert r["valid_paired_rows"] == 2
        assert r["missing_candidate_rows"] == 0
        assert r["missing_control_rows"] == 0
        assert r["duplicate_candidate_rows"] == 0
        assert r["missing_outcome_rows"] == 0

    def test_missing_control_arm(self):
        r = reconcile_pairs([cand("p1"), cand("p2")], [ctrl("p1")], [oc("p1")])
        assert r["missing_control_rows"] == 1
        assert "p2" in r["samples"]["missing_control"]
        assert r["valid_paired_rows"] == 1  # only p1 has both

    def test_missing_candidate_arm(self):
        r = reconcile_pairs([cand("p1")], [ctrl("p1"), ctrl("p2")], [oc("p1")])
        assert r["missing_candidate_rows"] == 1
        assert "p2" in r["samples"]["missing_candidate"]

    def test_duplicate_candidate_arm_detected_not_dropped(self):
        r = reconcile_pairs([cand("p1"), cand("p1")], [ctrl("p1")], [oc("p1")])
        assert r["duplicate_candidate_rows"] == 1
        assert r["raw_candidate_evaluation_rows"] == 2
        assert r["valid_paired_rows"] == 0  # dup excluded from valid

    def test_missing_outcome(self):
        r = reconcile_pairs([cand("p1", outcome_status=None)], [ctrl("p1")], [])
        assert r["missing_outcome_rows"] == 1

    def test_excluded_manual(self):
        r = reconcile_pairs([cand("p1"), cand("m1")], [ctrl("p1"), ctrl("m1")],
                            [oc("p1"), oc("m1")], excluded_manual_pair_ids=["m1"])
        assert r["excluded_manual_rows"] == 1
        assert r["valid_paired_rows"] == 1


class TestPairedComparison:
    def test_rows_and_symmetry(self):
        r = reconcile_pairs([cand("p1", verdict="WATCH", readiness="ready", setup=True)],
                            [ctrl("p1", verdict="ENTER")], [oc("p1")])
        out = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign")
        assert out["contract_version"] == "shadow_paired_comparison.v1"
        assert out["total_rows"] == 1
        row = out["rows"][0]
        assert row["candidate"]["strategy_code"] == "wyckoff_mtf_v2"
        assert row["control"]["strategy_code"] == "sma150_bounce"
        assert row["candidate"]["readiness_status"] == "ready"
        assert row["candidate"]["setup_present"] is True
        assert row["control"]["verdict"] == "ENTER"
        assert row["control"]["actionable"] is True
        assert "null_semantics" in out

    def test_null_returns_preserved_not_zeroed(self):
        r = reconcile_pairs([cand("p1")], [ctrl("p1")], [oc("p1")])  # returns all None
        out = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign")
        assert out["rows"][0]["outcome"]["returns"] == {
            "1d": None, "3d": None, "5d": None, "10d": None, "20d": None}
        assert out["rows"][0]["outcome"]["stop_price"] is None

    def test_pre_rollout_actionability(self):
        # verdict AVOID but pre-rollout eligible → candidate actionable = True
        r = reconcile_pairs(
            [cand("p1", verdict="WATCH", eligible=True, allow_enter=False,
                  readiness="ready", setup=True)],
            [ctrl("p1")], [oc("p1")])
        out = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign")
        assert out["rows"][0]["candidate"]["pre_rollout_enter_eligible"] is True
        assert out["rows"][0]["candidate"]["rollout_blocked"] is True
        assert out["rows"][0]["candidate"]["actionable"] is True

    def test_pagination_cursor_stable(self):
        c = [cand(f"p{i:03d}") for i in range(10)]
        k = [ctrl(f"p{i:03d}") for i in range(10)]
        o = [oc(f"p{i:03d}") for i in range(10)]
        r = reconcile_pairs(c, k, o)
        p1 = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign",
                                     cursor=0, limit=4)
        assert len(p1["rows"]) == 4 and p1["next_cursor"] == 4
        p2 = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign",
                                     cursor=4, limit=4)
        assert [x["pair_id"] for x in p1["rows"]] != [x["pair_id"] for x in p2["rows"]]
        assert p2["next_cursor"] == 8

    def test_population_filter(self):
        c = [cand("p1", readiness="ready", setup=True), cand("p2")]
        k = [ctrl("p1"), ctrl("p2")]
        o = [oc("p1"), oc("p2")]
        r = reconcile_pairs(c, k, o)
        out = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign",
                                      decision_population="B_candidate_ready")
        assert out["total_rows"] == 1
        assert out["rows"][0]["pair_id"] == "p1"


class TestPairedMetrics:
    def test_population_counts_and_suppressed_stats(self):
        c = [cand("p1", readiness="ready", setup=True)]
        k = [ctrl("p1", verdict="ENTER")]
        o = [oc("p1", rets={"1d": 0.02, "3d": None, "5d": None, "10d": None, "20d": None})]
        r = reconcile_pairs(c, k, o)
        m = build_paired_metrics(r, experiment_code=EXP, campaign_scope="campaign")
        assert m["contract_version"] == "shadow_paired_metrics.v1"
        assert m["population_counts"]["A_full"] == 1
        assert m["population_counts"]["B_candidate_ready"] == 1
        assert m["population_counts"]["E_control_actionable"] == 1
        h = m["per_population"]["A_full"]["horizons"]["1d"]
        assert h["candidate_n"] == 1
        assert h["inferential"] is None  # below MIN_INFERENTIAL_SAMPLE
        assert "MIN_INFERENTIAL_SAMPLE" in (h["inferential_suppressed_reason"] or "")

    def test_inferential_present_above_min_sample(self):
        n = MIN_INFERENTIAL_SAMPLE + 5
        c = [cand(f"p{i}", readiness="ready", setup=True) for i in range(n)]
        k = [ctrl(f"p{i}") for i in range(n)]
        o = [oc(f"p{i}", rets={"1d": (0.01 if i % 2 else -0.005),
                               "3d": None, "5d": None, "10d": None, "20d": None})
             for i in range(n)]
        r = reconcile_pairs(c, k, o)
        m = build_paired_metrics(r, experiment_code=EXP, campaign_scope="campaign")
        h = m["per_population"]["A_full"]["horizons"]["1d"]
        assert h["paired_n"] == n
        # The inferential BLOCK is present once paired_n >= MIN_INFERENTIAL_SAMPLE.
        assert h["inferential"] is not None
        assert h["inferential"]["bonferroni_family_size"] == 5
        # Candidate & control share the one per-pair outcome path, so raw paired
        # differences are all zero -> the paired-difference tests are degenerate
        # (None). Effect sizes/denominators are still reported. This is the
        # honest reflection of the data model (see interpretation_guard).
        assert h["mean_paired_difference"] == 0
        assert h["inferential"]["sign_test"] is None
        assert h["candidate_mean_return"] is not None


class TestStatistics:
    def test_gated_below_min(self):
        assert sign_test([0.1] * (MIN_INFERENTIAL_SAMPLE - 1)) is None
        assert wilcoxon_signed_rank([0.1] * (MIN_INFERENTIAL_SAMPLE - 1)) is None
        assert paired_t_test([0.1] * (MIN_INFERENTIAL_SAMPLE - 1)) is None
        assert bootstrap_ci([0.1] * (MIN_INFERENTIAL_SAMPLE - 1)) is None

    def test_deterministic_bootstrap(self):
        diffs = [((-1) ** i) * 0.01 * (i + 1) for i in range(40)]
        a = bootstrap_ci(diffs, seed=0)
        b = bootstrap_ci(diffs, seed=0)
        assert a == b  # deterministic for fixed input + seed

    def test_sign_test_all_positive(self):
        s = sign_test([0.01] * 40)
        assert s["positives"] == 40 and s["p_value"] <= 0.001


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
class _FakeConn:
    async def fetch(self, sql, *a):
        return []
    async def fetchval(self, sql, *a):
        return None
    async def fetchrow(self, sql, *a):
        return None


def _audit_on(monkeypatch, *, cand_records, ctrl_records, outcomes):
    monkeypatch.setattr(admin_mod.settings, "AUDIT_ONLY_MODE", True)
    monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
    app.dependency_overrides[get_worker_token] = lambda: "t"
    app.dependency_overrides[get_db] = lambda: _FakeConn()

    async def fake_access(db):
        return {"ready_for_closeout_audit": True, "reasons": [],
                "database_connection_mode": "audit_explicit"}
    monkeypatch.setattr(admin_mod, "_run_configured_access_check", fake_access)

    async def fake_records(filters):
        return cand_records if filters["strategy_code"] == "wyckoff_mtf_v2" else ctrl_records
    monkeypatch.setattr("app.workers.shadow.evidence_review.fetch_evidence_records",
                        fake_records)

    async def fake_outcomes(filters):
        return outcomes
    monkeypatch.setattr(admin_mod, "_evidence_outcome_rows", fake_outcomes)


def _teardown():
    app.dependency_overrides.pop(get_worker_token, None)
    app.dependency_overrides.pop(get_db, None)


class TestHttp:
    def test_paired_comparison_ok(self, monkeypatch):
        _audit_on(monkeypatch, cand_records=[cand("p1")], ctrl_records=[ctrl("p1")],
                  outcomes=[oc("p1")])
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-cohort/paired-comparison?experiment_code=" + EXP)
            assert r.status_code == 200, r.json()
            b = r.json()
            assert b["contract_version"] == "shadow_paired_comparison.v1"
            assert b["total_rows"] == 1
            assert b["reconciliation"]["valid_paired_rows"] == 1
            # no secret leakage
            assert "token" not in r.text.lower() and "password" not in r.text.lower()
        finally:
            _teardown()

    def test_paired_metrics_ok(self, monkeypatch):
        _audit_on(monkeypatch, cand_records=[cand("p1", readiness="ready")],
                  ctrl_records=[ctrl("p1")], outcomes=[oc("p1")])
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-cohort/paired-metrics?experiment_code=" + EXP)
            assert r.status_code == 200, r.json()
            assert r.json()["contract_version"] == "shadow_paired_metrics.v1"
            assert r.json()["population_counts"]["A_full"] == 1
        finally:
            _teardown()

    def test_selector_required_422(self, monkeypatch):
        _audit_on(monkeypatch, cand_records=[], ctrl_records=[], outcomes=[])
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-cohort/paired-comparison")
            assert r.status_code == 422
        finally:
            _teardown()


class TestAllowlist:
    def test_new_routes_get_only(self):
        for route in ("/api/admin/shadow-cohort/paired-comparison",
                      "/api/admin/shadow-cohort/paired-metrics",
                      "/api/admin/shadow-cohort/prospective-readiness"):
            assert is_audit_route_allowed("GET", route) is True
            assert is_audit_route_allowed("POST", route) is False
            assert is_audit_route_allowed("DELETE", route) is False
