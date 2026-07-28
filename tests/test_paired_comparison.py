"""Paired candidate-vs-control analytical surface v2 — pure builders + HTTP + allowlist.

No Supabase / Massive. Proves the v2 semantic corrections: WATCH is decomposed
(not equated with pre-rollout eligibility), the candidate signal populations are
separate, the outcome is identified as a SHARED market path, no misleading
candidate-minus-control strategy return is emitted, selection-conditioned
market-path populations (candidate-only / control-only / both / neither) are
distinct, plus reconciliation, symmetry, dup/missing detection, null handling,
pagination, the audit allowlist, auth, and no-secret output.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from main import app
from app.config import settings
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod
from app.audit_mode import is_audit_route_allowed
from app.paired_comparison import (
    CANDIDATE_SIGNAL_DEFINITION,
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
                 "frame_hash": "fh", "experiment_code": EXP, "experiment_version": "v",
                 "timeframe": "1d", "provider": "massive", "frame_bar_count": 1},
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
        r = reconcile_pairs([cand("p1"), cand("p2")], [ctrl("p1"), ctrl("p2")],
                            [oc("p1"), oc("p2")])
        assert r["valid_paired_rows"] == 2 and r["raw_pair_rows"] == 2

    def test_missing_control_arm(self):
        r = reconcile_pairs([cand("p1"), cand("p2")], [ctrl("p1")], [oc("p1")])
        assert r["missing_control_rows"] == 1 and r["valid_paired_rows"] == 1
        assert "p2" in r["samples"]["missing_control"]

    def test_missing_candidate_arm(self):
        r = reconcile_pairs([cand("p1")], [ctrl("p1"), ctrl("p2")], [oc("p1")])
        assert r["missing_candidate_rows"] == 1

    def test_duplicate_candidate_detected_not_dropped(self):
        r = reconcile_pairs([cand("p1"), cand("p1")], [ctrl("p1")], [oc("p1")])
        assert r["duplicate_candidate_rows"] == 1 and r["raw_candidate_evaluation_rows"] == 2
        assert r["valid_paired_rows"] == 0

    def test_missing_outcome(self):
        r = reconcile_pairs([cand("p1", outcome_status=None)], [ctrl("p1")], [])
        assert r["missing_outcome_rows"] == 1

    def test_excluded_manual(self):
        r = reconcile_pairs([cand("p1"), cand("m1")], [ctrl("p1"), ctrl("m1")],
                            [oc("p1"), oc("m1")], excluded_manual_pair_ids=["m1"])
        assert r["excluded_manual_rows"] == 1 and r["valid_paired_rows"] == 1


class TestSignalSemanticsV2:
    def test_contract_is_v2_with_signal_definition(self):
        r = reconcile_pairs([cand("p1")], [ctrl("p1")], [oc("p1")])
        out = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign")
        assert out["contract_version"] == "shadow_paired_comparison.v2"
        assert out["candidate_signal_definition"] == CANDIDATE_SIGNAL_DEFINITION
        # no broad 'actionable' field on the candidate block
        assert "actionable" not in out["rows"][0]["candidate"]
        assert out["rows"][0]["candidate"]["primary_signal_definition"] == CANDIDATE_SIGNAL_DEFINITION

    def test_watch_not_equivalent_to_pre_rollout_eligibility(self):
        # WATCH with valid setup but trigger NOT confirmed and NOT eligible
        waiting = cand("p1", verdict="WATCH", readiness="ready", setup=True)
        # WATCH that is a confirmed-trigger rollout-blocked would-enter
        blocked = cand("p2", verdict="WATCH", readiness="ready", setup=True,
                       trigger_state="confirmed", eligible=True, allow_enter=False)
        r = reconcile_pairs([waiting, blocked], [ctrl("p1"), ctrl("p2")],
                            [oc("p1"), oc("p2")])
        out = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign")
        rows = {x["pair_id"]: x for x in out["rows"]}
        w = rows["p1"]["candidate"]; b = rows["p2"]["candidate"]
        assert w["watch"] is True and w["pre_rollout_enter_eligible"] in (False, None)
        assert w["watch_classification"] == "valid_setup_trigger_unconfirmed"
        assert b["watch"] is True and b["pre_rollout_enter_eligible"] is True
        assert b["watch_classification"] == "trigger_confirmed_rollout_blocked"
        # The two WATCH records are NOT the same signal
        assert w["watch_classification"] != b["watch_classification"]
        assert w["primary_signal"] != b["primary_signal"]

    def test_separate_populations(self):
        c = [
            cand("s", readiness="ready", setup=True),                              # setup only
            cand("t", readiness="ready", setup=True, trigger_state="confirmed"),   # trigger confirmed
            cand("e", readiness="ready", setup=True, trigger_state="confirmed",
                 eligible=True, allow_enter=False, verdict="WATCH"),               # pre-rollout eligible + rollout-blocked
        ]
        k = [ctrl("s"), ctrl("t"), ctrl("e")]
        o = [oc("s"), oc("t"), oc("e")]
        m = build_paired_metrics(reconcile_pairs(c, k, o),
                                 experiment_code=EXP, campaign_scope="campaign")
        pc = m["population_counts"]
        assert pc["candidate_setup_population"] == 3
        assert pc["candidate_trigger_population"] == 2
        assert pc["candidate_pre_rollout_entry_population"] == 1
        assert pc["candidate_rollout_blocked_entry_population"] == 1
        assert pc["candidate_final_enter_population"] == 0  # allow_enter false
        assert pc["candidate_watch_population"] == 1


class TestSharedOutcomeSemanticsV2:
    def test_outcome_labeled_shared_and_no_paired_diff(self):
        r = reconcile_pairs([cand("p1")], [ctrl("p1")], [oc("p1")])
        out = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign")
        ob = out["rows"][0]["outcome"]
        assert ob["concept"] == "shared_market_path_outcome"
        assert ob["shared_across_arms"] is True
        assert ob["arm_conditioned_available"] is False
        m = build_paired_metrics(r, experiment_code=EXP, campaign_scope="campaign")
        # no candidate-minus-control strategy return anywhere
        assert "candidate_return_minus_control_return" in m["prohibited"]
        assert "per_population" not in m
        assert "selection_conditioned_market_path_metrics" in m

    def test_selection_populations_candidate_only_control_only_both_neither(self):
        # candidate-selected only (pre-rollout eligible), control AVOID
        conly = cand("co", readiness="ready", setup=True, trigger_state="confirmed",
                     eligible=True, verdict="WATCH")
        # control-selected only (ENTER), candidate AVOID
        c_c = cand("ko"); k_k = ctrl("ko", verdict="ENTER")
        # both selected
        bc = cand("bo", readiness="ready", setup=True, trigger_state="confirmed",
                  eligible=True, verdict="WATCH"); bk = ctrl("bo", verdict="ENTER")
        # neither
        nc = cand("no"); nk = ctrl("no")
        c = [conly, c_c, bc, nc]
        k = [ctrl("co"), k_k, bk, nk]
        o = [oc("co", rets={"1d": 0.05, "3d": None, "5d": None, "10d": None, "20d": None}),
             oc("ko"), oc("bo"), oc("no")]
        m = build_paired_metrics(reconcile_pairs(c, k, o),
                                 experiment_code=EXP, campaign_scope="campaign")
        s = m["selection_conditioned_market_path_metrics"]["selection_population_counts"]
        assert s["candidate_selected"] == 2  # co + bo
        assert s["control_selected"] == 2     # ko + bo
        assert s["both_selected"] == 1        # bo
        assert s["candidate_only"] == 1       # co
        assert s["control_only"] == 1         # ko
        assert s["neither_selected"] == 1     # no
        assert s["unconditional"] == 4
        # market-path distribution present for candidate_selected 1d
        cd = m["selection_conditioned_market_path_metrics"]["populations"]["candidate_selected"]["horizons"]["1d"]
        assert cd["n"] == 2 and cd["n_with_return"] == 1
        assert cd["mean_market_path_return"] == 0.05

    def test_null_returns_preserved(self):
        r = reconcile_pairs([cand("p1")], [ctrl("p1")], [oc("p1")])
        out = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign")
        mp = out["rows"][0]["outcome"]["market_path_returns"]
        assert mp == {"1d": None, "3d": None, "5d": None, "10d": None, "20d": None}
        assert out["rows"][0]["outcome"]["stop_price"] is None


class TestPagination:
    def test_cursor_stable(self):
        c = [cand(f"p{i:03d}") for i in range(10)]
        k = [ctrl(f"p{i:03d}") for i in range(10)]
        o = [oc(f"p{i:03d}") for i in range(10)]
        r = reconcile_pairs(c, k, o)
        p1 = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign", cursor=0, limit=4)
        p2 = build_paired_comparison(r, experiment_code=EXP, campaign_scope="campaign", cursor=4, limit=4)
        assert len(p1["rows"]) == 4 and p1["next_cursor"] == 4
        assert [x["pair_id"] for x in p1["rows"]] != [x["pair_id"] for x in p2["rows"]]


class TestStatistics:
    def test_gated_below_min(self):
        for f in (sign_test, wilcoxon_signed_rank, paired_t_test, bootstrap_ci):
            assert f([0.1] * (MIN_INFERENTIAL_SAMPLE - 1)) is None

    def test_deterministic_bootstrap(self):
        diffs = [((-1) ** i) * 0.01 * (i + 1) for i in range(40)]
        assert bootstrap_ci(diffs, seed=0) == bootstrap_ci(diffs, seed=0)

    def test_sign_test_all_positive(self):
        s = sign_test([0.01] * 40)
        assert s["positives"] == 40 and s["p_value"] <= 0.001


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
    monkeypatch.setattr("app.workers.shadow.evidence_review.fetch_evidence_records", fake_records)

    async def fake_outcomes(filters):
        return outcomes
    monkeypatch.setattr(admin_mod, "_evidence_outcome_rows", fake_outcomes)


def _teardown():
    app.dependency_overrides.pop(get_worker_token, None)
    app.dependency_overrides.pop(get_db, None)


class TestHttp:
    def test_paired_comparison_v2(self, monkeypatch):
        _audit_on(monkeypatch, cand_records=[cand("p1")], ctrl_records=[ctrl("p1")],
                  outcomes=[oc("p1")])
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-cohort/paired-comparison?experiment_code=" + EXP)
            assert r.status_code == 200, r.json()
            assert r.json()["contract_version"] == "shadow_paired_comparison.v2"
            assert r.json()["reconciliation"]["valid_paired_rows"] == 1
            assert "token" not in r.text.lower() and "password" not in r.text.lower()
        finally:
            _teardown()

    def test_paired_metrics_v2(self, monkeypatch):
        _audit_on(monkeypatch, cand_records=[cand("p1", readiness="ready")],
                  ctrl_records=[ctrl("p1", verdict="ENTER")], outcomes=[oc("p1")])
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-cohort/paired-metrics?experiment_code=" + EXP)
            assert r.status_code == 200, r.json()
            b = r.json()
            assert b["contract_version"] == "shadow_paired_metrics.v2"
            assert b["population_counts"]["control_signal_population"] == 1
            assert "selection_conditioned_market_path_metrics" in b
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
