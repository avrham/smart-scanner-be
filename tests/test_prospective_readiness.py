"""Prospective history-readiness — pure builder + HTTP. No provider, local-only.

Proves the effective thresholds (175 daily / 26 weekly / 24 monthly candidate;
200 daily control), daily->weekly/monthly completed-period derivation, 4H
not-locally-available handling, deterministic hashing, and the read-only GET
endpoint (no provider construction, no writes).
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from main import app
from app.config import settings
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod
from app.prospective_readiness import (
    CANDIDATE_MIN_DAILY_BARS,
    CANDIDATE_MIN_MONTHLY_PERIODS,
    CANDIDATE_MIN_WEEKLY_PERIODS,
    CONTROL_MIN_DAILY_BARS,
    build_prospective_readiness,
    evaluate_symbol,
)


def hist(symbol, daily, month_groups, week_groups, four_h=None):
    return {"symbol": symbol, "daily_bars": daily, "month_groups": month_groups,
            "week_groups": week_groups, "oldest": "2024-01-01",
            "latest": "2026-06-11", "four_hour_bars": four_h}


class TestThresholds:
    def test_effective_constants(self):
        assert CANDIDATE_MIN_DAILY_BARS == 175
        assert CANDIDATE_MIN_WEEKLY_PERIODS == 26
        assert CANDIDATE_MIN_MONTHLY_PERIODS == 24
        assert CONTROL_MIN_DAILY_BARS == 200

    def test_ready_symbol(self):
        # 520 daily, 26 month-groups (→25 completed >=24), 54 week-groups (→53>=26)
        r = evaluate_symbol(hist("AAA", 520, 26, 54))
        assert r["candidate_daily_ready"] is True
        assert r["candidate_weekly_ready"] is True
        assert r["candidate_monthly_ready"] is True
        assert r["candidate_overall_ready"] is True
        assert r["control_ready"] is True
        # 4H not locally available → unknown + blocking reason (never silently ok)
        assert r["candidate_4h_ready"] is None
        assert "four_hour_history_not_locally_available" in r["blocking_reasons"]

    def test_monthly_gate_binds(self):
        # plenty of daily bars & weeks, but only 10 completed months -> not ready
        r = evaluate_symbol(hist("BBB", 300, 11, 54))
        assert r["candidate_daily_ready"] is True
        assert r["candidate_weekly_ready"] is True
        assert r["candidate_monthly_ready"] is False
        assert r["candidate_overall_ready"] is False
        assert any("insufficient_monthly_periods" in b for b in r["blocking_reasons"])

    def test_control_needs_200(self):
        r = evaluate_symbol(hist("CCC", 180, 26, 54))
        assert r["candidate_daily_ready"] is True   # >=175
        assert r["control_ready"] is False          # <200
        assert any("control_insufficient_daily_history" in b for b in r["blocking_reasons"])

    def test_completed_period_drops_trailing_partial(self):
        # 24 month-groups present → 23 completed (< 24) because trailing partial dropped
        r = evaluate_symbol(hist("DDD", 520, 24, 54))
        assert r["available_completed_monthly_periods"] == 23
        assert r["candidate_monthly_ready"] is False

    def test_missing_returns_never_zero_history_confusion(self):
        r = evaluate_symbol(hist("EEE", 0, 0, 0))
        assert r["available_completed_daily_bars"] == 0
        assert r["both_ready"] is False


class TestAggregate:
    def test_counts_and_distribution(self):
        rows = [hist("AAA", 520, 26, 54), hist("BBB", 100, 5, 20)]
        out = build_prospective_readiness(["AAA", "BBB"], rows)
        assert out["contract_version"] == "shadow_prospective_readiness.v1"
        assert out["universe_size"] == 2
        assert out["candidate_ready_count"] == 1
        assert out["control_ready_count"] == 1
        assert out["both_ready_count"] == 1
        assert out["not_ready_count"] == 1
        assert out["provider_called"] is False
        d = out["history_depth_distribution"]
        assert d["minimum"] == 100 and d["maximum"] == 520
        assert d["required_daily_threshold"] == 200

    def test_missing_symbol_treated_as_zero(self):
        out = build_prospective_readiness(["AAA", "ZZZ"], [hist("AAA", 520, 26, 54)])
        zzz = [s for s in out["symbols"] if s["symbol"] == "ZZZ"][0]
        assert zzz["available_completed_daily_bars"] == 0
        assert zzz["both_ready"] is False

    def test_deterministic_hashes(self):
        rows = [hist("AAA", 520, 26, 54), hist("BBB", 300, 26, 54)]
        a = build_prospective_readiness(["AAA", "BBB"], rows)
        b = build_prospective_readiness(["BBB", "AAA"], rows)  # order-insensitive
        assert a["universe_hash"] == b["universe_hash"]
        assert a["config_hash"] == b["config_hash"]
        assert a["readiness_manifest_hash"] == b["readiness_manifest_hash"]

    def test_universe_hash_changes_with_symbols(self):
        a = build_prospective_readiness(["AAA"], [hist("AAA", 520, 26, 54)])
        b = build_prospective_readiness(["BBB"], [hist("BBB", 520, 26, 54)])
        assert a["universe_hash"] != b["universe_hash"]


# --------------------------------------------------------------------------- #
class _ReadinessConn:
    def __init__(self, rows):
        self._rows = rows
        self.write_calls = []

    async def fetch(self, sql, *a):
        if "daily_bars" in sql:
            return self._rows
        return []

    async def execute(self, sql, *a):
        self.write_calls.append(sql)
        return "OK"

    async def fetchval(self, sql, *a):
        return None


def _audit_on(monkeypatch, conn):
    monkeypatch.setattr(admin_mod.settings, "AUDIT_ONLY_MODE", True)
    monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
    app.dependency_overrides[get_worker_token] = lambda: "t"
    app.dependency_overrides[get_db] = lambda: conn

    async def fake_access(db):
        return {"ready_for_closeout_audit": True, "reasons": [],
                "database_connection_mode": "audit_explicit"}
    monkeypatch.setattr(admin_mod, "_run_configured_access_check", fake_access)


def _teardown():
    app.dependency_overrides.pop(get_worker_token, None)
    app.dependency_overrides.pop(get_db, None)


class TestHttp:
    def test_readiness_v2_no_provider_no_write(self, monkeypatch):
        # market_bars_4h query returns [] (store present, empty) -> 4H not-ready
        conn = _ReadinessConn([hist("AAA", 520, 26, 54), hist("BBB", 100, 5, 20)])
        _audit_on(monkeypatch, conn)
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-cohort/prospective-readiness?symbols=AAA,BBB")
            assert r.status_code == 200, r.json()
            b = r.json()
            assert b["contract_version"] == "shadow_prospective_readiness.v2"
            assert b["provider_called"] is False
            assert b["universe_size"] == 2
            assert b["four_hour_local_store_available"] is True  # [] rows returned
            assert b["four_hour_ready_count"] == 0                # empty 4H store
            assert b["both_ready_count"] == 0                     # 4H not ready
            assert conn.write_calls == []                         # no write query
        finally:
            _teardown()

    def test_symbols_required(self, monkeypatch):
        _audit_on(monkeypatch, _ReadinessConn([]))
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-cohort/prospective-readiness")
            assert r.status_code == 422
        finally:
            _teardown()

    def test_symbol_cap(self, monkeypatch):
        _audit_on(monkeypatch, _ReadinessConn([]))
        try:
            syms = ",".join(f"S{i}" for i in range(101))
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/shadow-cohort/prospective-readiness?symbols=" + syms)
            assert r.status_code == 422
        finally:
            _teardown()
