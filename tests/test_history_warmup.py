"""History-warmup foundation + prospective-readiness v2 — unit + HTTP.

No Supabase / Massive. Proves: v2 four-state per-timeframe readiness (incl. 4H
unknown-vs-not-ready-vs-stale-vs-ready), 4H freshness, combined/manifest hashes,
history-warmup access-check privilege verdict, preflight (provider_called=false
+ budget estimate), HISTORY_WARMUP_ONLY_MODE allowlist + method gate, and that
no provider is ever constructed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from main import app
from app.config import settings, Settings
from app.deps import get_db, get_worker_token
import app.routers.admin as admin_mod
from app.history_warmup_mode import is_history_warmup_route_allowed
from app.history_warmup import evaluate_history_warmup_access
from app.prospective_readiness import (
    build_prospective_readiness_v2, evaluate_symbol_v2,
    STATE_READY, STATE_NOT_READY, STATE_STALE, STATE_UNKNOWN,
)

NOW = datetime(2026, 7, 29, 12, 0, 0, tzinfo=timezone.utc)


def daily(symbol, count, months, weeks, latest="2026-07-29"):
    return {"symbol": symbol, "daily_bars": count, "month_groups": months,
            "week_groups": weeks, "oldest": "2024-01-01", "latest": latest}


def fourh(symbol, completed, latest="2026-07-29"):
    return {"symbol": symbol, "completed_4h_bars": completed,
            "oldest_4h": "2026-05-01", "latest_4h": latest}


# --------------------------------------------------------------------------- #
class TestReadinessV2States:
    def test_all_ready(self):
        r = evaluate_symbol_v2(symbol="AAA", daily=daily("AAA", 520, 26, 54),
                               fourh=fourh("AAA", 12), now=NOW)
        assert r["daily"]["state"] == STATE_READY
        assert r["weekly"]["state"] == STATE_READY
        assert r["monthly"]["state"] == STATE_READY
        assert r["four_hour"]["state"] == STATE_READY
        assert r["control_state"] == STATE_READY
        assert r["candidate_overall_state"] == STATE_READY
        assert r["both_ready"] is True

    def test_4h_unknown_when_store_unavailable(self):
        r = evaluate_symbol_v2(symbol="AAA", daily=daily("AAA", 520, 26, 54),
                               fourh=None, now=NOW)  # None => store unavailable
        assert r["four_hour"]["state"] == STATE_UNKNOWN
        assert r["candidate_overall_state"] == STATE_UNKNOWN
        assert r["both_ready"] is False

    def test_4h_not_ready_when_insufficient(self):
        r = evaluate_symbol_v2(symbol="AAA", daily=daily("AAA", 520, 26, 54),
                               fourh=fourh("AAA", 3), now=NOW)
        assert r["four_hour"]["state"] == STATE_NOT_READY
        assert r["both_ready"] is False

    def test_4h_stale_when_old(self):
        # enough bars, but latest completed 4H bar is far in the past
        r = evaluate_symbol_v2(symbol="AAA", daily=daily("AAA", 520, 26, 54),
                               fourh=fourh("AAA", 20, latest="2026-05-01"), now=NOW)
        assert r["four_hour"]["state"] == STATE_STALE
        assert r["both_ready"] is False

    def test_daily_stale_makes_not_launch_ready(self):
        r = evaluate_symbol_v2(symbol="AAA", daily=daily("AAA", 520, 26, 54, latest="2026-01-01"),
                               fourh=fourh("AAA", 20, latest="2026-01-01"), now=NOW)
        assert r["daily"]["state"] == STATE_STALE
        assert r["both_ready"] is False

    def test_control_needs_200(self):
        r = evaluate_symbol_v2(symbol="AAA", daily=daily("AAA", 180, 26, 54),
                               fourh=fourh("AAA", 12), now=NOW)
        assert r["daily"]["state"] == STATE_READY       # >=175
        assert r["control_state"] == STATE_NOT_READY     # <200
        assert r["both_ready"] is False


class TestReadinessV2Aggregate:
    def test_store_unavailable_all_4h_unknown(self):
        out = build_prospective_readiness_v2(["AAA", "BBB"],
                                             [daily("AAA", 520, 26, 54), daily("BBB", 520, 26, 54)],
                                             None, now=NOW)  # fourh_rows None => unavailable
        assert out["contract_version"] == "shadow_prospective_readiness.v2"
        assert out["four_hour_local_store_available"] is False
        assert out["four_hour_unknown_count"] == 2
        assert out["four_hour_ready_count"] == 0
        assert out["both_ready_count"] == 0

    def test_store_empty_all_4h_not_ready(self):
        out = build_prospective_readiness_v2(["AAA"], [daily("AAA", 520, 26, 54)],
                                             [], now=NOW)  # store exists, no rows
        assert out["four_hour_local_store_available"] is True
        assert out["symbols"][0]["four_hour"]["state"] == STATE_NOT_READY

    def test_ready_and_hashes_deterministic(self):
        args = (["AAA", "BBB"],
                [daily("AAA", 520, 26, 54), daily("BBB", 520, 26, 54)],
                [fourh("AAA", 12), fourh("BBB", 12)])
        a = build_prospective_readiness_v2(*args, now=NOW)
        b = build_prospective_readiness_v2(args[0][::-1], args[1], args[2], now=NOW)
        assert a["both_ready_count"] == 2 and a["fully_launch_ready_count"] == 2
        assert a["universe_hash"] == b["universe_hash"]
        assert a["combined_readiness_manifest_hash"] == b["combined_readiness_manifest_hash"]
        assert a["four_hour_manifest_hash"] == b["four_hour_manifest_hash"]


class TestAccessCheckPure:
    def _privs(self, *, market_rw=True, daily_rw=True, forbidden_write=False, delete=False):
        base = {"SELECT": True, "INSERT": True, "UPDATE": True, "DELETE": delete}
        return {
            "public.market_bars_4h": {**base, "INSERT": market_rw, "UPDATE": market_rw},
            "public.history_warmup_runs": dict(base),
            "public.daily_bars": {**base, "INSERT": daily_rw, "UPDATE": daily_rw},
            "public.patterns": {"SELECT": True},
            "public.pattern_configs": {"SELECT": True},
            "public.strategy_shadow_runs": {"SELECT": False, "INSERT": forbidden_write, "UPDATE": False, "DELETE": False},
            "public.strategy_shadow_evaluations": {"INSERT": forbidden_write},
            "public.strategy_shadow_pairs": {"INSERT": forbidden_write},
            "public.strategy_shadow_pair_outcomes": {"INSERT": forbidden_write},
        }

    def _exists(self):
        return {r: True for r in (
            "public.market_bars_4h", "public.history_warmup_runs", "public.daily_bars",
            "public.patterns", "public.pattern_configs", "public.strategy_shadow_runs",
            "public.strategy_shadow_evaluations", "public.strategy_shadow_pairs",
            "public.strategy_shadow_pair_outcomes")}

    def test_ready(self):
        out = evaluate_history_warmup_access(
            database_identity="smart_scanner_history_warmer",
            expected_role="smart_scanner_history_warmer",
            history_warmup_only_mode=True, scheduler_enabled=False,
            provider_name="massive", provider_credential_configured=True,
            relation_privileges=self._privs(), relation_exists=self._exists())
        assert out["ready"] is True and out["reasons"] == []
        assert out["provider_constructed"] is False
        assert out["market_bars_4h_writable"] and out["daily_bars_writable"]
        assert out["campaign_writes_forbidden"] and out["delete_forbidden"]

    def test_forbidden_write_blocks(self):
        out = evaluate_history_warmup_access(
            database_identity="smart_scanner_history_warmer",
            expected_role="smart_scanner_history_warmer",
            history_warmup_only_mode=True, scheduler_enabled=False,
            provider_name="massive", provider_credential_configured=True,
            relation_privileges=self._privs(forbidden_write=True), relation_exists=self._exists())
        assert out["ready"] is False
        assert any("forbidden_write" in r for r in out["reasons"])
        assert out["campaign_writes_forbidden"] is False

    def test_delete_blocks(self):
        out = evaluate_history_warmup_access(
            database_identity="smart_scanner_history_warmer",
            expected_role="smart_scanner_history_warmer",
            history_warmup_only_mode=True, scheduler_enabled=False,
            provider_name="massive", provider_credential_configured=True,
            relation_privileges=self._privs(delete=True), relation_exists=self._exists())
        assert out["ready"] is False and out["delete_forbidden"] is False

    def test_wrong_identity_and_scheduler_block(self):
        out = evaluate_history_warmup_access(
            database_identity="postgres", expected_role="smart_scanner_history_warmer",
            history_warmup_only_mode=True, scheduler_enabled=True,
            provider_name="massive", provider_credential_configured=True,
            relation_privileges=self._privs(), relation_exists=self._exists())
        assert out["ready"] is False
        assert "database_identity_mismatch" in out["reasons"]
        assert "scheduler_enabled" in out["reasons"]


class TestModeAndAllowlist:
    def test_config_default_false(self):
        assert Settings.model_fields["HISTORY_WARMUP_ONLY_MODE"].default is False

    def test_allowlist_get_only(self):
        for route in ("/api/admin/history-warmup/access-check",
                      "/api/admin/history-warmup/preflight",
                      "/version", "/health"):
            assert is_history_warmup_route_allowed("GET", route) is True
            assert is_history_warmup_route_allowed("POST", route) is False
        # non-allowlisted (incl. any execute) is blocked
        assert is_history_warmup_route_allowed("GET", "/api/admin/shadow-cohort/closeout") is False
        assert is_history_warmup_route_allowed("POST", "/api/admin/history-warmup/execute") is False


# --------------------------------------------------------------------------- #
class _WarmupConn:
    """Fake conn answering privilege probes + local readiness queries."""
    def __init__(self, *, identity="smart_scanner_history_warmer", exists=None,
                 privs=None, daily_rows=None, fourh_rows=None):
        self.identity = identity
        self.exists = exists or {}
        self.privs = privs or {}
        self.daily_rows = daily_rows or []
        self.fourh_rows = fourh_rows if fourh_rows is not None else []
        self.write_calls = []

    async def fetchval(self, sql, *a):
        if "current_user" in sql:
            return self.identity
        if "to_regclass" in sql:
            return "oid" if self.exists.get(a[0]) else None
        return None

    async def fetchrow(self, sql, *a):
        if "has_table_privilege" in sql:
            p = self.privs.get(a[0], {})
            return {"s": p.get("SELECT", False), "i": p.get("INSERT", False),
                    "u": p.get("UPDATE", False), "d": p.get("DELETE", False)}
        return None

    async def fetch(self, sql, *a):
        if "daily_bars" in sql:
            return self.daily_rows
        if "market_bars_4h" in sql:
            return self.fourh_rows
        return []

    async def execute(self, sql, *a):
        self.write_calls.append(sql)
        return "OK"


def _warmup_on(monkeypatch, conn):
    for s in (settings, admin_mod.settings):
        monkeypatch.setattr(s, "HISTORY_WARMUP_ONLY_MODE", True)
        monkeypatch.setattr(s, "AUDIT_ONLY_MODE", False)
        monkeypatch.setattr(s, "MAINTENANCE_ONLY_MODE", False)
        monkeypatch.setattr(s, "ENABLE_SCHEDULER", False)  # warmup mode never schedules
    app.dependency_overrides[get_worker_token] = lambda: "t"
    app.dependency_overrides[get_db] = lambda: conn


def _teardown():
    app.dependency_overrides.pop(get_worker_token, None)
    app.dependency_overrides.pop(get_db, None)


class TestHttp:
    def test_access_check_ready_no_provider(self, monkeypatch):
        rels = ("public.market_bars_4h", "public.history_warmup_runs", "public.daily_bars",
                "public.patterns", "public.pattern_configs", "public.strategy_shadow_runs",
                "public.strategy_shadow_evaluations", "public.strategy_shadow_pairs",
                "public.strategy_shadow_pair_outcomes")
        exists = {r: True for r in rels}
        privs = {r: {"SELECT": True, "INSERT": False, "UPDATE": False, "DELETE": False} for r in rels}
        for r in ("public.market_bars_4h", "public.history_warmup_runs", "public.daily_bars"):
            privs[r] = {"SELECT": True, "INSERT": True, "UPDATE": True, "DELETE": False}
        conn = _WarmupConn(exists=exists, privs=privs)
        _warmup_on(monkeypatch, conn)
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/history-warmup/access-check")
            assert r.status_code == 200, r.json()
            b = r.json()
            assert b["access_check_contract_version"] == "history_warmup_access_check.v1"
            assert b["ready"] is True and b["provider_constructed"] is False
            assert b["campaign_writes_forbidden"] and b["delete_forbidden"]
            assert conn.write_calls == []
        finally:
            _teardown()

    def test_preflight_provider_estimate_no_call(self, monkeypatch):
        conn = _WarmupConn(daily_rows=[daily("AAA", 100, 5, 20)], fourh_rows=[])
        _warmup_on(monkeypatch, conn)
        try:
            r = TestClient(app, raise_server_exceptions=False).get(
                "/api/admin/history-warmup/preflight?symbols=AAA")
            assert r.status_code == 200, r.json()
            b = r.json()
            assert b["contract_version"] == "history_warmup_preflight.v1"
            assert b["provider_called"] is False and b["provider_constructed"] is False
            assert b["provider_budget_estimate"]["four_hour_symbols_requiring_warmup"] == 1
            assert b["readiness"]["contract_version"] == "shadow_prospective_readiness.v2"
            assert conn.write_calls == []
        finally:
            _teardown()

    def test_warmup_routes_404_when_mode_off(self, monkeypatch):
        # mode off -> _require_history_warmup_mode returns 404
        for s in (settings, admin_mod.settings):
            monkeypatch.setattr(s, "HISTORY_WARMUP_ONLY_MODE", False)
        app.dependency_overrides[get_worker_token] = lambda: "t"
        app.dependency_overrides[get_db] = lambda: _WarmupConn()
        try:
            c = TestClient(app, raise_server_exceptions=False)
            assert c.get("/api/admin/history-warmup/access-check").status_code == 404
            assert c.get("/api/admin/history-warmup/preflight?symbols=AAA").status_code == 404
        finally:
            _teardown()
