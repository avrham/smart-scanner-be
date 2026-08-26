"""Smart Scanner product API (app/routers/scanner.py, app/scanner_view.py).

Covers latest-scan selection, the overview + symbol-detail contracts, the
no-signal / stale / not-ready / failed states, response schema stability,
and reachability under the read-only audit-only staging gate.
"""

import json
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from main import app
from app.deps import get_db
import app.routers.scanner as scanner_mod
import app.scanner_view as sv


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


# --------------------------------------------------------------------------- #
# Pure builder unit tests (app/scanner_view.py) — no DB, no HTTP.
# --------------------------------------------------------------------------- #

class TestClassifyScannerState:
    def test_no_campaign_yet(self):
        assert sv.classify_scanner_state(
            campaign_status=None, campaign_as_of_date=None,
            latest_completed_session="2026-08-25",
        ) == sv.SCANNER_STATE_NO_CAMPAIGN_YET

    def test_running(self):
        assert sv.classify_scanner_state(
            campaign_status="running", campaign_as_of_date=None,
            latest_completed_session="2026-08-25",
        ) == sv.SCANNER_STATE_RUNNING

    def test_failed(self):
        assert sv.classify_scanner_state(
            campaign_status="failed", campaign_as_of_date="2026-08-25",
            latest_completed_session="2026-08-25",
        ) == sv.SCANNER_STATE_FAILED

    def test_fresh_when_session_matches(self):
        assert sv.classify_scanner_state(
            campaign_status="completed", campaign_as_of_date="2026-08-25",
            latest_completed_session="2026-08-25",
        ) == sv.SCANNER_STATE_FRESH

    def test_stale_when_session_is_older(self):
        assert sv.classify_scanner_state(
            campaign_status="completed", campaign_as_of_date="2026-08-24",
            latest_completed_session="2026-08-25",
        ) == sv.SCANNER_STATE_STALE


class TestClassifySymbolState:
    def test_no_campaign_yet_is_not_ready(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_NO_CAMPAIGN_YET,
            has_candidate_result=False, candidate_verdict=None,
        ) == sv.SYMBOL_STATE_NOT_READY

    def test_running_is_not_ready(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_RUNNING,
            has_candidate_result=False, candidate_verdict=None,
        ) == sv.SYMBOL_STATE_NOT_READY

    def test_fresh_avoid_is_no_signal(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_FRESH,
            has_candidate_result=True, candidate_verdict="AVOID",
        ) == sv.SYMBOL_STATE_NO_SIGNAL

    def test_fresh_enter_is_valid_result(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_FRESH,
            has_candidate_result=True, candidate_verdict="ENTER",
        ) == sv.SYMBOL_STATE_VALID_RESULT

    def test_fresh_watch_is_valid_result(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_FRESH,
            has_candidate_result=True, candidate_verdict="WATCH",
        ) == sv.SYMBOL_STATE_VALID_RESULT

    def test_stale_scan_marks_symbol_stale(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_STALE,
            has_candidate_result=True, candidate_verdict="ENTER",
        ) == sv.SYMBOL_STATE_STALE

    def test_missing_evaluation_in_completed_run_is_not_ready(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_FRESH,
            has_candidate_result=False, candidate_verdict=None,
        ) == sv.SYMBOL_STATE_NOT_READY

    def test_failed_scan_without_result_is_failed(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_FAILED,
            has_candidate_result=False, candidate_verdict=None,
        ) == sv.SYMBOL_STATE_FAILED

    def test_failed_scan_with_partial_result_still_reported(self):
        assert sv.classify_symbol_state(
            scanner_state=sv.SCANNER_STATE_FAILED,
            has_candidate_result=True, candidate_verdict="ENTER",
        ) == sv.SYMBOL_STATE_VALID_RESULT


class TestBuildOverviewRow:
    def test_row_with_evidence(self):
        row = {
            "symbol": "AAPL", "candidate_verdict": "WATCH", "candidate_score": 0.7,
            "candidate_details": {
                "readiness": {"status": "ready"},
                "policy": {"setup_present": True, "trigger_confirmed": False,
                          "enter_eligible_without_rollout_gate": False,
                          "waiting_reasons": ["enter_disabled_shadow_only"]},
            },
            "control_verdict": "AVOID", "control_score": None,
        }
        out = sv.build_overview_row(row, scanner_state=sv.SCANNER_STATE_FRESH)
        assert out["symbol"] == "AAPL"
        assert out["symbol_state"] == sv.SYMBOL_STATE_VALID_RESULT
        assert out["agreement"] is False
        assert out["setup_present"] is True
        assert out["trigger_confirmed"] is False

    def test_row_without_candidate_evaluation(self):
        row = {"symbol": "MSFT", "candidate_verdict": None, "candidate_score": None,
               "candidate_details": None, "control_verdict": "AVOID", "control_score": 0.1}
        out = sv.build_overview_row(row, scanner_state=sv.SCANNER_STATE_FRESH)
        assert out["symbol_state"] == sv.SYMBOL_STATE_NOT_READY
        assert out["agreement"] is None
        assert out["setup_present"] is None


class TestSummarizeResults:
    def test_counts(self):
        rows = [
            {"symbol_state": sv.SYMBOL_STATE_VALID_RESULT},
            {"symbol_state": sv.SYMBOL_STATE_NO_SIGNAL},
            {"symbol_state": sv.SYMBOL_STATE_NO_SIGNAL},
        ]
        summary = sv.summarize_results(rows)
        assert summary["total"] == 3
        assert summary[sv.SYMBOL_STATE_VALID_RESULT] == 1
        assert summary[sv.SYMBOL_STATE_NO_SIGNAL] == 2
        assert summary[sv.SYMBOL_STATE_NOT_READY] == 0


# --------------------------------------------------------------------------- #
# HTTP-level tests (fake asyncpg connection; no real DB).
# --------------------------------------------------------------------------- #

RUN_ID = uuid4()

_CANDIDATE_DETAILS = {
    "readiness": {"status": "ready"},
    "policy": {"setup_present": True, "trigger_confirmed": True,
              "enter_eligible_without_rollout_gate": False,
              "waiting_reasons": ["enter_disabled_shadow_only"]},
    "four_hour_trigger": {"state": "confirmed"},
}


class FakeConn:
    """Routes fetch/fetchrow/fetchval calls by a distinctive SQL substring —
    same pattern as tests/test_history_warmup.py's _WarmupConn."""

    def __init__(self, *, campaign_row=None, result_rows=None, freshness_row=None,
                 symbol_row=None, daily_row=None, bar_rows=None, campaign_list_rows=None):
        self.campaign_row = campaign_row
        self.result_rows = result_rows or []
        self.freshness_row = freshness_row or {"oldest": None, "latest": None}
        self.symbol_row = symbol_row
        self.daily_row = daily_row or {"n": 0, "oldest": None, "latest": None}
        self.bar_rows = bar_rows or []
        self.campaign_list_rows = campaign_list_rows or []

    async def fetchrow(self, sql, *a):
        if "FROM strategy_shadow_runs" in sql:
            return self.campaign_row
        if "FROM daily_bars" in sql and "COUNT(*)::int AS n" in sql:
            return self.daily_row
        if "FROM daily_bars" in sql:
            return self.freshness_row
        if "FROM strategy_shadow_run_pairs rp" in sql and "p.symbol = $2" in sql:
            return self.symbol_row
        return None

    async def fetch(self, sql, *a):
        if "FROM strategy_shadow_run_pairs rp" in sql and "candidate_verdict" in sql:
            return self.result_rows
        if "trading_date, open, high, low, close, volume" in sql:
            return self.bar_rows
        if "telemetry->'pair_count'" in sql:
            return self.campaign_list_rows
        return []

    async def fetchval(self, sql, *a):
        return None


def _row(**kwargs):
    """dict subclass supporting both attribute-style asyncpg Record access
    (row['x']) and .get(...) the way real asyncpg Records do for __getitem__
    only — our production code only ever uses ['x'] / .get('x'), both of
    which a plain dict already satisfies."""
    return dict(kwargs)


def _use(conn):
    app.dependency_overrides[get_db] = lambda: conn


def _teardown():
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    _teardown()


class TestOverviewEndpoint:
    def test_no_campaign_yet(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        _use(FakeConn(campaign_row=None))
        resp = client.get("/api/scanner/overview")
        assert resp.status_code == 200
        body = resp.json()
        assert body["contract_version"] == sv.OVERVIEW_CONTRACT_VERSION
        assert body["scanner_state"] == sv.SCANNER_STATE_NO_CAMPAIGN_YET
        assert body["scan"] is None
        assert body["universe"] is None
        assert body["results"] == []
        assert body["results_summary"]["total"] == 0

    def test_fresh_campaign_with_results(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        campaign = _row(
            id=RUN_ID, experiment_code="wyckoff_v2_vs_baseline", experiment_version=1,
            status="completed", started_at=datetime(2026, 8, 25, 21, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 8, 25, 21, 5, tzinfo=timezone.utc), error_code=None,
            campaign_id=str(uuid4()), as_of_date="2026-08-25",
            requested_symbols=json.dumps(["AAPL", "MSFT"]),
        )
        results = [
            _row(symbol="AAPL", candidate_verdict="WATCH", candidate_score=0.7,
                 candidate_details=json.dumps(_CANDIDATE_DETAILS),
                 control_verdict="AVOID", control_score=None),
            _row(symbol="MSFT", candidate_verdict="AVOID", candidate_score=0.1,
                 candidate_details=json.dumps({"readiness": {"status": "ready"}, "policy": {}}),
                 control_verdict="AVOID", control_score=0.1),
        ]
        _use(FakeConn(campaign_row=campaign, result_rows=results,
                      freshness_row={"oldest": date(2026, 1, 2), "latest": date(2026, 8, 25)}))
        resp = client.get("/api/scanner/overview")
        assert resp.status_code == 200
        body = resp.json()
        assert body["scanner_state"] == sv.SCANNER_STATE_FRESH
        assert body["scan"]["scan_id"] == str(RUN_ID)
        assert body["scan"]["session_date"] == "2026-08-25"
        assert body["universe"]["symbol_count"] == 2
        assert body["data_freshness"]["latest_bar_date"] == "2026-08-25"
        by_symbol = {r["symbol"]: r for r in body["results"]}
        assert by_symbol["AAPL"]["symbol_state"] == sv.SYMBOL_STATE_VALID_RESULT
        assert by_symbol["MSFT"]["symbol_state"] == sv.SYMBOL_STATE_NO_SIGNAL
        assert body["results_summary"]["valid_result"] == 1
        assert body["results_summary"]["no_signal"] == 1
        assert body["strategy"]["allow_enter"] is False

    def test_stale_campaign(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        campaign = _row(
            id=RUN_ID, experiment_code="wyckoff_v2_vs_baseline", experiment_version=1,
            status="completed", started_at=datetime(2026, 8, 24, 21, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 8, 24, 21, 5, tzinfo=timezone.utc), error_code=None,
            campaign_id=str(uuid4()), as_of_date="2026-08-24",
            requested_symbols=json.dumps(["AAPL"]),
        )
        results = [_row(symbol="AAPL", candidate_verdict="ENTER", candidate_score=0.9,
                        candidate_details=json.dumps(_CANDIDATE_DETAILS),
                        control_verdict="ENTER", control_score=0.9)]
        _use(FakeConn(campaign_row=campaign, result_rows=results))
        body = client.get("/api/scanner/overview").json()
        assert body["scanner_state"] == sv.SCANNER_STATE_STALE
        assert body["results"][0]["symbol_state"] == sv.SYMBOL_STATE_STALE

    def test_failed_campaign(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        campaign = _row(
            id=RUN_ID, experiment_code="wyckoff_v2_vs_baseline", experiment_version=1,
            status="failed", started_at=datetime(2026, 8, 25, 21, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 8, 25, 21, 5, tzinfo=timezone.utc),
            error_code="campaign_count_mismatch",
            campaign_id=str(uuid4()), as_of_date="2026-08-25",
            requested_symbols=json.dumps(["AAPL"]),
        )
        _use(FakeConn(campaign_row=campaign, result_rows=[]))
        body = client.get("/api/scanner/overview").json()
        assert body["scanner_state"] == sv.SCANNER_STATE_FAILED
        assert body["results"] == []

    def test_session_query_param_is_forwarded(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        seen = {}

        async def fake_fetch_latest_campaign(db, *, session):
            seen["session"] = session
            return None

        monkeypatch.setattr(scanner_mod, "_fetch_latest_campaign", fake_fetch_latest_campaign)
        _use(FakeConn())
        client.get("/api/scanner/overview?session=2026-08-20")
        assert seen["session"] == "2026-08-20"


class TestSymbolDetailEndpoint:
    def _campaign(self, symbols=("AAPL",), status="completed", as_of="2026-08-25"):
        return _row(
            id=RUN_ID, experiment_code="wyckoff_v2_vs_baseline", experiment_version=1,
            status=status, started_at=datetime(2026, 8, 25, 21, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 8, 25, 21, 5, tzinfo=timezone.utc), error_code=None,
            campaign_id=str(uuid4()), as_of_date=as_of,
            requested_symbols=json.dumps(list(symbols)),
        )

    def test_valid_result_with_evidence(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        symbol_row = _row(
            candidate_verdict="ENTER", candidate_score=0.85, candidate_reason="setup+trigger",
            candidate_details=json.dumps(_CANDIDATE_DETAILS),
            control_verdict="AVOID", control_score=0.2, control_reason="below sma",
        )
        bars = [_row(trading_date=date(2026, 8, 25), open=1, high=2, low=0.5, close=1.5, volume=1000)]
        _use(FakeConn(campaign_row=self._campaign(), symbol_row=symbol_row,
                      daily_row={"n": 300, "oldest": date(2025, 1, 2), "latest": date(2026, 8, 25)},
                      bar_rows=bars))
        resp = client.get("/api/scanner/symbol?symbol=aapl")
        assert resp.status_code == 200
        body = resp.json()
        assert body["contract_version"] == sv.SYMBOL_DETAIL_CONTRACT_VERSION
        assert body["symbol"] == "AAPL"
        assert body["symbol_state"] == sv.SYMBOL_STATE_VALID_RESULT
        assert body["candidate"]["verdict"] == "ENTER"
        assert body["candidate"]["allow_enter"] is False
        assert body["candidate"]["evidence"]["trigger_confirmed"] is True
        assert body["control"]["verdict"] == "AVOID"
        assert body["readiness"]["daily_bar_count"] == 300
        assert len(body["recent_daily_bars"]) == 1
        assert body["recent_daily_bars"][0]["close"] == 1.5

    def test_unknown_symbol_is_404(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        _use(FakeConn(campaign_row=self._campaign(symbols=("AAPL",))))
        resp = client.get("/api/scanner/symbol?symbol=ZZZZ")
        assert resp.status_code == 404
        assert resp.json()["detail"]["error"] == "unknown_symbol"

    def test_no_campaign_available_is_404(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        _use(FakeConn(campaign_row=None))
        resp = client.get("/api/scanner/symbol?symbol=AAPL")
        assert resp.status_code == 404
        assert resp.json()["detail"]["error"] == "no_campaign_available"

    def test_no_signal_result(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        symbol_row = _row(
            candidate_verdict="AVOID", candidate_score=0.1, candidate_reason="no setup",
            candidate_details=json.dumps({"readiness": {"status": "ready"}, "policy": {}}),
            control_verdict="AVOID", control_score=0.1, control_reason="below sma",
        )
        _use(FakeConn(campaign_row=self._campaign(), symbol_row=symbol_row))
        body = client.get("/api/scanner/symbol?symbol=AAPL").json()
        assert body["symbol_state"] == sv.SYMBOL_STATE_NO_SIGNAL

    def test_missing_evaluation_is_not_ready(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        _use(FakeConn(campaign_row=self._campaign(), symbol_row=None))
        body = client.get("/api/scanner/symbol?symbol=AAPL").json()
        assert body["symbol_state"] == sv.SYMBOL_STATE_NOT_READY
        assert body["candidate"]["verdict"] is None
        assert body["candidate"]["evidence"] is None


class TestScansEndpoint:
    def test_list_shape(self, client):
        rows = [_row(id=RUN_ID, status="completed",
                     started_at=datetime(2026, 8, 25, 21, 0, tzinfo=timezone.utc),
                     finished_at=datetime(2026, 8, 25, 21, 5, tzinfo=timezone.utc),
                     as_of_date="2026-08-25", pair_count=json.dumps(25))]
        _use(FakeConn(campaign_list_rows=rows))
        resp = client.get("/api/scanner/scans")
        assert resp.status_code == 200
        body = resp.json()
        assert body["contract_version"] == sv.SCAN_LIST_CONTRACT_VERSION
        assert body["scans"][0]["scan_id"] == str(RUN_ID)
        assert body["scans"][0]["pair_count"] == 25
        # never leaks raw internal fields
        assert "provider" not in body["scans"][0]
        assert "requested_symbols" not in body["scans"][0]


# --------------------------------------------------------------------------- #
# Reachability under the read-only audit-only staging gate.
# --------------------------------------------------------------------------- #

class TestAuditOnlyReachability:
    def test_scanner_routes_allowlisted(self):
        from app.audit_mode import AUDIT_ONLY_ALLOWLIST, is_audit_route_allowed
        for path in ("/api/scanner/overview", "/api/scanner/symbol", "/api/scanner/scans"):
            assert path in AUDIT_ONLY_ALLOWLIST
            assert is_audit_route_allowed("GET", path) is True
            assert is_audit_route_allowed("POST", path) is False

    def test_overview_reachable_under_audit_only_mode(self, client, monkeypatch):
        from app.config import settings
        import main as main_mod
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", True)
        monkeypatch.setattr(main_mod.settings, "AUDIT_ONLY_MODE", True)
        _use(FakeConn(campaign_row=None))
        resp = client.get("/api/scanner/overview")
        assert resp.status_code == 200
        # a non-allowlisted route stays blocked
        resp2 = client.get("/api/admin/prospective/audit")
        assert resp2.status_code == 404
