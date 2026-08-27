"""Smart Scanner product API (app/routers/scanner.py, app/scanner_view.py).

Covers latest-scan selection, the overview + symbol-detail contracts, the
no-signal / stale / not-ready / failed states, response schema stability,
and reachability under the read-only audit-only staging gate.
"""

import json
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from main import app
from app.deps import get_db
import app.news as nw
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
                 symbol_row=None, daily_row=None, bar_rows=None, campaign_list_rows=None,
                 universe_rows=None, catalyst_rows=None, catalyst_state=None,
                 catalyst_error=None, news_rows=None, news_error=None):
        self.campaign_row = campaign_row
        self.result_rows = result_rows or []
        self.freshness_row = freshness_row or {"oldest": None, "latest": None}
        self.symbol_row = symbol_row
        self.daily_row = daily_row or {"n": 0, "oldest": None, "latest": None}
        self.bar_rows = bar_rows or []
        self.campaign_list_rows = campaign_list_rows or []
        # Pair-derived universe (SELECT DISTINCT p.symbol ...). None = no pairs
        # persisted, which is what makes the route fall back to requested_symbols.
        self.universe_rows = universe_rows
        # Catalyst reads are additive and must never be able to fail the route.
        self.catalyst_rows = catalyst_rows or []
        self.catalyst_state = catalyst_state or []
        self.catalyst_error = catalyst_error
        self.catalyst_queries = 0
        # News reads are additive too, and fail INDEPENDENTLY of catalysts.
        self.news_rows = news_rows or []
        self.news_error = news_error
        self.news_queries = 0
        self.news_query_args = None
        self.freshness_symbols = None

    async def fetchrow(self, sql, *a):
        if "FROM strategy_shadow_runs" in sql:
            return self.campaign_row
        if "FROM daily_bars" in sql and "COUNT(*)::int AS n" in sql:
            return self.daily_row
        if "FROM daily_bars" in sql:
            self.freshness_symbols = a[0] if a else None
            return self.freshness_row
        if "FROM strategy_shadow_run_pairs rp" in sql and "p.symbol = $2" in sql:
            return self.symbol_row
        return None

    async def fetch(self, sql, *a):
        if "FROM public.company_news_symbols" in sql:
            self.news_queries += 1
            self.news_query_args = a
            if self.news_error:
                raise self.news_error
            return self.news_rows
        if "FROM symbol_catalyst_events" in sql:
            self.catalyst_queries += 1
            if self.catalyst_error:
                raise self.catalyst_error
            return self.catalyst_rows
        if "FROM catalyst_source_state" in sql:
            if self.catalyst_error:
                raise self.catalyst_error
            return self.catalyst_state
        if "SELECT DISTINCT p.symbol" in sql:
            return self.universe_rows or []
        if "FROM strategy_shadow_run_pairs rp" in sql and "candidate_verdict" in sql:
            return self.result_rows
        if "trading_date, open, high, low, close, volume" in sql:
            return self.bar_rows
        if "FROM strategy_shadow_runs r" in sql:
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
# Attention tiers, cross-arm relationship and gate progression.
#
# These encode the forensic finding (ops/analysis/scanner_signal_forensics.py)
# that the fields the API used to lead with do not differentiate real symbols:
# setup_present was True for 100/100 evaluations, and candidate_score is absent
# for 75/100 with overlapping WATCH/AVOID ranges among the rest.
# --------------------------------------------------------------------------- #

def _details(setup_state="valid", structure="recognized", reason="watch_setup_valid",
             readiness="ready", trigger_confirmed=False, trigger_state="missing",
             waiting=("entry_reference_unavailable",)):
    return {
        "setup_state": setup_state,
        "trigger_state": trigger_state,
        "readiness": {"status": readiness},
        "structure": {"state": structure},
        "policy": {
            "setup_state": setup_state,
            "trigger_confirmed": trigger_confirmed,
            "trigger_state": trigger_state,
            "reason_code": reason,
            "enter_eligible_without_rollout_gate": False,
            "waiting_reasons": list(waiting),
        },
        "four_hour_trigger": {"state": trigger_state},
    }


class TestClassifyAttention:
    def test_no_evaluation_is_not_ready(self):
        assert sv.classify_attention(
            has_candidate_result=False, candidate_verdict=None, setup_state=None,
            readiness_status=None, control_verdict="AVOID",
        ) == sv.ATTENTION_NOT_READY

    def test_unready_inputs_are_not_ready_even_with_a_verdict(self):
        assert sv.classify_attention(
            has_candidate_result=True, candidate_verdict="AVOID",
            setup_state="unknown", readiness_status="insufficient_history",
            control_verdict="AVOID",
        ) == sv.ATTENTION_NOT_READY

    def test_candidate_signal_verdict_is_high_attention(self):
        for verdict in ("WATCH", "ENTER"):
            assert sv.classify_attention(
                has_candidate_result=True, candidate_verdict=verdict,
                setup_state="valid", readiness_status="ready",
                control_verdict="AVOID",
            ) == sv.ATTENTION_HIGH

    def test_baseline_only_flag_is_developing(self):
        assert sv.classify_attention(
            has_candidate_result=True, candidate_verdict="AVOID",
            setup_state="unknown", readiness_status="ready",
            control_verdict="ENTER",
        ) == sv.ATTENTION_DEVELOPING

    def test_disagreement_outranks_an_invalid_structure(self):
        # order of the branches is the definition: disagreement is checked first
        assert sv.classify_attention(
            has_candidate_result=True, candidate_verdict="AVOID",
            setup_state="invalid", readiness_status="ready",
            control_verdict="ENTER",
        ) == sv.ATTENTION_DEVELOPING

    def test_structure_read_and_rejected_is_low_attention(self):
        assert sv.classify_attention(
            has_candidate_result=True, candidate_verdict="AVOID",
            setup_state="invalid", readiness_status="ready",
            control_verdict="AVOID",
        ) == sv.ATTENTION_LOW

    def test_structure_unreadable_is_no_read_not_low(self):
        # "no opinion" must never be presented as "negative opinion"
        assert sv.classify_attention(
            has_candidate_result=True, candidate_verdict="AVOID",
            setup_state="unknown", readiness_status="ready",
            control_verdict="AVOID",
        ) == sv.ATTENTION_NO_READ

    def test_every_tier_is_declared_and_ordered(self):
        assert set(sv.ATTENTION_ORDER) == set(sv.ATTENTION_TIERS)
        assert sv.ATTENTION_ORDER[sv.ATTENTION_HIGH] == 0
        assert (sv.ATTENTION_ORDER[sv.ATTENTION_HIGH]
                < sv.ATTENTION_ORDER[sv.ATTENTION_DEVELOPING]
                < sv.ATTENTION_ORDER[sv.ATTENTION_LOW]
                < sv.ATTENTION_ORDER[sv.ATTENTION_NO_READ]
                < sv.ATTENTION_ORDER[sv.ATTENTION_NOT_READY])


class TestClassifyCrossArm:
    def test_both_flagged(self):
        assert sv.classify_cross_arm(candidate_verdict="WATCH", control_verdict="ENTER") \
            == sv.CROSS_ARM_BOTH_FLAGGED

    def test_candidate_only(self):
        assert sv.classify_cross_arm(candidate_verdict="WATCH", control_verdict="AVOID") \
            == sv.CROSS_ARM_CANDIDATE_ONLY

    def test_baseline_only(self):
        assert sv.classify_cross_arm(candidate_verdict="AVOID", control_verdict="ENTER") \
            == sv.CROSS_ARM_BASELINE_ONLY

    def test_neither(self):
        assert sv.classify_cross_arm(candidate_verdict="AVOID", control_verdict="AVOID") \
            == sv.CROSS_ARM_NEITHER

    def test_missing_arm_is_not_comparable(self):
        assert sv.classify_cross_arm(candidate_verdict=None, control_verdict="AVOID") \
            == sv.CROSS_ARM_NOT_COMPARABLE

    def test_replaces_the_near_tautological_agreement_flag(self):
        # the two arms share only AVOID, so verdict equality could only ever mean
        # "both said AVOID" — cross_arm distinguishes the three other cases.
        assert sv.classify_cross_arm(candidate_verdict="WATCH", control_verdict="AVOID") \
            != sv.classify_cross_arm(candidate_verdict="AVOID", control_verdict="ENTER")


class TestGateProgress:
    def test_records_where_the_strategy_stopped(self):
        gates = sv.build_gate_progress(_details(), allow_enter=False)
        assert [g["gate"] for g in gates] == list(sv.GATE_ORDER)
        by_gate = {g["gate"]: g for g in gates}
        assert by_gate[sv.GATE_STRUCTURE]["status"] == sv.GATE_PASSED
        assert by_gate[sv.GATE_SETUP]["status"] == sv.GATE_PASSED
        assert by_gate[sv.GATE_TRIGGER]["status"] == sv.GATE_BLOCKED
        assert by_gate[sv.GATE_TRIGGER]["code"] == "entry_reference_unavailable"
        assert by_gate[sv.GATE_ROLLOUT]["status"] == sv.GATE_BLOCKED
        assert by_gate[sv.GATE_ROLLOUT]["code"] == "enter_disabled_shadow_only"

    def test_unreadable_structure_is_unknown_not_blocked(self):
        gates = sv.build_gate_progress(
            _details(setup_state="unknown", structure="unknown",
                     reason="unknown_structure", waiting=()),
            allow_enter=False)
        by_gate = {g["gate"]: g for g in gates}
        assert by_gate[sv.GATE_STRUCTURE]["status"] == sv.GATE_UNKNOWN
        assert by_gate[sv.GATE_SETUP]["status"] == sv.GATE_UNKNOWN
        assert by_gate[sv.GATE_STRUCTURE]["code"] == "unknown_structure"

    def test_rejected_structure_is_blocked(self):
        gates = sv.build_gate_progress(
            _details(setup_state="invalid", structure="ambiguous",
                     reason="ambiguous_structure", waiting=()),
            allow_enter=False)
        by_gate = {g["gate"]: g for g in gates}
        assert by_gate[sv.GATE_STRUCTURE]["status"] == sv.GATE_BLOCKED
        assert by_gate[sv.GATE_SETUP]["status"] == sv.GATE_BLOCKED

    def test_no_evidence_yields_no_progress(self):
        assert sv.build_gate_progress(None, allow_enter=False) is None
        assert sv.build_blockers(None, allow_enter=False) == []

    def test_blockers_are_the_unpassed_gates_in_order(self):
        blockers = sv.build_blockers(_details(), allow_enter=False)
        assert [b["gate"] for b in blockers] == [sv.GATE_TRIGGER, sv.GATE_ROLLOUT]


class TestAttentionOrderingAndSummary:
    def _row(self, symbol, attention, setup_state, score=None):
        return {"symbol": symbol, "attention": attention,
                "setup_state": setup_state, "candidate_score": score}

    def test_orders_by_tier_then_structure_then_symbol(self):
        rows = [
            self._row("ZZZZ", sv.ATTENTION_NO_READ, "unknown"),
            self._row("BBBB", sv.ATTENTION_HIGH, "valid"),
            self._row("AAAA", sv.ATTENTION_HIGH, "valid"),
            self._row("CCCC", sv.ATTENTION_DEVELOPING, "invalid"),
            self._row("DDDD", sv.ATTENTION_NOT_READY, None),
        ]
        assert [r["symbol"] for r in sorted(rows, key=sv.attention_sort_key)] == [
            "AAAA", "BBBB", "CCCC", "ZZZZ", "DDDD"]

    def test_ordering_ignores_the_candidate_score(self):
        # a high score must never lift a lower tier above a higher one
        rows = [
            self._row("LOWTIER", sv.ATTENTION_LOW, "invalid", score=0.99),
            self._row("HIGHTIER", sv.ATTENTION_HIGH, "valid", score=0.01),
        ]
        assert [r["symbol"] for r in sorted(rows, key=sv.attention_sort_key)] == [
            "HIGHTIER", "LOWTIER"]

    def test_summary_counts_every_tier(self):
        rows = [
            self._row("A", sv.ATTENTION_HIGH, "valid"),
            self._row("B", sv.ATTENTION_HIGH, "valid"),
            self._row("C", sv.ATTENTION_NO_READ, "unknown"),
        ]
        summary = sv.summarize_attention(rows)
        assert summary["total"] == 3
        assert summary[sv.ATTENTION_HIGH] == 2
        assert summary[sv.ATTENTION_NO_READ] == 1
        assert summary[sv.ATTENTION_DEVELOPING] == 0
        assert set(summary) == set(sv.ATTENTION_TIERS) | {"total"}


class TestOverviewRowProductFields:
    def test_exposes_the_structural_read_not_just_the_boolean(self):
        row = {
            "symbol": "AAPL", "candidate_verdict": "WATCH", "candidate_score": 0.43,
            "candidate_details": _details(),
            "control_verdict": "AVOID", "control_score": 0.0,
        }
        out = sv.build_overview_row(row, scanner_state=sv.SCANNER_STATE_FRESH)
        assert out["attention"] == sv.ATTENTION_HIGH
        assert out["setup_state"] == "valid"
        assert out["structure_state"] == "recognized"
        assert out["reason_code"] == "watch_setup_valid"
        assert out["cross_arm"] == sv.CROSS_ARM_CANDIDATE_ONLY

    def test_setup_present_is_retained_but_is_true_for_every_state(self):
        # documents exactly why the UI must not lead with it
        for state in ("valid", "invalid", "unknown"):
            row = {"symbol": "X", "candidate_verdict": "AVOID", "candidate_score": None,
                   "candidate_details": _details(setup_state=state),
                   "control_verdict": "AVOID", "control_score": 0.0}
            out = sv.build_overview_row(row, scanner_state=sv.SCANNER_STATE_FRESH)
            assert out["setup_present"] is True
            assert out["setup_state"] == state

    def test_row_without_evaluation_is_not_ready(self):
        row = {"symbol": "PLTR", "candidate_verdict": None, "candidate_score": None,
               "candidate_details": None, "control_verdict": "AVOID", "control_score": 0.1}
        out = sv.build_overview_row(row, scanner_state=sv.SCANNER_STATE_FRESH)
        assert out["attention"] == sv.ATTENTION_NOT_READY
        assert out["setup_state"] is None
        assert out["cross_arm"] == sv.CROSS_ARM_NOT_COMPARABLE

    def test_stale_scan_keeps_the_attention_tier_informative(self):
        # symbol_state collapses to `stale`, attention must NOT
        row = {"symbol": "AAPL", "candidate_verdict": "WATCH", "candidate_score": 0.43,
               "candidate_details": _details(),
               "control_verdict": "AVOID", "control_score": 0.0}
        out = sv.build_overview_row(row, scanner_state=sv.SCANNER_STATE_STALE)
        assert out["symbol_state"] == sv.SYMBOL_STATE_STALE
        assert out["attention"] == sv.ATTENTION_HIGH


# --------------------------------------------------------------------------- #
# Regression: the persistence shape the DURABLE WORKER actually writes.
#
# app/workers/shadow/runner.py always writes the session at the telemetry ROOT
# (`telemetry["as_of_date"]`) and merges `telemetry.campaign` on top as operator
# metadata, and the durable worker runs ONE symbol per job against a single
# shared campaign run — so that run's `requested_symbols` ends up holding only
# the last job's single symbol while its `strategy_shadow_run_pairs` hold the
# real universe. Reading `telemetry->'campaign'->>'as_of_date'` or trusting
# `requested_symbols` therefore yields a null session and a 1-symbol universe
# against real pipeline data.
# --------------------------------------------------------------------------- #

DURABLE_UNIVERSE = [
    "AAPL", "AMD", "AMZN", "AVGO", "BAC", "CAT", "COST", "CRM", "CVX", "GE",
    "GOOGL", "GS", "HD", "JNJ", "JPM", "LLY", "META", "MSFT", "NFLX", "NVDA",
    "ORCL", "TSLA", "UNH", "WMT", "XOM",
]


class _RecordingConn(FakeConn):
    """FakeConn that also records every SQL string it was asked to run."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.sql_seen = []

    async def fetchrow(self, sql, *a):
        self.sql_seen.append(sql)
        return await super().fetchrow(sql, *a)

    async def fetch(self, sql, *a):
        self.sql_seen.append(sql)
        return await super().fetch(sql, *a)


def _durable_campaign(**over):
    """A run row shaped the way the durable worker leaves it: campaign block
    carrying per-symbol progress metadata, requested_symbols holding a single
    symbol, and the real session date only at the telemetry root."""
    row = dict(
        id=RUN_ID, experiment_code="wyckoff_v2_vs_baseline", experiment_version=1,
        status="completed", started_at=datetime(2026, 8, 26, 9, 12, tzinfo=timezone.utc),
        finished_at=datetime(2026, 8, 26, 11, 23, tzinfo=timezone.utc), error_code=None,
        campaign_id=str(uuid4()),
        # what the COALESCE in the route resolves to for this shape
        as_of_date="2026-08-25",
        requested_symbols=json.dumps(["AAPL"]),
    )
    row.update(over)
    return row


def _universe_rows(symbols=None):
    return [_row(symbol=s) for s in (symbols if symbols is not None else DURABLE_UNIVERSE)]


class TestDurableWorkerPersistenceShape:
    def test_as_of_date_sql_coalesces_nested_over_root(self):
        sql = scanner_mod._as_of_date_sql()
        assert "telemetry->'campaign'->>'as_of_date'" in sql
        assert "telemetry->>'as_of_date'" in sql
        assert sql.startswith("COALESCE(")

    def test_as_of_date_sql_supports_a_table_alias(self):
        sql = scanner_mod._as_of_date_sql("r")
        assert "r.telemetry->'campaign'->>'as_of_date'" in sql
        assert "r.telemetry->>'as_of_date'" in sql

    def test_latest_campaign_query_reads_both_session_paths(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        conn = _RecordingConn(campaign_row=None)
        _use(conn)
        client.get("/api/scanner/overview?session=2026-08-25")
        run_sql = [s for s in conn.sql_seen if "FROM strategy_shadow_runs" in s]
        assert run_sql, "the overview must query strategy_shadow_runs"
        # both the projection and the session filter must use the coalesced path
        assert run_sql[0].count("telemetry->>'as_of_date'") >= 2

    def test_universe_comes_from_persisted_pairs_not_requested_symbols(
        self, client, monkeypatch
    ):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        _use(FakeConn(campaign_row=_durable_campaign(),
                      universe_rows=_universe_rows(),
                      freshness_row={"oldest": date(2025, 1, 2), "latest": date(2026, 8, 25)}))
        body = client.get("/api/scanner/overview").json()
        assert body["scanner_state"] == sv.SCANNER_STATE_FRESH
        assert body["scan"]["session_date"] == "2026-08-25"
        assert body["universe"]["symbol_count"] == 25
        assert body["universe"]["symbols"] == DURABLE_UNIVERSE
        # never the stale single-symbol requested_symbols value
        assert body["universe"]["symbols"] != ["AAPL"]

    def test_data_freshness_uses_the_pair_derived_universe(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        conn = FakeConn(campaign_row=_durable_campaign(), universe_rows=_universe_rows())
        _use(conn)
        client.get("/api/scanner/overview")
        assert conn.freshness_symbols == DURABLE_UNIVERSE

    def test_universe_falls_back_to_requested_when_no_pairs_persisted(
        self, client, monkeypatch
    ):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        _use(FakeConn(campaign_row=_durable_campaign(status="running"),
                      universe_rows=[]))
        body = client.get("/api/scanner/overview").json()
        assert body["scanner_state"] == sv.SCANNER_STATE_RUNNING
        assert body["universe"]["symbols"] == ["AAPL"]

    def test_symbol_detail_accepts_a_symbol_known_only_from_pairs(
        self, client, monkeypatch
    ):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        symbol_row = _row(
            candidate_verdict="WATCH", candidate_score=0.7, candidate_reason="watch_setup_valid",
            candidate_details=json.dumps(_CANDIDATE_DETAILS),
            control_verdict="AVOID", control_score=0.2, control_reason="below sma",
        )
        _use(FakeConn(campaign_row=_durable_campaign(), universe_rows=_universe_rows(),
                      symbol_row=symbol_row,
                      daily_row={"n": 521, "oldest": date(2024, 8, 1), "latest": date(2026, 8, 25)}))
        # TSLA is absent from requested_symbols (["AAPL"]) but present in the pairs
        resp = client.get("/api/scanner/symbol?symbol=TSLA")
        assert resp.status_code == 200
        body = resp.json()
        assert body["symbol"] == "TSLA"
        assert body["scan"]["session_date"] == "2026-08-25"
        assert body["symbol_state"] == sv.SYMBOL_STATE_VALID_RESULT

    def test_symbol_outside_the_pair_universe_is_still_404(self, client, monkeypatch):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                             lambda now: date(2026, 8, 25))
        _use(FakeConn(campaign_row=_durable_campaign(), universe_rows=_universe_rows()))
        resp = client.get("/api/scanner/symbol?symbol=ZZZZ")
        assert resp.status_code == 404
        assert resp.json()["detail"]["error"] == "unknown_symbol"

    def test_scan_list_counts_real_pairs_and_reads_root_session(self, client):
        rows = [_row(id=RUN_ID, status="completed",
                     started_at=datetime(2026, 8, 26, 9, 12, tzinfo=timezone.utc),
                     finished_at=datetime(2026, 8, 26, 11, 23, tzinfo=timezone.utc),
                     as_of_date="2026-08-25", pair_count=25)]
        _use(FakeConn(campaign_list_rows=rows))
        body = client.get("/api/scanner/scans").json()
        assert body["scans"][0]["session_date"] == "2026-08-25"
        assert body["scans"][0]["pair_count"] == 25


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


# --------------------------------------------------------------------------- #
# Catalyst context on the Product API.
#
# The route's obligations are narrow and testable: expose the context, batch the
# read, and NEVER let a catalyst problem take the scanner down with it.
# --------------------------------------------------------------------------- #

_CATALYST_CAMPAIGN = _row(
    id=RUN_ID, experiment_code="wyckoff_v2_vs_baseline", experiment_version=1,
    status="completed", started_at=datetime(2026, 8, 25, 21, 0, tzinfo=timezone.utc),
    finished_at=datetime(2026, 8, 25, 21, 5, tzinfo=timezone.utc), error_code=None,
    campaign_id="c0ffee00-0000-4000-8000-000000000001", as_of_date="2026-08-25",
    requested_symbols=json.dumps(["AAPL", "MSFT"]),
)

_CATALYST_RESULTS = [
    _row(symbol="AAPL", candidate_verdict="WATCH", candidate_score=0.7,
         candidate_details=json.dumps(_CANDIDATE_DETAILS),
         control_verdict="AVOID", control_score=None),
    _row(symbol="MSFT", candidate_verdict="AVOID", candidate_score=0.1,
         candidate_details=json.dumps({"readiness": {"status": "ready"}, "policy": {}}),
         control_verdict="AVOID", control_score=0.1),
]


def _catalyst_event(symbol="AAPL", event_type="earnings", event_date=date(2026, 8, 26),
                    **over):
    row = {
        "symbol": symbol, "event_type": event_type, "event_date": event_date,
        "session_timing": "after_market", "certainty": "confirmed",
        "fiscal_period": "Q3", "fiscal_year": "2026",
        "source": "provider_earnings_calendar", "source_reference": "ref-1",
        "observed_at": datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
    }
    row.update(over)
    return _row(**row)


def _source_state(source, status="ok", *, age_hours=1.0, **over):
    """A source state relative to the REAL clock — freshness is measured against
    `now`, so a fixed past timestamp would silently read as stale."""
    moment = datetime.now(timezone.utc) - timedelta(hours=age_hours)
    row = {
        "source": source, "status": status,
        "last_refresh_at": moment,
        "last_success_at": moment if status == "ok" else None,
        "symbols_covered": 25, "events_upserted": 4, "detail": None,
    }
    row.update(over)
    return _row(**row)


def _catalyst_conn(**over):
    kwargs = dict(campaign_row=_CATALYST_CAMPAIGN, result_rows=_CATALYST_RESULTS,
                  freshness_row={"oldest": date(2026, 1, 2), "latest": date(2026, 8, 25)})
    kwargs.update(over)
    return FakeConn(**kwargs)


class TestOverviewCatalystContext:
    def _get(self, client, monkeypatch, conn):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                            lambda now: date(2026, 8, 25))
        _use(conn)
        resp = client.get("/api/scanner/overview")
        assert resp.status_code == 200
        return resp.json()

    def test_every_row_carries_a_compact_catalyst_block(self, client, monkeypatch):
        body = self._get(client, monkeypatch, _catalyst_conn(
            catalyst_rows=[_catalyst_event()],
            catalyst_state=[_source_state("provider_earnings_calendar"),
                            _source_state("provider_financial_report_filings")]))
        for row in body["results"]:
            # The earnings half of the row block is byte-for-byte what Catalyst
            # V1 shipped. `news` is a SIBLING key added by News V1 — the only
            # thing that grew — and it is asserted separately, in
            # TestOverviewNewsContext.
            assert set(row["catalyst_context"]) - {"news"} == {
                "earnings_status", "earnings_proximity", "earnings_sessions_until",
                "earnings_timing", "earnings_certainty", "earnings_notable",
                "last_report_proximity", "last_report_sessions_until",
                "last_report_notable"}

    def test_an_event_reaches_only_the_symbol_it_belongs_to(self, client, monkeypatch):
        body = self._get(client, monkeypatch, _catalyst_conn(
            catalyst_rows=[_catalyst_event(symbol="AAPL")],
            catalyst_state=[_source_state("provider_earnings_calendar"),
                            _source_state("provider_financial_report_filings")]))
        by_symbol = {r["symbol"]: r["catalyst_context"] for r in body["results"]}
        assert by_symbol["AAPL"]["earnings_notable"] is True
        assert by_symbol["MSFT"]["earnings_notable"] is False
        assert by_symbol["MSFT"]["earnings_proximity"] == "none_known"

    def test_the_universe_is_read_in_one_query_not_one_per_symbol(self, client, monkeypatch):
        conn = _catalyst_conn(
            catalyst_rows=[_catalyst_event()],
            catalyst_state=[_source_state("provider_earnings_calendar")])
        self._get(client, monkeypatch, conn)
        assert conn.catalyst_queries == 1

    def test_per_source_freshness_is_reported_separately(self, client, monkeypatch):
        body = self._get(client, monkeypatch, _catalyst_conn(
            catalyst_state=[
                _source_state("provider_earnings_calendar", status="unavailable",
                              detail="provider_not_entitled: HTTP 403"),
                _source_state("provider_financial_report_filings"),
            ]))
        sources = body["catalyst_sources"]
        assert sources["earnings"]["status"] == "unavailable"
        assert sources["earnings"]["reason"] == "source_unavailable"
        assert sources["financial_reports"]["status"] == "available"

    def test_a_source_that_never_ran_is_not_reported_as_no_events(self, client, monkeypatch):
        body = self._get(client, monkeypatch, _catalyst_conn(catalyst_state=[]))
        assert body["catalyst_sources"]["earnings"]["reason"] == "never_refreshed"
        for row in body["results"]:
            assert row["catalyst_context"]["earnings_status"] == "unavailable"

    def test_a_catalyst_failure_never_takes_the_scanner_down(self, client, monkeypatch):
        body = self._get(client, monkeypatch, _catalyst_conn(
            catalyst_error=RuntimeError("catalyst table is unreadable")))
        # The scan itself is unaffected...
        assert body["scanner_state"] == sv.SCANNER_STATE_FRESH
        assert len(body["results"]) == 2
        assert body["results"][0]["candidate_verdict"] is not None
        # ...and every catalyst field degrades to an honest "unavailable".
        assert body["catalyst_sources"]["earnings"]["status"] == "unavailable"
        for row in body["results"]:
            assert row["catalyst_context"]["earnings_status"] == "unavailable"
            assert row["catalyst_context"]["earnings_notable"] is False

    def test_attention_and_ordering_are_untouched_by_catalysts(self, client, monkeypatch):
        with_events = self._get(client, monkeypatch, _catalyst_conn(
            catalyst_rows=[_catalyst_event()],
            catalyst_state=[_source_state("provider_earnings_calendar")]))
        without = self._get(client, monkeypatch, _catalyst_conn(
            catalyst_error=RuntimeError("unreadable")))

        def decisions(body):
            return [{k: r[k] for k in
                     ("symbol", "attention", "candidate_verdict", "candidate_score",
                      "cross_arm", "reason_code", "setup_state")}
                    for r in body["results"]]

        assert decisions(with_events) == decisions(without)
        assert with_events["attention_summary"] == without["attention_summary"]


class TestSymbolDetailCatalystContext:
    def _get(self, client, monkeypatch, conn, symbol="AAPL"):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                            lambda now: date(2026, 8, 25))
        _use(conn)
        resp = client.get(f"/api/scanner/symbol?symbol={symbol}")
        assert resp.status_code == 200
        return resp.json()

    def _conn(self, **over):
        return _catalyst_conn(
            symbol_row=_row(
                candidate_verdict="WATCH", candidate_score=0.7,
                candidate_reason="setup valid",
                candidate_details=json.dumps(_CANDIDATE_DETAILS),
                control_verdict="AVOID", control_score=None, control_reason=None),
            daily_row={"n": 300, "oldest": date(2025, 1, 2),
                       "latest": date(2026, 8, 25)},
            **over)

    def test_the_detail_carries_full_provenance(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(
            catalyst_rows=[_catalyst_event()],
            catalyst_state=[_source_state("provider_earnings_calendar"),
                            _source_state("provider_financial_report_filings")]))
        earnings = body["catalyst_context"]["earnings"]
        assert body["catalyst_context"]["contract_version"] == \
            "smart_scanner_catalyst_context.v1"
        assert earnings["source"] == "provider_earnings_calendar"
        assert earnings["source_reference"] == "ref-1"
        assert earnings["observed_at"] is not None
        assert earnings["fiscal_period"] == "Q3"

    def test_earnings_and_filings_are_separate_blocks(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(
            catalyst_rows=[
                _catalyst_event(event_type="earnings", event_date=date(2026, 8, 26)),
                _catalyst_event(event_type="financial_report_filing",
                                event_date=date(2026, 8, 3), certainty="filed"),
            ],
            catalyst_state=[_source_state("provider_earnings_calendar"),
                            _source_state("provider_financial_report_filings")]))
        ctx = body["catalyst_context"]
        assert ctx["earnings"]["event_date"] == "2026-08-26"
        assert ctx["last_financial_report"]["event_date"] == "2026-08-03"
        assert ctx["last_financial_report"]["certainty"] == "filed"

    def test_a_catalyst_failure_never_takes_the_detail_down(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(
            catalyst_error=RuntimeError("unreadable")))
        assert body["candidate"]["verdict"] == "WATCH"
        assert body["catalyst_context"]["earnings"]["status"] == "unavailable"
        assert body["catalyst_context"]["earnings"]["notable"] is False


# =========================================================================== #
# News / Company Catalyst Context V1
# =========================================================================== #

def _news_row(**over):
    """One persisted news row as the Product API's join returns it."""
    row = {
        "symbol": "AAPL",
        "relevance": "primary",
        "published_at": datetime(2026, 8, 25, 14, 30, tzinfo=timezone.utc),
        "title": "Apple unveils a new Mac lineup",
        "title_normalized": "apple unveils a new mac lineup",
        "publisher": "Reuters",
        "article_url": "https://reuters.com/tech/apple-macs",
        "category": "product_announcement",
        "category_source": "derived_title",
        "scope": "company_specific",
        "ticker_breadth": 1,
    }
    row.update(over)
    return _row(**row)


class TestOverviewNewsContext:
    def _get(self, client, monkeypatch, conn):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                            lambda now: date(2026, 8, 25))
        _use(conn)
        resp = client.get("/api/scanner/overview")
        assert resp.status_code == 200
        return resp.json()

    def _conn(self, **over):
        base = dict(catalyst_state=[_source_state("provider_earnings_calendar"),
                                    _source_state("provider_financial_report_filings"),
                                    _source_state("provider_company_news")])
        base.update(over)
        return _catalyst_conn(**base)

    def test_every_row_carries_counts_not_a_feed(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(news_rows=[_news_row()]))
        for row in body["results"]:
            news = row["catalyst_context"]["news"]
            assert set(news) == {"status", "reason", "notable_count",
                                 "in_window_count", "top_category",
                                 "latest_published_at", "latest_proximity",
                                 "latest_headline"}
            assert "items" not in news

    def test_an_article_reaches_only_the_symbol_it_is_linked_to(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(
            news_rows=[_news_row(symbol="AAPL")]))
        by_symbol = {r["symbol"]: r["catalyst_context"]["news"]
                     for r in body["results"]}
        assert by_symbol["AAPL"]["notable_count"] == 1
        assert by_symbol["MSFT"]["notable_count"] == 0
        assert by_symbol["MSFT"]["latest_headline"] is None

    def test_the_whole_universe_is_read_in_one_query_not_one_per_symbol(
            self, client, monkeypatch):
        conn = self._conn(news_rows=[_news_row()])
        self._get(client, monkeypatch, conn)
        assert conn.news_queries == 1

    def test_the_query_is_bounded_by_the_session_close(self, client, monkeypatch):
        conn = self._conn(news_rows=[_news_row()])
        self._get(client, monkeypatch, conn)
        symbols, lower, upper = conn.news_query_args
        assert upper == nw.session_close_utc(date(2026, 8, 25))
        assert lower < upper
        assert set(symbols) == {"AAPL", "MSFT"}

    def test_a_market_wide_roundup_never_makes_a_row_speak(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(news_rows=[
            _news_row(title="5 stocks Berkshire owns", relevance="mentioned",
                      scope="market_wide", ticker_breadth=25)]))
        news = {r["symbol"]: r["catalyst_context"]["news"]
                for r in body["results"]}["AAPL"]
        assert news["in_window_count"] == 1     # stored and counted...
        assert news["notable_count"] == 0       # ...but silent

    def test_news_freshness_is_reported_as_its_own_source(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn())
        assert body["catalyst_sources"]["news"]["status"] == "available"

    def test_a_source_that_never_ran_is_not_reported_as_no_news(self, client, monkeypatch):
        body = self._get(client, monkeypatch, _catalyst_conn(catalyst_state=[]))
        assert body["catalyst_sources"]["news"]["reason"] == "never_refreshed"
        for row in body["results"]:
            assert row["catalyst_context"]["news"]["status"] == "unavailable"

    def test_a_news_failure_never_takes_the_scanner_down(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(
            news_error=RuntimeError("news tables are unreadable")))
        assert body["scanner_state"] == sv.SCANNER_STATE_FRESH
        assert len(body["results"]) == 2
        assert body["results"][0]["candidate_verdict"] is not None
        assert body["catalyst_sources"]["news"]["status"] == "unavailable"
        for row in body["results"]:
            assert row["catalyst_context"]["news"]["notable_count"] == 0

    def test_a_news_failure_leaves_the_earnings_dimension_intact(self, client, monkeypatch):
        # The two dimensions are wrapped separately on purpose.
        body = self._get(client, monkeypatch, self._conn(
            catalyst_rows=[_catalyst_event()],
            news_error=RuntimeError("news tables are unreadable")))
        assert body["catalyst_sources"]["earnings"]["status"] == "available"
        assert body["results"][0]["catalyst_context"]["earnings_notable"] is True
        assert body["catalyst_sources"]["news"]["status"] == "unavailable"

    def test_a_catalyst_failure_leaves_the_news_dimension_intact(self, client, monkeypatch):
        conn = _catalyst_conn(catalyst_error=RuntimeError("catalyst unreadable"))
        conn.news_rows = [_news_row()]
        conn.news_error = None
        conn.catalyst_state = [_source_state("provider_company_news")]
        body = self._get(client, monkeypatch, conn)
        assert body["catalyst_sources"]["earnings"]["status"] == "unavailable"

    def test_attention_and_ordering_are_untouched_by_news(self, client, monkeypatch):
        with_news = self._get(client, monkeypatch, self._conn(
            news_rows=[_news_row(symbol="AAPL")]))
        without = self._get(client, monkeypatch, self._conn(
            news_error=RuntimeError("unreadable")))

        def decisions(body):
            return [{k: r[k] for k in
                     ("symbol", "attention", "candidate_verdict", "candidate_score",
                      "cross_arm", "reason_code", "setup_state")}
                    for r in body["results"]]

        assert decisions(with_news) == decisions(without)
        assert ([r["symbol"] for r in with_news["results"]]
                == [r["symbol"] for r in without["results"]])


class TestSymbolDetailNewsContext:
    def _get(self, client, monkeypatch, conn, symbol="AAPL"):
        monkeypatch.setattr(scanner_mod, "resolve_latest_completed_session",
                            lambda now: date(2026, 8, 25))
        _use(conn)
        resp = client.get(f"/api/scanner/symbol?symbol={symbol}")
        assert resp.status_code == 200
        return resp.json()

    def _conn(self, **over):
        base = dict(
            symbol_row=_row(
                candidate_verdict="WATCH", candidate_score=0.7,
                candidate_reason="setup valid",
                candidate_details=json.dumps(_CANDIDATE_DETAILS),
                control_verdict="AVOID", control_score=None, control_reason=None),
            daily_row={"n": 300, "oldest": date(2025, 1, 2),
                       "latest": date(2026, 8, 25)},
            catalyst_state=[_source_state("provider_earnings_calendar"),
                            _source_state("provider_financial_report_filings"),
                            _source_state("provider_company_news")])
        base.update(over)
        return _catalyst_conn(**base)

    def test_the_detail_carries_bounded_items_with_provenance(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(news_rows=[_news_row()]))
        news = body["catalyst_context"]["news"]
        assert news["contract_version"] == "smart_scanner_news_context.v1"
        item = news["items"][0]
        assert item["headline"] == "Apple unveils a new Mac lineup"
        assert item["publisher"] == "Reuters"
        assert item["url"] == "https://reuters.com/tech/apple-macs"
        assert item["category"] == "product_announcement"
        assert item["proximity"] == "today"

    def test_no_provider_payload_and_no_opinion_reaches_the_client(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(news_rows=[_news_row()]))
        blob = json.dumps(body["catalyst_context"]["news"]).lower()
        for banned in ("sentiment", "reasoning", "insights", "description",
                       "keywords", "image_url", "score"):
            assert banned not in blob

    def test_news_sits_beside_earnings_rather_than_inside_it(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(
            catalyst_rows=[_catalyst_event()], news_rows=[_news_row()]))
        ctx = body["catalyst_context"]
        assert set(ctx) >= {"earnings", "last_financial_report", "news"}
        # Catalyst V1's own contract is untouched by the arrival of news.
        assert ctx["contract_version"] == "smart_scanner_catalyst_context.v1"
        assert "news" not in json.dumps(ctx["earnings"]).lower()

    def test_a_news_failure_never_takes_the_detail_down(self, client, monkeypatch):
        body = self._get(client, monkeypatch, self._conn(
            news_error=RuntimeError("unreadable")))
        assert body["candidate"]["verdict"] == "WATCH"
        assert body["catalyst_context"]["news"]["status"] == "unavailable"
        assert body["catalyst_context"]["news"]["items"] == []

    def test_the_detail_reads_news_in_one_query(self, client, monkeypatch):
        conn = self._conn(news_rows=[_news_row()])
        self._get(client, monkeypatch, conn)
        assert conn.news_queries == 1
