"""Phase 8.1A — outcome coverage for persisted ENTER and WATCH signals.

All tests use fakes and deterministic fixtures (no DB, no providers).

Covers:
  * migration 009 is additive, idempotent, backfills from signals.verdict only
    and fabricates/destroys nothing (and no migration 010 exists)
  * selection: ENTER + persisted WATCH eligible, AVOID excluded, dedup kept
  * semantics: ENTER=entry_reference, WATCH=candidate_observation, identical
    return math, no invented later entry, incomplete data stays incomplete
  * frozen provenance + coverage fields are copied onto outcome rows
  * GET /api/outcomes: ENTER-safe default, WATCH/ALL, AND-composed filters,
    malformed verdict rejected
  * metrics: neutral terminology, ENTER-only win_rate, verdict/version/policy/
    config grouping, incomplete rows never pollute completed averages
"""

import asyncio
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.routers.outcomes as outcomes_router
import app.workers.outcomes.persistence as outcomes_persistence
from app.workers.outcomes.calculator import (
    CALCULATION_VERSION,
    OUTCOME_COVERAGE_VERSION,
    reference_price_role_for_verdict,
)
from app.workers.outcomes.metrics import aggregate_outcomes, group_and_aggregate
from app.workers.outcomes.service import build_outcome_from_frames
from main import app


MIGRATIONS = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"
MIGRATION_009 = MIGRATIONS / "009_watch_outcome_coverage.sql"


def _async(value):
    async def _f(*args, **kwargs):
        return value
    return _f


# --------------------------------------------------------------------------- #
# Migration 009
# --------------------------------------------------------------------------- #

class TestMigration009:
    def _sql(self):
        return MIGRATION_009.read_text()

    def _statements(self):
        """Executable SQL only (comment lines stripped)."""
        return "\n".join(
            line for line in self._sql().splitlines()
            if not line.lstrip().startswith("--")
        )

    def test_migration_file_exists_and_boundary(self):
        assert MIGRATION_009.exists()
        # 010 is Phase 8.1B1; 011 is Phase 8.1B2; 012 is Phase 9C2;
        # nothing beyond 012.
        assert [p.name for p in MIGRATIONS.glob("010_*")] == [
            "010_sma150_shadow_evaluations.sql"
        ]
        assert [p.name for p in sorted(MIGRATIONS.glob("011_*"))] == [
            "011_shadow_pair_outcomes.sql"
        ]
        assert [p.name for p in sorted(MIGRATIONS.glob("012_*"))] == [
            "012_wyckoff_mtf_v2.sql"
        ]
        # Phase 9D3 adds exactly 013_wyckoff_v2_shadow_arms (arm-code
        # CHECK extension only); nothing later exists.
        assert [p.name for p in sorted(MIGRATIONS.glob("013_*"))] == [
            "013_wyckoff_v2_shadow_arms.sql"
        ]
        assert not list(MIGRATIONS.glob("014_*"))
    def test_migration_is_additive_and_idempotent(self):
        sql = self._statements()
        assert sql.count("ADD COLUMN IF NOT EXISTS") == 3
        for col in ("signal_verdict", "reference_price_role",
                    "outcome_coverage_version"):
            assert col in sql
        assert "CREATE INDEX IF NOT EXISTS" in sql
        # Re-run safety: the backfill only touches un-stamped rows.
        assert "o.signal_verdict IS NULL" in sql
        for forbidden in ("DROP ", "DELETE ", "TRUNCATE"):
            assert forbidden not in sql.upper()

    def test_backfill_derives_verdict_only_from_signals(self):
        sql = self._statements()
        # The verdict source is the signals join — never strategy name,
        # score or outcome return.
        assert "FROM public.signals s" in sql
        assert "s.id = o.signal_id" in sql
        assert "signal_verdict = s.verdict" in sql
        for never_used in ("strategy_version", "score", "ret_1d", "ret_20d",
                           "pattern_code"):
            assert never_used not in sql

    def test_migration_fabricates_no_outcome_rows(self):
        sql = self._statements()
        assert "INSERT INTO" not in sql.upper()

    def test_migration_never_modifies_signals_or_provenance(self):
        sql = self._statements()
        assert "UPDATE public.signal_outcomes" in sql
        assert "UPDATE public.signals" not in sql
        assert "UPDATE public.signal_provenance" not in sql
        assert "ALTER TABLE public.signals\n" not in sql
        assert "ALTER TABLE public.signal_provenance" not in sql

    def test_unique_signal_constraint_untouched(self):
        # The one-outcome-per-signal identity from 003 is preserved.
        assert "UNIQUE" not in self._statements().upper()
        assert "NOT NULL" not in self._statements().replace(
            "IS NOT NULL", ""
        )


# --------------------------------------------------------------------------- #
# Selection (get_signals_needing_outcomes)
# --------------------------------------------------------------------------- #

def _capture_selection(monkeypatch, rows=None, **kwargs):
    captured = {}

    class _FakeConn:
        async def fetch(self, query, *params):
            captured["query"] = query
            captured["params"] = params
            return rows or []

    monkeypatch.setattr(
        outcomes_persistence, "get_db_connection", _async(_FakeConn())
    )
    monkeypatch.setattr(
        outcomes_persistence, "release_db_connection", _async(None)
    )
    result = asyncio.run(
        outcomes_persistence.get_signals_needing_outcomes(**kwargs)
    )
    return captured, result


class TestSelection:
    def test_enter_and_watch_eligible_avoid_excluded(self, monkeypatch):
        captured, _ = _capture_selection(monkeypatch)
        assert "s.verdict IN ('ENTER', 'WATCH')" in captured["query"]
        assert "AVOID" not in captured["query"]

    def test_selection_reads_only_immutable_signals(self, monkeypatch):
        """Eligibility comes from the signals table: a WATCH that was never
        persisted has no row and can never receive an outcome. No telemetry
        table is consulted and no signal is inferred."""
        captured, _ = _capture_selection(monkeypatch)
        assert "FROM signals s" in captured["query"]
        for never_source in ("pattern_runs", "scan_run_signals", "telemetry"):
            assert never_source not in captured["query"]

    def test_dedup_still_effective(self, monkeypatch):
        captured, _ = _capture_selection(monkeypatch)
        # Only signals WITHOUT an existing outcome row are selected; the
        # UNIQUE(signal_id) upsert makes reruns idempotent per signal.
        assert "o.id IS NULL" in captured["query"]

    def test_include_recalc_branch_unchanged(self, monkeypatch):
        captured, _ = _capture_selection(monkeypatch, include_recalc=True)
        assert "'pending', 'error', 'insufficient_data'" in captured["query"]

    def test_selected_rows_carry_the_signal_verdict(self, monkeypatch):
        row = {
            "id": uuid.uuid4(), "symbol": "AAA",
            "pattern_code": "sma150_bounce_v3", "verdict": "WATCH",
            "snapshot_date": date(2026, 7, 1), "created_at": None,
            "details": "{}", "scan_run_id": None, "strategy_code": None,
            "strategy_version": None, "decision_policy_version": None,
            "config_hash": None, "provenance_version": None,
        }
        _, result = _capture_selection(monkeypatch, rows=[row])
        assert result[0]["verdict"] == "WATCH"


# --------------------------------------------------------------------------- #
# Semantics (pure builder)
# --------------------------------------------------------------------------- #

def _frame(days=30, start_price=100.0):
    dates = pd.date_range("2026-06-01", periods=days, freq="B")
    prices = [start_price + i for i in range(days)]
    return pd.DataFrame({
        "date": dates,
        "open": prices, "high": [p + 1 for p in prices],
        "low": [p - 1 for p in prices], "close": prices,
        "volume": 1_000_000,
    })


_PROV = {
    "scan_run_id": str(uuid.uuid4()),
    "strategy_code": "sma150_bounce_v3",
    "strategy_version": "sma150.v3",
    "decision_policy_version": "sma150_bounce.policy.v1",
    "config_hash": "cafebabe",
    "provenance_version": "provenance.v1",
}


def _signal(verdict=None, provenance=None):
    sig = {
        "signal_id": str(uuid.uuid4()),
        "symbol": "AAA",
        "pattern_code": "sma150_bounce_v3",
        "snapshot_date": date(2026, 6, 3),
        "created_at": None,
        "details": {},
    }
    if verdict is not None:
        sig["verdict"] = verdict
    if provenance is not None:
        sig["provenance"] = provenance
    return sig


class TestReferenceSemantics:
    def test_enter_uses_entry_reference(self):
        rec = build_outcome_from_frames(_signal("ENTER"), _frame())
        assert rec["signal_verdict"] == "ENTER"
        assert rec["reference_price_role"] == "entry_reference"
        assert rec["outcome_coverage_version"] == OUTCOME_COVERAGE_VERSION

    def test_watch_uses_candidate_observation(self):
        rec = build_outcome_from_frames(_signal("WATCH"), _frame())
        assert rec["signal_verdict"] == "WATCH"
        assert rec["reference_price_role"] == "candidate_observation"
        assert rec["outcome_coverage_version"] == OUTCOME_COVERAGE_VERSION

    def test_watch_does_not_invent_a_later_entry(self):
        """The WATCH reference is the decision-bar close (the observation
        price). It never moves forward to a hypothetical later trigger."""
        frame = _frame()
        rec = build_outcome_from_frames(_signal("WATCH"), frame)
        decision_bar_close = float(
            frame.loc[frame["date"].dt.date == date(2026, 6, 3), "close"].iloc[0]
        )
        assert rec["entry_price"] == decision_bar_close

    def test_same_numeric_formula_for_both_verdicts(self):
        frame = _frame()
        enter = build_outcome_from_frames(_signal("ENTER"), frame)
        watch = build_outcome_from_frames(_signal("WATCH"), frame)
        for key in ("entry_price", "ret_by_window", "max_favorable_excursion",
                    "max_adverse_excursion", "outcome_status",
                    "calculation_version"):
            assert enter[key] == watch[key]
        # The math version did NOT change because coverage expanded.
        assert enter["calculation_version"] == CALCULATION_VERSION == "outcome.v1"

    def test_incomplete_future_data_stays_incomplete(self):
        # Decision bar 2026-06-03 (index 2) + only 5 forward bars.
        frame = _frame(days=8)
        rec = build_outcome_from_frames(_signal("WATCH"), frame)
        assert rec["ret_by_window"][1] is not None
        assert rec["ret_by_window"][5] is not None
        # Missing windows are None, never fabricated zeros or losses.
        assert rec["ret_by_window"][10] is None
        assert rec["ret_by_window"][20] is None

    def test_no_frame_is_insufficient_data_not_failed(self):
        rec = build_outcome_from_frames(_signal("WATCH"), None)
        assert rec["outcome_status"] == "insufficient_data"
        assert rec["entry_price"] is None
        assert all(v is None for v in rec["ret_by_window"].values())

    def test_unknown_verdict_stays_unknown(self):
        rec = build_outcome_from_frames(_signal(), _frame())
        assert rec["signal_verdict"] is None
        assert rec["reference_price_role"] is None

    def test_role_mapping_never_infers(self):
        assert reference_price_role_for_verdict("ENTER") == "entry_reference"
        assert reference_price_role_for_verdict("WATCH") == "candidate_observation"
        assert reference_price_role_for_verdict(None) is None
        assert reference_price_role_for_verdict("AVOID") is None


class TestFrozenProvenance:
    def test_watch_outcome_copies_frozen_fields(self):
        rec = build_outcome_from_frames(
            _signal("WATCH", provenance=_PROV), _frame()
        )
        assert rec["strategy_code"] == "sma150_bounce_v3"
        assert rec["strategy_version"] == "sma150.v3"
        assert rec["decision_policy_version"] == "sma150_bounce.policy.v1"
        assert rec["config_hash"] == "cafebabe"
        assert rec["provenance_version"] == "provenance.v1"
        assert rec["signal_verdict"] == "WATCH"
        assert rec["outcome_coverage_version"] == OUTCOME_COVERAGE_VERSION

    def test_upsert_persists_coverage_columns(self, monkeypatch):
        captured = {}

        class _FakeConn:
            async def fetchrow(self, query, *args):
                captured["query"] = query
                captured["args"] = args
                return {"id": uuid.uuid4()}

        monkeypatch.setattr(
            outcomes_persistence, "get_db_connection", _async(_FakeConn())
        )
        monkeypatch.setattr(
            outcomes_persistence, "release_db_connection", _async(None)
        )

        rec = build_outcome_from_frames(
            _signal("WATCH", provenance=_PROV), _frame()
        )
        asyncio.run(outcomes_persistence.upsert_signal_outcome(rec))

        q = captured["query"]
        for col in ("signal_verdict", "reference_price_role",
                    "outcome_coverage_version"):
            assert col in q
        # Rerun idempotence: one row per signal, updated in place.
        assert "ON CONFLICT (signal_id) DO UPDATE" in q
        assert "WATCH" in captured["args"]
        assert "candidate_observation" in captured["args"]
        assert OUTCOME_COVERAGE_VERSION in captured["args"]


# --------------------------------------------------------------------------- #
# fetch_outcomes filter composition
# --------------------------------------------------------------------------- #

def _capture_fetch(monkeypatch, **kwargs):
    captured = {}

    class _FakeConn:
        async def fetch(self, query, *params):
            captured["query"] = query
            captured["params"] = params
            return []

    monkeypatch.setattr(
        outcomes_persistence, "get_db_connection", _async(_FakeConn())
    )
    monkeypatch.setattr(
        outcomes_persistence, "release_db_connection", _async(None)
    )
    asyncio.run(outcomes_persistence.fetch_outcomes(**kwargs))
    return captured


class TestFetchFilters:
    def test_enter_filter_includes_legacy_null_rows(self, monkeypatch):
        captured = _capture_fetch(monkeypatch, verdict="ENTER")
        assert ("(signal_verdict = 'ENTER' OR signal_verdict IS NULL)"
                in captured["query"])

    def test_watch_filter_is_strict(self, monkeypatch):
        captured = _capture_fetch(monkeypatch, verdict="WATCH")
        assert "signal_verdict = $" in captured["query"]
        assert "WATCH" in captured["params"]
        assert "IS NULL" not in captured["query"]

    def test_all_applies_no_verdict_filter(self, monkeypatch):
        captured = _capture_fetch(monkeypatch, verdict="ALL")
        assert "signal_verdict =" not in captured["query"]

    def test_filters_and_compose(self, monkeypatch):
        captured = _capture_fetch(
            monkeypatch,
            verdict="WATCH",
            strategy_code="sma150_bounce_v3",
            strategy_version="sma150.v3",
            decision_policy_version="sma150_bounce.policy.v1",
            config_hash="cafebabe",
            outcome_coverage_version=OUTCOME_COVERAGE_VERSION,
        )
        q = captured["query"]
        for clause in ("strategy_code = $", "strategy_version = $",
                       "decision_policy_version = $", "config_hash = $",
                       "outcome_coverage_version = $"):
            assert clause in q
        # AND-composed, single WHERE.
        assert q.count("WHERE") == 1
        assert " OR " not in q.replace(
            "(signal_verdict = 'ENTER' OR signal_verdict IS NULL)", ""
        )


# --------------------------------------------------------------------------- #
# API endpoints
# --------------------------------------------------------------------------- #

def _outcome_record(verdict, strategy_version="sma150.v3", ret5=2.0,
                    config_hash="cfg-a",
                    policy="sma150_bounce.policy.v1"):
    return {
        "id": str(uuid.uuid4()),
        "signal_id": str(uuid.uuid4()),
        "symbol": "AAA",
        "pattern_code": "sma150_bounce_v3",
        "side": "LONG",
        "signal_timestamp": datetime(2026, 6, 3, tzinfo=timezone.utc),
        "ret_by_window": {1: 1.0, 3: 1.5, 5: ret5, 10: None, 20: None},
        "returns": {"1D": 1.0, "3D": 1.5, "5D": ret5, "10D": None, "20D": None},
        "benchmark_returns": None,
        "same_ticker_buy_hold": None,
        "mfe": 3.0, "mae": -1.0,
        "simulated_r": None,
        "outcome_status": "calculated",
        "calculation_version": "outcome.v1",
        "strategy_code": "sma150_bounce_v3",
        "strategy_version": strategy_version,
        "decision_policy_version": policy,
        "config_hash": config_hash,
        "provenance_version": "provenance.v1",
        "signal_verdict": verdict,
        "reference_price_role": reference_price_role_for_verdict(verdict),
        "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
    }


@pytest.fixture
def api(monkeypatch):
    """TestClient whose fetch_outcomes is a recording fake."""
    calls = []
    records = {"rows": []}

    async def fake_fetch(**kwargs):
        calls.append(kwargs)
        rows = records["rows"]
        verdict = kwargs.get("verdict")
        if verdict == "ENTER":
            rows = [r for r in rows
                    if r["signal_verdict"] in ("ENTER", None)]
        elif verdict == "WATCH":
            rows = [r for r in rows if r["signal_verdict"] == "WATCH"]
        return rows

    monkeypatch.setattr(outcomes_router, "fetch_outcomes", fake_fetch)
    client = TestClient(app, raise_server_exceptions=False)
    return client, calls, records


class TestOutcomesApi:
    def test_default_is_enter_safe(self, api):
        client, calls, records = api
        records["rows"] = [
            _outcome_record("ENTER"), _outcome_record("WATCH")
        ]
        resp = client.get("/api/outcomes")
        assert resp.status_code == 200
        assert calls[0]["verdict"] == "ENTER"
        body = resp.json()
        assert len(body) == 1
        assert body[0]["signal_verdict"] == "ENTER"
        assert body[0]["reference_price_role"] == "entry_reference"

    def test_watch_returns_watch_only(self, api):
        client, calls, records = api
        records["rows"] = [
            _outcome_record("ENTER"), _outcome_record("WATCH")
        ]
        body = client.get("/api/outcomes", params={"verdict": "WATCH"}).json()
        assert len(body) == 1
        assert body[0]["signal_verdict"] == "WATCH"
        assert body[0]["reference_price_role"] == "candidate_observation"
        assert body[0]["outcome_coverage_version"] == OUTCOME_COVERAGE_VERSION

    def test_all_returns_both(self, api):
        client, calls, records = api
        records["rows"] = [
            _outcome_record("ENTER"), _outcome_record("WATCH")
        ]
        body = client.get("/api/outcomes", params={"verdict": "ALL"}).json()
        assert {r["signal_verdict"] for r in body} == {"ENTER", "WATCH"}

    def test_filters_are_forwarded_and_composed(self, api):
        client, calls, _ = api
        resp = client.get("/api/outcomes", params={
            "verdict": "WATCH",
            "strategy_code": "sma150_bounce_v3",
            "strategy_version": "sma150.v3",
            "decision_policy_version": "sma150_bounce.policy.v1",
            "config_hash": "cafebabe",
            "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
            "reference_price_role": "candidate_observation",
        })
        assert resp.status_code == 200
        kw = calls[0]
        assert kw["verdict"] == "WATCH"
        assert kw["strategy_code"] == "sma150_bounce_v3"
        assert kw["strategy_version"] == "sma150.v3"
        assert kw["decision_policy_version"] == "sma150_bounce.policy.v1"
        assert kw["config_hash"] == "cafebabe"
        assert kw["outcome_coverage_version"] == OUTCOME_COVERAGE_VERSION
        assert kw["reference_price_role"] == "candidate_observation"

    def test_malformed_verdict_rejected_safely(self, api):
        client, calls, _ = api
        resp = client.get("/api/outcomes", params={"verdict": "BOGUS"})
        assert resp.status_code == 422
        assert calls == []  # rejected before any query

    def test_no_internal_config_snapshot_exposed(self, api):
        client, _, records = api
        records["rows"] = [_outcome_record("WATCH")]
        body = client.get("/api/outcomes", params={"verdict": "WATCH"}).json()
        assert "config_snapshot" not in body[0]
        assert "evidence_snapshot" not in body[0]
        # The bounded identity hash is fine; full config is not exposed.
        assert body[0]["config_hash"] == "cfg-a"


class TestMetricsApi:
    def test_enter_default_keeps_win_rate(self, api):
        client, calls, records = api
        records["rows"] = [_outcome_record("ENTER"), _outcome_record("ENTER")]
        body = client.get("/api/outcomes/metrics", params={"window": 5}).json()
        assert calls[0]["verdict"] == "ENTER"
        assert body["metrics"]["win_rate"] == 1.0
        assert body["metrics"]["positive_return_rate"] == 1.0

    def test_watch_metrics_use_neutral_terminology(self, api):
        client, _, records = api
        records["rows"] = [
            _outcome_record("WATCH", ret5=2.0),
            _outcome_record("WATCH", ret5=-1.0),
        ]
        body = client.get(
            "/api/outcomes/metrics", params={"verdict": "WATCH", "window": 5}
        ).json()
        m = body["metrics"]
        assert "win_rate" not in m
        assert m["positive_return_rate"] == 0.5
        assert m["sample_count"] == 2
        assert m["completed_count"] == 2
        assert m["mean_return_pct"] == 0.5

    def test_group_by_verdict_separates_enter_and_watch(self, api):
        client, _, records = api
        records["rows"] = [
            _outcome_record("ENTER", ret5=3.0),
            _outcome_record("WATCH", ret5=-2.0),
        ]
        body = client.get("/api/outcomes/metrics", params={
            "verdict": "ALL", "group_by": "signal_verdict", "window": 5,
        }).json()
        groups = {g["signal_verdict"]: g for g in body["groups"]}
        assert set(groups) == {"ENTER", "WATCH"}
        assert groups["ENTER"]["mean_return_pct"] == 3.0
        assert groups["WATCH"]["mean_return_pct"] == -2.0

    def test_group_by_strategy_version_and_policy_and_config(self, api):
        client, _, records = api
        records["rows"] = [
            _outcome_record("ENTER", strategy_version="sma150.v2",
                            policy="strategy_decision.v1", config_hash="cfg-a"),
            _outcome_record("ENTER", strategy_version="sma150.v3",
                            policy="sma150_bounce.policy.v1",
                            config_hash="cfg-b"),
        ]
        body = client.get("/api/outcomes/metrics", params={
            "verdict": "ALL",
            "group_by": "strategy_version,decision_policy_version,config_hash",
            "window": 5,
        }).json()
        keys = {
            (g["strategy_version"], g["decision_policy_version"],
             g["config_hash"])
            for g in body["groups"]
        }
        assert keys == {
            ("sma150.v2", "strategy_decision.v1", "cfg-a"),
            ("sma150.v3", "sma150_bounce.policy.v1", "cfg-b"),
        }

    def test_malformed_verdict_rejected(self, api):
        client, _, _ = api
        resp = client.get("/api/outcomes/metrics", params={"verdict": "nope"})
        assert resp.status_code == 422


# --------------------------------------------------------------------------- #
# Metrics aggregation (pure)
# --------------------------------------------------------------------------- #

class TestMetricsAggregation:
    def test_incomplete_rows_do_not_pollute_completed_averages(self):
        rows = [
            _outcome_record("WATCH", ret5=4.0),
            _outcome_record("WATCH", ret5=None),  # incomplete at 5D
        ]
        m = aggregate_outcomes(rows, 5)
        assert m["sample_count"] == 2
        assert m["completed_count"] == 1
        assert m["incomplete_count"] == 1
        assert m["mean_return_pct"] == 4.0  # None never coerced to 0

    def test_enter_only_sample_keeps_win_rate(self):
        rows = [_outcome_record("ENTER", ret5=1.0)]
        m = aggregate_outcomes(rows, 5)
        assert m["win_rate"] == 1.0
        assert m["positive_return_rate"] == 1.0

    def test_mixed_sample_drops_trade_terminology(self):
        rows = [_outcome_record("ENTER", ret5=1.0),
                _outcome_record("WATCH", ret5=1.0)]
        m = aggregate_outcomes(rows, 5)
        assert "win_rate" not in m
        assert m["positive_return_rate"] == 1.0

    def test_legacy_records_without_verdict_keep_win_rate(self):
        legacy = _outcome_record("ENTER")
        legacy["signal_verdict"] = None
        m = aggregate_outcomes([legacy], 5)
        assert "win_rate" in m

    def test_v2_v3_and_verdict_groups_stay_distinct(self):
        rows = [
            _outcome_record("ENTER", strategy_version="sma150.v2"),
            _outcome_record("ENTER", strategy_version="sma150.v3"),
            _outcome_record("WATCH", strategy_version="sma150.v3"),
        ]
        groups = group_and_aggregate(
            rows, ["strategy_version", "signal_verdict"], 5
        )
        keys = {(g["strategy_version"], g["signal_verdict"]) for g in groups}
        assert keys == {
            ("sma150.v2", "ENTER"),
            ("sma150.v3", "ENTER"),
            ("sma150.v3", "WATCH"),
        }
