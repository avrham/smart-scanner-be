"""Regression for the live Phase 8.1A DHR failure (signal
0affa55a-06b0-4151-9280-6aa5ecd97699).

Root cause: sma150.v3 persists details["invalidation"] as a structured object
{"rule_code", "threshold_pct", "level"} while signal_outcomes.invalidation is
scalar NUMERIC. The outcome builder passed the dict straight into the asyncpg
NUMERIC bind, which raised TypeError; the exception fallback then persisted an
error row WITHOUT the signal's frozen provenance (strategy_version=NULL,
decision_policy_version=NULL live).

Fixes proven here (deterministic unit data only — no DB, no providers):
  * extract_numeric_level: structured invalidation -> numeric level; malformed
    -> None; scalars preserved; booleans rejected; never stringified
  * structured invalidation can no longer fail outcome persistence
  * EVERY outcome path (calculated, insufficient_data, error x ENTER/WATCH)
    copies the frozen provenance + verdict/reference-role/coverage identity
  * legacy signals keep NULL provenance (never inferred from pattern_code)
  * include_recalc selects error rows; the ON CONFLICT(signal_id) upsert
    repairs the SAME row in place (no duplicate, no manual delete)
"""

import asyncio
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import app.workers.outcomes.persistence as outcomes_persistence
import app.workers.outcomes.service as outcomes_service
from app.workers.outcomes.calculator import (
    CALCULATION_VERSION,
    OUTCOME_COVERAGE_VERSION,
    extract_numeric_level,
)
from app.workers.outcomes.service import build_outcome_from_frames
from app.workers.strategies import get_strategy
from app.workers.strategies.base import StrategyContext
from sma150_v3_frames import build_uptrend_frame


def _async(value):
    async def _f(*args, **kwargs):
        return value
    return _f


V3_PROV = {
    "scan_run_id": str(uuid.uuid4()),
    "strategy_code": "sma150_bounce_v3",
    "strategy_version": "sma150.v3",
    "decision_policy_version": "sma150_bounce.policy.v1",
    "config_hash": "cafebabe",
    "provenance_version": "provenance.v1",
}

FROZEN_FIELDS = (
    "scan_run_id", "strategy_code", "strategy_version",
    "decision_policy_version", "config_hash", "provenance_version",
)


def _v3_watch_result(symbol="DHR"):
    strategy = get_strategy("sma150_bounce_v3")
    df = build_uptrend_frame(trigger=False, vol_ratio=1.30)  # WATCH geometry
    result = strategy.evaluate(
        df,
        StrategyContext(symbol=symbol, pattern_code="sma150_bounce_v3",
                        config=strategy.default_config()),
    )
    assert result.verdict == "WATCH"
    return result, df


def _signal_from(result, verdict="WATCH", provenance=V3_PROV):
    sig = {
        "signal_id": str(uuid.uuid4()),
        "symbol": result.symbol,
        "pattern_code": result.pattern_code,
        "verdict": verdict,
        "snapshot_date": result.details["snapshot_date"],
        "created_at": None,
        "details": result.details,
    }
    if provenance is not None:
        sig["provenance"] = dict(provenance)
    return sig


# --------------------------------------------------------------------------- #
# 1. extract_numeric_level contract
# --------------------------------------------------------------------------- #

class TestExtractNumericLevel:
    def test_v3_structured_invalidation_extracts_level(self):
        value = {"rule_code": "daily_close_below_sma150_pct",
                 "threshold_pct": 2.0, "level": 123.1493}
        assert extract_numeric_level(value) == 123.1493

    def test_scalar_values_preserved(self):
        assert extract_numeric_level(101.5) == 101.5
        assert extract_numeric_level(100) == 100.0
        assert isinstance(extract_numeric_level(100), float)

    def test_malformed_structured_values_become_none(self):
        assert extract_numeric_level({"rule_code": "x"}) is None
        assert extract_numeric_level({"level": None}) is None
        assert extract_numeric_level({"level": "123.1"}) is None
        assert extract_numeric_level({"level": {"nested": 1}}) is None
        assert extract_numeric_level("123.1") is None
        assert extract_numeric_level([123.1]) is None
        assert extract_numeric_level(None) is None

    def test_booleans_rejected_not_coerced(self):
        assert extract_numeric_level(True) is None
        assert extract_numeric_level(False) is None
        assert extract_numeric_level({"level": True}) is None

    def test_never_stringifies(self):
        value = {"rule_code": "r", "level": 5.0}
        out = extract_numeric_level(value)
        assert not isinstance(out, str)
        assert out == 5.0


# --------------------------------------------------------------------------- #
# 2. Builder: structured invalidation is normalized, details untouched
# --------------------------------------------------------------------------- #

class TestBuilderNormalization:
    def test_v3_watch_outcome_stores_scalar_invalidation(self):
        result, df = _v3_watch_result()
        rec = build_outcome_from_frames(_signal_from(result), df)
        level = result.details["invalidation"]["level"]
        assert isinstance(rec["invalidation"], float)
        assert rec["invalidation"] == level

    def test_signal_details_keep_structured_evidence(self):
        result, df = _v3_watch_result()
        signal = _signal_from(result)
        build_outcome_from_frames(signal, df)
        # The strategy's persisted structured object is never modified.
        inv = signal["details"]["invalidation"]
        assert isinstance(inv, dict)
        assert set(inv) == {"rule_code", "threshold_pct", "level"}

    def test_dhr_like_watch_now_succeeds_with_full_identity(self):
        """The live-failure scenario end to end: a persisted v3 WATCH signal
        builds a valid outcome record with the exact identity required."""
        result, df = _v3_watch_result()
        rec = build_outcome_from_frames(_signal_from(result), df)
        assert rec["outcome_status"] in ("calculated", "insufficient_data")
        assert rec["strategy_version"] == "sma150.v3"
        assert rec["decision_policy_version"] == "sma150_bounce.policy.v1"
        assert rec["signal_verdict"] == "WATCH"
        assert rec["reference_price_role"] == "candidate_observation"
        assert rec["outcome_coverage_version"] == "candidate_outcomes.v1"
        assert rec["calculation_version"] == "outcome.v1"
        assert rec["invalidation"] is None or isinstance(
            rec["invalidation"], float
        )

    def test_structured_invalidation_cannot_fail_persistence(
        self, monkeypatch
    ):
        """The upsert binds invalidation into a scalar NUMERIC parameter: a
        dict there is exactly the live TypeError. Assert the bound value is
        numeric-or-None."""
        captured = {}

        class _FakeConn:
            async def fetchrow(self, query, *args):
                captured["args"] = args
                for a in args:
                    assert not isinstance(a, dict), (
                        "dict bound into a scalar outcome column"
                    )
                return {"id": uuid.uuid4()}

        monkeypatch.setattr(
            outcomes_persistence, "get_db_connection", _async(_FakeConn())
        )
        monkeypatch.setattr(
            outcomes_persistence, "release_db_connection", _async(None)
        )

        result, df = _v3_watch_result()
        rec = build_outcome_from_frames(_signal_from(result), df)
        asyncio.run(outcomes_persistence.upsert_signal_outcome(rec))
        # invalidation is parameter $10 (0-indexed 9).
        assert captured["args"][9] is None or isinstance(
            captured["args"][9], float
        )


# --------------------------------------------------------------------------- #
# 3. Frozen provenance on EVERY outcome path
# --------------------------------------------------------------------------- #

def _run_service(monkeypatch, signal, fail_build=False):
    """Run calculate_outcomes_for_signals with a fake provider and capture
    every upserted record."""
    upserts = []

    async def fake_upsert(record):
        upserts.append(record)
        return str(uuid.uuid4())

    class _FakeProvider:
        name = "fake"

        async def get_daily_history(self, symbol, timeseries=400):
            return {"historical": []}  # benchmarks/symbol: no data

    monkeypatch.setattr(
        outcomes_service, "get_signals_needing_outcomes", _async([signal])
    )
    monkeypatch.setattr(outcomes_service, "upsert_signal_outcome", fake_upsert)
    if fail_build:
        def boom(*args, **kwargs):
            raise RuntimeError("synthetic calculation failure")
        monkeypatch.setattr(outcomes_service, "build_outcome_from_frames", boom)

    summary = asyncio.run(
        outcomes_service.calculate_outcomes_for_signals(_FakeProvider())
    )
    return summary, upserts


class TestProvenanceOnAllPaths:
    def test_normal_watch_outcome_copies_frozen_provenance(self):
        result, df = _v3_watch_result()
        rec = build_outcome_from_frames(_signal_from(result), df)
        for field in FROZEN_FIELDS:
            assert rec[field] == V3_PROV[field], field

    def test_insufficient_data_watch_copies_frozen_provenance(self):
        result, _ = _v3_watch_result()
        rec = build_outcome_from_frames(_signal_from(result), None)
        assert rec["outcome_status"] == "insufficient_data"
        for field in FROZEN_FIELDS:
            assert rec[field] == V3_PROV[field], field
        assert rec["signal_verdict"] == "WATCH"
        assert rec["reference_price_role"] == "candidate_observation"
        assert rec["outcome_coverage_version"] == OUTCOME_COVERAGE_VERSION

    def test_error_watch_outcome_copies_frozen_provenance(self, monkeypatch):
        result, _ = _v3_watch_result()
        signal = _signal_from(result, verdict="WATCH")
        summary, upserts = _run_service(monkeypatch, signal, fail_build=True)

        assert summary["errors"] == 1
        assert len(upserts) == 1
        err = upserts[0]
        assert err["outcome_status"] == "error"
        for field in FROZEN_FIELDS:
            assert err[field] == V3_PROV[field], field
        assert err["signal_verdict"] == "WATCH"
        assert err["reference_price_role"] == "candidate_observation"
        assert err["outcome_coverage_version"] == OUTCOME_COVERAGE_VERSION
        assert err["calculation_version"] == CALCULATION_VERSION

    def test_error_enter_outcome_copies_frozen_provenance(self, monkeypatch):
        result, _ = _v3_watch_result()
        signal = _signal_from(result, verdict="ENTER")
        _, upserts = _run_service(monkeypatch, signal, fail_build=True)

        err = upserts[0]
        assert err["outcome_status"] == "error"
        for field in FROZEN_FIELDS:
            assert err[field] == V3_PROV[field], field
        assert err["signal_verdict"] == "ENTER"
        assert err["reference_price_role"] == "entry_reference"

    def test_legacy_signal_error_keeps_null_provenance(self, monkeypatch):
        """No provenance row -> NULLs preserved, never inferred from
        pattern_code."""
        result, _ = _v3_watch_result()
        signal = _signal_from(result, provenance=None)
        _, upserts = _run_service(monkeypatch, signal, fail_build=True)

        err = upserts[0]
        for field in FROZEN_FIELDS:
            assert err.get(field) is None, field


# --------------------------------------------------------------------------- #
# 4. Error-row recalc repairs in place
# --------------------------------------------------------------------------- #

class _UpsertStoreConn:
    """Emulates the ON CONFLICT (signal_id) DO UPDATE contract: one row per
    signal_id, repaired in place, id preserved."""

    def __init__(self):
        self.rows = {}  # signal_id -> {"id", "args"}

    async def fetchrow(self, query, *args):
        assert "ON CONFLICT (signal_id) DO UPDATE" in query
        signal_id = str(args[1])
        if signal_id in self.rows:
            existing = self.rows[signal_id]
            existing["args"] = args
            return {"id": existing["id"]}  # same row, updated in place
        row = {"id": args[0], "args": args}
        self.rows[signal_id] = row
        return {"id": row["id"]}


class TestErrorRowRecalc:
    def test_include_recalc_selects_error_rows(self, monkeypatch):
        captured = {}

        class _FakeConn:
            async def fetch(self, query, *params):
                captured["query"] = query
                return []

        monkeypatch.setattr(
            outcomes_persistence, "get_db_connection", _async(_FakeConn())
        )
        monkeypatch.setattr(
            outcomes_persistence, "release_db_connection", _async(None)
        )
        asyncio.run(
            outcomes_persistence.get_signals_needing_outcomes(
                include_recalc=True
            )
        )
        assert "'error'" in captured["query"]
        assert "o.id IS NULL OR o.outcome_status IN" in captured["query"]

    def test_rerun_repairs_error_row_in_place(self, monkeypatch):
        conn = _UpsertStoreConn()
        monkeypatch.setattr(
            outcomes_persistence, "get_db_connection", _async(conn)
        )
        monkeypatch.setattr(
            outcomes_persistence, "release_db_connection", _async(None)
        )

        result, df = _v3_watch_result()
        signal = _signal_from(result)

        # First pass: the DHR-style error row (as the fixed error path
        # writes it — provenance included).
        error_record = {
            "signal_id": signal["signal_id"],
            "symbol": "DHR",
            "pattern_code": "sma150_bounce_v3",
            **{f: V3_PROV[f] for f in FROZEN_FIELDS},
            "side": "LONG",
            "signal_timestamp": signal["snapshot_date"],
            "ret_by_window": {},
            "outcome_status": "error",
            "calculation_version": CALCULATION_VERSION,
            "signal_verdict": "WATCH",
            "reference_price_role": "candidate_observation",
            "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
        }
        first_id = asyncio.run(
            outcomes_persistence.upsert_signal_outcome(error_record)
        )

        # Recalculation succeeds -> the SAME row is repaired (no delete,
        # no duplicate).
        repaired = build_outcome_from_frames(signal, df)
        second_id = asyncio.run(
            outcomes_persistence.upsert_signal_outcome(repaired)
        )

        assert second_id == first_id
        assert len(conn.rows) == 1

        final_args = conn.rows[signal["signal_id"]]["args"]
        # outcome_status ($23, idx 22) and versions repaired in place.
        assert final_args[22] in ("calculated", "insufficient_data")
        assert final_args[23] == "outcome.v1"
        assert "sma150.v3" in final_args
        assert "sma150_bounce.policy.v1" in final_args
        assert "WATCH" in final_args
        assert "candidate_observation" in final_args
        assert OUTCOME_COVERAGE_VERSION in final_args
        # invalidation ($10, idx 9): numeric or None, never an object.
        assert final_args[9] is None or isinstance(final_args[9], float)


# --------------------------------------------------------------------------- #
# 5. No strategy / provider behavior changed
# --------------------------------------------------------------------------- #

class TestNoBehaviorDrift:
    def test_v3_still_emits_structured_invalidation(self):
        result, _ = _v3_watch_result()
        inv = result.details["invalidation"]
        assert isinstance(inv, dict)
        assert inv["rule_code"] == "daily_close_below_sma150_pct"
        assert isinstance(inv["level"], float)

    def test_versions_unchanged(self):
        assert CALCULATION_VERSION == "outcome.v1"
        assert OUTCOME_COVERAGE_VERSION == "candidate_outcomes.v1"

    def test_exactly_migration_011_and_012_wyckoff_only(self):
        """010 is Phase 8.1B1; 011 is Phase 8.1B2 pair outcomes; 012 is
        Phase 9C2 wyckoff_mtf_v2 registration only."""
        migrations = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"
        assert [p.name for p in migrations.glob("010_*")] == [
            "010_sma150_shadow_evaluations.sql"
        ]
        assert [p.name for p in sorted(migrations.glob("011_*"))] == [
            "011_shadow_pair_outcomes.sql"
        ]
        assert [p.name for p in sorted(migrations.glob("012_*"))] == [
            "012_wyckoff_mtf_v2.sql"
        ]
        assert not list(migrations.glob("013_*"))
        sql = (migrations / "012_wyckoff_mtf_v2.sql").read_text(encoding="utf-8")
        assert "strategy_shadow" not in sql.lower()
        assert "wyckoff_mtf_v2" in sql
