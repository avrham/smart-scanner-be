"""Phase 8.1B1 live pair_error fix: strict shadow JSON normalization.

Live failure: sma150.v2 stores pandas.Timestamp objects in
details["bounces_detail"][*]["date"]; _bound_details returned the raw dict
when under the byte bound, and persist_shadow_pair's strict json.dumps then
raised "TypeError: Object of type Timestamp is not JSON serializable",
rejecting every pair as pair_error.

These tests prove: (1) the exact original exception with a REAL v2
evaluation, (2) the closed normalization contract, (3) hash/persist
consistency (one normalized representation for hashing, bounding,
persistence and fingerprints), (4) deterministic bounded rejection
classification, (5) real end-to-end pair persistence through the REAL
persist_shadow_pair SQL flow.
"""

import asyncio
import hashlib
import json
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import app.workers.shadow.persistence as shadow_persistence
import app.workers.shadow.runner as shadow_runner
from app.workers.provenance import canonical_json, _sha256
from app.workers.shadow.constants import (
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
    FRAME_HARD_CAP_BARS,
)
from app.workers.shadow.fingerprints import compute_evaluation_fingerprint
from app.workers.shadow.frames import build_canonical_frame
from app.workers.shadow.runner import _bound_details, run_shadow_comparison
from app.workers.shadow.serialization import (
    ShadowSerializationError,
    normalize_json_safe,
    strict_json,
)
from app.workers.strategies import StrategyContext, get_strategy
from sma150_v3_frames import build_jbl_like_frame, build_uptrend_frame


MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"

# All fixture frames end well before this instant -> latest bar is a
# completed prior session under ny_session_close.v1, deterministically.
NOW_UTC = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def _run(coro):
    return asyncio.run(coro)


def frame_to_payload(df: pd.DataFrame) -> dict:
    return {
        "historical": [
            {
                "date": row["date"].strftime("%Y-%m-%d"),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
            for _, row in df.iterrows()
        ]
    }


def _real_v2_details_with_bounces():
    """REAL sma150.v2 evaluation (no mocks) whose details carry pandas
    Timestamps inside bounces_detail."""
    payload = frame_to_payload(build_uptrend_frame(trigger=True))
    frame = build_canonical_frame(
        "REPRO", payload, max_bars=FRAME_HARD_CAP_BARS, now_utc=NOW_UTC
    )
    strategy = get_strategy("sma150_bounce")
    context = StrategyContext(
        symbol="REPRO",
        pattern_code="sma150_bounce",
        config=strategy.default_config(),
        scanner_mode="shadow",
        scan_run_id="repro-run",
    )
    result = strategy.evaluate(frame.dataframe(), context)
    assert result.details.get("bounces_detail"), "fixture must produce bounces"
    return result


# --------------------------------------------------------------------------- #
# 1. Deterministic reproduction of the live exception
# --------------------------------------------------------------------------- #

class TestLiveReproduction:
    def test_real_v2_bounce_details_contain_pandas_timestamp(self):
        result = _real_v2_details_with_bounces()
        first = result.details["bounces_detail"][0]["date"]
        assert isinstance(first, pd.Timestamp)

    def test_unnormalized_details_raise_the_exact_live_exception(self):
        """The pre-fix behavior, reproduced on the RAW details: strict
        json.dumps of a snapshot containing the Timestamp raises exactly the
        live TypeError."""
        result = _real_v2_details_with_bounces()
        with pytest.raises(
            TypeError, match="Object of type Timestamp is not JSON serializable"
        ):
            json.dumps(result.details)

    def test_bound_details_now_returns_json_safe_snapshot(self):
        result = _real_v2_details_with_bounces()
        bounded = _bound_details(result.details)
        snap_date = bounded["snapshot"]["bounces_detail"][0]["date"]
        assert isinstance(snap_date, str)
        # Strict serialization (the exact persist_shadow_pair path) succeeds.
        text = strict_json(bounded["snapshot"])
        assert "bounces_detail" in text

    def test_raw_strategy_details_stay_untouched(self):
        """Normalization is a boundary, not a mutation: the strategy's own
        returned details still carry the Timestamp after bounding."""
        result = _real_v2_details_with_bounces()
        _bound_details(result.details)
        assert isinstance(
            result.details["bounces_detail"][0]["date"], pd.Timestamp
        )


# --------------------------------------------------------------------------- #
# 2. The closed normalization contract
# --------------------------------------------------------------------------- #

class TestNormalizationContract:
    def test_primitives_unchanged(self):
        assert normalize_json_safe(None) is None
        assert normalize_json_safe("abc") == "abc"
        assert normalize_json_safe(True) is True
        assert normalize_json_safe(False) is False
        assert normalize_json_safe(7) == 7
        assert normalize_json_safe(1.25) == 1.25

    def test_date_datetime_timestamp_become_iso_strings(self):
        assert normalize_json_safe(date(2026, 7, 17)) == "2026-07-17"
        assert (
            normalize_json_safe(datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc))
            == "2026-07-17T20:00:00+00:00"
        )
        assert (
            normalize_json_safe(pd.Timestamp("2026-07-17"))
            == "2026-07-17T00:00:00"
        )

    def test_numpy_scalars_become_python_numbers(self):
        out_int = normalize_json_safe(np.int64(42))
        out_float = normalize_json_safe(np.float64(1.5))
        assert out_int == 42 and type(out_int) is int
        assert out_float == 1.5 and type(out_float) is float
        assert normalize_json_safe(np.bool_(True)) is True

    def test_nested_structures_normalize_recursively(self):
        value = {
            "a": [{"d": pd.Timestamp("2026-01-02"), "n": np.float64(2.5)}],
            "b": {"inner": (np.int32(1), date(2026, 1, 3))},
        }
        out = normalize_json_safe(value)
        assert out == {
            "a": [{"d": "2026-01-02T00:00:00", "n": 2.5}],
            "b": {"inner": [1, "2026-01-03"]},
        }
        strict_json(out)  # must not raise

    def test_semantic_list_order_preserved(self):
        chronological = [
            {"date": pd.Timestamp("2026-03-01")},
            {"date": pd.Timestamp("2026-01-01")},
            {"date": pd.Timestamp("2026-02-01")},
        ]
        out = normalize_json_safe(chronological)
        assert [b["date"] for b in out] == [
            "2026-03-01T00:00:00", "2026-01-01T00:00:00", "2026-02-01T00:00:00"
        ]

    def test_non_string_dict_keys_reject(self):
        with pytest.raises(ShadowSerializationError) as exc:
            normalize_json_safe({1: "x"})
        assert exc.value.reason_code.startswith("non_string_dict_key")

    def test_sets_reject_never_silently_reordered(self):
        with pytest.raises(ShadowSerializationError) as exc:
            normalize_json_safe({"s": {3, 1, 2}})
        assert exc.value.reason_code == "set_not_json_safe"
        with pytest.raises(ShadowSerializationError):
            normalize_json_safe(frozenset({1}))

    def test_bytes_reject(self):
        with pytest.raises(ShadowSerializationError) as exc:
            normalize_json_safe(b"raw")
        assert exc.value.reason_code == "bytes_not_json_safe"

    def test_custom_objects_reject_no_str_fallback(self):
        class Opaque:
            def __str__(self):
                return "SECRET-TOKEN-VALUE"

        with pytest.raises(ShadowSerializationError) as exc:
            normalize_json_safe({"o": Opaque()})
        assert exc.value.reason_code == "unsupported_type:Opaque"
        # The error carries only the reason code + path — never the object's
        # string form.
        assert "SECRET-TOKEN-VALUE" not in str(exc.value)

    def test_nan_and_infinity_reject(self):
        for bad in (float("nan"), float("inf"), float("-inf"), np.float64("nan")):
            with pytest.raises(ShadowSerializationError) as exc:
                normalize_json_safe({"v": bad})
            assert exc.value.reason_code == "non_finite_float"

    def test_error_path_points_to_offending_field(self):
        with pytest.raises(ShadowSerializationError) as exc:
            normalize_json_safe({"a": [{"b": {1, 2}}]})
        assert exc.value.path == "$.a[0].b"

    def test_equivalent_inputs_produce_deterministic_representation(self):
        # Same-typed equal inputs always normalize identically; the intended
        # deterministic mapping is date -> date ISO, Timestamp/datetime ->
        # datetime ISO.
        a = normalize_json_safe(pd.Timestamp("2024-02-26"))
        b = normalize_json_safe(pd.Timestamp("2024-02-26"))
        assert a == b == "2024-02-26T00:00:00"
        assert normalize_json_safe(date(2024, 2, 26)) == "2024-02-26"
        assert (
            normalize_json_safe(datetime(2024, 2, 26))
            == normalize_json_safe(pd.Timestamp("2024-02-26"))
        )

    def test_strict_json_is_strict(self):
        with pytest.raises(ValueError):
            strict_json({"v": float("nan")})
        with pytest.raises(TypeError):
            strict_json({"v": pd.Timestamp("2026-01-01")})


# --------------------------------------------------------------------------- #
# 3. Hash / persistence consistency
# --------------------------------------------------------------------------- #

class TestHashPersistConsistency:
    def test_original_sha_computed_from_normalized_pre_pruning_details(self):
        result = _real_v2_details_with_bounces()
        bounded = _bound_details(result.details)
        normalized = normalize_json_safe(result.details)
        expected = hashlib.sha256(
            canonical_json(normalized).encode("utf-8")
        ).hexdigest()
        assert bounded["original_sha256"] == expected

    def test_persisted_snapshot_is_bounded_form_of_same_normalized_value(self):
        result = _real_v2_details_with_bounces()
        bounded = _bound_details(result.details)
        # Within the byte bound the snapshot IS the normalized details.
        assert bounded["snapshot"] == normalize_json_safe(result.details)

    def test_evaluation_fingerprint_deterministic_after_normalization(self):
        result = _real_v2_details_with_bounces()
        shas = {_bound_details(result.details)["original_sha256"] for _ in range(3)}
        assert len(shas) == 1
        fp_kwargs = dict(
            pair_fingerprint="pf",
            arm_code=CONTROL_ARM_CODE,
            strategy_code="sma150_bounce",
            strategy_version="sma150.v2",
            decision_policy_version="strategy_decision.v1",
            config_hash_value="ch",
            verdict=result.verdict,
            details_original_sha256=shas.pop(),
        )
        assert compute_evaluation_fingerprint(
            **fp_kwargs
        ) == compute_evaluation_fingerprint(**fp_kwargs)


# --------------------------------------------------------------------------- #
# 4. Runner rejection classification
# --------------------------------------------------------------------------- #

class _StubResult:
    def __init__(self, details):
        self.verdict = "AVOID"
        self.score = None
        self.reason = "stub"
        self.rejection_reason = "stub"
        self.details = details


class _StubStrategy:
    version = "stub.v1"
    decision_policy_version = "stub_policy.v1"

    def __init__(self, details):
        self._details = details

    def default_config(self):
        return {"sma_window": 150, "lookback_days_for_history": 365,
                "rebound_window_days": 10, "lookback_bars_for_history": 365,
                "min_history_bars": 200, "slope_lookback_bars": 20,
                "volume_window_bars": 20, "rebound_window_bars": 10}

    def evaluate(self, df, context):
        return _StubResult(self._details)


class _RunStore:
    def __init__(self):
        self.runs = {}

    async def create_run(self, run_id, **kwargs):
        self.runs[str(run_id)] = {"status": "running", **kwargs}
        return str(run_id)

    async def finalize_run(self, run_id, **kwargs):
        self.runs[str(run_id)].update(kwargs)


class FakeProvider:
    name = "fake_provider"

    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    async def get_daily_history(self, symbol, timeseries=400):
        self.calls.append(symbol)
        return self.payloads[symbol]


class TestRejectionClassification:
    def test_unsafe_details_recorded_as_details_not_json_safe(self, monkeypatch):
        store = _RunStore()
        monkeypatch.setattr(shadow_runner, "create_shadow_run", store.create_run)
        monkeypatch.setattr(shadow_runner, "finalize_shadow_run", store.finalize_run)

        async def forbidden_persist(**kwargs):
            raise AssertionError("must never reach persistence")
        monkeypatch.setattr(shadow_runner, "persist_shadow_pair", forbidden_persist)

        async def fake_resolve(pattern_code, defaults):
            return dict(defaults)
        monkeypatch.setattr(shadow_runner, "resolve_pattern_config", fake_resolve)

        unsafe = _StubStrategy({"symbol": "BADX", "weird": {1, 2, 3}})
        monkeypatch.setattr(shadow_runner, "get_strategy", lambda code: unsafe)

        provider = FakeProvider(
            {"BADX": frame_to_payload(build_uptrend_frame(trigger=False))}
        )
        summary = _run(run_shadow_comparison(provider, ["BADX"], now_utc=NOW_UTC))

        assert summary["status"] == "completed"
        telemetry = summary["telemetry"]
        assert telemetry["pair_count"] == 0
        assert telemetry["rejected_counts"] == {"details_not_json_safe": 1}
        assert telemetry["rejected_symbols"] == {"details_not_json_safe": ["BADX"]}
        # Bounded reason code only — no repr/traceback leaked into telemetry.
        assert "set" not in json.dumps(telemetry.get("rejected_counts"))


# --------------------------------------------------------------------------- #
# 5. Real end-to-end persistence through the REAL persist_shadow_pair SQL flow
# --------------------------------------------------------------------------- #

class _FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class _FakeShadowDB:
    """Emulates the strategy_shadow_* tables; stores the EXACT serialized
    argument values persist_shadow_pair passes to the driver."""

    def __init__(self):
        self.runs = {}
        self.pairs = {}          # (fingerprint, version) -> row for fetchrow
        self.pair_inserts = []   # full args tuples
        self.eval_inserts = []
        self.links = {}
        self.forbidden = []

    def transaction(self):
        return _FakeTx()

    async def fetchrow(self, query, *args):
        assert "strategy_shadow_pairs" in query
        return self.pairs.get((args[0], args[1]))

    async def execute(self, query, *args):
        for bad in ("INSERT INTO signals", "INSERT INTO signal_provenance",
                    "INSERT INTO scan_run_signals", "INSERT INTO signal_outcomes",
                    "UPDATE signals", "UPDATE signal_outcomes"):
            if bad in query:
                self.forbidden.append(bad)
        if "INSERT INTO strategy_shadow_runs" in query:
            self.runs[str(args[0])] = {"status": "running", "telemetry": None}
        elif "UPDATE strategy_shadow_runs" in query:
            self.runs[str(args[0])].update(
                {"status": args[1], "telemetry": args[2],
                 "error_code": args[3], "error_message": args[4]}
            )
        elif "INSERT INTO strategy_shadow_pairs" in query:
            self.pair_inserts.append(args)
            self.pairs[(args[15], args[16])] = {
                "id": args[0], "symbol": args[4], "frame_hash": args[10],
            }
        elif "INSERT INTO strategy_shadow_evaluations" in query:
            self.eval_inserts.append(args)
        elif "INSERT INTO strategy_shadow_run_pairs" in query:
            self.links.setdefault((str(args[0]), str(args[1])), args[2])


@pytest.fixture
def real_persistence_db(monkeypatch):
    db = _FakeShadowDB()

    async def get_conn():
        return db

    async def release(_conn):
        return None

    monkeypatch.setattr(shadow_persistence, "get_db_connection", get_conn)
    monkeypatch.setattr(shadow_persistence, "release_db_connection", release)

    async def fake_resolve(pattern_code, defaults):
        return dict(defaults)
    monkeypatch.setattr(shadow_runner, "resolve_pattern_config", fake_resolve)
    return db


class TestRealPairPersistence:
    """Full runner -> real persist_shadow_pair -> strict json.dumps flow.

    This is the exact path that failed live: no persistence stubs, only the
    DB driver itself is faked."""

    def test_jbl_like_v2_enter_v3_avoid_persists_one_pair(self, real_persistence_db):
        provider = FakeProvider({
            "JBLX": frame_to_payload(
                build_jbl_like_frame(touch_events=(322, 330, 338))
            ),
        })
        summary = _run(run_shadow_comparison(provider, ["JBLX"], now_utc=NOW_UTC))

        assert summary["status"] == "completed"
        assert summary["telemetry"]["pair_count"] == 1
        assert summary["telemetry"]["rejected_counts"] == {}
        assert len(real_persistence_db.pair_inserts) == 1
        assert len(real_persistence_db.eval_inserts) == 2

        pair = summary["pairs"][0]
        assert pair["control_verdict"] == "ENTER"
        assert pair["candidate_verdict"] == "AVOID"

        # The persisted v2 details_snapshot is strict JSON text whose bounce
        # dates are ISO strings (the live Timestamp is gone).
        by_arm = {ev[2]: ev for ev in real_persistence_db.eval_inserts}
        control_details = json.loads(by_arm[CONTROL_ARM_CODE][12])
        bounce_dates = [b["date"] for b in control_details["bounces_detail"]]
        assert bounce_dates and all(isinstance(d, str) for d in bounce_dates)
        # v2 has no evidence.v1; v3 does — both preserved verbatim.
        assert "evidence" not in control_details
        candidate_details = json.loads(by_arm[CANDIDATE_ARM_CODE][12])
        assert candidate_details["evidence"]["evidence_version"] == "evidence.v1"

    def test_dhr_like_pair_persists(self, real_persistence_db):
        provider = FakeProvider({
            "DHRX": frame_to_payload(
                build_uptrend_frame(trigger=False, vol_ratio=1.30)
            ),
        })
        summary = _run(run_shadow_comparison(provider, ["DHRX"], now_utc=NOW_UTC))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["pair_count"] == 1
        assert summary["telemetry"]["rejected_counts"] == {}
        assert len(real_persistence_db.pair_inserts) == 1
        assert len(real_persistence_db.eval_inserts) == 2

    def test_two_exact_runs_reuse_pair_and_add_occurrence(self, real_persistence_db):
        payloads = {
            "JBLX": frame_to_payload(
                build_jbl_like_frame(touch_events=(322, 330, 338))
            ),
        }
        first = _run(run_shadow_comparison(
            FakeProvider(payloads), ["JBLX"],
            run_id=str(uuid.uuid4()), now_utc=NOW_UTC,
        ))
        second = _run(run_shadow_comparison(
            FakeProvider(payloads), ["JBLX"],
            run_id=str(uuid.uuid4()), now_utc=NOW_UTC,
        ))
        assert first["telemetry"]["pairs_created"] == 1
        assert second["telemetry"]["pairs_created"] == 0
        assert second["telemetry"]["pairs_deduplicated"] == 1
        assert first["pairs"][0]["pair_id"] == second["pairs"][0]["pair_id"]
        assert len(real_persistence_db.pair_inserts) == 1   # pair frozen
        assert len(real_persistence_db.eval_inserts) == 2   # evaluations frozen
        assert len(real_persistence_db.links) == 2          # occurrence added

    def test_no_signal_tables_written(self, real_persistence_db):
        provider = FakeProvider({
            "DHRX": frame_to_payload(
                build_uptrend_frame(trigger=False, vol_ratio=1.30)
            ),
        })
        _run(run_shadow_comparison(provider, ["DHRX"], now_utc=NOW_UTC))
        assert real_persistence_db.forbidden == []

    def test_frame_config_and_telemetry_are_strict_json_text(self, real_persistence_db):
        provider = FakeProvider({
            "DHRX": frame_to_payload(
                build_uptrend_frame(trigger=False, vol_ratio=1.30)
            ),
        })
        run_id = str(uuid.uuid4())
        _run(run_shadow_comparison(
            provider, ["DHRX"], run_id=run_id, now_utc=NOW_UTC,
        ))
        # Frame snapshot column value round-trips as strict JSON.
        frame_text = real_persistence_db.pair_inserts[0][14]
        bars = json.loads(frame_text)
        assert isinstance(bars, list) and isinstance(bars[0]["date"], str)
        # Config snapshots round-trip too.
        for ev in real_persistence_db.eval_inserts:
            assert isinstance(json.loads(ev[7]), dict)
        # Run telemetry was stored via the strict serializer.
        telemetry_text = real_persistence_db.runs[run_id]["telemetry"]
        assert json.loads(telemetry_text)["pair_count"] == 1


# --------------------------------------------------------------------------- #
# 6. Migration boundary
# --------------------------------------------------------------------------- #

class TestMigrationBoundary:
    def test_migration_010_unchanged(self):
        """This fix is code-only: migration 010 stays byte-identical to the
        applied version."""
        text = (MIGRATIONS_DIR / "010_sma150_shadow_evaluations.sql").read_bytes()
        assert hashlib.sha256(text).hexdigest() == (
            "8b551311a2e421bb0a3bd8907970055b35122a79f1af714e4ba0b1a01d6c051d"
        )

    def test_exactly_migration_011_and_012_wyckoff_only(self):
        # Phase 8.1B2 added 011; Phase 9C2 adds exactly 012_wyckoff_mtf_v2.
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("011_*"))] == [
            "011_shadow_pair_outcomes.sql"
        ]
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("012_*"))] == [
            "012_wyckoff_mtf_v2.sql"
        ]
        # Phase 9D3 adds exactly 013_wyckoff_v2_shadow_arms (arm-code CHECK
        # extension only); nothing later exists.
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("013_*"))] == [
            "013_wyckoff_v2_shadow_arms.sql"
        ]
        assert not list(MIGRATIONS_DIR.glob("014_*"))
        sql = (MIGRATIONS_DIR / "012_wyckoff_mtf_v2.sql").read_text(encoding="utf-8")
        assert "strategy_shadow" not in sql.lower()
        assert "wyckoff_mtf_v2" in sql
