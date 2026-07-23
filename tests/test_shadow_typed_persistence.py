"""Phase 8.1B1 live pair_error fix #2: typed asyncpg parameter boundary.

Live failure (after JSON normalization was fixed):

    Shadow pair failed for JBL: invalid input for query argument $13:
    '2024-07-22' ('str' object has no attribute 'toordinal')

$13 of the strategy_shadow_pairs INSERT is frame_first_date. Migration 010
declares frame_first_date/frame_last_date as DATE; CanonicalFrame keeps them
as ISO strings (correct for hashing/JSON), and persist_shadow_pair passed the
strings straight to asyncpg, whose DATE codec calls .toordinal().

The previous fake DB driver accepted any Python object, so this codec
mismatch was invisible to the suite. This file adds a STRICT typed fake
connection that validates every parameter position against the exact
migration 010 column types and raises the same error asyncpg would.
"""

import asyncio
import hashlib
import json
import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import asyncpg

import app.workers.shadow.persistence as shadow_persistence
import app.workers.shadow.runner as shadow_runner
from app.workers.shadow.constants import (
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
    EVALUATION_FINGERPRINT_VERSION,
    FRAME_HARD_CAP_BARS,
    FRAME_SNAPSHOT_VERSION,
    PAIR_FINGERPRINT_VERSION,
)
from app.workers.shadow.frames import build_canonical_frame
from app.workers.shadow.runner import run_shadow_comparison
from app.workers.shadow.typed_values import (
    ShadowPersistenceTypeError,
    as_bool_param,
    as_date_param,
    as_int_param,
    as_score_param,
    as_utc_datetime_param,
    as_uuid_param,
)
from sma150_v3_frames import build_jbl_like_frame, build_uptrend_frame


MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"
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


# --------------------------------------------------------------------------- #
# 1. The exact live failure
# --------------------------------------------------------------------------- #

class TestLiveFailure:
    def test_canonical_frame_dates_are_iso_strings(self):
        """The raw ingredient of the bug: the canonical frame (correctly)
        keeps first/last dates as ISO strings for hashing and JSON."""
        payload = frame_to_payload(build_uptrend_frame(trigger=True))
        frame = build_canonical_frame(
            "REPRO", payload, max_bars=FRAME_HARD_CAP_BARS, now_utc=NOW_UTC
        )
        assert isinstance(frame.first_date, str)
        assert isinstance(frame.last_date, str)
        assert isinstance(frame.snapshot_date, date)

    def test_the_exact_codec_failure(self):
        """asyncpg's DATE codec calls .toordinal() on the parameter; an ISO
        string raises exactly the AttributeError observed live."""
        with pytest.raises(
            AttributeError, match="'str' object has no attribute 'toordinal'"
        ):
            "2024-07-22".toordinal()

    def test_strict_fake_reproduces_the_live_error_for_a_string_date(self):
        """A string reaching a DATE position raises the same asyncpg error
        class and message shape as the live failure."""
        conn = StrictTypedConn()
        with pytest.raises(asyncpg.DataError, match="toordinal"):
            _run(conn.execute(
                _PAIR_INSERT_SQL_MARKER,
                *_pair_args_with(frame_first_date="2024-07-22"),
            ))


# --------------------------------------------------------------------------- #
# 2. Typed converter contract
# --------------------------------------------------------------------------- #

class TestDateParam:
    def test_date_preserved(self):
        d = date(2026, 7, 17)
        assert as_date_param(d, "f") is d

    def test_datetime_explicitly_converted_to_date(self):
        dt = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
        assert as_date_param(dt, "f") == date(2026, 7, 17)

    def test_pandas_timestamp_converts_via_datetime_path(self):
        assert as_date_param(pd.Timestamp("2026-07-17"), "f") == date(2026, 7, 17)

    def test_iso_string_parsed(self):
        assert as_date_param("2024-07-22", "f") == date(2024, 7, 22)

    def test_malformed_string_rejects(self):
        with pytest.raises(ShadowPersistenceTypeError) as exc:
            as_date_param("22/07/2024", "frame_first_date")
        assert exc.value.reason_code == "invalid_iso_date"
        assert exc.value.field == "frame_first_date"
        assert "22/07/2024" not in str(exc.value)

    def test_other_types_reject(self):
        for bad in (20240722, None, 1.5):
            with pytest.raises(ShadowPersistenceTypeError):
                as_date_param(bad, "f")


class TestUtcDatetimeParam:
    def test_aware_normalizes_to_utc(self):
        tz3 = timezone(timedelta(hours=3))
        value = datetime(2026, 7, 17, 23, 0, tzinfo=tz3)
        out = as_utc_datetime_param(value, "f")
        assert out.tzinfo == timezone.utc
        assert out == value  # same instant
        assert out.hour == 20

    def test_utc_preserved(self):
        value = datetime(2026, 7, 17, tzinfo=timezone.utc)
        assert as_utc_datetime_param(value, "f") == value

    def test_naive_rejects(self):
        with pytest.raises(ShadowPersistenceTypeError) as exc:
            as_utc_datetime_param(datetime(2026, 7, 17), "market_data_as_of")
        assert exc.value.reason_code == "naive_datetime"

    def test_string_rejects(self):
        with pytest.raises(ShadowPersistenceTypeError):
            as_utc_datetime_param("2026-07-17T00:00:00+00:00", "f")


class TestUuidParam:
    def test_uuid_preserved(self):
        u = uuid.uuid4()
        assert as_uuid_param(u, "f") is u

    def test_canonical_string_parsed(self):
        u = uuid.uuid4()
        assert as_uuid_param(str(u), "f") == u

    def test_invalid_uuid_rejects(self):
        with pytest.raises(ShadowPersistenceTypeError) as exc:
            as_uuid_param("not-a-uuid", "run_id")
        assert exc.value.reason_code == "invalid_uuid"

    def test_other_types_reject(self):
        with pytest.raises(ShadowPersistenceTypeError):
            as_uuid_param(12345, "f")


class TestIntParam:
    def test_int_preserved(self):
        assert as_int_param(500, "f") == 500

    def test_bool_rejects(self):
        with pytest.raises(ShadowPersistenceTypeError) as exc:
            as_int_param(True, "frame_bar_count")
        assert exc.value.reason_code == "bool_not_int"

    def test_numpy_int_converts(self):
        out = as_int_param(np.int64(500), "f")
        assert out == 500 and type(out) is int

    def test_string_rejects(self):
        with pytest.raises(ShadowPersistenceTypeError):
            as_int_param("500", "f")


class TestScoreParam:
    def test_none_preserved(self):
        assert as_score_param(None, "f") is None

    def test_float_preserved(self):
        assert as_score_param(0.556, "f") == 0.556

    def test_int_becomes_float(self):
        out = as_score_param(1, "f")
        assert out == 1.0 and type(out) is float

    def test_numpy_float_converts(self):
        out = as_score_param(np.float64(0.5), "f")
        assert out == 0.5 and type(out) is float

    def test_bool_rejects(self):
        with pytest.raises(ShadowPersistenceTypeError):
            as_score_param(True, "score")

    def test_non_finite_rejects(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ShadowPersistenceTypeError) as exc:
                as_score_param(bad, "score")
            assert exc.value.reason_code == "non_finite_score"


class TestBoolParam:
    def test_bool_preserved(self):
        assert as_bool_param(True, "f") is True
        assert as_bool_param(False, "f") is False

    def test_non_bool_rejects(self):
        for bad in (1, 0, "true", None):
            with pytest.raises(ShadowPersistenceTypeError):
                as_bool_param(bad, "created_new_pair")


# --------------------------------------------------------------------------- #
# 3. Strict typed fake connection (validates asyncpg codec expectations)
# --------------------------------------------------------------------------- #

_PAIR_INSERT_SQL_MARKER = "INSERT INTO strategy_shadow_pairs"


def _codec_error(position: int, value, expected: str):
    """The same error class/shape asyncpg raises for a codec mismatch."""
    return asyncpg.DataError(
        f"invalid input for query argument ${position}: "
        f"got {type(value).__name__}, expected {expected}"
    )


def _check(position: int, value, kind: str):
    """Validate one parameter against its migration 010 column type."""
    if kind == "uuid":
        if not isinstance(value, uuid.UUID):
            raise _codec_error(position, value, "uuid")
    elif kind == "date":
        # asyncpg DATE codec: needs date; our boundary guarantees exact
        # datetime.date (never datetime, never str).
        if isinstance(value, str):
            raise asyncpg.DataError(
                f"invalid input for query argument ${position}: {value!r} "
                "('str' object has no attribute 'toordinal')"
            )
        if not isinstance(value, date) or isinstance(value, datetime):
            raise _codec_error(position, value, "date")
    elif kind == "timestamptz":
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise _codec_error(position, value, "aware datetime")
    elif kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise _codec_error(position, value, "int")
    elif kind == "int_or_null":
        if value is not None:
            _check(position, value, "int")
    elif kind == "float_or_null":
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, float)
        ):
            raise _codec_error(position, value, "float")
    elif kind == "bool":
        if not isinstance(value, bool):
            raise _codec_error(position, value, "bool")
    elif kind == "jsonb":
        if not isinstance(value, str):
            raise _codec_error(position, value, "jsonb text")
        json.loads(value)  # must be strict valid JSON
    elif kind == "jsonb_or_null":
        if value is not None:
            _check(position, value, "jsonb")
    elif kind == "text":
        if not isinstance(value, str):
            raise _codec_error(position, value, "text")
    elif kind == "text_or_null":
        if value is not None and not isinstance(value, str):
            raise _codec_error(position, value, "text")
    else:  # pragma: no cover
        raise AssertionError(f"unknown kind {kind}")


# Parameter-position type maps for every statement persist code executes,
# in exact migration 010 column order.
_RUN_INSERT_KINDS = ["uuid", "text", "text", "text_or_null", "jsonb",
                     "int_or_null"]
_RUN_UPDATE_KINDS = ["uuid", "text", "jsonb_or_null", "text_or_null",
                     "text_or_null"]
_PAIR_INSERT_KINDS = ["uuid", "uuid", "text", "text", "text", "text",
                      "text_or_null", "date", "timestamptz", "text", "text",
                      "int", "date", "date", "jsonb", "text", "text"]
_EVAL_INSERT_KINDS = ["uuid", "uuid", "text", "text", "text", "text", "text",
                      "jsonb", "text", "float_or_null", "text_or_null",
                      "text_or_null", "jsonb", "text_or_null", "text", "text"]
_LINK_INSERT_KINDS = ["uuid", "uuid", "bool"]


class _FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class StrictTypedConn:
    """Fake asyncpg connection that ENFORCES migration 010 column types on
    every parameter position, raising asyncpg.DataError exactly like the
    real driver would for a codec mismatch."""

    def __init__(self):
        self.runs = {}
        self.pairs = {}       # (fingerprint, version) -> row
        self.pair_inserts = []
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
            self._validate(args, _RUN_INSERT_KINDS)
            self.runs[str(args[0])] = {"status": "running", "telemetry": None}
        elif "UPDATE strategy_shadow_runs" in query:
            self._validate(args, _RUN_UPDATE_KINDS)
            self.runs[str(args[0])].update(
                {"status": args[1], "telemetry": args[2]}
            )
        elif _PAIR_INSERT_SQL_MARKER in query:
            self._validate(args, _PAIR_INSERT_KINDS)
            self.pair_inserts.append(args)
            self.pairs[(args[15], args[16])] = {
                "id": args[0], "symbol": args[4], "frame_hash": args[10],
            }
        elif "INSERT INTO strategy_shadow_evaluations" in query:
            self._validate(args, _EVAL_INSERT_KINDS)
            self.eval_inserts.append(args)
        elif "INSERT INTO strategy_shadow_run_pairs" in query:
            self._validate(args, _LINK_INSERT_KINDS)
            self.links.setdefault((str(args[0]), str(args[1])), args[2])

    @staticmethod
    def _validate(args, kinds):
        assert len(args) == len(kinds), (len(args), len(kinds))
        for i, (value, kind) in enumerate(zip(args, kinds), start=1):
            _check(i, value, kind)


def _pair_args_with(**overrides):
    """A fully typed pair-insert argument tuple, with overrides for
    negative tests."""
    base = {
        "id": uuid.uuid4(),
        "origin_run_id": uuid.uuid4(),
        "experiment_code": "sma150_v2_vs_v3",
        "experiment_version": "sma150_shadow.v1",
        "symbol": "JBL",
        "timeframe": "1d",
        "provider": "massive",
        "snapshot_date": date(2026, 7, 17),
        "market_data_as_of": datetime(2026, 7, 17, tzinfo=timezone.utc),
        "frame_snapshot_version": FRAME_SNAPSHOT_VERSION,
        "frame_hash": "fh",
        "frame_bar_count": 500,
        "frame_first_date": date(2024, 7, 22),
        "frame_last_date": date(2026, 7, 17),
        "frame_snapshot": "[]",
        "pair_fingerprint": "fp",
        "pair_fingerprint_version": PAIR_FINGERPRINT_VERSION,
    }
    base.update(overrides)
    return tuple(base.values())


@pytest.fixture
def strict_db(monkeypatch):
    db = StrictTypedConn()

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


class FakeProvider:
    name = "fake_provider"

    def __init__(self, payloads):
        self.payloads = payloads

    async def get_daily_history(self, symbol, timeseries=400):
        return self.payloads[symbol]


# --------------------------------------------------------------------------- #
# 4. End-to-end persistence through the strict typed fake
# --------------------------------------------------------------------------- #

class TestTypedEndToEnd:
    def test_jbl_like_pair_persists_with_exact_driver_types(self, strict_db):
        provider = FakeProvider({
            "JBLX": frame_to_payload(
                build_jbl_like_frame(touch_events=(322, 330, 338))
            ),
        })
        summary = _run(run_shadow_comparison(provider, ["JBLX"], now_utc=NOW_UTC))

        assert summary["status"] == "completed"
        assert summary["telemetry"]["pair_count"] == 1
        assert summary["telemetry"]["rejected_counts"] == {}
        assert summary["pairs"][0]["control_verdict"] == "ENTER"
        assert summary["pairs"][0]["candidate_verdict"] == "AVOID"

        args = strict_db.pair_inserts[0]
        # DATE columns arrive as datetime.date (exactly — never str/datetime).
        for idx in (7, 12, 13):   # snapshot_date, frame_first/last_date
            assert type(args[idx]) is date, idx
        # TIMESTAMPTZ arrives timezone-aware in UTC.
        assert args[8].tzinfo == timezone.utc
        # UUIDs are uuid.UUID.
        assert isinstance(args[0], uuid.UUID)
        assert isinstance(args[1], uuid.UUID)
        # INT is int, not bool.
        assert type(args[11]) is int
        # JSONB is strict JSON text.
        assert isinstance(json.loads(args[14]), list)

        for ev in strict_db.eval_inserts:
            assert isinstance(ev[0], uuid.UUID) and isinstance(ev[1], uuid.UUID)
            assert ev[9] is None or type(ev[9]) is float
            json.loads(ev[7]); json.loads(ev[12])

    def test_dhr_like_pair_persists(self, strict_db):
        provider = FakeProvider({
            "DHRX": frame_to_payload(
                build_uptrend_frame(trigger=False, vol_ratio=1.30)
            ),
        })
        summary = _run(run_shadow_comparison(provider, ["DHRX"], now_utc=NOW_UTC))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["pair_count"] == 1
        assert summary["telemetry"]["rejected_counts"] == {}
        assert len(strict_db.pair_inserts) == 1
        assert len(strict_db.eval_inserts) == 2

    def test_exact_rerun_reuses_pair_and_adds_occurrence(self, strict_db):
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
        assert first["pairs"][0]["pair_id"] == second["pairs"][0]["pair_id"]
        assert second["telemetry"]["pairs_deduplicated"] == 1
        assert len(strict_db.pair_inserts) == 1
        assert len(strict_db.eval_inserts) == 2
        assert len(strict_db.links) == 2

    def test_no_signal_tables_written(self, strict_db):
        provider = FakeProvider({
            "DHRX": frame_to_payload(
                build_uptrend_frame(trigger=False, vol_ratio=1.30)
            ),
        })
        _run(run_shadow_comparison(provider, ["DHRX"], now_utc=NOW_UTC))
        assert strict_db.forbidden == []

    def test_frame_hash_and_fingerprint_unchanged_by_typed_boundary(self, strict_db):
        """The typed boundary converts DRIVER parameters only: the canonical
        frame JSON still stores ISO date strings and the pair fingerprint is
        computed before persistence, from the canonical identities."""
        provider = FakeProvider({
            "DHRX": frame_to_payload(
                build_uptrend_frame(trigger=False, vol_ratio=1.30)
            ),
        })
        _run(run_shadow_comparison(provider, ["DHRX"], now_utc=NOW_UTC))
        args = strict_db.pair_inserts[0]
        bars = json.loads(args[14])
        assert isinstance(bars[0]["date"], str)      # frame JSON untouched
        assert args[16] == PAIR_FINGERPRINT_VERSION  # fingerprint version


# --------------------------------------------------------------------------- #
# 5. Error classification
# --------------------------------------------------------------------------- #

class _RunStore:
    def __init__(self):
        self.runs = {}

    async def create_run(self, run_id, **kwargs):
        self.runs[str(run_id)] = {"status": "running", **kwargs}
        return str(run_id)

    async def finalize_run(self, run_id, **kwargs):
        self.runs[str(run_id)].update(kwargs)


class TestErrorClassification:
    def _base_patches(self, monkeypatch):
        store = _RunStore()
        monkeypatch.setattr(shadow_runner, "create_shadow_run", store.create_run)
        monkeypatch.setattr(shadow_runner, "finalize_shadow_run", store.finalize_run)

        async def fake_resolve(pattern_code, defaults):
            return dict(defaults)
        monkeypatch.setattr(shadow_runner, "resolve_pattern_config", fake_resolve)
        return store

    def _provider(self):
        return FakeProvider({
            "DHRX": frame_to_payload(
                build_uptrend_frame(trigger=False, vol_ratio=1.30)
            ),
        })

    def test_type_error_classified_as_persistence_type_error(self, monkeypatch):
        self._base_patches(monkeypatch)

        async def typed_failure(**kwargs):
            raise ShadowPersistenceTypeError("invalid_iso_date", "frame_first_date")
        monkeypatch.setattr(shadow_runner, "persist_shadow_pair", typed_failure)

        summary = _run(run_shadow_comparison(self._provider(), ["DHRX"],
                                             now_utc=NOW_UTC))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["rejected_counts"] == {
            "persistence_type_error": 1
        }
        assert summary["telemetry"]["rejected_symbols"] == {
            "persistence_type_error": ["DHRX"]
        }

    def test_unexpected_db_failure_remains_pair_error(self, monkeypatch):
        self._base_patches(monkeypatch)

        async def db_failure(**kwargs):
            raise asyncpg.PostgresError("connection lost")
        monkeypatch.setattr(shadow_runner, "persist_shadow_pair", db_failure)

        summary = _run(run_shadow_comparison(self._provider(), ["DHRX"],
                                             now_utc=NOW_UTC))
        assert summary["telemetry"]["rejected_counts"] == {"pair_error": 1}

    def test_type_error_message_contains_no_raw_value(self):
        exc = ShadowPersistenceTypeError("invalid_iso_date", "frame_first_date")
        assert str(exc) == "invalid_iso_date for field frame_first_date"


# --------------------------------------------------------------------------- #
# 6. Optional real-PostgreSQL integration (skipped without TEST_DATABASE_URL)
# --------------------------------------------------------------------------- #

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL not set; real-codec integration test skipped",
)
class TestPostgresIntegration:
    def test_run_pair_evaluations_roundtrip_and_rollback(self, monkeypatch):
        """Real asyncpg codecs, real migration-010 tables, rolled back."""
        from app.workers.shadow.persistence import (
            create_shadow_run,
            persist_shadow_pair,
        )

        async def scenario():
            conn = await asyncpg.connect(TEST_DATABASE_URL)
            tx = conn.transaction()
            await tx.start()
            try:
                async def get_conn():
                    return conn

                async def release(_conn):
                    return None

                monkeypatch.setattr(
                    shadow_persistence, "get_db_connection", get_conn
                )
                monkeypatch.setattr(
                    shadow_persistence, "release_db_connection", release
                )

                run_id = str(uuid.uuid4())
                await create_shadow_run(
                    run_id, provider="integration",
                    requested_symbols=["ITGX"], requested_limit=1,
                )
                result = await persist_shadow_pair(
                    run_id=run_id,
                    pair={
                        "experiment_code": "sma150_v2_vs_v3",
                        "experiment_version": "sma150_shadow.v1",
                        "symbol": "ITGX",
                        "timeframe": "1d",
                        "provider": "integration",
                        "snapshot_date": date(2026, 7, 17),
                        "market_data_as_of": datetime(
                            2026, 7, 17, tzinfo=timezone.utc
                        ),
                        "frame_snapshot_version": FRAME_SNAPSHOT_VERSION,
                        "frame_hash": f"itg-{uuid.uuid4()}",
                        "frame_bar_count": 2,
                        # ISO strings, exactly like CanonicalFrame produces.
                        "frame_first_date": "2026-07-16",
                        "frame_last_date": "2026-07-17",
                        "frame_snapshot": [
                            {"date": "2026-07-16", "open": 1.0, "high": 2.0,
                             "low": 0.5, "close": 1.5, "volume": 10.0},
                            {"date": "2026-07-17", "open": 1.5, "high": 2.5,
                             "low": 1.0, "close": 2.0, "volume": 12.0},
                        ],
                        "pair_fingerprint": f"itg-fp-{uuid.uuid4()}",
                        "pair_fingerprint_version": PAIR_FINGERPRINT_VERSION,
                    },
                    evaluations=[
                        {
                            "arm_code": arm,
                            "strategy_code": code,
                            "strategy_version": version,
                            "decision_policy_version": policy,
                            "config_hash": "itg-ch",
                            "config_snapshot": {"sma_window": 150},
                            "verdict": verdict,
                            "score": score,
                            "reason": "integration",
                            "rejection_reason": None,
                            "details_snapshot": {"snapshot_date": "2026-07-17"},
                            "evidence_original_sha256": "itg-sha",
                            "evaluation_fingerprint": f"itg-ef-{uuid.uuid4()}",
                            "evaluation_fingerprint_version":
                                EVALUATION_FINGERPRINT_VERSION,
                        }
                        for arm, code, version, policy, verdict, score in (
                            (CONTROL_ARM_CODE, "sma150_bounce", "sma150.v2",
                             "strategy_decision.v1", "ENTER", 0.7),
                            (CANDIDATE_ARM_CODE, "sma150_bounce_v3", "sma150.v3",
                             "sma150_bounce.policy.v1", "AVOID", None),
                        )
                    ],
                )
                assert result["created_new_pair"] is True

                stored = await conn.fetchrow(
                    "SELECT snapshot_date, frame_first_date, frame_last_date "
                    "FROM strategy_shadow_pairs WHERE id = $1",
                    uuid.UUID(result["pair_id"]),
                )
                assert stored["frame_first_date"] == date(2026, 7, 16)
                assert stored["frame_last_date"] == date(2026, 7, 17)
            finally:
                await tx.rollback()
                await conn.close()

        _run(scenario())


# --------------------------------------------------------------------------- #
# 7. Migration boundary and version freeze
# --------------------------------------------------------------------------- #

class TestBoundaries:
    def test_migration_010_byte_identical(self):
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
        assert not list(MIGRATIONS_DIR.glob("013_*"))
        sql = (MIGRATIONS_DIR / "012_wyckoff_mtf_v2.sql").read_text(encoding="utf-8")
        assert "strategy_shadow" not in sql.lower()
        assert "wyckoff_mtf_v2" in sql

    def test_fingerprint_versions_unchanged(self):
        assert FRAME_SNAPSHOT_VERSION == "daily_ohlcv_snapshot.v1"
        assert PAIR_FINGERPRINT_VERSION == "shadow_pair_fingerprint.v1"
        assert EVALUATION_FINGERPRINT_VERSION == "shadow_evaluation_fingerprint.v1"
