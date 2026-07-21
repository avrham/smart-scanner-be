"""Phase 8.1B1: frozen paired shadow evaluations of sma150.v2 vs sma150.v3.

Deterministic unit tests only — no DB, no providers, no live calls. Covers:
migration 010, the canonical shared frame, strategy/config identity,
paired decisions (including the JBL-like v2-ENTER/v3-AVOID case),
immutability + occurrence linking, separation from normal signals/outcomes,
and the admin/read API contracts.
"""

import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

import app.routers.shadow as shadow_router_mod
import app.workers.shadow.persistence as shadow_persistence
import app.workers.shadow.runner as shadow_runner
from app.workers.provenance import config_hash
from app.workers.shadow.constants import (
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
    EXPERIMENT_CODE,
    EXPERIMENT_VERSION,
    FRAME_FETCH_MARGIN_BARS,
    FRAME_HARD_CAP_BARS,
    FRAME_SNAPSHOT_VERSION,
    MAX_FRAME_SNAPSHOT_BYTES,
    MAX_SHADOW_SYMBOLS,
    PAIR_FINGERPRINT_VERSION,
)
from app.workers.shadow.fingerprints import (
    compute_evaluation_fingerprint,
    compute_pair_fingerprint,
    disagreement_category,
)
from app.workers.shadow.frames import (
    FrameRejection,
    build_canonical_frame,
    compute_frame_hash,
    required_history_bars_v2,
    required_history_bars_v3,
    shared_required_history_bars,
)
from app.workers.shadow.persistence import ShadowIntegrityError, persist_shadow_pair
from app.workers.shadow.runner import (
    ShadowRequestError,
    normalize_shadow_symbols,
    run_shadow_comparison,
)
from main import app
from sma150_v3_frames import build_jbl_like_frame, build_uptrend_frame


MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"
MIGRATION_010 = MIGRATIONS_DIR / "010_sma150_shadow_evaluations.sql"
SHADOW_PKG = Path(__file__).resolve().parents[1] / "app" / "workers" / "shadow"

# All frames end well before this instant -> latest bar is a completed prior
# session under ny_session_close.v1, deterministically.
NOW_UTC = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


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


class FakeProvider:
    name = "fake_provider"

    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []
        self.requested_timeseries = []

    async def get_daily_history(self, symbol, timeseries=400):
        self.calls.append(symbol)
        self.requested_timeseries.append(timeseries)
        payload = self.payloads[symbol]
        if isinstance(payload, Exception):
            raise payload
        return payload


class ShadowStore:
    """In-memory stand-in for the strategy_shadow_* tables, mirroring the
    real insert-or-link semantics (used for runner-level tests)."""

    def __init__(self):
        self.runs = {}
        self.pairs = {}       # fingerprint -> {"id", "pair", "evaluations", "origin_run_id"}
        self.links = []

    async def create_run(self, run_id, **kwargs):
        self.runs[str(run_id)] = {"status": "running", **kwargs}
        return str(run_id)

    async def finalize_run(self, run_id, **kwargs):
        self.runs[str(run_id)].update(kwargs)

    async def persist_pair(self, *, run_id, pair, evaluations):
        fp = pair["pair_fingerprint"]
        if fp in self.pairs:
            existing = self.pairs[fp]
            if (
                existing["pair"]["symbol"] != pair["symbol"]
                or existing["pair"]["frame_hash"] != pair["frame_hash"]
            ):
                raise ShadowIntegrityError("incompatible fingerprint reuse")
            pair_id, created = existing["id"], False
        else:
            pair_id = str(uuid.uuid4())
            self.pairs[fp] = {
                "id": pair_id,
                "pair": pair,
                "evaluations": evaluations,
                "origin_run_id": str(run_id),
            }
            created = True
        self.links.append(
            {"run_id": str(run_id), "pair_id": pair_id, "created_new_pair": created}
        )
        return {"pair_id": pair_id, "created_new_pair": created}


@pytest.fixture
def store(monkeypatch):
    s = ShadowStore()
    monkeypatch.setattr(shadow_runner, "create_shadow_run", s.create_run)
    monkeypatch.setattr(shadow_runner, "finalize_shadow_run", s.finalize_run)
    monkeypatch.setattr(shadow_runner, "persist_shadow_pair", s.persist_pair)
    return s


@pytest.fixture
def default_configs(monkeypatch):
    """resolve_pattern_config -> strategy defaults (no DB)."""
    async def fake_resolve(pattern_code, defaults):
        return dict(defaults)
    monkeypatch.setattr(shadow_runner, "resolve_pattern_config", fake_resolve)
    return fake_resolve


def _run(coro):
    return asyncio.run(coro)


def _payloads():
    return {
        "ENTRX": frame_to_payload(build_uptrend_frame(trigger=True)),
        "WTCHX": frame_to_payload(
            build_uptrend_frame(trigger=False, vol_ratio=1.30)
        ),
        "JBLX": frame_to_payload(
            build_jbl_like_frame(touch_events=(322, 330, 338))
        ),
    }


# --------------------------------------------------------------------------- #
# Migration 010
# --------------------------------------------------------------------------- #

class TestMigration010:
    def _statements(self):
        text = MIGRATION_010.read_text()
        lines = [
            line for line in text.splitlines()
            if not line.strip().startswith("--")
        ]
        return "\n".join(lines)

    def test_migration_file_exists_with_exact_name(self):
        assert MIGRATION_010.exists()

    def test_exactly_migration_011_no_012(self):
        # Phase 8.1B2 adds exactly ONE new migration; 010 stays untouched.
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("011_*"))] == [
            "011_shadow_pair_outcomes.sql"
        ]
        assert not list(MIGRATIONS_DIR.glob("012_*"))

    def test_additive_and_idempotent(self):
        sql = self._statements()
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "CREATE INDEX IF NOT EXISTS" in sql
        # No destructive/mutating statements (FK "ON DELETE ..." clauses are
        # declarations, not statements, and are checked separately below).
        for forbidden in ("DROP ", "DELETE FROM", "TRUNCATE", "DO UPDATE",
                          "ALTER TABLE", "INSERT INTO"):
            assert forbidden not in sql, forbidden
        import re
        assert not re.search(r"^\s*UPDATE\b", sql, re.MULTILINE)

    def test_no_existing_table_modified(self):
        sql = self._statements()
        for table in ("signals", "signal_provenance", "signal_outcomes",
                      "scan_run_signals", "pattern_runs", "pattern_configs"):
            assert f"public.{table}" not in sql.replace(
                "strategy_shadow_", "SHADOW_"
            ), table

    def test_all_four_tables_created(self):
        sql = self._statements()
        for table in ("strategy_shadow_runs", "strategy_shadow_pairs",
                      "strategy_shadow_run_pairs", "strategy_shadow_evaluations"):
            assert f"CREATE TABLE IF NOT EXISTS public.{table}" in sql

    def test_constraints_and_indexes_present(self):
        sql = self._statements()
        # Run status lifecycle.
        assert "CHECK (status IN ('running', 'completed', 'failed'))" in sql
        # Arm and verdict domains.
        assert "CHECK (arm_code IN ('control_v2', 'candidate_v3'))" in sql
        assert "CHECK (verdict IN ('ENTER', 'WATCH', 'AVOID'))" in sql
        # Immutable-identity uniqueness.
        assert "strategy_shadow_pairs_fingerprint_uniq" in sql
        assert "(pair_fingerprint, pair_fingerprint_version)" in sql
        assert "strategy_shadow_evaluations_fingerprint_uniq" in sql
        assert "UNIQUE (pair_id, arm_code)" in sql
        # Occurrence PK.
        assert "PRIMARY KEY (run_id, pair_id)" in sql

    def test_safe_delete_behavior(self):
        sql = self._statements()
        # Deleting a run must not destroy immutable pair evidence.
        assert "origin_run_id UUID REFERENCES public.strategy_shadow_runs(id) "\
               "ON DELETE SET NULL" in sql


# --------------------------------------------------------------------------- #
# Canonical shared frame
# --------------------------------------------------------------------------- #

class TestCanonicalFrame:
    def test_chronological_order_enforced(self):
        payload = frame_to_payload(build_uptrend_frame())
        payload["historical"] = list(reversed(payload["historical"]))
        frame = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        dates = [b["date"] for b in frame.bars]
        assert dates == sorted(dates)

    def test_reversed_provider_ordering_yields_same_frame_and_hash(self):
        """Canonicalization happens BEFORE hashing: raw provider ordering is
        irrelevant to both the stored frame and its hash."""
        ascending = frame_to_payload(build_uptrend_frame())
        descending = {"historical": list(reversed(ascending["historical"]))}
        f_asc = build_canonical_frame("T", ascending, now_utc=NOW_UTC)
        f_desc = build_canonical_frame("T", descending, now_utc=NOW_UTC)
        assert f_asc.bars == f_desc.bars
        assert f_asc.frame_hash == f_desc.frame_hash

    def test_shuffled_provider_ordering_yields_same_frame_and_hash(self):
        import random
        payload = frame_to_payload(build_uptrend_frame())
        shuffled = {"historical": list(payload["historical"])}
        random.Random(42).shuffle(shuffled["historical"])
        f_orig = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        f_shuf = build_canonical_frame("T", shuffled, now_utc=NOW_UTC)
        assert f_orig.bars == f_shuf.bars
        assert f_orig.frame_hash == f_shuf.frame_hash

    def test_duplicate_session_dates_rejected(self):
        payload = frame_to_payload(build_uptrend_frame())
        payload["historical"].append(dict(payload["historical"][-1]))
        with pytest.raises(FrameRejection) as exc:
            build_canonical_frame("T", payload, now_utc=NOW_UTC)
        assert exc.value.reason_code == "duplicate_session_date"

    @pytest.mark.parametrize("mutation", [
        {"close": None},
        {"close": float("nan")},
        {"close": float("inf")},
        {"open": "abc"},
        {"low": -5.0},
        {"volume": -1.0},
        {"high": True},
    ])
    def test_malformed_ohlcv_rejected(self, mutation):
        payload = frame_to_payload(build_uptrend_frame())
        payload["historical"][5].update(mutation)
        with pytest.raises(FrameRejection) as exc:
            build_canonical_frame("T", payload, now_utc=NOW_UTC)
        assert exc.value.reason_code in ("malformed_ohlcv", "malformed_bar")

    def test_empty_payload_rejected(self):
        with pytest.raises(FrameRejection) as exc:
            build_canonical_frame("T", {"historical": []}, now_utc=NOW_UTC)
        assert exc.value.reason_code == "no_data"

    def test_partial_current_session_bar_excluded(self):
        # Session in progress: 11:00 New York on the last bar's date.
        now = datetime(2024, 6, 14, 15, 0, tzinfo=timezone.utc)
        payload = {"historical": [
            {"date": "2024-06-12", "open": 10, "high": 11, "low": 9,
             "close": 10.5, "volume": 1000},
            {"date": "2024-06-13", "open": 10.5, "high": 11.5, "low": 10,
             "close": 11.0, "volume": 1100},
            {"date": "2024-06-14", "open": 11, "high": 12, "low": 10.5,
             "close": 11.5, "volume": 500},   # still-open session
        ]}
        frame = build_canonical_frame("T", payload, now_utc=now)
        assert frame.last_date == "2024-06-13"
        assert frame.excluded_partial_bar_date == "2024-06-14"
        assert frame.completion["excluded_partial_bar_date"] == "2024-06-14"
        assert frame.completion["canonical_as_of_date"] == "2024-06-13"
        assert frame.completion["policy"] == "ny_session_close.v1"
        assert frame.market_data_as_of == datetime(
            2024, 6, 13, tzinfo=timezone.utc
        )

    def test_partial_bar_exclusion_preserves_depth_accounting(self):
        """The partial bar is excluded BEFORE the depth cap, so exclusion
        never shrinks an otherwise-full completed frame."""
        now = datetime(2024, 6, 14, 15, 0, tzinfo=timezone.utc)  # 11:00 NY
        payload = {"historical": [
            {"date": f"2024-06-{d:02d}", "open": 10, "high": 11, "low": 9,
             "close": 10.5, "volume": 1000}
            for d in (10, 11, 12, 13, 14)   # 14th is the open session
        ]}
        frame = build_canonical_frame("T", payload, max_bars=3, now_utc=now)
        # 5 raw bars -> partial 06-14 excluded -> 4 completed -> capped to 3.
        assert frame.excluded_partial_bar_date == "2024-06-14"
        assert frame.bar_count == 3
        assert [b["date"] for b in frame.bars] == [
            "2024-06-11", "2024-06-12", "2024-06-13"
        ]
        assert frame.last_date == "2024-06-13"

    def test_max_stored_frame_stays_within_byte_bound(self):
        """A hard-cap-sized frame with realistic float precision serializes
        far inside the 512 KiB bound — the limit safely accommodates the
        maximum canonical frame."""
        from app.workers.provenance import canonical_json
        bars = [
            {"date": f"2y{i:04d}", "open": 12345.678901, "high": 12399.987654,
             "low": 12300.123456, "close": 12350.554433,
             "volume": 987654321.0}
            for i in range(FRAME_HARD_CAP_BARS)
        ]
        size = len(canonical_json(bars).encode("utf-8"))
        assert size < MAX_FRAME_SNAPSHOT_BYTES
        # Generous headroom, not a near-miss.
        assert size < MAX_FRAME_SNAPSHOT_BYTES // 4

    def test_unknown_completion_rejects_the_pair(self):
        # Future-dated bar: completion cannot be proven.
        now = datetime(2024, 6, 14, 15, 0, tzinfo=timezone.utc)
        payload = {"historical": [
            {"date": "2024-06-20", "open": 10, "high": 11, "low": 9,
             "close": 10.5, "volume": 1000},
        ]}
        with pytest.raises(FrameRejection) as exc:
            build_canonical_frame("T", payload, now_utc=now)
        assert exc.value.reason_code == "unconfirmed_bar_completion"

    def test_only_partial_bar_rejects(self):
        now = datetime(2024, 6, 14, 15, 0, tzinfo=timezone.utc)
        payload = {"historical": [
            {"date": "2024-06-14", "open": 10, "high": 11, "low": 9,
             "close": 10.5, "volume": 1000},
        ]}
        with pytest.raises(FrameRejection):
            build_canonical_frame("T", payload, now_utc=now)

    def test_frame_capped_to_requested_depth(self):
        df = build_uptrend_frame(n=430)
        payload = frame_to_payload(df)
        frame = build_canonical_frame(
            "T", payload, max_bars=300, now_utc=NOW_UTC
        )
        assert frame.bar_count == 300
        # Most RECENT bars kept, chronological order preserved.
        assert frame.bars[-1]["date"] == payload["historical"][-1]["date"]
        dates = [b["date"] for b in frame.bars]
        assert dates == sorted(dates)

    def test_default_cap_is_the_hard_ceiling(self):
        df = build_uptrend_frame(n=430)
        payload = frame_to_payload(df)
        frame = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        # 430 completed bars < 600 hard cap: nothing is cut.
        assert frame.bar_count == 430
        # A requested depth can never exceed the documented hard ceiling.
        oversize = build_canonical_frame(
            "T", payload, max_bars=FRAME_HARD_CAP_BARS + 500, now_utc=NOW_UTC
        )
        assert oversize.bar_count <= FRAME_HARD_CAP_BARS

    def test_provider_shortfall_is_not_a_frame_rejection(self):
        """Fewer completed bars than requested is honest data, not an error:
        the frame is built and the strategies' readiness rules decide."""
        df = build_uptrend_frame(n=430)
        payload = frame_to_payload(df)
        frame = build_canonical_frame(
            "T", payload, max_bars=515, now_utc=NOW_UTC
        )
        assert frame.bar_count == 430    # all available bars, no fabrication

    def test_both_arms_get_equivalent_independent_copies(self):
        payload = frame_to_payload(build_uptrend_frame())
        frame = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        df_a = frame.dataframe()
        df_b = frame.dataframe()
        pd.testing.assert_frame_equal(df_a, df_b)
        assert df_a is not df_b
        df_a.loc[0, "close"] = 1.0   # mutating one copy...
        assert float(df_b["close"].iloc[0]) != 1.0     # ...never leaks
        assert frame.bars[0]["close"] != 1.0

    def test_frame_hash_changes_when_any_value_changes(self):
        payload = frame_to_payload(build_uptrend_frame())
        f1 = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        # Mutate a bar inside the capped (most recent) window.
        payload["historical"][-10]["close"] += 0.01
        f2 = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        assert f1.frame_hash != f2.frame_hash

    def test_frame_hash_changes_when_a_canonical_date_changes(self):
        payload = frame_to_payload(build_uptrend_frame())
        f1 = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        # Shift one non-final bar's date to an unused session date.
        payload["historical"][-3]["date"] = "2025-12-24"
        f2 = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        assert f1.frame_hash != f2.frame_hash

    def test_frame_hash_covers_stored_canonical_order(self):
        """The hash is over the STORED canonical (chronological) bar list —
        semantic list order enters the hash and is never alphabetically or
        recursively reordered by the canonical-JSON serializer. (The builder
        guarantees the stored order is chronological regardless of provider
        ordering — covered above.)"""
        bars = [
            {"date": "2024-01-02", "open": 1.0, "high": 2.0, "low": 0.5,
             "close": 1.5, "volume": 10.0},
            {"date": "2024-01-03", "open": 1.5, "high": 2.5, "low": 1.0,
             "close": 2.0, "volume": 11.0},
        ]
        assert compute_frame_hash(bars) != compute_frame_hash(list(reversed(bars)))

    def test_frame_snapshot_version_recorded(self):
        payload = frame_to_payload(build_uptrend_frame())
        frame = build_canonical_frame("T", payload, now_utc=NOW_UTC)
        assert frame.frame_snapshot_version == FRAME_SNAPSHOT_VERSION == \
            "daily_ohlcv_snapshot.v1"


# --------------------------------------------------------------------------- #
# History-depth contract (derived from resolved configs, SMA warm-up counted)
# --------------------------------------------------------------------------- #

class TestHistoryDepth:
    def _v2_defaults(self):
        from app.workers.patterns.sma150 import DEFAULT_CONFIG
        return DEFAULT_CONFIG.copy()

    def _v3_defaults(self):
        from app.workers.strategies import get_strategy
        return get_strategy("sma150_bounce_v3").default_config()

    def test_v3_defaults_require_515_completed_bars(self):
        """(sma_window - 1) + lookback_bars_for_history + 1 =
        149 + 365 + 1 = 515: the SMA warm-up counts — a bar can only join
        the bounce lookback once its SMA-150 is valid."""
        assert required_history_bars_v3(self._v3_defaults()) == 515

    def test_v2_defaults_require_515_completed_bars(self):
        """v2 slices its historical window over SMA-valid bars the same way
        (lookback_days_for_history is a bar-count offset), so its full
        configured lookback also needs 149 + 365 + 1."""
        assert required_history_bars_v2(self._v2_defaults()) == 515

    def test_requirement_tracks_sma_window_deterministically(self):
        cfg = self._v3_defaults()
        cfg["sma_window"] = 100
        assert required_history_bars_v3(cfg) == 99 + 365 + 1
        cfg["sma_window"] = 200
        assert required_history_bars_v3(cfg) == 199 + 365 + 1

    def test_requirement_tracks_lookback_deterministically(self):
        cfg = self._v3_defaults()
        cfg["lookback_bars_for_history"] = 200
        assert required_history_bars_v3(cfg) == 149 + 200 + 1
        cfg["lookback_bars_for_history"] = 400
        assert required_history_bars_v3(cfg) == 149 + 400 + 1

    def test_smaller_windows_still_respect_min_history_bars(self):
        cfg = self._v3_defaults()
        cfg["sma_window"] = 20
        cfg["lookback_bars_for_history"] = 50
        # 19 + 50 + 1 = 70 < min_history_bars=200 -> readiness gate wins.
        assert required_history_bars_v3(cfg) == 200

    def test_shared_depth_is_the_maximum_across_both_arms(self):
        v2 = self._v2_defaults()
        v3 = self._v3_defaults()
        assert shared_required_history_bars(v2, v3) == 515
        v3["lookback_bars_for_history"] = 100   # v3 shrinks -> v2 dominates
        assert shared_required_history_bars(v2, v3) == \
            required_history_bars_v2(v2)
        v2["lookback_days_for_history"] = 50    # both small -> max of the two
        assert shared_required_history_bars(v2, v3) == max(
            required_history_bars_v2(v2), required_history_bars_v3(v3)
        )

    def test_shared_depth_hard_capped_at_600(self):
        v2 = self._v2_defaults()
        v3 = self._v3_defaults()
        v3["lookback_bars_for_history"] = 5000   # runaway config
        assert shared_required_history_bars(v2, v3) == FRAME_HARD_CAP_BARS == 600

    def test_desired_depth_is_never_capped(self):
        """The uncapped configured requirement is preserved: a config
        needing 800 bars yields desired=800 while requested is capped
        at 600."""
        from app.workers.shadow.frames import desired_history_bars
        v2 = self._v2_defaults()
        v3 = self._v3_defaults()
        v3["lookback_bars_for_history"] = 650    # 149 + 650 + 1 = 800
        assert desired_history_bars(v2, v3) == 800
        assert shared_required_history_bars(v2, v3) == 600


# --------------------------------------------------------------------------- #
# Runner: shared fetch, identity, decisions
# --------------------------------------------------------------------------- #

class TestRunnerSharedFrame:
    def test_provider_fetched_once_per_symbol_not_per_arm(
        self, store, default_configs
    ):
        provider = FakeProvider(_payloads())
        _run(run_shadow_comparison(
            provider, ["ENTRX", "WTCHX", "JBLX"], now_utc=NOW_UTC
        ))
        assert provider.calls == ["ENTRX", "WTCHX", "JBLX"]  # once each

    def test_market_data_as_of_identical_for_both_arms(
        self, store, default_configs
    ):
        provider = FakeProvider(_payloads())
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        pair_entry = next(iter(store.pairs.values()))
        # ONE pair-level as-of; the frame is stored once per pair, not per arm.
        assert "market_data_as_of" in pair_entry["pair"]
        assert len(pair_entry["evaluations"]) == 2

    def test_fetch_depth_derived_from_configs_plus_margin(
        self, store, default_configs
    ):
        provider = FakeProvider(_payloads())
        summary = _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        # Both default arms need 515 completed bars; the fetch adds the
        # partial-bar margin.
        assert provider.requested_timeseries == [515 + FRAME_FETCH_MARGIN_BARS]
        t = summary["telemetry"]
        assert t["desired_history_bars"] == 515
        assert t["requested_history_bars"] == 515
        assert t["canonical_frame_cap"] == FRAME_HARD_CAP_BARS
        assert t["history_depth_capped"] is False

    def test_provider_shortfall_recorded_honestly(
        self, store, default_configs
    ):
        """430 available bars < 515 requested: both arms still evaluate the
        same available completed frame; depth is recorded, never inflated,
        and the symbol is not rejected for depth alone."""
        provider = FakeProvider(_payloads())
        summary = _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        t = summary["telemetry"]
        assert t["pair_count"] == 1                      # not rejected
        depth = t["completion"]["WTCHX"]
        assert depth["desired_history_bars"] == 515
        assert depth["requested_history_bars"] == 515
        assert depth["available_completed_bars"] == 430
        assert depth["history_depth_capped"] is False
        assert depth["history_depth_complete"] is False
        # Pair persisted with the ACTUAL frame depth for both arms.
        pair_entry = next(iter(store.pairs.values()))
        assert pair_entry["pair"]["frame_bar_count"] == 430
        assert len(pair_entry["evaluations"]) == 2

    def test_full_depth_recorded_when_provider_has_history(
        self, store, default_configs
    ):
        big = build_uptrend_frame(n=530)   # >= 515 completed bars available
        provider = FakeProvider({"DEEP": frame_to_payload(big)})
        summary = _run(run_shadow_comparison(provider, ["DEEP"], now_utc=NOW_UTC))
        depth = summary["telemetry"]["completion"]["DEEP"]
        assert depth["available_completed_bars"] == 515   # capped at requested
        assert depth["history_depth_capped"] is False
        assert depth["history_depth_complete"] is True    # below-cap need met
        pair_entry = next(iter(store.pairs.values()))
        assert pair_entry["pair"]["frame_bar_count"] == 515

    def test_capped_desired_depth_never_reports_complete(self, store, monkeypatch):
        """Configs needing 800 bars: requested is capped at 600, a full
        600-bar frame is honestly INCOMPLETE against the desired 800, and
        the pair is still evaluated identically by both arms."""
        async def big_lookback_resolve(pattern_code, defaults):
            merged = dict(defaults)
            if pattern_code == "sma150_bounce_v3":
                merged["lookback_bars_for_history"] = 650   # 149+650+1 = 800
            return merged
        monkeypatch.setattr(
            shadow_runner, "resolve_pattern_config", big_lookback_resolve
        )
        big = build_uptrend_frame(n=620)   # provider has >= 600 completed bars
        provider = FakeProvider({"CAPX": frame_to_payload(big)})
        summary = _run(run_shadow_comparison(provider, ["CAPX"], now_utc=NOW_UTC))

        t = summary["telemetry"]
        assert t["desired_history_bars"] == 800
        assert t["requested_history_bars"] == 600         # never above the cap
        assert t["history_depth_capped"] is True
        assert t["canonical_frame_cap"] == 600
        # Fetch stays bounded to requested + margin, NOT desired.
        assert provider.requested_timeseries == [600 + FRAME_FETCH_MARGIN_BARS]

        depth = t["completion"]["CAPX"]
        assert depth["desired_history_bars"] == 800
        assert depth["requested_history_bars"] == 600
        assert depth["available_completed_bars"] == 600   # cap filled...
        assert depth["history_depth_capped"] is True
        assert depth["history_depth_complete"] is False   # ...still incomplete

        # Not rejected: both arms evaluated the SAME capped frame.
        assert t["pair_count"] == 1
        pair_entry = next(iter(store.pairs.values()))
        assert pair_entry["pair"]["frame_bar_count"] == 600
        assert len(pair_entry["evaluations"]) == 2
        # Identity versions unchanged by depth metadata.
        assert pair_entry["pair"]["pair_fingerprint_version"] == \
            PAIR_FINGERPRINT_VERSION
        assert pair_entry["pair"]["frame_snapshot_version"] == \
            FRAME_SNAPSHOT_VERSION

    def test_requirement_of_exactly_600_with_600_bars_is_complete(
        self, store, monkeypatch
    ):
        async def exact_resolve(pattern_code, defaults):
            merged = dict(defaults)
            if pattern_code == "sma150_bounce_v3":
                merged["lookback_bars_for_history"] = 450   # 149+450+1 = 600
            return merged
        monkeypatch.setattr(shadow_runner, "resolve_pattern_config", exact_resolve)
        big = build_uptrend_frame(n=620)
        provider = FakeProvider({"EXCT": frame_to_payload(big)})
        summary = _run(run_shadow_comparison(provider, ["EXCT"], now_utc=NOW_UTC))
        t = summary["telemetry"]
        assert t["desired_history_bars"] == 600
        assert t["requested_history_bars"] == 600
        assert t["history_depth_capped"] is False          # not over the cap
        depth = t["completion"]["EXCT"]
        assert depth["available_completed_bars"] == 600
        assert depth["history_depth_complete"] is True     # desired == met

    def test_one_symbol_failure_never_aborts_others(
        self, store, default_configs
    ):
        payloads = _payloads()
        payloads["BOOM"] = RuntimeError("provider exploded")
        provider = FakeProvider(payloads)
        summary = _run(run_shadow_comparison(
            provider, ["BOOM", "WTCHX"], now_utc=NOW_UTC
        ))
        assert summary["status"] == "completed"
        t = summary["telemetry"]
        assert t["pair_count"] == 1
        assert t["rejected_counts"]["fetch_error"] == 1
        assert t["rejected_symbols"]["fetch_error"] == ["BOOM"]

    def test_all_symbols_rejected_still_completes(self, store, default_configs):
        provider = FakeProvider({"EMPTY": {"historical": []}})
        summary = _run(run_shadow_comparison(provider, ["EMPTY"], now_utc=NOW_UTC))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["terminal_reason"] == "no_valid_pairs"
        assert summary["telemetry"]["pair_count"] == 0

    def test_operational_exception_finalizes_run_failed(
        self, store, monkeypatch
    ):
        async def broken_resolve(pattern_code, defaults):
            raise RuntimeError("config backend down")
        monkeypatch.setattr(shadow_runner, "resolve_pattern_config", broken_resolve)
        provider = FakeProvider(_payloads())
        summary = _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        assert summary["status"] == "failed"
        run = next(iter(store.runs.values()))
        assert run["status"] == "failed"
        assert run["error_code"] == "shadow_run_exception"


class TestSymbolValidation:
    def test_empty_list_rejected(self):
        with pytest.raises(ShadowRequestError):
            normalize_shadow_symbols([])

    def test_whitespace_only_rejected(self):
        with pytest.raises(ShadowRequestError):
            normalize_shadow_symbols(["  ", ""])

    def test_non_list_rejected(self):
        with pytest.raises(ShadowRequestError):
            normalize_shadow_symbols("JBL,DHR")

    def test_normalization_dedup_preserves_order(self):
        assert normalize_shadow_symbols([" jbl ", "DHR", "jbl", "dhr", "AAPL"]) == \
            ["JBL", "DHR", "AAPL"]

    def test_hard_cap_25(self):
        ok = [f"S{i}" for i in range(MAX_SHADOW_SYMBOLS)]
        assert len(normalize_shadow_symbols(ok)) == 25
        with pytest.raises(ShadowRequestError):
            normalize_shadow_symbols([f"S{i}" for i in range(26)])


class TestIdentity:
    def test_real_strategy_and_policy_versions_used(self, store, default_configs):
        provider = FakeProvider(_payloads())
        summary = _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        t = summary["telemetry"]
        assert t["control_identity"]["strategy_version"] == "sma150.v2"
        assert t["control_identity"]["decision_policy_version"] == \
            "strategy_decision.v1"
        assert t["candidate_identity"]["strategy_version"] == "sma150.v3"
        assert t["candidate_identity"]["decision_policy_version"] == \
            "sma150_bounce.policy.v1"
        evaluations = next(iter(store.pairs.values()))["evaluations"]
        by_arm = {e["arm_code"]: e for e in evaluations}
        assert by_arm[CONTROL_ARM_CODE]["strategy_version"] == "sma150.v2"
        assert by_arm[CANDIDATE_ARM_CODE]["strategy_version"] == "sma150.v3"

    def test_operator_db_config_resolved_and_frozen(self, store, monkeypatch):
        async def operator_resolve(pattern_code, defaults):
            merged = dict(defaults)
            if pattern_code == "sma150_bounce_v3":
                merged["min_independent_bounces"] = 4   # operator override
            return merged
        monkeypatch.setattr(shadow_runner, "resolve_pattern_config", operator_resolve)
        provider = FakeProvider(_payloads())
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        evaluations = next(iter(store.pairs.values()))["evaluations"]
        by_arm = {e["arm_code"]: e for e in evaluations}
        assert by_arm[CANDIDATE_ARM_CODE]["config_snapshot"][
            "min_independent_bounces"] == 4
        # ...and the hash freezes the operator value, not the default.
        from app.workers.strategies import get_strategy
        default_hash = config_hash(get_strategy("sma150_bounce_v3").default_config())
        assert by_arm[CANDIDATE_ARM_CODE]["config_hash"] != default_hash

    def test_secret_shaped_config_values_removed(self, store, monkeypatch):
        async def leaky_resolve(pattern_code, defaults):
            merged = dict(defaults)
            merged["api_key"] = "sk-super-secret"
            return merged
        monkeypatch.setattr(shadow_runner, "resolve_pattern_config", leaky_resolve)
        provider = FakeProvider(_payloads())
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        for entry in store.pairs.values():
            text = json.dumps(entry["evaluations"], default=str)
            assert "sk-super-secret" not in text
            assert "api_key" not in text

    def test_config_dict_order_does_not_change_hash(self):
        a = {"x": 1, "y": {"b": 2, "a": 3}}
        b = {"y": {"a": 3, "b": 2}, "x": 1}
        assert config_hash(a) == config_hash(b)

    def test_run_id_does_not_affect_pair_fingerprint(
        self, store, default_configs
    ):
        provider = FakeProvider(_payloads())
        s1 = _run(run_shadow_comparison(
            provider, ["WTCHX"], run_id=str(uuid.uuid4()), now_utc=NOW_UTC
        ))
        s2 = _run(run_shadow_comparison(
            provider, ["WTCHX"], run_id=str(uuid.uuid4()), now_utc=NOW_UTC
        ))
        assert len(store.pairs) == 1                       # same immutable pair
        assert s1["telemetry"]["pairs_created"] == 1
        assert s2["telemetry"]["pairs_created"] == 0
        assert s2["telemetry"]["pairs_deduplicated"] == 1

    def test_meaningful_config_change_creates_new_pair(self, store, monkeypatch):
        provider = FakeProvider(_payloads())

        async def defaults_resolve(pattern_code, defaults):
            return dict(defaults)
        monkeypatch.setattr(shadow_runner, "resolve_pattern_config", defaults_resolve)
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))

        async def changed_resolve(pattern_code, defaults):
            merged = dict(defaults)
            if pattern_code == "sma150_bounce_v3":
                merged["min_median_rebound_pct"] = 7.5
            return merged
        monkeypatch.setattr(shadow_runner, "resolve_pattern_config", changed_resolve)
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))

        assert len(store.pairs) == 2   # distinct immutable identities

    def test_frame_change_creates_new_pair(self, store, default_configs):
        payloads = _payloads()
        provider = FakeProvider(payloads)
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        payloads["WTCHX"]["historical"][50]["close"] += 0.05
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        assert len(store.pairs) == 2

    def test_pair_fingerprint_payload_contains_both_arm_identities(self):
        control = {
            "strategy_code": "sma150_bounce", "strategy_version": "sma150.v2",
            "decision_policy_version": "strategy_decision.v1",
            "config_hash": "aaa",
        }
        candidate = {
            "strategy_code": "sma150_bounce_v3", "strategy_version": "sma150.v3",
            "decision_policy_version": "sma150_bounce.policy.v1",
            "config_hash": "bbb",
        }
        kwargs = dict(
            symbol="JBL", timeframe="1d", provider="fake", frame_hash="fh",
            snapshot_date="2026-07-17",
            market_data_as_of=datetime(2026, 7, 17, tzinfo=timezone.utc),
            control_identity=control, candidate_identity=candidate,
        )
        fp1 = compute_pair_fingerprint(**kwargs)
        # Changing EITHER arm's config hash changes the pair identity.
        fp2 = compute_pair_fingerprint(**{
            **kwargs, "control_identity": {**control, "config_hash": "zzz"}
        })
        fp3 = compute_pair_fingerprint(**{
            **kwargs, "candidate_identity": {**candidate, "config_hash": "zzz"}
        })
        assert fp1 != fp2 and fp1 != fp3 and fp2 != fp3

    def test_evaluation_fingerprint_covers_verdict_and_details(self):
        base = dict(
            pair_fingerprint="pf", arm_code=CONTROL_ARM_CODE,
            strategy_code="sma150_bounce", strategy_version="sma150.v2",
            decision_policy_version="strategy_decision.v1",
            config_hash_value="ch", verdict="ENTER",
            details_original_sha256="d1",
        )
        f1 = compute_evaluation_fingerprint(**base)
        assert f1 != compute_evaluation_fingerprint(**{**base, "verdict": "AVOID"})
        assert f1 != compute_evaluation_fingerprint(
            **{**base, "details_original_sha256": "d2"}
        )


class TestDecisions:
    def test_jbl_like_fixture_v2_enter_v3_avoid(self, store, default_configs):
        """The live-style JBL disagreement: clustered touches count as 3
        bounces for v2 (ENTER) but fail v3's independent-event setup rules
        (AVOID). Both decisions persist verbatim."""
        provider = FakeProvider(_payloads())
        summary = _run(run_shadow_comparison(provider, ["JBLX"], now_utc=NOW_UTC))
        pair = summary["pairs"][0]
        assert pair["control_verdict"] == "ENTER"
        assert pair["candidate_verdict"] == "AVOID"
        assert pair["agreement"] is False
        assert pair["disagreement_category"] == "v2_enter_v3_avoid"
        evaluations = next(iter(store.pairs.values()))["evaluations"]
        by_arm = {e["arm_code"]: e for e in evaluations}
        assert by_arm[CONTROL_ARM_CODE]["verdict"] == "ENTER"
        assert by_arm[CANDIDATE_ARM_CODE]["verdict"] == "AVOID"
        assert by_arm[CANDIDATE_ARM_CODE]["details_snapshot"][
            "setup_state"] == "invalid"

    def test_valid_unconfirmed_setup_produces_v3_watch(
        self, store, default_configs
    ):
        provider = FakeProvider(_payloads())
        summary = _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        assert summary["pairs"][0]["candidate_verdict"] == "WATCH"

    def test_enter_watch_avoid_all_persist(self, store, default_configs):
        provider = FakeProvider(_payloads())
        summary = _run(run_shadow_comparison(
            provider, ["ENTRX", "WTCHX", "JBLX"], now_utc=NOW_UTC
        ))
        candidate_verdicts = {
            p["symbol"]: p["candidate_verdict"] for p in summary["pairs"]
        }
        assert candidate_verdicts == {
            "ENTRX": "ENTER", "WTCHX": "WATCH", "JBLX": "AVOID",
        }
        t = summary["telemetry"]
        assert t["candidate_enter_count"] == 1
        assert t["candidate_watch_count"] == 1
        assert t["candidate_avoid_count"] == 1
        assert t["control_enter_count"] == 3
        # AVOID persisted as a shadow evaluation, never dropped.
        persisted_verdicts = {
            e["verdict"]
            for entry in store.pairs.values()
            for e in entry["evaluations"]
        }
        assert {"ENTER", "WATCH", "AVOID"} <= persisted_verdicts

    def test_missing_v2_evidence_v1_stays_explicitly_absent(
        self, store, default_configs
    ):
        provider = FakeProvider(_payloads())
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        evaluations = next(iter(store.pairs.values()))["evaluations"]
        by_arm = {e["arm_code"]: e for e in evaluations}
        # v2 never emitted evidence.v1 -> the snapshot must not invent it.
        assert "evidence" not in by_arm[CONTROL_ARM_CODE]["details_snapshot"]
        # v3's evidence.v1 bundle IS preserved.
        candidate_details = by_arm[CANDIDATE_ARM_CODE]["details_snapshot"]
        assert candidate_details["evidence"]["evidence_version"] == "evidence.v1"

    def test_ranking_score_never_changes_verdict(self, store, default_configs):
        provider = FakeProvider(_payloads())
        _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        evaluations = next(iter(store.pairs.values()))["evaluations"]
        by_arm = {e["arm_code"]: e for e in evaluations}
        candidate = by_arm[CANDIDATE_ARM_CODE]
        assert candidate["score"] is not None       # a score exists...
        assert candidate["verdict"] == "WATCH"      # ...but did not authorize ENTER

    def test_verdicts_never_normalized(self, store, default_configs):
        """AVOID is never converted to WATCH, WATCH never to ENTER, missing
        scores never zeroed."""
        provider = FakeProvider(_payloads())
        _run(run_shadow_comparison(
            provider, ["ENTRX", "WTCHX", "JBLX"], now_utc=NOW_UTC
        ))
        for entry in store.pairs.values():
            for ev in entry["evaluations"]:
                assert ev["verdict"] in ("ENTER", "WATCH", "AVOID")
                # Scores are the strategy's own output; None stays None.
                if ev["score"] is not None:
                    assert isinstance(ev["score"], float)

    def test_disagreement_categories_deterministic(self):
        assert disagreement_category("ENTER", "ENTER") == "same_enter"
        assert disagreement_category("WATCH", "WATCH") == "same_watch"
        assert disagreement_category("AVOID", "AVOID") == "same_avoid"
        assert disagreement_category("ENTER", "WATCH") == "v2_enter_v3_watch"
        assert disagreement_category("ENTER", "AVOID") == "v2_enter_v3_avoid"
        assert disagreement_category("WATCH", "ENTER") == "v2_watch_v3_enter"
        assert disagreement_category("WATCH", "AVOID") == "v2_watch_v3_avoid"
        assert disagreement_category("AVOID", "ENTER") == "v2_avoid_v3_enter"
        assert disagreement_category("AVOID", "WATCH") == "v2_avoid_v3_watch"


# --------------------------------------------------------------------------- #
# Immutability at the persistence layer (real persist_shadow_pair SQL flow)
# --------------------------------------------------------------------------- #

class _FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class _FakePairConn:
    """Emulates the strategy_shadow_* tables for persist_shadow_pair."""

    def __init__(self):
        self.pairs = {}      # fingerprint -> row dict
        self.evaluations = []
        self.links = {}      # (run_id, pair_id) -> row
        self.updates = 0

    def transaction(self):
        return _FakeTx()

    async def fetchrow(self, query, *args):
        assert "SELECT" in query
        fp, fpv = args
        row = self.pairs.get((fp, fpv))
        return row

    async def execute(self, query, *args):
        if query.strip().startswith("UPDATE"):
            self.updates += 1
            raise AssertionError("immutable shadow rows must never be UPDATEd")
        if "INSERT INTO strategy_shadow_pairs" in query:
            self.pairs[(args[15], args[16])] = {
                "id": args[0], "symbol": args[4], "frame_hash": args[10],
                "origin_run_id": args[1],
            }
        elif "INSERT INTO strategy_shadow_evaluations" in query:
            self.evaluations.append({"id": args[0], "pair_id": args[1],
                                     "arm_code": args[2], "verdict": args[8]})
        elif "INSERT INTO strategy_shadow_run_pairs" in query:
            key = (args[0], args[1])
            if key not in self.links:   # ON CONFLICT DO NOTHING
                self.links[key] = {"created_new_pair": args[2]}


def _pair_record(symbol="JBL", frame_hash="fh-1", fingerprint="fp-1"):
    return {
        "experiment_code": EXPERIMENT_CODE,
        "experiment_version": EXPERIMENT_VERSION,
        "symbol": symbol,
        "timeframe": "1d",
        "provider": "fake",
        "snapshot_date": datetime(2026, 7, 17, tzinfo=timezone.utc).date(),
        "market_data_as_of": datetime(2026, 7, 17, tzinfo=timezone.utc),
        "frame_snapshot_version": FRAME_SNAPSHOT_VERSION,
        "frame_hash": frame_hash,
        "frame_bar_count": 3,
        "frame_first_date": "2026-07-15",
        "frame_last_date": "2026-07-17",
        "frame_snapshot": [{"date": "2026-07-17", "open": 1, "high": 2,
                            "low": 0.5, "close": 1.5, "volume": 10}],
        "pair_fingerprint": fingerprint,
        "pair_fingerprint_version": PAIR_FINGERPRINT_VERSION,
    }


def _evaluation_records():
    out = []
    for arm, verdict in ((CONTROL_ARM_CODE, "ENTER"), (CANDIDATE_ARM_CODE, "AVOID")):
        out.append({
            "arm_code": arm,
            "strategy_code": "sma150_bounce" if arm == CONTROL_ARM_CODE
            else "sma150_bounce_v3",
            "strategy_version": "sma150.v2" if arm == CONTROL_ARM_CODE
            else "sma150.v3",
            "decision_policy_version": "strategy_decision.v1"
            if arm == CONTROL_ARM_CODE else "sma150_bounce.policy.v1",
            "config_hash": "ch",
            "config_snapshot": {"sma_window": 150},
            "verdict": verdict,
            "score": 0.5,
            "reason": "r",
            "rejection_reason": None,
            "details_snapshot": {"snapshot_date": "2026-07-17"},
            "evidence_original_sha256": "sha",
            "evaluation_fingerprint": f"ef-{arm}",
            "evaluation_fingerprint_version": "shadow_evaluation_fingerprint.v1",
        })
    return out


class TestImmutability:
    def _patch_conn(self, monkeypatch, conn):
        async def get_conn():
            return conn
        async def release(_conn):
            return None
        monkeypatch.setattr(shadow_persistence, "get_db_connection", get_conn)
        monkeypatch.setattr(shadow_persistence, "release_db_connection", release)

    def test_first_comparison_creates_pair_and_evaluations(self, monkeypatch):
        conn = _FakePairConn()
        self._patch_conn(monkeypatch, conn)
        result = _run(persist_shadow_pair(
            run_id=str(uuid.uuid4()), pair=_pair_record(),
            evaluations=_evaluation_records(),
        ))
        assert result["created_new_pair"] is True
        assert len(conn.pairs) == 1
        assert len(conn.evaluations) == 2
        assert len(conn.links) == 1

    def test_repeated_exact_comparison_reuses_and_links(self, monkeypatch):
        conn = _FakePairConn()
        self._patch_conn(monkeypatch, conn)
        run_a, run_b = str(uuid.uuid4()), str(uuid.uuid4())
        first = _run(persist_shadow_pair(
            run_id=run_a, pair=_pair_record(), evaluations=_evaluation_records()
        ))
        second = _run(persist_shadow_pair(
            run_id=run_b, pair=_pair_record(), evaluations=_evaluation_records()
        ))
        assert first["pair_id"] == second["pair_id"]
        assert second["created_new_pair"] is False
        assert len(conn.pairs) == 1                 # pair reused
        assert len(conn.evaluations) == 2           # evaluations reused
        assert len(conn.links) == 2                 # occurrence added
        # The origin run recorded on the pair is UNCHANGED.
        stored_pair = next(iter(conn.pairs.values()))
        assert str(stored_pair["origin_run_id"]) == run_a
        assert conn.updates == 0                    # nothing overwritten

    def test_incompatible_fingerprint_reuse_rejected(self, monkeypatch):
        conn = _FakePairConn()
        self._patch_conn(monkeypatch, conn)
        _run(persist_shadow_pair(
            run_id=str(uuid.uuid4()), pair=_pair_record(),
            evaluations=_evaluation_records(),
        ))
        with pytest.raises(ShadowIntegrityError):
            _run(persist_shadow_pair(
                run_id=str(uuid.uuid4()),
                pair=_pair_record(frame_hash="DIFFERENT"),
                evaluations=_evaluation_records(),
            ))

    def test_persistence_module_has_no_update_path_for_frozen_rows(self):
        source = (SHADOW_PKG / "persistence.py").read_text()
        assert "UPDATE strategy_shadow_pairs" not in source
        assert "UPDATE strategy_shadow_evaluations" not in source
        assert "UPDATE strategy_shadow_run_pairs" not in source


# --------------------------------------------------------------------------- #
# Separation from normal signals and outcomes
# --------------------------------------------------------------------------- #

class TestBoundary:
    def test_shadow_sources_never_touch_signal_tables(self):
        """No import/call of save_signal and no SQL against the signal
        tables anywhere in the shadow package or its router. (Docstrings may
        legitimately STATE the boundary; imports/calls/SQL may not.)"""
        sources = {
            py.name: py.read_text() for py in sorted(SHADOW_PKG.glob("*.py"))
        }
        sources["router"] = (
            Path(__file__).resolve().parents[1] / "app" / "routers" / "shadow.py"
        ).read_text()
        for name, source in sources.items():
            assert "save_signal(" not in source, name
            assert "import save_signal" not in source, name
            assert "INSERT INTO signals" not in source, name
            assert "INSERT INTO signal_provenance" not in source, name
            assert "INSERT INTO scan_run_signals" not in source, name
            assert "INSERT INTO signal_outcomes" not in source, name
            assert "UPDATE signals" not in source, name
            assert "UPDATE signal_outcomes" not in source, name

    def test_runner_completes_even_if_save_signal_would_raise(
        self, store, default_configs, monkeypatch
    ):
        import app.workers.persistence as signal_persistence

        async def forbidden(*args, **kwargs):
            raise AssertionError("shadow runner must never call save_signal")
        monkeypatch.setattr(signal_persistence, "save_signal", forbidden)

        provider = FakeProvider(_payloads())
        summary = _run(run_shadow_comparison(provider, ["WTCHX"], now_utc=NOW_UTC))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["pair_count"] == 1

    def test_v3_stays_disabled_in_migration_008(self):
        sql = (MIGRATIONS_DIR / "008_sma150_v3.sql").read_text()
        assert "false" in sql.lower()          # is_enabled=false seed
        shadow_sql = MIGRATION_010.read_text()
        assert "is_enabled" not in shadow_sql  # 010 never flips enablement

    def test_scheduler_unchanged(self):
        scheduler_source = (
            Path(__file__).resolve().parents[1] / "app" / "workers" / "scheduler.py"
        ).read_text()
        assert "shadow" not in scheduler_source.lower()

    def test_avoid_stays_shadow_only(self, store, default_configs):
        """A shadow AVOID never becomes a signal: only shadow persistence is
        invoked, and it records the AVOID verbatim."""
        provider = FakeProvider(_payloads())
        _run(run_shadow_comparison(provider, ["JBLX"], now_utc=NOW_UTC))
        evaluations = next(iter(store.pairs.values()))["evaluations"]
        assert any(e["verdict"] == "AVOID" for e in evaluations)


# --------------------------------------------------------------------------- #
# APIs
# --------------------------------------------------------------------------- #

@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


class TestAdminCompareApi:
    def _patch(self, monkeypatch, summary=None):
        import app.routers.admin as admin_mod

        recorded = {}

        async def fake_run(provider, symbols, run_id=None, now_utc=None):
            recorded["symbols"] = symbols
            recorded["run_id"] = run_id
            return summary or {
                "run_id": run_id, "status": "completed",
                "telemetry": {"pair_count": len(symbols)}, "pairs": [],
            }

        monkeypatch.setattr(shadow_runner, "run_shadow_comparison", fake_run)
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", lambda: FakeProvider({})
        )
        return recorded

    def test_synchronous_compare_returns_run_id_and_telemetry(
        self, client, monkeypatch
    ):
        recorded = self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/sma150/compare",
            json={"symbols": ["jbl", "DHR"], "run_in_background": False},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "completed"
        assert body["run_id"] == recorded["run_id"]
        assert recorded["symbols"] == ["JBL", "DHR"]

    def test_empty_symbols_rejected_422(self, client, monkeypatch):
        self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/sma150/compare", json={"symbols": []}
        )
        assert resp.status_code == 422

    def test_non_list_symbols_rejected_422(self, client, monkeypatch):
        self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/sma150/compare", json={"symbols": "JBL"}
        )
        assert resp.status_code == 422

    def test_26_symbols_rejected_422(self, client, monkeypatch):
        self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/sma150/compare",
            json={"symbols": [f"S{i}" for i in range(26)]},
        )
        assert resp.status_code == 422

    def test_background_mode_returns_run_id_without_resume_claim(
        self, client, monkeypatch
    ):
        self._patch(monkeypatch)
        resp = client.post(
            "/api/admin/shadow/sma150/compare",
            json={"symbols": ["JBL"], "run_in_background": True},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "run_id" in body
        assert "resum" not in json.dumps(body).lower()


class TestReadApis:
    def test_run_endpoint_returns_run(self, client, monkeypatch):
        run = {"run_id": "r", "status": "completed", "telemetry": {}}

        async def fake_fetch(run_id):
            return run
        monkeypatch.setattr(shadow_router_mod, "fetch_shadow_run", fake_fetch)
        rid = str(uuid.uuid4())
        assert client.get(f"/api/shadow/runs/{rid}").json()["status"] == "completed"

    def test_run_endpoint_404s(self, client, monkeypatch):
        async def fake_fetch(run_id):
            return None
        monkeypatch.setattr(shadow_router_mod, "fetch_shadow_run", fake_fetch)
        assert client.get(f"/api/shadow/runs/{uuid.uuid4()}").status_code == 404
        # Malformed id fails safely (no stacktrace, no 500).
        assert client.get("/api/shadow/runs/not-a-uuid").status_code == 404

    def test_pair_filters_forwarded_and_composed(self, client, monkeypatch):
        captured = {}

        async def fake_pairs(**kwargs):
            captured.update(kwargs)
            return []
        monkeypatch.setattr(shadow_router_mod, "fetch_shadow_pairs", fake_pairs)
        rid = str(uuid.uuid4())
        resp = client.get("/api/shadow/pairs", params={
            "run_id": rid,
            "symbol": "JBL",
            "control_verdict": "enter",
            "candidate_verdict": "avoid",
            "agreement": "false",
            "disagreement_category": "v2_enter_v3_avoid",
            "control_strategy_version": "sma150.v2",
            "candidate_strategy_version": "sma150.v3",
            "limit": 10,
        })
        assert resp.status_code == 200
        assert captured["run_id"] == rid
        assert captured["symbol"] == "JBL"
        assert captured["control_verdict"] == "ENTER"
        assert captured["candidate_verdict"] == "AVOID"
        assert captured["agreement"] is False
        assert captured["disagreement_category_filter"] == "v2_enter_v3_avoid"
        assert captured["limit"] == 10

    def test_malformed_filters_fail_safely(self, client, monkeypatch):
        async def fake_pairs(**kwargs):
            return []
        monkeypatch.setattr(shadow_router_mod, "fetch_shadow_pairs", fake_pairs)
        assert client.get(
            "/api/shadow/pairs", params={"control_verdict": "MAYBE"}
        ).status_code == 422
        assert client.get(
            "/api/shadow/pairs", params={"disagreement_category": "v2_up_v3_down"}
        ).status_code == 422
        assert client.get(
            "/api/shadow/pairs", params={"limit": 100000}
        ).status_code == 422

    def test_pair_list_omits_full_snapshots(self):
        """The bounded list summary never carries frame/details snapshots."""

        class Row(dict):
            def __getitem__(self, key):
                return dict.get(self, key)

        row = Row({
            "id": uuid.uuid4(), "origin_run_id": None,
            "experiment_code": EXPERIMENT_CODE,
            "experiment_version": EXPERIMENT_VERSION,
            "symbol": "JBL", "timeframe": "1d", "provider": "fake",
            "snapshot_date": "2026-07-17", "market_data_as_of": None,
            "frame_snapshot_version": FRAME_SNAPSHOT_VERSION,
            "frame_hash": "fh", "frame_bar_count": 400,
            "control_strategy_code": "sma150_bounce",
            "control_strategy_version": "sma150.v2",
            "control_decision_policy_version": "strategy_decision.v1",
            "control_config_hash": "c", "control_verdict": "ENTER",
            "control_score": 0.7, "control_reason": None,
            "control_rejection_reason": None,
            "candidate_strategy_code": "sma150_bounce_v3",
            "candidate_strategy_version": "sma150.v3",
            "candidate_decision_policy_version": "sma150_bounce.policy.v1",
            "candidate_config_hash": "x", "candidate_verdict": "AVOID",
            "candidate_score": None, "candidate_reason": None,
            "candidate_rejection_reason": "insufficient bounces",
            "created_at": None,
        })
        summary = shadow_persistence._pair_summary(row)
        assert "frame_snapshot" not in summary
        assert "details_snapshot" not in json.dumps(summary, default=str)
        assert summary["agreement"] is False
        assert summary["disagreement_category"] == "v2_enter_v3_avoid"
        assert summary["control"]["verdict"] == "ENTER"
        assert summary["candidate"]["verdict"] == "AVOID"

    def test_pair_detail_returns_bounded_frozen_data(self, client, monkeypatch):
        detail = {
            "pair_id": "p", "symbol": "JBL",
            "frame_snapshot": [{"date": "2026-07-17"}],
            "evaluations": {
                CONTROL_ARM_CODE: {"verdict": "ENTER"},
                CANDIDATE_ARM_CODE: {"verdict": "AVOID"},
            },
        }

        async def fake_detail(pair_id):
            return detail
        monkeypatch.setattr(
            shadow_router_mod, "fetch_shadow_pair_detail", fake_detail
        )
        resp = client.get(f"/api/shadow/pairs/{uuid.uuid4()}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["frame_snapshot"] == [{"date": "2026-07-17"}]
        assert body["evaluations"][CONTROL_ARM_CODE]["verdict"] == "ENTER"

    def test_pair_detail_404_on_malformed_or_missing(self, client, monkeypatch):
        async def fake_detail(pair_id):
            return None
        monkeypatch.setattr(
            shadow_router_mod, "fetch_shadow_pair_detail", fake_detail
        )
        assert client.get("/api/shadow/pairs/not-a-uuid").status_code == 404
        assert client.get(f"/api/shadow/pairs/{uuid.uuid4()}").status_code == 404


class TestPairQueryComposition:
    def test_filters_and_compose_in_sql(self, monkeypatch):
        captured = {}

        class _Conn:
            async def fetch(self, query, *params):
                captured["query"] = query
                captured["params"] = params
                return []

        async def get_conn():
            return _Conn()
        async def release(_):
            return None
        monkeypatch.setattr(shadow_persistence, "get_db_connection", get_conn)
        monkeypatch.setattr(shadow_persistence, "release_db_connection", release)

        _run(shadow_persistence.fetch_shadow_pairs(
            symbol="jbl", control_verdict="ENTER", candidate_verdict="AVOID",
            agreement=False, control_strategy_version="sma150.v2", limit=7,
        ))
        q = captured["query"]
        assert q.count(" AND ") >= 4
        assert "p.symbol = $1" in q
        assert "c.verdict = $2" in q
        assert "x.verdict = $3" in q
        assert "(c.verdict <> x.verdict)" in q
        assert "c.strategy_version = $4" in q
        assert captured["params"] == ("JBL", "ENTER", "AVOID", "sma150.v2", 7)


# --------------------------------------------------------------------------- #
# Version constants stay frozen
# --------------------------------------------------------------------------- #

class TestVersionConstants:
    def test_experiment_identity(self):
        assert EXPERIMENT_CODE == "sma150_v2_vs_v3"
        assert EXPERIMENT_VERSION == "sma150_shadow.v1"

    def test_snapshot_and_fingerprint_versions(self):
        from app.workers.shadow.constants import (
            EVALUATION_FINGERPRINT_VERSION,
        )
        assert FRAME_SNAPSHOT_VERSION == "daily_ohlcv_snapshot.v1"
        assert PAIR_FINGERPRINT_VERSION == "shadow_pair_fingerprint.v1"
        assert EVALUATION_FINGERPRINT_VERSION == "shadow_evaluation_fingerprint.v1"

    def test_frame_depth_bounds(self):
        assert FRAME_HARD_CAP_BARS == 600
        assert FRAME_FETCH_MARGIN_BARS >= 1
