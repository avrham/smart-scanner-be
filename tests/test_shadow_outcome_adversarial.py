"""Phase 8.1B2 adversarial pre-commit audit regressions.

Covers the contracts a hostile market/provider could break:
  1. reference-revision safety (a split must never become a -49% "return");
  2. snapshot-date continuity (no nearest-date substitution);
  3. SQL-boundary write-once atomicity (insert race + column guards);
  4. benchmark JSONB per-benchmark per-horizon maturation;
  5. MFE/MAE monotonic maturation;
  6. pair de-duplication across B1 run occurrence links;
  7. route ordering; 8. bounded fetch caching; 9. hash/revision history;
  10. status transitions.

No providers, no live DB. Deterministic fakes only.
"""

import asyncio
import inspect
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from app.workers.shadow.outcomes.constants import (
    MAX_REVISION_NOTES,
    MAX_REVISION_NOTES_BYTES,
    REASON_REFERENCE_REVISION,
    REASON_SNAPSHOT_BAR_MISSING,
    STATUS_COMPLETE,
    STATUS_ERROR,
    STATUS_PARTIAL,
    STATUS_PENDING,
)
from app.workers.shadow.outcomes.persistence import (
    merge_outcome_for_persistence,
)
from app.workers.shadow.outcomes.service import run_shadow_outcome_calculation


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = date(2026, 6, 12)  # Friday; first forward session Mon 2026-06-15
NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def _run(coro):
    return asyncio.run(coro)


def _provider_bar(d: str, close: float = 100.0):
    return {
        "trading_date": d,
        "open": close, "high": close, "low": close,
        "close": close, "volume": 1_000_000.0,
    }


class _FakeProvider:
    name = "massive"
    supports_bounded_daily_range = True

    def __init__(self, bars_by_symbol: Dict[str, List[Dict[str, Any]]]):
        self.bars_by_symbol = bars_by_symbol
        self.calls: List[Tuple[str, str, str]] = []

    async def get_daily_bars(self, symbol, from_date, to_date):
        self.calls.append((symbol, from_date, to_date))
        return list(self.bars_by_symbol.get(symbol, []))


def _pair(pair_id=None, symbol="DHR", frozen_close=100.0):
    return {
        "pair_id": pair_id or str(uuid.uuid4()),
        "symbol": symbol,
        "provider": "massive",
        "snapshot_date": SNAPSHOT,
        "frame_last_date": SNAPSHOT,
        "frame_bar_count": 500,
        "frame_last_bar": {
            "date": SNAPSHOT.isoformat(),
            "open": frozen_close, "high": frozen_close,
            "low": frozen_close, "close": frozen_close, "volume": 1.0,
        },
        "pair_fingerprint": f"pf-{symbol}",
        "pair_fingerprint_version": "shadow_pair_fingerprint.v1",
    }


def _patch_service(monkeypatch, pairs):
    """Fake persistence + cache around run_shadow_outcome_calculation."""
    import app.workers.shadow.outcomes.service as svc

    upserts: List[Dict[str, Any]] = []
    finalized: Dict[str, Any] = {}

    async def fake_create(run_id, **kwargs):
        return run_id

    async def fake_finalize(run_id, *, status, telemetry=None, **kwargs):
        finalized["status"] = status
        finalized["telemetry"] = telemetry

    async def fake_select(**kwargs):
        return list(pairs)

    async def fake_upsert(record):
        upserts.append(record)
        return {
            "outcome_id": str(uuid.uuid4()),
            "created_new": True,
            "outcome_status": record.get("outcome_status"),
        }

    async def fake_cache(*args, **kwargs):
        return None

    monkeypatch.setattr(svc, "create_outcome_run", fake_create)
    monkeypatch.setattr(svc, "finalize_outcome_run", fake_finalize)
    monkeypatch.setattr(svc, "select_pairs_for_outcomes", fake_select)
    monkeypatch.setattr(svc, "upsert_pair_outcome", fake_upsert)
    monkeypatch.setattr(
        "app.workers.market_store.bulk_upsert_daily_bars", fake_cache
    )
    return upserts, finalized


# --------------------------------------------------------------------------- #
# 1. Reference-revision safety
# --------------------------------------------------------------------------- #

class TestReferenceRevisionSafety:
    def test_split_never_becomes_a_new_horizon(self, monkeypatch):
        """Frozen 100, fetched snapshot close 50 (split), forward close 51:
        the system must NOT persist -49% as a horizon."""
        pair = _pair()
        upserts, finalized = _patch_service(monkeypatch, [pair])
        provider = _FakeProvider({
            "DHR": [
                _provider_bar("2026-06-12", 50.0),   # revised scale
                _provider_bar("2026-06-15", 51.0),   # forward on new scale
            ],
        })
        summary = _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        assert finalized["status"] == "completed"
        assert summary["telemetry"]["reference_revisions"] == 1
        assert len(upserts) == 1
        record = upserts[0]
        assert record["outcome_status"] == STATUS_ERROR
        assert record["error_code"] == REASON_REFERENCE_REVISION
        assert record["reference_revision_detected"] is True
        # No horizon values at all — especially not -49%.
        for field in ("ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d"):
            assert record.get(field) is None
        notes = record["revision_notes"]
        assert notes and notes[0]["reason_code"] == "reference_close_revision"
        assert notes[0]["existing_value"] == 100.0
        assert notes[0]["observed_value"] == 50.0

    def test_within_tolerance_is_not_a_revision(self, monkeypatch):
        pair = _pair()
        upserts, _ = _patch_service(monkeypatch, [pair])
        provider = _FakeProvider({
            "DHR": [
                _provider_bar("2026-06-12", 100.0 + 1e-10),
                _provider_bar("2026-06-15", 102.0),
            ],
        })
        summary = _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        assert summary["telemetry"]["reference_revisions"] == 0
        record = upserts[0]
        assert record["outcome_status"] == STATUS_PARTIAL
        assert record["reference_revision_detected"] is False
        assert record["ret_1d"] == pytest.approx(2.0)

    def test_revision_error_never_erases_frozen_horizons(self):
        """Merge level: a revision-rejection record against a partial row
        keeps the partial status and every frozen horizon."""
        existing = {
            "outcome_id": "x",
            "outcome_fingerprint": "fp",
            "outcome_fingerprint_version": "v",
            "calculation_version": "outcome.v1",
            "outcome_coverage_version": "cov",
            "forward_frame_version": "fwd",
            "reference_price": 100.0,
            "reference_price_role": "paired_decision_observation",
            "forward_provider": "massive",
            "available_forward_bars": 1,
            "forward_bars_hash": "h1",
            "ret_1d": 2.0, "ret_3d": None, "ret_5d": None,
            "ret_10d": None, "ret_20d": None,
            "max_favorable_excursion": 2.0,
            "max_adverse_excursion": -1.0,
            "mfe_mae_bar_count": 1,
            "benchmark_returns": None,
            "revision_notes": [],
            "reference_revision_detected": False,
            "outcome_status": STATUS_PARTIAL,
            "error_code": None, "error_message": None,
        }
        incoming = {
            "pair_id": "p",
            "outcome_fingerprint": "fp",
            "outcome_fingerprint_version": "v",
            "calculation_version": "outcome.v1",
            "outcome_coverage_version": "cov",
            "forward_frame_version": "fwd",
            "reference_price": None,
            "reference_price_role": "paired_decision_observation",
            "forward_provider": "massive",
            "available_forward_bars": 0,
            "outcome_status": STATUS_ERROR,
            "error_code": REASON_REFERENCE_REVISION,
            "error_message": "scale incompatible",
            "reference_revision_detected": True,
            "revision_notes": [{
                "reason_code": "reference_close_revision",
                "existing_value": 100.0,
                "observed_value": 50.0,
            }],
        }
        merged = merge_outcome_for_persistence(existing, incoming)
        assert merged["outcome_status"] == STATUS_PARTIAL   # not erased
        assert merged["ret_1d"] == 2.0                       # frozen
        assert merged["reference_price"] == 100.0            # frozen
        assert merged["reference_revision_detected"] is True
        assert any(
            n["reason_code"] == "reference_close_revision"
            for n in merged["revision_notes"]
        )


# --------------------------------------------------------------------------- #
# 2. Snapshot-date continuity
# --------------------------------------------------------------------------- #

class TestSnapshotBarContinuity:
    def test_missing_snapshot_bar_rejects_deterministically(self, monkeypatch):
        """Suspended/delisted-like: first returned bar is several sessions
        after snapshot_date — no substitution, no forward calculation."""
        pair = _pair()
        upserts, _ = _patch_service(monkeypatch, [pair])
        provider = _FakeProvider({
            "DHR": [
                _provider_bar("2026-06-17", 90.0),
                _provider_bar("2026-06-18", 91.0),
            ],
        })
        summary = _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        assert summary["telemetry"]["snapshot_bar_missing"] == 1
        record = upserts[0]
        assert record["outcome_status"] == STATUS_ERROR
        assert record["error_code"] == REASON_SNAPSHOT_BAR_MISSING
        for field in ("ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d"):
            assert record.get(field) is None

    def test_empty_range_is_snapshot_bar_missing(self, monkeypatch):
        pair = _pair()
        upserts, _ = _patch_service(monkeypatch, [pair])
        provider = _FakeProvider({"DHR": []})
        _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        assert upserts[0]["error_code"] == REASON_SNAPSHOT_BAR_MISSING


# --------------------------------------------------------------------------- #
# 3. Atomic write-once maturation at the SQL boundary
# --------------------------------------------------------------------------- #

class TestSqlBoundaryAtomicity:
    def _upsert_source(self):
        from app.workers.shadow.outcomes import persistence
        return inspect.getsource(persistence.upsert_pair_outcome)

    def test_insert_handles_concurrent_first_insert(self):
        source = self._upsert_source()
        assert "ON CONFLICT (pair_id) DO NOTHING" in source
        assert "RETURNING id" in source

    def test_reference_price_nullable_only_for_error_lifecycle(self):
        """Schema keeps reference_price/forward_provider nullable for honest
        error rows; the migration documents this and merge makes both
        immutable once set (COALESCE at the SQL boundary too)."""
        sql = (ROOT / "app" / "db" / "migrations"
               / "011_shadow_pair_outcomes.sql").read_text()
        assert "reference_price DOUBLE PRECISION," in sql
        assert "NULLABLE by design" in sql
        # An error-lifecycle record carries no reference; merging it over a
        # calculated row can never null the frozen reference.
        first = merge_outcome_for_persistence(None, _lifecycle_record())
        first["outcome_id"] = "row"
        merged = merge_outcome_for_persistence(first, _lifecycle_record(
            outcome_status=STATUS_ERROR, error_code="forward_fetch_error",
            reference_price=None, forward_provider=None,
            available_forward_bars=0,
        ))
        assert merged["reference_price"] == 100.0
        assert merged["forward_provider"] == "massive"

    def test_update_statement_freezes_horizons_in_sql(self):
        source = self._upsert_source()
        for w in (1, 3, 5, 10, 20):
            assert f"ret_{w}d = COALESCE(ret_{w}d," in source

    def test_update_statement_cannot_change_identity_or_reference(self):
        source = self._upsert_source()
        # Locate the UPDATE statement portion.
        update_sql = source.split("UPDATE strategy_shadow_pair_outcomes")[1]
        update_sql = update_sql.split("WHERE pair_id")[0]
        # Identity/version columns are never in the SET list.
        for forbidden in (
            "outcome_fingerprint =",
            "outcome_fingerprint_version =",
            "calculation_version =",
            "outcome_coverage_version =",
            "forward_frame_version =",
            "reference_price_role =",
            "pair_id =",
        ):
            assert forbidden not in update_sql
        # Reference and provider are immutable once set.
        assert "reference_price = COALESCE(reference_price," in update_sql
        assert "forward_provider = COALESCE(forward_provider," in update_sql
        # Bar count is monotonic and status cannot regress from complete.
        assert "GREATEST(available_forward_bars," in update_sql
        assert "WHEN outcome_status = 'complete' THEN 'complete'" in update_sql
        assert "reference_revision_detected OR" in update_sql

    def test_two_competing_maturation_writers(self):
        """Writer A observes 1D; writer B (merging after A) observes a
        DIFFERENT 1D plus 3D: final result retains A's frozen 1D and fills
        3D — neither writer replaces the first frozen value."""
        def calc(ret_1d, ret_3d, bars, hash_):
            return {
                "pair_id": "p",
                "outcome_fingerprint": "fp",
                "outcome_fingerprint_version": "v",
                "calculation_version": "outcome.v1",
                "outcome_coverage_version": "cov",
                "forward_frame_version": "fwd",
                "reference_price": 100.0,
                "reference_price_role": "paired_decision_observation",
                "forward_provider": "massive",
                "available_forward_bars": bars,
                "forward_bars_hash": hash_,
                "ret_1d": ret_1d, "ret_3d": ret_3d, "ret_5d": None,
                "ret_10d": None, "ret_20d": None,
                "max_favorable_excursion": 1.0,
                "max_adverse_excursion": -1.0,
                "mfe_mae_bar_count": bars,
                "benchmark_returns": None,
                "revision_notes": [],
                "reference_revision_detected": False,
                "outcome_status": STATUS_PARTIAL,
                "error_code": None, "error_message": None,
            }

        after_a = merge_outcome_for_persistence(None, calc(1.5, None, 1, "hA"))
        after_a["outcome_id"] = "row"
        final = merge_outcome_for_persistence(
            after_a, calc(9.9, 3.0, 3, "hB")
        )
        assert final["ret_1d"] == 1.5                      # A's value frozen
        assert final["ret_3d"] == 3.0                      # B's maturation kept
        assert final["available_forward_bars"] == 3
        assert any(
            n["reason_code"] == "horizon_value_divergence"
            for n in final["revision_notes"]
        )


# --------------------------------------------------------------------------- #
# 4. Benchmark JSONB per-benchmark per-horizon maturation
# --------------------------------------------------------------------------- #

def _lifecycle_record(**overrides):
    record = {
        "pair_id": "p",
        "outcome_fingerprint": "fp",
        "outcome_fingerprint_version": "v",
        "calculation_version": "outcome.v1",
        "outcome_coverage_version": "cov",
        "forward_frame_version": "fwd",
        "reference_price": 100.0,
        "reference_price_role": "paired_decision_observation",
        "forward_provider": "massive",
        "available_forward_bars": 1,
        "forward_bars_hash": "h1",
        "ret_1d": 1.0, "ret_3d": None, "ret_5d": None,
        "ret_10d": None, "ret_20d": None,
        "max_favorable_excursion": 1.0,
        "max_adverse_excursion": -1.0,
        "mfe_mae_bar_count": 1,
        "benchmark_returns": None,
        "revision_notes": [],
        "reference_revision_detected": False,
        "outcome_status": STATUS_PARTIAL,
        "error_code": None, "error_message": None,
    }
    record.update(overrides)
    return record


class TestBenchmarkJsonbMerging:
    def test_per_benchmark_per_horizon_maturation(self):
        """First calc: SPY 1D exists / SPY 3D NULL / QQQ all NULL. Second:
        SPY 3D + QQQ 1D + a CHANGED SPY 1D. Final: all matured values kept,
        original SPY 1D preserved and the change recorded."""
        first = merge_outcome_for_persistence(None, _lifecycle_record(
            benchmark_returns={
                "SPY": {"1D": 0.5, "3D": None, "5D": None,
                        "10D": None, "20D": None},
                "QQQ": {"1D": None, "3D": None, "5D": None,
                        "10D": None, "20D": None},
            },
        ))
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(first, _lifecycle_record(
            available_forward_bars=3,
            forward_bars_hash="h3",
            mfe_mae_bar_count=3,
            ret_3d=2.0,
            benchmark_returns={
                "SPY": {"1D": 9.9, "3D": 0.8, "5D": None,
                        "10D": None, "20D": None},
                "QQQ": {"1D": 0.2, "3D": None, "5D": None,
                        "10D": None, "20D": None},
            },
        ))
        bench = final["benchmark_returns"]
        assert bench["SPY"]["1D"] == 0.5    # frozen original, not 9.9
        assert bench["SPY"]["3D"] == 0.8    # newly matured
        assert bench["QQQ"]["1D"] == 0.2    # one benchmark matures alone
        assert bench["QQQ"]["3D"] is None
        note = next(
            n for n in final["revision_notes"]
            if n["reason_code"] == "benchmark_value_divergence"
        )
        assert note["benchmark"] == "SPY"
        assert note["existing_value"] == 0.5
        assert note["observed_value"] == 9.9

    def test_missing_incoming_benchmark_does_not_erase(self):
        first = merge_outcome_for_persistence(None, _lifecycle_record(
            benchmark_returns={
                "SPY": {"1D": 0.5, "3D": None, "5D": None,
                        "10D": None, "20D": None},
            },
        ))
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(
            first, _lifecycle_record(benchmark_returns=None)
        )
        assert final["benchmark_returns"]["SPY"]["1D"] == 0.5


# --------------------------------------------------------------------------- #
# 5. MFE/MAE monotonic maturation
# --------------------------------------------------------------------------- #

class TestMfeMaeMonotonicity:
    def test_normal_expansion_1_to_5_bars(self):
        first = merge_outcome_for_persistence(None, _lifecycle_record(
            mfe_mae_bar_count=1,
            max_favorable_excursion=2.0,
            max_adverse_excursion=-1.0,
        ))
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(first, _lifecycle_record(
            available_forward_bars=5,
            forward_bars_hash="h5",
            mfe_mae_bar_count=5,
            max_favorable_excursion=4.0,
            max_adverse_excursion=-3.0,
            ret_5d=1.0, ret_3d=0.5,
        ))
        assert final["max_favorable_excursion"] == 4.0
        assert final["max_adverse_excursion"] == -3.0
        assert final["mfe_mae_bar_count"] == 5

    def test_same_bar_count_with_revised_ohlcv_keeps_stored(self):
        first = merge_outcome_for_persistence(None, _lifecycle_record(
            mfe_mae_bar_count=3, available_forward_bars=3,
            max_favorable_excursion=5.0, max_adverse_excursion=-2.0,
        ))
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(first, _lifecycle_record(
            mfe_mae_bar_count=3, available_forward_bars=3,
            max_favorable_excursion=99.0, max_adverse_excursion=-99.0,
        ))
        assert final["max_favorable_excursion"] == 5.0
        assert final["max_adverse_excursion"] == -2.0
        assert final["mfe_mae_bar_count"] == 3

    def test_larger_count_with_non_monotonic_values_keeps_stored(self):
        """More bars but revised earlier highs/lows produce a SMALLER MFE /
        LESS ADVERSE MAE: the trustworthy stored excursions are retained and
        the violation is recorded."""
        first = merge_outcome_for_persistence(None, _lifecycle_record(
            mfe_mae_bar_count=3, available_forward_bars=3,
            max_favorable_excursion=6.0, max_adverse_excursion=-5.0,
        ))
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(first, _lifecycle_record(
            mfe_mae_bar_count=5, available_forward_bars=5,
            forward_bars_hash="h5",
            max_favorable_excursion=4.0,   # MFE cannot shrink
            max_adverse_excursion=-2.0,    # MAE cannot become less adverse
        ))
        assert final["max_favorable_excursion"] == 6.0
        assert final["max_adverse_excursion"] == -5.0
        assert final["mfe_mae_bar_count"] == 5   # count still advances
        reasons = {n["reason_code"] for n in final["revision_notes"]}
        assert "mfe_monotonicity_violation" in reasons
        assert "mae_monotonicity_violation" in reasons

    def test_bar_count_never_decreases(self):
        first = merge_outcome_for_persistence(None, _lifecycle_record(
            mfe_mae_bar_count=5, available_forward_bars=5,
        ))
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(first, _lifecycle_record(
            mfe_mae_bar_count=2, available_forward_bars=2,
            max_favorable_excursion=50.0,
        ))
        assert final["mfe_mae_bar_count"] == 5
        assert final["max_favorable_excursion"] == 1.0  # stored, not 50


# --------------------------------------------------------------------------- #
# 6. Pair de-duplication across run occurrence links
# --------------------------------------------------------------------------- #

class TestPairDeduplication:
    def test_list_sql_never_joins_run_occurrences(self):
        from app.workers.shadow.outcomes import persistence
        sql = persistence._OUTCOME_LIST_SQL
        assert "JOIN strategy_shadow_run_pairs" not in sql

    def test_run_id_filters_use_non_multiplying_subquery(self):
        from app.workers.shadow.outcomes import persistence
        source = inspect.getsource(persistence)
        # Every run_id filter goes through IN (SELECT ...), never a JOIN.
        assert source.count(
            "p.id IN (SELECT pair_id FROM strategy_shadow_run_pairs"
        ) >= 2
        assert "JOIN strategy_shadow_run_pairs" not in source

    def test_metrics_count_one_pair_once(self):
        """One pair with two B1 run occurrences produces ONE joined row
        (pair_id is UNIQUE on outcomes; run links are never joined), so
        metrics count it once and no aggregate return is duplicated."""
        from app.workers.shadow.outcomes.metrics import (
            aggregate_pair_outcome_metrics,
        )
        row = {
            "pair": {
                "experiment_code": "sma150_v2_vs_v3",
                "experiment_version": "sma150_shadow.v1",
            },
            "control": {
                "strategy_code": "sma150", "strategy_version": "sma150.v2",
                "decision_policy_version": "p1", "config_hash": "c1",
                "verdict": "ENTER",
            },
            "candidate": {
                "strategy_code": "sma150", "strategy_version": "sma150.v3",
                "decision_policy_version": "p2", "config_hash": "c2",
                "verdict": "WATCH",
            },
            "disagreement_category": "v2_enter_v3_watch",
            "outcome": {
                "calculation_version": "outcome.v1",
                "outcome_coverage_version": "shadow_pair_outcomes.v1",
                "forward_frame_version": "shadow_forward_bars.v1",
                "forward_provider": "massive",
                "outcome_status": STATUS_PARTIAL,
                "returns": {"1D": 2.0, "3D": None, "5D": None,
                            "10D": None, "20D": None},
                "max_favorable_excursion": 3.0,
                "max_adverse_excursion": -1.0,
                "benchmark_returns": {},
            },
        }
        groups = aggregate_pair_outcome_metrics([row])
        assert len(groups) == 1
        assert groups[0]["pair_count"] == 1
        w1 = next(w for w in groups[0]["by_window"] if w["window"] == "1D")
        assert w1["sample_count"] == 1
        assert w1["mean_return"] == pytest.approx(2.0)


# --------------------------------------------------------------------------- #
# 7. Route ordering
# --------------------------------------------------------------------------- #

class TestRouteOrdering:
    def test_static_outcome_routes_precede_parameterized(self):
        from main import app
        paths = [r.path for r in app.routes if hasattr(r, "path")]
        outcome_paths = [p for p in paths if "/shadow/outcomes" in p]
        metrics_idx = outcome_paths.index("/api/shadow/outcomes/metrics")
        detail_idx = outcome_paths.index("/api/shadow/outcomes/{pair_id}")
        assert metrics_idx < detail_idx

    def test_metrics_route_is_never_parsed_as_pair_id(self, monkeypatch):
        from fastapi.testclient import TestClient
        from main import app
        import app.routers.shadow as shadow_router_mod

        async def fake_fetch(**kwargs):
            return []

        async def fail_detail(pair_id):  # pragma: no cover - must not run
            raise AssertionError("metrics path routed to pair detail")

        monkeypatch.setattr(
            shadow_router_mod, "fetch_pair_outcomes", fake_fetch
        )
        monkeypatch.setattr(
            shadow_router_mod, "fetch_pair_outcome_detail", fail_detail
        )
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/shadow/outcomes/metrics")
        assert resp.status_code == 200
        assert "groups" in resp.json()


# --------------------------------------------------------------------------- #
# 8. Bounded fetch caching within one run
# --------------------------------------------------------------------------- #

class TestFetchCaching:
    def test_symbol_and_benchmarks_fetched_once_per_range(self, monkeypatch):
        pair_a = _pair(symbol="DHR")
        pair_b = _pair(symbol="DHR")   # same symbol/snapshot/provider
        upserts, _ = _patch_service(monkeypatch, [pair_a, pair_b])
        bars = {
            "DHR": [_provider_bar("2026-06-12", 100.0),
                    _provider_bar("2026-06-15", 102.0)],
            "SPY": [_provider_bar("2026-06-12", 400.0),
                    _provider_bar("2026-06-15", 402.0)],
            "QQQ": [_provider_bar("2026-06-12", 300.0),
                    _provider_bar("2026-06-15", 303.0)],
        }
        provider = _FakeProvider(bars)
        _run(run_shadow_outcome_calculation(
            provider,
            pair_ids=[pair_a["pair_id"], pair_b["pair_id"]],
            now_utc=NOW,
        ))
        symbols_fetched = [c[0] for c in provider.calls]
        assert symbols_fetched.count("DHR") == 1
        assert symbols_fetched.count("SPY") == 1
        assert symbols_fetched.count("QQQ") == 1
        assert len(upserts) == 2   # both pairs still got their own outcome


# --------------------------------------------------------------------------- #
# 9. Forward hash and revision history
# --------------------------------------------------------------------------- #

class TestForwardHashHistory:
    def test_superseded_hash_preserved_before_replacement(self):
        first = merge_outcome_for_persistence(None, _lifecycle_record())
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(first, _lifecycle_record(
            available_forward_bars=3, forward_bars_hash="h3",
            mfe_mae_bar_count=3, ret_3d=2.0,
        ))
        assert final["forward_bars_hash"] == "h3"
        note = next(
            n for n in final["revision_notes"]
            if n["reason_code"] == "forward_bars_hash_superseded"
        )
        assert note["existing_hash"] == "h1"
        assert note["observed_hash"] == "h3"

    def test_same_count_hash_change_is_revision_not_rewrite(self):
        first = merge_outcome_for_persistence(None, _lifecycle_record())
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(first, _lifecycle_record(
            forward_bars_hash="h1-revised", ret_1d=9.9,
        ))
        assert final["forward_bars_hash"] == "h1"     # frozen
        assert final["ret_1d"] == 1.0                 # frozen
        assert any(
            n["reason_code"] == "forward_bars_revision"
            for n in final["revision_notes"]
        )

    def test_repeated_identical_divergence_appends_one_note(self):
        first = merge_outcome_for_persistence(None, _lifecycle_record())
        first["outcome_id"] = "row"
        divergent = _lifecycle_record(ret_1d=9.9)
        once = merge_outcome_for_persistence(
            first, divergent, detected_at="2026-07-01T00:00:00Z"
        )
        once["outcome_id"] = "row"
        twice = merge_outcome_for_persistence(
            once, divergent, detected_at="2026-07-02T00:00:00Z"
        )
        divergence_notes = [
            n for n in twice["revision_notes"]
            if n["reason_code"] == "horizon_value_divergence"
        ]
        assert len(divergence_notes) == 1   # deduped despite new timestamp

    def test_notes_are_bounded(self):
        assert MAX_REVISION_NOTES == 40
        assert MAX_REVISION_NOTES_BYTES == 16 * 1024
        many = [
            {"reason_code": "horizon_value_divergence",
             "horizon": "1D", "existing_value": 1.0,
             "observed_value": float(i)}
            for i in range(200)
        ]
        merged = merge_outcome_for_persistence(
            None, _lifecycle_record(revision_notes=many)
        )
        assert len(merged["revision_notes"]) <= MAX_REVISION_NOTES


# --------------------------------------------------------------------------- #
# 10. Status transitions
# --------------------------------------------------------------------------- #

class TestStatusTransitions:
    def test_error_cannot_erase_partial(self):
        first = merge_outcome_for_persistence(None, _lifecycle_record())
        first["outcome_id"] = "row"
        final = merge_outcome_for_persistence(first, _lifecycle_record(
            outcome_status=STATUS_ERROR,
            error_code="forward_fetch_error",
            ret_1d=None, available_forward_bars=0,
            forward_bars_hash=None, mfe_mae_bar_count=None,
            max_favorable_excursion=None, max_adverse_excursion=None,
        ))
        assert final["outcome_status"] == STATUS_PARTIAL
        assert final["error_code"] is None
        assert final["ret_1d"] == 1.0
        assert any(
            n["reason_code"] == "recalculation_error"
            for n in final["revision_notes"]
        )

    def test_error_cannot_erase_complete(self):
        complete = merge_outcome_for_persistence(None, _lifecycle_record(
            outcome_status=STATUS_COMPLETE, available_forward_bars=20,
            mfe_mae_bar_count=20, ret_3d=1.0, ret_5d=1.0,
            ret_10d=1.0, ret_20d=1.0, forward_bars_hash="h20",
        ))
        complete["outcome_id"] = "row"
        final = merge_outcome_for_persistence(complete, _lifecycle_record(
            outcome_status=STATUS_ERROR, error_code="anything",
            available_forward_bars=0,
        ))
        assert final["outcome_status"] == STATUS_COMPLETE
        assert final["error_code"] is None

    def test_complete_survives_provider_returning_fewer_bars(self):
        complete = merge_outcome_for_persistence(None, _lifecycle_record(
            outcome_status=STATUS_COMPLETE, available_forward_bars=20,
            mfe_mae_bar_count=20, forward_bars_hash="h20",
        ))
        complete["outcome_id"] = "row"
        final = merge_outcome_for_persistence(complete, _lifecycle_record(
            outcome_status=STATUS_PARTIAL, available_forward_bars=5,
            mfe_mae_bar_count=5, forward_bars_hash="h5",
        ))
        assert final["outcome_status"] == STATUS_COMPLETE
        assert final["available_forward_bars"] == 20
        assert final["forward_bars_hash"] == "h20"

    def test_error_row_repaired_to_each_state(self):
        for status, bars in (
            (STATUS_PENDING, 0), (STATUS_PARTIAL, 3), (STATUS_COMPLETE, 20),
        ):
            error_row = merge_outcome_for_persistence(None, _lifecycle_record(
                outcome_status=STATUS_ERROR, error_code="provider_mismatch",
                ret_1d=None, available_forward_bars=0,
                forward_bars_hash=None, mfe_mae_bar_count=None,
                max_favorable_excursion=None, max_adverse_excursion=None,
                reference_price=None,
            ))
            error_row["outcome_id"] = "row"
            repaired = merge_outcome_for_persistence(
                error_row, _lifecycle_record(
                    outcome_status=status,
                    available_forward_bars=bars,
                    mfe_mae_bar_count=bars if bars else None,
                    ret_1d=1.0 if bars else None,
                )
            )
            assert repaired["outcome_status"] == status
            assert repaired["error_code"] is None

    def test_pending_to_partial_to_complete_monotonic(self):
        pending = merge_outcome_for_persistence(None, _lifecycle_record(
            outcome_status=STATUS_PENDING, available_forward_bars=0,
            ret_1d=None, forward_bars_hash=None, mfe_mae_bar_count=None,
            max_favorable_excursion=None, max_adverse_excursion=None,
        ))
        pending["outcome_id"] = "row"
        partial = merge_outcome_for_persistence(
            pending, _lifecycle_record()
        )
        assert partial["outcome_status"] == STATUS_PARTIAL
        partial["outcome_id"] = "row"
        complete = merge_outcome_for_persistence(
            partial, _lifecycle_record(
                outcome_status=STATUS_COMPLETE, available_forward_bars=20,
                mfe_mae_bar_count=20, forward_bars_hash="h20",
            )
        )
        assert complete["outcome_status"] == STATUS_COMPLETE
        # And back-pressure: partial input can no longer regress it.
        complete["outcome_id"] = "row"
        still = merge_outcome_for_persistence(
            complete, _lifecycle_record()
        )
        assert still["outcome_status"] == STATUS_COMPLETE
