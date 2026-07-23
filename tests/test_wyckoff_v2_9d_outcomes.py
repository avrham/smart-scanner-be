"""Phase 9D4: Wyckoff v2 shadow pairs through the existing outcomes
architecture (shadow_pair_outcomes.v1 — reused verbatim, never duplicated)."""

from __future__ import annotations

import ast
import asyncio
import pathlib
import subprocess
import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import pytest

from app.workers.shadow.outcomes.constants import (
    REASON_PROVIDER_MISMATCH,
    REASON_SNAPSHOT_BAR_MISSING,
    REFERENCE_PRICE_ROLE,
    STATUS_ERROR,
    STATUS_PENDING,
)
from app.workers.shadow.outcomes.persistence import merge_outcome_for_persistence
from app.workers.shadow.outcomes.service import run_shadow_outcome_calculation


ROOT = pathlib.Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def _run(coro):
    return asyncio.run(coro)


class _FakeProvider:
    def __init__(self, name: str, supports_range: bool, bars=None):
        self.name = name
        self.supports_bounded_daily_range = supports_range
        self.bars = bars or []
        self.calls: List[tuple] = []

    async def get_daily_bars(self, symbol, from_date, to_date):
        self.calls.append((symbol, from_date, to_date))
        return list(self.bars)


def _wyckoff_pair(*, close: float = 50.0) -> Dict[str, Any]:
    """A selected-pair record exactly as select_pairs_for_outcomes shapes it.

    The B2 selection/calculation contract is deliberately arm-agnostic: a
    wyckoff_v2_vs_baseline pair carries the same fields as an sma150 pair.
    """
    return {
        "pair_id": str(uuid.uuid4()),
        "symbol": "LONGX",
        "provider": "massive",
        "snapshot_date": date(2026, 7, 10),
        "frame_last_date": date(2026, 7, 10),
        "frame_bar_count": 560,
        "frame_last_bar": {
            "date": "2026-07-10",
            "open": close, "high": close + 0.5, "low": close - 0.5,
            "close": close, "volume": 900_000.0,
        },
        "pair_fingerprint": "wyckoff-pf-1",
        "pair_fingerprint_version": "shadow_pair_fingerprint.v1",
    }


def _bars(*entries) -> List[Dict[str, Any]]:
    return [
        {
            "trading_date": d,
            "open": px, "high": px + 0.5, "low": px - 0.5,
            "close": px, "volume": 1_000_000.0,
        }
        for d, px in entries
    ]


def _patch(monkeypatch, pairs):
    import app.workers.shadow.outcomes.persistence as pers
    import app.workers.shadow.outcomes.service as svc

    upserts: List[Dict[str, Any]] = []
    finalized: Dict[str, Any] = {}

    async def fake_create(run_id, **kwargs):
        return None

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
            "error_code": record.get("error_code"),
            "available_forward_bars": record.get("available_forward_bars", 0),
            "reference_revision_detected": record.get(
                "reference_revision_detected", False
            ),
        }

    async def fake_cache(*args, **kwargs):
        return None

    for mod in (pers, svc):
        monkeypatch.setattr(mod, "create_outcome_run", fake_create)
        monkeypatch.setattr(mod, "finalize_outcome_run", fake_finalize)
        monkeypatch.setattr(mod, "select_pairs_for_outcomes", fake_select)
        monkeypatch.setattr(mod, "upsert_pair_outcome", fake_upsert)
    monkeypatch.setattr(
        "app.workers.market_store.bulk_upsert_daily_bars", fake_cache
    )
    return upserts, finalized


class TestWyckoffPairOutcomes:
    def test_eligible_wyckoff_pair_receives_real_outcome(self, monkeypatch):
        pair = _wyckoff_pair(close=50.0)
        upserts, finalized = _patch(monkeypatch, [pair])
        provider = _FakeProvider(
            "massive", supports_range=True,
            bars=_bars(
                ("2026-07-10", 50.0),   # snapshot continuity bar
                ("2026-07-13", 51.0),   # +2.0%
                ("2026-07-14", 49.0),   # -2.0%
            ),
        )
        # SPY/QQQ benchmark fetches return the same shape (harmless values).
        summary = _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        assert summary["status"] == "completed"
        assert finalized["status"] == "completed"
        assert len(upserts) == 1
        record = upserts[0]
        # The reference is the FROZEN frame close — never a fresh quote,
        # never a guessed entry, never a configured default.
        assert record["reference_price"] == 50.0
        assert record["reference_price_role"] == REFERENCE_PRICE_ROLE
        assert record["ret_1d"] == pytest.approx(2.0)
        assert record["ret_3d"] is None          # not yet observable
        assert record["outcome_status"] == "partial"
        assert record["available_forward_bars"] == 2

    def test_missing_market_data_stays_explicit(self, monkeypatch):
        pair = _wyckoff_pair()
        upserts, _ = _patch(monkeypatch, [pair])
        provider = _FakeProvider(
            "massive", supports_range=True,
            bars=_bars(("2026-07-10", 50.0)),   # continuity bar only
        )
        _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        record = upserts[0]
        assert record["outcome_status"] == STATUS_PENDING
        # A missing forward horizon is NULL — never converted to zero.
        for field in ("ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d"):
            assert record[field] is None

    def test_snapshot_bar_missing_is_error_not_substitution(self, monkeypatch):
        pair = _wyckoff_pair()
        upserts, _ = _patch(monkeypatch, [pair])
        provider = _FakeProvider(
            "massive", supports_range=True,
            bars=_bars(("2026-07-13", 51.0)),   # no bar ON snapshot_date
        )
        _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        record = upserts[0]
        assert record["outcome_status"] == STATUS_ERROR
        assert record["error_code"] == REASON_SNAPSHOT_BAR_MISSING
        for field in ("ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d"):
            assert record.get(field) is None

    def test_provider_mismatch_never_mixes_providers(self, monkeypatch):
        pair = _wyckoff_pair()
        upserts, _ = _patch(monkeypatch, [pair])
        provider = _FakeProvider("fmp", supports_range=True, bars=[])
        summary = _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        assert summary["telemetry"]["provider_mismatch"] == 1
        assert provider.calls == []
        assert upserts[0]["error_code"] == REASON_PROVIDER_MISMATCH

    def test_forward_fetch_goes_through_provider_abstraction(self, monkeypatch):
        pair = _wyckoff_pair()
        _patch(monkeypatch, [pair])
        provider = _FakeProvider(
            "massive", supports_range=True,
            bars=_bars(("2026-07-10", 50.0), ("2026-07-13", 51.0)),
        )
        _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=NOW,
        ))
        # Symbol + SPY/QQQ benchmarks — all through get_daily_bars.
        symbols = [c[0] for c in provider.calls]
        assert symbols[0] == "LONGX"
        assert set(symbols[1:]) <= {"SPY", "QQQ"}


class TestOutcomeIdempotency:
    def test_frozen_horizon_never_overwritten(self):
        existing = {
            "reference_price": 50.0,
            "ret_1d": 2.0, "ret_3d": None, "ret_5d": None,
            "ret_10d": None, "ret_20d": None,
            "available_forward_bars": 2,
            "max_favorable_excursion": 2.0,
            "max_adverse_excursion": -1.0,
            "mfe_mae_bar_count": 2,
            "benchmark_returns": {"SPY": {"1D": 0.5}},
            "revision_notes": [],
            "outcome_status": "partial",
            "first_calculated_at": "2026-07-14T00:00:00+00:00",
        }
        calculated = {
            "reference_price": 50.0,
            "ret_1d": 6.0,   # divergent recalculation
            "ret_3d": 1.5,   # newly observable horizon
            "ret_5d": None, "ret_10d": None, "ret_20d": None,
            "available_forward_bars": 4,
            "max_favorable_excursion": 6.0,
            "max_adverse_excursion": -1.5,
            "mfe_mae_bar_count": 4,
            "benchmark_returns": {"SPY": {"1D": 0.5, "3D": 0.7}},
            "revision_notes": [],
            "outcome_status": "partial",
        }
        merged = merge_outcome_for_persistence(
            existing, calculated, detected_at="2026-07-20T00:00:00+00:00"
        )
        # Frozen horizon preserved; divergence recorded, never rewritten.
        assert merged["ret_1d"] == 2.0
        assert merged["ret_3d"] == 1.5
        assert any(
            n.get("reason_code") for n in merged["revision_notes"]
        )

    def test_null_horizon_matures_once(self):
        existing = {
            "reference_price": 50.0,
            "ret_1d": None, "ret_3d": None, "ret_5d": None,
            "ret_10d": None, "ret_20d": None,
            "available_forward_bars": 0,
            "max_favorable_excursion": None,
            "max_adverse_excursion": None,
            "mfe_mae_bar_count": None,
            "benchmark_returns": None,
            "revision_notes": [],
            "outcome_status": "pending_forward_bars",
            "first_calculated_at": None,
        }
        calculated = dict(existing)
        calculated.update({
            "ret_1d": 1.25, "available_forward_bars": 1,
            "outcome_status": "partial",
        })
        merged = merge_outcome_for_persistence(existing, calculated)
        assert merged["ret_1d"] == 1.25


class TestOutcomeIsolationBoundaries:
    def test_signal_outcomes_architecture_untouched(self):
        result = subprocess.run(
            ["git", "diff", "--", "app/workers/outcomes",
             "app/routers/outcomes.py"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        assert result.stdout.strip() == ""

    def test_no_provider_specific_imports_in_outcome_modules(self):
        for rel in (
            "app/workers/shadow/outcomes/service.py",
            "app/workers/shadow/outcomes/calculator.py",
            "app/workers/shadow/outcomes/persistence.py",
            "app/workers/shadow/runner.py",
            "app/workers/strategies/dry_run.py",
        ):
            tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names: List[str] = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                for name in names:
                    assert "fmp" not in name.lower(), (rel, name)
                    assert "massive" not in name.lower(), (rel, name)

    def test_wyckoff_outcome_path_needs_no_new_schema(self):
        """The B2 tables represent wyckoff pair outcomes as-is: the outcome
        row references only pair_id and market-path fields, never arm codes
        or strategy identities (those stay joined from B1 rows)."""
        sql = (
            ROOT / "app" / "db" / "migrations" / "011_shadow_pair_outcomes.sql"
        ).read_text(encoding="utf-8")
        assert "arm_code" not in sql
        assert "strategy_code" not in sql
        # 013 touches ONLY the evaluations CHECK constraint.
        sql13 = (
            ROOT / "app" / "db" / "migrations"
            / "013_wyckoff_v2_shadow_arms.sql"
        ).read_text(encoding="utf-8")
        assert "strategy_shadow_pair_outcomes" not in sql13
