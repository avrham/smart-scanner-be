"""Phase 8.1B2: service request normalization and provider policy.

Pure/unit tests — no live providers, no DB writes. Orchestration paths that
would touch persistence are exercised with fakes.
"""

import asyncio
import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import pytest

from app.workers.shadow.outcomes.constants import (
    DEFAULT_CALCULATION_LIMIT,
    FORWARD_CALENDAR_CAP_DAYS,
    MAX_CALCULATION_LIMIT,
    REASON_PROVIDER_MISMATCH,
    REASON_PROVIDER_RANGE_UNSUPPORTED,
)
from app.workers.shadow.outcomes.service import (
    ShadowOutcomeRequestError,
    forward_range_for,
    normalize_outcome_request,
    provider_supports_bounded_range,
    run_shadow_outcome_calculation,
)


def _run(coro):
    return asyncio.run(coro)


class TestNormalizeRequest:
    def test_requires_selector_or_pending(self):
        with pytest.raises(ShadowOutcomeRequestError):
            normalize_outcome_request()

    def test_pending_alone_is_valid(self):
        req = normalize_outcome_request(pending=True)
        assert req["pending"] is True
        assert req["limit"] == DEFAULT_CALCULATION_LIMIT

    def test_default_and_hard_limit(self):
        req = normalize_outcome_request(symbols=["DHR"])
        assert req["limit"] == 50
        with pytest.raises(ShadowOutcomeRequestError):
            normalize_outcome_request(symbols=["DHR"], limit=201)
        req2 = normalize_outcome_request(symbols=["DHR"], limit=MAX_CALCULATION_LIMIT)
        assert req2["limit"] == 200

    def test_malformed_pair_uuid_rejected(self):
        with pytest.raises(ShadowOutcomeRequestError):
            normalize_outcome_request(pair_ids=["not-a-uuid"])

    def test_malformed_run_uuid_rejected(self):
        with pytest.raises(ShadowOutcomeRequestError):
            normalize_outcome_request(run_id="bad")

    def test_pair_ids_and_symbols_normalized_and_deduped(self):
        pid = str(uuid.uuid4())
        req = normalize_outcome_request(
            pair_ids=[pid, pid.upper()],
            symbols=["dhr", "DHR", " jbl "],
        )
        assert req["pair_ids"] == [pid]
        assert req["symbols"] == ["DHR", "JBL"]

    def test_selectors_and_compose_fields_preserved(self):
        pid = str(uuid.uuid4())
        rid = str(uuid.uuid4())
        req = normalize_outcome_request(
            pair_ids=[pid], symbols=["DHR"], run_id=rid, pending=True
        )
        assert req["pair_ids"] == [pid]
        assert req["symbols"] == ["DHR"]
        assert req["run_id"] == rid
        assert req["pending"] is True


class TestProviderPolicy:
    def test_massive_supports_bounded_range(self):
        class MassiveLike:
            name = "massive"
            supports_bounded_daily_range = True

        assert provider_supports_bounded_range(MassiveLike()) is True

    def test_fmp_does_not_support_bounded_range(self):
        class FmpLike:
            name = "fmp"
            supports_bounded_daily_range = False

        assert provider_supports_bounded_range(FmpLike()) is False

    def test_missing_capability_defaults_false(self):
        class Unknown:
            name = "unknown"

        assert provider_supports_bounded_range(Unknown()) is False

    def test_forward_range_bounded_to_45_calendar_days(self):
        snap = date(2026, 1, 1)
        today = date(2026, 12, 31)
        start, end = forward_range_for(snap, today=today)
        assert start == snap
        assert end == snap + __import__("datetime").timedelta(
            days=FORWARD_CALENDAR_CAP_DAYS
        )
        assert (end - start).days == 45

    def test_forward_range_clamped_to_today(self):
        snap = date(2026, 7, 1)
        today = date(2026, 7, 10)
        start, end = forward_range_for(snap, today=today)
        assert start == snap and end == today


class _FakeProvider:
    def __init__(self, name: str, supports_range: bool, bars=None):
        self.name = name
        self.supports_bounded_daily_range = supports_range
        self.bars = bars or []
        self.calls: List[tuple] = []

    async def get_daily_bars(self, symbol, from_date, to_date):
        self.calls.append((symbol, from_date, to_date))
        return list(self.bars)


class TestOrchestrationProviderRejection:
    def _patch(self, monkeypatch, pairs):
        import app.workers.shadow.outcomes.service as svc
        import app.workers.shadow.outcomes.persistence as pers

        created = {}
        upserts: List[Dict[str, Any]] = []
        finalized = {}

        async def fake_create(run_id, **kwargs):
            created["run_id"] = run_id
            created.update(kwargs)

        async def fake_finalize(run_id, *, status, telemetry=None, **kwargs):
            finalized["run_id"] = run_id
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

        monkeypatch.setattr(pers, "create_outcome_run", fake_create)
        monkeypatch.setattr(pers, "finalize_outcome_run", fake_finalize)
        monkeypatch.setattr(pers, "select_pairs_for_outcomes", fake_select)
        monkeypatch.setattr(pers, "upsert_pair_outcome", fake_upsert)
        monkeypatch.setattr(svc, "create_outcome_run", fake_create)
        monkeypatch.setattr(svc, "finalize_outcome_run", fake_finalize)
        monkeypatch.setattr(svc, "select_pairs_for_outcomes", fake_select)
        monkeypatch.setattr(svc, "upsert_pair_outcome", fake_upsert)
        monkeypatch.setattr(
            "app.workers.market_store.bulk_upsert_daily_bars", fake_cache
        )
        return created, upserts, finalized

    def _pair(self, provider="massive"):
        return {
            "pair_id": str(uuid.uuid4()),
            "symbol": "DHR",
            "provider": provider,
            "snapshot_date": date(2026, 6, 12),
            "frame_last_date": date(2026, 6, 12),
            "frame_bar_count": 500,
            "frame_last_bar": {
                "date": "2026-06-12",
                "open": 100.0, "high": 100.0, "low": 100.0,
                "close": 100.0, "volume": 1.0,
            },
            "pair_fingerprint": "pf-1",
            "pair_fingerprint_version": "shadow_pair_fingerprint.v1",
        }

    def test_provider_mismatch_rejects_without_fetch(self, monkeypatch):
        pair = self._pair(provider="massive")
        _, upserts, finalized = self._patch(monkeypatch, [pair])
        provider = _FakeProvider("fmp", supports_range=True)
        summary = _run(run_shadow_outcome_calculation(
            provider,
            pair_ids=[pair["pair_id"]],
            now_utc=datetime(2026, 7, 20, tzinfo=timezone.utc),
        ))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["provider_mismatch"] == 1
        assert provider.calls == []
        assert upserts[0]["error_code"] == REASON_PROVIDER_MISMATCH
        assert finalized["status"] == "completed"

    def test_provider_range_unsupported_rejects(self, monkeypatch):
        pair = self._pair(provider="fmp")
        _, upserts, finalized = self._patch(monkeypatch, [pair])
        provider = _FakeProvider("fmp", supports_range=False)
        summary = _run(run_shadow_outcome_calculation(
            provider,
            pair_ids=[pair["pair_id"]],
            now_utc=datetime(2026, 7, 20, tzinfo=timezone.utc),
        ))
        assert summary["telemetry"]["provider_range_unsupported"] == 1
        assert provider.calls == []
        assert upserts[0]["error_code"] == REASON_PROVIDER_RANGE_UNSUPPORTED
        assert finalized["status"] == "completed"

    def test_date_range_retrieval_for_old_pair(self, monkeypatch):
        pair = self._pair(provider="massive")
        pair["snapshot_date"] = date(2025, 1, 2)
        pair["frame_last_date"] = date(2025, 1, 2)
        pair["frame_last_bar"] = {
            "date": "2025-01-02",
            "open": 100.0, "high": 100.0, "low": 100.0,
            "close": 100.0, "volume": 1.0,
        }
        _, upserts, _ = self._patch(monkeypatch, [pair])
        # The bounded range returns the snapshot-date continuity bar (and
        # nothing after it): continuity confirmed, zero forward bars.
        snapshot_bar = {
            "trading_date": "2025-01-02",
            "open": 100.0, "high": 100.0, "low": 100.0,
            "close": 100.0, "volume": 1.0,
        }
        provider = _FakeProvider(
            "massive", supports_range=True, bars=[snapshot_bar]
        )
        now = datetime(2026, 7, 20, tzinfo=timezone.utc)
        _run(run_shadow_outcome_calculation(
            provider, pair_ids=[pair["pair_id"]], now_utc=now,
        ))
        # Old pair still uses bounded date-range, not latest-N.
        assert provider.calls
        symbol, from_d, to_d = provider.calls[0]
        assert symbol == "DHR"
        assert from_d == "2025-01-02"
        assert to_d == "2025-02-16"  # +45 calendar days
        # Outcome persisted (pending — no forward bars).
        assert upserts
        assert upserts[0]["outcome_status"] == "pending_forward_bars"

    def test_one_pair_failure_does_not_abort_run(self, monkeypatch):
        good = self._pair(provider="massive")
        bad = self._pair(provider="other")
        _, upserts, finalized = self._patch(monkeypatch, [bad, good])
        provider = _FakeProvider("massive", supports_range=True, bars=[])
        summary = _run(run_shadow_outcome_calculation(
            provider,
            pair_ids=[bad["pair_id"], good["pair_id"]],
            now_utc=datetime(2026, 7, 20, tzinfo=timezone.utc),
        ))
        assert finalized["status"] == "completed"
        assert summary["telemetry"]["provider_mismatch"] == 1
        assert len(upserts) == 2
