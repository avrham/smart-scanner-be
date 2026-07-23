"""Phase 9E1: provider abstraction for canonical bounded intraday history."""

from __future__ import annotations

import ast
import asyncio
import pathlib
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import pytest

from app.providers.base import (
    IntradayHistoryUnsupportedError,
    MarketDataProvider,
)
from app.providers.massive import MassiveProvider
from app.workers.massive_client import MassiveApiError


ROOT = pathlib.Path(__file__).resolve().parents[1]


def _run(coro):
    return asyncio.run(coro)


def _ms(dt: datetime) -> float:
    return dt.timestamp() * 1000.0


class FakeMassiveClient:
    def __init__(self, aggs):
        self.aggs = aggs
        self.calls: List[tuple] = []

    async def get_aggs(self, symbol, multiplier, timespan, from_date, to_date,
                       adjusted=True, limit=50000):
        self.calls.append((symbol, multiplier, timespan, from_date, to_date))
        if isinstance(self.aggs, Exception):
            raise self.aggs
        return list(self.aggs)


def _agg(dt: datetime, o=50.0, h=51.0, l=49.0, c=50.5, v=1000.0):
    return {"t": _ms(dt), "o": o, "h": h, "l": l, "c": c, "v": v}


T0 = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 7, 15, 16, 0, tzinfo=timezone.utc)


class TestMassiveIntradayHistory:
    def _provider(self, aggs):
        return MassiveProvider(client=FakeMassiveClient(aggs))

    def test_normalized_ascending_ordering_and_tz_aware(self):
        provider = self._provider([_agg(T2), _agg(T0), _agg(T1)])
        result = _run(provider.get_intraday_history(
            "LONGX", multiplier=4, timespan="hour",
            start=date(2026, 7, 1), end=date(2026, 7, 16),
        ))
        starts = [b["start_utc"] for b in result["bars"]]
        assert starts == sorted(starts)
        assert starts == [T0, T1, T2]
        for s in starts:
            assert s.tzinfo is not None
            assert s.utcoffset().total_seconds() == 0
        assert result["provider"] == "massive"
        assert result["multiplier"] == 4
        assert result["timespan"] == "hour"
        assert result["requested_start"] == "2026-07-01"
        assert result["requested_end"] == "2026-07-16"

    def test_exact_duplicates_dropped_and_counted(self):
        provider = self._provider([_agg(T0), _agg(T0), _agg(T1)])
        result = _run(provider.get_intraday_history(
            "LONGX", multiplier=4, timespan="hour",
            start="2026-07-01", end="2026-07-16",
        ))
        assert [b["start_utc"] for b in result["bars"]] == [T0, T1]
        assert result["dropped_exact_duplicates"] == 1

    def test_conflicting_same_start_rows_preserved(self):
        # Same start, DIFFERENT values: the frame layer must see and reject.
        provider = self._provider([_agg(T0, c=50.5), _agg(T0, c=99.0)])
        result = _run(provider.get_intraday_history(
            "LONGX", multiplier=4, timespan="hour",
            start="2026-07-01", end="2026-07-16",
        ))
        assert len(result["bars"]) == 2
        assert result["dropped_exact_duplicates"] == 0

    def test_malformed_rows_skipped_and_counted(self):
        provider = self._provider([
            _agg(T0), {"t": "bad"}, {"o": 1.0}, _agg(T1),
        ])
        result = _run(provider.get_intraday_history(
            "LONGX", multiplier=4, timespan="hour",
            start="2026-07-01", end="2026-07-16",
        ))
        assert len(result["bars"]) == 2
        assert result["skipped_malformed"] == 2

    def test_bounds_are_required(self):
        provider = self._provider([_agg(T0)])
        with pytest.raises(ValueError):
            _run(provider.get_intraday_history(
                "LONGX", multiplier=4, timespan="hour", start=None, end=None,
            ))
        with pytest.raises(ValueError):
            _run(provider.get_intraday_history(
                "LONGX", multiplier=4, timespan="hour",
                start="2026-07-01", end=None,
            ))
        assert provider.client.calls == []

    def test_invalid_multiplier_and_timespan_reject(self):
        provider = self._provider([_agg(T0)])
        with pytest.raises(ValueError):
            _run(provider.get_intraday_history(
                "LONGX", multiplier=0, timespan="hour",
                start="2026-07-01", end="2026-07-16",
            ))
        with pytest.raises(ValueError):
            _run(provider.get_intraday_history(
                "LONGX", multiplier=4, timespan="fortnight",
                start="2026-07-01", end="2026-07-16",
            ))

    def test_limit_keeps_most_recent(self):
        provider = self._provider([_agg(T0), _agg(T1), _agg(T2)])
        result = _run(provider.get_intraday_history(
            "LONGX", multiplier=4, timespan="hour",
            start="2026-07-01", end="2026-07-16", limit=2,
        ))
        assert [b["start_utc"] for b in result["bars"]] == [T1, T2]

    def test_provider_error_propagates_explicitly(self):
        provider = self._provider(MassiveApiError("/v2/aggs", 503, "boom"))
        with pytest.raises(MassiveApiError):
            _run(provider.get_intraday_history(
                "LONGX", multiplier=4, timespan="hour",
                start="2026-07-01", end="2026-07-16",
            ))

    def test_partial_current_bar_is_not_excluded_here(self):
        # Completed-bar semantics belong to the canonical frame builder;
        # the provider returns the currently-forming bucket verbatim.
        recent = datetime.now(timezone.utc).replace(minute=0, second=0,
                                                    microsecond=0)
        provider = self._provider([_agg(recent)])
        result = _run(provider.get_intraday_history(
            "LONGX", multiplier=4, timespan="hour",
            start="2026-07-01", end="2099-01-01",
        ))
        assert len(result["bars"]) == 1

    def test_capability_flag(self):
        assert MassiveProvider(client=FakeMassiveClient([])) \
            .supports_intraday_history is True


class TestUnsupportedProviders:
    def test_base_default_is_typed_unsupported(self):
        class Minimal(MarketDataProvider):
            name = "minimal"

            async def sync_universe(self): ...
            async def get_daily_market_summary(self, trading_date): ...
            async def get_daily_bars(self, symbol, from_date, to_date): ...
            async def get_ticker_details(self, symbol): ...
            async def health_check(self): ...
            async def get_daily_history(self, symbol, timeseries=400): ...
            async def batch_historical_data(self, symbols, timeseries=350): ...
            async def fetch_historical_4h(self, symbol, limit=None): ...

        provider = Minimal()
        assert provider.supports_intraday_history is False
        with pytest.raises(IntradayHistoryUnsupportedError):
            _run(provider.get_intraday_history(
                "LONGX", multiplier=4, timespan="hour",
                start="2026-07-01", end="2026-07-16",
            ))

    def test_fmp_provider_stays_honestly_unsupported(self):
        # FMP's 4H endpoint serves a fixed latest-N window; a client-side
        # filter would silently misrepresent an as-of range, so FMP keeps
        # the conservative default (no dishonest shim).
        from app.providers.fmp_provider import FMPProvider

        assert FMPProvider.supports_intraday_history is False
        assert "get_intraday_history" not in FMPProvider.__dict__


class TestNoProviderSpecificImportsOutsideProviders:
    @pytest.mark.parametrize("rel", [
        "app/workers/shadow/frames_4h.py",
        "app/workers/shadow/runner.py",
        "app/workers/shadow/experiments.py",
        "app/workers/shadow/campaigns.py",
        "app/workers/shadow/strategy_metrics.py",
    ])
    def test_module_has_no_provider_specific_imports(self, rel):
        path = ROOT / rel
        if not path.exists():
            pytest.skip(f"{rel} not present")
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            names: List[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            for name in names:
                assert "fmp" not in name.lower(), (rel, name)
                assert "massive" not in name.lower(), (rel, name)
