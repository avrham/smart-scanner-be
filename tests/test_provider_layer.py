"""Provider factory + MassiveProvider behavior. No real API/DB calls."""

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest

from app.config import settings
from app.providers import ProviderConfigError, get_market_data_provider
from app.providers.fmp_provider import FMPProvider
from app.providers.massive import MassiveProvider
from app.workers import market_store
from app.workers.massive_client import MassiveApiError, MassiveClient
from app.workers.strategies import get_strategy
from app.workers.strategies.base import StrategyContext, StrategyDecision


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #

def test_factory_default_is_massive(monkeypatch):
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "massive")
    monkeypatch.setattr(settings, "MASSIVE_API_KEY", "test-key")
    provider = get_market_data_provider()
    assert isinstance(provider, MassiveProvider)
    assert provider.name == "massive"


def test_factory_massive_without_key_fails_clearly(monkeypatch):
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "massive")
    monkeypatch.setattr(settings, "MASSIVE_API_KEY", "")
    with pytest.raises(ProviderConfigError) as exc:
        get_market_data_provider()
    assert "MASSIVE_API_KEY" in str(exc.value)


def test_factory_fmp_fallback(monkeypatch):
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "fmp")
    provider = get_market_data_provider()
    assert isinstance(provider, FMPProvider)


def test_factory_unknown_provider(monkeypatch):
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "bloomberg")
    with pytest.raises(ProviderConfigError):
        get_market_data_provider()


# --------------------------------------------------------------------------- #
# Massive provider: local-first history
# --------------------------------------------------------------------------- #

def _local_bars(n, end_date):
    bars = []
    for i in range(n):
        d = end_date - timedelta(days=n - 1 - i)
        bars.append(
            {
                "symbol": "AAA",
                "trading_date": d,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 1_000_000.0,
                "vwap": None,
                "transaction_count": None,
            }
        )
    return bars


def _provider_with_raising_aggs():
    client = MassiveClient(api_key="k", requests_per_minute=1_000_000)

    async def no_aggs(*a, **k):
        raise AssertionError("aggs must not be called when local history is fresh")

    client.get_aggs = no_aggs
    return MassiveProvider(client=client)


def test_history_uses_local_bars_when_fresh(monkeypatch):
    today = datetime.now(timezone.utc).date()
    local = _local_bars(400, today - timedelta(days=1))

    async def fake_local(symbol, limit=600):
        return local

    monkeypatch.setattr(market_store, "get_local_daily_bars", fake_local)
    provider = _provider_with_raising_aggs()

    payload = asyncio.run(provider._daily_history_for("AAA", timeseries=350))
    assert payload["symbol"] == "AAA"
    assert len(payload["historical"]) == 350  # no fetch happened


def test_history_incremental_fetch_when_stale(monkeypatch):
    today = datetime.now(timezone.utc).date()
    latest_local = today - timedelta(days=30)
    local = _local_bars(400, latest_local)
    captured = {}

    async def fake_local(symbol, limit=600):
        return local

    async def fake_get_daily_bars(symbol, from_date, to_date):
        captured["from"] = from_date
        captured["to"] = to_date
        return []  # nothing new; provider keeps local data

    async def fake_upsert(bars, source="massive"):
        raise AssertionError("no bars to store when fetch is empty")

    monkeypatch.setattr(market_store, "get_local_daily_bars", fake_local)
    monkeypatch.setattr(market_store, "bulk_upsert_daily_bars", fake_upsert)

    provider = MassiveProvider(client=MassiveClient(api_key="k", requests_per_minute=1_000_000))
    provider.get_daily_bars = fake_get_daily_bars

    payload = asyncio.run(provider._daily_history_for("AAA", timeseries=350))
    # Incremental: fetch starts the day after the newest local bar.
    assert captured["from"] == str(latest_local + timedelta(days=1))
    assert len(payload["historical"]) == 350


def test_batch_history_never_raises_per_symbol(monkeypatch):
    async def boom(symbol, timeseries):
        raise RuntimeError("db down")

    provider = MassiveProvider(client=MassiveClient(api_key="k", requests_per_minute=1_000_000))
    provider._daily_history_for = boom

    out = asyncio.run(provider.batch_historical_data(["AAA"], timeseries=350))
    assert out["AAA"]["historical"] == []


def test_4h_returns_empty_on_provider_error():
    client = MassiveClient(api_key="k", requests_per_minute=1_000_000)

    async def failing_aggs(*a, **k):
        raise MassiveApiError("/v2/aggs", 403, "not on this plan")

    client.get_aggs = failing_aggs
    provider = MassiveProvider(client=client)

    payload = asyncio.run(provider.fetch_historical_4h("AAA"))
    assert payload == {"symbol": "AAA", "historical": []}


# --------------------------------------------------------------------------- #
# History honesty: strategies report insufficiency (Massive Basic ~2y)
# --------------------------------------------------------------------------- #

def test_wyckoff_reports_insufficient_history_explicitly():
    """~500 bars (Massive Basic ceiling) must NOT silently pass wyckoff (540)."""
    import numpy as np
    import pandas as pd

    n = 500
    dates = pd.date_range("2024-07-20", periods=n, freq="B")
    df = pd.DataFrame(
        {
            "date": dates,
            "open": 50.0 + np.arange(n) * 0.1,
            "high": 51.0 + np.arange(n) * 0.1,
            "low": 49.0 + np.arange(n) * 0.1,
            "close": 50.0 + np.arange(n) * 0.1,
            "volume": np.full(n, 1e6),
        }
    )
    ctx = StrategyContext(symbol="AAA", pattern_code="wyckoff_mtf", config=None)
    res = get_strategy("wyckoff_mtf").evaluate(df, ctx)

    assert res.decision == StrategyDecision.REJECT
    assert res.rejection_reason == "insufficient_daily_data"
    # Required vs available reported explicitly — no silent claims.
    assert res.details["daily_bars"] == n
    assert res.details["daily_bars_required"] == 540
