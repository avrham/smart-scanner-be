"""Phase 7A — local-only market-data coverage. No live API or DB calls."""

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.workers import coverage as coverage_module
from app.workers import market_store
from main import app


TRADING_DATE = date(2026, 7, 17)


def _bar(symbol, close, volume):
    return {"symbol": symbol, "close": close, "volume": volume}


def _setup_store(monkeypatch, bars, eligible, profiles, counts=None):
    monkeypatch.setattr(settings, "PRESCREEN_MIN_PRICE", 3.0)
    monkeypatch.setattr(settings, "PRESCREEN_MIN_VOLUME", 100_000)
    monkeypatch.setattr(settings, "PRESCREEN_MIN_DOLLAR_VOLUME", 1_000_000)
    monkeypatch.setattr(settings, "MASSIVE_PROFILE_CACHE_DAYS", 7)

    async def fake_counts():
        return counts or {"total": 100, "active": 90, "eligible": 60}

    async def fake_latest():
        return TRADING_DATE

    async def fake_bars(trading_date):
        return bars

    async def fake_eligible():
        return eligible

    async def fake_profiles(symbols):
        return [p for p in profiles if p["symbol"] in symbols]

    monkeypatch.setattr(market_store, "get_ticker_counts", fake_counts)
    monkeypatch.setattr(market_store, "get_latest_daily_bar_date", fake_latest)
    monkeypatch.setattr(market_store, "get_bars_for_date", fake_bars)
    monkeypatch.setattr(market_store, "get_eligible_symbols", fake_eligible)
    monkeypatch.setattr(market_store, "get_ticker_profiles", fake_profiles)


def test_coverage_local_calculations(monkeypatch):
    now = datetime.now(timezone.utc)
    bars = [
        _bar("FRESH", 100.0, 2_000_000),    # fresh profile, cap known
        _bar("STALE", 50.0, 1_000_000),     # stale profile, cap known
        _bar("NEVER", 200.0, 3_000_000),    # never synced, cap unknown
        _bar("NOCAP", 40.0, 900_000),       # synced but missing market cap
        _bar("CHEAP", 1.0, 50_000),         # fails prescreen (price)
        _bar("ALIEN", 500.0, 5_000_000),    # not in eligible universe
    ]
    eligible = {"FRESH", "STALE", "NEVER", "NOCAP", "CHEAP"}
    profiles = [
        {"symbol": "FRESH", "profile_synced_at": now - timedelta(days=1),
         "market_cap": 1e9, "enrichment_status": "enriched"},
        {"symbol": "STALE", "profile_synced_at": now - timedelta(days=30),
         "market_cap": 2e9, "enrichment_status": "enriched"},
        {"symbol": "NEVER", "profile_synced_at": None,
         "market_cap": None, "enrichment_status": None},
        {"symbol": "NOCAP", "profile_synced_at": now - timedelta(days=1),
         "market_cap": None, "enrichment_status": "missing_market_cap"},
    ]
    _setup_store(monkeypatch, bars, eligible, profiles)

    cov = asyncio.run(coverage_module.get_market_data_coverage(TRADING_DATE))

    assert cov["trading_date"] == str(TRADING_DATE)
    assert cov["tickers"] == {"total": 100, "active": 90, "eligible": 60}
    assert cov["daily_bars_for_date"] == 6
    assert cov["prescreen"]["survivors"] == 4
    assert cov["prescreen"]["reject_reasons"]["price_below_min"] == 1
    assert cov["prescreen"]["reject_reasons"]["not_in_universe"] == 1

    assert cov["profiles"]["fresh"] == 2          # FRESH + NOCAP (both synced <7d)
    assert cov["profiles"]["stale"] == 1          # STALE
    assert cov["profiles"]["never_synced"] == 1   # NEVER
    assert cov["profiles"]["missing_market_cap"] == 1
    assert cov["profiles"]["coverage_pct"] == 50.0

    assert cov["liquidity_ready_count"] == 2      # FRESH + STALE have caps
    assert cov["market_cap_unknown_count"] == 2   # NEVER + NOCAP

    # Deterministic next-enrichment preview: NEVER ($600M) before STALE ($50M).
    assert cov["next_enrichment_symbols"] == ["NEVER", "STALE"]
    assert cov["selection_strategy"] == "dollar_volume_desc"


def test_coverage_defaults_to_latest_stored_date(monkeypatch):
    _setup_store(monkeypatch, [], set(), [])
    cov = asyncio.run(coverage_module.get_market_data_coverage(None))
    assert cov["trading_date"] == str(TRADING_DATE)  # from get_latest_daily_bar_date


def test_coverage_with_no_local_bars_is_honestly_zero(monkeypatch):
    async def no_latest():
        return None

    async def fake_counts():
        return {"total": 0, "active": 0, "eligible": 0}

    monkeypatch.setattr(market_store, "get_latest_daily_bar_date", no_latest)
    monkeypatch.setattr(market_store, "get_ticker_counts", fake_counts)

    cov = asyncio.run(coverage_module.get_market_data_coverage(None))
    assert cov["trading_date"] is None
    assert cov["prescreen"]["survivors"] == 0
    assert cov["next_enrichment_symbols"] == []


def test_next_enrichment_symbols_capped_at_25(monkeypatch):
    bars = [_bar(f"S{i:03d}", 10.0 + i, 1_000_000) for i in range(40)]
    eligible = {b["symbol"] for b in bars}
    _setup_store(monkeypatch, bars, eligible, [])  # all never-synced

    cov = asyncio.run(coverage_module.get_market_data_coverage(TRADING_DATE))
    assert cov["prescreen"]["survivors"] == 40
    assert len(cov["next_enrichment_symbols"]) == 25


def test_coverage_endpoint_makes_no_provider_calls(monkeypatch):
    """The coverage endpoint must work even when provider construction would
    fail — proving it never touches a provider."""
    import app.providers as providers_module

    def boom():
        raise AssertionError("coverage must not construct a provider")

    monkeypatch.setattr(providers_module, "get_market_data_provider", boom)
    _setup_store(monkeypatch, [_bar("AAA", 10.0, 1_000_000)], {"AAA"}, [])

    client = TestClient(app)
    resp = client.get("/api/admin/market-data/coverage", params={"trading_date": "2026-07-17"})
    assert resp.status_code == 200
    assert resp.json()["prescreen"]["survivors"] == 1


def test_coverage_endpoint_rejects_bad_date():
    for bad in ("not-a-date", "2026/07/17", "20260717"):
        resp = TestClient(app).get(
            "/api/admin/market-data/coverage", params={"trading_date": bad}
        )
        assert resp.status_code == 400, bad


# --------------------------------------------------------------------------- #
# Provider parameter (validated label only — never a provider client)
# --------------------------------------------------------------------------- #

def test_coverage_returns_explicit_provider(monkeypatch):
    _setup_store(monkeypatch, [], set(), [])
    cov = asyncio.run(coverage_module.get_market_data_coverage(TRADING_DATE, provider="fmp"))
    assert cov["provider"] == "fmp"
    # Normalization: case/whitespace-insensitive exact match.
    cov = asyncio.run(
        coverage_module.get_market_data_coverage(TRADING_DATE, provider="  MASSIVE ")
    )
    assert cov["provider"] == "massive"


def test_coverage_defaults_to_configured_provider(monkeypatch):
    _setup_store(monkeypatch, [], set(), [])
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "fmp")
    cov = asyncio.run(coverage_module.get_market_data_coverage(TRADING_DATE))
    assert cov["provider"] == "fmp"


def test_coverage_rejects_unsupported_provider(monkeypatch):
    _setup_store(monkeypatch, [], set(), [])
    with pytest.raises(coverage_module.UnsupportedProviderError):
        asyncio.run(
            coverage_module.get_market_data_coverage(TRADING_DATE, provider="bloomberg")
        )

    resp = TestClient(app).get(
        "/api/admin/market-data/coverage",
        params={"trading_date": str(TRADING_DATE), "provider": "bloomberg"},
    )
    assert resp.status_code == 400
    assert "Unsupported provider" in resp.json()["detail"]


def test_coverage_provider_selection_never_constructs_a_provider(monkeypatch):
    """Even with an explicit provider param, no provider client is built and
    no network is touched — provider is a validated label only."""
    import app.providers as providers_module
    from app.routers import admin as admin_module

    def boom(*a, **k):
        raise AssertionError("coverage must not construct a provider")

    monkeypatch.setattr(providers_module, "get_market_data_provider", boom)
    monkeypatch.setattr(admin_module, "get_market_data_provider", boom)
    _setup_store(monkeypatch, [_bar("AAA", 10.0, 1_000_000)], {"AAA"}, [])

    resp = TestClient(app).get(
        "/api/admin/market-data/coverage",
        params={"trading_date": "2026-07-17", "provider": "fmp"},
    )
    assert resp.status_code == 200
    assert resp.json()["provider"] == "fmp"
