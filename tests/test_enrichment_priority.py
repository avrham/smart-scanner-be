"""Deterministic survivor enrichment prioritization. No live API/DB calls.

Covers:
- prioritize_enrichment ordering (dollar volume desc, volume desc, symbol asc)
- enrich_market_caps selecting stale survivors deterministically
- cached-fresh symbols skipped
- max_detail_calls bound enforced
- telemetry: selection_strategy, selected_symbols (capped), remaining_stale_survivors
"""

import asyncio
from datetime import date, datetime, timedelta

from app.config import settings
from app.providers.massive import MassiveProvider
from app.workers import market_store
from app.workers.screening import (
    ENRICHMENT_SELECTION_STRATEGY,
    prioritize_enrichment,
)


def _bar(symbol, close, volume):
    return {
        "symbol": symbol,
        "trading_date": date(2026, 7, 17),
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
        "vwap": None,
        "transaction_count": None,
    }


# --------------------------------------------------------------------------- #
# Pure ordering
# --------------------------------------------------------------------------- #

def test_priority_high_dollar_volume_first():
    bars = {
        "LOW": _bar("LOW", 10.0, 100_000),      # $1M
        "HIGH": _bar("HIGH", 200.0, 5_000_000),  # $1B
        "MID": _bar("MID", 50.0, 1_000_000),     # $50M
    }
    assert prioritize_enrichment(["LOW", "MID", "HIGH"], bars) == ["HIGH", "MID", "LOW"]


def test_priority_is_deterministic_regardless_of_input_order():
    bars = {
        "AAA": _bar("AAA", 10.0, 1_000_000),
        "BBB": _bar("BBB", 20.0, 2_000_000),
        "CCC": _bar("CCC", 5.0, 500_000),
    }
    expected = prioritize_enrichment(["AAA", "BBB", "CCC"], bars)
    for order in (["CCC", "AAA", "BBB"], ["BBB", "CCC", "AAA"], ["AAA", "CCC", "BBB"]):
        assert prioritize_enrichment(order, bars) == expected


def test_priority_tie_breaks_by_volume_then_symbol():
    # Same dollar volume ($100M) but different raw volume: higher volume wins.
    bars = {
        "PRICEY": _bar("PRICEY", 100.0, 1_000_000),
        "CHEAPY": _bar("CHEAPY", 10.0, 10_000_000),
    }
    assert prioritize_enrichment(["PRICEY", "CHEAPY"], bars) == ["CHEAPY", "PRICEY"]

    # Fully identical bars: symbol ascending is the stable tie-break.
    bars2 = {
        "ZZZ": _bar("ZZZ", 10.0, 1_000_000),
        "AAA": _bar("AAA", 10.0, 1_000_000),
        "MMM": _bar("MMM", 10.0, 1_000_000),
    }
    assert prioritize_enrichment(["ZZZ", "MMM", "AAA"], bars2) == ["AAA", "MMM", "ZZZ"]


def test_priority_missing_bar_sorts_last():
    bars = {"KNOWN": _bar("KNOWN", 10.0, 1_000_000)}
    assert prioritize_enrichment(["GHOST", "KNOWN"], bars) == ["KNOWN", "GHOST"]


# --------------------------------------------------------------------------- #
# enrich_market_caps integration (mocked store + client)
# --------------------------------------------------------------------------- #

class _FakeClient:
    def __init__(self):
        self.calls = []

    async def get_ticker_details(self, symbol):
        self.calls.append(symbol)
        return {"market_cap": 1_000_000_000.0}


def _setup(monkeypatch, bars, profiles_by_symbol):
    monkeypatch.setattr(settings, "PRESCREEN_MIN_PRICE", 1.0)
    monkeypatch.setattr(settings, "PRESCREEN_MIN_VOLUME", 1.0)
    monkeypatch.setattr(settings, "PRESCREEN_MIN_DOLLAR_VOLUME", 1.0)
    monkeypatch.setattr(settings, "MASSIVE_PROFILE_CACHE_DAYS", 7)

    async def fake_bars(trading_date):
        return bars

    async def fake_eligible():
        return {b["symbol"] for b in bars}

    async def fake_profiles(symbols):
        return [
            {"symbol": s, "profile_synced_at": ts}
            for s, ts in profiles_by_symbol.items()
            if s in symbols
        ]

    updated = []

    async def fake_update(symbol, market_cap, status):
        updated.append(symbol)

    monkeypatch.setattr(market_store, "get_bars_for_date", fake_bars)
    monkeypatch.setattr(market_store, "get_eligible_symbols", fake_eligible)
    monkeypatch.setattr(market_store, "get_ticker_profiles", fake_profiles)
    monkeypatch.setattr(market_store, "update_ticker_profile", fake_update)

    client = _FakeClient()
    return MassiveProvider(client=client), client, updated


def test_enrichment_selects_highest_dollar_volume_first(monkeypatch):
    bars = [
        _bar("SMALL", 5.0, 200_000),        # $1M
        _bar("META", 700.0, 15_000_000),    # $10.5B
        _bar("MID", 40.0, 2_000_000),       # $80M
    ]
    provider, client, _ = _setup(monkeypatch, bars, {})

    summary = asyncio.run(provider.enrich_market_caps(date(2026, 7, 17), max_detail_calls=2))

    assert client.calls == ["META", "MID"]  # SMALL deferred, not dropped silently
    assert summary["selected_symbols"] == ["META", "MID"]
    assert summary["remaining_stale_survivors"] == 1
    assert summary["selection_strategy"] == ENRICHMENT_SELECTION_STRATEGY


def test_enrichment_order_is_deterministic_across_db_row_order(monkeypatch):
    bars_a = [_bar("AAA", 10.0, 1_000_000), _bar("BBB", 20.0, 2_000_000), _bar("CCC", 5.0, 100_000)]
    bars_b = list(reversed(bars_a))

    provider_a, client_a, _ = _setup(monkeypatch, bars_a, {})
    asyncio.run(provider_a.enrich_market_caps(date(2026, 7, 17), max_detail_calls=3))

    provider_b, client_b, _ = _setup(monkeypatch, bars_b, {})
    asyncio.run(provider_b.enrich_market_caps(date(2026, 7, 17), max_detail_calls=3))

    assert client_a.calls == client_b.calls == ["BBB", "AAA", "CCC"]


def test_enrichment_skips_cached_fresh_symbols(monkeypatch):
    bars = [_bar("FRESH", 500.0, 10_000_000), _bar("STALE", 10.0, 100_000)]
    profiles = {"FRESH": datetime.utcnow() - timedelta(days=1)}  # within cache window
    provider, client, _ = _setup(monkeypatch, bars, profiles)

    summary = asyncio.run(provider.enrich_market_caps(date(2026, 7, 17), max_detail_calls=25))

    assert client.calls == ["STALE"]  # FRESH skipped despite higher dollar volume
    assert summary["cached_fresh"] == 1
    assert summary["remaining_stale_survivors"] == 0


def test_enrichment_respects_max_detail_calls(monkeypatch):
    bars = [_bar(f"SYM{i:02d}", 10.0 + i, 1_000_000 + i) for i in range(40)]
    provider, client, _ = _setup(monkeypatch, bars, {})

    summary = asyncio.run(provider.enrich_market_caps(date(2026, 7, 17), max_detail_calls=25))

    assert len(client.calls) == 25
    assert summary["detail_calls"] == 25
    assert summary["remaining_stale_survivors"] == 15
    assert len(summary["selected_symbols"]) <= 25


def test_selected_symbols_telemetry_is_capped_at_25(monkeypatch):
    bars = [_bar(f"SYM{i:02d}", 10.0 + i, 1_000_000) for i in range(60)]
    provider, client, _ = _setup(monkeypatch, bars, {})

    summary = asyncio.run(provider.enrich_market_caps(date(2026, 7, 17), max_detail_calls=30))

    assert len(client.calls) == 30            # bound applies to API calls
    assert len(summary["selected_symbols"]) == 25  # telemetry list stays capped
    assert summary["remaining_stale_survivors"] == 30
