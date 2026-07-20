"""Timezone safety for the profile freshness check. No live API/DB calls.

Regression for the live enrichment failure:
    "can't subtract offset-naive and offset-aware datetimes"
caused by naive datetime.utcnow() in enrich_market_caps vs. aware
profile_synced_at from the Supabase timestamptz column via asyncpg.
"""

import asyncio
from datetime import date, datetime, timedelta, timezone

from app.config import settings
from app.providers.massive import MassiveProvider
from app.workers import market_store
from app.workers.screening import needs_profile_refresh

CACHE_DAYS = 7
IDT = timezone(timedelta(hours=3))  # aware non-UTC zone


# --------------------------------------------------------------------------- #
# needs_profile_refresh: all aware/naive combinations
# --------------------------------------------------------------------------- #

def test_aware_profile_with_aware_utc_now():
    now = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
    fresh = now - timedelta(days=1)
    stale = now - timedelta(days=10)
    assert needs_profile_refresh(fresh, now, CACHE_DAYS) is False
    assert needs_profile_refresh(stale, now, CACHE_DAYS) is True


def test_aware_profile_from_other_timezone_normalizes_to_utc():
    now = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
    # 14:00 IDT == 11:00 UTC — one hour old, fresh.
    fresh_idt = datetime(2026, 7, 20, 14, 0, tzinfo=IDT)
    assert needs_profile_refresh(fresh_idt, now, CACHE_DAYS) is False
    stale_idt = datetime(2026, 7, 10, 14, 0, tzinfo=IDT)
    assert needs_profile_refresh(stale_idt, now, CACHE_DAYS) is True


def test_naive_legacy_profile_with_aware_now_treated_as_utc():
    now = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
    fresh_naive = datetime(2026, 7, 19, 12, 0)   # interpreted as UTC
    stale_naive = datetime(2026, 7, 1, 12, 0)
    assert needs_profile_refresh(fresh_naive, now, CACHE_DAYS) is False
    assert needs_profile_refresh(stale_naive, now, CACHE_DAYS) is True


def test_aware_profile_with_naive_legacy_now():
    naive_now = datetime(2026, 7, 20, 12, 0)     # interpreted as UTC
    fresh = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
    stale = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    assert needs_profile_refresh(fresh, naive_now, CACHE_DAYS) is False
    assert needs_profile_refresh(stale, naive_now, CACHE_DAYS) is True


def test_none_profile_synced_at_always_refreshes():
    now = datetime.now(timezone.utc)
    assert needs_profile_refresh(None, now, CACHE_DAYS) is True


def test_inputs_are_not_mutated():
    profile = datetime(2026, 7, 20, 14, 0, tzinfo=IDT)
    now = datetime(2026, 7, 20, 12, 0)
    needs_profile_refresh(profile, now, CACHE_DAYS)
    assert profile.tzinfo is IDT
    assert now.tzinfo is None


# --------------------------------------------------------------------------- #
# End-to-end regression: enrich_market_caps with aware timestamptz profiles
# --------------------------------------------------------------------------- #

class _CountingClient:
    def __init__(self):
        self.calls = []

    async def get_ticker_details(self, symbol):
        self.calls.append(symbol)
        return {"market_cap": 1_000_000_000.0}


def test_enrichment_survives_aware_timestamptz_profiles(monkeypatch):
    """The original live failure: aware profile_synced_at rows must not crash
    stale-list construction (which runs BEFORE any detail call)."""
    monkeypatch.setattr(settings, "PRESCREEN_MIN_PRICE", 1.0)
    monkeypatch.setattr(settings, "PRESCREEN_MIN_VOLUME", 1.0)
    monkeypatch.setattr(settings, "PRESCREEN_MIN_DOLLAR_VOLUME", 1.0)
    monkeypatch.setattr(settings, "MASSIVE_PROFILE_CACHE_DAYS", 7)

    bars = [
        {"symbol": "FRESH", "close": 100.0, "volume": 1_000_000},
        {"symbol": "STALE", "close": 50.0, "volume": 500_000},
    ]
    now_utc = datetime.now(timezone.utc)
    profiles = [
        # Aware timestamptz values exactly as asyncpg returns them.
        {"symbol": "FRESH", "profile_synced_at": now_utc - timedelta(days=1)},
        {"symbol": "STALE", "profile_synced_at": now_utc - timedelta(days=30)},
    ]

    async def fake_bars(trading_date):
        return bars

    async def fake_eligible():
        return {"FRESH", "STALE"}

    async def fake_profiles(symbols):
        return [p for p in profiles if p["symbol"] in symbols]

    async def fake_update(symbol, market_cap, status):
        pass

    monkeypatch.setattr(market_store, "get_bars_for_date", fake_bars)
    monkeypatch.setattr(market_store, "get_eligible_symbols", fake_eligible)
    monkeypatch.setattr(market_store, "get_ticker_profiles", fake_profiles)
    monkeypatch.setattr(market_store, "update_ticker_profile", fake_update)

    client = _CountingClient()
    provider = MassiveProvider(client=client)

    summary = asyncio.run(provider.enrich_market_caps(date(2026, 7, 17), max_detail_calls=25))

    assert client.calls == ["STALE"]
    assert summary["cached_fresh"] == 1
    assert summary["errors"] == 0
