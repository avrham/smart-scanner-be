"""Provider decoupling + rate limiter tests.

Proves: (1) no production code instantiates FMPClient outside the FMP module
and its provider adapter, (2) outcome calculation and legacy scan paths go
through the provider-neutral interface, (3) the Massive limiter allows the
configured burst within a rolling minute (no sleep-per-request).
"""

import asyncio
from pathlib import Path

import pandas as pd
import pytest

from app.config import settings
from app.providers import ProviderConfigError, get_market_data_provider
from app.workers.massive_client import MassiveClient
from app.workers.outcomes import service as outcomes_service


REPO = Path(__file__).resolve().parents[1]

# The ONLY files allowed to construct an FMPClient.
FMP_ALLOWED = {
    REPO / "app" / "workers" / "fmp_client.py",
    REPO / "app" / "providers" / "fmp_provider.py",
}


def test_no_direct_fmpclient_instantiation_in_production_code():
    offenders = []
    for py in (REPO / "app").rglob("*.py"):
        if py in FMP_ALLOWED:
            continue
        if "FMPClient(" in py.read_text():
            offenders.append(str(py.relative_to(REPO)))
    main_py = REPO / "main.py"
    if "FMPClient(" in main_py.read_text():
        offenders.append("main.py")
    assert offenders == [], f"direct FMPClient usage found in: {offenders}"


# --------------------------------------------------------------------------- #
# Startup independence: each provider works without the other's key
# --------------------------------------------------------------------------- #

def test_massive_selected_without_fmp_key(monkeypatch):
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "massive")
    monkeypatch.setattr(settings, "MASSIVE_API_KEY", "m-key")
    monkeypatch.setattr(settings, "FMP_API_KEY", "")  # missing FMP key is fine
    provider = get_market_data_provider()
    assert provider.name == "massive"


def test_fmp_selected_without_massive_key(monkeypatch):
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "fmp")
    monkeypatch.setattr(settings, "FMP_API_KEY", "f-key")
    monkeypatch.setattr(settings, "MASSIVE_API_KEY", "")  # missing Massive key is fine
    provider = get_market_data_provider()
    assert provider.name == "fmp"


# --------------------------------------------------------------------------- #
# Outcome calculation is provider-neutral
# --------------------------------------------------------------------------- #

class _FakeProvider:
    """Implements only the provider-neutral method the service may use."""

    name = "fake"

    def __init__(self):
        self.requested = []

    async def get_daily_history(self, symbol, timeseries=400):
        self.requested.append(symbol)
        return {
            "symbol": symbol,
            "historical": [
                {"date": "2026-07-01", "open": 10, "high": 11, "low": 9,
                 "close": 10.5, "volume": 1000}
            ],
        }


def test_outcome_service_uses_provider_interface(monkeypatch):
    provider = _FakeProvider()

    async def one_signal(**kwargs):
        return [
            {
                "signal_id": "00000000-0000-0000-0000-000000000001",
                "symbol": "AAA",
                "pattern_code": "sma150_bounce",
                "snapshot_date": "2026-07-01",
                "created_at": "2026-07-01T00:00:00",
                "details": {},
            }
        ]

    saved = []

    async def fake_upsert(record):
        saved.append(record)
        return "id"

    monkeypatch.setattr(outcomes_service, "get_signals_needing_outcomes", one_signal)
    monkeypatch.setattr(outcomes_service, "upsert_signal_outcome", fake_upsert)

    summary = asyncio.run(outcomes_service.calculate_outcomes_for_signals(provider, limit=1))

    # SPY/QQQ benchmarks + the signal's symbol all flowed through the
    # provider-neutral get_daily_history (no FMP-specific method exists here).
    assert set(provider.requested) == {"SPY", "QQQ", "AAA"}
    assert summary["signals_considered"] == 1
    assert len(saved) == 1


def test_admin_and_scheduler_reference_provider_factory():
    """Legacy scan + outcome endpoints and scheduler use the factory, not FMP."""
    admin_src = (REPO / "app" / "routers" / "admin.py").read_text()
    scheduler_src = (REPO / "app" / "workers" / "scheduler.py").read_text()
    assert "get_market_data_provider" in admin_src
    assert "FMPClient(" not in admin_src
    assert "get_market_data_provider" in scheduler_src
    assert "FMPClient(" not in scheduler_src


# --------------------------------------------------------------------------- #
# Rolling-window rate limiter
# --------------------------------------------------------------------------- #

def _limited_client(rpm=5):
    client = MassiveClient(api_key="k", requests_per_minute=rpm)
    clock = {"t": 1000.0}
    sleeps = []

    def fake_clock():
        return clock["t"]

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        clock["t"] += seconds

    client._clock = fake_clock
    client._sleep = fake_sleep
    return client, clock, sleeps


def test_limiter_allows_full_burst_within_minute():
    """5 requests must be schedulable immediately — NOT one per 12s/60s."""
    client, clock, sleeps = _limited_client(rpm=5)

    async def run():
        for _ in range(5):
            await client._throttle()

    asyncio.run(run())
    assert sleeps == []  # whole burst went through with zero waiting
    assert len(client._request_times) == 5


def test_limiter_blocks_sixth_request_until_window_frees():
    client, clock, sleeps = _limited_client(rpm=5)

    async def run():
        for _ in range(5):
            await client._throttle()
        await client._throttle()  # sixth: must wait ~60s for the window

    asyncio.run(run())
    assert sleeps, "sixth request should have waited"
    assert 59.0 <= sum(sleeps) <= 61.0
    # Never exceeded the limit inside any rolling minute.
    assert len(client._request_times) <= 5


def test_limiter_frees_capacity_after_window_passes():
    client, clock, sleeps = _limited_client(rpm=5)

    async def run():
        for _ in range(5):
            await client._throttle()
        clock["t"] += 61.0  # a minute passes with no requests
        await client._throttle()

    asyncio.run(run())
    assert sleeps == []  # capacity was free again, no sleeping
