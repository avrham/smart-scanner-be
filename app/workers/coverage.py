"""Market-data coverage observability (Phase 7A).

Answers "how ready is the local dataset for scanning?" using ONLY locally
stored data — this module performs no provider API calls by construction (it
imports the market store and pure screening helpers, never a provider or HTTP
client).
"""

from datetime import date, datetime, timezone
from typing import Any, Dict, Optional

from app.config import settings
from app.workers import market_store
from app.workers.screening import (
    ENRICHMENT_SELECTION_STRATEGY,
    needs_profile_refresh,
    prescreen_bars,
    prioritize_enrichment,
)

# Same bound as enrichment job telemetry.
NEXT_SYMBOLS_CAP = 25

# Providers this deployment knows about. Coverage only VALIDATES the name and
# echoes it — it never constructs a provider client or touches the network.
SUPPORTED_PROVIDERS = {"massive", "fmp"}


class UnsupportedProviderError(ValueError):
    """Provider name is not one of the supported providers."""


def resolve_provider_name(provider: Optional[str]) -> str:
    """Normalize the provider filter; default to the configured provider."""
    name = (provider or settings.MARKET_DATA_PROVIDER or "").strip().lower()
    if name not in SUPPORTED_PROVIDERS:
        raise UnsupportedProviderError(
            f"Unsupported provider '{name}'. Expected one of: "
            f"{', '.join(sorted(SUPPORTED_PROVIDERS))}"
        )
    return name


async def get_market_data_coverage(
    trading_date: Optional[date] = None,
    provider: Optional[str] = None,
) -> Dict[str, Any]:
    """Local-only coverage snapshot for the given (or latest stored) date.

    `provider` is a validated label included in the response (defaults to the
    configured MARKET_DATA_PROVIDER); no provider client is ever constructed.
    """
    provider_name = resolve_provider_name(provider)

    if trading_date is None:
        trading_date = await market_store.get_latest_daily_bar_date()

    counts = await market_store.get_ticker_counts()

    coverage: Dict[str, Any] = {
        "provider": provider_name,
        "trading_date": str(trading_date) if trading_date else None,
        "tickers": counts,
        "daily_bars_for_date": 0,
        "prescreen": {"survivors": 0, "reject_reasons": {}},
        "profiles": {
            "fresh": 0,
            "stale": 0,
            "never_synced": 0,
            "missing_market_cap": 0,
            "coverage_pct": 0.0,
        },
        "liquidity_ready_count": 0,
        "market_cap_unknown_count": 0,
        "next_enrichment_symbols": [],
        "selection_strategy": ENRICHMENT_SELECTION_STRATEGY,
    }
    if trading_date is None:
        return coverage  # no local bars yet — everything is honestly zero

    bars = await market_store.get_bars_for_date(trading_date)
    eligible = await market_store.get_eligible_symbols()
    coverage["daily_bars_for_date"] = len(bars)

    survivors, reject_counts = prescreen_bars(
        bars,
        eligible,
        min_price=settings.PRESCREEN_MIN_PRICE,
        min_volume=settings.PRESCREEN_MIN_VOLUME,
        min_dollar_volume=settings.PRESCREEN_MIN_DOLLAR_VOLUME,
    )
    coverage["prescreen"] = {"survivors": len(survivors), "reject_reasons": reject_counts}

    profiles = {p["symbol"]: p for p in await market_store.get_ticker_profiles(survivors)}
    now = datetime.now(timezone.utc)

    fresh = stale = never_synced = missing_cap = cap_known = 0
    stale_symbols = []
    for symbol in survivors:
        profile = profiles.get(symbol) or {}
        synced_at = profile.get("profile_synced_at")
        if synced_at is None:
            never_synced += 1
            stale_symbols.append(symbol)
        elif needs_profile_refresh(synced_at, now, settings.MASSIVE_PROFILE_CACHE_DAYS):
            stale += 1
            stale_symbols.append(symbol)
        else:
            fresh += 1
        if profile.get("market_cap") is not None:
            cap_known += 1
        elif profile.get("enrichment_status") == "missing_market_cap":
            missing_cap += 1

    coverage["profiles"] = {
        "fresh": fresh,
        "stale": stale,
        "never_synced": never_synced,
        "missing_market_cap": missing_cap,
        "coverage_pct": round(fresh / len(survivors) * 100, 1) if survivors else 0.0,
    }
    coverage["liquidity_ready_count"] = cap_known
    coverage["market_cap_unknown_count"] = len(survivors) - cap_known

    # Same deterministic ordering the enrichment job will use.
    bars_by_symbol = {b["symbol"]: b for b in bars if b.get("symbol")}
    coverage["next_enrichment_symbols"] = prioritize_enrichment(
        stale_symbols, bars_by_symbol
    )[:NEXT_SYMBOLS_CAP]

    return coverage
