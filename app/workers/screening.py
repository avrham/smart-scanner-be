"""Pure universe classification + cheap local pre-screening (Massive provider).

No I/O here. These functions decide:
  1. which reference tickers are ELIGIBLE for the universe (classification by
     the provider's type/exchange metadata — never by ticker suffix), and
  2. which symbols pass the cheap LOCAL pre-screen (price/volume/dollar volume
     computed from locally stored daily bars) BEFORE any per-ticker detail
     (market cap) API call is spent, and
  3. whether a cached ticker profile (market cap) needs refreshing.

Dollar volume = close * volume (documented choice: close is always present,
while VWAP is optional in grouped rows; using close keeps the metric defined
for every bar and deterministic).
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


# MIC -> legacy short exchange name used by the existing tickers.exchange
# column and funnel universe query.
MIC_TO_SHORT = {
    "XNAS": "NASDAQ",
    "XNYS": "NYSE",
    "XASE": "AMEX",
}

# Security types excluded by default (ETFs, funds, warrants, rights, preferred
# shares, units...). Uses Massive's `type` field, NOT ticker suffixes.
DEFAULT_ALLOWED_TYPES = {"CS"}


def classify_ticker(
    ticker: Dict[str, Any],
    allowed_exchanges: List[str],
    allowed_types: List[str],
    include_otc: bool = False,
) -> Tuple[bool, Optional[str]]:
    """Classify one /v3/reference/tickers row.

    Returns (eligible, rejection_reason). Reasons:
    'inactive' | 'not_us' | 'not_stocks_market' | 'otc_excluded' |
    'type_not_allowed' | 'exchange_not_allowed'.
    """
    if not ticker.get("active", False):
        return False, "inactive"

    locale = (ticker.get("locale") or "").lower()
    if locale != "us":
        return False, "not_us"

    market = (ticker.get("market") or "").lower()
    if market == "otc" and not include_otc:
        return False, "otc_excluded"
    if market != "stocks" and not (market == "otc" and include_otc):
        return False, "not_stocks_market"

    sec_type = (ticker.get("type") or "").upper()
    if sec_type not in {t.upper() for t in allowed_types}:
        return False, "type_not_allowed"

    exchange_mic = (ticker.get("primary_exchange") or "").upper()
    if exchange_mic not in {e.upper() for e in allowed_exchanges}:
        if not (include_otc and market == "otc"):
            return False, "exchange_not_allowed"

    return True, None


def dollar_volume(bar: Dict[str, Any]) -> float:
    """Local dollar-volume estimate: close * volume (see module docstring)."""
    try:
        return float(bar["close"]) * float(bar["volume"])
    except (KeyError, TypeError, ValueError):
        return 0.0


def prescreen_bar(
    bar: Dict[str, Any],
    min_price: float,
    min_volume: float,
    min_dollar_volume: float,
) -> Tuple[bool, Optional[str]]:
    """Cheap pre-screen for one canonical daily bar.

    Returns (passed, rejection_reason). Runs BEFORE any per-ticker detail call,
    so failing symbols never consume a Massive request.
    """
    try:
        close = float(bar["close"])
        volume = float(bar["volume"])
    except (KeyError, TypeError, ValueError):
        return False, "invalid_bar"

    if close < min_price:
        return False, "price_below_min"
    if volume < min_volume:
        return False, "volume_below_min"
    if dollar_volume(bar) < min_dollar_volume:
        return False, "dollar_volume_below_min"
    return True, None


def prescreen_bars(
    bars: List[Dict[str, Any]],
    eligible_symbols: set,
    min_price: float,
    min_volume: float,
    min_dollar_volume: float,
) -> Tuple[List[str], Dict[str, int]]:
    """Pre-screen a day's canonical bars against the eligible universe.

    Returns (passing symbols, rejection_reason_counts).
    """
    passed: List[str] = []
    reasons: Dict[str, int] = {}

    for bar in bars:
        symbol = bar.get("symbol")
        if symbol not in eligible_symbols:
            reasons["not_in_universe"] = reasons.get("not_in_universe", 0) + 1
            continue
        ok, reason = prescreen_bar(bar, min_price, min_volume, min_dollar_volume)
        if ok:
            passed.append(symbol)
        else:
            reasons[reason] = reasons.get(reason, 0) + 1

    return passed, reasons


# Deterministic enrichment ordering (see prioritize_enrichment).
ENRICHMENT_SELECTION_STRATEGY = "dollar_volume_desc"


def prioritize_enrichment(
    symbols: List[str],
    bars_by_symbol: Dict[str, Dict[str, Any]],
) -> List[str]:
    """Deterministic enrichment priority for stale pre-screen survivors.

    Order: highest dollar volume (close * volume) first, then highest raw
    volume, then symbol ascending as a stable tie-break. Market cap is NEVER
    used — it is the missing field enrichment exists to fetch.
    """
    def key(symbol: str):
        bar = bars_by_symbol.get(symbol) or {}
        try:
            volume = float(bar.get("volume") or 0.0)
        except (TypeError, ValueError):
            volume = 0.0
        return (-dollar_volume(bar), -volume, symbol)

    return sorted(symbols, key=key)


def _as_utc(value: datetime) -> datetime:
    """Normalize to a timezone-aware UTC datetime without mutating the input.

    Aware values are converted to UTC; naive legacy values (e.g. rows written
    with datetime.utcnow() before the timezone fix) are interpreted as UTC.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def needs_profile_refresh(
    profile_synced_at: Optional[datetime],
    now: datetime,
    cache_days: int,
) -> bool:
    """True when the cached ticker profile (market cap) is stale or missing.

    Timezone-safe: profile_synced_at comes from a timestamptz column via
    asyncpg (aware), but legacy values and some callers may be naive. Both
    sides are normalized to aware UTC before comparison; naive values are
    treated as UTC.
    """
    if profile_synced_at is None:
        return True
    return (_as_utc(now) - _as_utc(profile_synced_at)) > timedelta(days=cache_days)


def enrichment_status_for(market_cap: Optional[float]) -> str:
    """Missing market cap is preserved and flagged — NEVER coerced to 0."""
    return "enriched" if market_cap is not None else "missing_market_cap"
