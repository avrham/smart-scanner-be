"""Deterministic in-memory market-data provider for warmup-execute tests.

Performs NO network access, records every method call, and can be configured to
return ready/insufficient bars, simulate retryable/terminal provider errors, and
return corrected bars. It is injected explicitly (tests monkeypatch
`app.routers.admin._resolve_history_warmup_provider`); it is NEVER selectable via
`MARKET_DATA_PROVIDER` and constructs no real client.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# The real error type so map_provider_error classifies exactly as in production
# (constructing an exception performs no network access).
from app.workers.massive_client import MassiveApiError


def make_ready_daily(symbol: str, *, today: date, days: int = 800,
                     close: float = 1.5) -> List[Dict[str, Any]]:
    """Completed daily sessions from today-`days` through yesterday (fresh)."""
    out = []
    for i in range(days):
        d = today - timedelta(days=days) + timedelta(days=i)
        if d >= today:
            break
        out.append({"symbol": symbol.upper(), "trading_date": d,
                    "open": 1.0, "high": 2.0, "low": 0.5, "close": close,
                    "volume": 100.0, "vwap": 1.4, "transaction_count": 10})
    return out


def make_ready_4h(symbol: str, *, now: datetime, bars: int = 12,
                  close: float = 1.5) -> Dict[str, Any]:
    """`bars` completed 4H buckets whose ends are all <= now (fresh)."""
    now = now.astimezone(timezone.utc)
    # anchor the latest bar_end a few hours before now (safely completed)
    latest_end = now - timedelta(hours=5)
    out = []
    for i in range(bars):
        start = latest_end - timedelta(hours=4 * (bars - i))
        out.append({"start_utc": start, "open": 1.0, "high": 2.0, "low": 0.5,
                    "close": close, "volume": 100.0})
    return {"symbol": symbol.upper(), "provider": "fake", "multiplier": 4,
            "timespan": "hour", "bars": out, "skipped_malformed": 0,
            "dropped_exact_duplicates": 0}


def make_invalid_4h(symbol: str, *, now: datetime) -> Dict[str, Any]:
    """A single structurally invalid bar (high < low) -> persistence rejects it."""
    start = now.astimezone(timezone.utc) - timedelta(hours=9)
    return {"symbol": symbol.upper(), "provider": "fake", "bars": [
        {"start_utc": start, "open": 1.0, "high": 0.4, "low": 0.5,
         "close": 0.45, "volume": 100.0}]}


class FakeProvider:
    name = "fake"
    supports_bounded_daily_range = True
    supports_intraday_history = True

    def __init__(self, *, daily: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                 intraday: Optional[Dict[str, Dict[str, Any]]] = None,
                 daily_error: Optional[BaseException] = None,
                 intraday_error: Optional[BaseException] = None):
        self.daily = daily or {}
        self.intraday = intraday or {}
        self.daily_error = daily_error
        self.intraday_error = intraday_error
        self.calls: List[Dict[str, Any]] = []

    async def get_daily_bars(self, symbol: str, from_date: str, to_date: str
                             ) -> List[Dict[str, Any]]:
        self.calls.append({"method": "get_daily_bars", "symbol": symbol,
                           "from": from_date, "to": to_date})
        if self.daily_error is not None:
            raise self.daily_error
        return list(self.daily.get(symbol.upper(), []))

    async def get_intraday_history(self, symbol: str, *, multiplier: int,
                                   timespan: str, start=None, end=None,
                                   limit=None) -> Dict[str, Any]:
        self.calls.append({"method": "get_intraday_history", "symbol": symbol,
                           "multiplier": multiplier, "timespan": timespan})
        if self.intraday_error is not None:
            raise self.intraday_error
        return dict(self.intraday.get(symbol.upper(),
                                      {"symbol": symbol.upper(), "provider": "fake", "bars": []}))

    def call_count(self) -> int:
        return len(self.calls)


def rate_limited_error() -> MassiveApiError:
    return MassiveApiError("aggs", 429, "rate limited")


def auth_error() -> MassiveApiError:
    return MassiveApiError("aggs", 401, "authentication failed")


def unavailable_error() -> MassiveApiError:
    return MassiveApiError("aggs", 503, "service unavailable")
