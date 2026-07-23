"""Market data provider interface.

The scanner, strategies and admin routes depend on THIS contract — never on
Massive or FMP directly. Two groups of operations:

  * Discovery/ingestion: sync_universe, get_daily_market_summary,
    get_ticker_details, health_check.
  * Scan compatibility: batch_historical_data / fetch_historical_4h return the
    exact payload shapes the existing funnel already consumes
    ({"symbol", "historical": [...]}), so the funnel needs no changes.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union


class IntradayHistoryUnsupportedError(RuntimeError):
    """The configured provider cannot serve REAL bounded intraday ranges.

    Typed unsupported-capability state (Phase 9E1): callers must classify
    this explicitly instead of falling back to a latest-N shim that would
    silently misrepresent an as-of window.
    """


class MarketDataProvider(ABC):
    """Contract every market data provider implements."""

    name: str = "unknown"

    # Whether get_daily_bars performs REAL bounded date-range retrieval
    # (arbitrary historical [from_date, to_date] windows) rather than
    # filtering a fixed latest-N payload. Phase 8.1B2 outcome calculation
    # requires True: an old range served from a latest-N shim would silently
    # lose bars and corrupt forward-bar alignment. Conservative default.
    supports_bounded_daily_range: bool = False

    # Whether get_intraday_history performs REAL bounded intraday range
    # retrieval (Phase 9E1). Same honesty rule as
    # supports_bounded_daily_range: a provider whose intraday endpoint only
    # serves a fixed latest-N window must keep this False rather than
    # client-side-filter a window it cannot actually bound. Conservative
    # default.
    supports_intraday_history: bool = False

    # ---- discovery / ingestion ---- #

    @abstractmethod
    async def sync_universe(self) -> Dict[str, Any]:
        """Refresh the local ticker universe. Returns a summary dict."""

    @abstractmethod
    async def get_daily_market_summary(self, trading_date: str) -> Dict[str, Any]:
        """Ingest whole-market daily OHLCV for a date. Returns a summary dict."""

    @abstractmethod
    async def get_daily_bars(
        self, symbol: str, from_date: str, to_date: str
    ) -> List[Dict[str, Any]]:
        """Canonical daily bars for one symbol over a range."""

    @abstractmethod
    async def get_ticker_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Reference details (market cap etc.) for one symbol; None if unknown."""

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Safe provider status (never includes credentials)."""

    # ---- scan compatibility (funnel duck-typing) ---- #

    @abstractmethod
    async def get_daily_history(self, symbol: str, timeseries: int = 400) -> Dict[str, Any]:
        """FMP-shaped daily history payload for ONE symbol.

        Used by outcome calculation (single-symbol + benchmark fetches).
        """

    @abstractmethod
    async def batch_historical_data(
        self, symbols: List[str], timeseries: int = 350
    ) -> Dict[str, Dict[str, Any]]:
        """FMP-shaped daily history payloads per symbol."""

    @abstractmethod
    async def fetch_historical_4h(
        self, symbol: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """FMP-shaped 4H payload; empty `historical` when unavailable."""

    # ---- canonical intraday history (Phase 9E1) ---- #

    async def get_intraday_history(
        self,
        symbol: str,
        *,
        multiplier: int,
        timespan: str,
        start: Union[date, datetime, str, None] = None,
        end: Union[date, datetime, str, None] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Normalized typed intraday bars over an explicit bounded range.

        Generic timeframe request (e.g. multiplier=4, timespan="hour" for the
        canonical 4H frame). Implementations must return:

            {"symbol", "provider", "multiplier", "timespan",
             "requested_start", "requested_end",
             "bars": [{"start_utc": tz-aware datetime (bar START),
                       "open", "high", "low", "close", "volume"}, ...],
             "skipped_malformed": int,
             "dropped_exact_duplicates": int}

        Contract: bars sorted ascending by start_utc; exact-duplicate rows
        (identical start AND identical OHLCV) dropped deterministically
        (keep-first, counted); rows sharing a start with DIFFERENT values are
        preserved so the canonical frame layer can reject them explicitly;
        the currently-forming bucket is NOT excluded here — completed-bar
        semantics belong to the canonical frame builder. Providers that
        cannot honestly serve a bounded intraday range keep
        supports_intraday_history=False and raise
        IntradayHistoryUnsupportedError (this default).
        """
        raise IntradayHistoryUnsupportedError(
            f"provider '{self.name}' does not support bounded intraday history"
        )
