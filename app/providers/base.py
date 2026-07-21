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
from typing import Any, Dict, List, Optional


class MarketDataProvider(ABC):
    """Contract every market data provider implements."""

    name: str = "unknown"

    # Whether get_daily_bars performs REAL bounded date-range retrieval
    # (arbitrary historical [from_date, to_date] windows) rather than
    # filtering a fixed latest-N payload. Phase 8.1B2 outcome calculation
    # requires True: an old range served from a latest-N shim would silently
    # lose bars and corrupt forward-bar alignment. Conservative default.
    supports_bounded_daily_range: bool = False

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
