"""FMP implementation of MarketDataProvider (fallback provider).

Wraps the existing FMPClient without changing its behavior. Kept available so
MARKET_DATA_PROVIDER=fmp restores the pre-Massive flow exactly.
"""

import logging
from typing import Any, Dict, List, Optional

from app.config import settings
from app.providers.base import MarketDataProvider
from app.workers.fmp_client import FMPClient


logger = logging.getLogger(__name__)


class FMPProvider(MarketDataProvider):
    name = "fmp"

    def __init__(self, client: Optional[FMPClient] = None):
        self.client = client or FMPClient(
            api_key=settings.FMP_API_KEY,
            max_concurrent=settings.FMP_MAX_CONCURRENT,
        )

    async def sync_universe(self) -> Dict[str, Any]:
        # Reuses the existing screener-based refresh (1 request).
        from app.workers.tickers import refresh_tickers_cache

        saved = await refresh_tickers_cache(self.client)
        return {"provider": self.name, "stored": saved}

    async def get_daily_market_summary(self, trading_date: str) -> Dict[str, Any]:
        # FMP has no equivalent whole-market grouped endpoint in this codebase.
        return {
            "provider": self.name,
            "supported": False,
            "message": "grouped daily ingestion is Massive-only",
        }

    async def get_daily_bars(
        self, symbol: str, from_date: str, to_date: str
    ) -> List[Dict[str, Any]]:
        payload = await self.client.get_historical_data(symbol, timeseries=600)
        bars = []
        for row in payload.get("historical") or []:
            if not (from_date <= row.get("date", "") <= to_date):
                continue
            bars.append(
                {
                    "symbol": symbol,
                    "trading_date": row["date"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "vwap": None,
                    "transaction_count": None,
                }
            )
        return bars

    async def get_ticker_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        return await self.client.get_company_profile(symbol)

    async def health_check(self) -> Dict[str, Any]:
        return {
            "provider": self.name,
            "connectivity": "not_probed",
        }

    # Scan compatibility: direct delegation (identical to pre-provider flow).

    async def get_daily_history(self, symbol: str, timeseries: int = 400) -> Dict[str, Any]:
        return await self.client.get_historical_data(symbol, timeseries=timeseries)

    async def batch_historical_data(
        self, symbols: List[str], timeseries: int = 350
    ) -> Dict[str, Dict[str, Any]]:
        return await self.client.batch_historical_data(symbols, timeseries=timeseries)

    async def fetch_historical_4h(
        self, symbol: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        return await self.client.fetch_historical_4h(symbol, limit=limit)
