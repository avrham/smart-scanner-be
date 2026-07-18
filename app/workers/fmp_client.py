"""
FMP (Financial Modeling Prep) API Client
Handles rate limiting, retry logic, and async requests
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
import aiohttp
import time
from datetime import datetime

from app.config import settings


logger = logging.getLogger(__name__)


class FMPClient:
    """Async FMP API client with rate limiting and retry logic"""
    
    def __init__(self, api_key: str, max_concurrent: int = 10):
        self.api_key = api_key
        self.base_url = settings.FMP_BASE_URL
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = asyncio.Semaphore(settings.FMP_RATE_LIMIT_PER_MIN)
        self.last_request_time = 0
        
    @asynccontextmanager
    async def _rate_limited_slot(self):
        """Context manager for rate-limited API calls"""
        async with self.semaphore:
            # Ensure minimum time between requests (for rate limiting)
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            min_interval = 60.0 / settings.FMP_RATE_LIMIT_PER_MIN  # seconds between requests
            
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
            
            self.last_request_time = time.time()
            yield
    
    async def _request(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make rate-limited request with retry logic"""
        params = params or {}
        params["apikey"] = self.api_key
        
        async with self._rate_limited_slot():
            for attempt in range(5):  # Max 5 retries
                try:
                    timeout = aiohttp.ClientTimeout(total=30)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        url = f"{self.base_url}{path}"
                        
                        async with session.get(url, params=params) as response:
                            if response.status == 429:  # Rate limited
                                wait_time = 0.5 * (2 ** attempt)
                                logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                                await asyncio.sleep(wait_time)
                                continue
                            
                            if response.status >= 500:  # Server error
                                wait_time = 0.5 * (2 ** attempt)
                                logger.warning(f"Server error {response.status}, waiting {wait_time}s before retry")
                                await asyncio.sleep(wait_time)
                                continue
                            
                            response.raise_for_status()
                            return await response.json()
                            
                except asyncio.TimeoutError:
                    wait_time = 0.5 * (2 ** attempt)
                    logger.warning(f"Timeout on attempt {attempt + 1}, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
                    
                except Exception as e:
                    if attempt == 4:  # Last attempt
                        logger.error(f"FMP request failed after all retries: {path}, error: {e}")
                        raise
                    wait_time = 0.5 * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                    continue
        
        raise RuntimeError(f"FMP request failed after all retries: {path}")
    
    async def list_stocks(self) -> List[Dict[str, Any]]:
        """Get list of all available stocks"""
        logger.info("Fetching stock list from FMP")
        try:
            result = await self._request("/stock/list")
            logger.info(f"Retrieved {len(result)} stocks from FMP")
            return result
        except Exception as e:
            logger.error(f"Failed to fetch stock list: {e}")
            raise
    
    async def get_historical_data(
        self, 
        symbol: str, 
        timeseries: int = 350
    ) -> Dict[str, Any]:
        """Get historical price data for a symbol"""
        path = f"/historical-price-full/{symbol}"
        # Request full OHLCV candles; omitting serietype returns full bars
        params = {"timeseries": timeseries}
        
        try:
            result = await self._request(path, params)
            if "historical" not in result:
                logger.warning(f"No historical data for {symbol}")
                return {"symbol": symbol, "historical": []}
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            raise
    
    async def get_company_profile(self, symbol: str) -> Dict[str, Any]:
        """Get company profile information"""
        path = f"/profile/{symbol}"
        
        try:
            result = await self._request(path)
            return result[0] if result and isinstance(result, list) else result
            
        except Exception as e:
            logger.error(f"Failed to fetch company profile for {symbol}: {e}")
            raise
    
    async def get_stock_screener(
        self, 
        market_cap_more_than: int = 200_000_000,
        volume_more_than: int = 200_000,
        exchange: str = "NASDAQ,NYSE,AMEX",
        limit: int = 2000
    ) -> List[Dict[str, Any]]:
        """Get stocks using FMP stock screener with market cap and volume filters"""
        logger.info(f"Fetching stocks via screener: cap>${market_cap_more_than:,}, vol>{volume_more_than:,}")
        
        params = {
            "marketCapMoreThan": market_cap_more_than,
            "volumeMoreThan": volume_more_than,
            "exchange": exchange,
            "limit": limit
        }
        
        try:
            result = await self._request("/stock-screener", params)
            logger.info(f"Stock screener returned {len(result)} stocks")
            return result
        except Exception as e:
            logger.error(f"Failed to fetch stock screener: {e}")
            raise

    async def batch_historical_data(
        self, 
        symbols: List[str], 
        timeseries: int = 350
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch historical data for multiple symbols concurrently"""
        logger.info(f"Fetching historical data for {len(symbols)} symbols")
        
        async def fetch_one(symbol: str) -> tuple[str, Dict[str, Any]]:
            try:
                data = await self.get_historical_data(symbol, timeseries)
                return symbol, data
            except Exception as e:
                logger.warning(f"Failed to fetch data for {symbol}: {e}")
                return symbol, {"symbol": symbol, "historical": []}
        
        # Process in batches to avoid overwhelming the API
        batch_size = min(20, len(symbols))
        results = {}
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            tasks = [fetch_one(symbol) for symbol in batch]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Batch request failed: {result}")
                    continue
                
                symbol, data = result
                results[symbol] = data
            
            # Small delay between batches
            if i + batch_size < len(symbols):
                await asyncio.sleep(0.1)
        
        logger.info(f"Successfully fetched data for {len(results)} symbols")
        return results
