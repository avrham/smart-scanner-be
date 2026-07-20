"""
Ticker management and filtering utilities
Handles FMP stock list updates and candidate pool generation
"""

import logging
import random
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from app.workers.fmp_client import FMPClient
from app.workers.persistence import upsert_ticker, get_candidate_tickers
from app.config import settings


logger = logging.getLogger(__name__)


EXCHANGE_FILTERS = ["NASDAQ", "NYSE", "AMEX"]
EXCLUDED_SECTORS = ["ETF", "FUND", "REIT"]


async def refresh_tickers_cache(fmp: FMPClient) -> int:
    """
    Refresh ticker cache from the FMP stock screener.

    Uses the screener as the source because it returns REAL market cap AND REAL
    traded volume (B10 fix). The previous implementation fabricated volume from
    market_cap / price, which corrupted downstream liquidity filtering. We never
    fabricate volume: when it is unavailable we store NULL (unknown) so the
    universe filter can reject/downgrade the symbol per config.

    Returns number of tickers saved.
    """
    logger.info("Starting ticker cache refresh (real market cap + volume via screener)")

    try:
        screener_stocks = await fmp.get_stock_screener(
            market_cap_more_than=50_000_000,
            volume_more_than=100_000,
            exchange=",".join(EXCHANGE_FILTERS),
            limit=5000,
        )

        if not screener_stocks:
            logger.warning("Screener returned no stocks; ticker cache not refreshed")
            return 0

        logger.info(f"Retrieved {len(screener_stocks)} stocks from FMP screener")

        valid_count = 0
        unknown_volume_count = 0

        for stock in screener_stocks:
            symbol = (stock.get("symbol") or "").strip().upper()
            exchange = (stock.get("exchangeShortName") or "").strip().upper()

            if not symbol or len(symbol) > 10 or exchange not in EXCHANGE_FILTERS:
                continue

            price = stock.get("price")
            if price is not None and float(price) < 1.0:
                continue

            market_cap = stock.get("marketCap")
            # REAL traded volume from the screener; NEVER fabricated.
            raw_volume = stock.get("volume")
            last_volume = (
                float(raw_volume) if raw_volume not in (None, "", 0) else None
            )
            if last_volume is None:
                unknown_volume_count += 1

            try:
                await upsert_ticker(
                    symbol=symbol,
                    name=(stock.get("companyName") or "").strip(),
                    exchange=exchange,
                    market_cap=float(market_cap) if market_cap else None,
                    last_volume=last_volume,
                    is_active=True,
                    # Screener-passing rows are FMP's universe; mark them
                    # eligible so the funnel's eligible=true filter includes
                    # them when running on the FMP fallback.
                    eligible=True,
                )
                valid_count += 1
            except Exception as e:
                logger.warning(f"Failed to save ticker {symbol}: {e}")

        logger.info(
            f"Ticker refresh complete: saved {valid_count} tickers "
            f"({unknown_volume_count} with unknown volume)"
        )

        return valid_count

    except Exception as e:
        logger.error(f"Failed to refresh ticker cache: {e}")
        raise


async def load_candidate_pool(
    fmp: Any,
    min_market_cap: float = None,
    min_volume: float = None
) -> List[str]:
    """
    Load filtered candidate pool for scanning.

    `fmp` may be any MarketDataProvider; the dynamic screener/stock-list
    fallbacks below are FMP-specific and are skipped (hasattr-guarded) for
    providers that don't expose them (e.g. Massive, whose universe comes from
    the reference sync instead).
    """
    
    # Use config defaults if not provided
    if min_market_cap is None:
        min_market_cap = 200_000_000  # $200M
    
    if min_volume is None:
        min_volume = 200_000  # 200k shares
    
    logger.info(
        f"Loading candidate pool with min_cap=${min_market_cap:,.0f}, "
        f"min_volume={min_volume:,.0f}"
    )
    
    try:
        # Get candidates from database
        candidates = await get_candidate_tickers(
            min_market_cap=min_market_cap,
            min_volume=min_volume,
            exchanges=EXCHANGE_FILTERS
        )
        
        # If we don't have enough candidates, use dynamic fallback (skip slow refresh)
        if len(candidates) < 1000:
            logger.info(f"⚠️ Only {len(candidates)} candidates found in DB, using dynamic fallback")
            
            # Try to get a filtered list from FMP stock screener first
            # (FMP-only capability; other providers skip to the next fallback).
            try:
                if not hasattr(fmp, "get_stock_screener"):
                    raise AttributeError("provider has no stock screener")
                logger.info("Attempting to use FMP stock screener for dynamic candidates...")
                screener_stocks = await fmp.get_stock_screener(
                    market_cap_more_than=min_market_cap,
                    volume_more_than=min_volume,
                    limit=1000
                )
                
                if screener_stocks and len(screener_stocks) > 0:
                    dynamic_candidates = []
                    for stock in screener_stocks:
                        symbol = stock.get('symbol', '').strip().upper()
                        if symbol and len(symbol) <= 10 and symbol.isalpha():
                            dynamic_candidates.append(symbol)
                        
                        if len(dynamic_candidates) >= 500:
                            break
                    
                    if len(dynamic_candidates) > 0:
                        logger.info(f"📋 Using stock screener candidate list: {len(dynamic_candidates)} symbols")
                        return dynamic_candidates
                        
            except Exception as e:
                logger.warning(f"Stock screener failed, falling back to stock list: {e}")
            
            # Fallback to basic stock list (FMP-only; skipped for providers
            # without it — the curated list below is used instead)
            try:
                if not hasattr(fmp, "list_stocks"):
                    raise AttributeError("provider has no stock list")
                all_stocks = await fmp.list_stocks()
                dynamic_candidates = []
                
                # Use a curated list of major stocks from the full list
                for stock in all_stocks[:2000]:  # Take first 2000 to get good variety
                    symbol = stock.get('symbol', '').strip().upper()
                    exchange = stock.get('exchangeShortName', '').strip().upper()
                    price = stock.get('price', 0)
                    
                    # Filter for major exchanges and reasonable price (no market cap since not available)
                    if (symbol and len(symbol) <= 10 and symbol.isalpha() and
                        exchange in ['NASDAQ', 'NYSE', 'AMEX'] and
                        price and float(price) >= 5.0):  # $5 minimum to avoid penny stocks
                        dynamic_candidates.append(symbol)
                    
                    # Get enough candidates and stop
                    if len(dynamic_candidates) >= 500:
                        break
                
                if len(dynamic_candidates) > 0:
                    logger.info(f"📋 Using dynamic candidate list: {len(dynamic_candidates)} symbols")
                    return dynamic_candidates
                        
            except Exception as e:
                logger.error(f"Failed to get dynamic fallback list: {e}")
            
            # Use curated list of quality stocks as last resort
            curated_fallback = [
                # Large Cap Tech
                "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "NFLX", "ADBE", "CRM", "ORCL", 
                "INTC", "AMD", "QCOM", "AVGO", "TXN", "MU", "AMAT", "LRCX", "KLAC", "MRVL", "SNPS",
                
                # Financial Services  
                "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SPGI", "AXP", "V", "MA", "PYPL", "SQ",
                
                # Healthcare & Biotech
                "JNJ", "PFE", "UNH", "ABBV", "TMO", "ABT", "DHR", "BMY", "MRK", "LLY", "GILD", "AMGN",
                
                # Consumer & Retail
                "WMT", "HD", "PG", "KO", "PEP", "NKE", "SBUX", "MCD", "DIS", "COST", "TGT", "LOW",
                
                # Industrial & Energy
                "CAT", "DE", "GE", "HON", "UPS", "RTX", "LMT", "BA", "XOM", "CVX", "COP", "EOG",
                
                # Growth Stocks
                "SHOP", "ROKU", "ZOOM", "DOCU", "SNOW", "PLTR", "COIN", "RBLX", "SPOT", "SQ", "UBER", "LYFT"
            ]
            
            logger.info(f"📋 Using curated fallback list: {len(curated_fallback)} symbols")
            return curated_fallback
        
        # Additional filtering for quality
        filtered_candidates = []
        for symbol in candidates:
            # Skip symbols with weird characters or formats
            if not symbol.isalpha() or len(symbol) > 5:
                continue
            
            # Skip known problematic patterns
            skip_patterns = ["TEST", "WARR", "WS", "RT", "UN", "PFD"]
            if any(pattern in symbol for pattern in skip_patterns):
                continue
            
            filtered_candidates.append(symbol)
        
        logger.info(f"Candidate pool loaded: {len(filtered_candidates)} symbols")
        
        return filtered_candidates
        
    except Exception as e:
        logger.error(f"Failed to load candidate pool: {e}")
        # Return fallback list of well-known symbols
        fallback_symbols = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX",
            "ADBE", "CRM", "ORCL", "INTC", "AMD", "PYPL", "SPOT", "SQ",
            "SHOP", "ROKU", "ZOOM", "DOCU", "SNOW", "PLTR", "COIN", "RBLX",
            "JPM", "BAC", "WMT", "JNJ", "PG", "UNH", "HD", "MA", "V", "DIS",
            "NKE", "PFE", "TMO", "ABT", "KO", "PEP", "AVGO", "TXN", "QCOM",
            "HON", "LOW", "UPS", "RTX", "IBM", "CAT", "DE", "MMC", "GS",
            "AXP", "VZ", "T", "CMCSA", "PM", "UNP", "LMT", "BA", "GE", "F",
            "GM", "XOM", "CVX", "COP", "EOG", "SLB", "HAL", "BKR", "MPC",
            "VLO", "PSX", "OXY", "DVN", "PXD", "EOG", "COP", "XOM", "CVX"
        ]
        logger.info(f"Using fallback symbol list: {len(fallback_symbols)} symbols")
        return fallback_symbols


def select_random_batch(candidates: List[str], batch_size: int) -> List[str]:
    """Select random batch from candidates for scanning"""
    
    logger.info(f"🎲 Selecting batch: {len(candidates)} candidates available, batch_size={batch_size}")
    
    if len(candidates) <= batch_size:
        logger.info(f"🎲 Using all {len(candidates)} candidates (less than batch_size)")
        return candidates.copy()
    
    # Use random sampling without replacement
    selected = random.sample(candidates, batch_size)
    
    logger.info(f"🎲 Selected {len(selected)} symbols from {len(candidates)} candidates")
    
    return selected


async def update_ticker_volume(symbol: str, volume: float) -> None:
    """Update ticker with actual volume data from scan"""
    try:
        await upsert_ticker(
            symbol=symbol,
            last_volume=volume
        )
    except Exception as e:
        logger.warning(f"Failed to update volume for {symbol}: {e}")


def filter_by_liquidity(
    stock_data: Dict[str, Any],
    min_avg_volume: float = 200_000,
    min_price: float = 1.0,
) -> Tuple[bool, Optional[str]]:
    """
    Check liquidity using REAL data from the historical price series.

    Volume here is genuine traded volume from FMP historical bars (not
    fabricated). Market-cap enforcement is intentionally NOT performed here:
    shares outstanding are not present in the historical payload, so any
    market-cap number would be a guess. Market-cap filtering is applied at the
    universe level in get_candidate_tickers()/the screener instead (B9).

    Returns (passed, rejection_reason). rejection_reason is None when passed.
    """
    try:
        historical = stock_data.get("historical") if stock_data else None
        if not historical:
            return False, "no_historical_data"

        recent_data = historical[:20] if len(historical) >= 20 else historical
        if not recent_data:
            return False, "no_historical_data"

        volumes = [
            float(day.get("volume", 0) or 0)
            for day in recent_data
            if day.get("volume") is not None
        ]
        if not volumes:
            # Real volume unavailable: reject rather than invent a value.
            return False, "volume_unknown"

        avg_volume = sum(volumes) / len(volumes)
        latest_price = float(recent_data[0].get("close", 0) or 0)

        if latest_price < min_price:
            return False, "price_below_min"
        if avg_volume < min_avg_volume:
            return False, "avg_volume_below_min"

        return True, None

    except Exception as e:
        logger.warning(f"Failed to check liquidity: {e}")
        return False, "liquidity_check_error"
