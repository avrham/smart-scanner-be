"""
Ticker management and filtering utilities
Handles FMP stock list updates and candidate pool generation
"""

import logging
import random
from typing import List, Dict, Any
from datetime import datetime

from app.workers.fmp_client import FMPClient
from app.workers.persistence import upsert_ticker, get_candidate_tickers
from app.config import settings


logger = logging.getLogger(__name__)


EXCHANGE_FILTERS = ["NASDAQ", "NYSE", "AMEX"]
EXCLUDED_SECTORS = ["ETF", "FUND", "REIT"]


async def refresh_tickers_cache(fmp: FMPClient) -> int:
    """
    Refresh ticker cache from FMP stock list
    Returns number of tickers processed
    """
    logger.info("Starting ticker cache refresh")
    
    try:
        # Get stock list from FMP
        stock_list = await fmp.list_stocks()
        
        if not stock_list:
            logger.warning("No stocks returned from FMP")
            return 0
        
        logger.info(f"Retrieved {len(stock_list)} stocks from FMP")
        
        # Filter the stock list BEFORE processing to avoid processing 85k stocks
        logger.info("Pre-filtering stocks to reduce processing time...")
        filtered_stocks = []
        
        for stock in stock_list:
            # Quick pre-filter to avoid processing irrelevant stocks
            symbol = stock.get("symbol", "").strip().upper()
            exchange = stock.get("exchangeShortName", "").strip().upper()
            market_cap = stock.get("marketCap")
            price = stock.get("price")
            
            # Basic validation and filtering
            if (symbol and len(symbol) <= 10 and 
                exchange in EXCHANGE_FILTERS and
                price is not None and float(price) >= 1.0 and
                market_cap is not None and float(market_cap) >= 50_000_000):
                filtered_stocks.append(stock)
                
                # Limit to 5000 stocks to avoid extremely long processing
                if len(filtered_stocks) >= 5000:
                    break
        
        logger.info(f"Pre-filtered to {len(filtered_stocks)} relevant stocks (from {len(stock_list)})")
        
        processed_count = 0
        valid_count = 0
        
        # Process in batches for better performance
        batch_size = 100
        ticker_batch = []
        
        for stock in filtered_stocks:
            processed_count += 1
            
            # Extract stock information
            symbol = stock.get("symbol", "").strip().upper()
            name = stock.get("name", "").strip()
            exchange = stock.get("exchangeShortName", "").strip().upper()
            market_cap = stock.get("marketCap")
            price = stock.get("price")
            
            # Basic validation
            if not symbol or len(symbol) > 10:
                continue
            
            # Filter by exchange
            if exchange not in EXCHANGE_FILTERS:
                continue
            
            # Skip penny stocks and very low prices
            if price is not None and float(price) < 1.0:
                continue
            
            # Skip very small market cap
            if market_cap is not None and float(market_cap) < 50_000_000:  # $50M minimum
                continue
            
            # Estimate volume (FMP stock list doesn't always have volume)
            estimated_volume = None
            if market_cap and price:
                # Rough estimate based on market cap
                estimated_volume = max(100_000, float(market_cap) / (float(price) * 1000))
            
            # Add to batch
            ticker_batch.append({
                "symbol": symbol,
                "name": name,
                "exchange": exchange,
                "market_cap": float(market_cap) if market_cap else None,
                "last_volume": estimated_volume,
                "is_active": True
            })
            
            # Process batch when it reaches batch_size
            if len(ticker_batch) >= batch_size:
                try:
                    # Use individual upserts for now (batch_upsert_tickers has issues)
                    for ticker in ticker_batch:
                        await upsert_ticker(
                            symbol=ticker["symbol"],
                            name=ticker["name"],
                            exchange=ticker["exchange"],
                            market_cap=ticker["market_cap"],
                            last_volume=ticker["last_volume"],
                            is_active=ticker["is_active"]
                        )
                    valid_count += len(ticker_batch)
                    logger.info(f"Processed batch of {len(ticker_batch)} tickers")
                except Exception as e:
                    logger.warning(f"Failed to save ticker batch: {e}")
                
                ticker_batch = []
        
        # Process remaining tickers
        if ticker_batch:
            try:
                # Use individual upserts for now (batch_upsert_tickers has issues)
                for ticker in ticker_batch:
                    await upsert_ticker(
                        symbol=ticker["symbol"],
                        name=ticker["name"],
                        exchange=ticker["exchange"],
                        market_cap=ticker["market_cap"],
                        last_volume=ticker["last_volume"],
                        is_active=ticker["is_active"]
                    )
                valid_count += len(ticker_batch)
                logger.info(f"Processed final batch of {len(ticker_batch)} tickers")
            except Exception as e:
                logger.warning(f"Failed to save final ticker batch: {e}")
        
        logger.info(
            f"Ticker refresh complete: processed {processed_count}, "
            f"saved {valid_count} valid tickers"
        )
        
        return valid_count
        
    except Exception as e:
        logger.error(f"Failed to refresh ticker cache: {e}")
        raise


async def load_candidate_pool(
    fmp: FMPClient,
    min_market_cap: float = None,
    min_volume: float = None
) -> List[str]:
    """
    Load filtered candidate pool for scanning
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
            try:
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
            
            # Fallback to basic stock list (without market cap filtering since it's not available)
            try:
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
    min_market_cap: float = 200_000_000,
    min_avg_volume: float = 200_000
) -> bool:
    """
    Check if stock meets liquidity requirements based on actual data
    """
    
    try:
        # Extract data from FMP response
        if "historical" not in stock_data or not stock_data["historical"]:
            return False
        
        historical = stock_data["historical"]
        
        # Get recent volume data (last 20 days)
        recent_data = historical[:20] if len(historical) >= 20 else historical
        
        if not recent_data:
            return False
        
        # Calculate average volume
        volumes = [float(day.get("volume", 0)) for day in recent_data if day.get("volume")]
        
        if not volumes:
            return False
        
        avg_volume = sum(volumes) / len(volumes)
        
        # Get latest price for market cap estimation
        latest_price = float(recent_data[0].get("close", 0))
        
        # Rough market cap estimation (this is approximate)
        # In production, you'd want to get shares outstanding from FMP
        estimated_shares = 100_000_000  # Default estimate
        estimated_market_cap = latest_price * estimated_shares
        
        # Apply filters
        volume_check = avg_volume >= min_avg_volume
        price_check = latest_price >= 1.0  # No penny stocks
        
        return volume_check and price_check
        
    except Exception as e:
        logger.warning(f"Failed to check liquidity: {e}")
        return False
