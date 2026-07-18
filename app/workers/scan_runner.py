"""
Scan orchestration and batch processing
Main worker logic for running pattern scans
"""

import asyncio
import logging
import random
from datetime import datetime, date
from typing import List, Dict, Any, Optional

from app.workers.fmp_client import FMPClient
from app.workers.persistence import was_seen_today, mark_seen_today, save_signal, log_pattern_run
from app.workers.patterns.sma150 import evaluate_sma150_bounce
from app.workers.indicators import to_dataframe, validate_dataframe
from app.workers.tickers import load_candidate_pool, select_random_batch, filter_by_liquidity
from app.config import settings
from app.utils.events import event_bus
import pandas as pd


logger = logging.getLogger(__name__)


async def process_single_symbol(
    fmp: FMPClient,
    symbol: str,
    pattern_code: str = "sma150_bounce",
    scan_date: date = None,
    ignore_seen: bool = False
) -> Dict[str, Any]:
    """
    Process a single symbol for pattern detection
    
    Returns:
        dict: {
            "symbol": str,
            "processed": bool,
            "result": dict or None,
            "error": str or None
        }
    """
    
    if scan_date is None:
        scan_date = date.today()
    
    logger.debug(f"Processing {symbol}")
    
    try:
        # Check if already seen today (unless forced)
        if not ignore_seen and await was_seen_today(symbol, scan_date):
            logger.info(f"⏭️ {symbol} already scanned today, skipping")
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": "Already seen today"
            }
        
        # Fetch historical data
        try:
            logger.info(f"📊 Fetching data for {symbol}...")
            fmp_data = await fmp.get_historical_data(symbol, timeseries=350)
            data_points = len(fmp_data.get('historical', []))
            logger.info(f"📊 {symbol}: Got {data_points} data points from FMP")
        except Exception as e:
            logger.error(f"❌ FMP API error for {symbol}: {str(e)}")
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": f"FMP API error: {str(e)}"
            }
        
        if not fmp_data.get("historical"):
            logger.warning(f"⚠️ No historical data for {symbol}")
            await mark_seen_today(symbol, scan_date)
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": "No historical data"
            }
        
        # Convert to DataFrame
        try:
            df = to_dataframe(fmp_data)
            logger.info(f"📈 {symbol}: DataFrame created with {len(df)} rows")
        except Exception as e:
            logger.error(f"❌ DataFrame conversion error for {symbol}: {str(e)}")
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": f"DataFrame error: {str(e)}"
            }
        
        if not validate_dataframe(df, min_bars=200):
            logger.warning(f"⚠️ Insufficient or invalid data for {symbol} (need 200 bars, got {len(df)})")
            await mark_seen_today(symbol, scan_date)
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": "Invalid data"
            }
        
        # Check liquidity filters
        if not filter_by_liquidity(fmp_data):
            logger.info(f"💧 {symbol} failed liquidity check (market cap or volume too low)")
            await mark_seen_today(symbol, scan_date)
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": "Failed liquidity check"
            }
        
        # Run pattern detection
        try:
            logger.info(f"🔍 Running pattern detection for {symbol}...")
            if pattern_code == "sma150_bounce":
                result = evaluate_sma150_bounce(symbol, df)
                logger.info(f"🔍 {symbol}: {result['verdict']} (score: {result.get('score', 'N/A'):.3f}, reason: {result.get('reason', 'N/A')})")
            else:
                raise ValueError(f"Unknown pattern code: {pattern_code}")
        except Exception as e:
            logger.error(f"❌ Pattern detection error for {symbol}: {str(e)}")
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": f"Pattern detection error: {str(e)}"
            }
        
        # Mark as seen
        await mark_seen_today(symbol, scan_date)
        
        # Save signal if ENTER or if debug mode enabled
        should_save = (
            result["verdict"] == "ENTER" or 
            (settings.DEBUG_SAVE_AVOID and result["verdict"] == "AVOID")
        )
        
        if should_save:
            try:
                signal_id = await save_signal(
                    symbol=symbol,
                    pattern_code=pattern_code,
                    verdict=result["verdict"],
                    score=result["score"],
                    reason=result["reason"],
                    details=result["details"],
                    snapshot_date=date.fromisoformat(result["details"]["snapshot_date"])
                )
                
                result["signal_id"] = signal_id
                
                # Log ENTER signals
                if result["verdict"] == "ENTER":
                    logger.info(
                        f"🎯 ENTER signal for {symbol}: score={result['score']:.3f}, "
                        f"reason={result['reason']}"
                    )
                
            except Exception as e:
                logger.error(f"Failed to save signal for {symbol}: {e}")
                # Don't fail the entire processing, just log the error
                result["save_error"] = str(e)
        
        return {
            "symbol": symbol,
            "processed": True,
            "result": result,
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Failed to process {symbol}: {e}")
        
        # Still mark as seen to avoid retry loops
        try:
            await mark_seen_today(symbol, scan_date)
        except Exception:
            pass
        
        return {
            "symbol": symbol,
            "processed": False,
            "result": None,
            "error": str(e)
        }


async def process_single_symbol_with_data(
    symbol: str,
    fmp_data: Dict[str, Any],
    pattern_code: str = "sma150_bounce",
    scan_date: date = None,
    ignore_seen: bool = False
) -> Dict[str, Any]:
    """
    Process a single symbol for pattern detection using pre-fetched data
    
    Returns:
        dict: {
            "symbol": str,
            "processed": bool,
            "result": dict or None,
            "error": str or None
        }
    """
    
    if scan_date is None:
        scan_date = date.today()
    
    logger.debug(f"Processing {symbol} with pre-fetched data")
    
    try:
        # Check if already seen today (unless forced)
        if not ignore_seen and await was_seen_today(symbol, scan_date):
            logger.info(f"⏭️ {symbol} already scanned today, skipping")
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": "Already seen today"
            }
        
        if not fmp_data.get("historical"):
            logger.warning(f"⚠️ No historical data for {symbol}")
            await mark_seen_today(symbol, scan_date)
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": "No historical data"
            }
        
        # Convert to DataFrame
        try:
            df = to_dataframe(fmp_data)
            logger.info(f"📈 {symbol}: DataFrame created with {len(df)} rows")
        except Exception as e:
            logger.error(f"❌ DataFrame conversion error for {symbol}: {str(e)}")
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": f"DataFrame error: {str(e)}"
            }
        
        if not validate_dataframe(df, min_bars=200):
            logger.warning(f"⚠️ Insufficient or invalid data for {symbol} (need 200 bars, got {len(df)})")
            await mark_seen_today(symbol, scan_date)
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": "Invalid data"
            }
        
        # Check liquidity filters
        if not filter_by_liquidity(fmp_data):
            logger.info(f"💧 {symbol} failed liquidity check (market cap or volume too low)")
            await mark_seen_today(symbol, scan_date)
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": "Failed liquidity check"
            }
        
        # Run pattern detection
        try:
            logger.info(f"🔍 Running pattern detection for {symbol}...")
            if pattern_code == "sma150_bounce":
                result = evaluate_sma150_bounce(symbol, df)
                logger.info(f"🔍 {symbol}: {result['verdict']} (score: {result.get('score', 'N/A'):.3f}, reason: {result.get('reason', 'N/A')})")
            else:
                raise ValueError(f"Unknown pattern code: {pattern_code}")
        except Exception as e:
            logger.error(f"❌ Pattern detection error for {symbol}: {str(e)}")
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": f"Pattern detection error: {str(e)}"
            }
        
        # Mark as seen
        await mark_seen_today(symbol, scan_date)
        
        # Save signal if ENTER or if debug mode enabled
        should_save = (
            result["verdict"] == "ENTER" or 
            (settings.DEBUG_SAVE_AVOID and result["verdict"] == "AVOID")
        )
        
        if should_save:
            try:
                signal_id = await save_signal(
                    symbol=symbol,
                    pattern_code=pattern_code,
                    verdict=result["verdict"],
                    score=result["score"],
                    reason=result["reason"],
                    details=result["details"],
                    snapshot_date=date.fromisoformat(result["details"]["snapshot_date"])
                )
                
                result["signal_id"] = signal_id
                
                # Log ENTER signals
                if result["verdict"] == "ENTER":
                    logger.info(
                        f"🎯 ENTER signal for {symbol}: score={result['score']:.3f}, "
                        f"reason={result['reason']}"
                    )
                
            except Exception as e:
                logger.error(f"Failed to save signal for {symbol}: {e}")
                # Don't fail the entire processing, just log the error
                result["save_error"] = str(e)
        
        return {
            "symbol": symbol,
            "processed": True,
            "result": result,
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Failed to process {symbol}: {e}")
        
        # Still mark as seen to avoid retry loops
        try:
            await mark_seen_today(symbol, scan_date)
        except Exception:
            pass
        
        return {
            "symbol": symbol,
            "processed": False,
            "result": None,
            "error": str(e)
        }


async def run_scan_batch(
    fmp: FMPClient,
    batch_size: int = 150,
    pattern_code: str = "sma150_bounce",
    max_concurrent: int = 10,
    symbols: List[str] | None = None,
    ignore_seen: bool = False,
    scan_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run a complete scan batch
    
    Returns:
        dict: Summary statistics of the scan run
    """
    
    run_start = datetime.utcnow()
    logger.info(f"Starting scan batch: size={batch_size}, pattern={pattern_code}")
    
    try:
        # Determine symbols to scan
        if scan_id:
            await event_bus.publish(scan_id, {"type": "stage", "stage": "loading_candidates"})

        if symbols and len(symbols) > 0:
            # Use provided symbols (uppercased, unique, preserve order, cap by batch_size if set)
            seen: set[str] = set()
            normalized: List[str] = []
            for s in symbols:
                su = (s or "").strip().upper()
                if su and su not in seen:
                    seen.add(su)
                    normalized.append(su)
            selected_symbols = normalized[: batch_size or len(normalized)]
            logger.info(f"Using provided symbols: count={len(selected_symbols)}")
        else:
            # Load candidate pool from cache/FMP and pick a random batch
            candidate_pool = await load_candidate_pool(fmp)
            if not candidate_pool:
                logger.error("No candidates available for scanning")
                if scan_id:
                    await event_bus.publish(scan_id, {"type": "error", "error": "No candidates available"})
                return {
                    "success": False,
                    "error": "No candidates available",
                    "scanned_count": 0,
                    "enter_count": 0,
                    "rejected_count": 0
                }
            selected_symbols = select_random_batch(candidate_pool, batch_size)
        
        logger.info(f"Selected {len(selected_symbols)} symbols for scanning: {selected_symbols}")
        if scan_id:
            await event_bus.publish(scan_id, {"type": "stage", "stage": "fetching_data", "total": len(selected_symbols)})
        
        # Fetch all historical data in one batch to avoid rate limiting
        logger.info(f"📊 Fetching historical data for all {len(selected_symbols)} symbols in batch...")
        historical_data_batch = await fmp.batch_historical_data(selected_symbols, timeseries=350)
        logger.info(f"📊 Successfully fetched data for {len(historical_data_batch)} symbols")
        
        if scan_id:
            await event_bus.publish(scan_id, {"type": "stage", "stage": "processing", "total": len(selected_symbols)})
        
        # Process symbols with the pre-fetched data
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_semaphore(symbol: str):
            async with semaphore:
                return await process_single_symbol_with_data(
                    symbol,
                    historical_data_batch.get(symbol, {}),
                    pattern_code,
                    ignore_seen=ignore_seen
                )
        
        # Create tasks for all symbols
        tasks = [process_with_semaphore(symbol) for symbol in selected_symbols]
        
        # Process all symbols
        processed_so_far = 0
        results = []
        for coro in asyncio.as_completed(tasks):
            res = await coro
            results.append(res)
            processed_so_far += 1
            if scan_id:
                await event_bus.publish(scan_id, {"type": "progress", "processed": processed_so_far})
        
        # Analyze results
        scanned_count = 0
        enter_count = 0
        rejected_count = 0
        error_count = 0
        enter_signals = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task failed for {selected_symbols[i]}: {result}")
                error_count += 1
                continue
            
            if result["processed"]:
                scanned_count += 1
                
                if result["result"]:
                    if result["result"]["verdict"] == "ENTER":
                        enter_count += 1
                        enter_signals.append({
                            "symbol": result["symbol"],
                            "score": result["result"]["score"],
                            "reason": result["result"]["reason"]
                        })
                    else:
                        rejected_count += 1
            else:
                error_count += 1
                # Log detailed error information
                symbol = result.get("symbol", f"symbol_{i}")
                error_msg = result.get("error", "Unknown error")
                if "FMP API error" in error_msg:
                    logger.error(f"FMP API error for {symbol}: {error_msg}")
                elif "DataFrame error" in error_msg:
                    logger.error(f"DataFrame error for {symbol}: {error_msg}")
                elif "Pattern detection error" in error_msg:
                    logger.error(f"Pattern detection error for {symbol}: {error_msg}")
                else:
                    logger.error(f"Other error for {symbol}: {error_msg}")
        
        # Log pattern run telemetry
        try:
            await log_pattern_run(
                pattern_code=pattern_code,
                scanned_count=scanned_count,
                enter_count=enter_count,
                rejected_count=rejected_count,
                notes=f"Batch size: {batch_size}, Errors: {error_count}",
                run_started_at=run_start
            )
        except Exception as e:
            logger.error(f"Failed to log pattern run: {e}")
        
        run_duration = (datetime.utcnow() - run_start).total_seconds()
        
        # Summary
        summary = {
            "success": True,
            "run_duration_seconds": run_duration,
            "scanned_count": scanned_count,
            "enter_count": enter_count,
            "rejected_count": rejected_count,
            "error_count": error_count,
            "enter_signals": enter_signals,
            "batch_size": batch_size,
            "pattern_code": pattern_code,
            "started_at": run_start.isoformat(),
            "completed_at": datetime.utcnow().isoformat()
        }
        
        logger.info(
            f"📊 Scan batch completed: scanned={scanned_count}, "
            f"enter={enter_count}, rejected={rejected_count}, "
            f"errors={error_count}, duration={run_duration:.1f}s"
        )
        
        # Log detailed breakdown
        logger.info(f"📋 Detailed breakdown:")
        logger.info(f"  - Symbols processed: {scanned_count}")
        logger.info(f"  - ENTER signals: {enter_count}")
        logger.info(f"  - Rejected (AVOID): {rejected_count}")
        logger.info(f"  - Errors: {error_count}")
        logger.info(f"  - Total symbols attempted: {len(selected_symbols)}")
        
        # Log enter signals summary
        if enter_signals:
            logger.info("🎯 ENTER signals found:")
            for signal in enter_signals:
                logger.info(f"  {signal['symbol']}: {signal['score']:.3f} - {signal['reason']}")
        
        if scan_id:
            await event_bus.publish(scan_id, {"type": "finished", **summary})
        return summary
        
    except Exception as e:
        run_duration = (datetime.utcnow() - run_start).total_seconds()
        logger.error(f"Scan batch failed after {run_duration:.1f}s: {e}")
        
        return {
            "success": False,
            "error": str(e),
            "run_duration_seconds": run_duration,
            "scanned_count": 0,
            "enter_count": 0,
            "rejected_count": 0,
            "started_at": run_start.isoformat(),
            "completed_at": datetime.utcnow().isoformat()
        }


async def run_maintenance_tasks():
    """Run periodic maintenance tasks"""
    logger.info("Running maintenance tasks")
    
    try:
        from app.workers.maintenance import cleanup_daily_seen
        from app.deps import get_db
        
        # Cleanup old daily_seen entries
        async with get_db() as db:
            await cleanup_daily_seen(db)
        
        logger.info("Maintenance tasks completed")
        
    except Exception as e:
        logger.error(f"Maintenance tasks failed: {e}")


if __name__ == "__main__":
    # For testing - run a single batch
    async def test_run():
        from app.config import settings
        
        fmp = FMPClient(
            api_key=settings.FMP_API_KEY,
            max_concurrent=settings.FMP_MAX_CONCURRENT
        )
        
        result = await run_scan_batch(fmp, batch_size=10)
        print(f"Test run result: {result}")
    
    asyncio.run(test_run())
