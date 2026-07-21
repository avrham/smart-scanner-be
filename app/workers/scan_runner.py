"""
Scan orchestration and batch processing
Main worker logic for running pattern scans
"""

import asyncio
import json
import logging
import random
import uuid
from collections import Counter
from datetime import datetime, date
from typing import List, Dict, Any, Optional

from app.workers.persistence import was_seen_today, mark_seen_today, save_signal
from app.workers.patterns.sma150 import evaluate_sma150_bounce, DEFAULT_CONFIG, SCORE_VERSION
from app.workers.patterns.config import resolve_pattern_config
from app.workers.indicators import to_dataframe, validate_dataframe
from app.workers.tickers import load_candidate_pool, select_random_batch, filter_by_liquidity
from app.workers.provenance import (
    build_provenance,
    market_data_as_of_from_details,
    market_data_as_of_from_df,
)
from app.workers.scan_runs import create_scan_run, finalize_scan_run
from app.workers.strategies import StrategyContext, get_strategy
from app.config import settings
from app.utils.events import event_bus
import pandas as pd


def _resolve_default_config(pattern_code: str) -> Dict[str, Any]:
    """Safe defaults per pattern.

    sma150_bounce keeps its direct v2 defaults (unchanged behavior); any other
    explicitly selected pattern uses its registered strategy's defaults.
    """
    if pattern_code == "sma150_bounce":
        return DEFAULT_CONFIG.copy()
    try:
        return get_strategy(pattern_code).default_config()
    except Exception:
        return DEFAULT_CONFIG.copy()


def _evaluate_pattern(
    symbol: str,
    df: pd.DataFrame,
    pattern_code: str,
    config: Dict[str, Any],
    scan_run_id: Optional[str],
) -> tuple:
    """Evaluate one symbol on the legacy path.

    sma150_bounce keeps its DIRECT v2 evaluator call (byte-identical
    behavior). Any other explicitly selected pattern (e.g. sma150_bounce_v3)
    goes through the strategy registry — the minimum safe integration for
    Phase 8. Returns (legacy result dict, decision_policy_version or None).
    """
    if pattern_code == "sma150_bounce":
        return evaluate_sma150_bounce(symbol, df, config), None

    strategy = get_strategy(pattern_code)  # raises UnknownStrategyError
    context = StrategyContext(
        symbol=symbol,
        pattern_code=pattern_code,
        config=config,
        scanner_mode="legacy",
        scan_run_id=scan_run_id,
    )
    result = strategy.evaluate(df, context)
    legacy = {
        "verdict": result.verdict,
        "score": result.score,
        "reason": result.reason,
        "details": result.details,
    }
    return legacy, getattr(strategy, "decision_policy_version", None)


def _liquidity_params(config: Dict[str, Any]) -> Dict[str, float]:
    filters = config.get("min_liquidity_filters", {}) or {}
    return {
        "min_avg_volume": float(filters.get("min_daily_volume", 200_000)),
        "min_price": float(config.get("min_price", 1.0)),
    }


logger = logging.getLogger(__name__)


def _legacy_provenance(
    result: Dict[str, Any],
    pattern_code: str,
    config: Dict[str, Any],
    df: Optional[pd.DataFrame],
    scan_run_id: Optional[str],
    source_path: str,
    provider_name: Optional[str],
    decision_policy_version: Optional[str] = None,
) -> Dict[str, Any]:
    """Provenance for the legacy dict-result path (Phase 7B).

    strategy_version comes from the evaluator's own score_version (sma150.v2
    for the direct path; the strategy's version for registry patterns);
    market_data_as_of from the latest bar actually evaluated. Nothing is
    inferred from timestamps or pattern names beyond what the evaluator emits.
    """
    details = result.get("details") or {}
    return build_provenance(
        scan_run_id=scan_run_id,
        source_path=source_path,
        scanner_mode="legacy",
        provider=provider_name,
        strategy_code=pattern_code,
        strategy_version=details.get("score_version") or SCORE_VERSION,
        strategy_config=config,
        details=details,
        score_components=details.get("score_components"),
        # Same completion policy as the funnel path: prefer the strategy's
        # declared completed-bar as-of (v3), fall back to the frame (v2).
        market_data_as_of=(
            market_data_as_of_from_details(details)
            or market_data_as_of_from_df(df)
        ),
        decision_policy_version=decision_policy_version,
    )


async def process_single_symbol(
    fmp: Any,
    symbol: str,
    pattern_code: str = "sma150_bounce",
    scan_date: date = None,
    ignore_seen: bool = False,
    config: Optional[Dict[str, Any]] = None,
    scan_run_id: Optional[str] = None,
    source_path: str = "manual",  # documented no-scan-context path (direct call)
    provider_name: Optional[str] = None,
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

    if config is None:
        config = _resolve_default_config(pattern_code)
    
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
        
        # Check liquidity filters (real volume + price; reason captured for telemetry)
        liq_passed, liq_reason = filter_by_liquidity(fmp_data, **_liquidity_params(config))
        if not liq_passed:
            logger.info(f"💧 {symbol} failed liquidity check ({liq_reason})")
            await mark_seen_today(symbol, scan_date)
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": f"Failed liquidity check: {liq_reason}"
            }
        
        # Run pattern detection
        try:
            logger.info(f"🔍 Running pattern detection for {symbol}...")
            result, policy_version = _evaluate_pattern(
                symbol, df, pattern_code, config, scan_run_id
            )
            logger.info(f"🔍 {symbol}: {result['verdict']} (score: {result.get('score') if result.get('score') is not None else 'N/A'}, reason: {result.get('reason', 'N/A')})")
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
                save_result = await save_signal(
                    symbol=symbol,
                    pattern_code=pattern_code,
                    verdict=result["verdict"],
                    score=result["score"],
                    reason=result["reason"],
                    details=result["details"],
                    snapshot_date=date.fromisoformat(result["details"]["snapshot_date"]),
                    provenance=_legacy_provenance(
                        result, pattern_code, config, df,
                        scan_run_id, source_path, provider_name,
                        decision_policy_version=policy_version,
                    ),
                )

                result["signal_id"] = save_result["signal_id"]
                result["signal_created_new"] = save_result["created_new_signal"]
                
                # Log ENTER signals
                if result["verdict"] == "ENTER":
                    logger.info(
                        f"🎯 ENTER signal for {symbol}: score={result['score']}, "
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
    ignore_seen: bool = False,
    config: Optional[Dict[str, Any]] = None,
    scan_run_id: Optional[str] = None,
    source_path: str = "legacy",
    provider_name: Optional[str] = None,
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

    if config is None:
        config = _resolve_default_config(pattern_code)
    
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
        
        # Check liquidity filters (real volume + price; reason captured for telemetry)
        liq_passed, liq_reason = filter_by_liquidity(fmp_data, **_liquidity_params(config))
        if not liq_passed:
            logger.info(f"💧 {symbol} failed liquidity check ({liq_reason})")
            await mark_seen_today(symbol, scan_date)
            return {
                "symbol": symbol,
                "processed": False,
                "result": None,
                "error": f"Failed liquidity check: {liq_reason}"
            }
        
        # Run pattern detection
        try:
            logger.info(f"🔍 Running pattern detection for {symbol}...")
            result, policy_version = _evaluate_pattern(
                symbol, df, pattern_code, config, scan_run_id
            )
            logger.info(f"🔍 {symbol}: {result['verdict']} (score: {result.get('score') if result.get('score') is not None else 'N/A'}, reason: {result.get('reason', 'N/A')})")
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
                save_result = await save_signal(
                    symbol=symbol,
                    pattern_code=pattern_code,
                    verdict=result["verdict"],
                    score=result["score"],
                    reason=result["reason"],
                    details=result["details"],
                    snapshot_date=date.fromisoformat(result["details"]["snapshot_date"]),
                    provenance=_legacy_provenance(
                        result, pattern_code, config, df,
                        scan_run_id, source_path, provider_name,
                        decision_policy_version=policy_version,
                    ),
                )

                result["signal_id"] = save_result["signal_id"]
                result["signal_created_new"] = save_result["created_new_signal"]
                
                # Log ENTER signals
                if result["verdict"] == "ENTER":
                    logger.info(
                        f"🎯 ENTER signal for {symbol}: score={result['score']}, "
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
    fmp: Any,
    batch_size: int = 150,
    pattern_code: str = "sma150_bounce",
    max_concurrent: int = 10,
    symbols: List[str] | None = None,
    ignore_seen: bool = False,
    scan_id: Optional[str] = None,
    source_path: str = "legacy",
) -> Dict[str, Any]:
    """
    Run a complete scan batch
    
    Returns:
        dict: Summary statistics of the scan run
    """
    
    run_start = datetime.utcnow()
    logger.info(f"Starting scan batch: size={batch_size}, pattern={pattern_code}")

    # Phase 7B: every batch scan has a canonical scan-run identity (the same
    # UUID the admin endpoint/WebSocket use when they provided one).
    scan_id = scan_id or str(uuid.uuid4())
    provider_name = getattr(fmp, "name", None) or "unknown"

    # Resolve config once per run (B1): DB config overrides safe defaults.
    config = await resolve_pattern_config(pattern_code, _resolve_default_config(pattern_code))

    await create_scan_run(
        scan_run_id=scan_id,
        pattern_code=pattern_code,
        scanner_mode=source_path,
        provider=provider_name,
        dry_run=False,
        requested_limit=batch_size,
        scan_date=date.today(),
        run_started_at=run_start,
    )

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
                # Zero candidates is a NORMAL terminal outcome, not a failure:
                # the scan executed its candidate-loading step and legitimately
                # found nothing to evaluate. Finalize as completed with zero
                # counts, no error identity, and an explicit terminal_reason.
                logger.info("No candidates available for scanning; completing with zero results")
                telemetry = {
                    "pattern": pattern_code,
                    "terminal_reason": "no_candidates",
                    "total_evaluated": 0,
                    "scanned": 0,
                    "entered": 0,
                    "avoided": 0,
                    "errors": 0,
                    "signals_created": 0,
                    "signals_deduplicated": 0,
                    "signals_linked": 0,
                }
                try:
                    await finalize_scan_run(
                        scan_run_id=scan_id, status="completed",
                        scanned_count=0, enter_count=0, rejected_count=0,
                        telemetry=telemetry,
                    )
                except Exception as finalize_exc:
                    logger.error(f"Failed to finalize zero-candidate scan run: {finalize_exc}")
                summary = {
                    "success": True,
                    "scan_id": scan_id,
                    "terminal_reason": "no_candidates",
                    "telemetry": telemetry,
                    "scanned_count": 0,
                    "enter_count": 0,
                    "rejected_count": 0,
                    "error_count": 0,
                }
                if scan_id:
                    await event_bus.publish(scan_id, {"type": "finished", **summary})
                return summary
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
                    ignore_seen=ignore_seen,
                    config=config,
                    scan_run_id=scan_id,
                    source_path=source_path,
                    provider_name=provider_name,
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
        signals_created = 0
        signals_deduplicated = 0
        enter_signals = []
        rejection_reasons: Counter = Counter()
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task failed for {selected_symbols[i]}: {result}")
                error_count += 1
                rejection_reasons["task_exception"] += 1
                continue
            
            if result["processed"]:
                scanned_count += 1
                
                if result["result"]:
                    # Phase 7B immutable-identity accounting: a repeated exact
                    # signal is deduplicated, never counted as newly created.
                    if "signal_created_new" in result["result"]:
                        if result["result"]["signal_created_new"]:
                            signals_created += 1
                        else:
                            signals_deduplicated += 1
                    if result["result"]["verdict"] == "ENTER":
                        enter_count += 1
                        enter_signals.append({
                            "symbol": result["symbol"],
                            "score": result["result"]["score"],
                            "reason": result["result"]["reason"]
                        })
                    else:
                        rejected_count += 1
                        # Prefer the structured rejection_reason over free text.
                        details = (result["result"].get("details") or {})
                        reason = details.get("rejection_reason") or "avoided"
                        rejection_reasons[reason] += 1
            else:
                error_count += 1
                # Log detailed error information
                symbol = result.get("symbol", f"symbol_{i}")
                error_msg = result.get("error", "Unknown error")
                # Normalize to a compact reason key for telemetry.
                reason_key = error_msg.split(":")[0].strip().lower().replace(" ", "_") or "error"
                rejection_reasons[reason_key] += 1
                if "FMP API error" in error_msg:
                    logger.error(f"FMP API error for {symbol}: {error_msg}")
                elif "DataFrame error" in error_msg:
                    logger.error(f"DataFrame error for {symbol}: {error_msg}")
                elif "Pattern detection error" in error_msg:
                    logger.error(f"Pattern detection error for {symbol}: {error_msg}")
                else:
                    logger.error(f"Other error for {symbol}: {error_msg}")
        
        run_duration = (datetime.utcnow() - run_start).total_seconds()

        # Minimal reject-telemetry foundation (persisted in pattern_runs.notes
        # as JSON). This is the lightweight precursor to the Phase 3 funnel; it
        # does not add a new schema.
        telemetry = {
            "pattern": pattern_code,
            "total_evaluated": len(selected_symbols),
            "scanned": scanned_count,
            "entered": enter_count,
            "avoided": rejected_count,
            "errors": error_count,
            "signals_created": signals_created,
            "signals_deduplicated": signals_deduplicated,
            "signals_linked": signals_created + signals_deduplicated,
            "top_rejection_reasons": dict(rejection_reasons.most_common(10)),
            "config_used": {
                k: config.get(k)
                for k in (
                    "touch_tolerance_pct", "min_bounces", "min_avg_rebound_pct",
                    "min_volume_sma_ratio", "min_price", "score_threshold",
                )
            },
            "score_version": "sma150.v2",
            "runtime_seconds": round(run_duration, 2),
        }

        # Finalize the canonical scan-run row (counts + telemetry + status).
        try:
            await finalize_scan_run(
                scan_run_id=scan_id,
                status="completed",
                scanned_count=scanned_count,
                enter_count=enter_count,
                rejected_count=rejected_count,
                telemetry=telemetry,
            )
        except Exception as e:
            logger.error(f"Failed to finalize scan run: {e}")
        
        # Summary
        summary = {
            "success": True,
            "scan_id": scan_id,
            "run_duration_seconds": run_duration,
            "telemetry": telemetry,
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

        # HANDLED failure lifecycle: the canonical run row must never stay
        # 'running' after a handled exception (safe error identity, no
        # invented telemetry).
        try:
            await finalize_scan_run(
                scan_run_id=scan_id,
                status="failed",
                scanned_count=0,
                enter_count=0,
                rejected_count=0,
                telemetry=None,
                error_code="batch_scan_exception",
                error_message=str(e),
            )
        except Exception as finalize_exc:
            logger.error(f"Failed to mark scan run as failed: {finalize_exc}")

        return {
            "success": False,
            "error": str(e),
            "scan_id": scan_id,
            "run_duration_seconds": run_duration,
            "scanned_count": 0,
            "enter_count": 0,
            "rejected_count": 0,
            "started_at": run_start.isoformat(),
            "completed_at": datetime.utcnow().isoformat()
        }


async def run_maintenance_tasks():
    """Run periodic maintenance tasks.

    Fixes B7: previously used `async with get_db()`, but get_db() is an async
    generator (a FastAPI dependency), not an async context manager, so the
    scheduled maintenance job always raised. We acquire a pooled connection
    directly and release it back to the pool.
    """
    logger.info("Running maintenance tasks")

    from app.workers.maintenance import cleanup_daily_seen
    from app.workers.persistence import get_db_connection, release_db_connection

    conn = None
    try:
        conn = await get_db_connection()
        await cleanup_daily_seen(conn)
        logger.info("Maintenance tasks completed")
    except Exception as e:
        logger.error(f"Maintenance tasks failed: {e}")
    finally:
        await release_db_connection(conn)


if __name__ == "__main__":
    # For testing - run a single batch
    async def test_run():
        from app.config import settings
        
        from app.providers import get_market_data_provider

        provider = get_market_data_provider()
        result = await run_scan_batch(provider, batch_size=10)
        print(f"Test run result: {result}")
    
    asyncio.run(test_run())
