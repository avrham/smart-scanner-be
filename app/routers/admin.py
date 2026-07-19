"""
Admin API endpoints for Smart Scanner
Write endpoints protected by worker token
"""

from fastapi import APIRouter, Depends, BackgroundTasks, Body
from typing import Any, List, Optional
import uuid
from fastapi import WebSocket, WebSocketDisconnect
import logging
import asyncpg

from app.deps import get_db, get_worker_token
from app.workers.scan_runner import run_scan_batch
from app.workers.fmp_client import FMPClient
from app.workers.tickers import refresh_tickers_cache
from app.workers.maintenance import cleanup_daily_seen, clear_daily_seen
from app.workers.outcomes.service import calculate_outcomes_for_signals
from app.workers.scanner.funnel import run_funnel_scan
from app.config import settings
from app.utils.events import event_bus


router = APIRouter()


@router.post("/scan/start")
async def start_scan(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    pattern_code: str = Body("sma150_bounce"),
    batch_size: Optional[int] = Body(None),
    symbols: Any = Body(None),
    ignore_seen: bool = Body(False),
    return_details: bool = Body(False),
    scanner_mode: str = Body("legacy"),
    limit: Optional[int] = Body(None),
    dry_run: bool = Body(False),
):
    """Trigger a manual scan cycle for a given pattern (default sma150_bounce).

    scanner_mode:
      * "legacy" (default) - preserves the existing random-batch behavior and
        endpoint contract.
      * "funnel"  - Phase 3 hierarchical funnel. With dry_run=True it runs the
        cheap stages only (universe + liquidity), performs NO FMP calls and NO
        signal writes, and returns telemetry synchronously - the safe way to
        validate. Without dry_run it fetches history for liquidity survivors
        (bounded by `limit`) and evaluates the strategy.
    """

    chosen_batch_size = batch_size or settings.SCAN_BATCH_SIZE
    logger = logging.getLogger(__name__)

    # Phase 3: hierarchical funnel scanner (opt-in).
    if scanner_mode == "funnel":
        funnel_scan_id = str(uuid.uuid4())
        funnel_limit = limit if limit is not None else batch_size

        # dry_run is FMP-free and fast -> run synchronously and return telemetry.
        if dry_run:
            summary = await run_funnel_scan(
                fmp=None,
                pattern_code=pattern_code,
                limit=funnel_limit,
                ignore_seen=ignore_seen,
                dry_run=True,
                scan_id=funnel_scan_id,
            )
            return {"message": "Funnel dry-run completed", "scan_id": funnel_scan_id, **summary}

        async def run_funnel():
            run_logger = logging.getLogger(__name__)
            fmp = FMPClient(
                api_key=settings.FMP_API_KEY,
                max_concurrent=settings.FMP_MAX_CONCURRENT,
            )
            try:
                await run_funnel_scan(
                    fmp=fmp,
                    pattern_code=pattern_code,
                    limit=funnel_limit,
                    ignore_seen=ignore_seen,
                    dry_run=False,
                    scan_id=funnel_scan_id,
                )
            except Exception as e:
                run_logger.error(f"[ADMIN] funnel scan failed: {e}")
                await event_bus.publish(funnel_scan_id, {"type": "error", "error": str(e)})

        background_tasks.add_task(run_funnel)
        return {
            "message": "Funnel scan enqueued",
            "scanner_mode": "funnel",
            "pattern_code": pattern_code,
            "limit": funnel_limit,
            "scan_id": funnel_scan_id,
        }
    # Normalize symbols: accept list[str] or comma-separated string
    normalized_symbols: Optional[List[str]] = None
    try:
        if isinstance(symbols, list):
            normalized_symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]
        elif isinstance(symbols, str):
            normalized_symbols = [s.strip().upper() for s in symbols.split(',') if s.strip()]
    except Exception:
        normalized_symbols = None

    logger.info(
        f"[ADMIN] enqueue scan: pattern={pattern_code}, batch_size={chosen_batch_size}, symbols={len(normalized_symbols) if normalized_symbols else 0}, ignore_seen={ignore_seen}"
    )

    scan_id = str(uuid.uuid4())

    async def run_scan():
        run_logger = logging.getLogger(__name__)
        fmp = FMPClient(
            api_key=settings.FMP_API_KEY,
            max_concurrent=settings.FMP_MAX_CONCURRENT
        )
        try:
            run_logger.info(
                f"[ADMIN] scan started: pattern={pattern_code}, batch_size={chosen_batch_size}"
            )
            await event_bus.publish(scan_id, {"type": "started", "pattern": pattern_code, "batch_size": chosen_batch_size, "symbols": normalized_symbols or []})
            summary = await run_scan_batch(
                fmp,
                batch_size=chosen_batch_size,
                pattern_code=pattern_code,
                symbols=normalized_symbols,
                ignore_seen=ignore_seen,
                scan_id=scan_id
            )
            run_logger.info(
                f"[ADMIN] scan finished: scanned={summary.get('scanned_count')}, enter={summary.get('enter_count')}, rejected={summary.get('rejected_count')}"
            )
            await event_bus.publish(scan_id, {"type": "finished", **summary})
        except Exception as e:
            run_logger.error(f"[ADMIN] scan failed: {e}")
            await event_bus.publish(scan_id, {"type": "error", "error": str(e)})

    # If specific symbols are provided, run synchronously and return summary
    if normalized_symbols and len(normalized_symbols) > 0:
        await event_bus.publish(scan_id, {"type": "started", "pattern": pattern_code, "batch_size": chosen_batch_size, "symbols": normalized_symbols})
        summary = await run_scan_batch(
            FMPClient(api_key=settings.FMP_API_KEY, max_concurrent=settings.FMP_MAX_CONCURRENT),
            batch_size=chosen_batch_size,
            pattern_code=pattern_code,
            symbols=normalized_symbols,
            ignore_seen=ignore_seen,
            scan_id=scan_id
        )
        await event_bus.publish(scan_id, {"type": "finished", **summary})
        return {
            "message": "Scan completed",
            "batch_size": chosen_batch_size,
            "pattern_code": pattern_code,
            "scan_id": scan_id,
            **summary
        }

    # Default: enqueue background task
    background_tasks.add_task(run_scan)
    return {
        "message": "Scan enqueued",
        "batch_size": chosen_batch_size,
        "pattern_code": pattern_code,
        "scan_id": scan_id
    }


@router.websocket("/scan/ws/{scan_id}")
async def scan_ws(websocket: WebSocket, scan_id: str):
    await websocket.accept()
    queue = await event_bus.subscribe(scan_id)
    try:
        # Send initial ack so clients can show a live connection
        await websocket.send_json({"type": "ack", "scan_id": scan_id})
        # If we already have a latest event for this scan, send it immediately
        latest = await event_bus.latest(scan_id)
        if latest:
            await websocket.send_json(latest)
        while True:
            event = await queue.get()
            await websocket.send_json(event)
    except WebSocketDisconnect:
        await event_bus.unsubscribe(scan_id, queue)


@router.post("/tickers/refresh")
async def refresh_tickers(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token)
):
    """Refresh the tickers cache from FMP"""
    
    async def refresh_task():
        fmp = FMPClient(
            api_key=settings.FMP_API_KEY,
            max_concurrent=settings.FMP_MAX_CONCURRENT
        )
        await refresh_tickers_cache(fmp)
    
    background_tasks.add_task(refresh_task)
    
    return {"message": "Ticker refresh started"}


@router.post("/maintenance/reset-daily-seen")
async def reset_daily_seen(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db)
):
    """Clean up old daily_seen entries"""
    
    await cleanup_daily_seen(db)
    
    return {"message": "Daily seen cache cleaned"}


@router.post("/maintenance/clear-daily-seen")
async def clear_daily_seen_endpoint(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db)
):
    """Clear daily seen records for today"""
    
    count = await clear_daily_seen(db)
    
    return {"message": f"Cleared {count} daily seen records for today"}


@router.post("/outcomes/calculate")
async def calculate_outcomes(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token),
    limit: int = Body(50),
    pattern_code: Optional[str] = Body(None),
    include_recalc: bool = Body(False),
    run_in_background: bool = Body(True),
):
    """Compute outcomes for signals that need them (Phase 2).

    Bounded by `limit`. Protected by the worker token. This fetches historical
    OHLCV from FMP for the affected symbols plus SPY/QQQ, so it should be run
    deliberately (it is NOT scheduled and not enabled automatically).
    """
    logger = logging.getLogger(__name__)

    async def _run() -> dict:
        fmp = FMPClient(
            api_key=settings.FMP_API_KEY,
            max_concurrent=settings.FMP_MAX_CONCURRENT,
        )
        logger.info(
            "[ADMIN] outcome calc start: limit=%s, pattern=%s, recalc=%s",
            limit, pattern_code, include_recalc,
        )
        summary = await calculate_outcomes_for_signals(
            fmp,
            limit=limit,
            pattern_code=pattern_code,
            include_recalc=include_recalc,
        )
        logger.info("[ADMIN] outcome calc finished: %s", summary)
        return summary

    if run_in_background:
        background_tasks.add_task(_run)
        return {"message": "Outcome calculation enqueued", "limit": limit}

    summary = await _run()
    return {"message": "Outcome calculation completed", **summary}


@router.get("/status")
async def get_status(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db)
):
    """Get system status and statistics"""
    
    # Get latest pattern run stats
    stats_query = """
        SELECT pattern_code, 
               SUM(scanned_count) as total_scanned,
               SUM(enter_count) as total_enter,
               SUM(rejected_count) as total_rejected,
               MAX(run_started_at) as last_run
        FROM pattern_runs 
        WHERE run_started_at >= NOW() - INTERVAL '24 hours'
        GROUP BY pattern_code
    """
    
    stats = await db.fetch(stats_query)
    
    # Get daily seen count
    seen_query = """
        SELECT COUNT(*) as seen_today
        FROM daily_seen 
        WHERE seen_date = CURRENT_DATE
    """
    
    seen_result = await db.fetchrow(seen_query)
    
    return {
        "environment": settings.ENVIRONMENT,
        "debug_save_avoid": settings.DEBUG_SAVE_AVOID,
        "pattern_stats": [
            {
                "pattern_code": stat["pattern_code"],
                "total_scanned": stat["total_scanned"],
                "total_enter": stat["total_enter"],
                "total_rejected": stat["total_rejected"],
                "last_run": stat["last_run"]
            }
            for stat in stats
        ],
        "seen_today": seen_result["seen_today"] if seen_result else 0
    }
