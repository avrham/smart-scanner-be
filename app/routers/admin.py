"""
Admin API endpoints for Smart Scanner
Write endpoints protected by worker token
"""

from fastapi import APIRouter, Depends, BackgroundTasks, Body, HTTPException
from typing import Any, List, Optional
import re
import uuid
from fastapi import WebSocket, WebSocketDisconnect
import logging
import asyncpg

from app.deps import get_db, get_worker_token
from app.models.responses import (
    StrategyDiscoveryResponse,
    StrategyDryRunResponse,
)
from app.workers.scan_runner import run_scan_batch
from app.workers.maintenance import cleanup_daily_seen, clear_daily_seen
from app.workers.outcomes.service import calculate_outcomes_for_signals
from app.workers.scanner.funnel import run_funnel_scan
from app.workers import market_jobs, market_store
from app.workers.coverage import UnsupportedProviderError, get_market_data_coverage
from app.workers.strategies.discovery import (
    discover_all_strategies,
    discover_strategy,
)
from app.providers import ProviderConfigError, get_market_data_provider
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
    persist_watch: Optional[bool] = Body(None),
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

    persist_watch: controls WATCH persistence in BOTH funnel and legacy/manual
    modes, defaulting to each mode's existing safe behavior:
      * funnel  - Phase 5.2 WATCH persistence defaults to true; pass false to
        override.
      * legacy/manual - defaults to false (WATCH is evaluated and counted but
        NOT persisted); requires an explicit persist_watch=true to persist
        WATCH results through save_signal with full Phase 7B provenance.
    """

    chosen_batch_size = batch_size or settings.SCAN_BATCH_SIZE
    logger = logging.getLogger(__name__)

    # Phase 3: hierarchical funnel scanner (opt-in).
    if scanner_mode == "funnel":
        funnel_scan_id = str(uuid.uuid4())
        funnel_limit = limit if limit is not None else batch_size
        # None values are ignored by the funnel's config merge (keeps defaults).
        funnel_scanner_config = {"persist_watch_candidates": persist_watch}

        # dry_run is FMP-free and fast -> run synchronously and return telemetry.
        if dry_run:
            summary = await run_funnel_scan(
                fmp=None,
                pattern_code=pattern_code,
                limit=funnel_limit,
                scanner_config=funnel_scanner_config,
                ignore_seen=ignore_seen,
                dry_run=True,
                scan_id=funnel_scan_id,
            )
            return {"message": "Funnel dry-run completed", "scan_id": funnel_scan_id, **summary}

        # Funnel scans use the configured MarketDataProvider (Massive default,
        # FMP fallback). Fail fast with a clear JSON error if misconfigured.
        try:
            provider = get_market_data_provider()
        except ProviderConfigError as exc:
            raise HTTPException(status_code=503, detail=str(exc))

        async def run_funnel():
            run_logger = logging.getLogger(__name__)
            try:
                await run_funnel_scan(
                    fmp=provider,
                    pattern_code=pattern_code,
                    limit=funnel_limit,
                    scanner_config=funnel_scanner_config,
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

    # Legacy/manual WATCH persistence is opt-in: only an explicit
    # persist_watch=true enables it (None/False preserve existing behavior).
    legacy_persist_watch = persist_watch is True

    # Legacy scans also go through the configured MarketDataProvider.
    try:
        legacy_provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    async def run_scan():
        run_logger = logging.getLogger(__name__)
        fmp = legacy_provider
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
                scan_id=scan_id,
                persist_watch_candidates=legacy_persist_watch,
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
            legacy_provider,
            batch_size=chosen_batch_size,
            pattern_code=pattern_code,
            symbols=normalized_symbols,
            ignore_seen=ignore_seen,
            scan_id=scan_id,
            persist_watch_candidates=legacy_persist_watch,
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
    """Refresh the tickers cache via the configured provider.

    massive -> full reference universe sync; fmp -> legacy screener refresh.
    (Kept for backward compatibility; /universe/sync is the same operation.)
    """
    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    async def refresh_task():
        run_logger = logging.getLogger(__name__)
        try:
            summary = await provider.sync_universe()
            run_logger.info(f"[ADMIN] ticker refresh finished: {summary}")
        except Exception as e:
            run_logger.error(f"[ADMIN] ticker refresh failed: {e}")

    background_tasks.add_task(refresh_task)

    return {"message": "Ticker refresh started", "provider": provider.name}


@router.post("/universe/sync")
async def universe_sync(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token),
):
    """Sync the ticker universe from the configured provider (paginated).

    On Massive Basic this is ~12-13 reference requests paced at the configured
    rate limit (a few minutes). Runs in the background.
    """
    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    async def sync_task():
        run_logger = logging.getLogger(__name__)
        try:
            summary = await provider.sync_universe()
            run_logger.info(f"[ADMIN] universe sync finished: {summary}")
        except Exception as e:
            run_logger.error(f"[ADMIN] universe sync failed: {e}")

    background_tasks.add_task(sync_task)
    return {"message": "Universe sync started", "provider": provider.name}


@router.post("/market/daily-sync")
async def market_daily_sync(
    _: str = Depends(get_worker_token),
    trading_date: Optional[str] = Body(None, embed=True),
):
    """Ingest the whole-market grouped daily snapshot for one date (1 request).

    trading_date defaults to the most recent weekday (YYYY-MM-DD). Runs
    synchronously (single request) and returns the ingest summary.
    """
    from datetime import date as _date, timedelta as _timedelta

    if not trading_date:
        d = _date.today() - _timedelta(days=1)
        while d.weekday() >= 5:  # skip Sat/Sun
            d -= _timedelta(days=1)
        trading_date = str(d)

    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    summary = await provider.get_daily_market_summary(trading_date)
    return {"message": "Daily market sync completed", **summary}


@router.post("/universe/enrich")
async def universe_enrich(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token),
    trading_date: Optional[str] = Body(None, embed=True),
    max_detail_calls: int = Body(25, embed=True),
):
    """Survivor-only market-cap enrichment as a DURABLE job (Phase 7A).

    Creates a queued `market_data_jobs` row and runs asynchronously
    (queued -> running -> completed/failed) with bounded progress. Duplicate
    active jobs for the same provider + trading date are rejected by a
    database unique index — not an in-memory lock. Enrichment behavior is
    unchanged: local pre-screen, deterministic dollar-volume prioritization,
    fresh-profile skipping, rate limiter, max_detail_calls bound.

    trading_date is ALWAYS resolved before job insertion (NULL would bypass
    the duplicate-protection index): when omitted, the latest locally stored
    daily-bar date is used; with no local bars the request is rejected.
    """
    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    if provider.name != "massive":
        raise HTTPException(
            status_code=400, detail="Enrichment is only supported for the massive provider"
        )

    if trading_date:
        parsed_date = parse_trading_date(trading_date)
    else:
        parsed_date = await market_store.get_latest_daily_bar_date()
        if parsed_date is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "No locally stored daily bars — run POST /api/admin/market/daily-sync "
                    "first, or pass an explicit trading_date"
                ),
            )
        trading_date = str(parsed_date)

    # A crashed process must never block new work: recover stale jobs first.
    await market_jobs.recover_stale_jobs(settings.MARKET_DATA_JOB_STALE_MINUTES)

    try:
        job_id = await market_jobs.create_job(
            job_type=market_jobs.JOB_TYPE_ENRICHMENT,
            provider=provider.name,
            trading_date=parsed_date,
            requested_limit=max_detail_calls,
        )
    except market_jobs.DuplicateActiveJobError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    background_tasks.add_task(
        market_jobs.run_enrichment_job, job_id, provider, parsed_date, max_detail_calls
    )
    return {
        "message": "Enrichment job queued",
        "job_id": job_id,
        "status": "queued",
        "trading_date": trading_date,
        "max_detail_calls": max_detail_calls,
    }


@router.get("/market-data/jobs/{job_id}")
async def get_market_data_job(
    job_id: str,
    _: str = Depends(get_worker_token),
):
    """Status of a single durable market-data job."""
    job = await market_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


_TRADING_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_trading_date(value: str):
    """Strict YYYY-MM-DD validation (rejects other ISO variants)."""
    from datetime import date as _date

    if not _TRADING_DATE_RE.match(value or ""):
        raise HTTPException(status_code=400, detail="trading_date must be YYYY-MM-DD")
    try:
        return _date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="trading_date must be a valid YYYY-MM-DD date")


@router.get("/market-data/jobs")
async def list_market_data_jobs(
    _: str = Depends(get_worker_token),
    job_type: Optional[str] = None,
    provider: Optional[str] = None,
    status: Optional[str] = None,
    trading_date: Optional[str] = None,
    limit: int = 50,
):
    """Bounded, filtered listing of durable market-data jobs (newest first).

    Filters (job_type, provider, status, trading_date) compose with AND
    semantics; provider is an exact normalized match.
    """
    if status is not None and status not in market_jobs.JOB_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Expected one of: {', '.join(market_jobs.JOB_STATUSES)}",
        )
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")
    parsed_date = parse_trading_date(trading_date) if trading_date is not None else None
    jobs = await market_jobs.list_jobs(
        job_type=job_type,
        status=status,
        provider=provider,
        trading_date=parsed_date,
        limit=limit,
    )
    return {"jobs": jobs, "count": len(jobs)}


@router.get("/market-data/coverage")
async def market_data_coverage(
    _: str = Depends(get_worker_token),
    trading_date: Optional[str] = None,
    provider: Optional[str] = None,
):
    """Local-only market-data coverage snapshot (Phase 7A).

    Uses ONLY locally stored data — never constructs a provider client and
    never calls the network. `provider` defaults to the configured
    MARKET_DATA_PROVIDER and is echoed in the response after validation.
    Defaults to the latest stored trading date.
    """
    parsed = parse_trading_date(trading_date) if trading_date is not None else None
    try:
        return await get_market_data_coverage(parsed, provider=provider)
    except UnsupportedProviderError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


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
    OHLCV from the configured MarketDataProvider for the affected symbols plus
    SPY/QQQ, so it should be run deliberately (it is NOT scheduled and not
    enabled automatically).
    """
    logger = logging.getLogger(__name__)

    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    async def _run() -> dict:
        logger.info(
            "[ADMIN] outcome calc start: limit=%s, pattern=%s, recalc=%s",
            limit, pattern_code, include_recalc,
        )
        summary = await calculate_outcomes_for_signals(
            provider,
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


@router.post("/shadow/sma150/compare")
async def shadow_sma150_compare(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token),
    symbols: Any = Body(...),
    run_in_background: bool = Body(False),
):
    """Phase 8.1B1: frozen paired shadow evaluation of sma150.v2 vs sma150.v3.

    Evaluates BOTH strategies on the exact same canonical completed OHLCV
    frame (one fetch per symbol) and persists one immutable pair per exact
    comparison input, preserving ENTER, WATCH and AVOID. Shadow evaluations
    are never normal signals, never receive outcomes, and never change
    strategy enablement or the scheduler.

    Request: explicit symbols only (max 25), no universe scans. Synchronous
    by default (smoke-test friendly); run_in_background=true uses the
    in-process BackgroundTasks pattern (no resumable-execution claim).
    """
    from app.workers.shadow.runner import (
        ShadowRequestError,
        normalize_shadow_symbols,
        run_shadow_comparison,
    )

    logger = logging.getLogger(__name__)

    try:
        normalized = normalize_shadow_symbols(symbols)
    except ShadowRequestError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    run_id = str(uuid.uuid4())

    if run_in_background:
        async def _run() -> None:
            try:
                summary = await run_shadow_comparison(
                    provider, normalized, run_id=run_id
                )
                logger.info(
                    "[ADMIN] shadow run %s finished: status=%s",
                    run_id, summary.get("status"),
                )
            except Exception as exc:
                logger.error("[ADMIN] shadow run %s failed: %s", run_id, exc)

        background_tasks.add_task(_run)
        return {
            "message": "Shadow comparison enqueued",
            "run_id": run_id,
            "requested_count": len(normalized),
        }

    summary = await run_shadow_comparison(provider, normalized, run_id=run_id)
    return {"message": "Shadow comparison completed", **summary}


@router.post("/shadow/outcomes/calculate")
async def shadow_outcomes_calculate(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token),
    pair_ids: Optional[List[str]] = Body(None),
    symbols: Optional[List[str]] = Body(None),
    run_id: Optional[str] = Body(None),
    pending: bool = Body(False),
    limit: Optional[int] = Body(None),
    include_recalc: bool = Body(False),
    run_in_background: bool = Body(False),
):
    """Phase 8.1B2: bounded market-path outcome calculation for frozen B1
    shadow pairs.

    Exactly ONE outcome per pair (never per arm). Requires at least one
    selector (pair_ids / symbols / run_id) or pending=true — there is no
    unbounded all-history mode. Selectors AND-compose; limit defaults to 50
    with a hard cap of 200. Forward data must come from the frozen pair's
    provider (provider_mismatch otherwise) via bounded date-range retrieval
    (provider_range_unsupported otherwise). Synchronous by default
    (smoke-test friendly); run_in_background=true uses the in-process
    BackgroundTasks pattern (no resumable-execution claim). Never scheduled;
    never touches signals/signal_outcomes; never enables v3.
    """
    from app.workers.shadow.outcomes.service import (
        ShadowOutcomeRequestError,
        normalize_outcome_request,
        run_shadow_outcome_calculation,
    )

    logger = logging.getLogger(__name__)

    try:
        request = normalize_outcome_request(
            pair_ids=pair_ids,
            symbols=symbols,
            run_id=run_id,
            pending=pending,
            limit=limit,
            include_recalc=include_recalc,
        )
    except ShadowOutcomeRequestError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    outcome_run_id = str(uuid.uuid4())

    if run_in_background:
        async def _run() -> None:
            try:
                summary = await run_shadow_outcome_calculation(
                    provider,
                    pair_ids=request["pair_ids"] or None,
                    symbols=request["symbols"] or None,
                    run_id=request["run_id"],
                    pending=request["pending"],
                    limit=request["limit"],
                    include_recalc=request["include_recalc"],
                    outcome_run_id=outcome_run_id,
                )
                logger.info(
                    "[ADMIN] shadow outcome run %s finished: status=%s",
                    outcome_run_id, summary.get("status"),
                )
            except Exception as exc:
                logger.error(
                    "[ADMIN] shadow outcome run %s failed: %s",
                    outcome_run_id, exc,
                )

        background_tasks.add_task(_run)
        return {
            "message": "Shadow outcome calculation enqueued",
            "outcome_run_id": outcome_run_id,
            "limit": request["limit"],
        }

    summary = await run_shadow_outcome_calculation(
        provider,
        pair_ids=request["pair_ids"] or None,
        symbols=request["symbols"] or None,
        run_id=request["run_id"],
        pending=request["pending"],
        limit=request["limit"],
        include_recalc=request["include_recalc"],
        outcome_run_id=outcome_run_id,
    )
    return {"message": "Shadow outcome calculation completed", **summary}


def _discovery_to_response(item) -> StrategyDiscoveryResponse:
    return StrategyDiscoveryResponse(
        pattern_code=item.pattern_code,
        registered=item.registered,
        enabled=item.enabled,
        db_configured=item.db_configured,
        config_status=item.config_status,
        name=item.name,
        description=item.description,
        strategy_version=item.strategy_version,
        decision_policy_version=item.decision_policy_version,
        allow_enter=item.allow_enter,
        enable_4h_trigger=item.enable_4h_trigger,
        min_price=item.min_price,
        effective_config=item.effective_config,
    )


@router.get("/strategies", response_model=List[StrategyDiscoveryResponse])
async def list_admin_strategies(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
):
    """Read-only catalog of every canonically registered strategy.

    Includes disabled and unconfigured strategies. Does not enable strategies,
    mutate configuration, or invoke providers. Distinct from public
    GET /api/patterns which only returns is_enabled=true rows.
    """
    items = await discover_all_strategies(db)
    return [_discovery_to_response(item) for item in items]


@router.get(
    "/strategies/{pattern_code}",
    response_model=StrategyDiscoveryResponse,
)
async def get_admin_strategy(
    pattern_code: str,
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
):
    """Read-only discovery for one registered strategy code."""
    item = await discover_strategy(db, pattern_code)
    if item is None:
        raise HTTPException(
            status_code=404,
            detail=f"No strategy registered for pattern_code '{pattern_code}'",
        )
    return _discovery_to_response(item)


@router.post(
    "/strategies/{pattern_code}/dry-run",
    response_model=StrategyDryRunResponse,
)
async def strategy_dry_run(
    pattern_code: str,
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    symbol: str = Body(..., embed=True),
    evaluation_time_utc: Optional[str] = Body(None, embed=True),
):
    """Phase 9D1: explicit persistence-free dry-run of ONE registered strategy.

    Resolves the strategy through the canonical registry and its configuration
    through the canonical merge path (patterns/pattern_configs over strategy
    defaults), fetches daily history from the configured MarketDataProvider,
    evaluates deterministically on the canonical completed frame and returns
    a typed result with persisted=false.

    A registered but database-disabled strategy (e.g. wyckoff_mtf_v2) may be
    dry-run explicitly; this never enables it, never mutates configuration or
    rollout flags, and never creates a signal, watch, alert, notification,
    decision card or ranking input. There is no fallback strategy.
    """
    from app.workers.strategies.dry_run import (
        DryRunRequestError,
        DryRunUnknownStrategyError,
        run_strategy_dry_run,
    )

    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    try:
        result = await run_strategy_dry_run(
            db,
            provider,
            pattern_code=pattern_code,
            symbol=symbol,
            evaluation_time_utc=evaluation_time_utc,
        )
    except DryRunUnknownStrategyError:
        raise HTTPException(
            status_code=404,
            detail=f"No strategy registered for pattern_code '{pattern_code}'",
        )
    except DryRunRequestError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return StrategyDryRunResponse(**result)


@router.post("/strategies/{pattern_code}/shadow-run")
async def strategy_shadow_run(
    pattern_code: str,
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token),
    symbols: Any = Body(..., embed=True),
    run_in_background: bool = Body(False, embed=True),
):
    """Phase 9D2/9D6: explicit shadow evaluation of ONE candidate strategy
    through the canonical shadow runner.

    Resolves the declared shadow experiment whose CANDIDATE arm is
    `pattern_code` (wyckoff_mtf_v2 -> wyckoff_v2_vs_baseline against the
    sma150_bounce baseline; sma150_bounce_v3 -> the historical sma150
    experiment) and runs the bounded comparison over the explicit symbol
    list (max 25). Shadow rows are experiment evidence only: never signals,
    watches, alerts, notifications, decision cards, ranking inputs or
    scheduler results, and the run never enables the candidate or changes
    rollout flags.
    """
    from app.workers.shadow.experiments import (
        UnknownShadowExperimentError,
        experiment_for_candidate,
    )
    from app.workers.shadow.runner import (
        ShadowRequestError,
        normalize_shadow_symbols,
        run_shadow_comparison,
    )
    from app.workers.strategies.registry import (
        UnknownStrategyError,
        get_strategy,
    )

    logger = logging.getLogger(__name__)

    try:
        get_strategy(pattern_code)
    except UnknownStrategyError:
        raise HTTPException(
            status_code=404,
            detail=f"No strategy registered for pattern_code '{pattern_code}'",
        )
    try:
        experiment = experiment_for_candidate(pattern_code)
    except UnknownShadowExperimentError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    try:
        normalized = normalize_shadow_symbols(symbols)
    except ShadowRequestError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    run_id = str(uuid.uuid4())

    if run_in_background:
        async def _run() -> None:
            try:
                summary = await run_shadow_comparison(
                    provider, normalized, run_id=run_id, experiment=experiment
                )
                logger.info(
                    "[ADMIN] shadow run %s (%s) finished: status=%s",
                    run_id, experiment.experiment_code, summary.get("status"),
                )
            except Exception as exc:
                logger.error(
                    "[ADMIN] shadow run %s (%s) failed: %s",
                    run_id, experiment.experiment_code, exc,
                )

        background_tasks.add_task(_run)
        return {
            "message": "Shadow run enqueued",
            "run_id": run_id,
            "experiment_code": experiment.experiment_code,
            "experiment_version": experiment.experiment_version,
            "candidate_pattern_code": experiment.candidate_pattern_code,
            "control_pattern_code": experiment.control_pattern_code,
            "requested_count": len(normalized),
        }

    summary = await run_shadow_comparison(
        provider, normalized, run_id=run_id, experiment=experiment
    )
    return {
        "message": "Shadow run completed",
        "experiment_code": experiment.experiment_code,
        "experiment_version": experiment.experiment_version,
        "candidate_pattern_code": experiment.candidate_pattern_code,
        "control_pattern_code": experiment.control_pattern_code,
        **summary,
    }


_SHADOW_RUN_STATUSES = ("running", "completed", "failed")


def _experiment_code_for_pattern(pattern_code: Optional[str]) -> Optional[str]:
    """Map an optional candidate pattern_code filter to its experiment code."""
    if pattern_code is None:
        return None
    from app.workers.shadow.experiments import (
        UnknownShadowExperimentError,
        experiment_for_candidate,
    )

    try:
        return experiment_for_candidate(pattern_code).experiment_code
    except UnknownShadowExperimentError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.get("/shadow-runs")
async def list_shadow_runs(
    _: str = Depends(get_worker_token),
    pattern_code: Optional[str] = None,
    experiment_code: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
):
    """Phase 9D6: bounded newest-first shadow-run listing (read-only).

    `pattern_code` filters to the experiment whose candidate arm is that
    strategy; `experiment_code` filters directly. No provider client is
    constructed and nothing is written.
    """
    from app.workers.shadow.persistence import fetch_shadow_runs

    if status is not None and status not in _SHADOW_RUN_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"status must be one of {list(_SHADOW_RUN_STATUSES)}",
        )
    if limit < 1 or limit > 200:
        raise HTTPException(
            status_code=422, detail="limit must be between 1 and 200"
        )
    resolved_experiment = (
        experiment_code
        if experiment_code is not None
        else _experiment_code_for_pattern(pattern_code)
    )
    runs = await fetch_shadow_runs(
        experiment_code=resolved_experiment, status=status, limit=limit
    )
    return {"count": len(runs), "runs": runs}


@router.get("/shadow-runs/{run_id}")
async def get_admin_shadow_run(
    run_id: str,
    _: str = Depends(get_worker_token),
    pair_limit: int = 100,
):
    """Phase 9D6: one shadow run's bounded detail — the run row with its
    telemetry plus bounded pair summaries (never full frame/details
    snapshots)."""
    from app.workers.shadow.persistence import (
        fetch_shadow_pairs,
        fetch_shadow_run,
    )

    if pair_limit < 1 or pair_limit > 500:
        raise HTTPException(
            status_code=422, detail="pair_limit must be between 1 and 500"
        )
    try:
        validated = str(uuid.UUID(run_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="run not found")

    run = await fetch_shadow_run(validated)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    pairs = await fetch_shadow_pairs(run_id=validated, limit=pair_limit)
    return {**run, "pair_count": len(pairs), "pairs": pairs}


@router.get("/shadow-metrics")
async def strategy_shadow_metrics(
    _: str = Depends(get_worker_token),
    pattern_code: str = "wyckoff_mtf_v2",
    symbol: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    experiment_code: Optional[str] = None,
    limit: int = 500,
):
    """Phase 9D5/9D6: neutral strategy-filtered shadow DECISION metrics.

    Aggregates the strategy's frozen shadow evaluations (verdict counts,
    insufficient-data, rejected-setup, rollout-blocked, pre-rollout ENTER
    candidates, outcome coverage, failure-reason / waiting-reason /
    evidence-category distributions), grouped by strategy version, decision
    policy version and config hash. Missing outcomes stay missing; blocked
    and insufficient states stay separate. Read-only; market-path RETURN
    statistics live on GET /api/admin/shadow-comparison.
    """
    from app.workers.shadow.persistence import (
        fetch_strategy_shadow_evaluations,
    )
    from app.workers.shadow.strategy_metrics import (
        aggregate_strategy_shadow_metrics,
    )

    if not (pattern_code or "").strip():
        raise HTTPException(status_code=422, detail="pattern_code is required")
    if limit < 1 or limit > 2000:
        raise HTTPException(
            status_code=422, detail="limit must be between 1 and 2000"
        )
    records = await fetch_strategy_shadow_evaluations(
        strategy_code=pattern_code,
        symbol=symbol,
        strategy_version=strategy_version,
        decision_policy_version=decision_policy_version,
        experiment_code=experiment_code,
        limit=limit,
    )
    metrics = aggregate_strategy_shadow_metrics(records)
    return {
        "strategy_code": pattern_code,
        "record_limit": limit,
        **metrics,
    }


@router.get("/shadow-comparison")
async def strategy_shadow_comparison(
    _: str = Depends(get_worker_token),
    pattern_code: Optional[str] = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    symbol: Optional[str] = None,
    outcome_status: Optional[str] = None,
    limit: int = 1000,
):
    """Phase 9D5/9D6: strategy-filtered shadow COMPARISON metrics.

    Reuses the existing shadow_pair_resolution_metrics.v1 contract verbatim
    (identity-grouped neutral evidence, positive_return_rate, per-horizon
    mean/median returns, SPY/QQQ baseline-relative returns) over the joined
    pair outcomes where `pattern_code` is the candidate arm. Nothing is
    written and no provider client is constructed.
    """
    from app.workers.shadow.outcomes.constants import (
        METRICS_CONTRACT_VERSION,
        OUTCOME_STATUSES,
    )
    from app.workers.shadow.outcomes.metrics import (
        aggregate_pair_outcome_metrics,
    )
    from app.workers.shadow.outcomes.persistence import fetch_pair_outcomes

    if outcome_status is not None and outcome_status not in OUTCOME_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"outcome_status must be one of {list(OUTCOME_STATUSES)}",
        )
    if limit < 1 or limit > 5000:
        raise HTTPException(
            status_code=422, detail="limit must be between 1 and 5000"
        )
    rows = await fetch_pair_outcomes(
        candidate_strategy_code=pattern_code,
        experiment_code=experiment_code,
        symbol=symbol,
        outcome_status=outcome_status,
        limit=limit,
    )
    return {
        "metrics_contract_version": METRICS_CONTRACT_VERSION,
        "candidate_strategy_code": pattern_code,
        "experiment_code": experiment_code,
        "total_outcomes": len(rows),
        "groups": aggregate_pair_outcome_metrics(rows),
    }


@router.post("/shadow-campaigns")
async def create_shadow_campaign(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_worker_token),
    experiment_code: str = Body(...),
    symbols: Any = Body(...),
    max_symbols: Optional[int] = Body(None),
    as_of_date: Optional[str] = Body(None),
    run_in_background: bool = Body(False),
):
    """Phase 9E6: bounded operator-controlled shadow campaign.

    Plans and executes sequential bounded chunks (max 25 symbols each,
    campaign cap 100) of the declared experiment through the canonical
    shadow runner. `max_symbols` is a REQUIRED explicit safety bound; the
    symbol list is always explicit (no implicit universe), deterministic
    (sorted, deduplicated) and never silently truncated. Optional
    `as_of_date` pins every chunk to one historical session. Campaign chunk
    runs persist as normal strategy_shadow_runs rows carrying a frozen
    campaign telemetry block; retries are idempotent via pair-fingerprint
    dedupe. No scheduling, no watches, no decision cards, no alerts, no
    ranking, and no strategy enablement.
    """
    from app.workers.shadow.campaigns import (
        CampaignRequestError,
        plan_shadow_campaign,
        run_shadow_campaign,
    )
    from app.workers.shadow.experiments import UnknownShadowExperimentError

    logger = logging.getLogger(__name__)

    try:
        plan = plan_shadow_campaign(
            experiment_code=experiment_code,
            symbols=symbols,
            max_symbols=max_symbols,
            as_of_date=as_of_date,
        )
    except UnknownShadowExperimentError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except CampaignRequestError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    try:
        provider = get_market_data_provider()
    except ProviderConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    if run_in_background:
        async def _run() -> None:
            try:
                summary = await run_shadow_campaign(provider, plan)
                logger.info(
                    "[ADMIN] shadow campaign %s finished: status=%s",
                    plan["campaign_id"], summary.get("status"),
                )
            except Exception as exc:
                logger.error(
                    "[ADMIN] shadow campaign %s failed: %s",
                    plan["campaign_id"], exc,
                )

        background_tasks.add_task(_run)
        return {
            "message": "Shadow campaign enqueued",
            "campaign_id": plan["campaign_id"],
            "experiment_code": plan["experiment_code"],
            "requested_count": plan["requested_count"],
            "chunk_count": plan["chunk_count"],
            "as_of_date": plan["as_of_date"],
        }

    summary = await run_shadow_campaign(provider, plan)
    return {"message": "Shadow campaign completed", **summary}


@router.get("/shadow-campaigns")
async def list_shadow_campaigns(
    _: str = Depends(get_worker_token),
    limit: int = 100,
):
    """Phase 9E8: bounded newest-first campaign listing (read-only).

    Groups persisted campaign chunk runs by campaign_id. No provider client
    is constructed and nothing is written.
    """
    from app.workers.shadow.persistence import fetch_shadow_campaign_runs

    if limit < 1 or limit > 500:
        raise HTTPException(
            status_code=422, detail="limit must be between 1 and 500"
        )
    rows = await fetch_shadow_campaign_runs(limit=limit)
    campaigns: dict = {}
    order: List[str] = []
    for row in rows:
        block = row.get("campaign") or {}
        campaign_id = str(block.get("campaign_id") or "unknown")
        if campaign_id not in campaigns:
            order.append(campaign_id)
            campaigns[campaign_id] = {
                "campaign_id": campaign_id,
                "campaign_contract_version": block.get(
                    "campaign_contract_version"
                ),
                "experiment_code": row.get("experiment_code"),
                "experiment_version": row.get("experiment_version"),
                "as_of_date": block.get("as_of_date"),
                "requested_count": block.get("requested_count"),
                "chunk_count": block.get("chunk_count"),
                "observed_chunk_runs": 0,
                "run_statuses": {},
                "first_started_at": row.get("started_at"),
                "last_finished_at": row.get("finished_at"),
            }
        entry = campaigns[campaign_id]
        entry["observed_chunk_runs"] += 1
        status = str(row.get("status"))
        entry["run_statuses"][status] = entry["run_statuses"].get(status, 0) + 1
    return {
        "count": len(order),
        "campaigns": [campaigns[cid] for cid in order],
    }


@router.get("/shadow-campaigns/{campaign_id}")
async def get_shadow_campaign(
    campaign_id: str,
    _: str = Depends(get_worker_token),
    pair_limit: int = 100,
):
    """Phase 9E8: one campaign's bounded detail — its persisted chunk runs,
    per-symbol statuses (evaluated pairs + typed rejections), and outcome
    coverage (missing outcomes stay missing, never zero returns). Read-only:
    no provider call, no write."""
    from app.workers.shadow.persistence import (
        fetch_campaign_outcome_coverage,
        fetch_shadow_campaign_runs,
        fetch_shadow_pairs,
    )

    if pair_limit < 1 or pair_limit > 500:
        raise HTTPException(
            status_code=422, detail="pair_limit must be between 1 and 500"
        )
    runs = await fetch_shadow_campaign_runs(campaign_id=campaign_id, limit=200)
    if not runs:
        raise HTTPException(status_code=404, detail="campaign not found")

    symbol_statuses: dict = {}
    run_summaries = []
    for row in runs:
        run_id = row["run_id"]
        pairs = await fetch_shadow_pairs(run_id=run_id, limit=pair_limit)
        for pair in pairs:
            symbol_statuses[pair["symbol"]] = {
                "status": "evaluated",
                "run_id": run_id,
                "pair_id": pair["pair_id"],
                "control_verdict": pair["control"]["verdict"],
                "candidate_verdict": pair["candidate"]["verdict"],
                "disagreement_category": pair["disagreement_category"],
            }
        for reason, syms in (row.get("rejected_symbols") or {}).items():
            for symbol in syms:
                symbol_statuses.setdefault(symbol, {
                    "status": "rejected",
                    "reason_code": reason,
                    "run_id": run_id,
                })
        run_summaries.append({
            "run_id": run_id,
            "status": row["status"],
            "chunk_index": (row.get("campaign") or {}).get("chunk_index"),
            "pair_count": row.get("pair_count"),
            "pairs_created": row.get("pairs_created"),
            "pairs_deduplicated": row.get("pairs_deduplicated"),
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "error_code": row.get("error_code"),
        })

    coverage = await fetch_campaign_outcome_coverage(
        [row["run_id"] for row in runs]
    )
    first = runs[-1]
    block = first.get("campaign") or {}
    return {
        "campaign_id": campaign_id,
        "campaign_contract_version": block.get("campaign_contract_version"),
        "experiment_code": first.get("experiment_code"),
        "experiment_version": first.get("experiment_version"),
        "as_of_date": block.get("as_of_date"),
        "requested_count": block.get("requested_count"),
        "chunk_count": block.get("chunk_count"),
        "runs": run_summaries,
        "symbol_statuses": symbol_statuses,
        "outcome_coverage": coverage,
    }


def _parse_symbol_query(value: Optional[str]) -> Optional[List[str]]:
    """Parse a comma/space/newline-delimited symbol query into a raw list.

    Returns None when nothing usable is supplied so the audit can fall back to
    the persisted campaign symbol set (the strongest membership source).
    """
    if not value:
        return None
    from app.workers.shadow.universe_identity import parse_symbol_file_text

    tokens = parse_symbol_file_text(value)
    return tokens or None


@router.get("/shadow-campaigns/{campaign_id}/audit")
async def audit_shadow_campaign(
    campaign_id: str,
    _: str = Depends(get_worker_token),
    pattern_code: str = "wyckoff_mtf_v2",
    expected_symbols: Optional[str] = None,
    expected_count: Optional[int] = None,
    limit: int = 500,
):
    """Read-only post-run audit + verdict for ONE prospective campaign.

    Reuses the existing decision-state aggregation and adds the campaign
    identity, membership and side-effect invariants. Verdict is one of
    valid / invalid / incomplete / membership_unverifiable. ZERO confirmed
    triggers is a valid result. Nothing is written and no provider client is
    constructed. Expected membership is proven against the campaign's
    persisted requested symbols first; an explicit `expected_symbols` list
    (your frozen universe file) cross-checks it; `expected_count` alone is a
    weak assertion that can never prove membership.
    """
    from app.workers.shadow.campaign_audit import build_campaign_audit
    from app.workers.shadow.persistence import (
        fetch_shadow_campaign_runs,
        fetch_strategy_shadow_evaluations,
    )

    if limit < 1 or limit > 2000:
        raise HTTPException(
            status_code=422, detail="limit must be between 1 and 2000"
        )
    campaign_runs = await fetch_shadow_campaign_runs(
        campaign_id=campaign_id, limit=200
    )
    if not campaign_runs:
        raise HTTPException(status_code=404, detail="campaign not found")

    records = await fetch_strategy_shadow_evaluations(
        strategy_code=pattern_code,
        campaign_id=campaign_id,
        limit=limit,
    )
    audit = build_campaign_audit(
        records,
        campaign_runs,
        campaign_id=campaign_id,
        expected_symbols=_parse_symbol_query(expected_symbols),
        expected_count=expected_count,
    )
    return audit


async def _latest_completed_session_inputs():
    """Read-only inputs for completed-session resolution (no provider call).

    Returns (latest_bar_date, completion_state, reference_session_dates) where
    completion_state is the frozen ny_session_close.v1 verdict for the latest
    local bar and reference_session_dates is the recent SPY trading calendar.
    """
    from datetime import timedelta

    import pandas as pd

    from app.workers.strategies.bar_completion import assess_latest_bar_completion

    latest = await market_store.get_latest_daily_bar_date()
    if latest is None:
        return None, "unknown", []
    completion = assess_latest_bar_completion(
        pd.DataFrame({"date": [pd.Timestamp(latest)]})
    )
    bars = await market_store.get_local_daily_bars_range(
        "SPY", latest - timedelta(days=20), latest
    )
    reference = sorted({
        b["trading_date"] for b in bars if b.get("trading_date")
    })
    return latest, completion.get("state"), reference


@router.get("/shadow-campaign-preflight")
async def prospective_campaign_preflight(
    _: str = Depends(get_worker_token),
    symbols: str = "",
    experiment_code: str = "wyckoff_v2_vs_baseline",
    expected_count: int = 50,
):
    """Read-only safety preflight for ONE prospective campaign.

    Validates the supplied frozen universe (normalized/deduped/hashed, with
    duplicates and invalid tokens reported), resolves the latest COMPLETED
    trading session via the frozen ny_session_close.v1 policy (never a bare
    latest bar that could be partial), and checks whether an equivalent
    campaign already exists (same experiment + session + universe hash) so the
    operator resumes instead of duplicating. Never schedules, never creates,
    never mutates, never calls a provider.
    """
    from app.workers.shadow.experiments import (
        UnknownShadowExperimentError,
        get_experiment,
    )
    from app.workers.shadow.persistence import fetch_shadow_campaign_runs
    from app.workers.shadow.prospective_preflight import (
        build_prospective_preflight,
        classify_existing_campaigns,
        resolve_latest_completed_session,
    )
    from app.workers.shadow.universe_identity import (
        inspect_universe_symbols,
        parse_symbol_file_text,
    )

    try:
        get_experiment(experiment_code)
    except UnknownShadowExperimentError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    if not (symbols or "").strip():
        raise HTTPException(
            status_code=422,
            detail="symbols is required — the frozen universe must be explicit",
        )

    symbol_report = inspect_universe_symbols(
        parse_symbol_file_text(symbols), expected_count=expected_count
    )
    latest, completion_state, reference = (
        await _latest_completed_session_inputs()
    )
    session = resolve_latest_completed_session(
        latest_bar_date=latest,
        latest_bar_completion_state=completion_state,
        reference_session_dates=reference,
    )
    campaign_runs = await fetch_shadow_campaign_runs(limit=500)
    campaign_match = classify_existing_campaigns(
        campaign_runs,
        experiment_code=experiment_code,
        session_date=session["resolved_session"],
        universe_hash=symbol_report["universe_hash"],
    )
    return build_prospective_preflight(
        experiment_code=experiment_code,
        symbol_report=symbol_report,
        session=session,
        campaign_match=campaign_match,
        expected_count=expected_count,
    )


# --------------------------------------------------------------------------- #
# Phase 9F: shadow evidence review (read-only, advisory, never enabling)
# --------------------------------------------------------------------------- #

def _evidence_filters(
    pattern_code: str,
    experiment_code: Optional[str],
    strategy_version: Optional[str],
    decision_policy_version: Optional[str],
    config_hash: Optional[str],
    symbol: Optional[str],
    campaign_id: Optional[str],
    min_snapshot_date: Optional[str],
    max_snapshot_date: Optional[str],
    trigger_state: Optional[str],
    readiness: Optional[str],
    rollout_blocked: Optional[bool],
    outcome_maturity: Optional[str],
    limit: Optional[int],
):
    from app.workers.shadow.evidence_review import (
        EvidenceFilterError,
        normalize_evidence_filters,
    )

    try:
        return normalize_evidence_filters(
            strategy_code=pattern_code,
            experiment_code=experiment_code,
            strategy_version=strategy_version,
            decision_policy_version=decision_policy_version,
            config_hash=config_hash,
            symbol=symbol,
            campaign_id=campaign_id,
            min_snapshot_date=min_snapshot_date,
            max_snapshot_date=max_snapshot_date,
            trigger_state=trigger_state,
            readiness=readiness,
            rollout_blocked=rollout_blocked,
            outcome_maturity_filter=outcome_maturity,
            limit=limit,
        )
    except EvidenceFilterError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


async def _evidence_outcome_rows(filters) -> list:
    from app.workers.shadow.outcomes.persistence import fetch_pair_outcomes

    return await fetch_pair_outcomes(
        candidate_strategy_code=filters["strategy_code"],
        experiment_code=filters["experiment_code"],
        symbol=filters["symbol"],
        campaign_id=filters["campaign_id"],
        candidate_strategy_version=filters["strategy_version"],
        candidate_config_hash=filters["config_hash"],
        min_snapshot_date=filters["min_snapshot_date"],
        max_snapshot_date=filters["max_snapshot_date"],
        limit=filters["limit"],
    )


@router.get("/shadow-evidence/cohorts")
async def shadow_evidence_cohorts(
    _: str = Depends(get_worker_token),
    pattern_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    trigger_state: Optional[str] = None,
    readiness: Optional[str] = None,
    rollout_blocked: Optional[bool] = None,
    outcome_maturity: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Phase 9F2: explicit versioned cohorts over frozen shadow evidence.

    Read-only: no provider, no writes, no execution. Unknown strategy codes
    yield typed empty cohorts, never errors.
    """
    from app.workers.shadow.evidence_cohorts import build_cohorts
    from app.workers.shadow.evidence_review import (
        fetch_evidence_records,
        filters_for_response,
    )

    filters = _evidence_filters(
        pattern_code, experiment_code, strategy_version,
        decision_policy_version, config_hash, symbol, campaign_id,
        min_snapshot_date, max_snapshot_date, trigger_state, readiness,
        rollout_blocked, outcome_maturity, limit,
    )
    records = await fetch_evidence_records(filters)
    return {
        "filters": filters_for_response(filters),
        **build_cohorts(records),
    }


@router.get("/shadow-evidence/failures")
async def shadow_evidence_failures(
    _: str = Depends(get_worker_token),
    pattern_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    trigger_state: Optional[str] = None,
    readiness: Optional[str] = None,
    rollout_blocked: Optional[bool] = None,
    outcome_maturity: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Phase 9F6: failure / waiting / readiness / trigger-reason
    distributions (each vocabulary kept separate). Read-only."""
    from app.workers.shadow.evidence_cohorts import (
        build_failure_distributions,
    )
    from app.workers.shadow.evidence_review import (
        fetch_evidence_records,
        filters_for_response,
    )

    filters = _evidence_filters(
        pattern_code, experiment_code, strategy_version,
        decision_policy_version, config_hash, symbol, campaign_id,
        min_snapshot_date, max_snapshot_date, trigger_state, readiness,
        rollout_blocked, outcome_maturity, limit,
    )
    records = await fetch_evidence_records(filters)
    return {
        "filters": filters_for_response(filters),
        **build_failure_distributions(records),
    }


@router.get("/shadow-evidence/outcomes")
async def shadow_evidence_outcomes(
    _: str = Depends(get_worker_token),
    pattern_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Phase 9F3: grouped per-horizon outcome and benchmark evidence over
    the existing pair-outcome model (returns reused verbatim; missing
    outcomes reported, never zeroed). Read-only."""
    from app.workers.shadow.evidence_review import (
        fetch_evidence_records,
        filters_for_response,
        outcome_maturity as maturity_of,
    )
    from app.workers.shadow.outcome_evidence import build_outcome_evidence

    filters = _evidence_filters(
        pattern_code, experiment_code, strategy_version,
        decision_policy_version, config_hash, symbol, campaign_id,
        min_snapshot_date, max_snapshot_date, None, None, None, None, limit,
    )
    records = await fetch_evidence_records(filters)
    rows = await _evidence_outcome_rows(filters)
    missing = sum(1 for r in records if maturity_of(r) == "missing")
    return {
        "filters": filters_for_response(filters),
        **build_outcome_evidence(rows, missing_outcome_count=missing),
    }


@router.get("/shadow-evidence/quality")
async def shadow_evidence_quality(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    pattern_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Phase 9F4: versioned evidence-quality audit (blocking / warning /
    informational). Read-only; nothing is repaired or mutated."""
    from app.workers.shadow.evidence_review import (
        fetch_evidence_records,
        filters_for_response,
    )
    from app.workers.shadow.persistence import fetch_shadow_campaign_runs
    from app.workers.shadow.quality_audit import build_quality_audit
    from app.workers.strategies.discovery import discover_strategy

    filters = _evidence_filters(
        pattern_code, experiment_code, strategy_version,
        decision_policy_version, config_hash, symbol, campaign_id,
        min_snapshot_date, max_snapshot_date, None, None, None, None, limit,
    )
    records = await fetch_evidence_records(filters)
    rows = await _evidence_outcome_rows(filters)
    campaign_runs = await fetch_shadow_campaign_runs(
        campaign_id=filters["campaign_id"], limit=200
    )
    discovery = await discover_strategy(db, filters["strategy_code"])
    discovery_block = None
    if discovery is not None:
        discovery_block = {
            "db_configured": discovery.db_configured,
            "config_status": discovery.config_status,
        }
    audit = build_quality_audit(
        records,
        campaign_runs=campaign_runs,
        outcome_rows=rows,
        strategy_discovery=discovery_block,
    )
    return {"filters": filters_for_response(filters), **audit}


async def _cohort_trading_calendar(records) -> tuple:
    """Read-only trading calendar for the cohort from the local daily_bars.

    Uses SPY (always a benchmark of every shadow outcome) as the session
    reference. Returns (sorted session dates, latest completed session). An
    empty calendar leaves session-based eligibility honestly UNKNOWN — it is
    never inferred from calendar days.
    """
    from datetime import date as _date

    snapshots = [
        r.get("snapshot_date") for r in records if r.get("snapshot_date")
    ]
    latest = await market_store.get_latest_daily_bar_date()
    if not snapshots or latest is None:
        return [], latest

    def _as_date(v):
        return v if isinstance(v, _date) else _date.fromisoformat(str(v))

    min_snapshot = min(_as_date(s) for s in snapshots)
    bars = await market_store.get_local_daily_bars_range("SPY", min_snapshot, latest)
    session_dates = sorted({b["trading_date"] for b in bars if b.get("trading_date")})
    return session_dates, latest


async def _run_configured_access_check(db):
    """Run the access-check with the identity policy resolved from settings.

    Shared by the access-check endpoint and the closeout fail-closed gate so
    their readiness semantics can never diverge. `AUDIT_EXPECTED_DB_ROLE` is
    REQUIRED once a real AUDIT_DATABASE_URL is configured in audit mode.
    """
    from app.audit_access import run_access_check
    from app.audit_db import audit_database_configured, get_connection_mode

    require_expected_role = (
        settings.AUDIT_ONLY_MODE and audit_database_configured()
    )
    return await run_access_check(
        db,
        expected_role=(settings.AUDIT_EXPECTED_DB_ROLE or None),
        require_expected_role=require_expected_role,
        connection_mode=get_connection_mode(),
    )


@router.get("/shadow-cohort/access-check")
async def shadow_cohort_access_check(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
):
    """Read-only proof that the connected PostgreSQL identity has ONLY the
    privileges the cohort closeout audit requires (SELECT on the exact closeout
    relations), no write privileges, no elevated role attributes, and matches
    the expected audit role. Runs inside a read-only transaction and issues NO
    mutation SQL. Worker-token protected. Reports only safe capability
    information — never a connection string, hostname, password, Supabase URL,
    secret value or raw SQL error."""
    return await _run_configured_access_check(db)


@router.get("/shadow-cohort/closeout")
async def shadow_cohort_closeout(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    pattern_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Read-only closeout audit for an existing historical shadow cohort.

    Summarizes the cohort's decision-state coverage, versioned quality issues,
    per-horizon / per-status outcome counts, provider failures, the single
    `forward_fetch_error` rows, duplicate detection and — using COMPLETED
    TRADING SESSIONS, never calendar days — how many outcomes are eligible for
    maturation now versus not yet eligible. It is strictly read-only: it never
    matures anything. Maturation stays on the existing bounded endpoint
    POST /api/admin/shadow/outcomes/calculate.
    """
    from app.workers.shadow.cohort_closeout import build_cohort_closeout_audit
    from app.workers.shadow.evidence_review import (
        fetch_evidence_records,
        filters_for_response,
    )
    from app.workers.shadow.persistence import fetch_shadow_campaign_runs
    from app.workers.strategies.discovery import discover_strategy

    # Bounded-scope guard: an operational cohort is never "all history for a
    # strategy". Require at least one explicit cohort selector so a bare call
    # can never sweep the whole shadow record set.
    if not any([
        experiment_code, campaign_id, strategy_version, config_hash, symbol,
        min_snapshot_date, max_snapshot_date,
    ]):
        raise HTTPException(
            status_code=422,
            detail=(
                "a cohort selector is required (one or more of: experiment_code, "
                "campaign_id, strategy_version, config_hash, symbol, "
                "min_snapshot_date, max_snapshot_date) — the closeout audit never "
                "scans all shadow history"
            ),
        )

    # Fail-closed readiness gate (audit-only mode): the closeout refuses to run
    # unless the SAME connection satisfies the access-check contract — reusing
    # the identical service, never duplicating its logic. The operator does not
    # have to remember to run access-check first. Only bounded safe reason codes
    # are returned (never a DSN / host / credential).
    if settings.AUDIT_ONLY_MODE:
        access = await _run_configured_access_check(db)
        if not access["ready_for_closeout_audit"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "closeout_not_ready",
                    "database_connection_mode": access.get(
                        "database_connection_mode"
                    ),
                    "reasons": access["reasons"],
                },
            )

    filters = _evidence_filters(
        pattern_code, experiment_code, strategy_version,
        decision_policy_version, config_hash, symbol, campaign_id,
        min_snapshot_date, max_snapshot_date, None, None, None, None, limit,
    )
    records = await fetch_evidence_records(filters)
    outcome_rows = await _evidence_outcome_rows(filters)
    campaign_runs = await fetch_shadow_campaign_runs(
        campaign_id=filters["campaign_id"], limit=200
    )
    discovery = await discover_strategy(db, filters["strategy_code"])
    discovery_block = None
    if discovery is not None:
        discovery_block = {
            "db_configured": discovery.db_configured,
            "config_status": discovery.config_status,
        }
    session_dates, latest = await _cohort_trading_calendar(records)

    audit = build_cohort_closeout_audit(
        records,
        outcome_rows,
        session_dates=session_dates,
        latest_completed_session=latest,
        campaign_runs=campaign_runs,
        campaign_ids=(
            [filters["campaign_id"]] if filters["campaign_id"] else None
        ),
        strategy_discovery=discovery_block,
    )
    return {"filters": filters_for_response(filters), **audit}


@router.get("/shadow-cohort/maturation-plan")
async def shadow_cohort_maturation_plan(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    pattern_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    cohort_scope: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
    duplicate_focus: Optional[str] = None,
):
    """Read-only bounded maturation PLAN for an existing shadow cohort.

    `cohort_scope` is REQUIRED and selects the cohort:
      * `campaign`   — only campaign-linked eligible pairs (each with a valid
        persisted `telemetry.campaign` block): the EXECUTABLE maturation
        manifest. Manual/legacy non-campaign evidence is reported under
        `excluded_non_campaign_evidence` and never blocks.
      * `experiment` — every eligible pair (manual/legacy included): the broad
        read-only experiment-evidence view. It stays `safe_to_execute=false`
        while any eligible pair lacks verifiable campaign membership and must
        NOT be used as the execution manifest.

    Produces the COMPLETE, deterministically ordered, paginated manifest for the
    chosen scope, a SEPARATE retry plan for retryable failures, per-record
    attribution of duplicate (symbol, session) groups, campaign-membership
    verification and a scope-stamped stable manifest hash — then proves whether a
    later bounded maturation run is safe to execute.

    It is strictly read-only: it never matures, never recalculates, never calls
    a provider and issues no mutation SQL. Maturation itself stays on the
    existing bounded endpoint POST /api/admin/shadow/outcomes/calculate, which
    this staging app (audit-only, SELECT-only role, no provider) cannot reach.
    """
    from app.workers.shadow.evidence_review import (
        MAX_RECORD_LIMIT,
        fetch_evidence_records,
        filters_for_response,
    )
    from app.workers.shadow.maturation_plan import (
        COHORT_SCOPES,
        MAX_PAGE_LIMIT,
        build_maturation_plan,
    )

    # cohort_scope is explicit and required — never silently reinterpret the
    # legacy experiment-wide default as the executable campaign manifest.
    if cohort_scope is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "cohort_scope is required and must be one of "
                f"{list(COHORT_SCOPES)}: 'campaign' is the executable maturation "
                "manifest (campaign-linked records only); 'experiment' is the "
                "broad read-only experiment-evidence view"
            ),
        )
    if cohort_scope not in COHORT_SCOPES:
        raise HTTPException(
            status_code=422,
            detail=f"cohort_scope must be one of {list(COHORT_SCOPES)}",
        )

    # Bounded-scope guard: identical to closeout — an operational cohort is
    # never "all history for a strategy".
    if not any([
        experiment_code, campaign_id, strategy_version, config_hash, symbol,
        min_snapshot_date, max_snapshot_date,
    ]):
        raise HTTPException(
            status_code=422,
            detail=(
                "a cohort selector is required (one or more of: experiment_code, "
                "campaign_id, strategy_version, config_hash, symbol, "
                "min_snapshot_date, max_snapshot_date) — the maturation plan "
                "never scans all shadow history"
            ),
        )
    if limit < 1 or limit > MAX_PAGE_LIMIT:
        raise HTTPException(
            status_code=422,
            detail=f"limit must be between 1 and {MAX_PAGE_LIMIT}",
        )
    if offset < 0:
        raise HTTPException(status_code=422, detail="offset must be >= 0")

    # Same fail-closed readiness gate as closeout: refuse unless the SAME
    # connection satisfies the access-check contract.
    if settings.AUDIT_ONLY_MODE:
        access = await _run_configured_access_check(db)
        if not access["ready_for_closeout_audit"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "maturation_plan_not_ready",
                    "database_connection_mode": access.get(
                        "database_connection_mode"
                    ),
                    "reasons": access["reasons"],
                },
            )

    # The manifest must be COMPLETE regardless of page size, so the underlying
    # evidence read always fetches the whole cohort at the bounded hard cap
    # (never an unbounded request); page limit/offset only slice the eligible
    # manifest in memory.
    filters = _evidence_filters(
        pattern_code, experiment_code, strategy_version,
        decision_policy_version, config_hash, symbol, campaign_id,
        min_snapshot_date, max_snapshot_date, None, None, None, None,
        MAX_RECORD_LIMIT,
    )
    records = await fetch_evidence_records(filters)
    outcome_rows = await _evidence_outcome_rows(filters)
    session_dates, latest = await _cohort_trading_calendar(records)

    # Recover each pair's origin run (read-only, on the SAME audit connection,
    # within the already-granted strategy_shadow_pairs SELECT). Kept here rather
    # than widening the frozen shared evaluation read.
    pair_ids = [str(r["pair_id"]) for r in records if r.get("pair_id")]
    if pair_ids:
        run_rows = await db.fetch(
            "SELECT id, origin_run_id FROM strategy_shadow_pairs "
            "WHERE id = ANY($1::uuid[])",
            pair_ids,
        )
        run_by_pair = {
            str(r["id"]): (str(r["origin_run_id"]) if r["origin_run_id"] else None)
            for r in run_rows
        }
        # Per-pair campaign telemetry blocks from EVERY linked run (read-only,
        # already-granted relations). Campaign membership is decided ONLY from
        # persisted telemetry, never from symbol/date/overlap.
        from app.workers.shadow.persistence import _maybe_json

        block_rows = await db.fetch(
            "SELECT rp.pair_id AS pair_id, r.telemetry->'campaign' AS campaign "
            "FROM strategy_shadow_run_pairs rp "
            "JOIN strategy_shadow_runs r ON r.id = rp.run_id "
            "WHERE rp.pair_id = ANY($1::uuid[]) "
            "AND r.telemetry->'campaign' IS NOT NULL",
            pair_ids,
        )
        blocks_by_pair: dict = {}
        for br in block_rows:
            block = _maybe_json(br["campaign"])
            if isinstance(block, dict):
                blocks_by_pair.setdefault(str(br["pair_id"]), []).append(block)
        for record in records:
            rpid = str(record.get("pair_id"))
            record["run_id"] = run_by_pair.get(rpid)
            record["campaign_blocks"] = blocks_by_pair.get(rpid, [])

    focus_keys = (
        [k.strip().upper() for k in duplicate_focus.split(",") if k.strip()]
        if duplicate_focus else None
    )
    plan = build_maturation_plan(
        records,
        outcome_rows,
        cohort_scope=cohort_scope,
        applied_filters=filters_for_response(filters),
        session_dates=session_dates,
        latest_completed_session=latest,
        campaign_ids=([filters["campaign_id"]] if filters["campaign_id"] else None),
        page_limit=limit,
        page_offset=offset,
        records_possibly_truncated=(len(records) >= MAX_RECORD_LIMIT),
        duplicate_focus=focus_keys,
    )
    return plan


@router.get("/shadow-cohort/pair-lineage")
async def shadow_cohort_pair_lineage(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    pair_ids: Optional[str] = None,
):
    """Read-only lineage/provenance audit for an explicit bounded set of pairs.

    Joins each requested pair to its evaluations, every linked run (origin +
    run_pairs), each run's bounded/redacted telemetry (campaign block only —
    never an unrelated JSON dump), the run's sibling pairs and the outcome
    status, then deterministically classifies why the pair does or does not
    carry campaign membership. Requires explicit pair_ids (comma-separated, at
    most 20); no all-history mode. Strictly read-only: reuses the closeout audit
    access gate, constructs no provider and issues no mutation SQL.
    """
    from app.workers.shadow.pair_lineage import (
        MAX_LINEAGE_PAIR_IDS,
        build_pair_lineage,
        read_pair_lineage,
    )

    ids = [i.strip() for i in (pair_ids or "").split(",") if i.strip()]
    if not ids:
        raise HTTPException(
            status_code=422,
            detail="pair_ids is required (comma-separated UUIDs; the lineage "
                   "audit never scans all history)",
        )
    if len(ids) > MAX_LINEAGE_PAIR_IDS:
        raise HTTPException(
            status_code=422,
            detail=f"at most {MAX_LINEAGE_PAIR_IDS} pair_ids per request",
        )
    seen: set = set()
    canonical: List[str] = []
    for i in ids:
        try:
            cu = str(uuid.UUID(i))
        except (ValueError, AttributeError):
            raise HTTPException(
                status_code=422, detail="pair_ids contains a malformed UUID"
            )
        if cu not in seen:
            seen.add(cu)
            canonical.append(cu)

    # Same fail-closed readiness gate as closeout/maturation-plan.
    if settings.AUDIT_ONLY_MODE:
        access = await _run_configured_access_check(db)
        if not access["ready_for_closeout_audit"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "pair_lineage_not_ready",
                    "database_connection_mode": access.get(
                        "database_connection_mode"
                    ),
                    "reasons": access["reasons"],
                },
            )

    raw = await read_pair_lineage(db, canonical)
    return build_pair_lineage(raw, canonical)


# --------------------------------------------------------------------------- #
# Shadow Outcome Maintenance Environment (maintenance-only mode).
# --------------------------------------------------------------------------- #
def _require_maintenance_mode():
    """Maintenance routes only function on the maintenance app; elsewhere they
    return the stable hidden-route 404 (defence in depth on top of the gate)."""
    if not settings.MAINTENANCE_ONLY_MODE:
        raise HTTPException(status_code=404, detail="Not Found")


async def _recompute_maintenance_plan(db, *, experiment_code, cohort_scope,
                                      batch_size=None):
    """Recompute the COMPLETE campaign maturation plan with the maintenance
    identity — the same read the audit planner produces. Read-only."""
    from app.workers.shadow.evidence_review import (
        MAX_RECORD_LIMIT,
        fetch_evidence_records,
        filters_for_response,
    )
    from app.workers.shadow.maturation_plan import build_maturation_plan
    from app.workers.shadow.persistence import _maybe_json

    filters = _evidence_filters(
        "wyckoff_mtf_v2", experiment_code, None, None, None, None, None,
        None, None, None, None, None, None, MAX_RECORD_LIMIT,
    )
    records = await fetch_evidence_records(filters)
    outcome_rows = await _evidence_outcome_rows(filters)
    session_dates, latest = await _cohort_trading_calendar(records)
    pair_ids = [str(r["pair_id"]) for r in records if r.get("pair_id")]
    if pair_ids:
        run_rows = await db.fetch(
            "SELECT id, origin_run_id FROM strategy_shadow_pairs "
            "WHERE id = ANY($1::uuid[])", pair_ids)
        run_by_pair = {str(r["id"]): (str(r["origin_run_id"]) if r["origin_run_id"]
                                      else None) for r in run_rows}
        block_rows = await db.fetch(
            "SELECT rp.pair_id AS pair_id, r.telemetry->'campaign' AS campaign "
            "FROM strategy_shadow_run_pairs rp "
            "JOIN strategy_shadow_runs r ON r.id = rp.run_id "
            "WHERE rp.pair_id = ANY($1::uuid[]) "
            "AND r.telemetry->'campaign' IS NOT NULL", pair_ids)
        blocks_by_pair: dict = {}
        for br in block_rows:
            block = _maybe_json(br["campaign"])
            if isinstance(block, dict):
                blocks_by_pair.setdefault(str(br["pair_id"]), []).append(block)
        for record in records:
            rpid = str(record.get("pair_id"))
            record["run_id"] = run_by_pair.get(rpid)
            record["campaign_blocks"] = blocks_by_pair.get(rpid, [])
    kw = {} if batch_size is None else {"batch_size": batch_size}
    return build_maturation_plan(
        records, outcome_rows, cohort_scope=cohort_scope,
        applied_filters=filters_for_response(filters),
        session_dates=session_dates, latest_completed_session=latest,
        page_limit=MAX_RECORD_LIMIT, page_offset=0,
        records_possibly_truncated=(len(records) >= MAX_RECORD_LIMIT),
        **kw,
    )


@router.get("/shadow-maintenance/access-check")
async def shadow_maintenance_access_check(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
):
    """Read-only proof the connected identity is EXACTLY the least-privilege
    outcome-maintenance role and the process is a maintenance-only, Massive-
    backed, scheduler-disabled, single-mutation-route app. Never mutates, never
    constructs a provider, never calls Massive; returns only safe capability
    metadata (no DSN / host / password / API key / token)."""
    _require_maintenance_mode()
    from app.audit_db import get_connection_mode
    from app.maintenance_access import run_maintenance_access_check

    provider = (settings.MARKET_DATA_PROVIDER or "").lower()
    credential = bool((settings.MASSIVE_API_KEY or "").strip()) if provider == "massive" else False
    # Recompute the STABLE cohort lock hash (read-only) so the configured lock
    # can be verified. Never derived from the dynamic remaining manifest.
    current_lock = None
    cohort_pair_count = None
    try:
        plan = await _recompute_maintenance_plan(
            db, experiment_code=settings.MAINTENANCE_ALLOWED_EXPERIMENT_CODE,
            cohort_scope=settings.MAINTENANCE_ALLOWED_COHORT_SCOPE)
        current_lock = plan.get("cohort_lock_hash")
        cohort_pair_count = plan.get("cohort_pair_count")
    except Exception:
        current_lock = None  # evaluate() flags cohort_lock_unverifiable
    return await run_maintenance_access_check(
        db,
        expected_role=(settings.MAINTENANCE_EXPECTED_DB_ROLE or None),
        connection_mode=get_connection_mode(),
        provider=provider,
        provider_credential_configured=credential,
        scheduler_enabled=settings.ENABLE_SCHEDULER,
        maintenance_only_mode=settings.MAINTENANCE_ONLY_MODE,
        max_batch_size=settings.MAINTENANCE_MAX_BATCH_SIZE,
        mutation_route_count=1,
        locked_cohort_hash=(settings.MAINTENANCE_LOCKED_COHORT_HASH or None),
        current_cohort_lock_hash=current_lock,
        cohort_pair_count=cohort_pair_count,
    )


@router.get("/shadow-maintenance/preflight")
async def shadow_maintenance_preflight(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
):
    """Read-only manifest-locked preflight: recomputes the current campaign plan
    with the SAME identity execution uses, reports the live manifest count/hash,
    safe_to_execute, the excluded non-campaign records, the separate retry plan
    and the batch plan. Never writes, never calls Massive."""
    _require_maintenance_mode()
    exp = settings.MAINTENANCE_ALLOWED_EXPERIMENT_CODE
    scope = settings.MAINTENANCE_ALLOWED_COHORT_SCOPE
    plan = await _recompute_maintenance_plan(
        db, experiment_code=exp, cohort_scope=scope,
        batch_size=settings.MAINTENANCE_MAX_BATCH_SIZE)
    planning = plan["planning"]
    locked = (settings.MAINTENANCE_LOCKED_COHORT_HASH or "").strip()
    locked_matches = bool(locked) and locked == plan["cohort_lock_hash"]
    return {
        "contract_version": "shadow_maintenance_preflight.v2",
        "experiment_code": exp,
        "cohort_scope": scope,
        # STABLE cohort-membership lock (outcome-status blind)
        "cohort_lock_hash": plan["cohort_lock_hash"],
        "cohort_lock_hash_version": plan["cohort_lock_hash_version"],
        "cohort_pair_count": plan["cohort_pair_count"],
        "locked_cohort_hash_configured": bool(locked),
        "locked_cohort_hash_matches": locked_matches,
        # DYNAMIC remaining-work manifest (shrinks after each successful batch)
        "remaining_manifest_hash": plan["remaining_manifest_hash"],
        "remaining_manifest_hash_version": plan["remaining_manifest_hash_version"],
        "remaining_pair_count": plan["remaining_pair_count"],
        "normal_execution_complete": plan["normal_execution_complete"],
        "next_batch": plan["next_batch"],
        # retry stays separate
        "retry_plan_hash": plan["retry_plan"]["retry_plan_hash"],
        "retryable_failure_count": plan["retryable_failure_count"],
        "terminal_failure_count": plan["terminal_failure_count"],
        # context
        "safe_to_execute": planning["safe_to_execute"],
        "blocking_reasons": planning["blocking_reasons"],
        "experiment_eligible_unmatured_count": plan["experiment_eligible_unmatured_count"],
        "excluded_non_campaign_count": plan["excluded_non_campaign_eligible_count"],
        "campaign_membership_unverifiable_count": (
            plan["membership"]["campaign_membership_unverifiable_count"]),
        # execution available only when safe AND the stable lock matches
        "execution_available": bool(planning["safe_to_execute"] and locked_matches
                                    and plan["next_batch"]["available"]),
    }


@router.post("/shadow-maintenance/outcomes/execute")
async def shadow_maintenance_execute(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    body: Any = Body(...),
):
    """The ONE tightly-validated maintenance mutation route. Recomputes the live
    plan, validates the request against the manifest/retry hash and the exact
    server-computed batch slice, takes a single advisory lock, enforces
    idempotency, then reuses the existing outcome-calculation service. NOT
    invoked during the environment-readiness task."""
    _require_maintenance_mode()
    from app.maintenance_execute import (
        MAINTENANCE_ADVISORY_LOCK_KEY,
        MODE_RETRY,
        validate_normal,
        validate_retry,
    )

    exp = settings.MAINTENANCE_ALLOWED_EXPERIMENT_CODE
    scope = settings.MAINTENANCE_ALLOWED_COHORT_SCOPE
    locked = (settings.MAINTENANCE_LOCKED_COHORT_HASH or None)
    logger = logging.getLogger(__name__)
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="request body must be a JSON object")
    mode = body.get("mode")

    async def _statuses(pair_ids):
        if not pair_ids:
            return {}
        rows = await db.fetch(
            "SELECT pair_id, outcome_status FROM strategy_shadow_pair_outcomes "
            "WHERE pair_id = ANY($1::uuid[])", pair_ids)
        return {str(r["pair_id"]): r["outcome_status"] for r in rows}

    def _validate(p):
        if mode == MODE_RETRY:
            return validate_retry(p, body, allowed_experiment=exp,
                                  allowed_scope=scope, locked_cohort_hash=locked)
        return validate_normal(p, body, allowed_experiment=exp, allowed_scope=scope,
                               max_batch_size=settings.MAINTENANCE_MAX_BATCH_SIZE,
                               locked_cohort_hash=locked)

    plan = await _recompute_maintenance_plan(
        db, experiment_code=exp, cohort_scope=scope,
        batch_size=settings.MAINTENANCE_MAX_BATCH_SIZE)
    verdict = _validate(plan)

    # ---- replay / drift handling when strict validation fails -------------- #
    REPLAY_REASONS = {"remaining_manifest_hash_mismatch", "next_batch_hash_mismatch",
                      "pair_ids_not_expected_next_batch", "no_next_batch"}
    if not verdict["ok"]:
        reason = verdict["reason"]
        if reason == "cohort_lock_drift":
            raise HTTPException(status_code=409, detail={
                "error": "cohort_lock_drift",
                "detail": "the stable cohort membership changed — investigate; "
                          "this is NOT an idempotent replay"})
        if (mode != MODE_RETRY and reason in REPLAY_REASONS
                and body.get("cohort_lock_hash") == plan.get("cohort_lock_hash")):
            supplied = [str(p) for p in (body.get("pair_ids") or [])]
            st = await _statuses(supplied)
            complete = [p for p in supplied if st.get(p) == "complete"]
            if supplied and len(complete) == len(supplied):
                return {"status": "already_applied",
                        "detail": "this batch already matured; obtain a fresh preflight",
                        "pairs": [{"pair_id": p, "outcome_status": "complete"}
                                  for p in supplied]}
            if complete:
                raise HTTPException(status_code=409, detail={
                    "error": "stale_partial_batch",
                    "detail": "some pairs matured, others not — obtain a fresh preflight"})
        raise HTTPException(status_code=422, detail={
            "error": "maintenance_validation_failed", "reason": reason})

    validated = verdict["validated_pair_ids"]
    include_recalc = verdict["include_recalc"]
    batch_identity = verdict["batch_identity"]

    got_lock = await db.fetchval(
        "SELECT pg_try_advisory_lock($1)", MAINTENANCE_ADVISORY_LOCK_KEY)
    if not got_lock:
        raise HTTPException(status_code=409, detail={
            "error": "maintenance_execution_in_progress", "batch_identity": batch_identity})
    try:
        # DOUBLE-CHECK under the lock: recompute + re-validate so a preflight/
        # execution race cannot slip a stale batch past the first check.
        plan2 = await _recompute_maintenance_plan(
            db, experiment_code=exp, cohort_scope=scope,
            batch_size=settings.MAINTENANCE_MAX_BATCH_SIZE)
        verdict2 = _validate(plan2)
        if not verdict2["ok"] or verdict2["validated_pair_ids"] != validated:
            raise HTTPException(status_code=409, detail={
                "error": "maintenance_plan_changed_under_lock",
                "reason": verdict2.get("reason"),
                "detail": "obtain a fresh preflight and retry"})

        # Idempotency: inspect current outcome statuses BEFORE any provider call.
        status_by = await _statuses(validated)
        complete = [p for p in validated if status_by.get(p) == "complete"]
        if complete and len(complete) == len(validated):
            return {"status": "already_applied", "batch_identity": batch_identity,
                    "pairs": [{"pair_id": p, "status": "already_applied",
                               "outcome_status": "complete", "error_code": None,
                               "created_or_reused": "reused"} for p in validated]}
        if mode != MODE_RETRY and (complete or
                                   any(status_by.get(p) == "error" for p in validated)):
            raise HTTPException(status_code=409, detail={
                "error": "stale_partial_batch", "batch_identity": batch_identity,
                "detail": "some pairs already resolved — obtain a fresh preflight/manifest"})

        # Execute via the EXISTING service (not invoked in the readiness task).
        from app.workers.shadow.outcomes.service import run_shadow_outcome_calculation
        provider = get_market_data_provider()
        logger.info("[MAINT] executing %s (%d pairs, include_recalc=%s)",
                    batch_identity, len(validated), include_recalc)
        # The service is synchronous by construction (no run_in_background arg);
        # we deliberately never use background mode for maintenance execution.
        summary = await run_shadow_outcome_calculation(
            provider, pair_ids=validated, symbols=None, run_id=None, pending=False,
            limit=len(validated), include_recalc=include_recalc)
        after = await db.fetch(
            "SELECT pair_id, outcome_status, error_code FROM "
            "strategy_shadow_pair_outcomes WHERE pair_id = ANY($1::uuid[])", validated)
        st = {str(r["pair_id"]): r for r in after}
        return {
            "status": "executed", "batch_identity": batch_identity,
            "mode": mode, "outcome_run_id": summary.get("outcome_run_id"),
            "pairs": [{"pair_id": p,
                       "status": "processed",
                       "outcome_status": (st.get(p) or {}).get("outcome_status"),
                       "error_code": (st.get(p) or {}).get("error_code"),
                       "created_or_reused": "created_or_updated"} for p in validated],
        }
    finally:
        await db.fetchval("SELECT pg_advisory_unlock($1)", MAINTENANCE_ADVISORY_LOCK_KEY)


@router.get("/shadow-evidence/readiness")
async def shadow_evidence_readiness(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    pattern_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    limit: Optional[int] = None,
    min_evaluated: Optional[int] = None,
    min_unique_symbols: Optional[int] = None,
    min_unique_sessions: Optional[int] = None,
    min_trigger_confirmed: Optional[int] = None,
    min_matured_outcomes: Optional[int] = None,
    min_outcome_coverage: Optional[float] = None,
    max_provider_failure_rate: Optional[float] = None,
    max_frame_rejection_rate: Optional[float] = None,
    max_readiness_unknown_rate: Optional[float] = None,
    min_performance_sample: Optional[int] = None,
    min_divergent_resolved: Optional[int] = None,
    target_horizon: Optional[str] = None,
):
    """Phase 9F5: ADVISORY rollout-readiness decision.

    Transparent versioned policy over frozen evidence: every threshold,
    observed value and pass/fail condition is returned. This endpoint can
    only ever return an advisory state — it never enables the strategy and
    never mutates any configuration.
    """
    from app.workers.shadow.evidence_review import (
        fetch_evidence_records,
        filters_for_response,
    )
    from app.workers.shadow.persistence import fetch_shadow_campaign_runs
    from app.workers.shadow.quality_audit import build_quality_audit
    from app.workers.shadow.rollout_readiness import (
        ReadinessOverrideError,
        evaluate_rollout_readiness,
        resolve_thresholds,
    )
    from app.workers.strategies.discovery import discover_strategy

    filters = _evidence_filters(
        pattern_code, experiment_code, strategy_version,
        decision_policy_version, config_hash, symbol, campaign_id,
        min_snapshot_date, max_snapshot_date, None, None, None, None, limit,
    )
    try:
        thresholds = resolve_thresholds({
            "min_evaluated": min_evaluated,
            "min_unique_symbols": min_unique_symbols,
            "min_unique_sessions": min_unique_sessions,
            "min_trigger_confirmed": min_trigger_confirmed,
            "min_matured_outcomes": min_matured_outcomes,
            "min_outcome_coverage": min_outcome_coverage,
            "max_provider_failure_rate": max_provider_failure_rate,
            "max_frame_rejection_rate": max_frame_rejection_rate,
            "max_readiness_unknown_rate": max_readiness_unknown_rate,
            "min_performance_sample": min_performance_sample,
            "min_divergent_resolved": min_divergent_resolved,
            "target_horizon": target_horizon,
        })
    except ReadinessOverrideError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    records = await fetch_evidence_records(filters)
    rows = await _evidence_outcome_rows(filters)
    campaign_runs = await fetch_shadow_campaign_runs(
        campaign_id=filters["campaign_id"], limit=200
    )
    discovery = await discover_strategy(db, filters["strategy_code"])
    discovery_block = None
    if discovery is not None:
        discovery_block = {
            "db_configured": discovery.db_configured,
            "config_status": discovery.config_status,
        }
    audit = build_quality_audit(
        records,
        campaign_runs=campaign_runs,
        outcome_rows=rows,
        strategy_discovery=discovery_block,
    )
    latest = max(
        (str(r.get("created_at")) for r in records
         if r.get("created_at") is not None),
        default=None,
    )
    return evaluate_rollout_readiness(
        records,
        outcome_rows=rows,
        quality_audit=audit,
        thresholds=thresholds,
        filters=filters_for_response(filters),
        evidence_timestamp=latest,
    )


@router.get("/shadow-evidence/export")
async def shadow_evidence_export(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    pattern_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    limit: Optional[int] = None,
    max_record_references: int = 200,
):
    """Phase 9F7: bounded deterministic evidence package for human review.

    The deterministic evidence body is hashed (content_sha256) and separated
    from the generation timestamp. Read-only; never contains credentials.
    """
    from datetime import datetime, timezone

    from app.workers.shadow.evidence_export import build_evidence_export
    from app.workers.shadow.evidence_review import (
        fetch_evidence_records,
        outcome_maturity as maturity_of,
    )
    from app.workers.shadow.outcome_evidence import build_outcome_evidence
    from app.workers.shadow.persistence import fetch_shadow_campaign_runs
    from app.workers.shadow.quality_audit import build_quality_audit
    from app.workers.shadow.rollout_readiness import (
        evaluate_rollout_readiness,
        resolve_thresholds,
    )
    from app.workers.strategies.discovery import discover_strategy

    if max_record_references < 1 or max_record_references > 200:
        raise HTTPException(
            status_code=422,
            detail="max_record_references must be between 1 and 200",
        )
    filters = _evidence_filters(
        pattern_code, experiment_code, strategy_version,
        decision_policy_version, config_hash, symbol, campaign_id,
        min_snapshot_date, max_snapshot_date, None, None, None, None, limit,
    )
    records = await fetch_evidence_records(filters)
    rows = await _evidence_outcome_rows(filters)
    campaign_runs = await fetch_shadow_campaign_runs(
        campaign_id=filters["campaign_id"], limit=200
    )
    discovery = await discover_strategy(db, filters["strategy_code"])
    discovery_block = None
    if discovery is not None:
        discovery_block = {
            "db_configured": discovery.db_configured,
            "config_status": discovery.config_status,
        }
    audit = build_quality_audit(
        records,
        campaign_runs=campaign_runs,
        outcome_rows=rows,
        strategy_discovery=discovery_block,
    )
    latest = max(
        (str(r.get("created_at")) for r in records
         if r.get("created_at") is not None),
        default=None,
    )
    readiness = evaluate_rollout_readiness(
        records,
        outcome_rows=rows,
        quality_audit=audit,
        thresholds=resolve_thresholds(),
        filters=None,
        evidence_timestamp=latest,
    )
    missing = sum(1 for r in records if maturity_of(r) == "missing")
    return build_evidence_export(
        filters=filters,
        records=records,
        outcome_evidence=build_outcome_evidence(
            rows, missing_outcome_count=missing
        ),
        quality_audit=audit,
        readiness=readiness,
        campaign_runs=campaign_runs,
        generated_at=datetime.now(timezone.utc).isoformat(),
        max_record_references=max_record_references,
    )


@router.post("/shadow-campaign-plan")
async def shadow_campaign_plan(
    _: str = Depends(get_worker_token),
    experiment_code: str = Body("wyckoff_v2_vs_baseline"),
    candidate_symbols: Any = Body(...),
    as_of_sessions: Any = Body(...),
    max_symbols_per_campaign: Optional[int] = Body(None),
    target_unique_symbols: Optional[int] = Body(None),
    target_trigger_confirmed: Optional[int] = Body(None),
    target_matured_outcomes: Optional[int] = Body(None),
    target_horizon: str = Body("20D"),
    existing_evaluated_symbols: Optional[List[str]] = Body(None),
    existing_unique_symbols: int = Body(0),
    existing_trigger_confirmed: int = Body(0),
    existing_matured_outcomes: int = Body(0),
):
    """Phase 9F8: deterministic campaign PLAN — never an execution.

    Returns the exact bounded admin payloads an operator would submit,
    with executed=false, the migration-013 warning and the Massive
    requirement warning. No provider is constructed, nothing is written,
    nothing is scheduled.
    """
    from app.workers.shadow.campaign_planning import (
        CampaignPlanError,
        build_campaign_plan,
    )
    from app.workers.shadow.experiments import UnknownShadowExperimentError

    try:
        return build_campaign_plan(
            experiment_code=experiment_code,
            candidate_symbols=candidate_symbols,
            as_of_sessions=as_of_sessions,
            max_symbols_per_campaign=max_symbols_per_campaign,
            target_unique_symbols=target_unique_symbols,
            target_trigger_confirmed=target_trigger_confirmed,
            target_matured_outcomes=target_matured_outcomes,
            target_horizon=target_horizon,
            existing_evaluated_symbols=existing_evaluated_symbols,
            existing_unique_symbols=existing_unique_symbols,
            existing_trigger_confirmed=existing_trigger_confirmed,
            existing_matured_outcomes=existing_matured_outcomes,
        )
    except UnknownShadowExperimentError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except CampaignPlanError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


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
