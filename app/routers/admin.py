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
