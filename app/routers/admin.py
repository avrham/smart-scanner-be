"""
Admin API endpoints for Smart Scanner
Write endpoints protected by worker token
"""

from fastapi import APIRouter, Depends, BackgroundTasks, Body, HTTPException, Query, Response
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
# Prospective-experiment analytical surface (read-only; audit-only allowlist).
# --------------------------------------------------------------------------- #
async def _paired_reconciliation(db, *, experiment_code, campaign_id, symbol,
                                 strategy_version, config_hash, min_snapshot_date,
                                 max_snapshot_date, limit):
    """Fetch candidate + control evaluation rows and paired outcome rows through
    the EXISTING frozen readers and reconcile them. Read-only."""
    from app.workers.shadow.evidence_review import fetch_evidence_records
    from app.paired_comparison import (
        CANDIDATE_STRATEGY, CONTROL_STRATEGY, reconcile_pairs)

    cand_filters = _evidence_filters(
        CANDIDATE_STRATEGY, experiment_code, strategy_version, None, config_hash,
        symbol, campaign_id, min_snapshot_date, max_snapshot_date,
        None, None, None, None, limit)
    ctrl_filters = _evidence_filters(
        CONTROL_STRATEGY, experiment_code, None, None, None,
        symbol, campaign_id, min_snapshot_date, max_snapshot_date,
        None, None, None, None, limit)
    cand_records = await fetch_evidence_records(cand_filters)
    ctrl_records = await fetch_evidence_records(ctrl_filters)
    outcome_rows = await _evidence_outcome_rows(cand_filters)
    return reconcile_pairs(cand_records, ctrl_records, outcome_rows), cand_filters


def _require_paired_audit_gate_selector(experiment_code, campaign_id, symbol,
                                        strategy_version, config_hash,
                                        min_snapshot_date, max_snapshot_date):
    if not any([experiment_code, campaign_id, strategy_version, config_hash,
                symbol, min_snapshot_date, max_snapshot_date]):
        raise HTTPException(
            status_code=422,
            detail="a cohort selector is required (experiment_code / campaign_id "
                   "/ strategy_version / config_hash / symbol / snapshot dates) — "
                   "the paired surface never scans all shadow history")


async def _paired_readiness_gate(db):
    if settings.AUDIT_ONLY_MODE:
        access = await _run_configured_access_check(db)
        if not access["ready_for_closeout_audit"]:
            raise HTTPException(status_code=409, detail={
                "error": "paired_surface_not_ready",
                "database_connection_mode": access.get("database_connection_mode"),
                "reasons": access["reasons"]})


@router.get("/shadow-cohort/paired-comparison")
async def shadow_cohort_paired_comparison(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    experiment_code: Optional[str] = None,
    campaign_id: Optional[str] = None,
    campaign_scope: str = "campaign",
    symbol: Optional[str] = None,
    strategy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    horizon: Optional[str] = None,
    decision_population: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
    cursor: int = 0,
    limit: int = 100,
):
    """Read-only, bounded, cursor-paginated paired candidate-vs-control dataset
    (`shadow_paired_comparison.v1`). Reuses the frozen evidence/outcome readers;
    constructs no provider and issues no mutation SQL. Never exposes secrets."""
    from app.paired_comparison import build_paired_comparison
    _require_paired_audit_gate_selector(experiment_code, campaign_id, symbol,
                                        strategy_version, config_hash,
                                        min_snapshot_date, max_snapshot_date)
    await _paired_readiness_gate(db)
    recon, _ = await _paired_reconciliation(
        db, experiment_code=experiment_code, campaign_id=campaign_id, symbol=symbol,
        strategy_version=strategy_version, config_hash=config_hash,
        min_snapshot_date=min_snapshot_date, max_snapshot_date=max_snapshot_date,
        limit=2000)
    return build_paired_comparison(
        recon, experiment_code=experiment_code or "", campaign_scope=campaign_scope,
        horizon=horizon, decision_population=decision_population,
        cursor=cursor, limit=limit)


@router.get("/shadow-cohort/paired-metrics")
async def shadow_cohort_paired_metrics(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    experiment_code: Optional[str] = None,
    campaign_id: Optional[str] = None,
    campaign_scope: str = "campaign",
    symbol: Optional[str] = None,
    strategy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    min_snapshot_date: Optional[str] = None,
    max_snapshot_date: Optional[str] = None,
):
    """Read-only symmetric candidate/control population counts + per-horizon
    effect sizes and (min-sample-gated) paired statistics
    (`shadow_paired_metrics.v1`)."""
    from app.paired_comparison import build_paired_metrics
    _require_paired_audit_gate_selector(experiment_code, campaign_id, symbol,
                                        strategy_version, config_hash,
                                        min_snapshot_date, max_snapshot_date)
    await _paired_readiness_gate(db)
    recon, _ = await _paired_reconciliation(
        db, experiment_code=experiment_code, campaign_id=campaign_id, symbol=symbol,
        strategy_version=strategy_version, config_hash=config_hash,
        min_snapshot_date=min_snapshot_date, max_snapshot_date=max_snapshot_date,
        limit=2000)
    return build_paired_metrics(
        recon, experiment_code=experiment_code or "", campaign_scope=campaign_scope)


def _parse_symbols(symbols: Optional[str]) -> List[str]:
    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    if not syms:
        raise HTTPException(status_code=422,
                            detail="symbols is required (comma-separated; bounded)")
    syms = list(dict.fromkeys(syms))
    if len(syms) > 100:
        raise HTTPException(status_code=422, detail="at most 100 symbols per request")
    return syms


async def _fetch_local_readiness(db, syms):
    """Read-only local aggregates: daily_bars (always) + market_bars_4h (graceful
    — returns fourh_rows=None when the 4H relation is absent / not readable, so
    the v2 builder reports unknown_no_local_storage rather than erroring).
    Constructs no provider; issues no mutation."""
    daily = await db.fetch(
        """
        SELECT symbol, COUNT(*)::int AS daily_bars,
               MIN(trading_date) AS oldest, MAX(trading_date) AS latest,
               COUNT(DISTINCT date_trunc('month', trading_date))::int AS month_groups,
               COUNT(DISTINCT date_trunc('week', trading_date))::int AS week_groups
        FROM daily_bars WHERE symbol = ANY($1::text[]) GROUP BY symbol
        """, syms)
    daily_rows = [dict(r) for r in daily]
    fourh_rows = None
    try:
        fh = await db.fetch(
            """
            SELECT symbol,
                   COUNT(*) FILTER (WHERE is_completed)::int AS completed_4h_bars,
                   MIN(bar_end) FILTER (WHERE is_completed) AS oldest_4h,
                   MAX(bar_end) FILTER (WHERE is_completed) AS latest_4h
            FROM market_bars_4h WHERE symbol = ANY($1::text[]) GROUP BY symbol
            """, syms)
        fourh_rows = [dict(r) for r in fh]
    except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError):
        fourh_rows = None  # local 4H store not present/readable yet
    return daily_rows, fourh_rows


@router.get("/shadow-cohort/prospective-readiness")
async def shadow_cohort_prospective_readiness(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    symbols: Optional[str] = None,
):
    """Read-only prospective history-readiness (`shadow_prospective_readiness.v2`).
    Reads LOCAL daily_bars + market_bars_4h only — four-state per timeframe;
    constructs no provider, calls no market-data API, issues no mutation. GET
    only (audit-only read-only method gate). Degrades gracefully to
    unknown_no_local_storage for 4H when that table is not yet present."""
    from datetime import datetime, timezone
    from app.prospective_readiness import build_prospective_readiness_v2
    syms = _parse_symbols(symbols)
    await _paired_readiness_gate(db)
    daily_rows, fourh_rows = await _fetch_local_readiness(db, syms)
    return build_prospective_readiness_v2(syms, daily_rows, fourh_rows,
                                          now=datetime.now(timezone.utc))


# --------------------------------------------------------------------------- #
# History-Warmup foundation (read-only; HISTORY_WARMUP_ONLY_MODE routes).
# No provider-backed execute route exists in this task.
# --------------------------------------------------------------------------- #
def _require_history_warmup_mode():
    if not settings.HISTORY_WARMUP_ONLY_MODE:
        raise HTTPException(status_code=404, detail="Not Found")


@router.get("/history-warmup/access-check")
async def history_warmup_access_check(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
):
    """Read-only proof the connected identity is the least-privilege
    history-warmer role in warmup-only mode. Constructs NO provider, calls no
    market-data API, issues no mutation. `history_warmup_access_check.v1`."""
    _require_history_warmup_mode()
    from app.history_warmup import run_history_warmup_access_check
    provider = (settings.MARKET_DATA_PROVIDER or "").lower()
    credential = bool((settings.MASSIVE_API_KEY or "").strip()) if provider == "massive" else False
    return await run_history_warmup_access_check(
        db, expected_role=(settings.HISTORY_WARMUP_EXPECTED_DB_ROLE or None),
        history_warmup_only_mode=settings.HISTORY_WARMUP_ONLY_MODE,
        scheduler_enabled=settings.ENABLE_SCHEDULER,
        provider_name=provider or None,
        provider_credential_configured=credential)


def _resolve_history_warmup_provider():
    """Obtain the configured provider through the existing abstraction. Tests
    monkeypatch THIS function to inject a deterministic fake provider — the
    fake is never selectable via production environment variables."""
    from app.providers import get_market_data_provider
    return get_market_data_provider()


def _resolved_warmup_min_interval() -> int:
    from app.maintenance_cooldown import resolve_min_interval_seconds
    return resolve_min_interval_seconds(
        settings.HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS,
        maintenance_only_mode=settings.HISTORY_WARMUP_ONLY_MODE,
        provider=settings.MARKET_DATA_PROVIDER)


async def _latest_history_warmup_run(db):
    """Most recent history_warmup_runs row (drives the persisted cooldown)."""
    return await db.fetchrow(
        "SELECT id, status, finished_at, updated_at, started_at, created_at "
        "FROM history_warmup_runs "
        "ORDER BY COALESCE(finished_at, updated_at, started_at, created_at) DESC "
        "LIMIT 1")


async def _latest_run_items(db, symbols):
    """Latest history_warmup_run_items row per symbol (for the retry plan).
    Returns {} when the table is absent (pre-migration-015 deploy)."""
    try:
        rows = await db.fetch(
            "SELECT DISTINCT ON (symbol) symbol, status, error_code, error_class, "
            "retryable, attempt FROM history_warmup_run_items "
            "WHERE symbol = ANY($1::text[]) ORDER BY symbol, created_at DESC", symbols)
    except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError):
        return {}
    return {r["symbol"]: dict(r) for r in rows}


async def _warmup_preflight_state(db, syms, *, now):
    """Recompute the live readiness v2 + preflight v2 (server-authoritative)."""
    from app.prospective_readiness import build_prospective_readiness_v2
    from app.history_warmup_execute import build_preflight_v2
    from app.maintenance_cooldown import compute_cooldown
    daily_rows, fourh_rows = await _fetch_local_readiness(db, syms)
    readiness = build_prospective_readiness_v2(syms, daily_rows, fourh_rows, now=now)
    latest_items = await _latest_run_items(db, syms)
    cooldown = compute_cooldown(await _latest_history_warmup_run(db),
                                min_interval_seconds=_resolved_warmup_min_interval(),
                                now=now)
    preflight = build_preflight_v2(
        readiness, latest_items, cooldown,
        max_batch=settings.HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH)
    return preflight, readiness, cooldown


async def _provider_cooldown(db, *, now):
    """Server-authoritative provider cooldown — derived ONLY from runs that had
    provider activity (provider_activity_state != 'none'), using
    last_provider_activity_at or provider_activity_started_at. A run that never
    reached provider activity (pre-provider crash) NEVER establishes cooldown;
    a request rejected before provider construction never creates activity."""
    from app.maintenance_cooldown import compute_cooldown
    from app.history_warmup_execute import provider_activity_reference
    row = None
    try:
        row = await db.fetchrow(
            "SELECT id, status, provider_activity_state, last_provider_activity_at, "
            "provider_activity_started_at FROM history_warmup_runs "
            "WHERE provider_activity_state <> 'none' "
            "ORDER BY COALESCE(last_provider_activity_at, provider_activity_started_at) "
            "DESC LIMIT 1")
    except (asyncpg.UndefinedColumnError, asyncpg.UndefinedTableError,
            asyncpg.InsufficientPrivilegeError):
        row = None
    ref = provider_activity_reference(dict(row)) if row else None
    synthetic = ({"id": row["id"], "status": row["status"], "finished_at": ref}
                 if ref else None)
    return compute_cooldown(synthetic,
                            min_interval_seconds=_resolved_warmup_min_interval(), now=now)


async def _load_universe(db, *, universe_id=None, universe_code=None,
                         universe_version=None, require_frozen=False):
    """Load a universe + ordered membership; recompute the hash from membership
    and (for frozen) prove it equals the pinned hash. Raises HTTPException."""
    from app.history_warmup_execute import compute_universe_hash, UNIVERSE_FROZEN
    if universe_id:
        urow = await db.fetchrow(
            "SELECT * FROM history_warmup_universes WHERE id = $1", universe_id)
    elif universe_code:
        urow = await db.fetchrow(
            "SELECT * FROM history_warmup_universes WHERE universe_code=$1 "
            "AND universe_version=$2", str(universe_code).upper(), int(universe_version or 1))
    else:
        raise HTTPException(status_code=422, detail={"error": "universe_selector_required"})
    if urow is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_universe"})
    rows = await db.fetch(
        "SELECT symbol, ordinal FROM history_warmup_universe_symbols "
        "WHERE universe_id=$1 ORDER BY ordinal", urow["id"])
    symbols = [r["symbol"] for r in rows]
    recomputed = compute_universe_hash(
        universe_code=urow["universe_code"], universe_version=urow["universe_version"],
        symbols_in_ordinal_order=symbols)
    if require_frozen and urow["status"] != UNIVERSE_FROZEN:
        raise HTTPException(status_code=409, detail={
            "error": "universe_not_frozen", "status": urow["status"]})
    if urow["status"] == UNIVERSE_FROZEN and recomputed != urow["universe_hash"]:
        raise HTTPException(status_code=409, detail={"error": "universe_membership_mismatch"})
    effective_hash = urow["universe_hash"] if urow["status"] == UNIVERSE_FROZEN else recomputed
    return {"universe_id": str(urow["id"]), "universe_code": urow["universe_code"],
            "universe_version": urow["universe_version"], "status": urow["status"],
            "symbol_count": urow["symbol_count"], "symbols": symbols,
            "universe_hash": effective_hash, "recomputed_hash": recomputed}


async def _execution_state(db, universe_id, *, now):
    """Latest RUNNING run for this universe: active (lease valid) vs abandoned
    (lease expired). Used to surface in-progress/abandoned state in preflight."""
    row = await db.fetchrow(
        "SELECT id, status, provider_activity_state, execution_lease_expires_at "
        "FROM history_warmup_runs WHERE universe_id=$1 AND status='running' "
        "ORDER BY created_at DESC LIMIT 1", universe_id)
    if row is None:
        return {"active": False, "abandoned": False, "run_id": None,
                "lease_expires_at": None, "provider_activity_state": None}
    lease = row["execution_lease_expires_at"]
    active = lease is not None and lease > now
    return {"active": active, "abandoned": not active, "run_id": str(row["id"]),
            "lease_expires_at": lease.isoformat() if lease else None,
            "provider_activity_state": row["provider_activity_state"]}


async def _warmup_preflight_state_v3(db, universe, *, now):
    """Live readiness v2 over the FROZEN universe's members + preflight v3."""
    from app.prospective_readiness import build_prospective_readiness_v2
    from app.history_warmup_execute import build_preflight_v3
    syms = universe["symbols"]
    daily_rows, fourh_rows = await _fetch_local_readiness(db, syms)
    readiness = build_prospective_readiness_v2(syms, daily_rows, fourh_rows, now=now)
    latest_items = await _latest_run_items(db, syms)
    cooldown = await _provider_cooldown(db, now=now)
    exec_state = await _execution_state(db, universe["universe_id"], now=now)
    preflight = build_preflight_v3(
        universe=universe, readiness=readiness, latest_items=latest_items,
        cooldown=cooldown, execution_state=exec_state,
        max_batch=settings.HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH)
    return preflight, readiness, cooldown, exec_state


@router.post("/history-warmup/universes")
async def history_warmup_create_universe(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    body: Any = Body(...),
):
    """Create (and optionally atomically freeze) one bounded warmup universe
    (`history_warmup_universe_create.v1`). Normalizes/dedupes symbols, computes
    the deterministic universe hash, pins it at freeze. No provider, no campaign,
    no strategy execution. The warmer role may write ONLY the universe tables."""
    _require_history_warmup_mode()
    import json as _json
    from app.history_warmup_execute import (
        UNIVERSE_CREATE_CONTRACT_VERSION, normalize_universe_symbols,
        compute_universe_hash, UNIVERSE_FROZEN, UNIVERSE_DRAFT, UniverseError)
    from app.prospective_readiness import THRESHOLDS_V2
    from app.history_warmup_execute import _sha
    if settings.ENABLE_SCHEDULER:
        raise HTTPException(status_code=409, detail={"error": "scheduler_enabled"})
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="request body must be a JSON object")
    if body.get("contract_version") != UNIVERSE_CREATE_CONTRACT_VERSION:
        raise HTTPException(status_code=422, detail={"error": "bad_contract_version"})
    code = str(body.get("universe_code") or "").strip().upper()
    if not code or not code.isascii() or len(code) > 64 or any(c.isspace() for c in code):
        raise HTTPException(status_code=422, detail={"error": "invalid_universe_code"})
    version = body.get("universe_version", 1)
    if not isinstance(version, int) or version < 1:
        raise HTTPException(status_code=422, detail={"error": "invalid_universe_version"})
    try:
        norm = normalize_universe_symbols(
            body.get("symbols"), max_symbols=settings.HISTORY_WARMUP_MAX_UNIVERSE_SYMBOLS)
    except UniverseError as exc:
        raise HTTPException(status_code=422, detail={"error": exc.code, "detail": exc.detail})
    symbols = norm["symbols"]
    freeze = bool(body.get("freeze", False))
    config_hash = _sha(THRESHOLDS_V2)
    uhash = compute_universe_hash(universe_code=code, universe_version=version,
                                  symbols_in_ordinal_order=symbols)
    if await db.fetchval("SELECT 1 FROM history_warmup_universes WHERE universe_code=$1 "
                         "AND universe_version=$2", code, version):
        raise HTTPException(status_code=409, detail={"error": "universe_already_exists"})
    # create draft + membership, then (optionally) freeze — all before the guard
    # trigger locks the membership.
    row = await db.fetchrow(
        "INSERT INTO history_warmup_universes(universe_code, universe_version, "
        "config_hash, status, symbol_count) VALUES($1,$2,$3,'draft',$4) RETURNING id",
        code, version, config_hash, len(symbols))
    universe_id = str(row["id"])
    for ordinal, sym in enumerate(symbols):
        await db.execute(
            "INSERT INTO history_warmup_universe_symbols(universe_id, symbol, ordinal) "
            "VALUES($1,$2,$3)", universe_id, sym, ordinal)
    status = UNIVERSE_DRAFT
    if freeze:
        await db.execute(
            "UPDATE history_warmup_universes SET status='frozen', universe_hash=$2, "
            "frozen_at=NOW(), updated_at=NOW() WHERE id=$1", universe_id, uhash)
        status = UNIVERSE_FROZEN
    return {
        "contract_version": UNIVERSE_CREATE_CONTRACT_VERSION,
        "universe_id": universe_id, "universe_code": code, "universe_version": version,
        "status": status, "symbol_count": len(symbols),
        "universe_hash": uhash if freeze else None, "config_hash": config_hash,
        "duplicates_removed": norm["duplicates_removed"], "symbols": symbols,
        "provider_called": False,
    }


@router.get("/history-warmup/preflight")
async def history_warmup_preflight(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    universe_id: Optional[str] = None,
    universe_code: Optional[str] = None,
    universe_version: Optional[int] = None,
    symbols: Optional[str] = None,
):
    """History-warmup preflight. Executable form (`history_warmup_preflight.v3`)
    requires a FROZEN universe (by `universe_id` or `universe_code`+
    `universe_version`) and loads its members server-side. A `symbols` list is a
    read-only, EXPLICITLY NON-EXECUTABLE preview. Provider-free, no mutation."""
    _require_history_warmup_mode()
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    if universe_id or universe_code:
        universe = await _load_universe(
            db, universe_id=universe_id, universe_code=universe_code,
            universe_version=universe_version, require_frozen=True)
        preflight, _r, _c, _e = await _warmup_preflight_state_v3(db, universe, now=now)
        return preflight
    # non-executable preview over an ad hoc bounded symbol list
    syms = _parse_symbols(symbols)
    preflight, _readiness, _cooldown = await _warmup_preflight_state(db, syms, now=now)
    preflight["executable"] = False
    preflight["preview"] = True
    preflight["detail"] = ("ad hoc preview only — freeze a universe and pass "
                           "universe_id to obtain an executable preflight")
    preflight.pop("next_batch", None)
    return preflight


@router.post("/history-warmup/execute")
async def history_warmup_execute(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    body: Any = Body(...),
):
    """The ONE bounded history-warmup mutation route (`history_warmup_execute.v2`).
    Server independently loads the FROZEN universe by universe_id and recomputes
    membership/readiness/batch. Single-symbol batch; advisory-locked; cooldown
    derived from PROVIDER ACTIVITY (fail-closed after any crash post-provider);
    idempotent by deterministic execution identity; crash-safe leases +
    reconciliation. Provider obtained via the existing abstraction only after all
    gates pass and called OUTSIDE any open transaction."""
    _require_history_warmup_mode()
    import asyncio
    import json as _json
    import uuid as _uuid
    from datetime import datetime, timezone, timedelta
    from app.history_warmup_execute import (
        EXECUTE_CONTRACT_VERSION, EXECUTE_RESULT_CONTRACT_VERSION,
        HISTORY_WARMUP_ADVISORY_LOCK_KEY, MODE_NORMAL, MODE_RETRY,
        FORBIDDEN_REQUEST_FIELDS, FOUR_HOUR_FETCH_CALENDAR_DAYS,
        DAILY_WARMUP_TARGET_SESSIONS, execution_identity, validate_execute_request,
        normalize_daily_bars, normalize_4h_bars, upsert_daily_bars, upsert_4h_bars,
        map_provider_error, PROVIDER_ACTIVITY_STARTED, PROVIDER_ACTIVITY_COMPLETED,
    )
    from app.maintenance_cooldown import (
        COOLDOWN_BLOCKING_REASON, COOLDOWN_UNDER_LOCK_REASON, retry_after_seconds)
    logger = logging.getLogger(__name__)
    if settings.ENABLE_SCHEDULER:
        raise HTTPException(status_code=409, detail={"error": "scheduler_enabled"})
    max_batch = settings.HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH
    lease_seconds = max(1, int(settings.HISTORY_WARMUP_EXECUTION_LEASE_SECONDS))
    now = datetime.now(timezone.utc)

    # ---- basic request shape validation (before any recompute) -------------- #
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="request body must be a JSON object")
    present = [f for f in FORBIDDEN_REQUEST_FIELDS if body.get(f) is not None]
    if present:
        raise HTTPException(status_code=422, detail={
            "error": "forbidden_request_fields", "fields": sorted(present)})
    if body.get("contract_version") != EXECUTE_CONTRACT_VERSION:
        raise HTTPException(status_code=422, detail={"error": "bad_contract_version"})
    mode = body.get("mode")
    if mode not in (MODE_NORMAL, MODE_RETRY):
        raise HTTPException(status_code=422, detail={"error": "bad_mode"})
    universe_id_req = body.get("universe_id")
    if not universe_id_req:
        raise HTTPException(status_code=422, detail={"error": "universe_id_required"})
    raw_symbols = body.get("symbols")
    if not isinstance(raw_symbols, list) or not raw_symbols:
        raise HTTPException(status_code=422, detail={"error": "symbols_required"})
    symbols = [str(s).strip().upper() for s in raw_symbols]
    if len(symbols) != len(set(symbols)):
        raise HTTPException(status_code=422, detail={"error": "duplicate_symbols"})
    if len(symbols) > max_batch:
        raise HTTPException(status_code=422, detail={
            "error": "batch_size_out_of_range", "max": max_batch})
    if body.get("limit") != len(symbols):
        raise HTTPException(status_code=422, detail={"error": "limit_must_equal_symbol_count"})

    # ---- load + independently validate the FROZEN universe (before provider) - #
    try:
        universe = await _load_universe(db, universe_id=universe_id_req, require_frozen=True)
    except ValueError:
        raise HTTPException(status_code=422, detail={"error": "invalid_universe_id"})
    universe_symbols = set(universe["symbols"])
    if any(s not in universe_symbols for s in symbols):
        raise HTTPException(status_code=422, detail={"error": "symbol_not_in_universe"})

    plan_hash = (body.get("readiness_manifest_hash") if mode == MODE_NORMAL
                 else body.get("retry_plan_hash"))
    identity = execution_identity(
        mode=mode, universe_id=universe["universe_id"], universe_hash=body.get("universe_hash"),
        config_hash=body.get("config_hash"), plan_hash=plan_hash,
        next_batch_hash=body.get("next_batch_hash"), symbols=symbols)

    def _cooldown_409(cd, *, reason):
        return HTTPException(status_code=409, detail={
            "error": reason,
            "detail": "provider request window not yet cleared — obtain a fresh "
                      "preflight after the cooldown elapses",
            "min_batch_interval_seconds": cd["min_interval_seconds"],
            "next_execution_not_before": cd["next_execution_not_before"],
            "cooldown_remaining_seconds": cd["cooldown_remaining_seconds"]},
            headers={"Retry-After": str(retry_after_seconds(cd))})

    def _result(run_id, status, tel, readiness_before, readiness_after, batch_id):
        symbol = symbols[0]
        def _tf(rd, tf):
            for s in rd["symbols"]:
                if s["symbol"] == symbol:
                    return s[tf]["state"]
            return None
        return {
            "contract_version": EXECUTE_RESULT_CONTRACT_VERSION, "status": status,
            "mode": mode, "run_id": run_id, "batch_identity": batch_id,
            "universe_id": universe["universe_id"], "symbols": symbols,
            "provider_request_count": tel["req_count"],
            "daily": {k: tel["daily"][k] for k in ("inserted", "updated", "unchanged", "completed_count")},
            "four_hour": {k: tel["four_hour"][k] for k in ("inserted", "updated", "unchanged", "completed_count")},
            "error": ({"code": tel["err_code"], "class": tel["err_class"],
                       "retryable": tel["retryable"]} if tel["err_code"] else None),
            "readiness_before": {
                "four_hour": _tf(readiness_before, "four_hour"),
                "both_ready": next((s["both_ready"] for s in readiness_before["symbols"]
                                    if s["symbol"] == symbol), None)},
            "readiness_after": {
                "four_hour": _tf(readiness_after, "four_hour"),
                "both_ready": next((s["both_ready"] for s in readiness_after["symbols"]
                                    if s["symbol"] == symbol), None),
                "combined_readiness_manifest_hash": readiness_after["combined_readiness_manifest_hash"]},
            "cooldown": {"min_batch_interval_seconds": _resolved_warmup_min_interval(),
                         "next_execution_not_before": (
                             datetime.now(timezone.utc)
                             + timedelta(seconds=_resolved_warmup_min_interval())).isoformat()},
        }

    async def _do_symbol(run_id, symbol, run_mode):
        """Set the durable PROVIDER-ACTIVITY marker (committed before any provider
        call -> fail-closed cooldown), call the injected provider OUTSIDE any open
        transaction, persist through canonical idempotent upserts, and finalize
        the run + run item. Returns bounded telemetry."""
        # (Part 6) durable provider-activity marker BEFORE the first provider call
        await db.execute(
            "UPDATE history_warmup_runs SET provider_activity_state=$2, "
            "provider_activity_started_at=NOW(), heartbeat_at=NOW(), updated_at=NOW() "
            "WHERE id=$1", run_id, PROVIDER_ACTIVITY_STARTED)
        provider = _resolve_history_warmup_provider()
        provider_name = getattr(provider, "name", None) or "unknown"
        spacing = max(0, int(settings.HISTORY_WARMUP_PROVIDER_REQUEST_SPACING_SECONDS))
        tel = {"req_count": 0,
               "daily": {"inserted": 0, "updated": 0, "unchanged": 0, "completed_count": 0},
               "four_hour": {"inserted": 0, "updated": 0, "unchanged": 0, "completed_count": 0},
               "err_code": None, "err_class": None, "retryable": False}
        daily_status = four_hour_status = "pending"
        started = datetime.now(timezone.utc)
        logger.info("[HWX] start run=%s identity=%s mode=%s symbol=%s", run_id, identity[:16], run_mode, symbol)
        try:
            frm = now.date() - timedelta(days=int(DAILY_WARMUP_TARGET_SESSIONS * 1.75))
            to = now.date()
            await db.execute("UPDATE history_warmup_runs SET provider_request_count_attempted"
                             "=provider_request_count_attempted+1, last_provider_activity_at=NOW(),"
                             " heartbeat_at=NOW() WHERE id=$1", run_id)
            raw_daily = await provider.get_daily_bars(symbol, str(frm), str(to))
            tel["req_count"] += 1
            tel["daily"] = await upsert_daily_bars(db, normalize_daily_bars(raw_daily, now=now),
                                                   source=provider_name)
            daily_status = "completed"
            if spacing:
                await asyncio.sleep(spacing)
            await db.execute("UPDATE history_warmup_runs SET provider_request_count_attempted"
                             "=provider_request_count_attempted+1, last_provider_activity_at=NOW(),"
                             " heartbeat_at=NOW() WHERE id=$1", run_id)
            payload = await provider.get_intraday_history(
                symbol, multiplier=4, timespan="hour", start=to - timedelta(days=FOUR_HOUR_FETCH_CALENDAR_DAYS), end=to)
            tel["req_count"] += 1
            tel["four_hour"] = await upsert_4h_bars(db, normalize_4h_bars(payload, symbol=symbol, now=now))
            four_hour_status = "completed"
            item_status = "completed"
        except Exception as exc:  # noqa: BLE001 - mapped to a bounded safe code
            tel["err_code"], tel["err_class"] = map_provider_error(exc)
            tel["retryable"] = tel["err_class"] == "retryable"
            item_status = "failed"
            daily_status = daily_status if daily_status == "completed" else "failed"
            four_hour_status = ("failed" if daily_status == "completed" else "skipped") \
                if four_hour_status != "completed" else "completed"
            logger.warning("[HWX] symbol failed run=%s symbol=%s code=%s class=%s exc=%s",
                           run_id, symbol, tel["err_code"], tel["err_class"], type(exc).__name__)
        await db.execute(
            """
            INSERT INTO history_warmup_run_items(
              run_id, symbol, attempt, mode, status, daily_status, four_hour_status,
              daily_rows_inserted, daily_rows_updated, daily_rows_unchanged,
              four_hour_rows_inserted, four_hour_rows_updated, four_hour_rows_unchanged,
              provider_request_count, error_code, error_class, retryable,
              execution_identity, started_at, finished_at, created_at, updated_at)
            VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NOW(),NOW(),NOW())
            ON CONFLICT (run_id, symbol, attempt) DO NOTHING
            """,
            run_id, symbol, run_mode, item_status, daily_status, four_hour_status,
            tel["daily"]["inserted"], tel["daily"]["updated"], tel["daily"]["unchanged"],
            tel["four_hour"]["inserted"], tel["four_hour"]["updated"], tel["four_hour"]["unchanged"],
            tel["req_count"], tel["err_code"], tel["err_class"], tel["retryable"], identity, started)
        run_status = "completed" if item_status == "completed" else "failed"
        await db.execute(
            "UPDATE history_warmup_runs SET status=$2, provider_activity_state=$3, "
            "processed_symbol_count=1, provider_request_count=$4, error_code=$5, "
            "error_message=$6, finished_at=NOW(), updated_at=NOW(), last_provider_activity_at=NOW(), "
            "cooldown_last_finished_at=NOW(), "
            "cooldown_next_not_before=NOW() + ($7 || ' seconds')::interval WHERE id=$1",
            run_id, run_status, PROVIDER_ACTIVITY_COMPLETED, tel["req_count"], tel["err_code"],
            (tel["err_class"] if tel["err_code"] else None), str(_resolved_warmup_min_interval()))
        tel["run_status"] = run_status
        return tel

    # ---- idempotency / lease short-circuits (before provider, before lock) --- #
    prior = await db.fetchrow(
        "SELECT id, status, reconciled, provider_activity_state, "
        "execution_lease_expires_at, requested_symbols FROM history_warmup_runs "
        "WHERE idempotency_key = $1", identity)
    if prior is not None and prior["status"] == "completed":
        return {"contract_version": EXECUTE_RESULT_CONTRACT_VERSION,
                "status": "reconciled_complete" if prior["reconciled"] else "already_applied",
                "mode": mode, "run_id": str(prior["id"]), "universe_id": universe["universe_id"],
                "batch_identity": body.get("next_batch_hash"), "symbols": symbols,
                "provider_request_count": 0,
                "detail": "identical batch already applied; obtain a fresh preflight"}
    if prior is not None and prior["status"] == "running":
        lease = prior["execution_lease_expires_at"]
        if lease is not None and lease > now:
            # active lease -> genuine in-progress (identical request)
            raise HTTPException(status_code=409, detail={
                "error": "history_warmup_execution_in_progress",
                "run_id": str(prior["id"]),
                "lease_expires_at": lease.isoformat()},
                headers={"Retry-After": str(max(1, int((lease - now).total_seconds())))})
        # ABANDONED run (lease expired): reconcile or re-drive under the lock.
        got_lock = await db.fetchval(
            "SELECT pg_try_advisory_lock($1)", HISTORY_WARMUP_ADVISORY_LOCK_KEY)
        if not got_lock:
            raise HTTPException(status_code=409, detail={
                "error": "history_warmup_execution_locked"})
        try:
            again = await db.fetchrow(
                "SELECT status, reconciled FROM history_warmup_runs WHERE id=$1", prior["id"])
            if again["status"] == "completed":
                return {"contract_version": EXECUTE_RESULT_CONTRACT_VERSION,
                        "status": "reconciled_complete" if again["reconciled"] else "already_applied",
                        "mode": mode, "run_id": str(prior["id"]),
                        "universe_id": universe["universe_id"], "symbols": symbols,
                        "provider_request_count": 0}
            raw_stored = prior["requested_symbols"]
            if isinstance(raw_stored, str):
                raw_stored = _json.loads(raw_stored)
            stored_syms = list(raw_stored) if raw_stored else list(symbols)
            pf_r, readiness_r, _c, _e = await _warmup_preflight_state_v3(db, universe, now=now)
            all_ready = all(
                next((s["both_ready"] for s in readiness_r["symbols"] if s["symbol"] == sym), False)
                for sym in stored_syms)
            if all_ready:
                # (Part 15) provable completion from local bars -> reconcile, no provider
                await db.execute(
                    "INSERT INTO history_warmup_run_items(run_id,symbol,attempt,mode,status,"
                    "daily_status,four_hour_status,provider_request_count,execution_identity,"
                    "started_at,finished_at,created_at,updated_at) VALUES($1,$2,1,$3,'completed',"
                    "'completed','completed',0,$4,NOW(),NOW(),NOW(),NOW()) "
                    "ON CONFLICT (run_id,symbol,attempt) DO NOTHING",
                    str(prior["id"]), stored_syms[0], mode, identity)
                await db.execute(
                    "UPDATE history_warmup_runs SET status='completed', reconciled=TRUE, "
                    "finished_at=NOW(), updated_at=NOW() WHERE id=$1", prior["id"])
                return {"contract_version": EXECUTE_RESULT_CONTRACT_VERSION,
                        "status": "reconciled_complete", "mode": mode,
                        "run_id": str(prior["id"]), "universe_id": universe["universe_id"],
                        "symbols": symbols, "provider_request_count": 0,
                        "detail": "abandoned run reconciled from persisted local bars; no provider call"}
            # not reconcilable -> provider cooldown (from THIS run's activity) gates re-drive
            cooldown = await _provider_cooldown(db, now=datetime.now(timezone.utc))
            if not cooldown["execution_allowed_by_cooldown"]:
                raise _cooldown_409(cooldown, reason=COOLDOWN_BLOCKING_REASON)
            # safe re-drive reusing the same run marker (fresh lease)
            await db.execute(
                "UPDATE history_warmup_runs SET execution_lease_expires_at=NOW() + ($2 || ' seconds')"
                "::interval, heartbeat_at=NOW(), updated_at=NOW() WHERE id=$1",
                prior["id"], str(lease_seconds))
            tel = await _do_symbol(str(prior["id"]), stored_syms[0], mode)
            pf_after, readiness_after, _c2, _e2 = await _warmup_preflight_state_v3(
                db, universe, now=datetime.now(timezone.utc))
            return _result(str(prior["id"]),
                           "executed" if tel["run_status"] == "completed" else "failed",
                           tel, readiness_r, readiness_after, body.get("next_batch_hash"))
        finally:
            await db.fetchval("SELECT pg_advisory_unlock($1)", HISTORY_WARMUP_ADVISORY_LOCK_KEY)

    # ---- NORMAL path: recompute live preflight v3 + strict validation -------- #
    preflight, readiness_before, cooldown, _exec = await _warmup_preflight_state_v3(db, universe, now=now)
    verdict = validate_execute_request(body, preflight, max_batch=max_batch)
    if not verdict["ok"]:
        reason = verdict["reason"]
        code = 409 if reason in ("no_next_batch", "mode_not_current_batch",
                                 "symbols_not_server_selected_batch", "universe_id_mismatch") else 422
        raise HTTPException(status_code=code, detail={
            "error": "history_warmup_validation_failed", "reason": reason})
    if not cooldown["execution_allowed_by_cooldown"]:
        raise _cooldown_409(cooldown, reason=COOLDOWN_BLOCKING_REASON)

    got_lock = await db.fetchval(
        "SELECT pg_try_advisory_lock($1)", HISTORY_WARMUP_ADVISORY_LOCK_KEY)
    if not got_lock:
        raise HTTPException(status_code=409, detail={
            "error": "history_warmup_execution_locked", "batch_identity": verdict["batch_identity"]})
    try:
        preflight2, readiness_before, cooldown2, _e2 = await _warmup_preflight_state_v3(
            db, universe, now=datetime.now(timezone.utc))
        verdict2 = validate_execute_request(body, preflight2, max_batch=max_batch)
        if not verdict2["ok"] or verdict2["execution_identity"] != identity:
            raise HTTPException(status_code=409, detail={
                "error": "history_warmup_plan_changed_under_lock", "reason": verdict2.get("reason")})
        if not cooldown2["execution_allowed_by_cooldown"]:
            raise _cooldown_409(cooldown2, reason=COOLDOWN_UNDER_LOCK_REASON)
        again = await db.fetchval(
            "SELECT status FROM history_warmup_runs WHERE idempotency_key = $1", identity)
        if again == "completed":
            return {"contract_version": EXECUTE_RESULT_CONTRACT_VERSION,
                    "status": "already_applied", "mode": mode, "symbols": symbols,
                    "universe_id": universe["universe_id"], "provider_request_count": 0}

        symbol = symbols[0]
        run_id = str(_uuid.uuid4())
        # DURABLE pre-provider run marker (idempotency_key + lease; activity=none)
        await db.execute(
            """
            INSERT INTO history_warmup_runs(
              id, mode, status, universe_id, universe_hash, readiness_manifest_hash,
              requested_symbols, requested_symbol_count, idempotency_key,
              provider_activity_state, execution_lease_expires_at, heartbeat_at,
              started_at, created_at, updated_at)
            VALUES($1,$2,'running',$3,$4,$5,$6::jsonb,$7,$8,'none',
                   NOW() + ($9 || ' seconds')::interval, NOW(), NOW(), NOW(), NOW())
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            run_id, mode, universe["universe_id"], preflight2["universe_hash"],
            verdict2["plan_hash"], _json.dumps(symbols), len(symbols), identity, str(lease_seconds))
        run_row = await db.fetchrow(
            "SELECT id FROM history_warmup_runs WHERE idempotency_key = $1", identity)
        run_id = str(run_row["id"])
        tel = await _do_symbol(run_id, symbol, mode)
        pf_after, readiness_after, _c3, _e3 = await _warmup_preflight_state_v3(
            db, universe, now=datetime.now(timezone.utc))
        return _result(run_id, "executed" if tel["run_status"] == "completed" else "failed",
                       tel, readiness_before, readiness_after, verdict2["batch_identity"])
    finally:
        await db.fetchval("SELECT pg_advisory_unlock($1)", HISTORY_WARMUP_ADVISORY_LOCK_KEY)


# --------------------------------------------------------------------------- #
# Incremental daily-history refresh (history_incremental_refresh.v1) — DISTINCT
# from the initial-depth warmup above. Applies even when a symbol is already
# launch_ready; advances local daily_bars from each symbol's latest completed
# session through the latest safely-completed US session. Shares the SAME
# advisory lock, provider abstraction, cooldown and daily-bar persistence path
# as initial warmup (never a second provider client / rate limiter); bookkept
# in history_warmup_runs (mode='incremental') only — never
# history_warmup_run_items, whose mode CHECK constraint would reject it.
# --------------------------------------------------------------------------- #
async def _resolve_incremental_scope(db, *, registration_id, campaign_id, universe_id, symbols):
    """Explicit bounded scope ONLY — never a default-all-symbols query.
    Exactly one selector, priority: registration_id > campaign_id >
    universe_id > symbols. Returns (raw_symbols, source_detail)."""
    provided = [k for k, v in (
        ("registration_id", registration_id), ("campaign_id", campaign_id),
        ("universe_id", universe_id), ("symbols", symbols)) if v]
    if not provided:
        raise HTTPException(status_code=422, detail={
            "error": "explicit_scope_required",
            "detail": "one of registration_id, campaign_id, universe_id, symbols is required"})
    if registration_id:
        row = await db.fetchrow(
            "SELECT universe_id FROM prospective_campaign_registrations WHERE id=$1",
            registration_id)
        if row is None:
            raise HTTPException(status_code=404, detail={"error": "unknown_registration"})
        universe = await _load_universe(db, universe_id=str(row["universe_id"]))
        return universe["symbols"], {"source": "registration_id", "registration_id": str(registration_id),
                                     "universe_id": universe["universe_id"]}
    if campaign_id:
        row = await db.fetchrow(
            "SELECT universe_id FROM prospective_campaign_registrations WHERE campaign_id=$1",
            campaign_id)
        if row is None:
            raise HTTPException(status_code=404, detail={"error": "unknown_campaign"})
        universe = await _load_universe(db, universe_id=str(row["universe_id"]))
        return universe["symbols"], {"source": "campaign_id", "campaign_id": str(campaign_id),
                                     "universe_id": universe["universe_id"]}
    if universe_id:
        universe = await _load_universe(db, universe_id=universe_id)
        return universe["symbols"], {"source": "universe_id", "universe_id": universe["universe_id"]}
    raw = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    return raw, {"source": "symbols"}


async def _latest_local_daily_by_symbol(db, syms):
    rows = await db.fetch(
        "SELECT symbol, MAX(trading_date) AS latest FROM daily_bars "
        "WHERE symbol = ANY($1::text[]) GROUP BY symbol", syms)
    return {r["symbol"]: r["latest"] for r in rows}


def _latest_4h_maps(fourh_rows):
    """From _fetch_local_readiness 4H aggregates → (latest bar_end by symbol,
    latest ET session_date by symbol). Empty maps when the 4H store is
    absent/unreadable (→ v2 classifies those symbols unverifiable, never
    silently current)."""
    from zoneinfo import ZoneInfo
    end_by: Dict[str, Any] = {}
    session_by: Dict[str, Any] = {}
    for r in (fourh_rows or []):
        le = r.get("latest_4h")
        end_by[r["symbol"]] = le
        session_by[r["symbol"]] = (le.astimezone(ZoneInfo("America/New_York")).date()
                                   if le is not None else None)
    return end_by, session_by


@router.get("/history-warmup/incremental/preflight")
async def history_warmup_incremental_preflight(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    registration_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    universe_id: Optional[str] = None,
    symbols: Optional[str] = None,
    contract_version: Optional[str] = None,
):
    """Read-only incremental-refresh preflight. Explicit bounded scope only —
    no default-all-symbols path. Provider-free, no mutation. Pass
    ``contract_version=history_incremental_refresh.v2`` for the daily+4H
    preflight (three separate axes: history depth, daily freshness, 4H
    freshness); default is the v1 daily-only preflight (back-compat)."""
    _require_history_warmup_mode()
    from datetime import datetime, timezone
    from app.history_warmup_execute import (
        UniverseError, normalize_universe_symbols, build_incremental_preflight,
        build_incremental_preflight_v2, INCREMENTAL_REFRESH_CONTRACT_VERSION_V2)
    from app.prospective_session import resolve_latest_completed_session
    from app.prospective_readiness import build_prospective_readiness_v2
    now = datetime.now(timezone.utc)
    raw_symbols, scope = await _resolve_incremental_scope(
        db, registration_id=registration_id, campaign_id=campaign_id,
        universe_id=universe_id, symbols=symbols)
    try:
        norm = normalize_universe_symbols(
            raw_symbols, max_symbols=settings.HISTORY_WARMUP_MAX_UNIVERSE_SYMBOLS)
    except UniverseError as exc:
        raise HTTPException(status_code=422, detail={"error": exc.code, "detail": exc.detail})
    target_session = resolve_latest_completed_session(now)
    latest_by_symbol = await _latest_local_daily_by_symbol(db, norm["symbols"])
    daily_rows, fourh_rows = await _fetch_local_readiness(db, norm["symbols"])
    readiness = build_prospective_readiness_v2(norm["symbols"], daily_rows, fourh_rows, now=now)
    depth_ready = {s["symbol"]: bool(s.get("both_ready"))
                   for s in readiness.get("symbols", [])}
    cooldown = await _provider_cooldown(db, now=now)
    if contract_version == INCREMENTAL_REFRESH_CONTRACT_VERSION_V2:
        end_by, session_by = _latest_4h_maps(fourh_rows)
        preflight = build_incremental_preflight_v2(
            requested_symbol_count=len(raw_symbols), normalized_symbols=norm["symbols"],
            duplicates_removed=norm["duplicates_removed"], latest_daily_by_symbol=latest_by_symbol,
            latest_4h_end_by_symbol=end_by, latest_4h_session_by_symbol=session_by,
            target_session=target_session, depth_ready_by_symbol=depth_ready, cooldown=cooldown,
            max_batch=settings.HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH,
            provider_rate_limit_per_minute=settings.MASSIVE_REQUESTS_PER_MINUTE, now=now)
    else:
        preflight = build_incremental_preflight(
            requested_symbol_count=len(raw_symbols), normalized_symbols=norm["symbols"],
            duplicates_removed=norm["duplicates_removed"], latest_by_symbol=latest_by_symbol,
            target_session=target_session, depth_ready_by_symbol=depth_ready,
            cooldown=cooldown, max_batch=settings.HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH,
            provider_rate_limit_per_minute=settings.MASSIVE_REQUESTS_PER_MINUTE)
    preflight["scope"] = scope
    return preflight


async def history_incremental_refresh_execute_service(db, *, body, now=None):
    """INTERNAL bounded incremental-refresh service (no HTTP, no worker token, no
    mode gate). ONE symbol per call (matches the single-symbol-per-execute
    granularity); server independently recomputes the target session + per-symbol
    gap; advisory-locked (SAME lock as initial warmup — only one history-warmup
    execution of ANY kind runs at a time on this Machine); idempotent by
    deterministic (symbol, latest_local_session, target_completed_session)
    identity; provider obtained via the existing abstraction only after all gates
    pass and called OUTSIDE any open transaction.

    Operational-auth architecture: the automated path (the durable
    history-refresh worker in ``app.jobs.handlers.history_refresh_worker``) calls
    THIS function DIRECTLY against the DB as the least-privilege
    ``smart_scanner_history_warmer`` role — the ONLY component besides the
    history-warmup HTTP app that carries the provider credential. It never makes a
    self-HTTP call and never needs a human WORKER_TOKEN. The thin
    ``history_warmup_incremental_execute`` route below is only the operator/
    control-plane wrapper that adds the worker-token + warmup-mode gates. It still
    raises ``HTTPException`` (409 = transient/defer, 4xx = terminal) so both
    callers share one classification; the worker maps those to retry/terminal."""
    import json as _json
    import uuid as _uuid
    from datetime import date, datetime, timezone
    from app.history_warmup_execute import (
        INCREMENTAL_REFRESH_CONTRACT_VERSION, INCREMENTAL_EXECUTE_RESULT_CONTRACT_VERSION,
        INCREMENTAL_REFRESH_CONTRACT_VERSION_V2, INCREMENTAL_EXECUTE_RESULT_CONTRACT_VERSION_V2,
        HISTORY_WARMUP_ADVISORY_LOCK_KEY, MODE_INCREMENTAL, FORBIDDEN_REQUEST_FIELDS,
        UniverseError, normalize_universe_symbols, missing_trading_sessions,
        incremental_refresh_identity, normalize_daily_bars, upsert_daily_bars,
        classify_incremental_4h_state, incremental_4h_fetch_window,
        normalize_4h_bars, upsert_4h_bars, STATE_INCREMENTAL_REFRESH_NEEDED,
        map_provider_error, PROVIDER_ACTIVITY_STARTED, PROVIDER_ACTIVITY_COMPLETED,
        HISTORY_WARMUP_EXECUTION_IN_PROGRESS_REASON, HISTORY_WARMUP_EXECUTION_LOCKED_REASON,
    )
    from zoneinfo import ZoneInfo
    from app.prospective_session import resolve_latest_completed_session
    from app.maintenance_cooldown import (
        COOLDOWN_BLOCKING_REASON, COOLDOWN_UNDER_LOCK_REASON, retry_after_seconds)
    logger = logging.getLogger(__name__)
    if settings.ENABLE_SCHEDULER:
        raise HTTPException(status_code=409, detail={"error": "scheduler_enabled"})
    lease_seconds = max(1, int(settings.HISTORY_WARMUP_EXECUTION_LEASE_SECONDS))
    now = now or datetime.now(timezone.utc)

    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="request body must be a JSON object")
    present = [f for f in FORBIDDEN_REQUEST_FIELDS if body.get(f) is not None]
    if present:
        raise HTTPException(status_code=422, detail={
            "error": "forbidden_request_fields", "fields": sorted(present)})
    contract = body.get("contract_version")
    if contract not in (INCREMENTAL_REFRESH_CONTRACT_VERSION, INCREMENTAL_REFRESH_CONTRACT_VERSION_V2):
        raise HTTPException(status_code=422, detail={"error": "bad_contract_version"})
    is_v2 = contract == INCREMENTAL_REFRESH_CONTRACT_VERSION_V2
    result_contract = (INCREMENTAL_EXECUTE_RESULT_CONTRACT_VERSION_V2 if is_v2
                       else INCREMENTAL_EXECUTE_RESULT_CONTRACT_VERSION)
    symbol_raw = body.get("symbol")
    if not isinstance(symbol_raw, str) or not symbol_raw.strip():
        raise HTTPException(status_code=422, detail={"error": "symbol_required"})
    try:
        symbol = normalize_universe_symbols([symbol_raw], max_symbols=1)["symbols"][0]
    except UniverseError as exc:
        raise HTTPException(status_code=422, detail={"error": exc.code, "detail": exc.detail})

    target_str = body.get("target_completed_session")
    if not isinstance(target_str, str):
        raise HTTPException(status_code=422, detail={"error": "target_completed_session_required"})
    try:
        client_target = date.fromisoformat(target_str)
    except ValueError:
        raise HTTPException(status_code=422, detail={"error": "invalid_target_completed_session"})
    latest_str = body.get("latest_local_session")
    if latest_str is not None and not isinstance(latest_str, str):
        raise HTTPException(status_code=422, detail={"error": "invalid_latest_local_session"})
    client_latest = date.fromisoformat(latest_str) if latest_str else None
    # v2 additionally binds the observed latest local 4H session.
    latest_4h_str = body.get("latest_local_4h_session")
    if latest_4h_str is not None and not isinstance(latest_4h_str, str):
        raise HTTPException(status_code=422, detail={"error": "invalid_latest_local_4h_session"})
    client_latest_4h = date.fromisoformat(latest_4h_str) if latest_4h_str else None

    def _cooldown_409(cd, *, reason=COOLDOWN_BLOCKING_REASON):
        # `reason` distinguishes the pre-lock cooldown (COOLDOWN_BLOCKING_REASON)
        # from the under-advisory-lock re-check (COOLDOWN_UNDER_LOCK_REASON), the
        # same distinction the initial-warmup + maintenance routes make. Both are
        # in the authoritative HISTORY_WARMUP_TRANSIENT_409_REASONS set the durable
        # history-refresh worker waits on — never a queue-consuming failure.
        return HTTPException(status_code=409, detail={
            "error": reason,
            "detail": "provider request window not yet cleared — obtain a fresh "
                      "incremental preflight after the cooldown elapses",
            "cooldown_remaining_seconds": cd["cooldown_remaining_seconds"]},
            headers={"Retry-After": str(retry_after_seconds(cd))})

    # ---- idempotency FIRST (before any "is this still current" staleness
    # check) — an EXACT REPLAY of an already-completed request must return
    # already_applied even though the world (server_target/server_latest)
    # has since moved on; the identity is derived from the CLIENT-supplied
    # (symbol, latest, target) tuple, exactly what the original request used. #
    identity = incremental_refresh_identity(
        symbol=symbol, latest_local_session=client_latest, target_completed_session=client_target,
        refresh_contract_version=contract, latest_local_4h_session=client_latest_4h)
    prior = await db.fetchrow(
        "SELECT id, status, execution_lease_expires_at FROM history_warmup_runs "
        "WHERE idempotency_key=$1", identity)
    if prior is not None and prior["status"] == "completed":
        return {"contract_version": result_contract,
                "status": "already_applied", "mode": MODE_INCREMENTAL, "symbol": symbol,
                "run_id": str(prior["id"]), "target_completed_session": client_target.isoformat(),
                "provider_request_count": 0}
    if prior is not None and prior["status"] == "running":
        lease = prior["execution_lease_expires_at"]
        if lease is not None and lease > now:
            raise HTTPException(status_code=409, detail={
                "error": HISTORY_WARMUP_EXECUTION_IN_PROGRESS_REASON, "run_id": str(prior["id"]),
                "lease_expires_at": lease.isoformat()},
                headers={"Retry-After": str(max(1, int((lease - now).total_seconds())))})
        # lease expired (abandoned) — reconcile-or-redrive happens under the lock below

    # ---- NOT a replay of completed work: validate against CURRENT server
    # state before doing anything new. ------------------------------------ #
    server_target = resolve_latest_completed_session(now)
    if client_target != server_target:
        raise HTTPException(status_code=409, detail={
            "error": "stale_target_completed_session", "server_target": server_target.isoformat()})
    server_latest = await db.fetchval(
        "SELECT MAX(trading_date) FROM daily_bars WHERE symbol=$1", symbol)
    if client_latest != server_latest:
        raise HTTPException(status_code=409, detail={
            "error": "stale_latest_local_session",
            "server_latest_local_session": server_latest.isoformat() if server_latest else None})

    missing = missing_trading_sessions(server_latest, server_target)

    # v2: also assess 4H freshness. Server-side latest COMPLETED 4H session; a
    # refresh is needed when it lags the target. Validate the client's observed
    # 4H point too (symmetric with daily) so a stale view re-preflights.
    server_latest_4h_end = None
    server_latest_4h_session = None
    fourh_needs = False
    if is_v2:
        server_latest_4h_end = await db.fetchval(
            "SELECT MAX(bar_end) FROM market_bars_4h WHERE symbol=$1 AND is_completed", symbol)
        if server_latest_4h_end is not None:
            server_latest_4h_session = server_latest_4h_end.astimezone(
                ZoneInfo("America/New_York")).date()
        if client_latest_4h != server_latest_4h_session:
            raise HTTPException(status_code=409, detail={
                "error": "stale_latest_local_4h_session",
                "server_latest_local_4h_session": (server_latest_4h_session.isoformat()
                                                   if server_latest_4h_session else None)})
        fourh_needs = classify_incremental_4h_state(
            server_latest_4h_session, server_target) == STATE_INCREMENTAL_REFRESH_NEEDED

    if not missing and not fourh_needs:
        return {"contract_version": result_contract,
                "status": "no-op", "reason": "incremental_current", "mode": MODE_INCREMENTAL,
                "symbol": symbol, "target_completed_session": server_target.isoformat(),
                "latest_local_session": server_latest.isoformat() if server_latest else None,
                "provider_request_count": 0}

    cooldown = await _provider_cooldown(db, now=now)
    if not cooldown["execution_allowed_by_cooldown"]:
        raise _cooldown_409(cooldown)

    got_lock = await db.fetchval(
        "SELECT pg_try_advisory_lock($1)", HISTORY_WARMUP_ADVISORY_LOCK_KEY)
    if not got_lock:
        raise HTTPException(status_code=409, detail={"error": HISTORY_WARMUP_EXECUTION_LOCKED_REASON})
    try:
        again = await db.fetchrow(
            "SELECT id, status, execution_lease_expires_at FROM history_warmup_runs "
            "WHERE idempotency_key=$1", identity)
        if again is not None and again["status"] == "completed":
            return {"contract_version": result_contract,
                    "status": "already_applied", "mode": MODE_INCREMENTAL, "symbol": symbol,
                    "run_id": str(again["id"]), "target_completed_session": server_target.isoformat(),
                    "provider_request_count": 0}
        if again is not None and again["status"] == "running":
            lease = again["execution_lease_expires_at"]
            if lease is not None and lease > now:
                raise HTTPException(status_code=409, detail={
                    "error": HISTORY_WARMUP_EXECUTION_IN_PROGRESS_REASON, "run_id": str(again["id"])})
        cooldown2 = await _provider_cooldown(db, now=datetime.now(timezone.utc))
        if not cooldown2["execution_allowed_by_cooldown"]:
            # under the advisory lock: the SAME distinct reason the initial-warmup +
            # maintenance routes raise (this is the exact reason the live proof hit).
            raise _cooldown_409(cooldown2, reason=COOLDOWN_UNDER_LOCK_REASON)

        if again is None:
            run_id = str(_uuid.uuid4())
            await db.execute(
                """
                INSERT INTO history_warmup_runs(
                  id, mode, status, requested_symbols, requested_symbol_count,
                  idempotency_key, provider_activity_state, execution_lease_expires_at,
                  heartbeat_at, started_at, created_at, updated_at)
                VALUES($1,$2,'running',$3::jsonb,1,$4,'none',
                       NOW() + ($5 || ' seconds')::interval, NOW(), NOW(), NOW(), NOW())
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                run_id, MODE_INCREMENTAL, _json.dumps([symbol]), identity, str(lease_seconds))
            row = await db.fetchrow(
                "SELECT id FROM history_warmup_runs WHERE idempotency_key=$1", identity)
            run_id = str(row["id"])
        else:
            run_id = str(again["id"])
            await db.execute(
                "UPDATE history_warmup_runs SET execution_lease_expires_at=NOW() + "
                "($2 || ' seconds')::interval, heartbeat_at=NOW(), updated_at=NOW() WHERE id=$1",
                run_id, str(lease_seconds))

        await db.execute(
            "UPDATE history_warmup_runs SET provider_activity_state=$2, "
            "provider_activity_started_at=COALESCE(provider_activity_started_at, NOW()), "
            "heartbeat_at=NOW(), updated_at=NOW() WHERE id=$1", run_id, PROVIDER_ACTIVITY_STARTED)
        provider = _resolve_history_warmup_provider()
        provider_name = getattr(provider, "name", None) or "unknown"
        req_count = 0
        daily_tel = {"inserted": 0, "updated": 0, "unchanged": 0, "completed_count": 0}
        fourh_tel = {"inserted": 0, "updated": 0, "unchanged": 0, "completed_count": 0}
        err_code = err_class = None
        try:
            # (i) DAILY — only when there are missing sessions (skip refetch when
            # daily is already current; the v2 4H-only case does no daily work).
            if missing:
                frm = missing[0]
                to = missing[-1]
                await db.execute(
                    "UPDATE history_warmup_runs SET provider_request_count_attempted="
                    "provider_request_count_attempted+1, last_provider_activity_at=NOW(), "
                    "heartbeat_at=NOW() WHERE id=$1", run_id)
                raw_daily = await provider.get_daily_bars(symbol, str(frm), str(to))
                req_count += 1
                bars = normalize_daily_bars(raw_daily, now=now)
                # never persist a bar outside the requested missing-session set —
                # a provider returning a wider range than asked must not silently
                # rewrite arbitrary history via this bounded path.
                missing_set = set(missing)
                bars = [b for b in bars if b["symbol"] == symbol and b["trading_date"] in missing_set]
                daily_tel = await upsert_daily_bars(db, bars, source=provider_name)
            # (ii) 4H — v2 only, only when stale. Bounded window from the latest
            # local completed 4H bar_end to now; normalize drops the forming
            # bucket; upsert tolerates duplicate/out-of-order rows and never
            # persists incomplete/future bars.
            if is_v2 and fourh_needs:
                fh_from, fh_to = incremental_4h_fetch_window(
                    latest_4h_bar_end=server_latest_4h_end, now=now)
                await db.execute(
                    "UPDATE history_warmup_runs SET provider_request_count_attempted="
                    "provider_request_count_attempted+1, last_provider_activity_at=NOW(), "
                    "heartbeat_at=NOW() WHERE id=$1", run_id)
                payload = await provider.get_intraday_history(
                    symbol, multiplier=4, timespan="hour", start=fh_from, end=fh_to)
                req_count += 1
                fourh_tel = await upsert_4h_bars(db, normalize_4h_bars(payload, symbol=symbol, now=now))
            run_status = "completed"
        except Exception as exc:  # noqa: BLE001 - mapped to a bounded safe code
            err_code, err_class = map_provider_error(exc)
            run_status = "failed"
            logger.warning("[HWI] symbol failed run=%s symbol=%s code=%s class=%s exc=%s",
                           run_id, symbol, err_code, err_class, type(exc).__name__)
        await db.execute(
            "UPDATE history_warmup_runs SET status=$2, provider_activity_state=$3, "
            "processed_symbol_count=1, provider_request_count=$4, error_code=$5, "
            "error_message=$6, finished_at=NOW(), updated_at=NOW(), last_provider_activity_at=NOW(), "
            "cooldown_last_finished_at=NOW(), "
            "cooldown_next_not_before=NOW() + ($7 || ' seconds')::interval WHERE id=$1",
            run_id, run_status, PROVIDER_ACTIVITY_COMPLETED, req_count, err_code,
            err_class, str(_resolved_warmup_min_interval()))
        new_latest = await db.fetchval(
            "SELECT MAX(trading_date) FROM daily_bars WHERE symbol=$1", symbol)
        new_latest_4h = None
        if is_v2:
            new_latest_4h = await db.fetchval(
                "SELECT MAX(bar_end) FROM market_bars_4h WHERE symbol=$1 AND is_completed", symbol)
        result = {
            "contract_version": result_contract,
            "status": "executed" if run_status == "completed" else "failed",
            "mode": MODE_INCREMENTAL, "run_id": run_id, "symbol": symbol,
            "target_completed_session": server_target.isoformat(),
            "latest_local_session_before": server_latest.isoformat() if server_latest else None,
            "latest_local_session_after": new_latest.isoformat() if new_latest else None,
            "requested_missing_sessions": [d.isoformat() for d in missing],
            "provider_request_count": req_count,
            "daily": daily_tel,
            "error": ({"code": err_code, "class": err_class} if err_code else None),
        }
        if is_v2:
            result["four_hour"] = fourh_tel
            result["four_hour_refresh_needed"] = fourh_needs
            result["latest_local_4h_before"] = (
                server_latest_4h_end.isoformat() if server_latest_4h_end else None)
            result["latest_local_4h_after"] = new_latest_4h.isoformat() if new_latest_4h else None
        return result
    finally:
        await db.fetchval("SELECT pg_advisory_unlock($1)", HISTORY_WARMUP_ADVISORY_LOCK_KEY)


@router.post("/history-warmup/incremental/execute")
async def history_warmup_incremental_execute(
    _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db),
    body: Any = Body(...),
):
    """Operator/control-plane HTTP wrapper (worker-token + warmup-mode gated)
    around ``history_incremental_refresh_execute_service``. The automated
    daily-pipeline path does NOT use this route — the durable history-refresh
    worker calls the service directly, so automation never depends on a
    human-held WORKER_TOKEN or a self-HTTP call between Fly apps."""
    _require_history_warmup_mode()
    return await history_incremental_refresh_execute_service(db, body=body)


# --------------------------------------------------------------------------- #
# Prospective campaign (PROSPECTIVE_CAMPAIGN_ONLY_MODE). Local-data-only,
# provider-free frozen-universe candidate/control evaluation. Reuses the pure
# shadow runner via a local-history shim; creates NO outcomes.
# --------------------------------------------------------------------------- #
def _require_prospective_mode():
    if not settings.PROSPECTIVE_CAMPAIGN_ONLY_MODE:
        raise HTTPException(status_code=404, detail="Not Found")


async def _prospective_readiness(db, universe, *, now):
    from app.prospective_readiness import build_prospective_readiness_v2
    daily_rows, fourh_rows = await _fetch_local_readiness(db, universe["symbols"])
    return build_prospective_readiness_v2(universe["symbols"], daily_rows, fourh_rows, now=now)


async def _campaign_counts(db, run_id):
    import app.prospective_campaign as pc
    row = await db.fetchrow(
        "SELECT (SELECT count(*) FROM strategy_shadow_run_pairs WHERE run_id=$1)::int AS pairs, "
        "(SELECT count(*) FROM strategy_shadow_evaluations e JOIN strategy_shadow_run_pairs rp "
        "ON rp.pair_id=e.pair_id WHERE rp.run_id=$1)::int AS evals, "
        "(SELECT count(*) FROM strategy_shadow_evaluations e JOIN strategy_shadow_run_pairs rp "
        "ON rp.pair_id=e.pair_id WHERE rp.run_id=$1 AND e.arm_code=$2)::int AS cand, "
        "(SELECT count(*) FROM strategy_shadow_evaluations e JOIN strategy_shadow_run_pairs rp "
        "ON rp.pair_id=e.pair_id WHERE rp.run_id=$1 AND e.arm_code=$3)::int AS ctrl",
        run_id, pc.CANDIDATE_ARM_CODE, pc.CONTROL_ARM_CODE)
    return dict(row)


@router.get("/prospective/access-check")
async def prospective_access_check(
    _: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db)):
    """prospective_access_check.v1 — provider-free privilege verdict."""
    _require_prospective_mode()
    import app.prospective_campaign as pc
    identity = await db.fetchval("SELECT current_user")

    async def exists(rel):
        return (await db.fetchval("SELECT to_regclass($1)", rel)) is not None

    async def priv(rel, p):
        try:
            return bool(await db.fetchval("SELECT has_table_privilege($1,$2)", rel, p))
        except asyncpg.PostgresError:
            return False

    write_ok = {
        "prospective_campaign_registrations": await priv("public.prospective_campaign_registrations", "INSERT") if await exists("public.prospective_campaign_registrations") else False,
        "strategy_shadow_runs": await priv("public.strategy_shadow_runs", "INSERT"),
        "strategy_shadow_pairs": await priv("public.strategy_shadow_pairs", "INSERT"),
        "strategy_shadow_evaluations": await priv("public.strategy_shadow_evaluations", "INSERT"),
    }
    daily_w = await priv("public.daily_bars", "INSERT")
    fourh_w = await priv("public.market_bars_4h", "INSERT")
    outcome_w = await priv("public.strategy_shadow_pair_outcomes", "INSERT")
    delete_ok = await priv("public.strategy_shadow_pairs", "DELETE")
    reasons = []
    if identity != (settings.PROSPECTIVE_EXPECTED_DB_ROLE or None):
        reasons.append("database_identity_mismatch")
    if not settings.PROSPECTIVE_CAMPAIGN_ONLY_MODE:
        reasons.append("prospective_campaign_only_mode_disabled")
    if settings.ENABLE_SCHEDULER:
        reasons.append("scheduler_enabled")
    if not all(write_ok.values()):
        reasons.append(f"missing_campaign_writes:{sorted(k for k,v in write_ok.items() if not v)}")
    if daily_w or fourh_w:
        reasons.append("bar_writes_not_forbidden")
    if outcome_w:
        reasons.append("outcome_writes_not_forbidden")
    if delete_ok:
        reasons.append("delete_not_forbidden")
    return {
        "access_check_contract_version": pc.ACCESS_CHECK_CONTRACT_VERSION,
        "ready": not reasons, "reasons": reasons,
        "database_identity": identity,
        "expected_database_role": settings.PROSPECTIVE_EXPECTED_DB_ROLE or None,
        "prospective_campaign_only_mode": settings.PROSPECTIVE_CAMPAIGN_ONLY_MODE,
        "scheduler_enabled": settings.ENABLE_SCHEDULER,
        "provider_constructed": False,
        "provider_credential_configured": bool((settings.MASSIVE_API_KEY or "").strip()),
        "local_daily_store_available": await exists("public.daily_bars"),
        "local_4h_store_available": await exists("public.market_bars_4h"),
        "required_relations": ["daily_bars", "market_bars_4h",
                               "prospective_campaign_registrations",
                               "strategy_shadow_runs", "strategy_shadow_pairs",
                               "strategy_shadow_evaluations"],
        "required_privileges": {"select": ["daily_bars", "market_bars_4h"],
                                "insert_update": list(write_ok.keys())},
        "campaign_writes_allowed": bool(write_ok["strategy_shadow_runs"]),
        "pair_writes_allowed": bool(write_ok["strategy_shadow_pairs"]),
        "evaluation_writes_allowed": bool(write_ok["strategy_shadow_evaluations"]),
        "registration_writes_allowed": bool(write_ok["prospective_campaign_registrations"]),
        "outcome_writes_forbidden": not outcome_w,
        "bar_writes_forbidden": not (daily_w or fourh_w),
        "delete_forbidden": not delete_ok,
    }


async def _prospective_preflight_state(db, universe_id, experiment_code, *, now):
    import app.prospective_campaign as pc
    from app.prospective_session import resolve_snapshot
    universe = await _load_universe(db, universe_id=universe_id, require_frozen=True)
    readiness = await _prospective_readiness(db, universe, now=now)
    snap = resolve_snapshot(now)
    reg_id = pc.registration_identity(
        experiment_code=experiment_code, universe_id=universe["universe_id"],
        universe_hash=universe["universe_hash"], history_config_hash=readiness["config_hash"],
        snapshot_session_date=snap["snapshot_session_date"])
    existing = await db.fetchrow(
        "SELECT id, status, campaign_run_id FROM prospective_campaign_registrations "
        "WHERE registration_identity = $1", reg_id)
    both_ready = readiness["both_ready_count"] == len(universe["symbols"])
    blocking = []
    if universe["status"] != "frozen":
        blocking.append("universe_not_frozen")
    if not both_ready:
        nr = [s["symbol"] for s in readiness["symbols"] if not s["both_ready"]]
        blocking.append(f"symbols_not_ready:{nr[:10]}")
    if experiment_code != settings.PROSPECTIVE_ALLOWED_EXPERIMENT_CODE:
        blocking.append("experiment_not_allowed")
    if settings.ENABLE_SCHEDULER:
        blocking.append("scheduler_enabled")
    return universe, readiness, snap, reg_id, existing, blocking


@router.get("/prospective/preflight")
async def prospective_preflight(
    _: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db),
    universe_id: Optional[str] = None, experiment_code: Optional[str] = None):
    """prospective_preflight.v1 — server independently loads + validates the
    frozen universe, recomputes hashes, resolves the completed snapshot session.
    Provider-free."""
    _require_prospective_mode()
    import app.prospective_campaign as pc
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    exp = experiment_code or settings.PROSPECTIVE_ALLOWED_EXPERIMENT_CODE
    if not universe_id:
        raise HTTPException(status_code=422, detail={"error": "universe_id_required"})
    universe, readiness, snap, reg_id, existing, blocking = await _prospective_preflight_state(
        db, universe_id, exp, now=now)
    return {
        "contract_version": pc.PREFLIGHT_CONTRACT_VERSION,
        "provider_called": False, "provider_constructed": False,
        "experiment_code": exp,
        "experiment_contract_version": pc.EXPERIMENT_CONTRACT_VERSION,
        "universe_id": universe["universe_id"], "universe_code": universe["universe_code"],
        "universe_version": universe["universe_version"], "universe_hash": universe["universe_hash"],
        "universe_status": universe["status"], "symbol_count": universe["symbol_count"],
        "history_config_hash": readiness["config_hash"],
        "history_readiness_manifest_hash": readiness["combined_readiness_manifest_hash"],
        "all_ready": readiness["both_ready_count"] == universe["symbol_count"],
        "both_ready_count": readiness["both_ready_count"],
        "snapshot_session_date": snap["snapshot_session_date"],
        "snapshot_cutoff_at": snap["snapshot_cutoff_at"],
        "market_calendar_version": snap["market_calendar_version"],
        "candidate_strategy_code": pc.CANDIDATE_STRATEGY_CODE,
        "candidate_strategy_version": pc.CANDIDATE_STRATEGY_VERSION,
        "candidate_signal_definition": pc.CANDIDATE_SIGNAL_DEFINITION,
        "candidate_allow_enter": pc.CANDIDATE_ALLOW_ENTER,
        "control_strategy_code": pc.CONTROL_STRATEGY_CODE,
        "control_strategy_version": pc.CONTROL_STRATEGY_VERSION,
        "registration_identity": reg_id,
        "existing_registration_id": str(existing["id"]) if existing else None,
        "existing_registration_status": existing["status"] if existing else None,
        "existing_campaign_run_id": str(existing["campaign_run_id"]) if existing and existing["campaign_run_id"] else None,
        "execution_available": not blocking,
        "blocking_reasons": blocking,
        "readiness": readiness,
    }


@router.post("/prospective/register")
async def prospective_register(
    _: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db),
    body: Any = Body(...)):
    """prospective_campaign_registration.v1 — persist the immutable prospective
    identity. Server owns strategy versions / allow_enter / snapshot. Idempotent."""
    _require_prospective_mode()
    import json as _json
    import app.prospective_campaign as pc
    from datetime import datetime, timezone, date as _date
    if settings.ENABLE_SCHEDULER:
        raise HTTPException(status_code=409, detail={"error": "scheduler_enabled"})
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="request body must be a JSON object")
    if body.get("contract_version") != pc.REGISTRATION_CONTRACT_VERSION:
        raise HTTPException(status_code=422, detail={"error": "bad_contract_version"})
    exp = body.get("experiment_code")
    if exp != settings.PROSPECTIVE_ALLOWED_EXPERIMENT_CODE:
        raise HTTPException(status_code=422, detail={"error": "experiment_not_allowed"})
    forbidden = [f for f in ("strategy_version", "candidate_strategy_version", "allow_enter",
                             "provider", "entry_price", "outcome_horizon", "symbols")
                 if body.get(f) is not None]
    if forbidden:
        raise HTTPException(status_code=422, detail={"error": "forbidden_request_fields", "fields": sorted(forbidden)})
    universe_id = body.get("universe_id")
    if not universe_id:
        raise HTTPException(status_code=422, detail={"error": "universe_id_required"})
    now = datetime.now(timezone.utc)
    universe, readiness, snap, reg_id, existing, blocking = await _prospective_preflight_state(
        db, universe_id, exp, now=now)
    # validate the client's pinned hashes/snapshot against fresh server values
    if body.get("universe_hash") != universe["universe_hash"]:
        raise HTTPException(status_code=409, detail={"error": "stale_universe_hash"})
    if body.get("history_config_hash") not in (None, readiness["config_hash"]):
        raise HTTPException(status_code=409, detail={"error": "stale_history_config_hash"})
    if body.get("history_readiness_manifest_hash") not in (None, readiness["combined_readiness_manifest_hash"]):
        raise HTTPException(status_code=409, detail={"error": "stale_history_manifest"})
    if body.get("snapshot_session_date") not in (None, snap["snapshot_session_date"]):
        raise HTTPException(status_code=409, detail={"error": "invalid_snapshot_session"})
    if blocking:
        raise HTTPException(status_code=409, detail={"error": "registration_blocked", "reasons": blocking})
    if existing is not None:
        return {"contract_version": pc.REGISTRATION_CONTRACT_VERSION,
                "status": "already_registered", "registration_id": str(existing["id"]),
                "registration_identity": reg_id, "detail": "identical registration exists"}
    row = await db.fetchrow(
        """
        INSERT INTO prospective_campaign_registrations(
          experiment_code, experiment_contract_version, universe_id, universe_code,
          universe_version, universe_hash, history_config_hash, history_readiness_manifest_hash,
          candidate_strategy_code, candidate_strategy_version, candidate_signal_definition,
          candidate_allow_enter, control_strategy_code, control_strategy_version,
          snapshot_session_date, snapshot_cutoff_at, market_calendar_version,
          registration_identity, status, created_at, updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,FALSE,$12,$13,$14,$15,$16,$17,'registered',NOW(),NOW())
        ON CONFLICT (registration_identity) DO NOTHING RETURNING id
        """,
        exp, pc.EXPERIMENT_CONTRACT_VERSION, universe["universe_id"], universe["universe_code"],
        universe["universe_version"], universe["universe_hash"], readiness["config_hash"],
        readiness["combined_readiness_manifest_hash"], pc.CANDIDATE_STRATEGY_CODE,
        pc.CANDIDATE_STRATEGY_VERSION, pc.CANDIDATE_SIGNAL_DEFINITION, pc.CONTROL_STRATEGY_CODE,
        pc.CONTROL_STRATEGY_VERSION,
        _date.fromisoformat(snap["snapshot_session_date"]),
        datetime.fromisoformat(snap["snapshot_cutoff_at"]),
        snap["market_calendar_version"], reg_id)
    if row is None:  # race: created between preflight and insert
        again = await db.fetchrow("SELECT id FROM prospective_campaign_registrations WHERE registration_identity=$1", reg_id)
        return {"contract_version": pc.REGISTRATION_CONTRACT_VERSION, "status": "already_registered",
                "registration_id": str(again["id"]), "registration_identity": reg_id}
    return {"contract_version": pc.REGISTRATION_CONTRACT_VERSION, "status": "registered",
            "registration_id": str(row["id"]), "registration_identity": reg_id,
            "snapshot_session_date": snap["snapshot_session_date"],
            "snapshot_cutoff_at": snap["snapshot_cutoff_at"],
            "universe_hash": universe["universe_hash"],
            "history_readiness_manifest_hash": readiness["combined_readiness_manifest_hash"]}


@router.post("/prospective/execute")
async def prospective_execute(
    _: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db),
    body: Any = Body(...)):
    """prospective_campaign_execute.v1 — create ONE campaign, 25 pairs, 50
    evaluations, 0 outcomes, using ONLY local bars. Advisory-locked + idempotent."""
    _require_prospective_mode()
    import json as _json
    import app.prospective_campaign as pc
    from datetime import datetime, timezone
    from app.prospective_local_provider import LocalHistoryProvider
    from app.workers.shadow.campaigns import plan_shadow_campaign, run_shadow_campaign
    logger = logging.getLogger(__name__)
    if settings.ENABLE_SCHEDULER:
        raise HTTPException(status_code=409, detail={"error": "scheduler_enabled"})
    if not isinstance(body, dict) or body.get("contract_version") != pc.EXECUTE_CONTRACT_VERSION:
        raise HTTPException(status_code=422, detail={"error": "bad_contract_version"})
    reg_pk = body.get("registration_id")
    if not reg_pk:
        raise HTTPException(status_code=422, detail={"error": "registration_id_required"})
    now = datetime.now(timezone.utc)
    lease_s = max(1, int(settings.PROSPECTIVE_EXECUTION_LEASE_SECONDS))

    reg = await db.fetchrow(
        "SELECT * FROM prospective_campaign_registrations WHERE id = $1", reg_pk)
    if reg is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_registration"})
    exec_id = pc.campaign_execution_identity(
        registration_identity_value=reg["registration_identity"], universe_hash=reg["universe_hash"],
        history_readiness_manifest_hash=reg["history_readiness_manifest_hash"],
        snapshot_session_date=reg["snapshot_session_date"].isoformat())

    async def _already(r):
        counts = await _campaign_counts(db, r["campaign_run_id"]) if r["campaign_run_id"] else {"pairs": r["pair_count"], "evals": r["candidate_evaluation_count"]+r["control_evaluation_count"], "cand": r["candidate_evaluation_count"], "ctrl": r["control_evaluation_count"]}
        return {"contract_version": pc.EXECUTE_CONTRACT_VERSION, "status": "already_applied",
                "registration_id": str(r["id"]), "campaign_id": str(r["campaign_id"]) if r["campaign_id"] else None,
                "campaign_run_id": str(r["campaign_run_id"]) if r["campaign_run_id"] else None,
                "pair_count": counts["pairs"], "candidate_evaluations": counts["cand"],
                "control_evaluations": counts["ctrl"], "outcomes": 0, "provider_request_count": 0}
    if reg["status"] == "completed":
        return await _already(reg)

    got_lock = await db.fetchval("SELECT pg_try_advisory_lock($1)", pc.PROSPECTIVE_ADVISORY_LOCK_KEY)
    if not got_lock:
        raise HTTPException(status_code=409, detail={"error": "prospective_campaign_execution_locked"})
    try:
        reg = await db.fetchrow("SELECT * FROM prospective_campaign_registrations WHERE id=$1", reg_pk)
        if reg["status"] == "completed":
            return await _already(reg)
        if reg["status"] == "executing" and reg["execution_lease_expires_at"] and reg["execution_lease_expires_at"] > now:
            raise HTTPException(status_code=409, detail={
                "error": "prospective_campaign_execution_in_progress",
                "registration_id": str(reg["id"]),
                "lease_expires_at": reg["execution_lease_expires_at"].isoformat()})
        # revalidate immutable identities against fresh server state
        universe = await _load_universe(db, universe_id=str(reg["universe_id"]), require_frozen=True)
        readiness = await _prospective_readiness(db, universe, now=now)
        if body.get("registration_identity") not in (None, reg["registration_identity"]):
            raise HTTPException(status_code=409, detail={"error": "registration_identity_mismatch"})
        if universe["universe_hash"] != reg["universe_hash"] or body.get("universe_hash") not in (None, reg["universe_hash"]):
            raise HTTPException(status_code=409, detail={"error": "stale_universe"})
        if readiness["combined_readiness_manifest_hash"] != reg["history_readiness_manifest_hash"] or body.get("history_readiness_manifest_hash") not in (None, reg["history_readiness_manifest_hash"]):
            raise HTTPException(status_code=409, detail={"error": "stale_history_manifest"})
        if body.get("snapshot_session_date") not in (None, reg["snapshot_session_date"].isoformat()):
            raise HTTPException(status_code=409, detail={"error": "invalid_snapshot_session"})
        if readiness["both_ready_count"] != len(universe["symbols"]):
            raise HTTPException(status_code=409, detail={"error": "history_not_ready"})

        expected = len(universe["symbols"])
        # crash recovery: a prior campaign already fully persisted -> reconcile
        if reg["campaign_run_id"]:
            c = await _campaign_counts(db, reg["campaign_run_id"])
            if c["pairs"] == expected and c["cand"] == expected and c["ctrl"] == expected:
                await db.execute(
                    "UPDATE prospective_campaign_registrations SET status='completed', "
                    "pair_count=$3, candidate_evaluation_count=$3, control_evaluation_count=$3, "
                    "campaign_execution_identity=$2, finished_at=NOW(), updated_at=NOW() WHERE id=$1",
                    reg_pk, exec_id, expected)
                reg = await db.fetchrow("SELECT * FROM prospective_campaign_registrations WHERE id=$1", reg_pk)
                return await _already(reg)

        # mark executing + lease
        await db.execute(
            "UPDATE prospective_campaign_registrations SET status='executing', "
            "campaign_execution_identity=$2, execution_lease_expires_at=NOW() + ($3||' seconds')::interval, "
            "updated_at=NOW() WHERE id=$1", reg_pk, exec_id, str(lease_s))

        # build the local-history shim (NO provider) + run the reused campaign
        shim = LocalHistoryProvider(
            snapshot_session_date=reg["snapshot_session_date"],
            snapshot_cutoff_at=reg["snapshot_cutoff_at"])
        plan = plan_shadow_campaign(
            experiment_code=reg["experiment_code"], symbols=universe["symbols"],
            max_symbols=len(universe["symbols"]),
            as_of_date=reg["snapshot_session_date"].isoformat())
        started = datetime.now(timezone.utc)
        logger.info("[PROS] execute reg=%s exp=%s symbols=%d snapshot=%s",
                    reg_pk, reg["experiment_code"], len(universe["symbols"]),
                    reg["snapshot_session_date"])
        summary = await run_shadow_campaign(shim, plan, now_utc=reg["snapshot_cutoff_at"])
        run_ids = [r["run_id"] for r in (summary.get("runs") or []) if r.get("run_id")]
        # one chunk of 25 -> one run
        run_id = run_ids[0] if run_ids else (summary.get("runs") or [{}])[0].get("run_id")
        counts = await _campaign_counts(db, run_id) if run_id else {"pairs": 0, "evals": 0, "cand": 0, "ctrl": 0}
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()

        if not (counts["pairs"] == expected and counts["cand"] == expected and counts["ctrl"] == expected):
            await db.execute(
                "UPDATE prospective_campaign_registrations SET status='failed', "
                "error_code=$2, error_message=$3, updated_at=NOW() WHERE id=$1",
                reg_pk, "campaign_count_mismatch",
                f"pairs={counts['pairs']} cand={counts['cand']} ctrl={counts['ctrl']} expected={expected}")
            raise HTTPException(status_code=500, detail={
                "error": "prospective_campaign_incomplete", "counts": counts, "expected": expected})
        outcomes = await db.fetchval("SELECT count(*)::int FROM strategy_shadow_pair_outcomes")
        await db.execute(
            "UPDATE prospective_campaign_registrations SET status='completed', "
            "campaign_id=$2, campaign_run_id=$3, pair_count=$5, candidate_evaluation_count=$5, "
            "control_evaluation_count=$5, telemetry=$4::jsonb, finished_at=NOW(), "
            "updated_at=NOW(), execution_lease_expires_at=NULL WHERE id=$1",
            reg_pk, plan.get("campaign_id"), run_id,
            _json.dumps({"duration_s": round(elapsed, 2), "provider": shim.name,
                         "daily_reads": shim.daily_reads, "intraday_reads": shim.intraday_reads}),
            expected)
        logger.info("[PROS] done reg=%s run=%s pairs=%d cand=%d ctrl=%d outcomes=%d dur=%.2f",
                    reg_pk, run_id, counts["pairs"], counts["cand"], counts["ctrl"], outcomes, elapsed)
        return {"contract_version": pc.EXECUTE_CONTRACT_VERSION, "status": "executed",
                "registration_id": str(reg_pk), "campaign_id": str(plan.get("campaign_id")),
                "campaign_run_id": str(run_id), "pair_count": counts["pairs"],
                "candidate_evaluations": counts["cand"], "control_evaluations": counts["ctrl"],
                "outcomes": int(outcomes or 0), "provider_request_count": 0,
                "provider_constructed": False, "duration_seconds": round(elapsed, 2)}
    finally:
        await db.fetchval("SELECT pg_advisory_unlock($1)", pc.PROSPECTIVE_ADVISORY_LOCK_KEY)


@router.get("/prospective/audit")
async def prospective_audit(
    _: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db),
    registration_id: Optional[str] = None):
    """prospective_campaign_audit.v1 — read-only reconciliation + descriptive
    candidate/control signal distributions. provider_called=false, outcomes=0."""
    _require_prospective_mode()
    import app.prospective_campaign as pc
    if not registration_id:
        reg = await db.fetchrow("SELECT * FROM prospective_campaign_registrations ORDER BY created_at DESC LIMIT 1")
    else:
        reg = await db.fetchrow("SELECT * FROM prospective_campaign_registrations WHERE id=$1", registration_id)
    if reg is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_registration"})
    run_id = reg["campaign_run_id"]
    counts = await _campaign_counts(db, run_id) if run_id else {"pairs": 0, "evals": 0, "cand": 0, "ctrl": 0}
    cand_rows = ctrl_rows = []
    if run_id:
        cand_rows = await db.fetch(
            "SELECT e.verdict, e.score, e.details_snapshot, p.symbol FROM strategy_shadow_evaluations e "
            "JOIN strategy_shadow_run_pairs rp ON rp.pair_id=e.pair_id "
            "JOIN strategy_shadow_pairs p ON p.id=e.pair_id "
            "WHERE rp.run_id=$1 AND e.arm_code=$2", run_id, pc.CANDIDATE_ARM_CODE)
        ctrl_rows = await db.fetch(
            "SELECT e.verdict, e.score, p.symbol FROM strategy_shadow_evaluations e "
            "JOIN strategy_shadow_run_pairs rp ON rp.pair_id=e.pair_id "
            "JOIN strategy_shadow_pairs p ON p.id=e.pair_id "
            "WHERE rp.run_id=$1 AND e.arm_code=$2", run_id, pc.CONTROL_ARM_CODE)
    cand_decisions, cand_readiness, watch_reasons = {}, {}, {}
    setup_n = trig_n = pre_entry_n = rollout_blocked_n = score_cov = 0
    cand_signal_syms = set()
    fourh_states = {}
    import json as _json
    def _details(v):
        if isinstance(v, str):
            try:
                return _json.loads(v)
            except (ValueError, TypeError):
                return {}
        return v or {}
    for r in cand_rows:
        cand_decisions[r["verdict"]] = cand_decisions.get(r["verdict"], 0) + 1
        sig = pc.candidate_signal_fields(_details(r["details_snapshot"]))
        cand_readiness[sig["readiness_status"]] = cand_readiness.get(sig["readiness_status"], 0) + 1
        setup_n += 1 if sig["setup_present"] else 0
        trig_n += 1 if sig["trigger_confirmed"] else 0
        if sig["enter_eligible_without_rollout_gate"]:
            pre_entry_n += 1; cand_signal_syms.add(r["symbol"])
        rollout_blocked_n += 1 if sig["rollout_blocked"] else 0
        score_cov += 1 if r["score"] is not None else 0
        fs = sig.get("four_hour_state"); fourh_states[fs] = fourh_states.get(fs, 0) + 1
        if r["verdict"] == "WATCH":
            for wr in (sig["waiting_reasons"] or ["unspecified"]):
                watch_reasons[wr] = watch_reasons.get(wr, 0) + 1
    ctrl_decisions = {}
    ctrl_signal_syms = set()
    for r in ctrl_rows:
        ctrl_decisions[r["verdict"]] = ctrl_decisions.get(r["verdict"], 0) + 1
        if r["verdict"] == "ENTER":
            ctrl_signal_syms.add(r["symbol"])
    outcomes = await db.fetchval("SELECT count(*)::int FROM strategy_shadow_pair_outcomes")
    # ---- audit v2: durable-queue job / task / worker state -----------------
    from datetime import datetime as _dt, timezone as _tz
    _now = _dt.now(_tz.utc)
    job_block = None
    # The durable queue is optional infrastructure (migration 018). If the queue
    # tables are absent or unreadable, the audit degrades gracefully (job=None)
    # rather than failing — the shadow reconciliation above is authoritative.
    try:
        job = await db.fetchrow(
            "SELECT * FROM job_runs WHERE registration_id=$1 AND job_type='prospective_campaign' "
            "ORDER BY created_at DESC LIMIT 1", reg["id"])
    except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError):
        job = None
    if job is not None:
        tc = await db.fetchrow(
            "SELECT count(*)::int AS total,"
            " count(*) FILTER (WHERE status='queued')::int AS queued,"
            " count(*) FILTER (WHERE status IN ('leased','running'))::int AS running,"
            " count(*) FILTER (WHERE status='retryable')::int AS retryable,"
            " count(*) FILTER (WHERE status='succeeded')::int AS succeeded,"
            " count(*) FILTER (WHERE status='failed')::int AS failed,"
            " count(*) FILTER (WHERE status='cancelled')::int AS cancelled,"
            " count(*) FILTER (WHERE status IN ('leased','running') AND lease_expires_at > $2)::int AS active_leases,"
            " count(*) FILTER (WHERE status IN ('leased','running') AND lease_expires_at <= $2)::int AS expired_leases "
            "FROM job_tasks WHERE job_id=$1", job["id"], _now)
        last_event = await db.fetchrow(
            "SELECT event_type, safe_message, created_at FROM job_events WHERE job_id=$1 "
            "ORDER BY created_at DESC, id DESC LIMIT 1", job["id"])
        workers = await db.fetch(
            "SELECT worker_id, status, draining, last_heartbeat_at, current_task_id,"
            " (NOW() - last_heartbeat_at) > (($2)::text || ' seconds')::interval AS stale "
            "FROM job_workers WHERE $1 = ANY(queue_names) ORDER BY last_heartbeat_at DESC LIMIT 10",
            "prospective", str(int(getattr(settings, "JOB_WORKER_STALE_SECONDS", 90))))
        job_block = {
            "job_id": str(job["id"]), "job_status": job["status"],
            "total_tasks": tc["total"], "queued_tasks": tc["queued"],
            "running_tasks": tc["running"], "retryable_tasks": tc["retryable"],
            "succeeded_tasks": tc["succeeded"], "failed_tasks": tc["failed"],
            "cancelled_tasks": tc["cancelled"], "active_leases": tc["active_leases"],
            "expired_leases": tc["expired_leases"],
            "last_job_event": ({"event_type": last_event["event_type"],
                                "safe_message": last_event["safe_message"],
                                "created_at": last_event["created_at"].isoformat()}
                               if last_event else None),
            "workers": [{"worker_id": w["worker_id"], "status": w["status"],
                         "draining": w["draining"], "stale": bool(w["stale"]),
                         "current_task_id": str(w["current_task_id"]) if w["current_task_id"] else None,
                         "last_heartbeat_at": w["last_heartbeat_at"].isoformat()} for w in workers],
        }
    return {
        "contract_version": pc.AUDIT_CONTRACT_VERSION,
        "provider_called": False,
        "job": job_block,
        "registration_id": str(reg["id"]), "registration_identity": reg["registration_identity"],
        "registration_status": reg["status"],
        "campaign_id": str(reg["campaign_id"]) if reg["campaign_id"] else None,
        "campaign_run_id": str(run_id) if run_id else None,
        "experiment_code": reg["experiment_code"],
        "experiment_contract_version": reg["experiment_contract_version"],
        "universe_id": str(reg["universe_id"]), "universe_hash": reg["universe_hash"],
        "snapshot_session_date": reg["snapshot_session_date"].isoformat(),
        "snapshot_cutoff_at": reg["snapshot_cutoff_at"].isoformat(),
        "pair_count": counts["pairs"],
        "candidate_evaluation_count": counts["cand"], "control_evaluation_count": counts["ctrl"],
        "missing_candidate_arms": max(0, counts["pairs"] - counts["cand"]),
        "missing_control_arms": max(0, counts["pairs"] - counts["ctrl"]),
        "duplicate_arms": max(0, counts["evals"] - counts["cand"] - counts["ctrl"]),
        "candidate_readiness_distribution": cand_readiness,
        "candidate_decision_counts": cand_decisions,
        "candidate_setup_present_count": setup_n,
        "candidate_trigger_confirmed_count": trig_n,
        "candidate_pre_rollout_entry_count": pre_entry_n,
        "candidate_rollout_blocked_count": rollout_blocked_n,
        "candidate_watch_classification_counts": watch_reasons,
        "candidate_score_coverage": score_cov,
        "four_hour_frame_states": fourh_states,
        "control_decision_counts": ctrl_decisions,
        "control_signal_count": len(ctrl_signal_syms),
        "both_signal_intersection_count": len(cand_signal_syms & ctrl_signal_syms),
        "outcome_count": int(outcomes or 0),
        "campaign_completion_state": reg["status"],
    }


@router.get("/prospective/evidence-dashboard")
async def prospective_evidence_dashboard(
    _: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db)):
    """prospective_evidence_dashboard.v1 — read-only aggregated evidence payload
    for the NEXT UI tranche (no UI here). Composes the existing per-campaign
    audit (reused, not re-derived) with per-campaign horizon coverage and a
    bounded operational-state block. NEVER exposes credentials/DSNs/roles or raw
    evidence blobs; provider-free; no mutation."""
    _require_prospective_mode()
    from datetime import datetime, timezone
    from app.prospective_session import resolve_latest_completed_session
    from app.jobs import daily_pipeline as DP
    now = datetime.now(timezone.utc)

    regs = await db.fetch(
        "SELECT id, campaign_run_id, snapshot_session_date, status, universe_id, universe_hash "
        "FROM prospective_campaign_registrations ORDER BY snapshot_session_date")
    campaigns = []
    agg = {"campaigns": 0, "pairs": 0, "evaluations": 0, "outcome_rows": 0, "horizon_observations": 0}
    horizons = ("ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d")
    for r in regs:
        audit = await prospective_audit(_="dashboard", db=db, registration_id=str(r["id"]))
        cov = {"n": 0, "fb": None}
        status_dist = {}
        if r["campaign_run_id"]:
            row = await db.fetchrow(
                "SELECT COUNT(*) n, MAX(available_forward_bars) fb, "
                "COUNT(ret_1d) c1, COUNT(ret_3d) c3, COUNT(ret_5d) c5, "
                "COUNT(ret_10d) c10, COUNT(ret_20d) c20 "
                "FROM strategy_shadow_pair_outcomes o JOIN strategy_shadow_run_pairs rp "
                "ON rp.pair_id=o.pair_id WHERE rp.run_id=$1", r["campaign_run_id"])
            cov = {"n": int(row["n"] or 0), "fb": row["fb"],
                   "ret_1d": int(row["c1"] or 0), "ret_3d": int(row["c3"] or 0),
                   "ret_5d": int(row["c5"] or 0), "ret_10d": int(row["c10"] or 0),
                   "ret_20d": int(row["c20"] or 0)}
            sd = await db.fetch(
                "SELECT o.outcome_status, COUNT(*) c FROM strategy_shadow_pair_outcomes o "
                "JOIN strategy_shadow_run_pairs rp ON rp.pair_id=o.pair_id "
                "WHERE rp.run_id=$1 GROUP BY 1", r["campaign_run_id"])
            status_dist = {row2["outcome_status"]: int(row2["c"]) for row2 in sd}
            agg["horizon_observations"] += sum(cov[h] for h in horizons)
        campaigns.append({
            "registration_id": str(r["id"]),
            "snapshot_session": r["snapshot_session_date"].isoformat(),
            "campaign_completion_state": audit["campaign_completion_state"],
            "pair_count": audit["pair_count"],
            "candidate_evaluation_count": audit["candidate_evaluation_count"],
            "control_evaluation_count": audit["control_evaluation_count"],
            "candidate_decision_counts": audit["candidate_decision_counts"],
            "control_decision_counts": audit["control_decision_counts"],
            "candidate_setup_present_count": audit["candidate_setup_present_count"],
            "candidate_trigger_confirmed_count": audit["candidate_trigger_confirmed_count"],
            "candidate_pre_rollout_entry_count": audit["candidate_pre_rollout_entry_count"],
            "control_signal_count": audit["control_signal_count"],
            "outcome_status_distribution": status_dist,
            "horizon_coverage": cov,
        })
        agg["campaigns"] += 1
        agg["pairs"] += audit["pair_count"]
        agg["evaluations"] += audit["candidate_evaluation_count"] + audit["control_evaluation_count"]
        agg["outcome_rows"] += cov["n"]

    # bounded operational state (no DSNs / passwords / role internals)
    workers = await db.fetch(
        "SELECT worker_type, status, deployed_git_sha, "
        "(NOW()-last_heartbeat_at) < interval '180 seconds' AS fresh "
        "FROM job_workers WHERE (NOW()-last_heartbeat_at) < interval '900 seconds' "
        "ORDER BY worker_type")
    sched = await db.fetchrow(
        "SELECT schedule_code, enabled, paused, timezone, market_close_delay_minutes, next_run_at "
        "FROM job_schedules WHERE schedule_code='SMART-SCANNER-DAILY-PIPELINE'")
    occ = await DP.latest_pipeline_occurrence(db)
    occ_view = DP.build_status_view(occ) if occ else None

    return {
        "contract_version": "prospective_evidence_dashboard.v1",
        "provider_called": False,
        "generated_at": now.isoformat(),
        "latest_completed_session": str(resolve_latest_completed_session(now)),
        "universe": {
            "universe_id": str(regs[0]["universe_id"]) if regs else None,
            "universe_hash": regs[0]["universe_hash"] if regs else None,
            "symbol_count": settings.HISTORY_WARMUP_MAX_UNIVERSE_SYMBOLS if not regs else None,
        },
        "campaign_count": len(campaigns),
        "latest_campaign": campaigns[-1] if campaigns else None,
        "campaigns": campaigns,
        "aggregate": agg,
        "operational": {
            "workers": [{"worker_type": w["worker_type"], "status": w["status"],
                         "deployed_git_sha": w["deployed_git_sha"], "fresh": bool(w["fresh"])}
                        for w in workers],
            "schedule": ({"schedule_code": sched["schedule_code"], "enabled": sched["enabled"],
                          "paused": sched["paused"], "timezone": sched["timezone"],
                          "market_close_delay_minutes": sched["market_close_delay_minutes"],
                          "next_run_at": sched["next_run_at"].isoformat() if sched["next_run_at"] else None}
                         if sched else None),
            "latest_pipeline_occurrence": occ_view,
        },
    }


@router.post("/prospective/jobs")
async def prospective_enqueue_jobs(
    response: Response, _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db), body: Any = Body(...)):
    """prospective_campaign_enqueue.v1 — create ONE durable job + 25 symbol tasks
    atomically (the PRIMARY execution path; the worker processes them). NO
    strategy evaluation and NO provider construction happen in this request.
    Idempotent: exact replay → already_queued (same job); completed → already_applied."""
    _require_prospective_mode()
    from app.jobs import contracts as jc
    from app.jobs.contracts import JobError
    from app.jobs.prospective_enqueue import enqueue_prospective_campaign
    if settings.ENABLE_SCHEDULER:
        raise HTTPException(status_code=409, detail={"error": "scheduler_enabled"})
    if not isinstance(body, dict) or body.get("contract_version") != jc.PROSPECTIVE_CAMPAIGN_ENQUEUE_CONTRACT:
        raise HTTPException(status_code=422, detail={"error": "bad_contract_version"})
    reg_id = body.get("registration_id")
    reg_identity = body.get("registration_identity")
    if not reg_id or not reg_identity:
        raise HTTPException(status_code=422, detail={"error": "registration_id_and_identity_required"})
    try:
        uuid.UUID(str(reg_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail={"error": "invalid_registration_id"})
    try:
        result = await enqueue_prospective_campaign(
            db, registration_id=str(reg_id), registration_identity=str(reg_identity),
            requested_by="prospective_api")
    except JobError as e:
        raise HTTPException(status_code=409, detail={"error": e.safe_error_code})
    response.status_code = 200 if result.get("status") == "already_applied" else 202
    return result


@router.get("/prospective/outcomes/preflight")
async def prospective_outcome_preflight(
    _: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db),
    registration_id: str = Query(...), registration_identity: str = Query(...)):
    """prospective_outcome_maturity_preflight.v1 — read-only maturity report
    for outcome maturation of a COMPLETED campaign's frozen pairs. Reuses the
    existing eligibility classifier unchanged; NO provider construction, NO
    outcome calculation, NO write."""
    _require_prospective_mode()
    from app.jobs.contracts import JobError
    from app.jobs.prospective_outcome_enqueue import build_outcome_maturity_preflight
    try:
        uuid.UUID(str(registration_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail={"error": "invalid_registration_id"})
    try:
        return await build_outcome_maturity_preflight(
            db, registration_id=str(registration_id),
            registration_identity=str(registration_identity))
    except JobError as e:
        raise HTTPException(status_code=409, detail={"error": e.safe_error_code})


@router.post("/prospective/outcomes/jobs")
async def prospective_outcome_enqueue_jobs(
    response: Response, _: str = Depends(get_worker_token),
    db: asyncpg.Connection = Depends(get_db), body: Any = Body(...)):
    """prospective_outcome_maturation_enqueue.v1 — create ONE durable job +
    one task per MATURE-ELIGIBLE pair (never unmatured horizons, never a
    duplicate of an already-applied outcome). NO strategy evaluation and NO
    provider construction happen in this request. Idempotent: exact replay →
    already_queued/already_applied (same job); zero eligible pairs →
    no_eligible_work (no job created, safely re-checkable later)."""
    _require_prospective_mode()
    from app.jobs import contracts as jc
    from app.jobs.contracts import JobError
    from app.jobs.prospective_outcome_enqueue import enqueue_outcome_maturation
    if settings.ENABLE_SCHEDULER:
        raise HTTPException(status_code=409, detail={"error": "scheduler_enabled"})
    if (not isinstance(body, dict)
            or body.get("contract_version") != jc.PROSPECTIVE_OUTCOME_MATURATION_ENQUEUE_CONTRACT):
        raise HTTPException(status_code=422, detail={"error": "bad_contract_version"})
    reg_id = body.get("registration_id")
    reg_identity = body.get("registration_identity")
    if not reg_id or not reg_identity:
        raise HTTPException(status_code=422, detail={"error": "registration_id_and_identity_required"})
    try:
        uuid.UUID(str(reg_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail={"error": "invalid_registration_id"})
    try:
        result = await enqueue_outcome_maturation(
            db, registration_id=str(reg_id), registration_identity=str(reg_identity),
            requested_by="prospective_outcome_api")
    except JobError as e:
        raise HTTPException(status_code=409, detail={"error": e.safe_error_code})
    if result.get("status") == "no_eligible_work":
        response.status_code = 200
    else:
        response.status_code = 200 if result.get("status") == "already_applied" else 202
    return result


async def _v2_outcome_maturation_stage(db, occ, occurrence_id, universe, now):
    """daily_pipeline v2 outcome_maturation: matures ALL eligible campaigns
    (bounded prior sweep + the current one if it happens to be eligible) and
    records the CURRENT campaign's maturity TRUTHFULLY (deferred when it has no
    completed forward session yet — never a fabricated success, never a
    whole-occurrence blocker). Real data problems (terminal failures, forward
    history absent where expected) BLOCK with an explicit reason code. NEVER
    calls a provider; outcome writes are local-history-only via the dedicated
    outcome worker."""
    from app.jobs import daily_pipeline as DP
    from app.jobs import daily_pipeline_maturation as DM
    from app.jobs.contracts import JobError
    from app.jobs.prospective_outcome_enqueue import (
        build_outcome_maturity_preflight, enqueue_outcome_maturation)
    from app.prospective_session import resolve_latest_completed_session

    summary = DP.pipeline_summary(occ)
    # (a) never mature until the campaign job that produced the pairs is terminal.
    campaign_job_id = summary.get("campaign_job_id")
    if campaign_job_id:
        job = await db.fetchrow("SELECT status FROM job_runs WHERE id=$1", campaign_job_id)
        if job is not None and job["status"] not in ("succeeded", "failed", "cancelled"):
            return await DP.record_stage_result(db, occurrence_id, stage=DP.STAGE_OUTCOME_MATURATION,
                result={"state": DP.STAGE_STATE_IN_PROGRESS, "waiting_on": "campaign_job"})
    reg_id = summary.get("campaign_registration_id")
    if not reg_id:
        return await DP.record_stage_result(db, occurrence_id, stage=DP.STAGE_OUTCOME_MATURATION,
            result={"state": DP.STAGE_STATE_TERMINAL_FAILURE, "reason": "missing_campaign_registration_id"})
    reg_row = await db.fetchrow(
        "SELECT registration_identity, snapshot_session_date FROM "
        "prospective_campaign_registrations WHERE id=$1", reg_id)
    if reg_row is None:
        return await DP.record_stage_result(db, occurrence_id, stage=DP.STAGE_OUTCOME_MATURATION,
            result={"state": DP.STAGE_STATE_TERMINAL_FAILURE, "reason": "unknown_registration"})
    reg_identity = reg_row["registration_identity"]
    exp = settings.PROSPECTIVE_ALLOWED_EXPERIMENT_CODE
    target = str(resolve_latest_completed_session(now))

    # (b) current campaign maturity — truthful; may be deferred (NOT a failure).
    try:
        cur_pf = await build_outcome_maturity_preflight(
            db, registration_id=str(reg_id), registration_identity=reg_identity)
    except JobError as e:
        return await DP.record_stage_result(db, occurrence_id, stage=DP.STAGE_OUTCOME_MATURATION,
            result={"state": DP.STAGE_STATE_TERMINAL_FAILURE, "safe_error_code": e.safe_error_code})
    current = DM.classify_current_campaign_maturity(
        preflight=cur_pf, snapshot_session=str(reg_row["snapshot_session_date"]),
        target_session=target)
    if current["status"] == DM.CURRENT_UNVERIFIABLE:
        return await DP.record_stage_result(db, occurrence_id, stage=DP.STAGE_OUTCOME_MATURATION,
            result={"state": DP.STAGE_STATE_BLOCKED, "reason": current["reason"],
                    "current_campaign_maturity": current})

    # (c) bounded prior-campaign discovery (same experiment + frozen universe).
    plan = await DM.select_eligible_prior_registrations(
        db, experiment_code=exp, universe_id=universe["universe_id"],
        universe_hash=universe["universe_hash"], current_registration_id=str(reg_id),
        target_session=target)

    # (d) maturation targets: eligible priors + the current campaign iff it is
    # itself maturable this round (normally it is not — session-N has 0 forward).
    targets = [{"registration_id": e["registration_id"],
                "registration_identity": e["registration_identity"], "role": "prior"}
               for e in plan["eligible"]]
    if current["status"] == DM.CURRENT_MATURING:
        targets.append({"registration_id": str(reg_id),
                        "registration_identity": reg_identity, "role": "current"})

    # (e) enqueue/recognize each round idempotently; a round is terminal when its
    # job succeeded/failed/cancelled or there was no eligible work.
    jobs = []
    all_terminal = True
    for t in targets:
        try:
            enq = await enqueue_outcome_maturation(
                db, registration_id=t["registration_id"],
                registration_identity=t["registration_identity"],
                requested_by="daily_pipeline_v2")
        except JobError as e:
            all_terminal = False
            jobs.append({"registration_id": t["registration_id"], "role": t["role"],
                         "job_id": None, "enqueue_status": "error", "safe_error_code": e.safe_error_code})
            continue
        jid = enq.get("job_id")
        jstatus = enq.get("job_status")
        if jid:
            jr = await db.fetchrow("SELECT status FROM job_runs WHERE id=$1", jid)
            jstatus = jr["status"] if jr else jstatus
        terminal = (enq.get("status") == "no_eligible_work") or (
            jstatus in ("succeeded", "failed", "cancelled"))
        if not terminal:
            all_terminal = False
        jobs.append({"registration_id": t["registration_id"], "role": t["role"],
                     "job_id": jid, "enqueue_status": enq.get("status"), "job_status": jstatus})

    prior_block = {
        "contract_version": DM.PRIOR_DISCOVERY_CONTRACT,
        "candidate_count": plan["candidate_count"], "eligible_count": plan["eligible_count"],
        "max_lookback_sessions": plan["max_lookback_sessions"],
        "max_lookback_days": plan["max_lookback_days"],
        "eligible": plan["eligible"], "jobs": jobs,
    }
    state = DP.STAGE_STATE_COMPLETED if all_terminal else DP.STAGE_STATE_IN_PROGRESS
    result = {"state": state, "current_campaign_maturity": current, "prior_maturation": prior_block}
    if not all_terminal:
        result["waiting_on"] = "outcome_jobs"
    return await DP.record_stage_result(db, occurrence_id, stage=DP.STAGE_OUTCOME_MATURATION, result=result)


async def _v2_audit_report_stage(db, occ, occurrence_id):
    """daily_pipeline v2 audit_report: audit the CURRENT campaign and, separately,
    report every PRIOR campaign whose maturation round succeeded during this
    occurrence. Completes the occurrence — a deferred current campaign is a
    truthful longitudinal state, not a pipeline failure."""
    from app.jobs import daily_pipeline as DP
    summary = DP.pipeline_summary(occ)
    reg_id = summary.get("campaign_registration_id")
    if not reg_id:
        return await DP.record_stage_result(db, occurrence_id, stage=DP.STAGE_AUDIT_REPORT,
            result={"state": DP.STAGE_STATE_TERMINAL_FAILURE, "reason": "missing_campaign_registration_id"})
    audit = await prospective_audit(_="daily_pipeline_v2", db=db, registration_id=reg_id)
    prior = summary.get("prior_maturation") or {}
    prior_audits = []
    for j in (prior.get("jobs") or []):
        if j.get("role") == "prior" and j.get("job_status") == "succeeded":
            pa = await prospective_audit(_="daily_pipeline_v2", db=db, registration_id=j["registration_id"])
            prior_audits.append({"registration_id": j["registration_id"],
                                 "pair_count": pa["pair_count"], "outcome_count": pa["outcome_count"]})
    cur_maturity = summary.get("current_campaign_maturity") or {}
    return await DP.record_stage_result(db, occurrence_id, stage=DP.STAGE_AUDIT_REPORT, result={
        "state": DP.STAGE_STATE_COMPLETED,
        "pair_count": audit["pair_count"], "outcome_count": audit["outcome_count"],
        "campaign_completion_state": audit["campaign_completion_state"],
        "current_campaign_maturity_status": cur_maturity.get("status"),
        "prior_campaigns_newly_audited": prior_audits,
        "prior_campaigns_audited_count": len(prior_audits)})


async def advance_daily_pipeline_service(db: asyncpg.Connection, *, body: Any):
    """INTERNAL orchestration service — no HTTP, no worker token. ONE bounded
    advance of the durable daily-pipeline occurrence for ``universe_id`` (v1 or
    v2, selected by ``body['contract_version']``). Resolves the latest completed
    session itself (never a client-supplied date), performs at most the current
    stage's bounded unit of work, and persists via
    app.jobs.daily_pipeline.record_stage_result. Idempotent; NEVER calls a
    provider.

    Operational-auth architecture: the automated/recurring path calls THIS
    function DIRECTLY against the DB with an appropriately-privileged role — it
    never requires a human-held WORKER_TOKEN and never makes a self-HTTP call
    between Fly apps. The thin ``daily_pipeline_advance`` route below is only an
    operator/control-plane wrapper that adds the worker-token gate."""
    _require_prospective_mode()
    import uuid as _uuid
    from datetime import datetime, timezone
    from app.jobs import daily_pipeline as DP
    from app.jobs.contracts import JobError
    from app.jobs.prospective_enqueue import enqueue_prospective_campaign
    from app.jobs.prospective_outcome_enqueue import build_outcome_maturity_preflight, enqueue_outcome_maturation
    from app.prospective_session import resolve_latest_completed_session
    if not isinstance(body, dict) or body.get("contract_version") not in DP.PIPELINE_CONTRACT_VERSIONS:
        raise HTTPException(status_code=422, detail={"error": "bad_contract_version"})
    pcv = body["contract_version"]
    universe_id = body.get("universe_id")
    if not universe_id:
        raise HTTPException(status_code=422, detail={"error": "universe_id_required"})
    schedule_code = body.get("schedule_code") or "SMART-SCANNER-DAILY-PIPELINE"
    schedule_version = int(body.get("schedule_version") or 1)

    universe = await _load_universe(db, universe_id=universe_id, require_frozen=True)
    now = datetime.now(timezone.utc)
    resolved_session_date = str(resolve_latest_completed_session(now))

    occ = await DP.ensure_pipeline_occurrence(
        db, schedule_code=schedule_code, schedule_version=schedule_version,
        resolved_session_date=resolved_session_date, frozen_universe_hash=universe["universe_hash"],
        universe_id=universe["universe_id"], pipeline_contract_version=pcv)
    # NEVER reopen/rewrite an already-terminal occurrence: a repeated advance for
    # the SAME occurrence identity (schedule, session, universe, contract) that has
    # already succeeded/failed returns its immutable status view. History recovery
    # happens only for a still-RUNNING occurrence; a FRESH occurrence (distinct
    # schedule identity/version) for the same universe/session gets its own row and
    # can recognize/recover the failed LOGICAL history job independently.
    if occ.get("status") in ("succeeded", "failed"):
        return DP.build_status_view(occ)
    stage = DP.current_stage(occ)
    occurrence_id = str(occ["id"])

    if stage == DP.STAGE_HISTORY_REFRESH:
        # AUTOMATIC history refresh: the driver never calls a provider. When the
        # frozen universe's daily+4H history is stale it enqueues (or recognizes)
        # ONE durable refresh job whose per-symbol tasks are executed by the
        # dedicated history-refresh worker (the only automated component with the
        # provider credential). The stage WAITS (in_progress) while that child
        # job runs, re-checks readiness, and advances — no operator HTTP call.
        from app.jobs import history_refresh as HR
        summary = DP.pipeline_summary(occ)
        history_job_id = summary.get("history_job_id")
        # Recompute readiness the SAME way the campaign stage gates on it
        # (daily + 4H, per symbol). Once every symbol is both-ready the stage is
        # done regardless of child-job bookkeeping — this also resolves the
        # "child job succeeded and the universe is now current" case cleanly.
        readiness = await _prospective_readiness(db, universe, now=now)
        both_ready = int(readiness.get("both_ready_count") or 0)
        total = len(universe["symbols"])
        if both_ready == total:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_COMPLETED, "symbols_checked": total,
                "history_job_id": history_job_id})
        elif history_job_id and (await HR.history_refresh_job_status(db, history_job_id)) == "succeeded":
            # Provider work finished yet the universe is STILL not current → the
            # provider genuinely lacks the data to reach the target. This is a real
            # DATA-intervention condition (not normal waiting), so BLOCKED is the
            # truthful state.
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_BLOCKED, "reason": "history_incomplete_after_refresh",
                "history_job_id": history_job_id, "not_ready_count": total - both_ready,
                "operator_action": "inspect the history_incremental_refresh job + provider "
                                   "coverage for the not-ready symbols"})
        elif history_job_id and (await HR.history_refresh_job_status(db, history_job_id)) in (
                "queued", "running", "retryable"):
            # queued / running / retryable → normal async work in flight; WAIT.
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_IN_PROGRESS, "waiting_on": "history_refresh_job",
                "history_job_id": history_job_id, "not_ready_count": total - both_ready})
        else:
            # Stale and either NO child yet, or the current child FAILED. A single
            # generation-aware call enqueues a fresh generation-0 job, or (when a
            # prior logical history job is failed + retry-exhausted-retryable)
            # creates/recognizes exactly ONE bounded recovery SUCCESSOR — never
            # mutating the predecessor's job/task/attempt evidence.
            try:
                enq = await HR.enqueue_history_incremental_refresh(
                    db, universe_id=universe["universe_id"], universe_hash=universe["universe_hash"],
                    symbols=universe["symbols"], resolved_session_date=resolved_session_date,
                    contract_version=HR.HISTORY_REFRESH_CONTRACT_VERSION_V2,
                    requested_by="daily_pipeline")
            except JobError as e:
                occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                    "state": DP.STAGE_STATE_TERMINAL_FAILURE, "safe_error_code": e.safe_error_code})
                return DP.build_status_view(occ)
            if enq.get("recoverable") is False:
                # not recoverable / recovery budget exhausted → fail closed.
                occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                    "state": DP.STAGE_STATE_TERMINAL_FAILURE, "reason": "history_refresh_job_failed",
                    "history_job_id": enq.get("job_id"),
                    "history_refresh_status": enq.get("status"),
                    "recovery_generation": enq.get("recovery_generation")})
            else:
                occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                    "state": DP.STAGE_STATE_IN_PROGRESS, "waiting_on": "history_refresh_job",
                    "history_job_id": enq.get("job_id"), "history_refresh_status": enq.get("status"),
                    "recovery_generation": enq.get("recovery_generation"),
                    "predecessor_history_job_id": enq.get("predecessor_history_job_id"),
                    "not_ready_count": total - both_ready})

    elif stage == DP.STAGE_PROSPECTIVE_CAMPAIGN:
        exp = settings.PROSPECTIVE_ALLOWED_EXPERIMENT_CODE
        _u, readiness, snap, reg_identity, existing, blocking = await _prospective_preflight_state(
            db, universe["universe_id"], exp, now=now)
        if existing is not None and existing["status"] == "completed":
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_COMPLETED, "campaign_registration_id": str(existing["id"])})
        elif blocking:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_BLOCKED, "reasons": blocking})
        else:
            if existing is None:
                reg = await prospective_register(_="daily_pipeline", db=db, body={
                    "contract_version": "prospective_campaign_registration.v1",
                    "experiment_code": exp, "universe_id": universe["universe_id"],
                    "universe_hash": universe["universe_hash"],
                    "history_config_hash": readiness["config_hash"],
                    "history_readiness_manifest_hash": readiness["combined_readiness_manifest_hash"],
                    "snapshot_session_date": snap["snapshot_session_date"],
                    "snapshot_cutoff_at": snap["snapshot_cutoff_at"],
                    "candidate_signal_definition": "pre_rollout_enter_eligible.v1"})
                registration_id = reg["registration_id"]
            else:
                registration_id = str(existing["id"])
            try:
                enq = await enqueue_prospective_campaign(
                    db, registration_id=registration_id, registration_identity=reg_identity,
                    requested_by="daily_pipeline")
            except JobError as e:
                occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                    "state": DP.STAGE_STATE_TERMINAL_FAILURE, "safe_error_code": e.safe_error_code})
            else:
                occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                    "state": DP.STAGE_STATE_IN_PROGRESS,
                    "campaign_registration_id": registration_id,
                    "campaign_job_id": enq.get("job_id")})

    elif stage == DP.STAGE_OUTCOME_MATURATION and pcv == DP.PIPELINE_CONTRACT_VERSION_V2:
        occ = await _v2_outcome_maturation_stage(db, occ, occurrence_id, universe, now)
        return DP.build_status_view(occ)

    elif stage == DP.STAGE_OUTCOME_MATURATION:
        summary = DP.pipeline_summary(occ)
        # (a) Never mature until the campaign job that produced the pairs is
        # terminal — otherwise the frozen pair set could still be growing.
        campaign_job_id = summary.get("campaign_job_id")
        if campaign_job_id:
            job = await db.fetchrow("SELECT status FROM job_runs WHERE id=$1", campaign_job_id)
            if job is not None and job["status"] not in ("succeeded", "failed", "cancelled"):
                occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                    "state": DP.STAGE_STATE_IN_PROGRESS, "waiting_on": "campaign_job"})
                return DP.build_status_view(occ)
        # (b) Resolve THIS occurrence's own campaign — never another's. The
        # outcome job is scoped to this registration; there is no cross-campaign
        # reuse (a mismatched registration_id would mint a distinct idempotency
        # key and a distinct job).
        reg_id = summary.get("campaign_registration_id")
        if not reg_id:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_TERMINAL_FAILURE, "reason": "missing_campaign_registration_id"})
            return DP.build_status_view(occ)
        reg_row = await db.fetchrow(
            "SELECT registration_identity FROM prospective_campaign_registrations WHERE id=$1", reg_id)
        if reg_row is None:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_TERMINAL_FAILURE, "reason": "unknown_registration"})
            return DP.build_status_view(occ)
        reg_identity = reg_row["registration_identity"]
        # (c) If a prior advance already recorded an outcome job, recognise and
        # resume it rather than re-deriving — idempotent across restarts. A
        # succeeded job completes the stage; a still-running job keeps it
        # in_progress; only a failed/cancelled job falls through to a fresh
        # eligibility round.
        outcome_job_id = summary.get("outcome_job_id")
        if outcome_job_id:
            oj = await db.fetchrow("SELECT status FROM job_runs WHERE id=$1", outcome_job_id)
            if oj is not None:
                if oj["status"] == "succeeded":
                    occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                        "state": DP.STAGE_STATE_COMPLETED, "outcome_result": "recognized_succeeded",
                        "outcome_job_id": outcome_job_id})
                    return DP.build_status_view(occ)
                if oj["status"] not in ("failed", "cancelled"):
                    occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                        "state": DP.STAGE_STATE_IN_PROGRESS, "waiting_on": "outcome_job",
                        "outcome_job_id": outcome_job_id})
                    return DP.build_status_view(occ)
        # (d) Read-only maturity preflight (never constructs a provider).
        try:
            pf = await build_outcome_maturity_preflight(
                db, registration_id=str(reg_id), registration_identity=reg_identity)
        except JobError as e:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_TERMINAL_FAILURE, "safe_error_code": e.safe_error_code})
            return DP.build_status_view(occ)
        # (e) Unknown eligibility is NOT success — stay blocked and re-checkable.
        if int(pf.get("eligibility_unknown_count", 0)) > 0:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_BLOCKED, "reason": "eligibility_unknown",
                "eligibility_unknown_count": int(pf["eligibility_unknown_count"])})
            return DP.build_status_view(occ)
        # (f) No mature-eligible pair yet (e.g. the campaign's own session has no
        # completed forward sessions) → a bounded, honest no-op success. A later
        # occurrence (once forward sessions exist) enqueues its own round.
        actionable = int(pf.get("enqueue_available_count", 0))
        if actionable == 0:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_COMPLETED, "outcome_result": "no_eligible_work",
                "eligible_pair_count": 0, "matured_count": int(pf.get("matured_count", 0)),
                "outcome_job_id": None})
            return DP.build_status_view(occ)
        # (g) Idempotently enqueue-or-recognise the outcome job for the eligible
        # set. A repeat of the same eligible set maps to the SAME job (no
        # duplicate); a genuinely later round gets its own job.
        try:
            enq = await enqueue_outcome_maturation(
                db, registration_id=str(reg_id), registration_identity=reg_identity,
                requested_by="daily_pipeline")
        except JobError as e:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_TERMINAL_FAILURE, "safe_error_code": e.safe_error_code})
            return DP.build_status_view(occ)
        job_done = enq.get("status") == "already_applied" or enq.get("job_status") == "succeeded"
        occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
            "state": DP.STAGE_STATE_COMPLETED if job_done else DP.STAGE_STATE_IN_PROGRESS,
            "outcome_result": enq.get("status"),
            "outcome_job_id": enq.get("job_id"),
            "eligible_pair_count": int(enq.get("total_task_count") or 0),
            **({} if job_done else {"waiting_on": "outcome_job"})})

    elif stage == DP.STAGE_AUDIT_REPORT and pcv == DP.PIPELINE_CONTRACT_VERSION_V2:
        occ = await _v2_audit_report_stage(db, occ, occurrence_id)
        return DP.build_status_view(occ)

    elif stage == DP.STAGE_AUDIT_REPORT:
        summary = DP.pipeline_summary(occ)
        reg_id = summary.get("campaign_registration_id")
        if not reg_id:
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_TERMINAL_FAILURE, "reason": "missing_campaign_registration_id"})
        else:
            audit = await prospective_audit(_="daily_pipeline", db=db, registration_id=reg_id)
            occ = await DP.record_stage_result(db, occurrence_id, stage=stage, result={
                "state": DP.STAGE_STATE_COMPLETED,
                "pair_count": audit["pair_count"], "outcome_count": audit["outcome_count"],
                "campaign_completion_state": audit["campaign_completion_state"]})

    return DP.build_status_view(occ)


@router.post("/daily-pipeline/advance")
async def daily_pipeline_advance(
    _: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db),
    body: Any = Body(...)):
    """Operator/control-plane HTTP wrapper (worker-token gated) around
    ``advance_daily_pipeline_service``. The automated/recurring pipeline does NOT
    use this route — it calls the service function directly against the DB, so
    automation never depends on a human-held WORKER_TOKEN or self-HTTP."""
    return await advance_daily_pipeline_service(db, body=body)


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


async def _latest_maintenance_run(db):
    """The most recent MAINTENANCE-tagged outcome run, or None. Read-only.

    Maintenance executions tag their run row with a `maintenance` block inside
    `requested_selector` (set at creation, so it survives a crash or restart and
    is queryable even if the run never finalizes). Generic runs from the calc
    endpoint / scheduler carry no such marker and are therefore never selected —
    the cooldown is scoped strictly to provider-backed maintenance execution.
    The row is ordered by the documented reference-timestamp precedence so the
    latest attempt (successful OR failed) wins.
    """
    row = await db.fetchrow(
        """
        SELECT id, status, created_at, started_at, finished_at, updated_at
        FROM strategy_shadow_outcome_runs
        WHERE requested_selector -> 'maintenance' IS NOT NULL
        ORDER BY COALESCE(finished_at, updated_at, started_at, created_at) DESC
        LIMIT 1
        """)
    # Return a plain dict so the pure cooldown helper's `.get(...)` is portable
    # across asyncpg versions (Record.get availability varies).
    return dict(row) if row is not None else None


def _resolved_min_batch_interval() -> int:
    """Effective server-enforced batch interval for the current process."""
    from app.maintenance_cooldown import resolve_min_interval_seconds
    return resolve_min_interval_seconds(
        settings.MAINTENANCE_MIN_BATCH_INTERVAL_SECONDS,
        maintenance_only_mode=settings.MAINTENANCE_ONLY_MODE,
        provider=(settings.MARKET_DATA_PROVIDER or ""))


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
        min_batch_interval_seconds=_resolved_min_batch_interval(),
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

    # Provider pacing (read-only; never constructs or calls the provider). The
    # cooldown is derived from the latest persisted maintenance run, so it holds
    # across restart / auto-stop / token rotation. cohort+manifest may be safe
    # and the environment ready, yet execution is temporarily unavailable.
    from datetime import datetime, timezone
    from app.maintenance_cooldown import (
        COOLDOWN_BLOCKING_REASON, compute_cooldown)
    min_interval = _resolved_min_batch_interval()
    latest_run = await _latest_maintenance_run(db)
    cooldown = compute_cooldown(
        latest_run, min_interval_seconds=min_interval,
        now=datetime.now(timezone.utc))
    cooldown_active = not cooldown["execution_allowed_by_cooldown"]
    blocking_reasons = list(planning["blocking_reasons"]) + (
        [COOLDOWN_BLOCKING_REASON] if cooldown_active else [])

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
        "blocking_reasons": blocking_reasons,
        "experiment_eligible_unmatured_count": plan["experiment_eligible_unmatured_count"],
        "excluded_non_campaign_count": plan["excluded_non_campaign_eligible_count"],
        "campaign_membership_unverifiable_count": (
            plan["membership"]["campaign_membership_unverifiable_count"]),
        # provider pacing (temporary unavailability distinct from readiness)
        "min_batch_interval_seconds": cooldown["min_interval_seconds"],
        "last_execution_finished_at": cooldown["last_execution_finished_at"],
        "next_execution_not_before": cooldown["next_execution_not_before"],
        "cooldown_remaining_seconds": cooldown["cooldown_remaining_seconds"],
        "execution_allowed_by_cooldown": cooldown["execution_allowed_by_cooldown"],
        # execution available only when safe AND the stable lock matches AND a
        # next batch exists AND the provider cooldown has cleared.
        "execution_available": bool(planning["safe_to_execute"] and locked_matches
                                    and plan["next_batch"]["available"]
                                    and cooldown["execution_allowed_by_cooldown"]),
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
    import json as _json
    import uuid as _uuid
    from datetime import datetime, timezone
    from app.maintenance_execute import (
        EXECUTE_CONTRACT_VERSION,
        MAINTENANCE_ADVISORY_LOCK_KEY,
        MODE_RETRY,
        validate_normal,
        validate_retry,
    )
    from app.maintenance_cooldown import (
        COOLDOWN_BLOCKING_REASON,
        COOLDOWN_UNDER_LOCK_REASON,
        compute_cooldown,
        retry_after_seconds,
    )

    exp = settings.MAINTENANCE_ALLOWED_EXPERIMENT_CODE
    scope = settings.MAINTENANCE_ALLOWED_COHORT_SCOPE
    locked = (settings.MAINTENANCE_LOCKED_COHORT_HASH or None)
    min_interval = _resolved_min_batch_interval()
    logger = logging.getLogger(__name__)
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="request body must be a JSON object")
    mode = body.get("mode")

    def _cooldown_409(cooldown, *, reason):
        """Safe 409 for an active provider cooldown (+ whole-second Retry-After).
        Never exposes a pair id, provider payload, DSN or token."""
        return HTTPException(
            status_code=409,
            detail={
                "error": reason,
                "detail": "provider request window not yet cleared — obtain a "
                          "fresh preflight after the cooldown elapses",
                "min_batch_interval_seconds": cooldown["min_interval_seconds"],
                "next_execution_not_before": cooldown["next_execution_not_before"],
                "cooldown_remaining_seconds": cooldown["cooldown_remaining_seconds"],
            },
            headers={"Retry-After": str(retry_after_seconds(cooldown))})

    async def _cooldown_now():
        return compute_cooldown(
            await _latest_maintenance_run(db),
            min_interval_seconds=min_interval,
            now=datetime.now(timezone.utc))

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

    # ---- provider cooldown (Step 4): BEFORE the advisory lock, before any ---- #
    # provider construction / call / outcome-run creation / outcome write. A
    # genuine idempotent replay (stale hashes, pairs already complete) returned
    # `already_applied` above and never reaches this gate.
    cooldown = await _cooldown_now()
    if not cooldown["execution_allowed_by_cooldown"]:
        raise _cooldown_409(cooldown, reason=COOLDOWN_BLOCKING_REASON)

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

        # DOUBLE-CHECK the cooldown under the lock (Step 8): a request may have
        # passed the initial gate and then waited on the advisory lock while
        # another batch completed. Recompute from the latest persisted run and
        # reject WITHOUT constructing the provider if the window reopened.
        cooldown2 = await _cooldown_now()
        if not cooldown2["execution_allowed_by_cooldown"]:
            raise _cooldown_409(cooldown2, reason=COOLDOWN_UNDER_LOCK_REASON)

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
        provider_name = getattr(provider, "name", None) or "unknown"

        # PRE-CREATE the outcome-run row with a bounded maintenance marker in
        # requested_selector so this attempt is distinctly identifiable for the
        # cooldown query — set at creation, so it survives a crash/restart even
        # if the run never finalizes. The service is passed this id and its own
        # create_outcome_run is a no-op (ON CONFLICT (id) DO NOTHING). No secret
        # (token / key / DSN / header / payload) is ever stored.
        outcome_run_id = str(_uuid.uuid4())
        maintenance_marker = {
            "contract_version": EXECUTE_CONTRACT_VERSION,
            "mode": mode,
            "batch_identity": batch_identity,
            "cohort_lock_hash": plan2.get("cohort_lock_hash"),
            "pair_count": len(validated),
        }
        if mode == MODE_RETRY:
            maintenance_marker["retry_plan_hash"] = (
                (plan2.get("retry_plan") or {}).get("retry_plan_hash"))
        else:
            maintenance_marker["remaining_manifest_hash"] = plan2.get("remaining_manifest_hash")
            maintenance_marker["next_batch_hash"] = (
                (plan2.get("next_batch") or {}).get("next_batch_hash"))
        pre_selector = {
            "pair_ids": list(validated), "symbols": [], "run_id": None,
            "pending": False, "include_recalc": include_recalc,
            "maintenance": maintenance_marker,
        }
        await db.execute(
            """
            INSERT INTO strategy_shadow_outcome_runs (
                id, status, requested_selector, requested_limit, provider,
                started_at, created_at, updated_at
            )
            VALUES ($1, 'running', $2, $3, $4, NOW(), NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            outcome_run_id, _json.dumps(pre_selector), len(validated), provider_name)

        # Observability (Step 12): safe structured maintenance batch telemetry.
        # Accurate per-provider 429/attempt/retry counters would require invasive
        # changes to the (boundary-frozen) provider client, so they are omitted
        # here — the provider client already emits 429 WARNING lines. We record
        # the batch envelope only; no key / credentialed URL / response body.
        started_at = datetime.now(timezone.utc)
        logger.info("[MAINT] batch started run=%s identity=%s mode=%s pairs=%d "
                    "include_recalc=%s min_interval_s=%d",
                    outcome_run_id, batch_identity, mode, len(validated),
                    include_recalc, min_interval)
        summary = await run_shadow_outcome_calculation(
            provider, pair_ids=validated, symbols=None, run_id=None, pending=False,
            limit=len(validated), include_recalc=include_recalc,
            outcome_run_id=outcome_run_id)
        elapsed_s = (datetime.now(timezone.utc) - started_at).total_seconds()
        after = await db.fetch(
            "SELECT pair_id, outcome_status, error_code FROM "
            "strategy_shadow_pair_outcomes WHERE pair_id = ANY($1::uuid[])", validated)
        st = {str(r["pair_id"]): r for r in after}
        error_count = sum(1 for p in validated
                          if (st.get(p) or {}).get("outcome_status") == "error")
        logger.info("[MAINT] batch finished run=%s status=%s pairs=%d errors=%d "
                    "duration_s=%.3f", outcome_run_id, summary.get("status"),
                    len(validated), error_count, elapsed_s)
        return {
            "status": "executed", "batch_identity": batch_identity,
            "mode": mode, "outcome_run_id": summary.get("outcome_run_id") or outcome_run_id,
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
