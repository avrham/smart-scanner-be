"""Hierarchical funnel scanner (Phase 3).

Two layers:
  * PURE core (no I/O): stage classifiers + a RejectionTracker + telemetry
    assembly. Fully unit-testable.
  * async orchestrator `run_funnel_scan(...)`: loads the universe from the ticker
    cache, runs the stages, fetches history ONLY for liquidity survivors, and
    persists staged telemetry into pattern_runs.notes.

SAFETY:
  * Cheap stages (0/1) never touch FMP. `dry_run=True` stops after Stage 1, so it
    is completely FMP-free and safe for validation/tests.
  * Stage 4 (4H) is survivor-only and OFF by default (Phase 5.1): it requires an
    explicit opt-in (scanner enable_expensive_stages or pattern enable_4h_trigger),
    a strategy that declares "4h" in required_timeframes, AND a WATCH result from
    Stage 3 — i.e. monthly/weekly/daily already valid, trigger missing.
  * The survivor set fed to the (expensive) history fetch is bounded by `limit`
    / max_universe_size to prevent broad FMP usage; 4H fetches are a subset of it.
"""

import asyncio
import logging
import uuid
from collections import Counter
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.workers.indicators import to_dataframe, validate_dataframe
from app.workers.patterns.config import resolve_pattern_config
from app.workers.strategies import (
    StrategyContext,
    StrategyDecision,
    StrategyResult,
    get_strategy,
)
from app.workers.strategies.decision_card import build_decision_card
from app.workers.persistence import (
    get_universe_tickers,
    mark_seen_today,
    save_signal,
    was_seen_today,
)
from app.workers.provenance import (
    build_provenance,
    market_data_as_of_from_details,
    market_data_as_of_from_df,
)
from app.workers.scan_runs import create_scan_run, finalize_scan_run
from app.config import settings
from app.utils.events import event_bus


logger = logging.getLogger(__name__)

SCANNER_VERSION = "funnel_v1"

# Minimal, safe scanner-level defaults. Kept small on purpose (no large config
# system). Strategy thresholds still come from the pattern config resolver.
DEFAULT_SCANNER_CONFIG: Dict[str, Any] = {
    "max_universe_size": 500,      # cap survivors sent to the expensive fetch
    "sample_rejections_limit": 25, # cap stored per-symbol reject samples
    "allow_unknown_volume": False, # never include unknown-volume names by default
    "enable_expensive_stages": False,  # Stage 4 (4H etc.) stays off in Phase 3
    "persist_watch_candidates": True,  # Phase 5.2: save WATCH + decision card
    "scanner_version": SCANNER_VERSION,
}

MIN_BARS = 200

# Cap for the bounded result-symbol lists included in telemetry/summary
# (enter_symbols / watch_symbols / evaluated_symbols). Never raw payloads.
RESULT_SYMBOLS_CAP = 25


# --------------------------------------------------------------------------- #
# Pure stage classifiers
# --------------------------------------------------------------------------- #

def classify_liquidity(
    ticker: Dict[str, Any],
    min_market_cap: float,
    min_daily_volume: float,
    allow_unknown_volume: bool = False,
) -> Optional[str]:
    """Stage 1 classification for a single ticker.

    Returns None if the ticker passes, otherwise a rejection reason string:
    'market_cap_unknown' | 'market_cap_below_min' | 'volume_unknown' |
    'volume_below_min'. Never fabricates missing values.
    """
    market_cap = ticker.get("market_cap")
    volume = ticker.get("last_volume")

    if market_cap is None:
        return "market_cap_unknown"
    if market_cap < min_market_cap:
        return "market_cap_below_min"

    if volume is None:
        return None if allow_unknown_volume else "volume_unknown"
    if volume < min_daily_volume:
        return "volume_below_min"

    return None


def cheap_prefilter(
    df: Optional[pd.DataFrame],
    min_price: float,
    min_bars: int = MIN_BARS,
) -> Optional[str]:
    """Stage 2 cheap daily prefilter on already-fetched OHLCV.

    Returns None if the symbol passes, else a rejection reason:
    'no_data' | 'missing_columns' | 'insufficient_history' | 'invalid_ohlcv' |
    'price_below_min'. Intentionally minimal (no over-engineering).
    """
    if df is None or df.empty:
        return "no_data"

    required = ["date", "open", "high", "low", "close", "volume"]
    if not all(col in df.columns for col in required):
        return "missing_columns"

    if len(df) < min_bars:
        return "insufficient_history"

    if not validate_dataframe(df, min_bars=min_bars):
        return "invalid_ohlcv"

    latest_price = float(df.iloc[-1]["close"])
    if latest_price < min_price:
        return "price_below_min"

    return None


class RejectionTracker:
    """Accumulates rejection reason counts + a capped list of per-symbol samples."""

    def __init__(self, sample_limit: int = 25):
        self.counts: Counter = Counter()
        self.samples: List[Dict[str, Any]] = []
        self.sample_limit = sample_limit

    def add(self, symbol: str, stage: str, reason: str) -> None:
        self.counts[reason] += 1
        if len(self.samples) < self.sample_limit:
            self.samples.append({"symbol": symbol, "stage": stage, "reason": reason})

    def as_dict(self) -> Dict[str, Any]:
        return {
            "rejection_reason_counts": dict(self.counts),
            "sample_rejections": list(self.samples),
        }


def apply_liquidity_filter(
    tickers: List[Dict[str, Any]],
    min_market_cap: float,
    min_daily_volume: float,
    allow_unknown_volume: bool,
    tracker: RejectionTracker,
) -> List[Dict[str, Any]]:
    """Stage 1: return liquidity survivors, recording rejects in `tracker`."""
    survivors: List[Dict[str, Any]] = []
    for t in tickers:
        reason = classify_liquidity(
            t, min_market_cap, min_daily_volume, allow_unknown_volume
        )
        if reason is None:
            survivors.append(t)
        else:
            tracker.add(t.get("symbol", "?"), "liquidity", reason)
    return survivors


def build_config_summary(
    pattern_config: Dict[str, Any],
    scanner_config: Dict[str, Any],
    limit: Optional[int],
) -> Dict[str, Any]:
    """Compact, safe summary of the effective config (for telemetry)."""
    liq = pattern_config.get("min_liquidity_filters", {}) or {}
    return {
        "min_market_cap": liq.get("min_market_cap"),
        "min_daily_volume": liq.get("min_daily_volume"),
        "min_price": pattern_config.get("min_price"),
        "score_threshold": pattern_config.get("score_threshold"),
        "allow_unknown_volume": scanner_config.get("allow_unknown_volume"),
        "max_universe_size": scanner_config.get("max_universe_size"),
        "limit": limit,
    }


def build_data_source(provider_name: str, dry_run: bool) -> str:
    """Provider-aware data_source label. Never claims a provider that wasn't used."""
    if dry_run:
        return "tickers_cache only (dry_run: no historical provider calls)"
    return f"tickers_cache + {provider_name}_historical"


def assemble_telemetry(
    *,
    pattern_code: str,
    scanner_config: Dict[str, Any],
    config_summary: Dict[str, Any],
    started_at: datetime,
    finished_at: datetime,
    stage_counts: Dict[str, int],
    tracker: RejectionTracker,
    api_call_counts: Dict[str, int],
    dry_run: bool,
    extra_notes: List[str],
    market_data_provider: str = "unknown",
    result_symbols: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    """Assemble the structured telemetry object stored in pattern_runs.notes."""
    telemetry = {
        "scanner_version": scanner_config.get("scanner_version", SCANNER_VERSION),
        "pattern_code": pattern_code,
        "config_summary": config_summary,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "runtime_seconds": round((finished_at - started_at).total_seconds(), 2),
        "universe_count": stage_counts.get("stage_0_universe", 0),
        "stage_counts": stage_counts,
        "api_call_counts": api_call_counts,
        "market_data_provider": market_data_provider,
        "data_source": build_data_source(market_data_provider, dry_run),
        "dry_run": dry_run,
        "notes": extra_notes,
    }
    telemetry.update(result_symbols or {
        "enter_symbols": [], "watch_symbols": [], "evaluated_symbols": [],
    })
    telemetry.update(tracker.as_dict())
    # Zero candidates is a NORMAL completed outcome; the explicit terminal
    # reason distinguishes "nothing to evaluate" from "evaluated, no setup"
    # without any error identity.
    if stage_counts.get("stage_0_universe", 0) == 0 and not dry_run:
        telemetry["terminal_reason"] = "no_candidates"
    return telemetry


# --------------------------------------------------------------------------- #
# Async orchestrator (I/O)
# --------------------------------------------------------------------------- #

def _merge_scanner_config(overrides: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cfg = dict(DEFAULT_SCANNER_CONFIG)
    if overrides:
        cfg.update({k: v for k, v in overrides.items() if v is not None})
    return cfg


async def run_funnel_scan(
    fmp: Optional[Any],  # any MarketDataProvider (massive default, fmp fallback)
    pattern_code: str = "sma150_bounce",
    limit: Optional[int] = None,
    scanner_config: Optional[Dict[str, Any]] = None,
    ignore_seen: bool = False,
    dry_run: bool = False,
    scan_id: Optional[str] = None,
    scan_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Run the hierarchical funnel and persist staged telemetry.

    dry_run=True runs Stages 0-1 only (NO FMP, NO signal writes) and returns the
    telemetry — the safe validation path. Otherwise history is fetched for
    liquidity survivors (bounded by limit/max_universe_size), prefiltered, and
    the strategy is evaluated; ENTER signals are saved via the existing pipeline.
    """
    started_at = datetime.utcnow()
    scan_date = scan_date or date.today()
    # Phase 7B: the scan ALWAYS has a canonical identity — the same UUID the
    # admin endpoint returned / the WebSocket uses. Generated here only when a
    # caller (e.g. a test) didn't provide one.
    scan_id = scan_id or str(uuid.uuid4())
    scfg = _merge_scanner_config(scanner_config)
    tracker = RejectionTracker(sample_limit=int(scfg["sample_rejections_limit"]))
    extra_notes: List[str] = []
    api_call_counts: Dict[str, int] = {"historical_fetches": 0, "four_hour_fetches": 0}
    # Provider identity for telemetry (never claim a provider that wasn't used).
    provider_name = getattr(fmp, "name", None) or ("none" if fmp is None else "unknown")
    # Bounded result visibility (capped symbol lists; no raw payloads).
    result_symbols: Dict[str, List[str]] = {
        "enter_symbols": [], "watch_symbols": [], "evaluated_symbols": [],
    }

    def _track_symbol(bucket: str, symbol: str) -> None:
        if len(result_symbols[bucket]) < RESULT_SYMBOLS_CAP:
            result_symbols[bucket].append(symbol)

    # Phase 4/5: resolve the strategy first (fails fast on unknown pattern) and
    # use ITS defaults for the Phase 1 config resolver (DB overrides on top).
    strategy = get_strategy(pattern_code)
    pattern_config = await resolve_pattern_config(pattern_code, strategy.default_config())
    liq = pattern_config.get("min_liquidity_filters", {}) or {}
    min_market_cap = float(liq.get("min_market_cap", 200_000_000))
    min_daily_volume = float(liq.get("min_daily_volume", 200_000))
    min_price = float(pattern_config.get("min_price", 1.0))

    stage_counts: Dict[str, int] = {
        "stage_0_universe": 0,
        "stage_1_liquidity_passed": 0,
        "stage_2_prefilter_passed": 0,
        "stage_3_evaluated": 0,
        "stage_4_4h_fetched": 0,
        "enter_count": 0,
        "watch_count": 0,   # not supported by sma150_bounce (documented)
        "watch_saved_count": 0,  # Phase 5.2: WATCH candidates persisted
        "reject_count": 0,
        # Phase 7B immutable-identity accounting: a repeated exact signal is
        # deduplicated (linked to this scan), never counted as newly created.
        "signals_created": 0,
        "signals_deduplicated": 0,
        "signals_linked": 0,
    }

    # Phase 7B: create the CANONICAL scan-run row at scan START (status=
    # 'running') so persisted signals can FK-link to this exact run via
    # signal_provenance.scan_run_id. Same UUID as the endpoint/WebSocket.
    await create_scan_run(
        scan_run_id=scan_id,
        pattern_code=pattern_code,
        scanner_mode="funnel",
        provider=provider_name,
        dry_run=dry_run,
        requested_limit=limit,
        scan_date=scan_date,
        run_started_at=started_at,
    )

    try:
        return await _run_funnel_stages(
            fmp=fmp,
            pattern_code=pattern_code,
            limit=limit,
            ignore_seen=ignore_seen,
            dry_run=dry_run,
            scan_id=scan_id,
            scan_date=scan_date,
            started_at=started_at,
            scfg=scfg,
            tracker=tracker,
            extra_notes=extra_notes,
            api_call_counts=api_call_counts,
            provider_name=provider_name,
            result_symbols=result_symbols,
            _track_symbol=_track_symbol,
            strategy=strategy,
            pattern_config=pattern_config,
            stage_counts=stage_counts,
            min_market_cap=min_market_cap,
            min_daily_volume=min_daily_volume,
            min_price=min_price,
        )
    except Exception as exc:
        # HANDLED failure lifecycle: the canonical run row must never stay
        # 'running' after a handled exception. Counts reflect whatever
        # completed before the failure; partial telemetry only, nothing
        # invented. (Abrupt process death cannot run this block — see the
        # roadmap: stale 'running' rows are forensic, never blocking.)
        await _persist_telemetry(
            scan_id, "failed", stage_counts,
            {"partial": True, "stage_counts": dict(stage_counts)},
            error_code="funnel_scan_exception",
            error_message=str(exc),
        )
        raise


async def _run_funnel_stages(
    *,
    fmp: Optional[Any],
    pattern_code: str,
    limit: Optional[int],
    ignore_seen: bool,
    dry_run: bool,
    scan_id: str,
    scan_date: date,
    started_at: datetime,
    scfg: Dict[str, Any],
    tracker: RejectionTracker,
    extra_notes: List[str],
    api_call_counts: Dict[str, int],
    provider_name: str,
    result_symbols: Dict[str, List[str]],
    _track_symbol,
    strategy: Any,
    pattern_config: Dict[str, Any],
    stage_counts: Dict[str, int],
    min_market_cap: float,
    min_daily_volume: float,
    min_price: float,
) -> Dict[str, Any]:
    """Stages 0-4 of the funnel (split out so a failure finalizes the run)."""
    if scan_id:
        await event_bus.publish(scan_id, {"type": "stage", "stage": "universe_build"})

    # Stage 0 - universe from the ticker cache (real values, includes NULLs).
    universe = await get_universe_tickers()
    stage_counts["stage_0_universe"] = len(universe)

    # Stage 1 - liquidity filter (cheap, no FMP).
    survivors = apply_liquidity_filter(
        universe, min_market_cap, min_daily_volume, bool(scfg["allow_unknown_volume"]), tracker
    )
    stage_counts["stage_1_liquidity_passed"] = len(survivors)

    # Bound the survivor set that will hit the expensive fetch.
    cap = int(limit) if limit else int(scfg["max_universe_size"])
    bounded = survivors[:cap]
    if len(survivors) > len(bounded):
        extra_notes.append(
            f"survivors capped {len(survivors)}->{len(bounded)} (limit={cap})"
        )

    # Phase 5.1: survivor-only 4H gate. Explicit opt-in via scanner-level
    # enable_expensive_stages OR pattern-level enable_4h_trigger. Only strategies
    # that declare "4h" in required_timeframes (wyckoff_mtf) ever fetch it, and
    # only for candidates that already passed liquidity + prefilter + monthly/
    # weekly/daily (i.e. Stage 3 returned WATCH awaiting a trigger).
    expensive_enabled = bool(scfg["enable_expensive_stages"]) or bool(
        pattern_config.get("enable_4h_trigger", False)
    )
    strategy_wants_4h = "4h" in (getattr(strategy, "required_timeframes", None) or [])
    use_4h = expensive_enabled and strategy_wants_4h
    # Phase 5.2: persist WATCH candidates (with decision cards) unless disabled.
    persist_watch = bool(scfg.get("persist_watch_candidates", True))
    if not expensive_enabled:
        extra_notes.append("expensive stages (4H) disabled")
    elif not strategy_wants_4h:
        extra_notes.append("expensive stages enabled but strategy does not use 4H")
    else:
        extra_notes.append("4H trigger enabled: survivor-only fetches")

    if dry_run:
        extra_notes.append("dry_run: stages 2-3 skipped, no provider calls, no writes")
        finished_at = datetime.utcnow()
        telemetry = assemble_telemetry(
            pattern_code=pattern_code,
            scanner_config=scfg,
            config_summary=build_config_summary(pattern_config, scfg, limit),
            started_at=started_at,
            finished_at=finished_at,
            stage_counts=stage_counts,
            tracker=tracker,
            api_call_counts=api_call_counts,
            dry_run=True,
            extra_notes=extra_notes,
            market_data_provider=provider_name,
            result_symbols=result_symbols,
        )
        await _persist_telemetry(scan_id, "completed", stage_counts, telemetry)
        if scan_id:
            await event_bus.publish(scan_id, {"type": "finished", "telemetry": telemetry})
        return _summary(telemetry, stage_counts, dry_run=True)

    # Stage 2/3 require FMP.
    if fmp is None:
        raise ValueError("run_funnel_scan requires an FMP client when dry_run is False")

    # Stage 3 evaluates through the strategy interface (resolved above). Size the
    # daily history fetch + prefilter to the strategy's needs (e.g. Wyckoff needs
    # deep history for monthly bars). Still ONE bounded call per survivor.
    min_bars = int(getattr(strategy, "min_daily_bars", MIN_BARS))
    timeseries = max(350, min_bars + 60)

    bounded_symbols = [t["symbol"] for t in bounded]
    if scan_id:
        await event_bus.publish(
            scan_id, {"type": "stage", "stage": "fetching_data", "total": len(bounded_symbols)}
        )

    historical_batch = await fmp.batch_historical_data(bounded_symbols, timeseries=timeseries)
    api_call_counts["historical_fetches"] = len(bounded_symbols)

    if scan_id:
        await event_bus.publish(
            scan_id, {"type": "stage", "stage": "evaluating", "total": len(bounded_symbols)}
        )

    # Phase 7B: decision-relevant scanner settings included in each signal's
    # config snapshot/hash (secrets are stripped by the provenance builder).
    scanner_settings = {
        "scanner_version": scfg.get("scanner_version", SCANNER_VERSION),
        "max_universe_size": scfg.get("max_universe_size"),
        "allow_unknown_volume": scfg.get("allow_unknown_volume"),
        "enable_expensive_stages": expensive_enabled,
        "persist_watch_candidates": persist_watch,
        "requested_limit": limit,
        "min_daily_bars": min_bars,
        "required_timeframes": list(getattr(strategy, "required_timeframes", None) or []),
    }

    def _provenance_for(
        result: StrategyResult,
        df: Optional[pd.DataFrame],
        details: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Provenance from the REAL strategy result + the evaluated dataframe.

        `details` already includes the decision card so the evidence snapshot
        (and hence the immutable fingerprint) covers the full persisted
        deterministic evidence.
        """
        return build_provenance(
            scan_run_id=scan_id,
            source_path="funnel",
            scanner_mode="funnel",
            provider=provider_name,
            strategy_code=result.pattern_code,
            strategy_version=result.strategy_version or getattr(strategy, "version", "unknown"),
            strategy_config=pattern_config,
            scanner_settings=scanner_settings,
            details=details,
            score_components=result.score_components,
            # Phase 8: a strategy enforcing completed-bar semantics declares
            # the as-of of the bar it ACTUALLY evaluated (a partial latest
            # bar may have been excluded); fall back to the frame otherwise.
            market_data_as_of=(
                market_data_as_of_from_details(details)
                or market_data_as_of_from_df(df)
            ),
            # Phase 8: strategies with an explicit decision policy (sma150.v3)
            # name it; None keeps the legacy implicit policy.
            decision_policy_version=getattr(strategy, "decision_policy_version", None),
        )

    for ticker in bounded:
        symbol = ticker["symbol"]
        try:
            if not ignore_seen and await was_seen_today(symbol, scan_date):
                tracker.add(symbol, "prefilter", "already_seen_today")
                continue

            fmp_data = historical_batch.get(symbol, {})
            try:
                df = to_dataframe(fmp_data) if fmp_data.get("historical") else None
            except Exception:
                df = None

            # Stage 2 - cheap prefilter (min_bars sized to the strategy).
            reason = cheap_prefilter(df, min_price, min_bars=min_bars)
            if reason is not None:
                tracker.add(symbol, "prefilter", reason)
                await mark_seen_today(symbol, scan_date)
                continue
            stage_counts["stage_2_prefilter_passed"] += 1

            # Stage 3 - strategy evaluation on survivors only (via registry).
            context = StrategyContext(
                symbol=symbol,
                pattern_code=pattern_code,
                config=pattern_config,
                scanner_mode="funnel",
                scan_run_id=scan_id,
            )
            result = strategy.evaluate(df, context)
            stage_counts["stage_3_evaluated"] += 1
            _track_symbol("evaluated_symbols", symbol)

            # Stage 4 - survivor-only 4H trigger (Phase 5.1). Fetch 4H ONLY when
            # the strategy said WATCH (monthly/weekly/daily valid, trigger
            # missing) and the expensive gate is explicitly enabled. Daily data
            # is reused; one extra bounded call per WATCH survivor at most.
            if use_4h and result.decision == StrategyDecision.WATCH:
                df_4h = await _fetch_4h(fmp, symbol)
                api_call_counts["four_hour_fetches"] += 1
                stage_counts["stage_4_4h_fetched"] += 1
                if df_4h is not None and not df_4h.empty:
                    context_4h = StrategyContext(
                        symbol=symbol,
                        pattern_code=pattern_code,
                        config={**pattern_config, "enable_4h_trigger": True},
                        scanner_mode="funnel",
                        scan_run_id=scan_id,
                        data_meta={"df_4h": df_4h},
                    )
                    result = strategy.evaluate(df, context_4h)

            await mark_seen_today(symbol, scan_date)

            if result.decision == StrategyDecision.ENTER:
                stage_counts["enter_count"] += 1
                _track_symbol("enter_symbols", symbol)
                await _maybe_save(result, df, _provenance_for, stage_counts)
            elif result.decision == StrategyDecision.WATCH:
                # Phase 5.2: WATCH candidates are valuable decision-support data.
                # Persist them (with a decision card) when enabled; outcome
                # tracking only ever consumes verdict='ENTER', so WATCH rows are
                # inspectable history, not entries.
                stage_counts["watch_count"] += 1
                _track_symbol("watch_symbols", symbol)
                if persist_watch and await _maybe_save(result, df, _provenance_for, stage_counts):
                    stage_counts["watch_saved_count"] += 1
            else:
                # Low-quality AVOID/REJECT are never persisted unless debug mode.
                stage_counts["reject_count"] += 1
                tracker.add(
                    symbol, "evaluation",
                    result.rejection_reason or "avoided",
                )
                if settings.DEBUG_SAVE_AVOID:
                    await _maybe_save(result, df, _provenance_for, stage_counts)
        except Exception as exc:  # never let one symbol abort the run
            logger.error("Funnel eval failed for %s: %s", symbol, exc)
            tracker.add(symbol, "evaluation", "eval_error")

    finished_at = datetime.utcnow()
    telemetry = assemble_telemetry(
        pattern_code=pattern_code,
        scanner_config=scfg,
        config_summary=build_config_summary(pattern_config, scfg, limit),
        started_at=started_at,
        finished_at=finished_at,
        stage_counts=stage_counts,
        tracker=tracker,
        api_call_counts=api_call_counts,
        dry_run=False,
        extra_notes=extra_notes,
        market_data_provider=provider_name,
        result_symbols=result_symbols,
    )
    await _persist_telemetry(scan_id, "completed", stage_counts, telemetry)
    if scan_id:
        await event_bus.publish(scan_id, {"type": "finished", "telemetry": telemetry})
    return _summary(telemetry, stage_counts, dry_run=False)


async def _fetch_4h(fmp: Any, symbol: str) -> Optional[pd.DataFrame]:
    """Fetch + normalize 4H bars for ONE survivor. Never raises.

    Returns None when the endpoint is unsupported/empty so the caller keeps the
    WATCH decision instead of failing the scan. No fake data is ever created.
    """
    try:
        payload = await fmp.fetch_historical_4h(symbol)
        if not payload.get("historical"):
            return None
        return to_dataframe(payload)
    except Exception as exc:
        logger.warning("4H fetch/normalize failed for %s: %s", symbol, exc)
        return None


async def _maybe_save(
    result: StrategyResult,
    df: Optional[pd.DataFrame],
    provenance_for,
    stage_counts: Dict[str, int],
) -> bool:
    """Persist a signal via the canonical immutable pipeline.

    Phase 5.2: a deterministic decision card is attached at
    `details.decision_card` (built only from StrategyResult fields — nothing is
    invented). Phase 7B: the card is added BEFORE the provenance/evidence
    snapshot is built so the immutable fingerprint covers the full persisted
    evidence; signal + provenance + scan occurrence link share one
    transaction. A repeated exact fingerprint is deduplicated (linked, never
    re-created or overwritten). Returns True when the signal was persisted or
    linked.
    """
    try:
        details = dict(result.details or {})
        details["decision_card"] = build_decision_card(result)
        provenance = provenance_for(result, df, details)
        save_result = await save_signal(
            symbol=result.symbol,
            pattern_code=result.pattern_code,
            verdict=result.verdict,
            score=result.score,
            reason=result.reason,
            details=details,
            snapshot_date=date.fromisoformat(result.details["snapshot_date"]),
            provenance=provenance,
        )
        if save_result.get("created_new_signal"):
            stage_counts["signals_created"] += 1
        else:
            stage_counts["signals_deduplicated"] += 1
        stage_counts["signals_linked"] += 1
        return True
    except Exception as exc:
        logger.error("Failed to save funnel signal for %s: %s", result.symbol, exc)
        return False


async def _persist_telemetry(
    scan_id: str,
    status: str,
    stage_counts: Dict[str, int],
    telemetry: Optional[Dict[str, Any]],
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Finalize the canonical scan-run row (counts + telemetry + status +
    safe error identity for failed runs)."""
    try:
        await finalize_scan_run(
            scan_run_id=scan_id,
            status=status,
            scanned_count=stage_counts["stage_3_evaluated"],
            enter_count=stage_counts["enter_count"],
            rejected_count=stage_counts["reject_count"],
            telemetry=telemetry,
            error_code=error_code,
            error_message=error_message,
        )
    except Exception as exc:
        logger.error("Failed to persist funnel telemetry: %s", exc)


def _summary(
    telemetry: Dict[str, Any], stage_counts: Dict[str, int], dry_run: bool
) -> Dict[str, Any]:
    return {
        "success": True,
        "scanner_version": telemetry["scanner_version"],
        "market_data_provider": telemetry.get("market_data_provider"),
        "dry_run": dry_run,
        "scanned_count": stage_counts["stage_3_evaluated"],
        "enter_count": stage_counts["enter_count"],
        "rejected_count": stage_counts["reject_count"],
        # Bounded result visibility (capped at RESULT_SYMBOLS_CAP each).
        "enter_symbols": telemetry.get("enter_symbols", []),
        "watch_symbols": telemetry.get("watch_symbols", []),
        "evaluated_symbols": telemetry.get("evaluated_symbols", []),
        "stage_counts": stage_counts,
        "telemetry": telemetry,
    }
