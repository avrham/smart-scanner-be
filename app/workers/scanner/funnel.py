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
  * Expensive stages (Stage 4 / 4H) are DISABLED; the hook is a documented no-op.
  * The survivor set fed to the (expensive) history fetch is bounded by `limit`
    / max_universe_size to prevent broad FMP usage.
"""

import asyncio
import json
import logging
from collections import Counter
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.workers.fmp_client import FMPClient
from app.workers.indicators import to_dataframe, validate_dataframe
from app.workers.patterns.config import resolve_pattern_config
from app.workers.strategies import (
    StrategyContext,
    StrategyDecision,
    StrategyResult,
    get_strategy,
)
from app.workers.persistence import (
    get_universe_tickers,
    log_pattern_run,
    mark_seen_today,
    save_signal,
    was_seen_today,
)
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
    "scanner_version": SCANNER_VERSION,
}

MIN_BARS = 200


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
        "data_source": "tickers_cache + fmp_historical",
        "dry_run": dry_run,
        "notes": extra_notes,
    }
    telemetry.update(tracker.as_dict())
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
    fmp: Optional[FMPClient],
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
    scfg = _merge_scanner_config(scanner_config)
    tracker = RejectionTracker(sample_limit=int(scfg["sample_rejections_limit"]))
    extra_notes: List[str] = []
    api_call_counts: Dict[str, int] = {"historical_fetches": 0}

    # Phase 4/5: resolve the strategy first (fails fast on unknown pattern) and
    # use ITS defaults for the Phase 1 config resolver (DB overrides on top).
    strategy = get_strategy(pattern_code)
    pattern_config = await resolve_pattern_config(pattern_code, strategy.default_config())
    liq = pattern_config.get("min_liquidity_filters", {}) or {}
    min_market_cap = float(liq.get("min_market_cap", 200_000_000))
    min_daily_volume = float(liq.get("min_daily_volume", 200_000))
    min_price = float(pattern_config.get("min_price", 1.0))

    if scan_id:
        await event_bus.publish(scan_id, {"type": "stage", "stage": "universe_build"})

    # Stage 0 - universe from the ticker cache (real values, includes NULLs).
    universe = await get_universe_tickers()
    stage_counts: Dict[str, int] = {
        "stage_0_universe": len(universe),
        "stage_1_liquidity_passed": 0,
        "stage_2_prefilter_passed": 0,
        "stage_3_evaluated": 0,
        "enter_count": 0,
        "watch_count": 0,   # not supported by sma150_bounce (documented)
        "reject_count": 0,
    }

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

    if not scfg["enable_expensive_stages"]:
        extra_notes.append("expensive stages (4H) disabled in Phase 3")

    if dry_run:
        extra_notes.append("dry_run: stages 2-3 skipped, no FMP calls, no writes")
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
        )
        await _persist_telemetry(pattern_code, stage_counts, telemetry, started_at)
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
            await mark_seen_today(symbol, scan_date)

            if result.decision == StrategyDecision.ENTER:
                stage_counts["enter_count"] += 1
                await _maybe_save(result)
            elif result.decision == StrategyDecision.WATCH:
                # Not produced by sma150_bounce today; supported for future
                # strategies. WATCH candidates are counted but not persisted yet.
                stage_counts["watch_count"] += 1
            else:
                stage_counts["reject_count"] += 1
                tracker.add(
                    symbol, "evaluation",
                    result.rejection_reason or "avoided",
                )
                if settings.DEBUG_SAVE_AVOID:
                    await _maybe_save(result)
        except Exception as exc:  # never let one symbol abort the run
            logger.error("Funnel eval failed for %s: %s", symbol, exc)
            tracker.add(symbol, "evaluation", "eval_error")

    # Stage 4 hook - expensive data only for survivors (disabled in Phase 3).
    # Intentionally a no-op; wired here so a future strategy can request 4H data
    # for the ENTER/WATCH survivors without restructuring the funnel.

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
    )
    await _persist_telemetry(pattern_code, stage_counts, telemetry, started_at)
    if scan_id:
        await event_bus.publish(scan_id, {"type": "finished", "telemetry": telemetry})
    return _summary(telemetry, stage_counts, dry_run=False)


async def _maybe_save(result: StrategyResult) -> None:
    """Persist a signal via the existing pipeline (Phase 2 compatible).

    Persists the strategy's `details` verbatim so downstream (UI + Phase 2
    outcome tracking) sees exactly the same payload as before Phase 4.
    """
    try:
        await save_signal(
            symbol=result.symbol,
            pattern_code=result.pattern_code,
            verdict=result.verdict,
            score=result.score,
            reason=result.reason,
            details=result.details,
            snapshot_date=date.fromisoformat(result.details["snapshot_date"]),
        )
    except Exception as exc:
        logger.error("Failed to save funnel signal for %s: %s", result.symbol, exc)


async def _persist_telemetry(
    pattern_code: str,
    stage_counts: Dict[str, int],
    telemetry: Dict[str, Any],
    started_at: datetime,
) -> None:
    try:
        await log_pattern_run(
            pattern_code=pattern_code,
            scanned_count=stage_counts["stage_3_evaluated"],
            enter_count=stage_counts["enter_count"],
            rejected_count=stage_counts["reject_count"],
            notes=json.dumps(telemetry),
            run_started_at=started_at,
        )
    except Exception as exc:
        logger.error("Failed to persist funnel telemetry: %s", exc)


def _summary(
    telemetry: Dict[str, Any], stage_counts: Dict[str, int], dry_run: bool
) -> Dict[str, Any]:
    return {
        "success": True,
        "scanner_version": telemetry["scanner_version"],
        "dry_run": dry_run,
        "scanned_count": stage_counts["stage_3_evaluated"],
        "enter_count": stage_counts["enter_count"],
        "rejected_count": stage_counts["reject_count"],
        "stage_counts": stage_counts,
        "telemetry": telemetry,
    }
