"""Outcome calculation service (Phase 2).

Two layers:
  * build_outcome_from_frames(...)  - PURE: takes DataFrames + a signal and
    produces a complete outcome record. Fully unit-testable, no I/O.
  * calculate_outcomes_for_signals(...) - async orchestration: loads signals
    from the DB, fetches OHLCV (symbol + SPY/QQQ) via the configured
    MarketDataProvider, calls the pure builder, and persists. One symbol failing
    never aborts the whole run.

SAFETY: this module performs provider API calls when the async orchestrator
runs, but it is only ever invoked on demand (admin endpoint) and is bounded by
`limit`. It is NOT wired into the scheduler and is not enabled automatically.
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from app.workers.indicators import to_dataframe
from app.workers.outcomes.baselines import (
    baseline_delta,
    compute_benchmark_returns,
    to_labeled,
)
from app.workers.outcomes.calculator import (
    CALCULATION_VERSION,
    HOLDING_WINDOWS,
    MAX_WINDOW,
    OUTCOME_COVERAGE_VERSION,
    compute_buy_hold_returns,
    compute_forward_returns,
    compute_mfe_mae,
    compute_simulated_r,
    compute_stop_target_hits,
    reference_price_role_for_verdict,
)
from app.workers.outcomes.persistence import (
    fetch_outcomes,
    get_signals_needing_outcomes,
    upsert_signal_outcome,
)


logger = logging.getLogger(__name__)

BENCHMARK_SYMBOLS = ["SPY", "QQQ"]


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def _entry_index(df: pd.DataFrame, snapshot_date: date) -> Optional[int]:
    """Index of the bar whose date == snapshot_date (the decision bar)."""
    if df is None or df.empty or snapshot_date is None:
        return None
    dates = df["date"].dt.date
    matches = df.index[dates == snapshot_date].tolist()
    return int(matches[0]) if matches else None


def _forward_series(df: pd.DataFrame, entry_idx: int, col: str) -> List[float]:
    """Values of `col` for bars strictly after entry_idx, capped at MAX_WINDOW."""
    return [float(v) for v in df[col].iloc[entry_idx + 1 : entry_idx + 1 + MAX_WINDOW].tolist()]


def _benchmark_returns_for(
    benchmark_frames: Dict[str, Optional[pd.DataFrame]],
    snapshot_date: date,
) -> Dict[str, Dict[str, Optional[float]]]:
    """Compute per-benchmark buy&hold return maps aligned to the entry date."""
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for name, bdf in benchmark_frames.items():
        idx = _entry_index(bdf, snapshot_date) if bdf is not None else None
        if idx is None:
            out[name] = compute_benchmark_returns(None, [])
            continue
        b_entry = float(bdf["close"].iloc[idx])
        b_forward = _forward_series(bdf, idx, "close")
        out[name] = compute_benchmark_returns(b_entry, b_forward)
    return out


def build_outcome_from_frames(
    signal: Dict[str, Any],
    symbol_df: Optional[pd.DataFrame],
    benchmark_frames: Optional[Dict[str, Optional[pd.DataFrame]]] = None,
) -> Dict[str, Any]:
    """Build a complete outcome record for one signal. PURE (no I/O).

    `signal` must contain: signal_id, symbol, pattern_code, snapshot_date,
    created_at, details. Side/stop/target/invalidation are read from
    `details` when present (sma150_bounce has none -> LONG, no stop/target).
    """
    benchmark_frames = benchmark_frames or {}
    details = signal.get("details") or {}
    side = (details.get("side") or "LONG").upper()
    stop_price = details.get("stop_price")
    target_price = details.get("target_price")
    invalidation = details.get("invalidation")
    snapshot_date = _as_date(signal.get("snapshot_date"))
    signal_timestamp = signal.get("created_at") or signal.get("snapshot_date")

    # Phase 7B: freeze the exact version identity being evaluated. Legacy
    # signals without a provenance row keep NULLs (never inferred/faked).
    prov = signal.get("provenance") or {}

    # Phase 8.1A: the verdict is copied from the immutable signal row ONLY.
    # A WATCH outcome's reference price is the candidate-observation price —
    # the same decision-bar close outcome.v1 already uses — never an invented
    # later entry. Unknown verdict (legacy caller) stays None.
    signal_verdict = signal.get("verdict")

    base_record: Dict[str, Any] = {
        "signal_id": signal["signal_id"],
        "symbol": signal["symbol"],
        "pattern_code": signal.get("pattern_code"),
        "scan_run_id": prov.get("scan_run_id"),
        "strategy_code": prov.get("strategy_code"),
        "strategy_version": prov.get("strategy_version"),
        "decision_policy_version": prov.get("decision_policy_version"),
        "config_hash": prov.get("config_hash"),
        "provenance_version": prov.get("provenance_version"),
        "signal_verdict": signal_verdict,
        "reference_price_role": reference_price_role_for_verdict(signal_verdict),
        "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
        "side": side if side in ("LONG", "SHORT") else "LONG",
        "signal_timestamp": signal_timestamp,
        "entry_price": None,
        "stop_price": stop_price,
        "target_price": target_price,
        "invalidation": invalidation,
        "ret_by_window": {w: None for w in HOLDING_WINDOWS},
        "benchmark_returns": None,
        "same_ticker_buy_hold": None,
        "max_favorable_excursion": None,
        "max_adverse_excursion": None,
        "hit_stop": None,
        "hit_target": None,
        "simulated_r": None,
        "outcome_status": "insufficient_data",
        "calculation_version": CALCULATION_VERSION,
    }

    entry_idx = _entry_index(symbol_df, snapshot_date)
    if entry_idx is None:
        return base_record

    entry_price = float(symbol_df["close"].iloc[entry_idx])
    forward_closes = _forward_series(symbol_df, entry_idx, "close")
    if not forward_closes:
        base_record["entry_price"] = entry_price
        return base_record

    forward_highs = _forward_series(symbol_df, entry_idx, "high")
    forward_lows = _forward_series(symbol_df, entry_idx, "low")

    ret_by_window = compute_forward_returns(entry_price, forward_closes, base_record["side"])
    same_ticker = to_labeled(compute_buy_hold_returns(entry_price, forward_closes))
    mfe, mae = compute_mfe_mae(entry_price, forward_highs, forward_lows, base_record["side"])
    hit_stop, hit_target = compute_stop_target_hits(
        entry_price, stop_price, target_price, forward_highs, forward_lows, base_record["side"]
    )

    # Simulated R uses the largest available window's realized return over risk.
    available = [w for w in HOLDING_WINDOWS if ret_by_window[w] is not None]
    exit_return = ret_by_window[max(available)] if available else None
    simulated_r = compute_simulated_r(entry_price, stop_price, exit_return, base_record["side"])

    benchmark_returns = _benchmark_returns_for(benchmark_frames, snapshot_date)

    base_record.update(
        {
            "entry_price": entry_price,
            "ret_by_window": ret_by_window,
            "benchmark_returns": benchmark_returns,
            "same_ticker_buy_hold": same_ticker,
            "max_favorable_excursion": mfe,
            "max_adverse_excursion": mae,
            "hit_stop": hit_stop,
            "hit_target": hit_target,
            "simulated_r": simulated_r,
            "outcome_status": "calculated",
        }
    )
    return base_record


async def _fetch_frame(provider, symbol: str, timeseries: int = 400) -> Optional[pd.DataFrame]:
    """Fetch one symbol's daily frame via the provider-neutral interface."""
    try:
        data = await provider.get_daily_history(symbol, timeseries=timeseries)
        df = to_dataframe(data)
        return df if not df.empty else None
    except Exception as exc:
        logger.warning("Failed to fetch/convert history for %s: %s", symbol, exc)
        return None


async def calculate_outcomes_for_signals(
    provider,
    limit: int = 100,
    pattern_code: Optional[str] = None,
    include_recalc: bool = False,
) -> Dict[str, Any]:
    """Load signals needing outcomes, compute, and persist. Returns a summary.

    Bounded by `limit`. Never aborts on a single symbol failure.
    """
    signals = await get_signals_needing_outcomes(
        limit=limit, pattern_code=pattern_code, include_recalc=include_recalc
    )
    summary = {
        "signals_considered": len(signals),
        "calculated": 0,
        "insufficient_data": 0,
        "errors": 0,
    }
    if not signals:
        logger.info("No signals need outcome calculation")
        return summary

    # Fetch benchmarks once for the whole run.
    benchmark_frames: Dict[str, Optional[pd.DataFrame]] = {}
    for bench in BENCHMARK_SYMBOLS:
        benchmark_frames[bench] = await _fetch_frame(provider, bench)

    # Cache per-symbol frames (a symbol may have multiple signals).
    symbol_cache: Dict[str, Optional[pd.DataFrame]] = {}

    for signal in signals:
        symbol = signal["symbol"]
        try:
            if symbol not in symbol_cache:
                symbol_cache[symbol] = await _fetch_frame(provider, symbol)
            symbol_df = symbol_cache[symbol]

            record = build_outcome_from_frames(signal, symbol_df, benchmark_frames)
            await upsert_signal_outcome(record)

            status = record["outcome_status"]
            if status == "calculated":
                summary["calculated"] += 1
            else:
                summary["insufficient_data"] += 1
        except Exception as exc:
            summary["errors"] += 1
            logger.error("Outcome calculation failed for %s: %s", symbol, exc)
            try:
                await upsert_signal_outcome(
                    {
                        "signal_id": signal["signal_id"],
                        "symbol": symbol,
                        "pattern_code": signal.get("pattern_code"),
                        "side": "LONG",
                        "signal_timestamp": signal.get("created_at")
                        or signal.get("snapshot_date"),
                        "ret_by_window": {},
                        "outcome_status": "error",
                        "calculation_version": CALCULATION_VERSION,
                        "signal_verdict": signal.get("verdict"),
                        "reference_price_role": reference_price_role_for_verdict(
                            signal.get("verdict")
                        ),
                        "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
                    }
                )
            except Exception:
                logger.error("Also failed to persist error outcome for %s", symbol)

    logger.info("Outcome calculation summary: %s", summary)
    return summary


# Re-export for convenience.
__all__ = [
    "build_outcome_from_frames",
    "calculate_outcomes_for_signals",
    "fetch_outcomes",
]
