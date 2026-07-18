"""Pure aggregation of many outcomes into honest summary statistics.

Guiding principle (Evidence Engine): never present a strategy as useful without
BOTH a sample size and a baseline comparison. Every aggregate here always
reports `sample_size`, and baseline deltas sit next to the raw averages.

Input: a list of "outcome records" (plain dicts), each shaped like:

    {
        "side": "LONG",
        "pattern_code": "sma150_bounce",
        "symbol": "AAPL",
        "ret_by_window": {1: 0.4, 3: 1.1, 5: None, 10: 2.0, 20: -0.5},
        "same_ticker_buy_hold": {"1D": 0.4, ...},          # labeled map
        "benchmark_returns": {"SPY": {"1D": 0.2, ...}, "QQQ": {...}},
        "simulated_r": 1.2 or None,
        "mfe": 3.1 or None,
        "mae": -1.2 or None,
        "outcome_status": "calculated",
    }

All returns are PERCENT. Functions are pure (no I/O).
"""

from statistics import mean, median
from typing import Any, Dict, List, Optional

from app.workers.outcomes.calculator import HOLDING_WINDOWS, window_label


def _mean_or_none(values: List[float]) -> Optional[float]:
    return mean(values) if values else None


def _median_or_none(values: List[float]) -> Optional[float]:
    return median(values) if values else None


def _profit_factor(returns: List[float]) -> Optional[float]:
    """Sum of gains / absolute sum of losses.

    None when there are no losing trades (undefined / infinite) or no sample,
    so callers must not treat a missing value as "great".
    """
    gains = sum(r for r in returns if r > 0)
    losses = sum(r for r in returns if r < 0)
    if losses == 0:
        return None
    return gains / abs(losses)


def _baseline_delta_mean(
    records: List[Dict[str, Any]],
    window: int,
    baseline_getter,
) -> Optional[float]:
    """Mean of (signal_return - baseline_return) over records where both exist."""
    label = window_label(window)
    deltas: List[float] = []
    for rec in records:
        sig = (rec.get("ret_by_window") or {}).get(window)
        base = baseline_getter(rec, label)
        if sig is not None and base is not None:
            deltas.append(sig - base)
    return _mean_or_none(deltas)


def _same_ticker_getter(rec: Dict[str, Any], label: str) -> Optional[float]:
    return (rec.get("same_ticker_buy_hold") or {}).get(label)


def _benchmark_getter(symbol: str):
    def getter(rec: Dict[str, Any], label: str) -> Optional[float]:
        bench = rec.get("benchmark_returns") or {}
        return (bench.get(symbol) or {}).get(label)

    return getter


def aggregate_outcomes(
    outcomes: List[Dict[str, Any]],
    window: int,
) -> Dict[str, Any]:
    """Aggregate a flat list of outcome records for a single holding window.

    Only records with a non-None return for `window` count toward the sample.
    """
    records = [
        rec
        for rec in outcomes
        if (rec.get("ret_by_window") or {}).get(window) is not None
    ]
    returns = [rec["ret_by_window"][window] for rec in records]

    sample_size = len(returns)
    rs = [rec["simulated_r"] for rec in records if rec.get("simulated_r") is not None]
    mfes = [rec["mfe"] for rec in records if rec.get("mfe") is not None]
    maes = [rec["mae"] for rec in records if rec.get("mae") is not None]

    wins = sum(1 for r in returns if r > 0)

    return {
        "window": window_label(window),
        "sample_size": sample_size,
        "win_rate": (wins / sample_size) if sample_size else None,
        "avg_return": _mean_or_none(returns),
        "median_return": _median_or_none(returns),
        "avg_r": _mean_or_none(rs),
        "profit_factor": _profit_factor(returns),
        "avg_mfe": _mean_or_none(mfes),
        "avg_mae": _mean_or_none(maes),
        "baseline_delta_vs_same_ticker": _baseline_delta_mean(
            records, window, _same_ticker_getter
        ),
        "baseline_delta_vs_spy": _baseline_delta_mean(
            records, window, _benchmark_getter("SPY")
        ),
        "baseline_delta_vs_qqq": _baseline_delta_mean(
            records, window, _benchmark_getter("QQQ")
        ),
    }


def aggregate_all_windows(
    outcomes: List[Dict[str, Any]],
    windows: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """Aggregate across every holding window."""
    windows = windows or HOLDING_WINDOWS
    return [aggregate_outcomes(outcomes, w) for w in windows]


def group_and_aggregate(
    outcomes: List[Dict[str, Any]],
    group_by: List[str],
    window: int,
) -> List[Dict[str, Any]]:
    """Group outcomes by the given keys, then aggregate each group for `window`.

    `group_by` keys are read directly from each record (e.g. ["pattern_code",
    "side", "symbol"]). Records missing a key are grouped under None.
    """
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for rec in outcomes:
        key = tuple(rec.get(k) for k in group_by)
        groups.setdefault(key, []).append(rec)

    results: List[Dict[str, Any]] = []
    for key, recs in groups.items():
        agg = aggregate_outcomes(recs, window)
        for i, k in enumerate(group_by):
            agg[k] = key[i]
        results.append(agg)

    # Stable, readable ordering: largest sample first.
    results.sort(key=lambda r: r["sample_size"], reverse=True)
    return results
