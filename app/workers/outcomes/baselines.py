"""Pure baseline calculations and signal-vs-baseline deltas.

Baselines are deliberately simple and honest:
  * same-ticker buy & hold (naive LONG hold of the signal's own symbol)
  * SPY buy & hold over the same window
  * QQQ buy & hold over the same window (when data is available)

All values are PERCENT. Functions are pure (no I/O). The service layer is
responsible for aligning benchmark bars to the signal's entry date and passing
the aligned forward closes in here.
"""

from typing import Dict, List, Optional

from app.workers.outcomes.calculator import (
    HOLDING_WINDOWS,
    compute_buy_hold_returns,
    window_label,
)


def compute_benchmark_returns(
    benchmark_entry_price: Optional[float],
    benchmark_forward_closes: List[float],
    windows: Optional[List[int]] = None,
) -> Dict[str, Optional[float]]:
    """Naive LONG buy & hold return (%) per window for a benchmark.

    Keyed by window label ("1D".."20D") so it serializes cleanly to JSONB.
    Returns an all-None map when the benchmark entry price is missing/invalid.
    """
    windows = windows or HOLDING_WINDOWS
    if not benchmark_entry_price or benchmark_entry_price <= 0:
        return {window_label(w): None for w in windows}
    by_window = compute_buy_hold_returns(
        benchmark_entry_price, benchmark_forward_closes, windows
    )
    return {window_label(w): by_window[w] for w in windows}


def to_labeled(
    by_window_days: Dict[int, Optional[float]]
) -> Dict[str, Optional[float]]:
    """Convert a {days: value} map to a {'ND': value} label map for JSONB."""
    return {window_label(w): v for w, v in by_window_days.items()}


def baseline_delta(
    signal_by_window: Dict[int, Optional[float]],
    baseline_labeled: Optional[Dict[str, Optional[float]]],
    windows: Optional[List[int]] = None,
) -> Dict[str, Optional[float]]:
    """Signal return minus baseline return, per window (PERCENT points).

    `signal_by_window` is keyed by int days; `baseline_labeled` is keyed by
    label ("1D"..). Delta is None when either side is missing for that window.
    """
    windows = windows or HOLDING_WINDOWS
    baseline_labeled = baseline_labeled or {}
    out: Dict[str, Optional[float]] = {}
    for w in windows:
        label = window_label(w)
        sig = signal_by_window.get(w)
        base = baseline_labeled.get(label)
        out[label] = (sig - base) if (sig is not None and base is not None) else None
    return out
