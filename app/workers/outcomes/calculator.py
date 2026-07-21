"""Pure outcome math for signals.

Every function here is deterministic and free of DB/network I/O so it can be
unit-tested in isolation. All returns/excursions are expressed in PERCENT
(e.g. 3.5 means +3.5%). "side" is 'LONG' or 'SHORT'.

Conventions:
  * "forward bars" are the bars strictly AFTER the entry bar, oldest first.
  * A holding window of N uses the Nth forward bar's close as the exit.
  * If there are fewer than N forward bars, that window's return is None
    (we never invent data).
"""

from typing import Dict, List, Optional, Tuple


HOLDING_WINDOWS: List[int] = [1, 3, 5, 10, 20]
MAX_WINDOW: int = max(HOLDING_WINDOWS)
CALCULATION_VERSION: str = "outcome.v1"

# Phase 8.1A: outcome COVERAGE identity, separate from the calculation math.
# The return formulas above are identical for ENTER and WATCH; what differs is
# what the frozen reference price MEANS. An ENTER outcome's reference is the
# decision-bar close used as the entry reference. A WATCH outcome's reference
# is "the market price when the candidate was observed" — it is NOT an
# executed/recommended entry and is never moved forward to a later trigger.
OUTCOME_COVERAGE_VERSION: str = "candidate_outcomes.v1"

REFERENCE_PRICE_ROLES: Dict[str, str] = {
    "ENTER": "entry_reference",
    "WATCH": "candidate_observation",
}


def reference_price_role_for_verdict(verdict: Optional[str]) -> Optional[str]:
    """Reference-price role for a verdict; None for unknown/legacy (never
    inferred from strategy name, score or return)."""
    if verdict is None:
        return None
    return REFERENCE_PRICE_ROLES.get(str(verdict).upper())

LONG = "LONG"
SHORT = "SHORT"


def window_label(days: int) -> str:
    """Canonical label for a holding window (used as JSON keys), e.g. 5 -> '5D'."""
    return f"{days}D"


def _validate_side(side: str) -> str:
    if side not in (LONG, SHORT):
        raise ValueError(f"side must be 'LONG' or 'SHORT', got {side!r}")
    return side


def signed_return_pct(entry_price: float, exit_price: float, side: str) -> float:
    """Side-adjusted percentage return between entry and exit.

    LONG profits when price rises; SHORT profits when price falls.
    """
    _validate_side(side)
    if entry_price is None or entry_price <= 0:
        raise ValueError("entry_price must be a positive number")
    raw = (exit_price - entry_price) / entry_price * 100.0
    return raw if side == LONG else -raw


def compute_forward_returns(
    entry_price: float,
    forward_closes: List[float],
    side: str,
    windows: Optional[List[int]] = None,
) -> Dict[int, Optional[float]]:
    """Side-adjusted return (%) at each holding window.

    Returns a dict {window_days: pct_or_None}. A window is None when there are
    not enough forward closes to reach it.
    """
    windows = windows or HOLDING_WINDOWS
    out: Dict[int, Optional[float]] = {}
    for w in windows:
        if len(forward_closes) >= w:
            out[w] = signed_return_pct(entry_price, forward_closes[w - 1], side)
        else:
            out[w] = None
    return out


def compute_buy_hold_returns(
    entry_price: float,
    forward_closes: List[float],
    windows: Optional[List[int]] = None,
) -> Dict[int, Optional[float]]:
    """Naive LONG buy & hold return (%) per window for the same instrument.

    This is intentionally always LONG: the baseline question is "would simply
    buying and holding have done better?" regardless of the signal's side.
    """
    return compute_forward_returns(entry_price, forward_closes, LONG, windows)


def compute_mfe_mae(
    entry_price: float,
    forward_highs: List[float],
    forward_lows: List[float],
    side: str,
    window: int = MAX_WINDOW,
) -> Tuple[Optional[float], Optional[float]]:
    """Maximum favorable / adverse excursion (%) over the first `window` bars.

    MFE is the best unrealized move in the signal's favor (>= 0 typically);
    MAE is the worst unrealized move against it (<= 0 typically). Returns
    (None, None) when there are no forward bars.

    For LONG: favorable uses highs, adverse uses lows.
    For SHORT: favorable uses lows (price falling), adverse uses highs.
    """
    _validate_side(side)
    if entry_price is None or entry_price <= 0:
        raise ValueError("entry_price must be a positive number")

    n = min(window, len(forward_highs), len(forward_lows))
    if n <= 0:
        return None, None

    highs = forward_highs[:n]
    lows = forward_lows[:n]

    if side == LONG:
        favorable = [(h - entry_price) / entry_price * 100.0 for h in highs]
        adverse = [(low - entry_price) / entry_price * 100.0 for low in lows]
    else:  # SHORT
        favorable = [(entry_price - low) / entry_price * 100.0 for low in lows]
        adverse = [(entry_price - h) / entry_price * 100.0 for h in highs]

    return max(favorable), min(adverse)


def compute_stop_target_hits(
    entry_price: float,
    stop_price: Optional[float],
    target_price: Optional[float],
    forward_highs: List[float],
    forward_lows: List[float],
    side: str,
    window: int = MAX_WINDOW,
) -> Tuple[Optional[bool], Optional[bool]]:
    """Whether stop / target was touched within `window` forward bars.

    Returns (hit_stop, hit_target). Each element is None when the corresponding
    level is not defined (we cannot evaluate what does not exist).

    Note: this is intentionally path-agnostic within a bar (we cannot know
    intrabar ordering of high vs low), so both can be True for the same window.
    Downstream callers should treat that conservatively.
    """
    _validate_side(side)
    n = min(window, len(forward_highs), len(forward_lows))
    highs = forward_highs[:n]
    lows = forward_lows[:n]

    hit_stop: Optional[bool] = None
    hit_target: Optional[bool] = None

    if stop_price is not None:
        if side == LONG:
            hit_stop = any(low <= stop_price for low in lows)
        else:
            hit_stop = any(h >= stop_price for h in highs)

    if target_price is not None:
        if side == LONG:
            hit_target = any(h >= target_price for h in highs)
        else:
            hit_target = any(low <= target_price for low in lows)

    return hit_stop, hit_target


def compute_simulated_r(
    entry_price: float,
    stop_price: Optional[float],
    exit_return_pct: Optional[float],
    side: str,
) -> Optional[float]:
    """Simplified R multiple = end-of-window return / initial risk.

    Risk is the distance from entry to stop, expressed in percent. This is a
    deliberately simple, non-path-dependent R (it uses the realized
    end-of-window return over the initial planned risk). Returns None when no
    stop is defined or risk is zero.
    """
    _validate_side(side)
    if stop_price is None or exit_return_pct is None or entry_price is None or entry_price <= 0:
        return None
    risk_pct = abs(entry_price - stop_price) / entry_price * 100.0
    if risk_pct == 0:
        return None
    return exit_return_pct / risk_pct
