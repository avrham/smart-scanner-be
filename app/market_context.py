"""Deterministic market-context builders for the Smart Scanner product API.

PURE: no DB, no network, no provider. Given already-fetched daily bars, every
output here is a deterministic function of stored prices and volumes.

WHAT THIS IS NOT
----------------
This is context that sits BESIDE the strategy result, never inside it. Nothing
here feeds back into the Wyckoff verdict, the attention tier, or any gate. A
symbol can be high_attention with weak relative strength — that disagreement is
information, and it is deliberately not blended away into one number.

TRUTH IN NAMING
---------------
The store holds daily bars for the 25 frozen candidate symbols and nothing else
— no index series, no sector ETF, no sector/industry metadata (the `tickers`
table is empty and has no sector column at all). So:

  * "relative strength" here is measured against the SCANNER UNIVERSE MEDIAN,
    not against the market. It is reported as `comparator=scanner_universe_median`
    and must never be presented as market-relative performance.
  * breadth computed over those 25 symbols is `scanner_universe` breadth, NOT
    market breadth.
  * benchmark and sector context report `unavailable` with a reason, rather
    than substituting a proxy that would read as something it is not.

THRESHOLDS
----------
Every threshold below is a documented round number chosen a priori (equal
thirds for cross-sectional rank; 1.5x / 0.7x for volume). None was fitted to
outcomes, and they must not be tuned against the four completed campaigns.
"""

from __future__ import annotations

from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence

MARKET_CONTEXT_CONTRACT_VERSION = "smart_scanner_market_context.v1"

# ---- availability states --------------------------------------------------- #
# Explicit, never null-as-a-meaning: product behaviour depends on telling
# "we looked and there is nothing" apart from "we cannot look".
STATUS_AVAILABLE = "available"
STATUS_INSUFFICIENT_HISTORY = "insufficient_history"
STATUS_UNAVAILABLE = "unavailable"

CONTEXT_STATUSES = (STATUS_AVAILABLE, STATUS_INSUFFICIENT_HISTORY, STATUS_UNAVAILABLE)

# ---- relative strength ------------------------------------------------------ #
COMPARATOR_UNIVERSE_MEDIAN = "scanner_universe_median"

RS_OUTPERFORMING = "outperforming"
RS_IN_LINE = "in_line"
RS_UNDERPERFORMING = "underperforming"
RS_CATEGORIES = (RS_OUTPERFORMING, RS_IN_LINE, RS_UNDERPERFORMING)

# Trading-day horizons. 20D is the headline: it is roughly one trading month,
# less noisy than 5D, and matches the longest matured outcome horizon available.
RS_HORIZONS: Sequence[int] = (5, 20, 60)
RS_PRIMARY_HORIZON = 20

# Equal thirds of the cross-sectional rank. Chosen a priori, not fitted.
RS_UPPER_PERCENTILE = 100.0 * 2.0 / 3.0
RS_LOWER_PERCENTILE = 100.0 / 3.0

# ---- volume ----------------------------------------------------------------- #
VOLUME_ELEVATED = "elevated"
VOLUME_NORMAL = "normal"
VOLUME_LIGHT = "light"
VOLUME_CATEGORIES = (VOLUME_ELEVATED, VOLUME_NORMAL, VOLUME_LIGHT)

VOLUME_AVERAGE_WINDOW = 20      # trailing bars, excluding the session itself
VOLUME_PERCENTILE_WINDOW = 60   # trailing bars for the rank
VOLUME_ELEVATED_RATIO = 1.5     # conventional round numbers, chosen a priori
VOLUME_LIGHT_RATIO = 0.7

# ---- breadth ---------------------------------------------------------------- #
BREADTH_SCOPE_SCANNER_UNIVERSE = "scanner_universe"
BREADTH_TREND_WINDOW = 50


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def _closes(bars: Sequence[Dict[str, Any]]) -> List[float]:
    return [float(b["close"]) for b in bars]


def _pct_change(newer: float, older: float) -> Optional[float]:
    """Percent change, guarding a zero/negative reference price."""
    if older is None or newer is None or older <= 0:
        return None
    return (newer / older - 1.0) * 100.0


def horizon_return_pct(bars: Sequence[Dict[str, Any]], days: int) -> Optional[float]:
    """Return over `days` trading bars ending at the last bar supplied.

    `bars` must be ascending by date and already truncated to the session.
    Needs days+1 bars; fewer means insufficient history, not zero.
    """
    if len(bars) < days + 1:
        return None
    closes = _closes(bars)
    return _pct_change(closes[-1], closes[-1 - days])


def percentile_rank(value: float, population: Sequence[float]) -> Optional[float]:
    """Share of the population strictly below `value`, as 0..100.

    With a single-member population there is nothing to rank against, which is
    reported as None rather than a meaningless 0 or 100.
    """
    if not population or len(population) < 2:
        return None
    below = sum(1 for v in population if v < value)
    return 100.0 * below / (len(population) - 1)


def classify_relative_strength(percentile: Optional[float]) -> Optional[str]:
    if percentile is None:
        return None
    if percentile >= RS_UPPER_PERCENTILE:
        return RS_OUTPERFORMING
    if percentile <= RS_LOWER_PERCENTILE:
        return RS_UNDERPERFORMING
    return RS_IN_LINE


def classify_volume(relative_volume: Optional[float]) -> Optional[str]:
    if relative_volume is None:
        return None
    if relative_volume >= VOLUME_ELEVATED_RATIO:
        return VOLUME_ELEVATED
    if relative_volume <= VOLUME_LIGHT_RATIO:
        return VOLUME_LIGHT
    return VOLUME_NORMAL


def _round(value: Optional[float], places: int = 2) -> Optional[float]:
    return None if value is None else round(value, places)


# --------------------------------------------------------------------------- #
# relative strength
# --------------------------------------------------------------------------- #

def build_relative_strength(
    symbol: str,
    bars_by_symbol: Dict[str, Sequence[Dict[str, Any]]],
) -> Dict[str, Any]:
    """Cross-sectional strength of one symbol against the scanner universe.

    `bars_by_symbol` holds ascending daily bars already truncated to the scan
    session for every symbol in the universe (including `symbol`).
    """
    own = bars_by_symbol.get(symbol) or []
    universe_size = len(bars_by_symbol)

    if not own:
        return {
            "status": STATUS_UNAVAILABLE,
            "reason": "no_stored_bars_for_symbol",
            "comparator": COMPARATOR_UNIVERSE_MEDIAN,
            "comparator_symbol_count": universe_size,
            "primary_horizon_days": RS_PRIMARY_HORIZON,
            "category": None,
            "excess_pct": None,
            "percentile": None,
            "horizons": [],
        }

    horizons: List[Dict[str, Any]] = []
    for days in RS_HORIZONS:
        own_return = horizon_return_pct(own, days)
        peers = [
            r for r in (
                horizon_return_pct(b, days) for b in bars_by_symbol.values()
            ) if r is not None
        ]
        if own_return is None or len(peers) < 2:
            horizons.append({
                "days": days,
                "status": STATUS_INSUFFICIENT_HISTORY,
                "return_pct": _round(own_return),
                "universe_median_pct": None,
                "excess_pct": None,
                "percentile": None,
                "category": None,
                "comparator_symbol_count": len(peers),
            })
            continue
        med = median(peers)
        pct = percentile_rank(own_return, peers)
        horizons.append({
            "days": days,
            "status": STATUS_AVAILABLE,
            "return_pct": _round(own_return),
            "universe_median_pct": _round(med),
            "excess_pct": _round(own_return - med),
            "percentile": _round(pct, 1),
            "category": classify_relative_strength(pct),
            "comparator_symbol_count": len(peers),
        })

    primary = next(
        (h for h in horizons if h["days"] == RS_PRIMARY_HORIZON), None
    )
    available = primary is not None and primary["status"] == STATUS_AVAILABLE
    return {
        "status": STATUS_AVAILABLE if available else STATUS_INSUFFICIENT_HISTORY,
        # Named so it can never be mistaken for market-relative performance.
        "comparator": COMPARATOR_UNIVERSE_MEDIAN,
        "comparator_symbol_count": universe_size,
        "primary_horizon_days": RS_PRIMARY_HORIZON,
        "category": primary["category"] if available else None,
        "excess_pct": primary["excess_pct"] if available else None,
        "percentile": primary["percentile"] if available else None,
        "horizons": horizons,
    }


# --------------------------------------------------------------------------- #
# volume
# --------------------------------------------------------------------------- #

def build_volume_context(bars: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Is participation unusual for this symbol, on its own recent history?

    Compares the session's volume with the mean of the preceding
    VOLUME_AVERAGE_WINDOW bars — the session itself is excluded so a spike is
    never diluted by its own value.
    """
    if len(bars) < VOLUME_AVERAGE_WINDOW + 1:
        return {
            "status": STATUS_INSUFFICIENT_HISTORY,
            "category": None,
            "relative_volume": None,
            "session_volume": None,
            "average_volume": None,
            "average_window_days": VOLUME_AVERAGE_WINDOW,
            "percentile": None,
            "percentile_window_days": VOLUME_PERCENTILE_WINDOW,
        }

    volumes = [float(b["volume"] or 0.0) for b in bars]
    session_volume = volumes[-1]
    window = volumes[-1 - VOLUME_AVERAGE_WINDOW:-1]
    average = sum(window) / len(window) if window else 0.0

    # A zero trailing average makes the ratio meaningless rather than infinite.
    relative = (session_volume / average) if average > 0 else None

    tail = volumes[-VOLUME_PERCENTILE_WINDOW:] if len(volumes) >= 2 else []
    pct = percentile_rank(session_volume, tail) if tail else None

    return {
        "status": STATUS_AVAILABLE if relative is not None else STATUS_INSUFFICIENT_HISTORY,
        "category": classify_volume(relative),
        "relative_volume": _round(relative),
        "session_volume": session_volume,
        "average_volume": _round(average, 0),
        "average_window_days": VOLUME_AVERAGE_WINDOW,
        "percentile": _round(pct, 1),
        "percentile_window_days": VOLUME_PERCENTILE_WINDOW,
    }


# --------------------------------------------------------------------------- #
# breadth — of the SCANNER UNIVERSE, not of the market
# --------------------------------------------------------------------------- #

def build_universe_breadth(
    bars_by_symbol: Dict[str, Sequence[Dict[str, Any]]],
) -> Dict[str, Any]:
    """How broadly the 25 scanned symbols are advancing.

    Deliberately NOT called market breadth: 25 large caps chosen for a Wyckoff
    qualification universe are not a representative sample of the US market.
    Widening this to a real breadth measure needs more symbols, which is a data
    expansion, not a calculation change.
    """
    symbols = [s for s, b in bars_by_symbol.items() if b]
    total = len(symbols)
    if total == 0:
        return {
            "status": STATUS_UNAVAILABLE,
            "reason": "no_stored_bars",
            "scope": BREADTH_SCOPE_SCANNER_UNIVERSE,
            "symbol_count": 0,
        }

    def _share(days: int) -> Optional[Dict[str, Any]]:
        rets = [
            r for r in (
                horizon_return_pct(bars_by_symbol[s], days) for s in symbols
            ) if r is not None
        ]
        if not rets:
            return None
        positive = sum(1 for r in rets if r > 0)
        return {
            "positive": positive,
            "measured": len(rets),
            "pct": _round(100.0 * positive / len(rets), 1),
        }

    above_trend: Optional[Dict[str, Any]] = None
    measured = 0
    above = 0
    for s in symbols:
        bars = bars_by_symbol[s]
        if len(bars) < BREADTH_TREND_WINDOW:
            continue
        closes = _closes(bars)
        sma = sum(closes[-BREADTH_TREND_WINDOW:]) / BREADTH_TREND_WINDOW
        measured += 1
        if closes[-1] > sma:
            above += 1
    if measured:
        above_trend = {
            "above": above,
            "measured": measured,
            "pct": _round(100.0 * above / measured, 1),
            "window_days": BREADTH_TREND_WINDOW,
        }

    return {
        "status": STATUS_AVAILABLE,
        "scope": BREADTH_SCOPE_SCANNER_UNIVERSE,
        "symbol_count": total,
        "positive_5d": _share(5),
        "positive_20d": _share(20),
        "above_trend": above_trend,
    }


# --------------------------------------------------------------------------- #
# the unavailable dimensions, reported honestly
# --------------------------------------------------------------------------- #

def build_benchmark_context() -> Dict[str, Any]:
    """No index or ETF series is stored — only the 25 candidate symbols — so
    there is nothing to measure against. Reported rather than proxied."""
    return {
        "status": STATUS_UNAVAILABLE,
        "reason": "no_benchmark_series_stored",
        "detail": (
            "Daily bars are stored only for the frozen candidate universe. "
            "Relative strength is measured against the scanner universe median."
        ),
    }


def build_sector_context() -> Dict[str, Any]:
    """The store has no sector/industry metadata at all — the `tickers` table is
    empty and carries no sector column — so peer/sector comparison would be
    invented, not derived."""
    return {
        "status": STATUS_UNAVAILABLE,
        "reason": "no_sector_metadata_stored",
        "detail": "No sector or industry data exists for the scanned symbols.",
    }


def build_market_context(
    symbol: str,
    bars_by_symbol: Dict[str, Sequence[Dict[str, Any]]],
    *,
    as_of_session: Optional[str],
) -> Dict[str, Any]:
    """The full per-symbol context object exposed by the Product API."""
    return {
        "contract_version": MARKET_CONTEXT_CONTRACT_VERSION,
        "as_of_session": as_of_session,
        "relative_strength": build_relative_strength(symbol, bars_by_symbol),
        "volume_context": build_volume_context(bars_by_symbol.get(symbol) or []),
        "benchmark_context": build_benchmark_context(),
        "sector_context": build_sector_context(),
    }


__all__ = [
    "MARKET_CONTEXT_CONTRACT_VERSION",
    "STATUS_AVAILABLE", "STATUS_INSUFFICIENT_HISTORY", "STATUS_UNAVAILABLE",
    "CONTEXT_STATUSES",
    "COMPARATOR_UNIVERSE_MEDIAN",
    "RS_OUTPERFORMING", "RS_IN_LINE", "RS_UNDERPERFORMING", "RS_CATEGORIES",
    "RS_HORIZONS", "RS_PRIMARY_HORIZON",
    "VOLUME_ELEVATED", "VOLUME_NORMAL", "VOLUME_LIGHT", "VOLUME_CATEGORIES",
    "VOLUME_AVERAGE_WINDOW", "VOLUME_PERCENTILE_WINDOW",
    "BREADTH_SCOPE_SCANNER_UNIVERSE", "BREADTH_TREND_WINDOW",
    "horizon_return_pct", "percentile_rank",
    "classify_relative_strength", "classify_volume",
    "build_relative_strength", "build_volume_context", "build_universe_breadth",
    "build_benchmark_context", "build_sector_context", "build_market_context",
]
