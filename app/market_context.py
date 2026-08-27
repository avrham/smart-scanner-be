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

import app.reference_market as rm

MARKET_CONTEXT_CONTRACT_VERSION = "smart_scanner_market_context.v2"

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
# Benchmark- and sector-relative strength
#
# FORMULATION (documented, not implied): for symbol S, reference R and horizon H
# trading sessions,
#
#     symbol_return_H    = (close_S(D) / close_S(D-H) - 1) * 100
#     reference_return_H = (close_R(D) / close_R(D-H) - 1) * 100
#     relative_return_H  = symbol_return_H - reference_return_H
#
# i.e. the arithmetic difference of simple percent returns over the SAME set of
# completed market sessions. It is a spread in percentage points, not a ratio
# and not an oscillator — this is never RSI.
#
# Alignment: both series are truncated at the same scan session before the
# window is taken, and a horizon resolves only when BOTH have H+1 bars. A
# reference with short history yields `insufficient_history`, never a silently
# shorter window.
# --------------------------------------------------------------------------- #

REL_OUTPERFORMING = "outperforming"
REL_IN_LINE = "in_line"
REL_UNDERPERFORMING = "underperforming"
REL_CATEGORIES = (REL_OUTPERFORMING, REL_IN_LINE, REL_UNDERPERFORMING)

# Display band in PERCENTAGE POINTS. A spread inside +/- this reads as in line,
# so a trivial difference does not flip a label. An a priori round number, not
# fitted to any outcome; the exact relative_return_pct is always returned too.
REL_NEUTRAL_BAND_PCT = 1.0


def classify_relative_return(relative_return_pct: Optional[float]) -> Optional[str]:
    if relative_return_pct is None:
        return None
    if relative_return_pct > REL_NEUTRAL_BAND_PCT:
        return REL_OUTPERFORMING
    if relative_return_pct < -REL_NEUTRAL_BAND_PCT:
        return REL_UNDERPERFORMING
    return REL_IN_LINE


def build_reference_relative_strength(
    symbol_bars: Sequence[Dict[str, Any]],
    reference_bars: Optional[Sequence[Dict[str, Any]]],
    *,
    reference_symbol: Optional[str],
    reference_kind: str,
    unavailable_reason: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Relative strength of one symbol against ONE named reference series."""
    base: Dict[str, Any] = {
        "reference_symbol": reference_symbol,
        "reference_kind": reference_kind,
        "primary_horizon_days": RS_PRIMARY_HORIZON,
        "category": None,
        "relative_return_pct": None,
        "horizons": [],
    }
    if extra:
        base.update(extra)

    if not reference_symbol or not reference_bars:
        base["status"] = STATUS_UNAVAILABLE
        base["reason"] = unavailable_reason or "no_reference_series_stored"
        return base
    if not symbol_bars:
        base["status"] = STATUS_UNAVAILABLE
        base["reason"] = "no_stored_bars_for_symbol"
        return base

    horizons: List[Dict[str, Any]] = []
    for days in RS_HORIZONS:
        own = horizon_return_pct(symbol_bars, days)
        ref = horizon_return_pct(reference_bars, days)
        if own is None or ref is None:
            horizons.append({
                "days": days,
                "status": STATUS_INSUFFICIENT_HISTORY,
                "symbol_return_pct": _round(own),
                "reference_return_pct": _round(ref),
                "relative_return_pct": None,
                "category": None,
            })
            continue
        relative = own - ref
        horizons.append({
            "days": days,
            "status": STATUS_AVAILABLE,
            "symbol_return_pct": _round(own),
            "reference_return_pct": _round(ref),
            "relative_return_pct": _round(relative),
            "category": classify_relative_return(relative),
        })

    primary = next((h for h in horizons if h["days"] == RS_PRIMARY_HORIZON), None)
    ok = primary is not None and primary["status"] == STATUS_AVAILABLE
    base["status"] = STATUS_AVAILABLE if ok else STATUS_INSUFFICIENT_HISTORY
    base["category"] = primary["category"] if ok else None
    base["relative_return_pct"] = primary["relative_return_pct"] if ok else None
    base["horizons"] = horizons
    return base


def build_benchmark_relative_strength(
    symbol: str,
    bars_by_symbol: Dict[str, Sequence[Dict[str, Any]]],
    reference_bars: Dict[str, Sequence[Dict[str, Any]]],
) -> Dict[str, Any]:
    """Against the designated broad-market benchmark, plus secondary references."""
    primary = rm.PRIMARY_BENCHMARK
    block = build_reference_relative_strength(
        bars_by_symbol.get(symbol) or [],
        reference_bars.get(primary),
        reference_symbol=primary,
        reference_kind=rm.REFERENCE_BROAD_MARKET,
        unavailable_reason="no_benchmark_series_stored",
        extra={"reference_name": (rm.REFERENCE_BY_SYMBOL.get(primary).name
                                  if primary in rm.REFERENCE_BY_SYMBOL else None)},
    )
    # Secondary broad references are reported ALONGSIDE — never as the benchmark.
    secondary = []
    for sym in rm.SECONDARY_BENCHMARKS:
        entry = rm.REFERENCE_BY_SYMBOL.get(sym)
        secondary.append(build_reference_relative_strength(
            bars_by_symbol.get(symbol) or [],
            reference_bars.get(sym),
            reference_symbol=sym,
            reference_kind=rm.REFERENCE_BROAD_MARKET,
            unavailable_reason="no_benchmark_series_stored",
            extra={"reference_name": entry.name if entry else None},
        ))
    block["secondary_references"] = secondary
    return block


def build_sector_relative_strength(
    symbol: str,
    bars_by_symbol: Dict[str, Sequence[Dict[str, Any]]],
    reference_bars: Dict[str, Sequence[Dict[str, Any]]],
) -> Dict[str, Any]:
    """Against the symbol's OWN sector benchmark.

    Answers "is this strong because its whole sector is strong, or is it
    outperforming its own sector?". When the sector is unmapped or its ETF has
    no stored history the result is explicitly unavailable — the broad benchmark
    is never silently substituted.
    """
    sector = rm.sector_for(symbol)
    benchmark = rm.sector_benchmark_for(symbol)
    extra = {
        "sector": sector,
        "sector_registry": rm.sector_registry_provenance(),
        "reference_name": (rm.REFERENCE_BY_SYMBOL.get(benchmark).name
                           if benchmark in rm.REFERENCE_BY_SYMBOL else None),
    }
    if sector is None:
        block = build_reference_relative_strength(
            bars_by_symbol.get(symbol) or [], None,
            reference_symbol=None, reference_kind=rm.REFERENCE_SECTOR,
            unavailable_reason="no_sector_metadata_for_symbol", extra=extra)
        return block
    return build_reference_relative_strength(
        bars_by_symbol.get(symbol) or [],
        reference_bars.get(benchmark) if benchmark else None,
        reference_symbol=benchmark,
        reference_kind=rm.REFERENCE_SECTOR,
        unavailable_reason="no_sector_benchmark_series_stored",
        extra=extra,
    )


# --------------------------------------------------------------------------- #
# Market Regime V1
#
# A small, auditable description of the broad environment — NOT a prediction and
# NOT an input to any verdict or tier. Two pieces of benchmark evidence decide
# the category; universe breadth is reported alongside as secondary colour only,
# so the regime cannot be moved by the 25-symbol sample.
#
#   trend      : SPY close vs its own 50-session simple moving average
#   direction  : sign of SPY's 20-session return
#
#   supportive        trend above AND direction up
#   defensive         trend below AND direction down
#   mixed             the two disagree
#   insufficient_data fewer than 51 stored SPY sessions
#
# Thresholds are structural (above/below, positive/negative). Nothing was tuned.
# --------------------------------------------------------------------------- #

REGIME_SUPPORTIVE = "supportive"
REGIME_MIXED = "mixed"
REGIME_DEFENSIVE = "defensive"
REGIME_INSUFFICIENT = "insufficient_data"
REGIME_CATEGORIES = (REGIME_SUPPORTIVE, REGIME_MIXED, REGIME_DEFENSIVE,
                     REGIME_INSUFFICIENT)

REGIME_TREND_WINDOW = 50
REGIME_DIRECTION_HORIZON = 20


def build_market_regime(
    reference_bars: Dict[str, Sequence[Dict[str, Any]]],
    *,
    universe_breadth: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    benchmark = rm.PRIMARY_BENCHMARK
    bars = reference_bars.get(benchmark) or []
    breadth_note = None
    if universe_breadth and universe_breadth.get("status") == STATUS_AVAILABLE:
        above = universe_breadth.get("above_trend") or {}
        breadth_note = {
            "scope": universe_breadth.get("scope"),
            "symbol_count": universe_breadth.get("symbol_count"),
            "above_trend_pct": above.get("pct"),
            "note": "Secondary colour only — it does not decide the regime.",
        }

    base: Dict[str, Any] = {
        "benchmark_symbol": benchmark,
        "trend_window_days": REGIME_TREND_WINDOW,
        "direction_horizon_days": REGIME_DIRECTION_HORIZON,
        "regime": REGIME_INSUFFICIENT,
        "trend": None,
        "direction": None,
        "benchmark_close": None,
        "benchmark_trend_average": None,
        "benchmark_return_pct": None,
        "universe_breadth": breadth_note,
    }

    if len(bars) < REGIME_TREND_WINDOW + 1:
        base["status"] = STATUS_INSUFFICIENT_HISTORY if bars else STATUS_UNAVAILABLE
        base["reason"] = ("no_benchmark_series_stored" if not bars
                          else "insufficient_benchmark_history")
        return base

    closes = _closes(bars)
    sma = sum(closes[-REGIME_TREND_WINDOW:]) / REGIME_TREND_WINDOW
    close = closes[-1]
    direction_return = horizon_return_pct(bars, REGIME_DIRECTION_HORIZON)
    if direction_return is None:
        base["status"] = STATUS_INSUFFICIENT_HISTORY
        base["reason"] = "insufficient_benchmark_history"
        return base

    trend_above = close > sma
    direction_up = direction_return > 0
    if trend_above and direction_up:
        regime = REGIME_SUPPORTIVE
    elif not trend_above and not direction_up:
        regime = REGIME_DEFENSIVE
    else:
        regime = REGIME_MIXED

    base.update({
        "status": STATUS_AVAILABLE,
        "regime": regime,
        "trend": "above" if trend_above else "below",
        "direction": "up" if direction_up else "down",
        "benchmark_close": _round(close),
        "benchmark_trend_average": _round(sma),
        "benchmark_return_pct": _round(direction_return),
    })
    return base


def build_market_context(
    symbol: str,
    bars_by_symbol: Dict[str, Sequence[Dict[str, Any]]],
    *,
    as_of_session: Optional[str],
    reference_bars: Optional[Dict[str, Sequence[Dict[str, Any]]]] = None,
    universe_breadth: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """The full per-symbol context object exposed by the Product API.

    Three reference frames are kept SEPARATE and each names its own comparator,
    so a consumer never has to wonder "relative to what?":

      scanner_universe_relative_strength -> vs the other scanned symbols
      benchmark_relative_strength        -> vs the broad market (SPY)
      sector_relative_strength           -> vs the symbol's own sector ETF

    They are never blended into a composite score, and none of them touches the
    strategy verdict or the attention tier.
    """
    refs = reference_bars or {}
    return {
        "contract_version": MARKET_CONTEXT_CONTRACT_VERSION,
        "as_of_session": as_of_session,
        "scanner_universe_relative_strength": build_relative_strength(
            symbol, bars_by_symbol),
        "benchmark_relative_strength": build_benchmark_relative_strength(
            symbol, bars_by_symbol, refs),
        "sector_relative_strength": build_sector_relative_strength(
            symbol, bars_by_symbol, refs),
        "volume_context": build_volume_context(bars_by_symbol.get(symbol) or []),
        "market_regime": build_market_regime(
            refs, universe_breadth=universe_breadth),
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
    "build_market_context",
    "REL_OUTPERFORMING", "REL_IN_LINE", "REL_UNDERPERFORMING", "REL_CATEGORIES",
    "REL_NEUTRAL_BAND_PCT", "classify_relative_return",
    "build_reference_relative_strength", "build_benchmark_relative_strength",
    "build_sector_relative_strength",
    "REGIME_SUPPORTIVE", "REGIME_MIXED", "REGIME_DEFENSIVE",
    "REGIME_INSUFFICIENT", "REGIME_CATEGORIES",
    "REGIME_TREND_WINDOW", "REGIME_DIRECTION_HORIZON", "build_market_regime",
]
