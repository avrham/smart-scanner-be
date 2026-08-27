"""Reference-market registry: benchmarks, sector benchmarks, sector mapping.

THE NON-NEGOTIABLE RULE
-----------------------
Reference symbols exist ONLY to provide context. They must never become scanner
candidates. Three independent mechanisms enforce that, and tests assert each:

  1. Membership. Candidacy is derived from a FROZEN history-warmup universe
     (`prospective_register` takes a universe_id and explicitly forbids a
     `symbols` field). Reference symbols live in their own separate universe
     (`REFERENCE_UNIVERSE_CODE`), never in the candidate universe.
  2. Derivation. The Product API's scan universe comes from that run's
     `strategy_shadow_run_pairs`, so a symbol can only appear on the scanner
     screen if the campaign actually evaluated it.
  3. Registry. `is_reference_symbol()` is the single explicit predicate, and
     `assert_no_reference_symbols()` is available to any code path that builds a
     candidate list.

Sharing the `daily_bars` table is deliberate and safe: nothing derives
candidates by enumerating that table.

SECTOR METADATA SOURCE
----------------------
The store has no sector metadata — the `tickers` table is empty and its schema
has no sector column — and the market-data provider's profile path supplies
market cap / eligibility, not GICS sector. So the mapping below is an EXPLICIT,
VERSIONED registry rather than a provider lookup or an inference.

It is a data table in one place, not a mapping hidden inside presentation code,
so it can be reviewed as data. Sector is never inferred from price behaviour and
never produced by a model.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

# The frozen warmup universe that carries reference history. Deliberately a
# DIFFERENT universe from the candidate one, so campaign registration (which
# pins a universe_id) can never pick these symbols up.
#
# FRESHNESS / OPERATING MODEL
# ---------------------------
# No second pipeline is needed to keep this current. The canonical history
# lifecycle is already universe-driven end to end:
#   * app/jobs/history_refresh.py takes (universe_id, universe_hash) and fans
#     out one bounded refresh task per member symbol;
#   * app/jobs/handlers/daily_pipeline_driver.py takes universe_id in its job
#     PAYLOAD, not from configuration.
# So refreshing reference data is the same operation as refreshing candidate
# data, pointed at this universe id. Nothing here needs new code, new secrets,
# new scheduling architecture or a new worker.
#
# What remains a deliberate, separate decision: enabling the production daily
# pipeline at all. Until that switch is thrown, reference history is refreshed
# the same way it was created — by running the existing bounded warmup/refresh
# path against this universe on demand.
REFERENCE_UNIVERSE_CODE = "SMART-SCANNER-REFERENCE-MARKET-V1"
CANDIDATE_UNIVERSE_CODE = "WYCKOFF-HISTORY-WARMUP-QUALIFICATION"

# ---- reference kinds -------------------------------------------------------- #
REFERENCE_BROAD_MARKET = "broad_market"
REFERENCE_SECTOR = "sector"
REFERENCE_KINDS = (REFERENCE_BROAD_MARKET, REFERENCE_SECTOR)

# ---- registry provenance ---------------------------------------------------- #
SECTOR_REGISTRY_VERSION = "sector_registry.v1"
SECTOR_REGISTRY_SOURCE = "manual_gics_sector_to_spdr_select_sector_etf"
SECTOR_REGISTRY_EFFECTIVE_DATE = "2026-08-27"


class ReferenceSymbol:
    __slots__ = ("symbol", "kind", "name", "reason")

    def __init__(self, symbol: str, kind: str, name: str, reason: str):
        self.symbol = symbol
        self.kind = kind
        self.name = name
        self.reason = reason

    def to_dict(self) -> Dict[str, str]:
        return {"symbol": self.symbol, "kind": self.kind,
                "name": self.name, "reason": self.reason}


# The complete reference set. Each entry states why it exists — nothing is here
# merely because it is a liquid ETF.
REFERENCE_SYMBOLS: Tuple[ReferenceSymbol, ...] = (
    ReferenceSymbol(
        "SPY", REFERENCE_BROAD_MARKET, "SPDR S&P 500 ETF Trust",
        "Primary broad-market benchmark: the reference frame for "
        "benchmark-relative strength and for Market Regime V1.",
    ),
    ReferenceSymbol(
        "QQQ", REFERENCE_BROAD_MARKET, "Invesco QQQ Trust (Nasdaq-100)",
        "Secondary broad reference. The scanned universe is mega-cap heavy "
        "(10 of 25 are technology or communication services), so a Nasdaq-100 "
        "reading is a materially different read of the same session than SPY.",
    ),
    ReferenceSymbol("XLK", REFERENCE_SECTOR, "Technology Select Sector SPDR",
                    "Sector benchmark for the 7 technology names."),
    ReferenceSymbol("XLC", REFERENCE_SECTOR, "Communication Services Select Sector SPDR",
                    "Sector benchmark for the 3 communication services names."),
    ReferenceSymbol("XLY", REFERENCE_SECTOR, "Consumer Discretionary Select Sector SPDR",
                    "Sector benchmark for the 3 consumer discretionary names."),
    ReferenceSymbol("XLP", REFERENCE_SECTOR, "Consumer Staples Select Sector SPDR",
                    "Sector benchmark for the 2 consumer staples names."),
    ReferenceSymbol("XLF", REFERENCE_SECTOR, "Financial Select Sector SPDR",
                    "Sector benchmark for the 3 financials names."),
    ReferenceSymbol("XLV", REFERENCE_SECTOR, "Health Care Select Sector SPDR",
                    "Sector benchmark for the 3 health care names."),
    ReferenceSymbol("XLE", REFERENCE_SECTOR, "Energy Select Sector SPDR",
                    "Sector benchmark for the 2 energy names."),
    ReferenceSymbol("XLI", REFERENCE_SECTOR, "Industrial Select Sector SPDR",
                    "Sector benchmark for the 2 industrials names."),
)

REFERENCE_BY_SYMBOL: Dict[str, ReferenceSymbol] = {
    r.symbol: r for r in REFERENCE_SYMBOLS
}

#: The single designated broad benchmark for benchmark-relative strength.
PRIMARY_BENCHMARK = "SPY"

#: Additional broad references, reported alongside but never THE benchmark.
SECONDARY_BENCHMARKS: Tuple[str, ...] = ("QQQ",)


# ---- sector mapping for the frozen candidate universe ----------------------- #
# GICS sector -> SPDR Select Sector ETF. Reviewed as data; see the provenance
# constants above. A symbol absent from this map yields an explicit
# "unavailable" sector context — never a silent fallback to the broad benchmark.
SECTOR_BENCHMARKS: Dict[str, str] = {
    "Information Technology": "XLK",
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Financials": "XLF",
    "Health Care": "XLV",
    "Energy": "XLE",
    "Industrials": "XLI",
}

SYMBOL_SECTORS: Dict[str, str] = {
    # Information Technology
    "AAPL": "Information Technology",
    "AMD": "Information Technology",
    "AVGO": "Information Technology",
    "CRM": "Information Technology",
    "MSFT": "Information Technology",
    "NVDA": "Information Technology",
    "ORCL": "Information Technology",
    # Communication Services
    "GOOGL": "Communication Services",
    "META": "Communication Services",
    "NFLX": "Communication Services",
    # Consumer Discretionary
    "AMZN": "Consumer Discretionary",
    "HD": "Consumer Discretionary",
    "TSLA": "Consumer Discretionary",
    # Consumer Staples
    "COST": "Consumer Staples",
    "WMT": "Consumer Staples",
    # Financials
    "BAC": "Financials",
    "GS": "Financials",
    "JPM": "Financials",
    # Health Care
    "JNJ": "Health Care",
    "LLY": "Health Care",
    "UNH": "Health Care",
    # Energy
    "CVX": "Energy",
    "XOM": "Energy",
    # Industrials
    "CAT": "Industrials",
    "GE": "Industrials",
}


# --------------------------------------------------------------------------- #
# predicates and lookups
# --------------------------------------------------------------------------- #

def reference_symbols(kind: Optional[str] = None) -> List[str]:
    """Every reference symbol, optionally filtered to one kind."""
    return [r.symbol for r in REFERENCE_SYMBOLS if kind is None or r.kind == kind]


def is_reference_symbol(symbol: Optional[str]) -> bool:
    """True when this symbol exists only to provide context."""
    if not symbol:
        return False
    return symbol.strip().upper() in REFERENCE_BY_SYMBOL


def reference_kind(symbol: str) -> Optional[str]:
    entry = REFERENCE_BY_SYMBOL.get((symbol or "").strip().upper())
    return entry.kind if entry else None


def sector_for(symbol: Optional[str]) -> Optional[str]:
    if not symbol:
        return None
    return SYMBOL_SECTORS.get(symbol.strip().upper())


def sector_benchmark_for(symbol: Optional[str]) -> Optional[str]:
    """The sector ETF for this symbol, or None when unmapped.

    Returning None is meaningful: the caller must then report sector context as
    unavailable. It must NEVER substitute the broad benchmark.
    """
    sector = sector_for(symbol)
    if sector is None:
        return None
    return SECTOR_BENCHMARKS.get(sector)


def sector_registry_provenance() -> Dict[str, str]:
    """Auditable provenance for the mapping, surfaced through the API."""
    return {
        "version": SECTOR_REGISTRY_VERSION,
        "source": SECTOR_REGISTRY_SOURCE,
        "effective_date": SECTOR_REGISTRY_EFFECTIVE_DATE,
    }


def assert_no_reference_symbols(symbols: Sequence[str]) -> None:
    """Guard for any path that builds a CANDIDATE list.

    Raises rather than filtering: a reference symbol reaching a candidate list
    is a programming error, and silently dropping it would hide the bug.
    """
    offenders = sorted({s.strip().upper() for s in symbols if is_reference_symbol(s)})
    if offenders:
        raise ValueError(
            "reference symbols may never be scanner candidates: "
            f"{offenders}"
        )


__all__ = [
    "REFERENCE_UNIVERSE_CODE", "CANDIDATE_UNIVERSE_CODE",
    "REFERENCE_BROAD_MARKET", "REFERENCE_SECTOR", "REFERENCE_KINDS",
    "REFERENCE_SYMBOLS", "REFERENCE_BY_SYMBOL",
    "PRIMARY_BENCHMARK", "SECONDARY_BENCHMARKS",
    "SECTOR_BENCHMARKS", "SYMBOL_SECTORS",
    "SECTOR_REGISTRY_VERSION", "SECTOR_REGISTRY_SOURCE",
    "SECTOR_REGISTRY_EFFECTIVE_DATE",
    "reference_symbols", "is_reference_symbol", "reference_kind",
    "sector_for", "sector_benchmark_for", "sector_registry_provenance",
    "assert_no_reference_symbols",
]
