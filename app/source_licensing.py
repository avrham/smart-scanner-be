"""Which external sources may be SHOWN, and which may only be researched.

One vocabulary, one predicate, one list of relations the Product API may never
read. Everything that needs to make a visibility decision imports from here so
the decision is made once.

WHY THIS IS A MODULE AND NOT A COMMENT
--------------------------------------
Wave 1 already had this rule and enforced it by construction: the FMP discovery
table was simply never referenced from a router. That works exactly until the
second internal-only source arrives, at which point "nobody wired it up" stops
being an argument and becomes a hope. Wave 2 adds analyst change events, which
come from the same restricted provider and are far more tempting to show on a
symbol screen than a movers list is. So the rule gets a name, a test, and a
database boundary that agrees with it.

THE THREE CLASSES
-----------------
    product_display_allowed   the licence (or the absence of one, in the case
                              of U.S. Government work) permits showing this
                              data to a reader of the product.
    internal_research_only    we are entitled to FETCH and STORE it, and not
                              to publish it. Ingest freely; stop at the
                              database.
    unknown_restriction       nobody has established the position. Treated
                              exactly like internal_research_only for display.

Only the first may be displayed. `unknown_restriction` is a real answer rather
than a placeholder — Finviz publishes no terms of use anywhere, and recording
that honestly is better than guessing in either direction.

WHAT "NEVER LEAKS" MEANS HERE
-----------------------------
Three independent layers, because any one of them can be forgotten:

  1. The Product API's database role holds NO privilege on the internal-only
     relations (ops/sql/create_smart_scanner_product_reader.sql). A router that
     tried would get `permission denied`, not data.
  2. The registry rows the Product API returns are filtered through
     `product_visible_rows`, so an internal-only source is not even named.
  3. A test walks the router module and asserts the forbidden relation names do
     not appear in it at all.

Paraphrasing restricted data into a product field would defeat all three, and
is out of bounds for the same reason: the licence restricts the DATA, not the
spelling.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

# --------------------------------------------------------------------------- #
# vocabulary
# --------------------------------------------------------------------------- #
LICENSING_PRODUCT_DISPLAY = "product_display_allowed"
LICENSING_INTERNAL_ONLY = "internal_research_only"
LICENSING_UNKNOWN = "unknown_restriction"

LICENSING_CLASSES = (LICENSING_PRODUCT_DISPLAY, LICENSING_INTERNAL_ONLY,
                     LICENSING_UNKNOWN)

#: The code-side statement of the licence position, as MEASURED, per source.
#:
#: The database registry carries the same value and is the operational source
#: of truth. This map exists so the answer is still correct when the registry
#: row is missing, NULL (a database still on migration 022), or unreachable —
#: and so the position is reviewable in a diff.
SOURCE_LICENSING: Dict[str, str] = {
    # The alert is the account owner's own indicator firing on their own chart
    # and reaching their own single-tenant deployment. Wave 1 displays these
    # and that is unchanged. NOTE the boundary: this covers the SIGNAL we were
    # sent, never TradingView's market data, and it would need re-examining
    # before this product served a second tenant.
    "tradingview": LICENSING_PRODUCT_DISPLAY,
    "ai_edge": LICENSING_PRODUCT_DISPLAY,

    # MEASURED: FMP's individual plans are personal and non-commercial and
    # forbid integrating the data into tools accessible by third parties.
    # Discovery candidates AND analyst grade changes both land here.
    "fmp": LICENSING_INTERNAL_ONLY,

    # Its terms forbid redistribution and third-party access outright.
    "trendspider": LICENSING_INTERNAL_ONLY,

    # No Terms of Use is published anywhere on the site. That is not permission.
    "finviz": LICENSING_UNKNOWN,
    # No API by policy, and downloads are contractually restricted by its own
    # upstream vendors; the position for any derived display is unestablished.
    "koyfin": LICENSING_UNKNOWN,
    # A connector, not a data holder: whatever it returns carries the LICENCE
    # OF THE UNDERLYING PROVIDER, which cannot be resolved in advance.
    "openbb": LICENSING_UNKNOWN,

    # Works of the U.S. Government are not subject to copyright protection in
    # the United States (17 U.S.C. sec. 105). The FOMC calendar and the BEA
    # release schedule are published by federal agencies as public information.
    "federal_reserve": LICENSING_PRODUCT_DISPLAY,
    "bea": LICENSING_PRODUCT_DISPLAY,
}

#: Relations whose contents are internal-only. The Product API must not read
#: them, and its database role is not granted them.
PRODUCT_FORBIDDEN_RELATIONS = (
    "external_discovery_candidates",   # FMP movers (023)
    "analyst_grade_events",            # FMP analyst grade changes (024)
    "external_signal_deliveries",      # raw third-party payloads (022)
)


def resolve_visibility(source: Optional[str],
                       declared: Optional[str] = None) -> str:
    """The licence class for a source name.

    `declared` is the value the database registry carries. It wins when it is a
    recognised class, so an operator can tighten a source without a deploy —
    but it can never introduce an unrecognised value, and it can never turn a
    source we have no record of into a displayable one.
    """
    if declared in LICENSING_CLASSES:
        return declared
    return SOURCE_LICENSING.get((source or "").strip().lower(),
                                LICENSING_UNKNOWN)


def is_product_displayable(visibility: Optional[str]) -> bool:
    """One rule, stated once: only an explicit allowance permits display."""
    return visibility == LICENSING_PRODUCT_DISPLAY


def source_is_product_displayable(source: Optional[str],
                                  declared: Optional[str] = None) -> bool:
    return is_product_displayable(resolve_visibility(source, declared))


def product_visible_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Registry rows the Product API may name, with the class stamped on.

    Filtering the REGISTRY, not just the data, is deliberate. Listing a source
    the product can never show gives a reader a name to ask about and an
    expectation we cannot meet, and it is the failure mode that would precede
    someone deciding a single field "wouldn't hurt".
    """
    out: List[Dict[str, Any]] = []
    for row in rows:
        visibility = resolve_visibility(row.get("source"),
                                        row.get("licensing_visibility"))
        if is_product_displayable(visibility):
            enriched = dict(row)
            enriched["licensing_visibility"] = visibility
            out.append(enriched)
    return out


def internal_only_sources() -> List[str]:
    """Every source name that must never appear in a product payload."""
    return sorted(name for name, cls in SOURCE_LICENSING.items()
                  if not is_product_displayable(cls))


def find_licensing_leaks(payload: Any) -> List[str]:
    """Internal-only source names found anywhere inside a product payload.

    Walks strings, keys and containers. Used by the Product API tests as a
    blunt backstop: it does not know WHY a name is there, only that it must not
    be, which is exactly the property a leak test wants.
    """
    forbidden = set(internal_only_sources())
    hits: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(key, str) and key.strip().lower() in forbidden:
                    hits.append(key)
                walk(value)
        elif isinstance(node, (list, tuple, set)):
            for item in node:
                walk(item)
        elif isinstance(node, str):
            token = node.strip().lower()
            if token in forbidden:
                hits.append(node)

    walk(payload)
    return sorted(set(hits))


__all__ = [
    "LICENSING_PRODUCT_DISPLAY", "LICENSING_INTERNAL_ONLY", "LICENSING_UNKNOWN",
    "LICENSING_CLASSES", "SOURCE_LICENSING", "PRODUCT_FORBIDDEN_RELATIONS",
    "resolve_visibility", "is_product_displayable",
    "source_is_product_displayable", "product_visible_rows",
    "internal_only_sources", "find_licensing_leaks",
]
