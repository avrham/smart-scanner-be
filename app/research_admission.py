"""Reject what can be rejected cheaply, BEFORE spending a provider request.

PURE. Given a price, its source and the canonical minimum, decides whether a
discovered symbol is worth ~90 seconds and one provider call.

WHY THIS EXISTS — A MEASURED FINDING, NOT A GUESS
--------------------------------------------------
The first live research cohort warmed five symbols and scanned three. All three
failed the SAME hard gate:

    CELU  0 -> 500 bars -> AVOID / price_below_minimum
    NVD   0 -> 500 bars -> AVOID / price_below_minimum
    PPCB  0 -> 437 bars -> AVOID / price_below_minimum

Every one of those was knowable before the fetch. FMP's movers feeds are
dominated by sub-dollar stocks, and `min_price` is a hard, canonical,
already-existing gate — so the history was bought to learn something the
discovery row already said.

WHAT THIS IS NOT
----------------
It is not a new strategy rule and it introduces no threshold of its own. The
minimum is READ from the canonical resolved configuration (`min_price`, 5.0 by
default in `wyckoff_v2/constants.py`) — the same number the strategy itself
applies. If an operator changes it in `pattern_configs`, admission changes with
it, because both read the same resolved config. Nothing here is tuned and
nothing here is a score.

It is also not a substitute for the scan. A symbol that passes admission is
still evaluated in full by the canonical strategy, which may reject it for any
of its own reasons. Admission only removes the cases that are already decided.

PRICE SOURCE, AND THE LICENCE THAT COMES WITH IT
------------------------------------------------
Two sources, tried in that order:

  1. `local_daily_bars` — the last close we already hold, written by the
     canonical provider path. Costs nothing, is the same data the scan will
     use, and carries no third-party restriction.
  2. `discovery_snapshot` — the price FMP already sent us inside the movers
     row. Costs nothing either: it is already stored. It is FMP data and is
     therefore INTERNAL RESEARCH ONLY.

Neither costs a provider request, which is the whole point.

The second source has a consequence worth stating plainly rather than
discovering later: when admission rejects a symbol on a discovery-snapshot
price, THE DECISION ITSELF is derived from restricted data. Today that is
contained, because the entire research domain is internal and has no product
surface. If research is ever exposed, `price_below_minimum` carrying
`price_source = discovery_snapshot` is a restricted-derived field and must be
re-examined — which is why the source is stored on the row rather than
forgotten.

TEMPORAL TRUTHFULNESS
---------------------
A discovery-snapshot price describes the market session the snapshot
describes — `reference_session_date`, not the session it became actionable in
(migration 025). The admission row records which session its price came from,
so a decision can always be placed in time.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Optional

#: The decision vocabulary. Three answers, and "we do not know" is one of them
#: — a symbol with no usable price is NOT rejected, because rejecting on
#: absent evidence would quietly filter out exactly the symbols our data is
#: weakest on.
ADMISSION_ELIGIBLE = "eligible_for_history"
ADMISSION_REJECTED = "rejected_before_history"
ADMISSION_UNKNOWN = "insufficient_admission_data"
ADMISSION_STATES = (ADMISSION_ELIGIBLE, ADMISSION_REJECTED, ADMISSION_UNKNOWN)

#: The strategy's OWN reason string, reused verbatim so an admission rejection
#: and a scan rejection are recognisably the same fact arriving earlier.
REASON_PRICE_BELOW_MINIMUM = "price_below_minimum"
REASON_NO_PRICE = "no_admission_price_available"
REASON_PRICE_OK = "price_at_or_above_minimum"

ADMISSION_REASONS = (REASON_PRICE_BELOW_MINIMUM, REASON_NO_PRICE,
                     REASON_PRICE_OK)

#: Where the admission price came from, and therefore what may be done with it.
PRICE_SOURCE_LOCAL_BARS = "local_daily_bars"          # canonical, unrestricted
PRICE_SOURCE_DISCOVERY = "discovery_snapshot"          # FMP, internal-only
PRICE_SOURCES = (PRICE_SOURCE_LOCAL_BARS, PRICE_SOURCE_DISCOVERY)

#: Which sources carry a third-party restriction into the decision.
RESTRICTED_PRICE_SOURCES = (PRICE_SOURCE_DISCOVERY,)


def resolve_min_price(config: Dict[str, Any]) -> Optional[float]:
    """The canonical minimum, read from the resolved strategy config.

    Never a constant of this module's own. If the value is missing or unusable
    the answer is None, and admission then declines to reject anything — an
    admission gate that invents its own threshold when it cannot find the real
    one is worse than no gate.
    """
    raw = (config or {}).get("min_price")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def is_restricted_source(price_source: Optional[str]) -> bool:
    return price_source in RESTRICTED_PRICE_SOURCES


def evaluate_admission(*, price: Optional[float],
                       price_source: Optional[str],
                       min_price: Optional[float],
                       reference_session: Optional[date] = None,
                       now: Optional[datetime] = None) -> Dict[str, Any]:
    """One symbol's admission decision, with its provenance attached.

    The comparison is `price < min_price` — a price EXACTLY at the minimum is
    admitted, because that is what the strategy's own gate does and admission
    must never be stricter than the rule it is anticipating.
    """
    moment = now or datetime.now(timezone.utc)
    verdict: Dict[str, Any] = {
        "state": ADMISSION_UNKNOWN,
        "reason": REASON_NO_PRICE,
        "price": None,
        "price_source": price_source,
        "min_price": min_price,
        "reference_session": reference_session,
        "evaluated_at": moment,
        "restricted_source": is_restricted_source(price_source),
        "provider_request_avoided": False,
    }
    if price is None or min_price is None:
        # No price, or no canonical minimum to compare against. Both are
        # "unknown", never "rejected": the gate exists to save requests on
        # symbols we KNOW are out, not to discard the ones we cannot see.
        return verdict

    verdict["price"] = float(price)
    if float(price) < float(min_price):
        verdict.update({"state": ADMISSION_REJECTED,
                        "reason": REASON_PRICE_BELOW_MINIMUM,
                        "provider_request_avoided": True})
        return verdict

    verdict.update({"state": ADMISSION_ELIGIBLE, "reason": REASON_PRICE_OK})
    return verdict


def admits_history(verdict: Dict[str, Any]) -> bool:
    """Whether this symbol may proceed to a provider request.

    `insufficient_admission_data` PROCEEDS. We do not spend the request when we
    know the answer; we do spend it when we do not.
    """
    return verdict.get("state") in (ADMISSION_ELIGIBLE, ADMISSION_UNKNOWN)


__all__ = [
    "ADMISSION_ELIGIBLE", "ADMISSION_REJECTED", "ADMISSION_UNKNOWN",
    "ADMISSION_STATES", "REASON_PRICE_BELOW_MINIMUM", "REASON_NO_PRICE",
    "REASON_PRICE_OK", "ADMISSION_REASONS",
    "PRICE_SOURCE_LOCAL_BARS", "PRICE_SOURCE_DISCOVERY", "PRICE_SOURCES",
    "RESTRICTED_PRICE_SOURCES", "resolve_min_price", "is_restricted_source",
    "evaluate_admission", "admits_history",
]
