"""ONE lifecycle state per research symbol, and totals that must add up.

WHY THIS MODULE EXISTS
----------------------
The previous milestone reported a funnel that was true line by line and
unauditable as a whole:

    ADMISSION PASSED     15
    HISTORY READY         8
    RESEARCH SCANNED      7

Fifteen symbols passed admission and eight became ready. Where the other seven
went was not in the report, and could not be derived from it, because the three
columns that describe a symbol's position — `admission_state`, `state` and
`candidate_state` — were each counted independently. A symbol could therefore
be counted in two rows, or in none, and nothing would notice.

THE FIX IS NOT MORE COUNTERS
----------------------------
It is one function, `lifecycle_state`, that maps a row to EXACTLY ONE state,
and a summary whose parts are required to sum to the whole. Counting stops
being a set of `count(*) FILTER (...)` expressions that happen to be written
next to each other and becomes a partition.

`count(*) FILTER (WHERE state IN ('research_ready','research_scanned'))` and
`count(*) FILTER (WHERE candidate_state = 'research_candidate')` overlap by
construction. A partition cannot.

CONSERVATION IS CHECKED, NOT HOPED FOR
--------------------------------------
`check_conservation` returns the violations, and the lifecycle raises on them.
An impossible funnel must be a loud failure at the moment it is produced, not a
discrepancy someone notices in a report three sessions later. The invariants are
derived from the lifecycle as it actually is, not asserted from the outside:

    selected = admission_rejected + admission_passed
                                  + admission_unknown + admission_pending

    admitted_to_history = every state after admission

    scanned = classification_pending + scanned_not_candidate
                                     + research_candidate

TWO RATES, TWO DENOMINATORS, NEVER ONE WORD
-------------------------------------------
The previous report called 15/45 and 1/7 "conversion". They measure different
things over different populations and must not share a name:

    admission pass rate      15 / 45 = 33.3%   of symbols SELECTED
    candidate conversion      1 /  7 = 14.3%   of symbols SCANNED

So every rate here carries its numerator, its denominator, and the NAME of the
population the denominator is. A bare percentage is not reportable.

NO SCORE, NO RANKING
--------------------
This module counts and partitions. It does not order symbols, weight states, or
produce a number that could be read as quality.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import app.research_admission as ra
import app.research_universe as ru

RESEARCH_FUNNEL_CONTRACT_VERSION = "smart_scanner_research_funnel.v1"


# --------------------------------------------------------------------------- #
# the partition
#
# Ordered by the lifecycle, and mutually exclusive by construction: the
# derivation returns at the FIRST branch that matches, and the branches are
# arranged so an earlier one is never also true of a later one.
# --------------------------------------------------------------------------- #

#: Admission has not been evaluated for this symbol yet. Real, and separate
#: from `insufficient_admission_data`: "we have not asked" is not "we asked and
#: could not tell".
LIFECYCLE_ADMISSION_PENDING = "admission_pending"

#: Rejected before any provider request. TERMINAL for this run: the symbol
#: never enters warmup, and this count IS the provider requests avoided.
LIFECYCLE_ADMISSION_REJECTED = "admission_rejected"

#: Admitted, waiting for history. Nothing has been fetched yet.
LIFECYCLE_HISTORY_PENDING = "history_pending"

#: Partial history on hand; more needed before the canonical readiness gate.
LIFECYCLE_HISTORY_WARMING = "history_warming"

#: The provider cannot give us this symbol. Terminal — retrying changes nothing.
LIFECYCLE_HISTORY_UNAVAILABLE = "history_unavailable"

#: Warmup failed to the attempt ceiling. Retryable in principle, parked in fact.
LIFECYCLE_HISTORY_FAILED = "history_failed"

#: Enough history; the research scan has not run yet.
LIFECYCLE_SCAN_PENDING = "scan_pending"

#: Scanned, but the candidate classification has not been applied. Should be
#: transient. Given its own state rather than folded into either neighbour,
#: because folding it is how a symbol gets counted as a non-candidate before
#: anything has actually judged it.
LIFECYCLE_CLASSIFICATION_PENDING = "classification_pending"

#: Scanned and did not survive the screen.
LIFECYCLE_SCANNED_NOT_CANDIDATE = "scanned_not_candidate"

#: Scanned and survived. The only state that means "worth a human look".
LIFECYCLE_RESEARCH_CANDIDATE = "research_candidate"

LIFECYCLE_STATES: Tuple[str, ...] = (
    LIFECYCLE_ADMISSION_PENDING,
    LIFECYCLE_ADMISSION_REJECTED,
    LIFECYCLE_HISTORY_PENDING,
    LIFECYCLE_HISTORY_WARMING,
    LIFECYCLE_HISTORY_UNAVAILABLE,
    LIFECYCLE_HISTORY_FAILED,
    LIFECYCLE_SCAN_PENDING,
    LIFECYCLE_CLASSIFICATION_PENDING,
    LIFECYCLE_SCANNED_NOT_CANDIDATE,
    LIFECYCLE_RESEARCH_CANDIDATE,
)

#: States a symbol reaches only by having been ADMITTED past the price gate.
#: Everything except the two admission states.
POST_ADMISSION_STATES: Tuple[str, ...] = (
    LIFECYCLE_HISTORY_PENDING, LIFECYCLE_HISTORY_WARMING,
    LIFECYCLE_HISTORY_UNAVAILABLE, LIFECYCLE_HISTORY_FAILED,
    LIFECYCLE_SCAN_PENDING, LIFECYCLE_CLASSIFICATION_PENDING,
    LIFECYCLE_SCANNED_NOT_CANDIDATE, LIFECYCLE_RESEARCH_CANDIDATE,
)

#: States that mean the research scan has actually run. The denominator of the
#: candidate conversion rate, and nothing else.
SCANNED_STATES: Tuple[str, ...] = (
    LIFECYCLE_CLASSIFICATION_PENDING, LIFECYCLE_SCANNED_NOT_CANDIDATE,
    LIFECYCLE_RESEARCH_CANDIDATE,
)

#: States from which no further work will happen without an external change.
TERMINAL_LIFECYCLE_STATES: Tuple[str, ...] = (
    LIFECYCLE_ADMISSION_REJECTED, LIFECYCLE_HISTORY_UNAVAILABLE,
    LIFECYCLE_HISTORY_FAILED,
)


class FunnelConservationError(AssertionError):
    """The funnel does not add up.

    An AssertionError subclass on purpose: this is a broken invariant, not a
    recoverable condition, and it must not be caught by an `except Exception`
    that was written to absorb provider failures.
    """


# --------------------------------------------------------------------------- #
# derivation
# --------------------------------------------------------------------------- #

def lifecycle_state(row: Dict[str, Any]) -> str:
    """The ONE state this symbol is in. Total, deterministic, order-dependent.

    Reads the three stored columns in lifecycle order and returns at the first
    match, so no row can satisfy two branches. Every fall-through has an
    explicit destination — there is no `return None` and no implicit default
    that would let an unrecognised row vanish from the totals.
    """
    admission = row.get("admission_state")
    if admission is None:
        return LIFECYCLE_ADMISSION_PENDING
    if admission == ra.ADMISSION_REJECTED:
        return LIFECYCLE_ADMISSION_REJECTED

    # Past the gate (eligible OR insufficient data — both proceed to history,
    # which is the admission contract: we skip the request when we know the
    # answer and spend it when we do not).
    state = row.get("state")
    if state == ru.STATE_UNAVAILABLE:
        return LIFECYCLE_HISTORY_UNAVAILABLE
    if state == ru.STATE_FAILED:
        return LIFECYCLE_HISTORY_FAILED
    if state == ru.STATE_HISTORY_WARMING:
        return LIFECYCLE_HISTORY_WARMING
    if state == ru.STATE_RESEARCH_SCANNED:
        candidate = row.get("candidate_state")
        if candidate == ru.CANDIDATE_RESEARCH_CANDIDATE:
            return LIFECYCLE_RESEARCH_CANDIDATE
        if candidate == ru.CANDIDATE_SCANNED_NOT_CANDIDATE:
            return LIFECYCLE_SCANNED_NOT_CANDIDATE
        # Scanned, not yet classified (or classified as insufficient/
        # unavailable, which contradicts `research_scanned` and is therefore
        # also "not judged yet"). Never silently a non-candidate.
        return LIFECYCLE_CLASSIFICATION_PENDING
    if state == ru.STATE_RESEARCH_READY:
        return LIFECYCLE_SCAN_PENDING
    # `discovered`, `history_required`, an unknown/NULL state: admitted and
    # waiting for history. Unknown lands here rather than nowhere.
    return LIFECYCLE_HISTORY_PENDING


def admission_tier(row: Dict[str, Any]) -> str:
    """The admission outcome alone, for the admission-rate denominator.

    Deliberately separate from `lifecycle_state`: a symbol that PASSED
    admission and later failed warmup still passed admission, and the pass rate
    must not silently shrink as symbols move on down the funnel.
    """
    admission = row.get("admission_state")
    if admission is None:
        return LIFECYCLE_ADMISSION_PENDING
    if admission == ra.ADMISSION_REJECTED:
        return ra.ADMISSION_REJECTED
    if admission == ra.ADMISSION_UNKNOWN:
        return ra.ADMISSION_UNKNOWN
    return ra.ADMISSION_ELIGIBLE


# --------------------------------------------------------------------------- #
# rates — always numerator, denominator, and the NAME of the population
# --------------------------------------------------------------------------- #

def rate(numerator: int, denominator: int, *, of: str) -> Dict[str, Any]:
    """A rate that cannot be quoted without its denominator.

    `percent` is None when the denominator is zero. Zero over zero is not 0%,
    and reporting it as 0% is how "we scanned nothing" becomes "nothing
    converted".
    """
    num, den = int(numerator), int(denominator)
    return {"numerator": num, "denominator": den, "of": of,
            "percent": round(100.0 * num / den, 1) if den else None}


# --------------------------------------------------------------------------- #
# the summary
# --------------------------------------------------------------------------- #

def summarise(rows: Iterable[Dict[str, Any]], *,
              provider_calls_used: int = 0,
              provider_calls_avoided: Optional[int] = None) -> Dict[str, Any]:
    """Partition the rows, count each state once, and derive the rates.

    Returns the counts for EVERY state including zeros, so a state going to
    zero is visible as a zero rather than as a missing key that a reader has to
    interpret.
    """
    rows = list(rows)
    states: Dict[str, int] = {s: 0 for s in LIFECYCLE_STATES}
    tiers: Dict[str, int] = {
        LIFECYCLE_ADMISSION_PENDING: 0, ra.ADMISSION_ELIGIBLE: 0,
        ra.ADMISSION_REJECTED: 0, ra.ADMISSION_UNKNOWN: 0}
    per_symbol: List[Dict[str, str]] = []

    for row in rows:
        state = lifecycle_state(row)
        states[state] += 1
        tiers[admission_tier(row)] += 1
        per_symbol.append({"symbol": row.get("symbol"),
                           "lifecycle_state": state,
                           "admission_tier": admission_tier(row)})

    selected = len(rows)
    passed = tiers[ra.ADMISSION_ELIGIBLE]
    unknown = tiers[ra.ADMISSION_UNKNOWN]
    rejected = tiers[ra.ADMISSION_REJECTED]
    pending = tiers[LIFECYCLE_ADMISSION_PENDING]
    admitted = sum(states[s] for s in POST_ADMISSION_STATES)
    scanned = sum(states[s] for s in SCANNED_STATES)
    candidates = states[LIFECYCLE_RESEARCH_CANDIDATE]

    summary: Dict[str, Any] = {
        "contract_version": RESEARCH_FUNNEL_CONTRACT_VERSION,
        "selected_for_research": selected,
        "states": states,
        "admission": {
            "passed": passed, "rejected": rejected,
            "unknown": unknown, "pending": pending},
        "admitted_to_history": admitted,
        "scanned": scanned,
        "research_candidates": candidates,
        "rates": {
            # 15/45 — of every symbol selected into the research pool.
            "admission_pass_rate":
                rate(passed, selected, of="symbols_selected_for_research"),
            # 8/15 — of admitted symbols, how many reached usable history.
            "history_readiness_rate":
                rate(states[LIFECYCLE_SCAN_PENDING] + scanned, admitted,
                     of="symbols_admitted_to_history"),
            # 1/7 — of SCANNED symbols. A different population, a different
            # word: this is never called the admission rate and never merged
            # with it.
            "candidate_conversion_rate":
                rate(candidates, scanned, of="symbols_scanned"),
        },
        "provider": {
            "calls_used": int(provider_calls_used),
            # None means NOT MEASURED, which is different from zero. A caller
            # that only wants the partition (a blocked run, a state dump) has
            # not claimed anything about provider cost, and the conservation
            # check must not invent a claim on its behalf.
            "calls_avoided": (None if provider_calls_avoided is None
                              else int(provider_calls_avoided)),
            # Cost per survivor. None with no survivors — dividing by zero
            # candidates and printing a big number would read like a verdict on
            # the cohort rather than an absence of one.
            "calls_per_candidate": (round(provider_calls_used / candidates, 2)
                                    if candidates else None),
        },
        "per_symbol": per_symbol,
    }
    summary["conservation"] = check_conservation(summary)
    return summary


# --------------------------------------------------------------------------- #
# conservation
# --------------------------------------------------------------------------- #

def check_conservation(summary: Dict[str, Any]) -> Dict[str, Any]:
    """Every invariant, each as a named equation with both sides reported.

    A boolean would say the funnel is broken; this says WHICH equation broke
    and by how much, which is the difference between a bug report and a puzzle.
    """
    states = summary["states"]
    adm = summary["admission"]
    selected = summary["selected_for_research"]

    checks: List[Dict[str, Any]] = []

    def eq(name: str, left: int, right: int, expression: str) -> None:
        checks.append({"invariant": name, "expression": expression,
                       "left": int(left), "right": int(right),
                       "ok": int(left) == int(right)})

    # 1. The partition is total: every symbol is in exactly one state.
    eq("states_partition_all_symbols",
       sum(states.values()), selected,
       "sum(states) == selected_for_research")

    # 2. Admission tiers also partition the same population.
    eq("admission_tiers_partition_all_symbols",
       adm["passed"] + adm["rejected"] + adm["unknown"] + adm["pending"],
       selected,
       "passed + rejected + unknown + pending == selected_for_research")

    # 3. Rejected symbols never advance: the rejected tier and the rejected
    #    lifecycle state are the same symbols.
    eq("rejected_never_advances",
       states[LIFECYCLE_ADMISSION_REJECTED], adm["rejected"],
       "states.admission_rejected == admission.rejected")

    # 4. Everything past admission came through admission.
    eq("post_admission_equals_admitted",
       summary["admitted_to_history"],
       adm["passed"] + adm["unknown"],
       "admitted_to_history == admission.passed + admission.unknown")

    # 5. The scanned population is exactly the three post-scan states.
    eq("scanned_partition",
       summary["scanned"],
       states[LIFECYCLE_CLASSIFICATION_PENDING]
       + states[LIFECYCLE_SCANNED_NOT_CANDIDATE]
       + states[LIFECYCLE_RESEARCH_CANDIDATE],
       "scanned == classification_pending + scanned_not_candidate "
       "+ research_candidate")

    # 6. A candidate is a scanned symbol. Enrichment, discovery strength and
    #    catalysts cannot create one; only the screen can.
    eq("candidates_are_scanned",
       min(summary["research_candidates"], summary["scanned"]),
       summary["research_candidates"],
       "research_candidates <= scanned")

    # 7. The rate denominators are the populations they claim to be — so a
    #    future edit cannot quietly re-point a rate at a different cohort.
    rates = summary["rates"]
    eq("admission_rate_denominator_is_selected",
       rates["admission_pass_rate"]["denominator"], selected,
       "admission_pass_rate.denominator == selected_for_research")
    eq("candidate_rate_denominator_is_scanned",
       rates["candidate_conversion_rate"]["denominator"], summary["scanned"],
       "candidate_conversion_rate.denominator == scanned")

    # 8. Avoided calls are exactly the rejections — the gate's whole purpose,
    #    stated as an equation rather than as a claim in a report. Checked only
    #    when the caller actually measured them: `None` means not measured, and
    #    an unmeasured value must not be compared as though it were zero.
    avoided = summary["provider"]["calls_avoided"]
    if avoided is not None:
        eq("avoided_calls_equal_rejections",
           avoided, adm["rejected"],
           "provider.calls_avoided == admission.rejected")

    violations = [c for c in checks if not c["ok"]]
    return {"ok": not violations, "checks": checks, "violations": violations}


def assert_conservation(summary: Dict[str, Any]) -> None:
    """Fail loudly. Called by the lifecycle before a run is allowed to report.

    An impossible funnel is not a number to publish with a caveat.

    RECOMPUTED, never read from the summary. The stored `conservation` block is
    a snapshot from the moment the summary was built; anything that touched a
    counter afterwards — which is exactly the class of bug this guards — would
    otherwise be waved through by its own stale verdict.
    """
    result = check_conservation(summary)
    if result["ok"]:
        return
    detail = "; ".join(
        f"{v['invariant']}: {v['expression']} but {v['left']} != {v['right']}"
        for v in result["violations"])
    raise FunnelConservationError(
        f"research funnel does not conserve symbols — {detail}")


FUNNEL_ROW_SQL = """
SELECT symbol, admission_state, state, candidate_state
FROM public.research_symbols
"""


async def load_funnel(conn, *, provider_calls_used: int = 0,
                      provider_calls_avoided: Optional[int] = None,
                      ) -> Dict[str, Any]:
    """The whole research pool, partitioned. One query, one pass, no FILTERs.

    Reading the rows and partitioning in code rather than writing nine
    `count(*) FILTER (...)` expressions is the point: SQL filters cannot be
    proven disjoint, and a function that returns one string per row is disjoint
    by construction.

    `provider_calls_avoided` defaults to None — NOT MEASURED — because a
    read-only dump of the pool has not observed a run's provider cost. A zero
    default made a standalone `--summary` report a conservation violation
    against its own invented measurement.
    """
    rows = [dict(r) for r in await conn.fetch(FUNNEL_ROW_SQL)]
    return summarise(rows, provider_calls_used=provider_calls_used,
                     provider_calls_avoided=provider_calls_avoided)


__all__ = [
    "RESEARCH_FUNNEL_CONTRACT_VERSION", "LIFECYCLE_STATES",
    "POST_ADMISSION_STATES", "SCANNED_STATES", "TERMINAL_LIFECYCLE_STATES",
    "LIFECYCLE_ADMISSION_PENDING", "LIFECYCLE_ADMISSION_REJECTED",
    "LIFECYCLE_HISTORY_PENDING", "LIFECYCLE_HISTORY_WARMING",
    "LIFECYCLE_HISTORY_UNAVAILABLE", "LIFECYCLE_HISTORY_FAILED",
    "LIFECYCLE_SCAN_PENDING", "LIFECYCLE_CLASSIFICATION_PENDING",
    "LIFECYCLE_SCANNED_NOT_CANDIDATE", "LIFECYCLE_RESEARCH_CANDIDATE",
    "FunnelConservationError", "lifecycle_state", "admission_tier", "rate",
    "summarise", "check_conservation", "assert_conservation", "load_funnel",
]
