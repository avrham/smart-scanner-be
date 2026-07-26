"""Trading-session maturation eligibility (shadow_maturation_eligibility.v1).

Classifies, for one frozen pair, whether its market-path outcome can be
matured further RIGHT NOW — measured in completed TRADING SESSIONS, never in
calendar days. The trading calendar is the set of session dates actually
present in the local `daily_bars` cache (the repository's existing
market-session record); the pure classifier here takes that count as an
explicit input so it stays fully unit-testable with no I/O.

The taxonomy is deliberately explicit so a report can distinguish a not-yet-
eligible pair (correct to leave alone) from an actual failure that needs
action:

    matured                      complete — fully resolved, nothing to do
    eligible                     >= MIN completed forward sessions AND not yet
                                 fully matured — a bounded maturation run can
                                 add at least one horizon now
    not_yet_eligible             fewer than MIN completed forward sessions —
                                 leave it alone (never a failure)
    retryable_failure            an error row that a re-run can plausibly
                                 repair (transient fetch / provider config)
    terminal_failure             an error row a plain re-run cannot fix
                                 (frozen split/revision, code-level type error)
    missing_market_session_data  the frozen snapshot-date session is absent
                                 from the provider range (continuity
                                 unconfirmed) — data problem, not a horizon gap
    eligibility_unknown          the local trading calendar is unavailable, so
                                 session-based eligibility cannot be proven
                                 (reported honestly, never assumed eligible)

PURE — no I/O, no writes.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, List, Optional

from app.workers.outcomes.calculator import HOLDING_WINDOWS
from app.workers.shadow.outcomes.constants import (
    REASON_PROVIDER_MISMATCH,
    REASON_PROVIDER_RANGE_UNSUPPORTED,
    REASON_REFERENCE_REVISION,
    REASON_SNAPSHOT_BAR_MISSING,
    STATUS_COMPLETE,
    STATUS_PARTIAL,
    STATUS_PENDING,
)


ELIGIBILITY_CONTRACT_VERSION = "shadow_maturation_eligibility.v1"

# Horizons are TRADING SESSIONS after the snapshot session (outcome.v1
# HOLDING_WINDOWS = [1, 3, 5, 10, 20]). One completed forward session is the
# minimum that can resolve any horizon; the largest window fully matures a pair.
MIN_MATURATION_SESSIONS = min(HOLDING_WINDOWS)
FULL_MATURATION_SESSIONS = max(HOLDING_WINDOWS)

# Eligibility states (closed vocabulary).
ELIGIBILITY_MATURED = "matured"
ELIGIBILITY_ELIGIBLE = "eligible"
ELIGIBILITY_NOT_YET = "not_yet_eligible"
ELIGIBILITY_RETRYABLE = "retryable_failure"
ELIGIBILITY_TERMINAL = "terminal_failure"
ELIGIBILITY_MISSING_SESSION_DATA = "missing_market_session_data"
ELIGIBILITY_UNKNOWN = "eligibility_unknown"

ELIGIBILITY_STATES = (
    ELIGIBILITY_MATURED,
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_NOT_YET,
    ELIGIBILITY_RETRYABLE,
    ELIGIBILITY_TERMINAL,
    ELIGIBILITY_MISSING_SESSION_DATA,
    ELIGIBILITY_UNKNOWN,
)

# A re-run (optionally with include_recalc) can plausibly repair these.
RETRYABLE_ERROR_CODES = frozenset({
    "forward_fetch_error",
    "outcome_error",
    REASON_PROVIDER_MISMATCH,
    REASON_PROVIDER_RANGE_UNSUPPORTED,
})
# A plain re-run cannot fix these: frozen split/revision, or a code-level
# persistence type error that needs a fix, not a retry.
TERMINAL_ERROR_CODES = frozenset({
    REASON_REFERENCE_REVISION,
    "persistence_type_error",
})
# The frozen snapshot-date session is missing from the provider range.
MISSING_SESSION_ERROR_CODES = frozenset({
    REASON_SNAPSHOT_BAR_MISSING,
})

# States that still require operator action to close the cohort out.
UNRESOLVED_STATES = frozenset({
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_RETRYABLE,
})


def completed_forward_sessions(
    session_dates: Iterable[date],
    snapshot_date: date,
    *,
    latest_completed_session: Optional[date] = None,
) -> Optional[int]:
    """Count completed TRADING sessions strictly after snapshot_date.

    `session_dates` is the local trading calendar (distinct session dates from
    daily_bars for a reference symbol). Sessions after an optional
    `latest_completed_session` cap are excluded. Returns None when the
    calendar is empty — eligibility is then unknown, never assumed.
    """
    dates = [d for d in session_dates if d is not None]
    if not dates:
        return None
    count = 0
    for d in dates:
        if d <= snapshot_date:
            continue
        if latest_completed_session is not None and d > latest_completed_session:
            continue
        count += 1
    return count


def classify_error_code(error_code: Optional[str]) -> str:
    """Map an outcome error_code onto an eligibility failure state.

    Unknown error codes default to retryable so an operator can attempt a
    bounded re-run; a persistent unknown error therefore stays visible rather
    than being silently declared terminal.
    """
    if error_code in MISSING_SESSION_ERROR_CODES:
        return ELIGIBILITY_MISSING_SESSION_DATA
    if error_code in TERMINAL_ERROR_CODES:
        return ELIGIBILITY_TERMINAL
    # Everything else (including None or unrecognized) is treated as retryable.
    return ELIGIBILITY_RETRYABLE


def classify_maturation_eligibility(
    *,
    outcome_status: Optional[str],
    error_code: Optional[str] = None,
    completed_forward_sessions: Optional[int] = None,
    min_maturation_sessions: int = MIN_MATURATION_SESSIONS,
) -> str:
    """Classify one pair's current maturation eligibility.

    `outcome_status` is the existing outcome row status (or None when no
    outcome row exists yet). Sessions are completed forward TRADING sessions.
    """
    if outcome_status == STATUS_COMPLETE:
        return ELIGIBILITY_MATURED

    if outcome_status == "error":
        return classify_error_code(error_code)

    # No row yet, pending, or partial: a maturation run may add a horizon once
    # enough forward sessions have completed. Partial rows re-run idempotently.
    if outcome_status in (None, STATUS_PENDING, STATUS_PARTIAL):
        if completed_forward_sessions is None:
            return ELIGIBILITY_UNKNOWN
        if completed_forward_sessions < min_maturation_sessions:
            return ELIGIBILITY_NOT_YET
        return ELIGIBILITY_ELIGIBLE

    # Any unrecognized status is reported honestly as unknown.
    return ELIGIBILITY_UNKNOWN


def summarize_eligibility(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate a list of per-pair eligibility inputs into counts.

    Each item may provide `outcome_status`, `error_code` and
    `completed_forward_sessions`. Returns a deterministic counts map over the
    full closed vocabulary (every state present, zeros included) plus the
    unresolved total that still requires action.
    """
    counts: Dict[str, int] = {state: 0 for state in ELIGIBILITY_STATES}
    for item in items:
        state = classify_maturation_eligibility(
            outcome_status=item.get("outcome_status"),
            error_code=item.get("error_code"),
            completed_forward_sessions=item.get("completed_forward_sessions"),
        )
        counts[state] = counts.get(state, 0) + 1
    unresolved = sum(counts[s] for s in UNRESOLVED_STATES)
    return {
        "eligibility_contract_version": ELIGIBILITY_CONTRACT_VERSION,
        "min_maturation_sessions": MIN_MATURATION_SESSIONS,
        "full_maturation_sessions": FULL_MATURATION_SESSIONS,
        "counts": counts,
        "unresolved_action_required_count": unresolved,
    }


__all__ = [
    "ELIGIBILITY_CONTRACT_VERSION",
    "MIN_MATURATION_SESSIONS",
    "FULL_MATURATION_SESSIONS",
    "ELIGIBILITY_STATES",
    "ELIGIBILITY_MATURED",
    "ELIGIBILITY_ELIGIBLE",
    "ELIGIBILITY_NOT_YET",
    "ELIGIBILITY_RETRYABLE",
    "ELIGIBILITY_TERMINAL",
    "ELIGIBILITY_MISSING_SESSION_DATA",
    "ELIGIBILITY_UNKNOWN",
    "UNRESOLVED_STATES",
    "completed_forward_sessions",
    "classify_error_code",
    "classify_maturation_eligibility",
    "summarize_eligibility",
]
