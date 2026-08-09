"""Bounded prior-campaign outcome-maturation discovery + current-campaign
maturity classification for the daily-pipeline v2 outcome stage.

Separates PIPELINE-level completion from OUTCOME-level maturity:

  * a CURRENT campaign for completed session N legitimately has ZERO completed
    forward trading sessions strictly after N at creation time, so its own
    outcome eligibility is honestly deferred — never a fabricated success and
    never a whole-occurrence blocker;
  * PRIOR campaigns (same experiment / strategy versions / frozen universe /
    prospective pipeline) whose forward window has since elapsed become eligible
    for additional maturation rounds (1D -> 3D -> 5D -> 10D -> 20D) as new local
    trading sessions arrive.

Strictly read/plan only: NO provider, NO shared Supabase, NO campaign/evaluation
/pair mutation, and a BOUNDED lookback (never an unbounded history scan). The
actual enqueue/persist is done by the existing idempotent
prospective_outcome_maturation.v1 machinery; this module only decides WHAT is
eligible and HOW to describe the current campaign truthfully.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import asyncpg

from app.jobs.prospective_outcome_enqueue import build_outcome_maturity_preflight
from app.workers.shadow.outcomes.eligibility import FULL_MATURATION_SESSIONS

CURRENT_MATURITY_CONTRACT = "daily_pipeline_current_campaign_maturity.v1"
PRIOR_DISCOVERY_CONTRACT = "daily_pipeline_prior_maturation_discovery.v1"

# A campaign reaches its longest horizon (20D) after FULL_MATURATION_SESSIONS
# completed forward sessions; after that it is fully matured and needs no more
# rounds. The bounded candidate window is therefore the longest horizon plus an
# operational-recovery margin (so a pipeline that was paused for a few sessions
# still catches up), expressed in TRADING sessions and translated to a generous
# CALENDAR prefilter for the SQL — the authoritative eligibility decision is the
# per-campaign session-count preflight, never calendar subtraction.
PRIOR_MATURATION_RECOVERY_SESSIONS = 5
PRIOR_MATURATION_MAX_LOOKBACK_SESSIONS = FULL_MATURATION_SESSIONS + PRIOR_MATURATION_RECOVERY_SESSIONS  # 25
# ~7 calendar days per 5 trading sessions, plus a week of slack; strictly an
# upper-bound prefilter to keep the candidate set small.
PRIOR_MATURATION_MAX_LOOKBACK_DAYS = 45

# current-campaign maturity statuses
CURRENT_MATURED = "current_campaign_outcomes_matured"
CURRENT_MATURING = "current_campaign_outcomes_maturing"
CURRENT_DEFERRED = "current_campaign_outcomes_deferred"
CURRENT_UNVERIFIABLE = "current_campaign_outcomes_unverifiable"

# reason codes (so `deferred` can never hide a real data problem)
REASON_NO_FORWARD_SESSION_YET = "no_completed_forward_session_yet"
REASON_AWAITING_FURTHER_SESSIONS = "awaiting_further_forward_sessions"
REASON_ALL_HORIZONS_COMPLETE = "all_horizons_complete"
REASON_ELIGIBLE_THIS_ROUND = "eligible_forward_sessions_available"
REASON_FORWARD_HISTORY_ABSENT = "forward_history_absent_where_expected"
REASON_RETRYABLE_FAILURES = "retryable_outcome_failures_present"
REASON_TERMINAL_FAILURES = "terminal_outcome_failures_present"


def classify_current_campaign_maturity(*, preflight: Dict[str, Any],
                                       snapshot_session: str,
                                       target_session: str) -> Dict[str, Any]:
    """Pure. Decide the CURRENT campaign's truthful maturity status from its
    outcome preflight and its snapshot vs the occurrence's resolved target
    session. NEVER marks an outcome row successful; NEVER fabricates eligibility.

    The critical distinction is deferred-vs-unverifiable when eligibility is
    unknown (the local forward calendar is empty):
      * snapshot == target  -> the campaign IS for the latest completed session,
        so zero forward sessions is EXPECTED -> deferred;
      * snapshot <  target  -> forward sessions SHOULD exist but the local
        calendar is empty -> a real data problem -> unverifiable (a blocker).
    """
    unknown = int(preflight.get("eligibility_unknown_count", 0))
    eligible = int(preflight.get("enqueue_available_count", 0))
    matured = int(preflight.get("matured_count", 0))
    pairs = int(preflight.get("pair_count", 0))
    sessions = int(preflight.get("local_session_dates_count", 0))
    retryable = int(preflight.get("retryable_count", 0))
    terminal = int(preflight.get("terminal_count", 0))
    base = {
        "contract_version": CURRENT_MATURITY_CONTRACT,
        "registration_id": preflight.get("registration_id"),
        "snapshot_session": str(snapshot_session),
        "target_session": str(target_session),
        "completed_forward_sessions": sessions,
        "pair_count": pairs,
        "eligible_count": eligible,
        "matured_count": matured,
        "retryable_count": retryable,
        "terminal_count": terminal,
        "eligibility_unknown_count": unknown,
    }
    # Terminal failures are a genuine problem regardless of horizon progress.
    if terminal > 0:
        return {**base, "status": CURRENT_UNVERIFIABLE, "reason": REASON_TERMINAL_FAILURES}
    if unknown > 0:
        if str(snapshot_session) < str(target_session):
            return {**base, "status": CURRENT_UNVERIFIABLE, "reason": REASON_FORWARD_HISTORY_ABSENT}
        return {**base, "status": CURRENT_DEFERRED, "reason": REASON_NO_FORWARD_SESSION_YET}
    if eligible > 0:
        # Forward sessions are available now — this round will mature them.
        return {**base, "status": CURRENT_MATURING, "reason": REASON_ELIGIBLE_THIS_ROUND}
    # eligible == 0, unknown == 0, terminal == 0
    if pairs > 0 and matured == pairs:
        return {**base, "status": CURRENT_MATURED, "reason": REASON_ALL_HORIZONS_COMPLETE}
    # not_yet: the local calendar is known but fewer than the minimum sessions
    # have elapsed for a new horizon (or a benign retryable remains).
    reason = REASON_RETRYABLE_FAILURES if retryable > 0 else REASON_AWAITING_FURTHER_SESSIONS
    return {**base, "status": CURRENT_DEFERRED, "reason": reason}


async def _candidate_prior_registrations(
        conn: asyncpg.Connection, *, experiment_code: str, universe_id: str,
        universe_hash: str, current_registration_id: Optional[str],
        target_session: str, max_lookback_days: int = PRIOR_MATURATION_MAX_LOOKBACK_DAYS,
        ) -> List[Dict[str, Any]]:
    """BOUNDED candidate set: completed+executed prospective registrations of the
    SAME experiment AND frozen universe (id + hash), snapshot strictly before the
    target session and within the calendar lookback, excluding the current one.
    Never scans unrelated experiments/universes; never touches shared Supabase.
    """
    target = target_session if isinstance(target_session, date) else date.fromisoformat(str(target_session))
    rows = await conn.fetch(
        "SELECT id, registration_identity, snapshot_session_date, campaign_run_id, "
        "candidate_strategy_version, control_strategy_version "
        "FROM prospective_campaign_registrations "
        "WHERE experiment_code = $1 "
        "  AND universe_id = $2 "
        "  AND universe_hash = $3 "
        "  AND status = 'completed' "
        "  AND campaign_run_id IS NOT NULL "
        "  AND ($4::uuid IS NULL OR id <> $4) "
        "  AND snapshot_session_date < $5 "
        "  AND snapshot_session_date >= ($5 - ($6 || ' days')::interval) "
        "ORDER BY snapshot_session_date ASC",
        experiment_code, universe_id, universe_hash,
        current_registration_id, target, str(int(max_lookback_days)))
    return [dict(r) for r in rows]


async def select_eligible_prior_registrations(
        conn: asyncpg.Connection, *, experiment_code: str, universe_id: str,
        universe_hash: str, current_registration_id: Optional[str],
        target_session: str,
        max_lookback_days: int = PRIOR_MATURATION_MAX_LOOKBACK_DAYS) -> Dict[str, Any]:
    """Discover prior campaigns that have NEWLY eligible outcome work because the
    target session now exists. For each bounded candidate, run the existing
    read-only outcome preflight and keep those with enqueue_available_count > 0.
    Returns a bounded, deterministic plan (no enqueue, no mutation, no provider).
    """
    candidates = await _candidate_prior_registrations(
        conn, experiment_code=experiment_code, universe_id=universe_id,
        universe_hash=universe_hash, current_registration_id=current_registration_id,
        target_session=target_session, max_lookback_days=max_lookback_days)
    eligible: List[Dict[str, Any]] = []
    considered: List[Dict[str, Any]] = []
    for cand in candidates:
        pf = await build_outcome_maturity_preflight(
            conn, registration_id=str(cand["id"]),
            registration_identity=cand["registration_identity"])
        entry = {
            "registration_id": str(cand["id"]),
            "registration_identity": cand["registration_identity"],
            "snapshot_session": str(cand["snapshot_session_date"]),
            "completed_forward_sessions": pf.get("local_session_dates_count"),
            "enqueue_available_count": pf.get("enqueue_available_count"),
            "matured_count": pf.get("matured_count"),
            "eligibility_unknown_count": pf.get("eligibility_unknown_count"),
        }
        considered.append(entry)
        if int(pf.get("enqueue_available_count", 0)) > 0:
            eligible.append(entry)
    return {
        "contract_version": PRIOR_DISCOVERY_CONTRACT,
        "target_session": str(target_session),
        "experiment_code": experiment_code,
        "universe_id": universe_id,
        "universe_hash": universe_hash,
        "max_lookback_days": int(max_lookback_days),
        "max_lookback_sessions": PRIOR_MATURATION_MAX_LOOKBACK_SESSIONS,
        "candidate_count": len(candidates),
        "considered": considered,
        "eligible": eligible,
        "eligible_count": len(eligible),
    }


__all__ = [
    "CURRENT_MATURITY_CONTRACT", "PRIOR_DISCOVERY_CONTRACT",
    "PRIOR_MATURATION_MAX_LOOKBACK_SESSIONS", "PRIOR_MATURATION_MAX_LOOKBACK_DAYS",
    "CURRENT_MATURED", "CURRENT_MATURING", "CURRENT_DEFERRED", "CURRENT_UNVERIFIABLE",
    "classify_current_campaign_maturity", "select_eligible_prior_registrations",
]
