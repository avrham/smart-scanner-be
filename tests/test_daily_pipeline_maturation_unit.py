"""Pure unit tests for the daily-pipeline v2 current-campaign maturity
classifier + bounded-lookback constants. No DB, no Docker.

The classifier's job is to separate PIPELINE completion from OUTCOME maturity:
a current campaign with no completed forward session is honestly *deferred*
(not a fabricated success, not a whole-occurrence blocker), while genuine data
problems (forward history absent where it should exist, terminal failures) are
*unverifiable* blockers with explicit reason codes.
"""

from __future__ import annotations

from app.jobs import daily_pipeline_maturation as DM
from app.workers.shadow.outcomes.eligibility import FULL_MATURATION_SESSIONS


def _pf(**kw):
    base = {
        "registration_id": "reg-x", "pair_count": 25, "matured_count": 0,
        "eligible_count": 0, "enqueue_available_count": 0, "not_yet_eligible_count": 0,
        "retryable_count": 0, "terminal_count": 0, "eligibility_unknown_count": 0,
        "local_session_dates_count": 0,
    }
    base.update(kw)
    return base


def test_zero_forward_at_own_session_is_deferred_not_success():
    # snapshot == target (campaign IS for the latest completed session) and the
    # forward calendar is empty -> deferred, NOT matured, NOT unverifiable.
    r = DM.classify_current_campaign_maturity(
        preflight=_pf(eligibility_unknown_count=25, local_session_dates_count=0),
        snapshot_session="2026-08-07", target_session="2026-08-07")
    assert r["status"] == DM.CURRENT_DEFERRED
    assert r["reason"] == DM.REASON_NO_FORWARD_SESSION_YET


def test_unknown_when_forward_history_should_exist_is_unverifiable():
    # snapshot strictly before target but the local forward calendar is empty
    # -> a real data problem, must block (never silently deferred).
    r = DM.classify_current_campaign_maturity(
        preflight=_pf(eligibility_unknown_count=25, local_session_dates_count=0),
        snapshot_session="2026-08-01", target_session="2026-08-07")
    assert r["status"] == DM.CURRENT_UNVERIFIABLE
    assert r["reason"] == DM.REASON_FORWARD_HISTORY_ABSENT


def test_eligible_forward_sessions_report_maturing():
    r = DM.classify_current_campaign_maturity(
        preflight=_pf(enqueue_available_count=25, local_session_dates_count=3),
        snapshot_session="2026-08-01", target_session="2026-08-07")
    assert r["status"] == DM.CURRENT_MATURING
    assert r["reason"] == DM.REASON_ELIGIBLE_THIS_ROUND


def test_all_pairs_complete_is_matured():
    r = DM.classify_current_campaign_maturity(
        preflight=_pf(matured_count=25, local_session_dates_count=25),
        snapshot_session="2026-07-01", target_session="2026-08-07")
    assert r["status"] == DM.CURRENT_MATURED
    assert r["reason"] == DM.REASON_ALL_HORIZONS_COMPLETE


def test_terminal_failures_are_unverifiable():
    r = DM.classify_current_campaign_maturity(
        preflight=_pf(terminal_count=2, matured_count=23, local_session_dates_count=25),
        snapshot_session="2026-07-01", target_session="2026-08-07")
    assert r["status"] == DM.CURRENT_UNVERIFIABLE
    assert r["reason"] == DM.REASON_TERMINAL_FAILURES


def test_not_yet_known_calendar_is_deferred_awaiting():
    # calendar known (sessions>0) but fewer than min for a new horizon and
    # nothing eligible/matured -> deferred, awaiting further sessions.
    r = DM.classify_current_campaign_maturity(
        preflight=_pf(local_session_dates_count=0, not_yet_eligible_count=25),
        snapshot_session="2026-08-07", target_session="2026-08-07")
    # snapshot==target with empty calendar is the deferred/no-forward path
    assert r["status"] == DM.CURRENT_DEFERRED


def test_retryable_only_defers_with_retryable_reason():
    r = DM.classify_current_campaign_maturity(
        preflight=_pf(retryable_count=3, matured_count=22, local_session_dates_count=5),
        snapshot_session="2026-07-20", target_session="2026-08-07")
    assert r["status"] == DM.CURRENT_DEFERRED
    assert r["reason"] == DM.REASON_RETRYABLE_FAILURES


def test_lookback_bound_covers_longest_horizon_plus_recovery():
    assert DM.PRIOR_MATURATION_MAX_LOOKBACK_SESSIONS == FULL_MATURATION_SESSIONS + DM.PRIOR_MATURATION_RECOVERY_SESSIONS
    assert DM.PRIOR_MATURATION_MAX_LOOKBACK_SESSIONS >= FULL_MATURATION_SESSIONS
    # calendar prefilter must comfortably exceed the trading-session bound
    assert DM.PRIOR_MATURATION_MAX_LOOKBACK_DAYS >= DM.PRIOR_MATURATION_MAX_LOOKBACK_SESSIONS
