"""Trading-session maturation eligibility (shadow_maturation_eligibility.v1).

Proves eligibility is measured in COMPLETED TRADING SESSIONS (never calendar
days) and that every distinct state — matured, eligible, not-yet-eligible,
retryable/terminal failure, missing session data, unknown — is classified
honestly.
"""

from __future__ import annotations

from datetime import date, timedelta

from app.workers.shadow.outcomes.eligibility import (
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_MATURED,
    ELIGIBILITY_MISSING_SESSION_DATA,
    ELIGIBILITY_NOT_YET,
    ELIGIBILITY_RETRYABLE,
    ELIGIBILITY_TERMINAL,
    ELIGIBILITY_UNKNOWN,
    FULL_MATURATION_SESSIONS,
    MIN_MATURATION_SESSIONS,
    classify_maturation_eligibility,
    completed_forward_sessions,
    summarize_eligibility,
)


def _weekday_calendar(start: date, n: int) -> list:
    """n weekday 'sessions' (a stand-in trading calendar)."""
    out: list = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


class TestCompletedForwardSessions:
    def test_counts_sessions_strictly_after_snapshot(self):
        cal = _weekday_calendar(date(2026, 5, 1), 20)
        snapshot = cal[0]
        # 19 sessions strictly after the snapshot session.
        assert completed_forward_sessions(cal, snapshot) == 19

    def test_respects_latest_completed_cap(self):
        cal = _weekday_calendar(date(2026, 5, 1), 20)
        snapshot = cal[0]
        cap = cal[5]
        assert completed_forward_sessions(
            cal, snapshot, latest_completed_session=cap
        ) == 5

    def test_empty_calendar_is_unknown_not_zero(self):
        assert completed_forward_sessions([], date(2026, 5, 1)) is None

    def test_uses_trading_sessions_not_calendar_days(self):
        # 3 trading sessions can span a weekend/holiday (>3 calendar days),
        # yet eligibility counts the 3 sessions, never the calendar delta.
        cal = [date(2026, 5, 1), date(2026, 5, 4), date(2026, 5, 5)]
        snapshot = date(2026, 5, 1)
        assert completed_forward_sessions(cal, snapshot) == 2


class TestClassification:
    def test_complete_is_matured(self):
        assert classify_maturation_eligibility(
            outcome_status="complete", completed_forward_sessions=25
        ) == ELIGIBILITY_MATURED

    def test_missing_row_with_enough_sessions_is_eligible(self):
        assert classify_maturation_eligibility(
            outcome_status=None,
            completed_forward_sessions=MIN_MATURATION_SESSIONS,
        ) == ELIGIBILITY_ELIGIBLE

    def test_missing_row_with_too_few_sessions_is_not_yet(self):
        assert classify_maturation_eligibility(
            outcome_status=None,
            completed_forward_sessions=MIN_MATURATION_SESSIONS - 1,
        ) == ELIGIBILITY_NOT_YET

    def test_zero_forward_sessions_is_not_yet(self):
        assert classify_maturation_eligibility(
            outcome_status=None, completed_forward_sessions=0
        ) == ELIGIBILITY_NOT_YET

    def test_partial_with_more_sessions_stays_eligible(self):
        assert classify_maturation_eligibility(
            outcome_status="partial",
            completed_forward_sessions=FULL_MATURATION_SESSIONS,
        ) == ELIGIBILITY_ELIGIBLE

    def test_pending_without_calendar_is_unknown(self):
        assert classify_maturation_eligibility(
            outcome_status="pending_forward_bars",
            completed_forward_sessions=None,
        ) == ELIGIBILITY_UNKNOWN

    def test_forward_fetch_error_is_retryable(self):
        assert classify_maturation_eligibility(
            outcome_status="error", error_code="forward_fetch_error",
        ) == ELIGIBILITY_RETRYABLE

    def test_provider_errors_are_retryable(self):
        for code in ("provider_mismatch", "provider_range_unsupported"):
            assert classify_maturation_eligibility(
                outcome_status="error", error_code=code,
            ) == ELIGIBILITY_RETRYABLE

    def test_reference_revision_is_terminal(self):
        assert classify_maturation_eligibility(
            outcome_status="error", error_code="reference_revision_detected",
        ) == ELIGIBILITY_TERMINAL

    def test_snapshot_bar_missing_is_missing_session_data(self):
        assert classify_maturation_eligibility(
            outcome_status="error", error_code="snapshot_bar_missing",
        ) == ELIGIBILITY_MISSING_SESSION_DATA

    def test_unknown_error_defaults_retryable(self):
        assert classify_maturation_eligibility(
            outcome_status="error", error_code="something_new",
        ) == ELIGIBILITY_RETRYABLE


class TestSummary:
    def test_counts_and_unresolved_total(self):
        items = [
            {"outcome_status": "complete", "completed_forward_sessions": 25},
            {"outcome_status": None, "completed_forward_sessions": 5},
            {"outcome_status": None, "completed_forward_sessions": 0},
            {"outcome_status": "error", "error_code": "forward_fetch_error"},
            {"outcome_status": "error",
             "error_code": "reference_revision_detected"},
        ]
        summary = summarize_eligibility(items)
        counts = summary["counts"]
        assert counts["matured"] == 1
        assert counts["eligible"] == 1
        assert counts["not_yet_eligible"] == 1
        assert counts["retryable_failure"] == 1
        assert counts["terminal_failure"] == 1
        # unresolved = eligible + retryable
        assert summary["unresolved_action_required_count"] == 2
        assert summary["min_maturation_sessions"] == MIN_MATURATION_SESSIONS
        assert summary["full_maturation_sessions"] == FULL_MATURATION_SESSIONS

    def test_every_state_present_with_zeros(self):
        summary = summarize_eligibility([])
        assert set(summary["counts"]) == {
            "matured", "eligible", "not_yet_eligible", "retryable_failure",
            "terminal_failure", "missing_market_session_data",
            "eligibility_unknown",
        }
        assert all(v == 0 for v in summary["counts"].values())
