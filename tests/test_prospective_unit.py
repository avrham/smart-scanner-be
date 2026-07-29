"""Pure unit tests for the prospective pipeline: completed-session resolver,
registration/execution identities, candidate signal extraction, mode allowlist."""

from __future__ import annotations

from datetime import date, datetime, timezone

from app.prospective_session import (
    resolve_latest_completed_session, session_cutoff_utc, is_trading_day,
    us_market_holidays, MARKET_CALENDAR_VERSION)
from app.prospective_campaign import (
    registration_identity, campaign_execution_identity, candidate_signal_fields,
    CANDIDATE_ALLOW_ENTER)
from app.prospective_mode import is_prospective_route_allowed


def _utc(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


class TestSessionResolver:
    def test_after_close_trading_day(self):
        # 2026-07-28 is a Tuesday; 20:30 UTC = 16:30 EDT (>= close)
        assert resolve_latest_completed_session(_utc(2026, 7, 28, 20, 30)) == date(2026, 7, 28)

    def test_open_market_before_close(self):
        # 2026-07-28 17:00 UTC = 13:00 EDT (< close) -> prior trading day 07-27 (Mon)
        assert resolve_latest_completed_session(_utc(2026, 7, 28, 17, 0)) == date(2026, 7, 27)

    def test_weekend_returns_friday(self):
        # Sunday 2026-07-26 -> Friday 2026-07-24
        assert resolve_latest_completed_session(_utc(2026, 7, 26, 18, 0)) == date(2026, 7, 24)

    def test_holiday_skipped(self):
        # Jul 4 2026 is Saturday -> observed holiday Fri Jul 3; that day is not a
        # trading day, so an afternoon on Jul 3 resolves back to Thu Jul 2.
        assert not is_trading_day(date(2026, 7, 3))
        assert resolve_latest_completed_session(_utc(2026, 7, 3, 21, 0)) == date(2026, 7, 2)

    def test_early_close_day_counts_after_regular_close(self):
        # Day after Thanksgiving 2026 = Fri Nov 27 (early close 13:00 ET) IS a
        # trading day; after 16:00 ET it is a fully completed session.
        assert is_trading_day(date(2026, 11, 27))
        assert resolve_latest_completed_session(_utc(2026, 11, 27, 22, 0)) == date(2026, 11, 27)

    def test_partial_current_session_excluded(self):
        # do not use a partial 2026-07-29 (Wed) session before close
        assert resolve_latest_completed_session(_utc(2026, 7, 29, 14, 0)) != date(2026, 7, 29)

    def test_cutoff_is_utc_1600_et(self):
        cut = session_cutoff_utc(date(2026, 7, 28))  # EDT (UTC-4) -> 20:00 UTC
        assert cut == datetime(2026, 7, 28, 20, 0, tzinfo=timezone.utc)

    def test_known_2026_holidays(self):
        h = us_market_holidays(2026)
        assert date(2026, 1, 1) in h and date(2026, 12, 25) in h
        assert date(2026, 4, 3) in h        # Good Friday
        assert date(2026, 11, 26) in h      # Thanksgiving
        assert date(2026, 7, 3) in h        # Independence (observed)


class TestIdentities:
    def _kw(self):
        return dict(experiment_code="wyckoff_v2_vs_baseline", universe_id="u1",
                    universe_hash="sha256:uh", history_config_hash="sha256:ch",
                    snapshot_session_date="2026-07-28")

    def test_registration_identity_deterministic_and_sensitive(self):
        a = registration_identity(**self._kw())
        assert a == registration_identity(**self._kw()) and a.startswith("pcr:")
        assert a != registration_identity(**{**self._kw(), "snapshot_session_date": "2026-07-27"})
        assert a != registration_identity(**{**self._kw(), "universe_hash": "sha256:x"})

    def test_execution_identity_binds_registration(self):
        e = campaign_execution_identity(registration_identity_value="pcr:1",
                                        universe_hash="sha256:uh",
                                        history_readiness_manifest_hash="sha256:rh",
                                        snapshot_session_date="2026-07-28")
        assert e.startswith("pcx:")
        assert e != campaign_execution_identity(registration_identity_value="pcr:2",
                                                universe_hash="sha256:uh",
                                                history_readiness_manifest_hash="sha256:rh",
                                                snapshot_session_date="2026-07-28")

    def test_allow_enter_pinned_false(self):
        assert CANDIDATE_ALLOW_ENTER is False


class TestCandidateSignalFields:
    def test_pre_rollout_enter_eligible_is_primary_not_watch(self):
        # a fully-eligible-but-shadow-only candidate: verdict WATCH, but the
        # primary signal is enter_eligible_without_rollout_gate=True.
        details = {"readiness": {"status": "ready"}, "setup_state": "spring",
                   "trigger_state": "confirmed",
                   "policy": {"enter_eligible_without_rollout_gate": True,
                              "trigger_confirmed": True,
                              "waiting_reasons": ["enter_disabled_shadow_only"]},
                   "four_hour_trigger": {"state": "confirmed"}}
        sig = candidate_signal_fields(details)
        assert sig["enter_eligible_without_rollout_gate"] is True
        assert sig["rollout_blocked"] is True
        assert sig["trigger_confirmed"] is True
        assert sig["readiness_status"] == "ready"
        assert sig["four_hour_state"] == "confirmed"


class TestModeAllowlist:
    def test_allowlist(self):
        for r in ("/api/admin/prospective/access-check", "/api/admin/prospective/preflight",
                  "/api/admin/prospective/audit", "/version", "/health"):
            assert is_prospective_route_allowed("GET", r) is True
        assert is_prospective_route_allowed("POST", "/api/admin/prospective/register") is True
        assert is_prospective_route_allowed("POST", "/api/admin/prospective/execute") is True
        assert is_prospective_route_allowed("GET", "/api/admin/prospective/execute") is False
        assert is_prospective_route_allowed("POST", "/api/admin/prospective/audit") is False
        # every non-prospective admin route is blocked in this mode
        assert is_prospective_route_allowed("GET", "/api/admin/shadow-cohort/closeout") is False
        assert is_prospective_route_allowed("POST", "/api/admin/history-warmup/execute") is False
