"""Earnings catalyst context (app/catalyst.py).

Catalyst data is CONTEXT. The risk this layer carries is not arithmetic — it is
claiming to know something we do not. So most of these tests are about the
boundary between "there is no event", "we cannot see the schedule", and "we
could not have known this at the time".

Nothing here should ever influence a verdict, a Wyckoff value or an attention
tier; `test_catalyst_isolation.py` holds that guarantee.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

import app.catalyst as cat

UTC = timezone.utc

# 2026-08-26 is a Wednesday; the surrounding weekdays are ordinary sessions.
SESSION = date(2026, 8, 26)


def observed(days_before=1):
    return datetime(SESSION.year, SESSION.month, SESSION.day,
                    12, 0, tzinfo=UTC) - timedelta(days=days_before)


def event(event_date, *, event_type=cat.EVENT_EARNINGS, timing="unknown",
          certainty="confirmed", observed_at=None, symbol="AAPL", **extra):
    row = {
        "symbol": symbol,
        "event_type": event_type,
        "event_date": event_date,
        "session_timing": timing,
        "certainty": certainty,
        "fiscal_period": "Q3",
        "fiscal_year": "2026",
        "source": "provider_earnings_calendar",
        "source_reference": "ref-1",
        "observed_at": observed_at if observed_at is not None else observed(),
    }
    row.update(extra)
    return row


def fresh(status=cat.STATUS_AVAILABLE, reason=None):
    return {"status": status, "reason": reason, "last_refresh_at": None,
            "last_success_at": None, "age_hours": 1.0, "detail": None}


def ctx(events, *, session=SESSION, earnings=None, filings=None,
        point_in_time=True):
    return cat.build_catalyst_context(
        events, as_of_session=session,
        earnings_freshness=earnings or fresh(),
        filings_freshness=filings or fresh(),
        require_point_in_time=point_in_time)


# --------------------------------------------------------------------------- #
# Session counting — the unit the product actually reasons in
# --------------------------------------------------------------------------- #

class TestTradingSessions:
    def test_counts_zero_for_the_same_session(self):
        assert cat.trading_sessions_between(SESSION, SESSION) == 0

    def test_counts_trading_sessions_not_calendar_days(self):
        # Wed 26th -> Mon 31st is 5 calendar days but only 3 sessions.
        assert cat.trading_sessions_between(SESSION, date(2026, 8, 31)) == 3

    def test_is_negative_for_a_past_event(self):
        assert cat.trading_sessions_between(SESSION, date(2026, 8, 24)) == -2

    def test_skips_a_weekend_in_both_directions(self):
        fri, mon = date(2026, 8, 28), date(2026, 8, 31)
        assert cat.trading_sessions_between(fri, mon) == 1
        assert cat.trading_sessions_between(mon, fri) == -1

    def test_gives_up_rather_than_walking_forever(self):
        assert cat.trading_sessions_between(SESSION, date(2032, 1, 1)) is None


class TestProximity:
    @pytest.mark.parametrize("sessions,expected", [
        (0, cat.PROXIMITY_TODAY),
        (1, cat.PROXIMITY_IMMINENT),
        (2, cat.PROXIMITY_IMMINENT),
        (3, cat.PROXIMITY_NEAR),
        (7, cat.PROXIMITY_NEAR),
        (8, cat.PROXIMITY_UPCOMING),
        (21, cat.PROXIMITY_UPCOMING),
        (22, cat.PROXIMITY_DISTANT),
        (-1, cat.PROXIMITY_RECENT),
        (-5, cat.PROXIMITY_RECENT),
        (-6, cat.PROXIMITY_DISTANT),   # known, but too far back to matter
        (None, cat.PROXIMITY_NONE_KNOWN),
    ])
    def test_buckets_are_a_priori_and_exhaustive(self, sessions, expected):
        assert cat.classify_proximity(sessions) == expected

    def test_distant_is_not_notable_so_the_ui_stays_silent(self):
        assert not cat.is_notable(cat.PROXIMITY_DISTANT)
        assert not cat.is_notable(cat.PROXIMITY_NONE_KNOWN)

    def test_everything_the_user_should_see_is_notable(self):
        for p in (cat.PROXIMITY_TODAY, cat.PROXIMITY_IMMINENT,
                  cat.PROXIMITY_NEAR, cat.PROXIMITY_UPCOMING,
                  cat.PROXIMITY_RECENT):
            assert cat.is_notable(p)


# --------------------------------------------------------------------------- #
# Session timing — "today" means four different things
# --------------------------------------------------------------------------- #

class TestSameSessionTiming:
    def test_before_market_means_it_already_happened(self):
        assert cat.resolve_same_session("before_market") == cat.SAME_SESSION_BEFORE_OPEN

    def test_after_market_means_it_has_not_happened_yet(self):
        assert cat.resolve_same_session("after_market") == cat.SAME_SESSION_AFTER_CLOSE

    def test_unknown_timing_stays_unknown_and_is_never_guessed(self):
        assert cat.resolve_same_session("unknown") == cat.SAME_SESSION_UNKNOWN
        assert cat.resolve_same_session(None) == cat.SAME_SESSION_UNKNOWN
        assert cat.resolve_same_session("garbage") == cat.SAME_SESSION_UNKNOWN

    def test_same_session_is_resolved_only_for_a_same_day_event(self):
        today = ctx([event(SESSION, timing="after_market")])["earnings"]
        assert today["proximity"] == cat.PROXIMITY_TODAY
        assert today["same_session"] == cat.SAME_SESSION_AFTER_CLOSE

        tomorrow = ctx([event(date(2026, 8, 27), timing="after_market")])["earnings"]
        assert tomorrow["same_session"] is None


# --------------------------------------------------------------------------- #
# Freshness — an empty table is ambiguous without it
# --------------------------------------------------------------------------- #

class TestFreshness:
    NOW = datetime(2026, 8, 26, 20, 0, tzinfo=UTC)

    def test_a_source_that_never_ran_is_not_the_same_as_no_events(self):
        f = cat.evaluate_freshness(None, now=self.NOW)
        assert f["status"] == cat.STATUS_UNAVAILABLE
        assert f["reason"] == cat.REASON_NEVER_REFRESHED

    def test_an_entitlement_failure_is_reported_as_source_unavailable(self):
        f = cat.evaluate_freshness(
            {"status": "unavailable", "last_refresh_at": self.NOW,
             "last_success_at": None, "detail": "provider_not_entitled"},
            now=self.NOW)
        assert f["status"] == cat.STATUS_UNAVAILABLE
        assert f["reason"] == cat.REASON_SOURCE_UNAVAILABLE

    def test_a_recent_success_is_available(self):
        f = cat.evaluate_freshness(
            {"status": "ok", "last_refresh_at": self.NOW,
             "last_success_at": self.NOW - timedelta(hours=2)}, now=self.NOW)
        assert f["status"] == cat.STATUS_AVAILABLE
        assert f["age_hours"] == pytest.approx(2.0, abs=0.01)

    def test_an_old_success_degrades_to_stale_rather_than_lying(self):
        f = cat.evaluate_freshness(
            {"status": "ok", "last_refresh_at": self.NOW,
             "last_success_at": self.NOW - timedelta(hours=48)}, now=self.NOW)
        assert f["status"] == cat.STATUS_STALE
        assert f["reason"] == cat.REASON_STALE_REFRESH

    def test_the_staleness_boundary_is_explicit(self):
        just_inside = cat.evaluate_freshness(
            {"status": "ok", "last_refresh_at": self.NOW,
             "last_success_at": self.NOW - timedelta(
                 hours=cat.FRESHNESS_MAX_AGE_HOURS - 0.1)}, now=self.NOW)
        just_outside = cat.evaluate_freshness(
            {"status": "ok", "last_refresh_at": self.NOW,
             "last_success_at": self.NOW - timedelta(
                 hours=cat.FRESHNESS_MAX_AGE_HOURS + 0.1)}, now=self.NOW)
        assert just_inside["status"] == cat.STATUS_AVAILABLE
        assert just_outside["status"] == cat.STATUS_STALE

    def test_stale_still_serves_the_event_it_knows_about(self):
        c = ctx([event(date(2026, 8, 28))],
                earnings=fresh(cat.STATUS_STALE, cat.REASON_STALE_REFRESH))
        assert c["earnings"]["status"] == cat.STATUS_STALE
        assert c["earnings"]["event_date"] == "2026-08-28"
        assert c["earnings"]["notable"] is True


# --------------------------------------------------------------------------- #
# Choosing the event that matters
# --------------------------------------------------------------------------- #

class TestSelection:
    def test_prefers_the_nearest_upcoming_event(self):
        e = ctx([event(date(2026, 9, 30)), event(date(2026, 8, 28))])["earnings"]
        assert e["event_date"] == "2026-08-28"

    def test_an_event_dated_today_wins_over_a_later_one(self):
        e = ctx([event(date(2026, 8, 28)), event(SESSION)])["earnings"]
        assert e["event_date"] == SESSION.isoformat()

    def test_falls_back_to_the_most_recent_past_event(self):
        e = ctx([event(date(2026, 8, 10)), event(date(2026, 8, 20))])["earnings"]
        assert e["event_date"] == "2026-08-20"
        assert e["proximity"] == cat.PROXIMITY_RECENT

    def test_a_long_past_event_is_known_but_not_surfaced(self):
        # `distant` and `none_known` are different facts: one says "we know of
        # an event, it is just far away", the other says "we looked and there is
        # none". Both stay silent, but the product must not confuse them.
        e = ctx([event(date(2026, 5, 1))])["earnings"]
        assert e["proximity"] == cat.PROXIMITY_DISTANT
        assert e["event_date"] == "2026-05-01"
        assert e["notable"] is False

    def test_no_events_at_all_is_a_clean_silent_answer(self):
        e = ctx([])["earnings"]
        assert e["status"] == cat.STATUS_AVAILABLE
        assert e["event_date"] is None
        assert e["proximity"] == cat.PROXIMITY_NONE_KNOWN
        assert e["notable"] is False

    def test_earnings_and_filings_never_bleed_into_each_other(self):
        c = ctx([
            event(date(2026, 8, 28), event_type=cat.EVENT_EARNINGS),
            event(date(2026, 8, 3), event_type=cat.EVENT_FILING,
                  certainty="filed"),
        ])
        assert c["earnings"]["event_date"] == "2026-08-28"
        assert c["last_financial_report"]["event_date"] == "2026-08-03"

    def test_a_filing_only_symbol_reports_no_known_earnings(self):
        c = ctx([event(date(2026, 8, 3), event_type=cat.EVENT_FILING,
                       certainty="filed")])
        assert c["earnings"]["event_date"] is None
        assert c["earnings"]["notable"] is False
        assert c["last_financial_report"]["certainty"] == "filed"


class TestCertainty:
    def test_confirmed_is_carried_through_verbatim(self):
        e = ctx([event(date(2026, 8, 28), certainty="confirmed")])["earnings"]
        assert e["certainty"] == "confirmed"

    def test_estimated_is_carried_through_verbatim_and_never_upgraded(self):
        e = ctx([event(date(2026, 8, 28), certainty="estimated")])["earnings"]
        assert e["certainty"] == "estimated"
        assert e["notable"] is True

    def test_provenance_survives_into_the_product_object(self):
        e = ctx([event(date(2026, 8, 28))])["earnings"]
        assert e["source"] == "provider_earnings_calendar"
        assert e["source_reference"] == "ref-1"
        assert e["observed_at"] is not None
        assert e["fiscal_period"] == "Q3" and e["fiscal_year"] == "2026"


# --------------------------------------------------------------------------- #
# Point-in-time correctness — the lookahead guarantee
# --------------------------------------------------------------------------- #

class TestPointInTime:
    def test_a_future_event_we_only_learned_about_later_is_withheld(self):
        historical = date(2026, 7, 1)
        c = cat.build_catalyst_context(
            [event(date(2026, 7, 20), observed_at=observed(0))],
            as_of_session=historical,
            earnings_freshness=fresh(), filings_freshness=fresh())
        assert c["earnings"]["event_date"] is None
        assert c["earnings"]["status"] == cat.STATUS_UNAVAILABLE
        assert c["earnings"]["reason"] == cat.REASON_NO_POINT_IN_TIME

    def test_a_future_event_observed_in_time_is_offered(self):
        historical = date(2026, 7, 1)
        c = cat.build_catalyst_context(
            [event(date(2026, 7, 20),
                   observed_at=datetime(2026, 6, 25, 9, 0, tzinfo=UTC))],
            as_of_session=historical,
            earnings_freshness=fresh(), filings_freshness=fresh())
        assert c["earnings"]["event_date"] == "2026-07-20"

    def test_a_past_event_needs_no_lookahead_guard(self):
        # Already occurred by the session, so knowing it involves no foresight.
        c = cat.build_catalyst_context(
            [event(date(2026, 6, 25), observed_at=observed(0))],
            as_of_session=date(2026, 6, 29),
            earnings_freshness=fresh(), filings_freshness=fresh())
        assert c["earnings"]["event_date"] == "2026-06-25"

    def test_the_guard_can_be_disabled_only_explicitly(self):
        c = cat.build_catalyst_context(
            [event(date(2026, 7, 20), observed_at=observed(0))],
            as_of_session=date(2026, 7, 1),
            earnings_freshness=fresh(), filings_freshness=fresh(),
            require_point_in_time=False)
        assert c["earnings"]["event_date"] == "2026-07-20"

    def test_the_live_session_is_unaffected_by_the_guard(self):
        e = ctx([event(date(2026, 8, 28), observed_at=observed(1))])["earnings"]
        assert e["event_date"] == "2026-08-28"


# --------------------------------------------------------------------------- #
# Degradation — the scanner must survive every catalyst failure
# --------------------------------------------------------------------------- #

class TestDegradation:
    def test_an_unavailable_source_says_so_instead_of_claiming_no_event(self):
        c = ctx([event(date(2026, 8, 28))],
                earnings=fresh(cat.STATUS_UNAVAILABLE,
                               cat.REASON_SOURCE_UNAVAILABLE))
        e = c["earnings"]
        assert e["status"] == cat.STATUS_UNAVAILABLE
        assert e["reason"] == cat.REASON_SOURCE_UNAVAILABLE
        assert e["event_date"] is None
        assert e["notable"] is False

    def test_one_source_failing_does_not_hide_the_other(self):
        c = ctx([event(date(2026, 8, 3), event_type=cat.EVENT_FILING,
                       certainty="filed")],
                earnings=fresh(cat.STATUS_UNAVAILABLE,
                               cat.REASON_SOURCE_UNAVAILABLE))
        assert c["earnings"]["status"] == cat.STATUS_UNAVAILABLE
        assert c["last_financial_report"]["status"] == cat.STATUS_AVAILABLE
        assert c["last_financial_report"]["event_date"] == "2026-08-03"

    def test_a_missing_session_cannot_produce_a_confident_answer(self):
        c = cat.build_catalyst_context(
            [event(date(2026, 8, 28))], as_of_session=None,
            earnings_freshness=fresh(), filings_freshness=fresh())
        assert c["as_of_session"] is None
        assert c["earnings"]["status"] == cat.STATUS_UNAVAILABLE
        assert c["earnings"]["event_date"] is None

    def test_the_total_failure_object_is_shaped_like_a_normal_one(self):
        empty = cat.empty_catalyst_context()
        normal = ctx([event(date(2026, 8, 28))])
        assert empty.keys() == normal.keys()
        assert empty["earnings"].keys() == normal["earnings"].keys()
        assert empty["earnings"]["status"] == cat.STATUS_UNAVAILABLE
        assert empty["earnings"]["notable"] is False


# --------------------------------------------------------------------------- #
# Contract shape
# --------------------------------------------------------------------------- #

class TestContract:
    def test_the_context_is_versioned(self):
        assert ctx([])["contract_version"] == "smart_scanner_catalyst_context.v1"

    def test_every_block_has_an_identical_shape_on_every_branch(self):
        branches = [
            ctx([]),
            ctx([event(date(2026, 8, 28))]),
            ctx([event(SESSION, timing="before_market")]),
            ctx([event(date(2026, 8, 20))]),
            ctx([event(date(2026, 5, 1))]),
            ctx([], earnings=fresh(cat.STATUS_UNAVAILABLE,
                                   cat.REASON_SOURCE_UNAVAILABLE)),
            ctx([], earnings=fresh(cat.STATUS_STALE, cat.REASON_STALE_REFRESH)),
            cat.empty_catalyst_context(),
        ]
        shape = set(branches[0]["earnings"].keys())
        for b in branches:
            assert set(b["earnings"].keys()) == shape
            assert set(b["last_financial_report"].keys()) == shape

    def test_the_row_subset_is_compact_and_carries_the_silence_gate(self):
        row = cat.build_row_catalyst(ctx([event(date(2026, 8, 28))]))
        assert set(row) == {
            "earnings_status", "earnings_proximity", "earnings_sessions_until",
            "earnings_timing", "earnings_certainty", "earnings_notable",
            "last_report_proximity", "last_report_sessions_until",
            "last_report_notable"}
        assert row["earnings_notable"] is True

    def test_the_row_subset_stays_silent_when_there_is_nothing_to_say(self):
        row = cat.build_row_catalyst(ctx([]))
        assert row["earnings_notable"] is False
        assert row["earnings_proximity"] == cat.PROXIMITY_NONE_KNOWN
        assert row["earnings_sessions_until"] is None

    def test_the_row_subset_never_carries_free_text(self):
        row = cat.build_row_catalyst(ctx([event(date(2026, 8, 28))]))
        assert "source_reference" not in row
        assert "detail" not in row

    def test_every_emitted_status_is_in_the_declared_vocabulary(self):
        for c in (ctx([]), ctx([event(date(2026, 8, 28))]),
                  cat.empty_catalyst_context()):
            assert c["earnings"]["status"] in cat.CATALYST_STATUSES
            assert c["earnings"]["proximity"] in cat.PROXIMITIES
