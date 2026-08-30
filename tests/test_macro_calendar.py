"""The market-calendar reading layer: proximity, point in time, and silence.

Everything here is pure. The properties worth guarding are the ones that would
turn a calendar into a claim: a macro event acquiring a direction, a score, a
per-symbol field, or an influence on a verdict.
"""

from datetime import date, datetime, timezone

import app.macro_calendar as mc
import app.scanner_view as sv

UTC = timezone.utc
NOW = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
SESSION = date(2026, 8, 31)


def event(event_type=mc.EVENT_FOMC_RATE_DECISION, *, scheduled,
          observed=datetime(2026, 8, 1, tzinfo=UTC), listing="listed",
          source="federal_reserve", title="FOMC meeting"):
    return {"source": source, "event_type": event_type, "title": title,
            "scheduled_date": scheduled, "scheduled_start_date": None,
            "scheduled_time_local": None,
            "scheduled_timezone": "America/New_York",
            "source_listing": listing, "has_press_conference": None,
            "has_projections": None,
            "source_reference": "https://example.invalid/calendar",
            "first_observed_at": observed, "observed_at": observed}


FRESH = {"status": mc.AVAIL_AVAILABLE, "reason": None, "age_hours": 2.0,
         "last_refresh_at": None, "last_success_at": None, "detail": None,
         "per_source": {}}


class TestProximity:
    def test_vocabulary_is_calendar_days_not_sessions(self):
        assert mc.classify_proximity(0) == mc.PROXIMITY_TODAY
        assert mc.classify_proximity(1) == mc.PROXIMITY_TOMORROW
        assert mc.classify_proximity(2) == mc.PROXIMITY_WITHIN_3_DAYS
        assert mc.classify_proximity(3) == mc.PROXIMITY_WITHIN_3_DAYS
        assert mc.classify_proximity(4) == mc.PROXIMITY_NONE_NEARBY
        assert mc.classify_proximity(-1) == mc.PROXIMITY_RECENTLY_RELEASED
        assert mc.classify_proximity(-3) == mc.PROXIMITY_RECENTLY_RELEASED
        assert mc.classify_proximity(-4) == mc.PROXIMITY_NONE_NEARBY
        assert mc.classify_proximity(None) == mc.PROXIMITY_UNAVAILABLE

    def test_every_value_is_in_the_declared_vocabulary(self):
        for days in range(-40, 40):
            assert mc.classify_proximity(days) in mc.PROXIMITIES

    def test_only_the_near_ones_are_banner_worthy(self):
        assert mc.is_nearby(mc.PROXIMITY_TODAY)
        assert mc.is_nearby(mc.PROXIMITY_TOMORROW)
        assert mc.is_nearby(mc.PROXIMITY_WITHIN_3_DAYS)
        assert not mc.is_nearby(mc.PROXIMITY_RECENTLY_RELEASED)
        assert not mc.is_nearby(mc.PROXIMITY_NONE_NEARBY)


class TestEventStatus:
    def test_derived_from_the_calendar_and_the_listing(self):
        assert mc.event_status(date(2026, 9, 16), listing="listed",
                               as_of=SESSION) == mc.STATUS_SCHEDULED
        assert mc.event_status(date(2026, 8, 20), listing="listed",
                               as_of=SESSION) == mc.STATUS_RELEASED

    def test_withdrawn_is_unknown_never_cancelled(self):
        # We have no standing to say an agency cancelled anything; only that
        # it stopped listing it.
        assert mc.event_status(date(2026, 9, 16), listing="withdrawn",
                               as_of=SESSION) == mc.STATUS_UNKNOWN


class TestPointInTime:
    def test_an_event_we_had_not_yet_read_is_invisible(self):
        # Added to the Fed's page on the 2nd; a session on the 1st may not see
        # it even though its scheduled date is later.
        late = event(scheduled=date(2026, 9, 16),
                     observed=datetime(2026, 9, 2, 15, 0, tzinfo=UTC))
        assert not mc.is_visible_to_session(late["first_observed_at"],
                                            date(2026, 9, 1))
        assert mc.is_visible_to_session(late["first_observed_at"],
                                        date(2026, 9, 2))

    def test_selection_applies_the_gate(self):
        rows = [event(scheduled=date(2026, 9, 16),
                      observed=datetime(2026, 9, 2, tzinfo=UTC))]
        assert mc.select_visible_events(
            rows, as_of_session=date(2026, 9, 1),
            as_of_date=date(2026, 9, 1)) == []
        assert len(mc.select_visible_events(
            rows, as_of_session=date(2026, 9, 3),
            as_of_date=date(2026, 9, 3))) == 1

    def test_missing_observation_is_never_visible(self):
        assert not mc.is_visible_to_session(None, SESSION)


class TestContext:
    def test_headline_is_the_nearest_upcoming_event(self):
        rows = [event(scheduled=date(2026, 9, 16)),
                event(mc.EVENT_PCE, scheduled=date(2026, 9, 1),
                      source="bea", title="Personal Income and Outlays")]
        ctx = mc.build_market_calendar_context(
            rows, as_of_session=SESSION, as_of_date=SESSION, freshness=FRESH)
        assert ctx["headline"]["event_type"] == mc.EVENT_PCE
        assert ctx["proximity"] == mc.PROXIMITY_TOMORROW
        assert len(ctx["upcoming"]) == 2

    def test_block_is_market_wide_and_says_so(self):
        ctx = mc.build_market_calendar_context(
            [event(scheduled=date(2026, 9, 1))], as_of_session=SESSION, as_of_date=SESSION,
            freshness=FRESH)
        assert ctx["applies_to"] == "market_wide"
        assert ctx["contract_version"] == mc.MARKET_CALENDAR_CONTRACT_VERSION

    def test_no_direction_and_no_score_anywhere_in_the_block(self):
        ctx = mc.build_market_calendar_context(
            [event(scheduled=date(2026, 9, 1)),
             event(mc.EVENT_GDP, scheduled=date(2026, 8, 29), source="bea")],
            as_of_session=SESSION, as_of_date=SESSION, freshness=FRESH)
        banned = {"score", "direction", "risk", "impact", "bullish", "bearish",
                  "confidence", "rank", "weight"}
        def walk(node):
            if isinstance(node, dict):
                for key, value in node.items():
                    assert key not in banned, f"forbidden field {key!r}"
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)
        walk(ctx)

    def test_recently_released_only_when_nothing_is_near(self):
        rows = [event(mc.EVENT_GDP, scheduled=date(2026, 8, 29), source="bea")]
        ctx = mc.build_market_calendar_context(
            rows, as_of_session=SESSION, as_of_date=SESSION, freshness=FRESH)
        assert ctx["proximity"] == mc.PROXIMITY_RECENTLY_RELEASED
        assert ctx["recent"] and not ctx["upcoming"]

    def test_a_distant_event_does_not_headline_as_nearby(self):
        ctx = mc.build_market_calendar_context(
            [event(scheduled=date(2026, 9, 16))], as_of_session=SESSION, as_of_date=SESSION,
            freshness=FRESH)
        assert ctx["proximity"] == mc.PROXIMITY_NONE_NEARBY
        assert ctx["upcoming"]           # still available, just not a banner

    def test_empty_calendar_is_none_nearby_not_unavailable(self):
        ctx = mc.build_market_calendar_context(
            [], as_of_session=SESSION, as_of_date=SESSION, freshness=FRESH)
        assert ctx["status"] == mc.AVAIL_AVAILABLE
        assert ctx["proximity"] == mc.PROXIMITY_NONE_NEARBY
        assert ctx["headline"] is None


class TestUnavailable:
    def test_unavailable_never_shows_events(self):
        stale = {"status": mc.AVAIL_UNAVAILABLE,
                 "reason": mc.REASON_SOURCE_UNAVAILABLE, "age_hours": None,
                 "per_source": {}}
        ctx = mc.build_market_calendar_context(
            [event(scheduled=date(2026, 9, 1))], as_of_session=SESSION, as_of_date=SESSION,
            freshness=stale)
        assert ctx["status"] == mc.AVAIL_UNAVAILABLE
        assert ctx["upcoming"] == [] and ctx["headline"] is None
        assert ctx["proximity"] == mc.PROXIMITY_UNAVAILABLE

    def test_empty_context_helper_is_unavailable(self):
        ctx = mc.empty_market_calendar_context()
        assert ctx["status"] == mc.AVAIL_UNAVAILABLE
        assert ctx["proximity"] == mc.PROXIMITY_UNAVAILABLE

    def test_never_refreshed_is_distinguishable_from_a_broken_source(self):
        assert mc.evaluate_freshness(None, now=NOW)["reason"] \
            == mc.REASON_NEVER_REFRESHED
        broken = {"status": "error", "last_refresh_at": NOW,
                  "last_success_at": None, "detail": "unparseable"}
        assert mc.evaluate_freshness(broken, now=NOW)["reason"] \
            == mc.REASON_SOURCE_UNAVAILABLE

    def test_stale_after_the_declared_window(self):
        old = {"status": "ok", "last_refresh_at": NOW,
               "last_success_at": datetime(2026, 8, 25, tzinfo=UTC),
               "detail": None}
        verdict = mc.evaluate_freshness(old, now=NOW)
        assert verdict["status"] == mc.AVAIL_STALE
        assert verdict["reason"] == mc.REASON_STALE_REFRESH

    def test_combine_is_best_of_so_one_dead_publisher_is_not_blindness(self):
        combined = mc.combine_freshness({
            "federal_reserve": {"status": mc.AVAIL_UNAVAILABLE,
                                "reason": "source_unavailable",
                                "age_hours": None},
            "bea": {"status": mc.AVAIL_AVAILABLE, "reason": None,
                    "age_hours": 1.0}})
        assert combined["status"] == mc.AVAIL_AVAILABLE
        assert combined["per_source"]["federal_reserve"]["status"] \
            == mc.AVAIL_UNAVAILABLE


class TestNoStrategyInfluence:
    def test_macro_module_never_imports_the_strategy_or_attention_layer(self):
        # IMPORT lines only. The prose talks about attention and the strategy
        # at length — saying what this module must not touch is the point of
        # the docstring — so the check is on the dependency, not the word.
        imports = [line for line in
                   open("app/macro_calendar.py", encoding="utf-8")
                   if line.startswith(("import ", "from "))
                   or line.lstrip().startswith(("import app.", "from app."))]
        joined = "".join(imports)
        for forbidden in ("scanner_view", "prospective_campaign",
                          "market_context", "external_signals",
                          "prospective_readiness", "wyckoff"):
            assert forbidden not in joined, forbidden

    def test_attention_tiers_are_unchanged_by_this_wave(self):
        # A snapshot, so a macro change that touched attention would fail here
        # rather than in a screenshot.
        assert sv.ATTENTION_TIERS == ("high_attention", "developing",
                                      "low_attention", "no_read", "not_ready")


class TestDisplayAnchor:
    """Proximity must be counted from the READER's session, not a stale scan.

    This is the same asymmetry the external-signal layer already handles, and
    it bites harder here: a calendar is entirely about "when". Counting the
    days to an FOMC meeting from a scan that is five days old does not produce
    a cautious answer — it produces a wrong one, on a screen whose whole
    content is a number of days.
    """

    NOW = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)   # a Sunday

    def test_a_stale_scan_does_not_drag_the_calendar_backwards(self):
        anchor = mc.resolve_anchor_session(date(2026, 8, 25), now=self.NOW,
                                           pinned=False)
        assert anchor == date(2026, 8, 31)           # the next trading session

    def test_a_pinned_session_is_honoured_strictly(self):
        # A historical view must not learn about a meeting announced later.
        assert mc.resolve_anchor_session(date(2026, 8, 25), now=self.NOW,
                                         pinned=True) == date(2026, 8, 25)

    def test_a_current_scan_is_left_alone(self):
        assert mc.resolve_anchor_session(date(2026, 9, 30), now=self.NOW,
                                         pinned=False) == date(2026, 9, 30)

    def test_no_scan_stays_none(self):
        assert mc.resolve_anchor_session(None, now=self.NOW,
                                         pinned=False) is None

    def test_the_block_reports_both_sessions_when_they_differ(self):
        ctx = mc.build_market_calendar_context(
            [event(scheduled=date(2026, 9, 1))],
            as_of_session=date(2026, 8, 31), as_of_date=date(2026, 8, 31),
            scan_session=date(2026, 8, 25), freshness=FRESH)
        assert ctx["as_of_session"] == "2026-08-31"
        assert ctx["scan_session"] == "2026-08-25"
        assert ctx["anchor_is_scan_session"] is False

    def test_the_flag_is_true_when_they_agree(self):
        ctx = mc.build_market_calendar_context(
            [], as_of_session=date(2026, 8, 31), as_of_date=date(2026, 8, 31),
            scan_session=date(2026, 8, 31), freshness=FRESH)
        assert ctx["anchor_is_scan_session"] is True

    def test_the_point_in_time_gate_still_applies_to_the_anchor(self):
        # An event we only observed on 2026-09-02 is invisible to a reader
        # standing in 2026-08-31, however current that anchor is.
        late = event(scheduled=date(2026, 9, 16),
                     observed=datetime(2026, 9, 2, tzinfo=UTC))
        ctx = mc.build_market_calendar_context(
            [late], as_of_session=date(2026, 8, 31), as_of_date=date(2026, 8, 31),
            scan_session=date(2026, 8, 25), freshness=FRESH)
        assert ctx["upcoming"] == []
