"""The two macro parsers, against markup shaped exactly like the live pages.

Both parsers are pure, so every test here runs with no network and no database.
The cases chosen are the ones that produce a WRONG DATE rather than an error —
a meeting spanning a year boundary, a regional release whose title starts with
"GDP", a table that rolls into the next year — because a wrong date in a
calendar is invisible until somebody trades on it.
"""

import asyncio
from datetime import date, datetime, time, timezone

import pytest

import app.macro_calendar as mc
import app.macro_ingest as mi
from tests.support.macro_markup import (BEA_LAYOUT_CHANGED_HTML,
                                        BEA_SCHEDULE_HTML,
                                        FOMC_CALENDAR_HTML,
                                        FOMC_LAYOUT_CHANGED_HTML)

UTC = timezone.utc
NOW = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


class TestFomcParsing:
    def test_decision_date_is_the_last_day_of_the_meeting(self):
        events = mi.parse_fomc_calendar(FOMC_CALENDAR_HTML, observed_at=NOW)
        january = next(e for e in events
                       if e["scheduled_start_date"] == date(2026, 1, 27))
        assert january["scheduled_date"] == date(2026, 1, 28)
        assert january["event_type"] == mc.EVENT_FOMC_RATE_DECISION

    def test_month_and_year_boundary_meeting(self):
        # 'December/January 31-1' on the 2026 panel is 2026-12-31 -> 2027-01-01.
        events = mi.parse_fomc_calendar(FOMC_CALENDAR_HTML, observed_at=NOW)
        crossing = next(e for e in events
                        if e["scheduled_start_date"] == date(2026, 12, 31))
        assert crossing["scheduled_date"] == date(2027, 1, 1)

    def test_projection_asterisk_is_read(self):
        events = mi.parse_fomc_calendar(FOMC_CALENDAR_HTML, observed_at=NOW)
        march = next(e for e in events
                     if e["scheduled_date"] == date(2026, 3, 18))
        assert march["has_projections"] is True
        january = next(e for e in events
                       if e["scheduled_date"] == date(2026, 1, 28))
        assert january["has_projections"] is None

    def test_press_conference_absent_means_unknown_not_false(self):
        events = mi.parse_fomc_calendar(FOMC_CALENDAR_HTML, observed_at=NOW)
        september = next(e for e in events
                         if e["scheduled_date"] == date(2026, 9, 16))
        # The Fed has not posted the page yet. NULL, never False: "not
        # published" and "no press conference" are different claims.
        assert september["has_press_conference"] is None
        january = next(e for e in events
                       if e["scheduled_date"] == date(2026, 1, 28))
        assert january["has_press_conference"] is True

    def test_page_footer_does_not_leak_into_the_last_meeting(self):
        # The footer navigation contains a link matching the press-conference
        # pattern. If the meeting block ran to the end of the document, the
        # last meeting would falsely claim one.
        events = mi.parse_fomc_calendar(FOMC_CALENDAR_HTML, observed_at=NOW)
        last = max(events, key=lambda e: e["scheduled_date"])
        assert last["scheduled_date"] == date(2027, 1, 1)
        assert last["has_press_conference"] is None

    def test_no_clock_is_invented(self):
        events = mi.parse_fomc_calendar(FOMC_CALENDAR_HTML, observed_at=NOW)
        assert all(e["scheduled_time_local"] is None for e in events)

    def test_layout_change_raises_rather_than_returning_empty(self):
        with pytest.raises(mi.MacroSourceUnavailable) as excinfo:
            mi.parse_fomc_calendar(FOMC_LAYOUT_CHANGED_HTML, observed_at=NOW)
        assert excinfo.value.reason == "unparseable"


class TestBeaParsing:
    def test_only_gdp_and_pce_are_kept(self):
        events = mi.parse_bea_schedule(BEA_SCHEDULE_HTML, observed_at=NOW)
        assert {e["event_type"] for e in events} == {mc.EVENT_GDP,
                                                    mc.EVENT_PCE}
        assert not any("International Trade" in e["title"] for e in events)

    def test_regional_gdp_is_not_a_national_release(self):
        events = mi.parse_bea_schedule(BEA_SCHEDULE_HTML, observed_at=NOW)
        assert not any("by County" in e["title"] for e in events)

    def test_year_rolls_forward_when_the_month_goes_backwards(self):
        events = mi.parse_bea_schedule(BEA_SCHEDULE_HTML, observed_at=NOW)
        january = next(e for e in events if "4th Quarter" in e["title"])
        assert january["scheduled_date"] == date(2027, 1, 29)

    def test_release_clock_is_parsed(self):
        events = mi.parse_bea_schedule(BEA_SCHEDULE_HTML, observed_at=NOW)
        assert events[0]["scheduled_time_local"] == time(8, 30)
        assert events[0]["scheduled_timezone"] == "America/New_York"

    def test_classification_is_pure_and_conservative(self):
        assert mi.classify_bea_release("GDP (Advance Estimate), Q3") == mc.EVENT_GDP
        assert mi.classify_bea_release("Gross Domestic Product, 2026") == mc.EVENT_GDP
        assert mi.classify_bea_release("Personal Income and Outlays, July") == mc.EVENT_PCE
        assert mi.classify_bea_release("GDP by State, 2025") is None
        assert mi.classify_bea_release("Corporate Profits") is None
        assert mi.classify_bea_release("") is None

    def test_layout_change_raises(self):
        with pytest.raises(mi.MacroSourceUnavailable):
            mi.parse_bea_schedule(BEA_LAYOUT_CHANGED_HTML, observed_at=NOW)


class FakeConn:
    def __init__(self):
        self.events = []
        self.state = {}
        self.withdrawn = 0

    async def fetchrow(self, sql, *args):
        self.events.append(args)
        return {"inserted": True}

    async def execute(self, sql, *args):
        if "source_listing = 'withdrawn'" in sql:
            self.withdrawn += 1
            return "UPDATE 0"
        self.state[args[0]] = {"status": args[1], "detail": args[5]}
        return "INSERT 0 1"


class FakeClient:
    def __init__(self, pages=None, failures=None):
        self.pages = pages or {}
        self.failures = failures or {}

    async def get_text(self, url):
        if url in self.failures:
            raise self.failures[url]
        return self.pages.get(url, "")

    async def pause(self):
        return None


class TestRefreshIsolation:
    def test_both_sources_write_their_own_freshness_row(self):
        conn = FakeConn()
        client = FakeClient({mi.FOMC_CALENDAR_URL: FOMC_CALENDAR_HTML,
                             mi.BEA_SCHEDULE_URL: BEA_SCHEDULE_HTML})
        summary = asyncio.run(
            mi.refresh_macro_calendar(conn, client, now=NOW))
        assert summary["status"] == mi.STATE_OK
        assert conn.state[mc.source_state_key("federal_reserve")]["status"] == "ok"
        assert conn.state[mc.source_state_key("bea")]["status"] == "ok"

    def test_one_publisher_failing_does_not_cost_the_other(self):
        conn = FakeConn()
        client = FakeClient(
            {mi.BEA_SCHEDULE_URL: BEA_SCHEDULE_HTML},
            {mi.FOMC_CALENDAR_URL: mi.MacroSourceUnavailable("forbidden")})
        summary = asyncio.run(
            mi.refresh_macro_calendar(conn, client, now=NOW))
        # Partial success IS success, with the failure named.
        assert summary["status"] == mi.STATE_OK
        assert summary["sources"]["federal_reserve"]["reason"] == "forbidden"
        assert summary["sources"]["bea"]["status"] == mi.STATE_OK
        assert conn.state[mc.source_state_key("bea")]["status"] == "ok"
        assert conn.state[mc.source_state_key("federal_reserve")]["status"] \
            == "unavailable"

    def test_no_client_is_unavailable_not_an_error(self):
        conn = FakeConn()
        summary = asyncio.run(mi.refresh_macro_calendar(conn, None, now=NOW))
        assert summary["status"] == mi.STATE_UNAVAILABLE
        for source in mc.MACRO_SOURCES:
            assert conn.state[mc.source_state_key(source)]["status"] \
                == "unavailable"

    def test_an_unexpected_exception_is_absorbed_per_source(self):
        conn = FakeConn()
        client = FakeClient(
            {mi.BEA_SCHEDULE_URL: BEA_SCHEDULE_HTML},
            {mi.FOMC_CALENDAR_URL: ValueError("boom")})
        summary = asyncio.run(
            mi.refresh_macro_calendar(conn, client, now=NOW))
        assert summary["sources"]["federal_reserve"]["status"] == mi.STATE_ERROR
        assert summary["sources"]["bea"]["status"] == mi.STATE_OK

    def test_withdrawal_sweep_marks_rather_than_deletes(self):
        conn = FakeConn()
        client = FakeClient({mi.FOMC_CALENDAR_URL: FOMC_CALENDAR_HTML,
                             mi.BEA_SCHEDULE_URL: BEA_SCHEDULE_HTML})
        asyncio.run(mi.refresh_macro_calendar(conn, client, now=NOW))
        assert conn.withdrawn == 2          # one sweep per source, no DELETE
