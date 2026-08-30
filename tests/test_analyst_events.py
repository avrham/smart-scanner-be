"""Analyst grade change events: normalisation, point in time, and the licence.

Every test here is pure or uses fakes — no FMP, no database. Two properties
carry real weight:

  * `session_date` is ALWAYS strictly after the event date, because the
    provider ships no clock and a same-day anchor would manufacture edge that
    a later study would report as a finding.
  * nothing this module produces can reach the Product API, and the test for
    that reads the router itself rather than trusting a convention.
"""

import asyncio
from datetime import date, datetime, timezone

import pytest

import app.analyst_events as ae
from app.source_licensing import LICENSING_INTERNAL_ONLY

UTC = timezone.utc
NOW = datetime(2026, 8, 30, 17, 0, tzinfo=UTC)
UNIVERSE = {"AAPL", "NVDA", "MSFT"}


def row(symbol="NVDA", day="2026-08-25", company="Jefferies",
        previous="Hold", new="Buy", action="upgrade"):
    return {"symbol": symbol, "date": day, "gradingCompany": company,
            "previousGrade": previous, "newGrade": new, "action": action}


class TestActionVocabulary:
    def test_provider_words_map_onto_ours(self):
        assert ae.normalize_action("upgrade") == ae.ACTION_UPGRADE
        assert ae.normalize_action("Downgrade") == ae.ACTION_DOWNGRADE
        assert ae.normalize_action("maintain") == ae.ACTION_MAINTAIN
        assert ae.normalize_action("hold") == ae.ACTION_MAINTAIN
        assert ae.normalize_action("initialise") == ae.ACTION_INITIALISE
        assert ae.normalize_action("initiated") == ae.ACTION_INITIALISE

    def test_an_unknown_word_is_other_never_guessed(self):
        assert ae.normalize_action("resumed coverage") == ae.ACTION_OTHER
        assert ae.normalize_action(None) == ae.ACTION_OTHER
        assert ae.normalize_action("") == ae.ACTION_OTHER

    def test_every_mapping_lands_in_the_declared_vocabulary(self):
        for word in ("upgrade", "downgrade", "hold", "reiterate", "init",
                     "nonsense", "", None):
            assert ae.normalize_action(word) in ae.ACTIONS

    def test_the_providers_own_word_is_kept_beside_ours(self):
        event = ae.normalize_grade_event(row(action="Reiterated"),
                                         observed_at=NOW)
        assert event["action"] == "Reiterated"
        assert event["action_normalized"] == ae.ACTION_MAINTAIN


class TestPointInTime:
    def test_session_is_strictly_after_the_event_date(self):
        # 2026-08-25 is a Tuesday.
        event = ae.normalize_grade_event(row(day="2026-08-25"),
                                         observed_at=NOW)
        assert event["event_date"] == date(2026, 8, 25)
        assert event["session_date"] == date(2026, 8, 26)

    def test_a_friday_event_anchors_on_the_following_monday(self):
        event = ae.normalize_grade_event(row(day="2026-08-28"),
                                         observed_at=NOW)
        assert event["session_date"] == date(2026, 8, 31)

    def test_a_weekend_event_anchors_on_the_next_trading_day(self):
        event = ae.normalize_grade_event(row(day="2026-08-29"),
                                         observed_at=NOW)
        assert event["session_date"] == date(2026, 8, 31)

    def test_session_is_never_the_event_day_itself(self):
        for day in ("2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27",
                    "2026-08-28", "2026-08-29", "2026-08-30"):
            event = ae.normalize_grade_event(row(day=day), observed_at=NOW)
            assert event["session_date"] > event["event_date"]


class TestNormalisation:
    def test_unusable_rows_are_dropped_not_mangled(self):
        assert ae.normalize_grade_event(row(day="not-a-date"),
                                        observed_at=NOW) is None
        assert ae.normalize_grade_event(row(symbol="1234!"),
                                        observed_at=NOW) is None
        assert ae.normalize_grade_event(row(company=None),
                                        observed_at=NOW) is None
        assert ae.normalize_grade_event("not a dict", observed_at=NOW) is None

    def test_universe_membership_is_recorded_not_enforced(self):
        inside = ae.normalize_grade_event(row(symbol="NVDA"), observed_at=NOW,
                                          universe=UNIVERSE)
        outside = ae.normalize_grade_event(row(symbol="CRM"), observed_at=NOW,
                                           universe=UNIVERSE)
        assert inside["in_scanner_universe"] is True
        # A symbol outside the frozen universe is STORED with the flag false.
        # It is never rejected, and it never becomes a universe member.
        assert outside is not None and outside["in_scanner_universe"] is False

    def test_every_row_is_stamped_internal_research_only(self):
        event = ae.normalize_grade_event(row(), observed_at=NOW)
        assert event["licensing_visibility"] == LICENSING_INTERNAL_ONLY

    def test_history_is_bounded_by_the_since_date(self):
        rows = [row(day="2026-08-25"), row(day="2026-01-02", company="UBS")]
        kept = ae.normalize_grade_history(rows, observed_at=NOW,
                                          since=date(2026, 8, 1))
        assert len(kept) == 1
        assert kept[0]["event_date"] == date(2026, 8, 25)

    def test_identical_rows_are_de_duplicated(self):
        kept = ae.normalize_grade_history([row(), row()], observed_at=NOW,
                                          since=date(2020, 1, 1))
        assert len(kept) == 1

    def test_two_actions_by_one_firm_on_one_day_are_both_kept(self):
        # An initiation and a change can arrive on the same date from the same
        # house; collapsing them would silently lose an event.
        rows = [row(action="initialise", previous=None, new="Buy"),
                row(action="upgrade", previous="Hold", new="Buy")]
        kept = ae.normalize_grade_history(rows, observed_at=NOW,
                                          since=date(2020, 1, 1))
        assert len(kept) == 2

    def test_symbol_hint_covers_a_payload_that_omits_the_symbol(self):
        payload = {"date": "2026-08-25", "gradingCompany": "UBS",
                   "action": "upgrade", "newGrade": "Buy"}
        event = ae.normalize_grade_event(payload, observed_at=NOW,
                                         symbol_hint="AAPL")
        assert event["symbol"] == "AAPL"


class FakeConn:
    def __init__(self):
        self.inserted = []
        self.state = {}

    async def fetchrow(self, sql, *args):
        key = (args[1], args[2], args[4], args[7], args[5], args[6])
        if key in {tuple(k) for k in self.inserted}:
            return None
        self.inserted.append(list(key))
        return {"id": len(self.inserted)}

    async def execute(self, sql, *args):
        self.state[args[0]] = {"status": args[1], "covered": args[4],
                               "written": args[5], "detail": args[6]}
        return "INSERT 0 1"


class FakeClient:
    def __init__(self, per_symbol=None, failures=None):
        self.per_symbol = per_symbol or {}
        self.failures = failures or {}
        self.calls = []

    async def get_list(self, path, params=None):
        symbol = (params or {}).get("symbol")
        self.calls.append((path, symbol))
        if symbol in self.failures:
            raise self.failures[symbol]
        return self.per_symbol.get(symbol, [])

    async def pause(self):
        return None


class TestRefresh:
    def test_symbols_are_requested_as_parameters_not_spelled_into_the_path(self):
        # httpx replaces a URL's query string when params are given, so a
        # symbol in the path would silently vanish and every call would return
        # the market-wide feed.
        conn, client = FakeConn(), FakeClient({"NVDA": [row()]})
        asyncio.run(ae.refresh_analyst_grades(
            conn, client, symbols=["NVDA"], since=date(2020, 1, 1), now=NOW))
        assert client.calls == [(ae.GRADES_PATH, "NVDA")]

    def test_one_failing_symbol_does_not_cost_the_others(self):
        from app.external_discovery import DiscoverySourceUnavailable
        conn = FakeConn()
        client = FakeClient(
            {"NVDA": [row(symbol="NVDA")], "AAPL": [row(symbol="AAPL")]},
            {"MSFT": DiscoverySourceUnavailable("not_entitled")})
        summary = asyncio.run(ae.refresh_analyst_grades(
            conn, client, symbols=["NVDA", "MSFT", "AAPL"],
            since=date(2020, 1, 1), now=NOW))
        assert summary["status"] == ae.STATE_OK
        assert summary["inserted"] == 2
        assert summary["failures"] == ["MSFT:not_entitled"]
        assert summary["symbols"]["MSFT"]["reason"] == "not_entitled"

    def test_no_credential_is_unavailable_not_an_error(self):
        conn = FakeConn()
        summary = asyncio.run(ae.refresh_analyst_grades(
            conn, None, symbols=["NVDA"], now=NOW))
        assert summary["status"] == ae.STATE_UNAVAILABLE
        assert summary["reason"] == "missing_api_key"
        assert conn.state[ae.SOURCE_STATE_FMP_GRADES]["status"] == "unavailable"

    def test_a_total_failure_is_reported_unavailable(self):
        from app.external_discovery import DiscoverySourceUnavailable
        conn = FakeConn()
        client = FakeClient({}, {"NVDA": DiscoverySourceUnavailable("unauthorized")})
        summary = asyncio.run(ae.refresh_analyst_grades(
            conn, client, symbols=["NVDA"], now=NOW))
        assert summary["status"] == ae.STATE_UNAVAILABLE
        assert conn.state[ae.SOURCE_STATE_FMP_GRADES]["status"] == "unavailable"

    def test_a_rerun_writes_nothing_new(self):
        conn = FakeConn()
        client = FakeClient({"NVDA": [row()]})
        first = asyncio.run(ae.refresh_analyst_grades(
            conn, client, symbols=["NVDA"], since=date(2020, 1, 1), now=NOW))
        second = asyncio.run(ae.refresh_analyst_grades(
            conn, client, symbols=["NVDA"], since=date(2020, 1, 1), now=NOW))
        assert first["inserted"] == 1
        assert second["inserted"] == 0 and second["duplicate"] == 1

    def test_freshness_key_is_distinct_from_the_movers_key(self):
        import app.external_discovery as ed
        assert ae.SOURCE_STATE_FMP_GRADES != ed.SOURCE_STATE_FMP_DISCOVERY

    def test_the_module_has_no_opinion_about_which_symbols_matter(self):
        conn = FakeConn()
        summary = asyncio.run(ae.refresh_analyst_grades(
            conn, FakeClient(), symbols=[], now=NOW))
        assert summary["status"] == ae.STATE_UNAVAILABLE
        assert summary["reason"] == "no_symbols"


class TestNoProductReach:
    def test_the_scanner_router_never_names_this_relation(self):
        router = open("app/routers/scanner.py", encoding="utf-8").read()
        assert "analyst_grade_events" not in router
        assert "analyst_events" not in router

    def test_this_module_never_imports_a_router_or_the_scanner_view(self):
        source = open("app/analyst_events.py", encoding="utf-8").read()
        for forbidden in ("app.routers", "scanner_view",
                          "prospective_campaign"):
            assert forbidden not in source, forbidden
