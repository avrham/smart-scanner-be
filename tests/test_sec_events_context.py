"""The pure SEC model: what a session was allowed to know, and when it knew it.

Every test here runs without a database, without the SEC and without the
FastAPI app — `app.sec_events` is a deterministic function of stored rows and
one session date, which is what makes the honesty claims checkable.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

import app.sec_events as se

UTC = timezone.utc


def filing(*, accepted: str = "2026-08-25T13:30:00+00:00",
           form: str = "8-K", items=("2.02", "9.01"),
           accession: str = "0000320193-26-000018",
           filing_date: str = "2026-08-25",
           period: str = "2026-08-24"):
    """One persisted row, shaped exactly like the Product API's SELECT."""
    codes = list(items)
    return {
        "accession_number": accession,
        "cik": "0000320193",
        "form": form,
        "accepted_at": datetime.fromisoformat(accepted),
        "filing_date": date.fromisoformat(filing_date),
        "period_of_report": date.fromisoformat(period) if period else None,
        "item_codes": codes,
        "event_types": se.classify_event_types(codes),
        "taxonomy_version": se.SEC_TAXONOMY_VERSION,
        "is_primary_event": se.is_primary_event(codes),
        "amends_accession_number": None,
        "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/x/aapl.htm",
    }


FRESH = {"status": se.STATUS_AVAILABLE, "reason": None,
         "last_refresh_at": "2026-08-25T21:00:00+00:00",
         "last_success_at": "2026-08-25T21:00:00+00:00",
         "age_hours": 1.0, "detail": None}


# --------------------------------------------------------------------------- #
# item codes and the taxonomy
# --------------------------------------------------------------------------- #

class TestItemCodes:
    def test_edgars_comma_separated_field_becomes_ordered_codes(self):
        assert se.parse_item_codes("2.02,9.01") == ["2.02", "9.01"]
        assert se.parse_item_codes(" 5.02 , 9.01 ") == ["5.02", "9.01"]

    def test_a_repeated_code_is_carried_once(self):
        assert se.parse_item_codes("9.01,9.01") == ["9.01"]

    def test_a_filing_with_no_items_yields_no_codes(self):
        assert se.parse_item_codes(None) == []
        assert se.parse_item_codes("") == []

    def test_junk_is_dropped_rather_than_stored_as_a_code(self):
        assert se.parse_item_codes("2.02,not-an-item,9.01") == ["2.02", "9.01"]

    def test_codes_keep_their_two_digit_shape(self):
        assert se.normalize_item_code("2.2") == "2.02"
        assert se.normalize_item_code("02.02") == "2.02"


class TestTaxonomy:
    @pytest.mark.parametrize("code,expected", [
        ("1.01", se.EVENT_MATERIAL_AGREEMENT),
        ("1.03", "bankruptcy_or_receivership"),
        ("1.05", "cybersecurity_incident"),
        ("2.01", "acquisition_or_disposition"),
        ("2.02", se.EVENT_RESULTS),
        ("2.03", "financial_obligation"),
        ("2.06", "restructuring_or_impairment"),
        ("3.01", "delisting_or_listing"),
        ("3.02", "equity_or_capital"),
        ("4.01", "accountant_change"),
        ("5.02", se.EVENT_MANAGEMENT_CHANGE),
        ("5.03", "charter_or_governance"),
        ("5.07", "shareholder_matters"),
        ("7.01", se.EVENT_REGULATION_FD),
        ("8.01", se.EVENT_OTHER_MATERIAL),
        ("9.01", se.EVENT_EXHIBITS),
    ])
    def test_each_item_maps_to_its_own_sec_section_semantics(self, code, expected):
        assert se.classify_event_types([code]) == [expected]

    def test_an_unmapped_code_becomes_unknown_rather_than_disappearing(self):
        # Reporting "no event type" for a filing that HAS an item would be a
        # stronger claim than the data supports.
        assert se.classify_event_types(["1.99"]) == [se.EVENT_UNKNOWN]

    def test_every_mapped_family_is_in_the_declared_vocabulary(self):
        for family in se.ITEM_EVENT_TYPES.values():
            assert family in se.EVENT_TYPES

    def test_the_taxonomy_is_versioned_so_a_revision_is_visible(self):
        assert se.SEC_TAXONOMY_VERSION == "sec_8k_items.v1"


# --------------------------------------------------------------------------- #
# primary vs supporting — structural, never importance
# --------------------------------------------------------------------------- #

class TestPrimaryVsSupporting:
    def test_exhibits_alone_is_not_a_material_event(self):
        assert se.is_primary_event(["9.01"]) is False

    def test_regulation_fd_alone_is_not_a_material_event(self):
        # 7.01 describes the CHANNEL something was furnished through, not what
        # happened.
        assert se.is_primary_event(["7.01"]) is False
        assert se.is_primary_event(["7.01", "9.01"]) is False

    def test_exhibits_attached_to_a_real_event_do_not_demote_it(self):
        assert se.is_primary_event(["2.02", "9.01"]) is True

    def test_a_filing_with_no_items_is_treated_as_primary(self):
        # We know a formal current report was filed. Calling it "supporting"
        # would claim more than we know.
        assert se.is_primary_event([]) is True

    def test_the_row_label_ignores_supporting_items(self):
        assert se.primary_event_types(["2.02", "9.01"]) == [se.EVENT_RESULTS]
        assert se.primary_event_types(["7.01", "9.01"]) == []

    def test_primary_is_not_a_ranking_between_events(self):
        # Results and a management change are both primary. Nothing orders one
        # above the other.
        assert se.is_primary_event(["2.02"]) == se.is_primary_event(["5.02"])


class TestMultiItemFilings:
    def test_every_family_present_in_the_filing_is_reported(self):
        codes = ["2.02", "5.02", "9.01"]
        assert se.classify_event_types(codes) == [
            se.EVENT_RESULTS, se.EVENT_MANAGEMENT_CHANGE, se.EVENT_EXHIBITS]

    def test_a_real_multi_item_filing_keeps_all_its_codes(self):
        # WMT 2025-11-20 really did file 2.02, 3.01, 7.01 and 9.01 together.
        item = se.build_sec_item({**filing(items=("2.02", "3.01", "7.01", "9.01")),
                                  "_session": date(2026, 8, 25), "_sessions_ago": 0,
                                  "_proximity": se.PROXIMITY_TODAY})
        assert item["item_codes"] == ["2.02", "3.01", "7.01", "9.01"]
        assert item["primary_event_types"] == [
            se.EVENT_RESULTS, "delisting_or_listing"]

    def test_a_multi_item_filing_is_not_flattened_to_one_category(self):
        ctx = se.build_sec_context([filing(items=("1.01", "5.02", "9.01"))],
                                   as_of_session=date(2026, 8, 25), freshness=FRESH)
        assert len(ctx["items"][0]["event_types"]) == 3


# --------------------------------------------------------------------------- #
# amendments
# --------------------------------------------------------------------------- #

class TestAmendments:
    def test_an_amendment_is_recognised_by_its_form(self):
        assert se.is_amendment("8-K/A") is True
        assert se.is_amendment("8-K") is False

    def test_an_amendment_never_replaces_the_filing_it_amends(self):
        # Both really exist: WMT filed an 8-K at 14:01:15Z and an 8-K/A two
        # minutes later on 2026-01-16. Two disclosures, two rows.
        picked = se.select_visible_filings([
            filing(accession="0000104169-26-000023", form="8-K",
                   items=("5.02", "9.01"), accepted="2026-01-16T14:01:15+00:00",
                   filing_date="2026-01-16", period="2026-01-14"),
            filing(accession="0000104169-26-000024", form="8-K/A",
                   items=("5.02",), accepted="2026-01-16T14:03:05+00:00",
                   filing_date="2026-01-16", period="2026-01-14"),
        ], as_of_session=date(2026, 1, 16))
        assert len(picked) == 2
        assert [p["form"] for p in picked] == ["8-K/A", "8-K"]   # newest first

    def test_an_amendment_carries_the_amendment_flag_to_the_product(self):
        item = se.build_sec_item({**filing(form="8-K/A"),
                                  "_session": date(2026, 8, 25), "_sessions_ago": 0,
                                  "_proximity": se.PROXIMITY_TODAY})
        assert item["is_amendment"] is True
        assert item["form"] == "8-K/A"


# --------------------------------------------------------------------------- #
# point-in-time — the whole point of the layer
# --------------------------------------------------------------------------- #

class TestPointInTime:
    def test_the_gate_is_acceptance_not_the_event_date(self):
        # The mission's own example: event 27 July, accepted 30 July 08:10 ET,
        # scan 29 July. The event date would show it; acceptance must not.
        row = filing(accepted="2026-07-30T12:10:00+00:00",
                     filing_date="2026-07-30", period="2026-07-27")
        assert se.select_visible_filings([row], as_of_session=date(2026, 7, 29)) == []

    def test_the_same_filing_is_visible_once_the_session_reaches_it(self):
        row = filing(accepted="2026-07-30T12:10:00+00:00",
                     filing_date="2026-07-30", period="2026-07-27")
        assert se.select_visible_filings([row], as_of_session=date(2026, 7, 30))

    def test_period_of_report_is_reported_but_never_gates(self):
        row = filing(accepted="2026-07-30T12:10:00+00:00", period="2026-07-27")
        item = se.build_sec_item({**row, "_session": date(2026, 7, 30),
                                  "_sessions_ago": 0,
                                  "_proximity": se.PROXIMITY_TODAY})
        assert item["period_of_report"] == "2026-07-27"
        assert item["accepted_at"].startswith("2026-07-30")

    def test_the_boundary_is_the_close_not_midnight(self):
        session = date(2026, 8, 25)
        assert se.is_visible_to_session(
            datetime(2026, 8, 25, 19, 59, tzinfo=UTC), session)
        assert not se.is_visible_to_session(
            datetime(2026, 8, 25, 20, 1, tzinfo=UTC), session)

    def test_a_filing_accepted_after_the_close_belongs_to_the_next_session(self):
        # Apple really did file at 2026-07-30T20:30:28Z, half an hour after the
        # bell. That is Friday's disclosure.
        assert se.effective_session(
            datetime(2026, 7, 30, 20, 30, 28, tzinfo=UTC)) == date(2026, 7, 31)

    def test_a_friday_evening_filing_lands_on_monday(self):
        assert se.effective_session(
            datetime(2026, 8, 21, 23, 0, tzinfo=UTC)) == date(2026, 8, 24)

    def test_ingestion_time_is_irrelevant_to_visibility(self):
        # Back-filling EDGAR today does not make an old filing newly knowable,
        # and does not hide it either: acceptance decides, and nothing in this
        # path reads observed_at.
        row = filing(accepted="2026-08-20T13:00:00+00:00")
        row["observed_at"] = datetime(2026, 8, 28, tzinfo=UTC)
        assert se.select_visible_filings([row], as_of_session=date(2026, 8, 25))

    def test_the_market_clock_is_the_same_one_the_news_layer_uses(self):
        import app.news as nw
        assert se.session_close_utc is nw.session_close_utc


# --------------------------------------------------------------------------- #
# relevance windows
# --------------------------------------------------------------------------- #

class TestProximityWindows:
    @pytest.mark.parametrize("sessions_ago,expected", [
        (0, se.PROXIMITY_TODAY),
        (1, se.PROXIMITY_RECENT), (5, se.PROXIMITY_RECENT),
        (6, se.PROXIMITY_OLDER_CONTEXT), (20, se.PROXIMITY_OLDER_CONTEXT),
        (21, se.PROXIMITY_OUT_OF_WINDOW), (400, se.PROXIMITY_OUT_OF_WINDOW),
        (None, se.PROXIMITY_OUT_OF_WINDOW),
    ])
    def test_boundaries_are_exactly_where_they_are_documented(self, sessions_ago, expected):
        assert se.classify_proximity(sessions_ago) == expected

    def test_sec_windows_are_wider_than_the_news_windows(self):
        # A headline is stale in days; a formally disclosed corporate event is
        # still the relevant fact a fortnight later.
        import app.news as nw
        assert se.RECENT_MAX_SESSIONS > nw.RECENT_MAX_SESSIONS
        assert se.OLDER_CONTEXT_MAX_SESSIONS > nw.OLDER_CONTEXT_MAX_SESSIONS

    def test_a_badge_cannot_sit_on_a_symbol_indefinitely(self):
        old = filing(accepted="2026-05-01T13:00:00+00:00", filing_date="2026-05-01")
        assert se.select_visible_filings([old], as_of_session=date(2026, 8, 25)) == []

    def test_only_today_and_recent_may_reach_the_scanner_list(self):
        assert se.NOTABLE_PROXIMITIES == (se.PROXIMITY_TODAY, se.PROXIMITY_RECENT)
        assert not se.is_notable(se.PROXIMITY_OLDER_CONTEXT, primary=True)

    def test_a_weekend_gap_is_counted_in_sessions_not_days(self):
        picked = se.select_visible_filings(
            [filing(accepted="2026-08-21T23:00:00+00:00", filing_date="2026-08-21")],
            as_of_session=date(2026, 8, 25))
        assert picked[0]["_sessions_ago"] == 1


class TestNotability:
    def test_a_supporting_only_filing_never_speaks_on_a_row(self):
        assert se.is_notable(se.PROXIMITY_TODAY, primary=False) is False

    def test_a_primary_filing_close_to_the_session_does(self):
        assert se.is_notable(se.PROXIMITY_TODAY, primary=True) is True
        assert se.is_notable(se.PROXIMITY_RECENT, primary=True) is True


# --------------------------------------------------------------------------- #
# freshness
# --------------------------------------------------------------------------- #

class TestFreshness:
    NOW = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)

    def test_never_refreshed_is_not_the_same_as_nothing_filed(self):
        verdict = se.evaluate_freshness(None, now=self.NOW)
        assert verdict["status"] == se.STATUS_UNAVAILABLE
        assert verdict["reason"] == se.REASON_NEVER_REFRESHED

    def test_an_unreachable_source_is_reported_as_unavailable(self):
        verdict = se.evaluate_freshness(
            {"status": "unavailable", "last_success_at": None,
             "last_refresh_at": self.NOW, "detail": "sec_rate_limited"},
            now=self.NOW)
        assert verdict["status"] == se.STATUS_UNAVAILABLE
        assert verdict["detail"] == "sec_rate_limited"

    def test_a_stalled_ingestion_makes_the_dimension_stale(self):
        verdict = se.evaluate_freshness(
            {"status": "ok", "last_success_at": self.NOW - timedelta(hours=40),
             "last_refresh_at": self.NOW, "detail": None}, now=self.NOW)
        assert verdict["status"] == se.STATUS_STALE

    def test_one_daily_run_is_not_stale_but_a_missed_one_is(self):
        # 30h = one daily cadence plus slack.
        fresh = se.evaluate_freshness(
            {"status": "ok", "last_success_at": self.NOW - timedelta(hours=25),
             "last_refresh_at": self.NOW, "detail": None}, now=self.NOW)
        assert fresh["status"] == se.STATUS_AVAILABLE

    def test_sec_tolerates_more_age_than_news_does(self):
        import app.news as nw
        assert se.FRESHNESS_MAX_AGE_HOURS > nw.FRESHNESS_MAX_AGE_HOURS


# --------------------------------------------------------------------------- #
# the product objects
# --------------------------------------------------------------------------- #

class TestSecContext:
    def test_an_unavailable_source_yields_no_items_and_says_why(self):
        ctx = se.build_sec_context(
            [filing()], as_of_session=date(2026, 8, 25),
            freshness={"status": se.STATUS_UNAVAILABLE,
                       "reason": se.REASON_SOURCE_UNAVAILABLE,
                       "last_refresh_at": None, "last_success_at": None,
                       "age_hours": None, "detail": None})
        assert ctx["items"] == []
        assert ctx["notable_count"] == 0
        assert ctx["reason"] == se.REASON_SOURCE_UNAVAILABLE

    def test_a_supporting_only_filing_is_carried_but_never_notable(self):
        ctx = se.build_sec_context([filing(items=("7.01", "9.01"))],
                                   as_of_session=date(2026, 8, 25), freshness=FRESH)
        assert ctx["in_window_count"] == 1
        assert ctx["primary_event_count"] == 0
        assert ctx["notable_count"] == 0
        assert ctx["top_event_type"] is None

    def test_the_top_event_type_describes_the_newest_notable_filing(self):
        ctx = se.build_sec_context([
            filing(accession="0000320193-26-000018", items=("5.02",),
                   accepted="2026-08-25T14:00:00+00:00"),
            filing(accession="0000320193-26-000017", items=("2.02", "9.01"),
                   accepted="2026-08-24T14:00:00+00:00"),
        ], as_of_session=date(2026, 8, 25), freshness=FRESH)
        assert ctx["top_event_type"] == se.EVENT_MANAGEMENT_CHANGE
        assert ctx["notable_count"] == 2

    def test_an_item_exposes_structured_evidence_and_a_link_only(self):
        ctx = se.build_sec_context([filing()], as_of_session=date(2026, 8, 25),
                                   freshness=FRESH)
        assert set(ctx["items"][0]) == {
            "accession_number", "form", "is_amendment", "amends_accession_number",
            "accepted_at", "session", "sessions_ago", "proximity", "filing_date",
            "period_of_report", "item_codes", "event_types",
            "primary_event_types", "is_primary_event", "taxonomy_version",
            "source_reference", "notable"}

    def test_the_source_link_points_at_the_filing_itself(self):
        ctx = se.build_sec_context([filing()], as_of_session=date(2026, 8, 25),
                                   freshness=FRESH)
        assert ctx["items"][0]["source_reference"].startswith(
            "https://www.sec.gov/Archives/edgar/data/")

    def test_an_available_source_with_no_filings_is_not_an_error(self):
        # The common case: most companies file nothing in most weeks.
        ctx = se.build_sec_context([], as_of_session=date(2026, 8, 25),
                                   freshness=FRESH)
        assert ctx["status"] == se.STATUS_AVAILABLE
        assert ctx["in_window_count"] == 0
        assert ctx["latest_accepted_at"] is None

    def test_the_payload_is_bounded(self):
        many = [filing(accession=f"000032019{i}-26-00001{i}",
                       accepted=f"2026-08-25T{10 + i:02d}:00:00+00:00")
                for i in range(9)]
        ctx = se.build_sec_context(many, as_of_session=date(2026, 8, 25),
                                   freshness=FRESH)
        assert len(ctx["items"]) == se.MAX_DETAIL_ITEMS


class TestRowSec:
    def test_a_quiet_row_carries_nothing_to_print(self):
        row = se.build_row_sec(se.build_sec_context(
            [], as_of_session=date(2026, 8, 25), freshness=FRESH))
        assert row["notable_count"] == 0
        assert row["latest_form"] is None
        assert row["top_event_type"] is None

    def test_a_loud_row_carries_counts_and_codes_not_filings(self):
        row = se.build_row_sec(se.build_sec_context(
            [filing(items=("5.02", "9.01"))],
            as_of_session=date(2026, 8, 25), freshness=FRESH))
        assert row["notable_count"] == 1
        assert row["latest_form"] == "8-K"
        assert row["latest_item_codes"] == ["5.02", "9.01"]
        assert "items" not in row

    def test_an_empty_context_is_a_complete_unavailable_block(self):
        ctx = se.empty_sec_context()
        assert ctx["status"] == se.STATUS_UNAVAILABLE
        assert ctx["items"] == []
        assert se.build_row_sec(ctx)["notable_count"] == 0


class TestTheVocabularyCannotBecomeARanking:
    def _sample(self):
        return se.build_sec_context([filing(items=("5.02", "9.01"))],
                                    as_of_session=date(2026, 8, 25),
                                    freshness=FRESH)

    def test_no_output_is_a_score_a_rating_or_a_direction(self):
        blob = repr(self._sample()).lower()
        for banned in ("score", "rating", "weight", "rank", "sentiment",
                       "bullish", "bearish", "positive", "negative"):
            assert banned not in blob, f"{banned} leaked into the SEC contract"

    def test_the_only_numbers_are_distances_and_counts(self):
        ctx = self._sample()
        numeric = {k for k, v in ctx.items()
                   if isinstance(v, (int, float)) and not isinstance(v, bool)}
        numeric |= {k for k, v in ctx["items"][0].items()
                    if isinstance(v, (int, float)) and not isinstance(v, bool)}
        assert numeric <= {"sessions_ago", "in_window_count", "notable_count",
                           "primary_event_count", "window_sessions", "age_hours"}

    def test_the_vocabulary_contains_no_action_words(self):
        forbidden = {"buy", "sell", "long", "short", "enter", "exit", "target",
                     "bullish", "bearish", "signal", "recommend", "good", "bad"}
        for token in se.EVENT_TYPES + se.PROXIMITIES + se.SEC_STATUSES:
            assert not (set(token.split("_")) & forbidden), \
                f"action word in the SEC vocabulary: {token}"
