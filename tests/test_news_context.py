"""The pure news model: what a session may see, and what it must stay quiet about.

Every test here runs without a database, without a provider and without the
FastAPI app — `app.news` is a deterministic function of stored rows and one
session date, and that is exactly what makes the honesty claims checkable.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

import app.news as nw

UTC = timezone.utc


def article(*, published: str, title: str = "Apple posts record quarter",
            publisher: str = "Reuters", url: str = "https://reuters.com/a/1",
            scope: str = nw.SCOPE_COMPANY_SPECIFIC,
            relevance: str = nw.RELEVANCE_PRIMARY,
            category: str = nw.CATEGORY_GENERAL,
            breadth: int = 1):
    """One persisted row, shaped exactly like the Product API's SELECT."""
    return {
        "published_at": datetime.fromisoformat(published),
        "title": title,
        "title_normalized": nw.normalize_title(title),
        "publisher": publisher,
        "article_url": url,
        "category": category,
        "category_source": nw.CATEGORY_SOURCE_DEFAULT,
        "scope": scope,
        "relevance": relevance,
        "ticker_breadth": breadth,
    }


FRESH = {"status": nw.STATUS_AVAILABLE, "reason": None,
         "last_refresh_at": "2026-08-25T21:00:00+00:00",
         "last_success_at": "2026-08-25T21:00:00+00:00",
         "age_hours": 1.0, "detail": None}


# --------------------------------------------------------------------------- #
# normalisation — the auditable half of the dedupe rule
# --------------------------------------------------------------------------- #

class TestNormalisation:
    def test_titles_normalise_past_typography_but_not_past_meaning(self):
        assert (nw.normalize_title("Apple's Q3: Record Revenue!")
                == nw.normalize_title("APPLE S  Q3   RECORD REVENUE"))
        assert nw.normalize_title("Apple beats") != nw.normalize_title("Apple misses")

    def test_an_empty_title_normalises_to_empty_rather_than_crashing(self):
        assert nw.normalize_title(None) == ""
        assert nw.normalize_title("   ") == ""

    def test_tracking_parameters_do_not_make_the_same_article_look_new(self):
        # This is the real provider shape: the same story arrives with a fresh
        # `?source=` on every refresh.
        a = nw.canonical_url("https://www.fool.com/investing/2026/08/27/x/?source=iedfolrf1")
        b = nw.canonical_url("https://fool.com/investing/2026/08/27/x?source=OTHER#frag")
        assert a == b == "https://fool.com/investing/2026/08/27/x"

    def test_two_different_articles_keep_different_canonical_urls(self):
        assert (nw.canonical_url("https://reuters.com/a/1")
                != nw.canonical_url("https://reuters.com/a/2"))


# --------------------------------------------------------------------------- #
# scope — how many companies a story is actually about
# --------------------------------------------------------------------------- #

class TestScope:
    @pytest.mark.parametrize("breadth,expected", [
        (0, nw.SCOPE_COMPANY_SPECIFIC), (1, nw.SCOPE_COMPANY_SPECIFIC),
        (3, nw.SCOPE_COMPANY_SPECIFIC), (4, nw.SCOPE_MULTI_COMPANY),
        (10, nw.SCOPE_MULTI_COMPANY), (11, nw.SCOPE_MARKET_WIDE),
        (25, nw.SCOPE_MARKET_WIDE),
    ])
    def test_boundaries_are_exactly_where_they_are_documented(self, breadth, expected):
        assert nw.classify_scope(breadth) == expected

    def test_a_twenty_five_ticker_roundup_is_never_a_company_catalyst(self):
        # The provider really does attach 25 tickers to a Berkshire portfolio
        # piece, AAPL among them. It is not Apple news.
        assert nw.classify_scope(25) == nw.SCOPE_MARKET_WIDE
        assert not nw.is_notable(nw.PROXIMITY_TODAY, scope=nw.SCOPE_MARKET_WIDE,
                                 relevance=nw.RELEVANCE_PRIMARY)


# --------------------------------------------------------------------------- #
# category — small, high precision, and honest about not knowing
# --------------------------------------------------------------------------- #

class TestCategory:
    @pytest.mark.parametrize("title,expected", [
        ("Apple reports Q3 earnings, beats estimates", nw.CATEGORY_EARNINGS_RESULTS),
        ("Nvidia raises outlook for the year", nw.CATEGORY_GUIDANCE),
        ("Morgan Stanley upgrades Tesla, lifts price target", nw.CATEGORY_ANALYST_ACTION),
        ("Broadcom acquires a networking startup", nw.CATEGORY_MERGER_ACQUISITION),
        ("FTC opens antitrust investigation into Amazon", nw.CATEGORY_REGULATORY_LEGAL),
        ("Intel CEO steps down after four years", nw.CATEGORY_MANAGEMENT),
        ("Microsoft launches a new Copilot tier", nw.CATEGORY_PRODUCT_ANNOUNCEMENT),
        ("Chevron raises dividend and expands buyback", nw.CATEGORY_FINANCING_CAPITAL),
    ])
    def test_unambiguous_headlines_get_their_category(self, title, expected):
        category, source = nw.classify_category(title)
        assert category == expected
        assert source == nw.CATEGORY_SOURCE_DERIVED_TITLE

    def test_an_ordinary_headline_admits_we_do_not_know(self):
        category, source = nw.classify_category(
            "If You'd Invested $1,000 in Home Depot 15 Years Ago")
        assert category == nw.CATEGORY_GENERAL
        assert source == nw.CATEGORY_SOURCE_DEFAULT

    def test_a_missing_title_never_invents_a_category(self):
        assert nw.classify_category(None) == (nw.CATEGORY_GENERAL,
                                              nw.CATEGORY_SOURCE_DEFAULT)

    def test_every_produced_category_is_in_the_declared_vocabulary(self):
        for title in ("Apple reports Q3 earnings", "random words here", ""):
            category, source = nw.classify_category(title)
            assert category in nw.CATEGORIES
            assert source in nw.CATEGORY_SOURCES


# --------------------------------------------------------------------------- #
# relevance — does the headline actually name this company
# --------------------------------------------------------------------------- #

class TestRelevance:
    def test_the_ticker_in_the_headline_counts(self):
        assert nw.title_names_company("AAPL slips after the close", "AAPL")

    def test_the_company_name_in_the_headline_counts(self):
        assert nw.title_names_company("Home Depot lifts its dividend", "HD")
        assert nw.title_names_company("Alphabet wins appeal", "GOOGL")

    def test_a_ticker_hidden_inside_a_word_does_not_count(self):
        # The reason ticker matching is word-bounded: "GE" is a substring of a
        # great many ordinary words.
        assert not nw.title_names_company("Investors get nervous", "GE")
        assert not nw.title_names_company("A large hedge fund", "HD")

    def test_a_ticker_merely_attached_by_the_provider_is_only_mentioned(self):
        title = "Greg Abel Has Kept 60% of Berkshire's Portfolio in 5 Companies"
        assert nw.classify_relevance(title, "AAPL") == nw.RELEVANCE_MENTIONED
        assert nw.classify_relevance("Apple unveils new Macs", "AAPL") == \
            nw.RELEVANCE_PRIMARY

    def test_a_symbol_outside_the_frozen_universe_still_gets_the_ticker_test(self):
        assert nw.title_names_company("ZZZZ announces a merger", "ZZZZ")
        assert not nw.title_names_company("Nothing to see", "ZZZZ")


# --------------------------------------------------------------------------- #
# the market clock — where hindsight would enter if anywhere
# --------------------------------------------------------------------------- #

class TestSessionClock:
    def test_the_close_follows_daylight_saving_rather_than_a_fixed_offset(self):
        assert nw.session_close_utc(date(2026, 8, 25)) == \
            datetime(2026, 8, 25, 20, 0, tzinfo=UTC)     # EDT
        assert nw.session_close_utc(date(2026, 1, 15)) == \
            datetime(2026, 1, 15, 21, 0, tzinfo=UTC)     # EST

    def test_an_article_before_the_close_belongs_to_that_session(self):
        assert nw.effective_session(
            datetime(2026, 8, 25, 13, 30, tzinfo=UTC)) == date(2026, 8, 25)

    def test_an_article_after_the_close_rolls_to_the_next_session(self):
        assert nw.effective_session(
            datetime(2026, 8, 25, 22, 15, tzinfo=UTC)) == date(2026, 8, 26)

    def test_a_friday_evening_story_lands_on_monday_not_saturday(self):
        assert nw.effective_session(
            datetime(2026, 8, 21, 23, 0, tzinfo=UTC)) == date(2026, 8, 24)

    def test_a_naive_timestamp_is_read_as_utc_rather_than_local_time(self):
        assert nw.effective_session(datetime(2026, 8, 25, 13, 30)) == date(2026, 8, 25)


class TestPointInTime:
    """The rule that inverts the earnings rule: publication is a PAST fact, so
    the gate is the clock — never whether our ingestion had run."""

    def test_an_article_published_after_the_session_is_invisible_to_it(self):
        # The exact case the mission names: session 2026-07-29 must never see a
        # story published 2026-07-30.
        assert not nw.is_visible_to_session(
            datetime(2026, 7, 30, 12, 0, tzinfo=UTC), date(2026, 7, 29))

    def test_an_article_published_before_the_session_is_visible_to_it(self):
        assert nw.is_visible_to_session(
            datetime(2026, 7, 28, 12, 0, tzinfo=UTC), date(2026, 7, 29))

    def test_the_boundary_is_the_close_not_midnight(self):
        session = date(2026, 8, 25)
        assert nw.is_visible_to_session(
            datetime(2026, 8, 25, 19, 59, tzinfo=UTC), session)
        assert not nw.is_visible_to_session(
            datetime(2026, 8, 25, 20, 1, tzinfo=UTC), session)

    def test_a_late_article_is_filtered_out_of_a_historical_selection(self):
        picked = nw.select_visible_articles(
            [article(published="2026-07-30T12:00:00+00:00", title="Apple after"),
             article(published="2026-07-28T12:00:00+00:00", title="Apple before")],
            as_of_session=date(2026, 7, 29))
        assert [p["title"] for p in picked] == ["Apple before"]

    def test_ingestion_time_is_irrelevant_to_visibility(self):
        # An article back-filled today is still legitimate context for an older
        # session: it was public then. Nothing in the visible-article path reads
        # `observed_at`, and this is the test that keeps it that way.
        row = article(published="2026-07-28T12:00:00+00:00")
        row["observed_at"] = datetime(2026, 8, 27, tzinfo=UTC)
        assert nw.select_visible_articles([row], as_of_session=date(2026, 7, 29))


# --------------------------------------------------------------------------- #
# relevance windows — fixed a priori, counted in trading sessions
# --------------------------------------------------------------------------- #

class TestProximityWindows:
    @pytest.mark.parametrize("sessions_ago,expected", [
        (0, nw.PROXIMITY_TODAY),
        (1, nw.PROXIMITY_RECENT), (3, nw.PROXIMITY_RECENT),
        (4, nw.PROXIMITY_OLDER_CONTEXT), (7, nw.PROXIMITY_OLDER_CONTEXT),
        (8, nw.PROXIMITY_OUT_OF_WINDOW), (400, nw.PROXIMITY_OUT_OF_WINDOW),
        (None, nw.PROXIMITY_OUT_OF_WINDOW),
    ])
    def test_boundaries_are_exactly_where_they_are_documented(self, sessions_ago, expected):
        assert nw.classify_proximity(sessions_ago) == expected

    def test_only_today_and_recent_may_reach_the_scanner_list(self):
        assert nw.NOTABLE_PROXIMITIES == (nw.PROXIMITY_TODAY, nw.PROXIMITY_RECENT)
        assert not nw.is_notable(nw.PROXIMITY_OLDER_CONTEXT,
                                 scope=nw.SCOPE_COMPANY_SPECIFIC,
                                 relevance=nw.RELEVANCE_PRIMARY)

    def test_a_weekend_gap_is_counted_in_sessions_not_days(self):
        # Friday evening -> Monday session is ONE session, not three days.
        picked = nw.select_visible_articles(
            [article(published="2026-08-21T23:00:00+00:00")],
            as_of_session=date(2026, 8, 25))
        assert picked[0]["_sessions_ago"] == 1
        assert picked[0]["_proximity"] == nw.PROXIMITY_RECENT

    def test_an_ancient_article_is_dropped_entirely(self):
        assert nw.select_visible_articles(
            [article(published="2026-06-01T12:00:00+00:00")],
            as_of_session=date(2026, 8, 25)) == []


# --------------------------------------------------------------------------- #
# the near-duplicate rule
# --------------------------------------------------------------------------- #

class TestNearDuplicates:
    def test_the_same_publisher_reposting_its_own_headline_is_collapsed(self):
        picked = nw.select_visible_articles([
            article(published="2026-08-25T14:00:00+00:00",
                    title="Apple posts record quarter", publisher="Reuters",
                    url="https://reuters.com/a/1"),
            article(published="2026-08-25T13:00:00+00:00",
                    title="Apple Posts Record Quarter!", publisher="Reuters",
                    url="https://reuters.com/a/2"),
        ], as_of_session=date(2026, 8, 25))
        assert len(picked) == 1
        assert picked[0]["article_url"] == "https://reuters.com/a/1"  # newest kept

    def test_two_independent_publishers_on_one_event_both_survive(self):
        # Deliberate: two outlets covering the same event are two pieces of
        # evidence that it mattered. Collapsing them would hide the signal.
        picked = nw.select_visible_articles([
            article(published="2026-08-25T14:00:00+00:00",
                    title="Apple posts record quarter", publisher="Reuters"),
            article(published="2026-08-25T13:00:00+00:00",
                    title="Apple posts record quarter", publisher="Bloomberg",
                    url="https://bloomberg.com/a/9"),
        ], as_of_session=date(2026, 8, 25))
        assert len(picked) == 2

    def test_the_payload_is_bounded_however_loud_the_feed_is(self):
        many = [article(published=f"2026-08-25T{h:02d}:00:00+00:00",
                        title=f"Apple story {h}", url=f"https://reuters.com/a/{h}")
                for h in range(0, 20)]
        picked = nw.select_visible_articles(many, as_of_session=date(2026, 8, 25))
        assert len(picked) == nw.MAX_DETAIL_ITEMS

    def test_selection_is_newest_first(self):
        picked = nw.select_visible_articles([
            article(published="2026-08-24T14:00:00+00:00", title="older",
                    url="https://reuters.com/a/1"),
            article(published="2026-08-25T14:00:00+00:00", title="newer",
                    url="https://reuters.com/a/2"),
        ], as_of_session=date(2026, 8, 25))
        assert [p["title"] for p in picked] == ["newer", "older"]


# --------------------------------------------------------------------------- #
# freshness — never present old news as current
# --------------------------------------------------------------------------- #

class TestFreshness:
    NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)

    def test_never_refreshed_is_not_the_same_as_nothing_happened(self):
        verdict = nw.evaluate_freshness(None, now=self.NOW)
        assert verdict["status"] == nw.STATUS_UNAVAILABLE
        assert verdict["reason"] == nw.REASON_NEVER_REFRESHED

    def test_an_unavailable_source_is_reported_as_unavailable(self):
        verdict = nw.evaluate_freshness(
            {"status": "unavailable", "last_success_at": None,
             "last_refresh_at": self.NOW, "detail": "provider_not_entitled"},
            now=self.NOW)
        assert verdict["status"] == nw.STATUS_UNAVAILABLE
        assert verdict["reason"] == nw.REASON_SOURCE_UNAVAILABLE
        assert verdict["detail"] == "provider_not_entitled"

    def test_a_stalled_ingestion_makes_the_dimension_stale_not_available(self):
        verdict = nw.evaluate_freshness(
            {"status": "ok", "last_success_at": self.NOW - timedelta(hours=30),
             "last_refresh_at": self.NOW, "detail": None}, now=self.NOW)
        assert verdict["status"] == nw.STATUS_STALE
        assert verdict["reason"] == nw.REASON_STALE_REFRESH

    def test_news_goes_stale_faster_than_an_earnings_date_does(self):
        import app.catalyst as cat
        assert nw.FRESHNESS_MAX_AGE_HOURS < cat.FRESHNESS_MAX_AGE_HOURS

    def test_a_recent_success_is_available(self):
        verdict = nw.evaluate_freshness(
            {"status": "ok", "last_success_at": self.NOW - timedelta(hours=2),
             "last_refresh_at": self.NOW, "detail": None}, now=self.NOW)
        assert verdict["status"] == nw.STATUS_AVAILABLE
        assert verdict["reason"] is None


# --------------------------------------------------------------------------- #
# the product objects
# --------------------------------------------------------------------------- #

class TestNewsContext:
    def test_an_unavailable_source_yields_no_items_and_says_why(self):
        ctx = nw.build_news_context(
            [article(published="2026-08-25T14:00:00+00:00")], symbol="AAPL",
            as_of_session=date(2026, 8, 25),
            freshness={"status": nw.STATUS_UNAVAILABLE,
                       "reason": nw.REASON_SOURCE_UNAVAILABLE,
                       "last_refresh_at": None, "last_success_at": None,
                       "age_hours": None, "detail": None})
        assert ctx["items"] == []
        assert ctx["status"] == nw.STATUS_UNAVAILABLE
        assert ctx["reason"] == nw.REASON_SOURCE_UNAVAILABLE
        assert ctx["notable_count"] == 0

    def test_a_market_wide_mention_is_carried_but_never_notable(self):
        ctx = nw.build_news_context([
            article(published="2026-08-25T14:00:00+00:00",
                    title="5 stocks Berkshire owns", scope=nw.SCOPE_MARKET_WIDE,
                    relevance=nw.RELEVANCE_MENTIONED, breadth=25),
        ], symbol="AAPL", as_of_session=date(2026, 8, 25), freshness=FRESH)
        assert ctx["in_window_count"] == 1
        assert ctx["notable_count"] == 0
        assert ctx["top_category"] is None

    def test_the_top_category_describes_the_newest_notable_item_only(self):
        ctx = nw.build_news_context([
            article(published="2026-08-25T14:00:00+00:00", title="Apple upgraded",
                    category=nw.CATEGORY_ANALYST_ACTION, url="https://r.com/1"),
            article(published="2026-08-24T14:00:00+00:00", title="Apple sued",
                    category=nw.CATEGORY_REGULATORY_LEGAL, url="https://r.com/2"),
        ], symbol="AAPL", as_of_session=date(2026, 8, 25), freshness=FRESH)
        assert ctx["top_category"] == nw.CATEGORY_ANALYST_ACTION
        assert ctx["notable_count"] == 2

    def test_an_item_exposes_no_provider_payload_and_no_opinion(self):
        ctx = nw.build_news_context(
            [article(published="2026-08-25T14:00:00+00:00")], symbol="AAPL",
            as_of_session=date(2026, 8, 25), freshness=FRESH)
        item = ctx["items"][0]
        assert set(item) == {
            "published_at", "session", "sessions_ago", "proximity", "headline",
            "publisher", "url", "category", "category_source", "scope",
            "relevance", "notable"}

    def test_the_contract_version_is_declared_on_the_block_itself(self):
        ctx = nw.build_news_context([], symbol="AAPL",
                                    as_of_session=date(2026, 8, 25),
                                    freshness=FRESH)
        assert ctx["contract_version"] == nw.NEWS_CONTEXT_CONTRACT_VERSION

    def test_an_available_source_with_no_articles_is_not_an_error(self):
        # Coverage on this plan is genuinely uneven; several index constituents
        # return nothing for weeks. That is a fact about the feed, and it must
        # read as "available, zero items", never as a failure.
        ctx = nw.build_news_context([], symbol="NFLX",
                                    as_of_session=date(2026, 8, 25),
                                    freshness=FRESH)
        assert ctx["status"] == nw.STATUS_AVAILABLE
        assert ctx["in_window_count"] == 0
        assert ctx["latest_published_at"] is None


class TestRowNews:
    def test_a_quiet_row_carries_nothing_to_print(self):
        row = nw.build_row_news(nw.build_news_context(
            [], symbol="NFLX", as_of_session=date(2026, 8, 25), freshness=FRESH))
        assert row["notable_count"] == 0
        assert row["latest_headline"] is None
        assert row["latest_proximity"] is None
        assert row["top_category"] is None

    def test_a_loud_row_carries_counts_and_one_headline_not_a_feed(self):
        ctx = nw.build_news_context(
            [article(published=f"2026-08-25T{h:02d}:00:00+00:00",
                     title=f"Apple story {h}", url=f"https://r.com/{h}")
             for h in range(10, 16)],
            symbol="AAPL", as_of_session=date(2026, 8, 25), freshness=FRESH)
        row = nw.build_row_news(ctx)
        assert row["notable_count"] == 6
        assert isinstance(row["latest_headline"], str)
        assert "items" not in row

    def test_an_empty_context_is_a_complete_unavailable_block(self):
        ctx = nw.empty_news_context(symbol="AAPL")
        assert ctx["status"] == nw.STATUS_UNAVAILABLE
        assert ctx["items"] == []
        assert nw.build_row_news(ctx)["notable_count"] == 0
