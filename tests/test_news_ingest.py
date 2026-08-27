"""Company-news ingestion: normalisation, entitlement, and the dedupe rule.

The DB is a small in-memory fake that enforces exactly the two constraints the
real schema declares — `UNIQUE (provider, provider_article_id)` on articles and
`UNIQUE (symbol, article_id)` on links. That is enough to prove idempotency and
duplicate handling without a live Postgres, and the same statements are exercised
against a real database in the staging run.
"""

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest

import app.news as nw
import app.news_ingest as ni

UTC = timezone.utc
run = asyncio.run
NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)


def provider_article(**over):
    """A provider row in its real shape, opinion fields included — so the tests
    can prove those fields are dropped rather than merely absent."""
    row = {
        "id": "abc123",
        "publisher": {"name": "Reuters", "homepage_url": "https://reuters.com/",
                      "logo_url": "https://s3/logo.svg",
                      "favicon_url": "https://s3/fav.ico"},
        "title": "Apple reports Q3 earnings, beats estimates",
        "author": "A Reporter",
        "published_utc": "2026-08-25T14:32:00Z",
        "article_url": "https://www.reuters.com/tech/apple-q3/?source=feed1",
        "tickers": ["AAPL", "MSFT"],
        "image_url": "https://img/x.png",
        "description": "An AI-written summary of what the article argues.",
        "keywords": ["earnings", "technology"],
        "insights": [{"ticker": "AAPL", "sentiment": "positive",
                      "sentiment_reasoning": "The article frames results well."}],
    }
    row.update(over)
    return row


# --------------------------------------------------------------------------- #
# a minimal fake DB that enforces the real constraints
# --------------------------------------------------------------------------- #

class FakeConn:
    def __init__(self):
        self.articles = {}      # id -> row
        self.by_provider = {}   # (provider, provider_article_id) -> id
        self.links = {}         # (symbol, article_id) -> relevance
        self.source_state = {}
        self._next = 0

    async def fetchrow(self, sql, *args):
        if "INSERT INTO public.company_news_articles" in sql:
            return await self._upsert(sql, *args)
        if "canonical_url" in sql:
            provider, canonical = args
            for row in self.articles.values():
                if row["provider"] == provider and row["canonical_url"] == canonical:
                    return {"id": row["id"],
                            "provider_article_id": row["provider_article_id"]}
            return None
        raise AssertionError(f"unexpected fetchrow: {sql[:60]}")

    async def fetchval(self, sql, *args):   # pragma: no cover - unused
        raise AssertionError("articles are written through fetchrow")

    async def _upsert(self, sql, *args):
        assert "INSERT INTO public.company_news_articles" in sql
        cols = ["provider", "provider_article_id", "published_at", "title",
                "title_normalized", "publisher", "publisher_home_url", "author",
                "article_url", "canonical_url", "ticker_breadth", "scope",
                "category", "category_source", "observed_at"]
        row = dict(zip(cols, args))
        key = (row["provider"], row["provider_article_id"])
        if key in self.by_provider:              # ON CONFLICT DO UPDATE
            existing = self.by_provider[key]
            self.articles[existing].update(row)
            self.articles[existing]["id"] = existing
            return {"id": existing, "inserted": False}
        self._next += 1
        row["id"] = self._next
        self.articles[self._next] = row
        self.by_provider[key] = self._next
        return {"id": self._next, "inserted": True}

    async def execute(self, sql, *args):
        if "company_news_symbols" in sql:
            article_id, symbol, relevance = args
            self.links[(symbol, article_id)] = relevance
            return
        if "catalyst_source_state" in sql:
            self.source_state[args[0]] = {
                "source": args[0], "status": args[1], "last_refresh_at": args[2],
                "last_success_at": args[3], "symbols_covered": args[4],
                "events_upserted": args[5], "detail": args[6]}
            return
        raise AssertionError(f"unexpected execute: {sql[:60]}")


class FakeClient:
    """Stands in for MassiveClient. Records every path it was asked for."""

    def __init__(self, pages=None, error=None):
        self.pages = pages or []
        self.error = error
        self.calls = []

    async def _request(self, path, params=None):
        self.calls.append((path, params))
        if self.error:
            raise self.error
        return self.pages.pop(0) if self.pages else {"results": []}


class ProviderError(Exception):
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


# --------------------------------------------------------------------------- #
# normalisation
# --------------------------------------------------------------------------- #

class TestNormalizeArticle:
    def test_the_stored_row_carries_only_checkable_facts(self):
        row = ni.normalize_article(provider_article(), observed_at=NOW)
        assert set(row) == {
            "provider", "provider_article_id", "published_at", "title",
            "title_normalized", "publisher", "publisher_home_url", "author",
            "article_url", "canonical_url", "ticker_breadth", "scope",
            "category", "category_source", "observed_at", "_tickers"}

    def test_sentiment_and_the_machine_summary_are_dropped_at_the_boundary(self):
        # The product's whole claim is that it reports what happened and
        # refuses to say what it means. This is where that is enforced.
        row = ni.normalize_article(provider_article(), observed_at=NOW)
        blob = repr(row).lower()
        for banned in ("sentiment", "reasoning", "an ai-written summary",
                       "description", "keywords", "image"):
            assert banned not in blob

    def test_publication_time_is_parsed_as_utc(self):
        row = ni.normalize_article(provider_article(), observed_at=NOW)
        assert row["published_at"] == datetime(2026, 8, 25, 14, 32, tzinfo=UTC)

    def test_tracking_parameters_are_stripped_into_the_canonical_url(self):
        row = ni.normalize_article(provider_article(), observed_at=NOW)
        assert row["canonical_url"] == "https://reuters.com/tech/apple-q3"
        assert row["article_url"].endswith("?source=feed1")   # the clickable one

    def test_scope_and_category_are_derived_and_labelled(self):
        row = ni.normalize_article(provider_article(), observed_at=NOW)
        assert row["ticker_breadth"] == 2
        assert row["scope"] == nw.SCOPE_COMPANY_SPECIFIC
        assert row["category"] == nw.CATEGORY_EARNINGS_RESULTS
        assert row["category_source"] == nw.CATEGORY_SOURCE_DERIVED_TITLE

    @pytest.mark.parametrize("missing", ["id", "published_utc", "title", "article_url"])
    def test_an_article_missing_an_honesty_critical_field_is_dropped(self, missing):
        # Dedupe, point-in-time and provenance all rest on these four. A row
        # without one is dropped, never stored with an invented part.
        assert ni.normalize_article(provider_article(**{missing: None}),
                                    observed_at=NOW) is None

    def test_duplicate_tickers_do_not_inflate_the_breadth_count(self):
        row = ni.normalize_article(
            provider_article(tickers=["AAPL", "aapl", "AAPL ", "MSFT"]),
            observed_at=NOW)
        assert row["ticker_breadth"] == 2

    def test_a_missing_publisher_becomes_unknown_rather_than_empty(self):
        row = ni.normalize_article(provider_article(publisher={}), observed_at=NOW)
        assert row["publisher"] == "unknown"


# --------------------------------------------------------------------------- #
# provider access
# --------------------------------------------------------------------------- #

class TestFetch:
    @pytest.mark.parametrize("status", [401, 402, 403, 404])
    def test_an_entitlement_failure_is_an_unavailable_source(self, status):
        client = FakeClient(error=ProviderError(status))
        with pytest.raises(ni.NewsSourceUnavailable) as caught:
            run(ni.fetch_company_news(client, ["AAPL"], observed_at=NOW))
        assert caught.value.reason == "provider_not_entitled"
        assert str(status) in caught.value.detail

    def test_an_unrelated_provider_error_is_not_disguised_as_entitlement(self):
        client = FakeClient(error=ProviderError(500))
        with pytest.raises(ProviderError):
            run(ni.fetch_company_news(client, ["AAPL"], observed_at=NOW))

    def test_only_the_entitled_path_is_ever_called(self):
        client = FakeClient(pages=[{"results": [provider_article()]}])
        run(ni.fetch_company_news(client, ["AAPL"], observed_at=NOW))
        assert all(path == ni.NEWS_PATH or path.startswith("http")
                   for path, _ in client.calls)
        for banned in ni.UNENTITLED_NEWS_PATHS:
            assert all(banned not in path for path, _ in client.calls)

    def test_the_fetch_is_bounded_however_many_pages_the_feed_offers(self):
        endless = [{"results": [provider_article(id=f"a{i}")],
                    "next_url": "https://api/next"} for i in range(20)]
        client = FakeClient(pages=endless)
        run(ni.fetch_company_news(client, ["AAPL"], observed_at=NOW, max_pages=2))
        assert len(client.calls) == 2

    def test_the_lookback_window_is_sent_to_the_provider(self):
        client = FakeClient(pages=[{"results": []}])
        run(ni.fetch_company_news(client, ["AAPL"], observed_at=NOW,
                                    lookback_days=14))
        _, params = client.calls[0]
        assert params["published_utc.gte"] == "2026-08-13"
        assert params["ticker"] == "AAPL"


# --------------------------------------------------------------------------- #
# persistence: the dedupe rule
# --------------------------------------------------------------------------- #

class TestUpsertAndDedupe:
    def test_running_the_same_refresh_twice_stores_one_article(self):
        conn = FakeConn()
        rows = [ni.normalize_article(provider_article(), observed_at=NOW)]
        first = run(ni.upsert_articles(conn, rows, universe={"AAPL", "MSFT"}))
        second = run(ni.upsert_articles(conn, rows, universe={"AAPL", "MSFT"}))
        assert len(conn.articles) == 1
        assert first["articles_inserted"] == 1
        assert second["articles_inserted"] == 0     # idempotent
        assert second["articles_updated"] == 1
        assert len(conn.links) == 2          # AAPL + MSFT, still two

    def test_the_same_story_re_issued_under_a_new_id_is_not_stored_twice(self):
        conn = FakeConn()
        original = ni.normalize_article(provider_article(), observed_at=NOW)
        reissued = ni.normalize_article(
            provider_article(id="different-id",
                             article_url="https://reuters.com/tech/apple-q3?src=2"),
            observed_at=NOW)
        run(ni.upsert_articles(conn, [original], universe={"AAPL"}))
        stats = run(ni.upsert_articles(conn, [reissued], universe={"AAPL"}))
        assert len(conn.articles) == 1
        assert stats["canonical_duplicates"] == 1

    def test_a_genuinely_different_story_is_stored_alongside(self):
        conn = FakeConn()
        run(ni.upsert_articles(
            conn, [ni.normalize_article(provider_article(), observed_at=NOW)],
            universe={"AAPL"}))
        run(ni.upsert_articles(conn, [ni.normalize_article(
            provider_article(id="second", title="Apple sued over App Store",
                             article_url="https://reuters.com/tech/apple-suit"),
            observed_at=NOW)], universe={"AAPL"}))
        assert len(conn.articles) == 2

    def test_only_tickers_we_actually_track_get_linked(self):
        conn = FakeConn()
        row = ni.normalize_article(
            provider_article(tickers=["AAPL", "MSFT", "BRK.A", "KO"]),
            observed_at=NOW)
        run(ni.upsert_articles(conn, [row], universe={"AAPL", "MSFT"}))
        assert {s for s, _ in conn.links} == {"AAPL", "MSFT"}

    def test_relevance_is_decided_per_symbol_not_per_article(self):
        # One article, two symbols, two different answers: the headline names
        # Apple and merely lists Microsoft.
        conn = FakeConn()
        row = ni.normalize_article(
            provider_article(title="Apple reports Q3 earnings, beats estimates",
                             tickers=["AAPL", "MSFT"]), observed_at=NOW)
        run(ni.upsert_articles(conn, [row], universe={"AAPL", "MSFT"}))
        article_id = next(iter(conn.articles))
        assert conn.links[("AAPL", article_id)] == nw.RELEVANCE_PRIMARY
        assert conn.links[("MSFT", article_id)] == nw.RELEVANCE_MENTIONED

    def test_a_second_refresh_that_finds_a_new_symbol_links_it(self):
        conn = FakeConn()
        row = ni.normalize_article(provider_article(), observed_at=NOW)
        run(ni.upsert_articles(conn, [row], universe={"AAPL"}))
        run(ni.upsert_articles(conn, [row], universe={"AAPL", "MSFT"}))
        assert len(conn.articles) == 1
        assert {s for s, _ in conn.links} == {"AAPL", "MSFT"}


# --------------------------------------------------------------------------- #
# the refresh as a whole
# --------------------------------------------------------------------------- #

class TestRefresh:
    def test_a_successful_refresh_records_an_ok_source_state(self):
        conn = FakeConn()
        client = FakeClient(pages=[{"results": [provider_article()]}])
        summary = run(ni.refresh_company_news(conn, client, ["AAPL"], now=NOW))
        assert summary["status"] == ni.STATE_OK
        state = conn.source_state[nw.SOURCE_COMPANY_NEWS]
        assert state["status"] == "ok"
        assert state["last_success_at"] == NOW

    def test_an_entitlement_failure_is_recorded_not_raised(self):
        # A news outage must cost the product its news dimension and nothing
        # else — so the refresh returns, and the state says why.
        conn = FakeConn()
        client = FakeClient(error=ProviderError(403))
        summary = run(ni.refresh_company_news(conn, client, ["AAPL"], now=NOW))
        assert summary["status"] == ni.STATE_UNAVAILABLE
        state = conn.source_state[nw.SOURCE_COMPANY_NEWS]
        assert state["status"] == "unavailable"
        assert state["last_success_at"] is None
        assert "provider_not_entitled" in state["detail"]

    def test_an_unexpected_failure_is_recorded_as_an_error_not_a_success(self):
        conn = FakeConn()
        client = FakeClient(error=ProviderError(500))
        summary = run(ni.refresh_company_news(conn, client, ["AAPL"], now=NOW))
        assert summary["status"] == ni.STATE_ERROR
        assert conn.source_state[nw.SOURCE_COMPANY_NEWS]["last_success_at"] is None

    def test_an_empty_but_working_feed_is_a_success_with_zero_articles(self):
        conn = FakeConn()
        summary = run(ni.refresh_company_news(
            conn, FakeClient(pages=[{"results": []}]), ["NFLX"], now=NOW))
        assert summary["status"] == ni.STATE_OK
        assert summary["articles_inserted"] == 0
        assert conn.source_state[nw.SOURCE_COMPANY_NEWS]["status"] == "ok"

    def test_symbols_are_normalised_before_they_reach_the_provider(self):
        conn = FakeConn()
        client = FakeClient(pages=[{"results": []}])
        run(ni.refresh_company_news(conn, client, [" aapl ", ""], now=NOW))
        assert client.calls[0][1]["ticker"] == "AAPL"
        assert len(client.calls) == 1
