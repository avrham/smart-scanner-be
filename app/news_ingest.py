"""Bounded company-news ingestion for the Smart Scanner catalyst layer.

Shaped like a job handler: an open connection in, a bounded summary out, no
scheduling of its own. It is the ONLY component here that touches the provider,
and it must run where the provider credential already lives (the history-warmup
worker). The Product API holds no credential and only ever reads what this
writes.

PROVIDER REALITY, MEASURED NOT ASSUMED
--------------------------------------
The configured provider exposes a news feed at `/v2/reference/news` and our
plan IS entitled to it (HTTP 200, verified live). Two neighbouring feeds are
not, and are deliberately never called:

    /benzinga/v1/news              403 NOT_AUTHORIZED
    /benzinga/v1/analyst-insights  403 not entitled

The entitled feed is therefore the whole source, and it carries no category or
channel field — which is why `app.news.classify_category` derives categories
from headlines and labels every one of them `derived_title` or `default`.

WHAT IS DROPPED AT THE BOUNDARY
-------------------------------
Each provider article ships `insights[].sentiment`, `insights[].
sentiment_reasoning`, an AI-written `description`, machine `keywords` and an
`image_url`. None of it is persisted. This is not an oversight and not a
storage saving: a sentiment label stored beside a scanner verdict would be read
as the same kind of fact, and V1's whole claim is that it reports what happened
and refuses to say what it means.

COVERAGE IS UNEVEN, AND THAT IS A FACT ABOUT THE FEED
-----------------------------------------------------
Volume per symbol differs by two orders of magnitude on this plan (mega-cap AI
names return hundreds of articles per month; several index constituents return
single digits). Silence for a symbol therefore means "this feed published
nothing about it", never "nothing happened" — which is why the product reports
source availability separately from article counts.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

# ---- shared vocabulary ------------------------------------------------------ #
# Imported, not redeclared: `app/news.py` owns these words, so what we persist
# and what the product says can never drift apart.
from app.news import (  # noqa: F401
    CATEGORIES, CATEGORY_GENERAL, CATEGORY_SOURCE_DEFAULT,
    CATEGORY_SOURCE_DERIVED_TITLE, RELEVANCE_MENTIONED, RELEVANCE_PRIMARY,
    SCOPE_COMPANY_SPECIFIC, SCOPE_MARKET_WIDE, SCOPE_MULTI_COMPANY,
    SOURCE_COMPANY_NEWS, canonical_url, classify_category, classify_relevance,
    classify_scope, normalize_title,
)

# ---- source state (same vocabulary as catalyst ingestion) ------------------- #
STATE_OK = "ok"
STATE_UNAVAILABLE = "unavailable"
STATE_ERROR = "error"
STATE_NEVER_RUN = "never_run"

#: The one entitled news path. Anything else is a different product decision.
NEWS_PATH = "/v2/reference/news"
#: Paths proven NOT entitled on this plan — recorded so nobody re-discovers it.
UNENTITLED_NEWS_PATHS = ("/benzinga/v1/news", "/benzinga/v1/analyst-insights")

PROVIDER = "massive"

#: Hard per-symbol bounds. Ingestion must never become an unbounded crawl.
PAGE_SIZE = 100
MAX_PAGES_PER_SYMBOL = 2
#: Calendar days of history to request. The product window is 7 TRADING
#: sessions; 14 calendar days covers it with room for holidays.
DEFAULT_LOOKBACK_DAYS = 14


class NewsSourceUnavailable(Exception):
    """The feed exists but this deployment cannot read it (entitlement,
    credential, or removal). Carries a short, secret-free reason."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


# --------------------------------------------------------------------------- #
# Normalisation — pure, so it is fully testable without a provider
# --------------------------------------------------------------------------- #

def _as_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _text(value: Any, limit: int = 400) -> Optional[str]:
    if value is None:
        return None
    out = str(value).strip()
    return out[:limit] or None


def normalize_article(row: Dict[str, Any], *,
                      observed_at: datetime) -> Optional[Dict[str, Any]]:
    """One provider article -> one storable row, or None if unusable.

    An article with no stable id, no publication time, no title or no URL is
    dropped rather than stored with invented parts: every downstream honesty
    guarantee (dedupe, point-in-time, provenance) rests on those four fields.
    """
    article_id = _text(row.get("id"), 200)
    published_at = _as_datetime(row.get("published_utc"))
    title = _text(row.get("title"), 800)
    url = _text(row.get("article_url"), 1000)
    if not article_id or published_at is None or not title or not url:
        return None

    publisher_block = row.get("publisher") or {}
    publisher = _text(publisher_block.get("name"), 200) or "unknown"

    tickers = sorted({str(t).strip().upper()
                      for t in (row.get("tickers") or []) if str(t).strip()})
    category, category_source = classify_category(title)

    return {
        "provider": PROVIDER,
        "provider_article_id": article_id,
        "published_at": published_at,
        "title": title,
        "title_normalized": normalize_title(title),
        "publisher": publisher,
        "publisher_home_url": _text(publisher_block.get("homepage_url"), 500),
        "author": _text(row.get("author"), 200),
        "article_url": url,
        "canonical_url": canonical_url(url),
        "ticker_breadth": len(tickers),
        "scope": classify_scope(len(tickers)),
        "category": category,
        "category_source": category_source,
        "observed_at": observed_at,
        # Transport only — never a column. Used to build the symbol links.
        "_tickers": tickers,
    }


# --------------------------------------------------------------------------- #
# Provider access
# --------------------------------------------------------------------------- #

async def fetch_company_news(client, symbols: Sequence[str], *,
                             observed_at: datetime,
                             lookback_days: int = DEFAULT_LOOKBACK_DAYS,
                             max_pages: int = MAX_PAGES_PER_SYMBOL,
                             ) -> List[Dict[str, Any]]:
    """Bounded per-symbol news fetch.

    Raises NewsSourceUnavailable when the deployment is not entitled to the
    feed, so an entitlement failure is reported as an unavailable SOURCE rather
    than as an empty result — an absence measured through a blind source is not
    evidence of absence.
    """
    since = (observed_at - timedelta(days=lookback_days)).date().isoformat()
    articles: List[Dict[str, Any]] = []
    for symbol in symbols:
        params: Optional[Dict[str, Any]] = {
            "ticker": symbol, "published_utc.gte": since,
            "order": "desc", "sort": "published_utc", "limit": PAGE_SIZE,
        }
        path = NEWS_PATH
        for _ in range(max(1, max_pages)):
            try:
                payload = await client._request(path, params)
            except Exception as exc:  # provider error object is already sanitised
                status = getattr(exc, "status_code", None)
                if status in (401, 402, 403, 404):
                    raise NewsSourceUnavailable(
                        "provider_not_entitled",
                        f"news feed returned HTTP {status} for this plan",
                    ) from exc
                raise
            for row in (payload or {}).get("results") or []:
                record = normalize_article(row, observed_at=observed_at)
                if record:
                    articles.append(record)
            next_url = (payload or {}).get("next_url")
            if not next_url:
                break
            path, params = next_url, None
    return articles


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #

FIND_BY_CANONICAL_SQL = """
SELECT id, provider_article_id FROM public.company_news_articles
WHERE provider = $1 AND canonical_url = $2
LIMIT 1
"""

UPSERT_ARTICLE_SQL = """
INSERT INTO public.company_news_articles (
    provider, provider_article_id, published_at, title, title_normalized,
    publisher, publisher_home_url, author, article_url, canonical_url,
    ticker_breadth, scope, category, category_source, observed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (provider, provider_article_id) DO UPDATE SET
    published_at = EXCLUDED.published_at,
    title = EXCLUDED.title,
    title_normalized = EXCLUDED.title_normalized,
    publisher = EXCLUDED.publisher,
    publisher_home_url = COALESCE(EXCLUDED.publisher_home_url,
                                  company_news_articles.publisher_home_url),
    author = COALESCE(EXCLUDED.author, company_news_articles.author),
    article_url = EXCLUDED.article_url,
    canonical_url = EXCLUDED.canonical_url,
    ticker_breadth = EXCLUDED.ticker_breadth,
    scope = EXCLUDED.scope,
    category = EXCLUDED.category,
    category_source = EXCLUDED.category_source,
    updated_at = NOW()
RETURNING id, (xmax = 0) AS inserted
"""

LINK_SQL = """
INSERT INTO public.company_news_symbols (article_id, symbol, relevance)
VALUES ($1,$2,$3)
ON CONFLICT (symbol, article_id) DO UPDATE SET relevance = EXCLUDED.relevance
"""

SOURCE_STATE_SQL = """
INSERT INTO public.catalyst_source_state (
    source, status, last_refresh_at, last_success_at,
    symbols_covered, events_upserted, detail, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
ON CONFLICT (source) DO UPDATE SET
    status = EXCLUDED.status,
    last_refresh_at = EXCLUDED.last_refresh_at,
    last_success_at = COALESCE(EXCLUDED.last_success_at,
                               catalyst_source_state.last_success_at),
    symbols_covered = EXCLUDED.symbols_covered,
    events_upserted = EXCLUDED.events_upserted,
    detail = EXCLUDED.detail,
    updated_at = NOW()
"""


async def upsert_articles(conn, articles: Iterable[Dict[str, Any]], *,
                          universe: Optional[Set[str]] = None) -> Dict[str, int]:
    """Idempotent write of articles + their per-symbol links.

    THE DEDUPE RULE, in full and in this order:

      1. provider identity — `(provider, provider_article_id)` is UNIQUE, so a
         second refresh updates the existing row. This is why re-running
         ingestion cannot grow the table.
      2. canonical URL — before inserting an article we have never seen by id,
         we look for one already stored under the same scheme+host+path. A hit
         means the provider re-issued the same story under a new id; we reuse
         the stored article and attach any new symbol links to it. This is
         handled here rather than by a UNIQUE constraint so that a collision
         SKIPS one article instead of failing a whole refresh.

    Near-duplicate headlines are deliberately NOT resolved here. Two publishers
    covering the same event are two independent facts, and which of them a
    reader should see is a presentation decision — made once, at read time, in
    `app.news.select_visible_articles`.
    """
    # `inserted` vs `updated` is the point: a second refresh over the same
    # window must report zero inserts. A single "written" counter would hide
    # exactly the failure this is here to detect.
    stats = {"articles_seen": 0, "articles_inserted": 0, "articles_updated": 0,
             "canonical_duplicates": 0, "links_written": 0}

    for article in articles:
        stats["articles_seen"] += 1
        article_id = None

        existing = await conn.fetchrow(FIND_BY_CANONICAL_SQL, article["provider"],
                                       article["canonical_url"])
        if existing is not None and \
                existing["provider_article_id"] != article["provider_article_id"]:
            article_id = existing["id"]
            stats["canonical_duplicates"] += 1
        else:
            written = await conn.fetchrow(
                UPSERT_ARTICLE_SQL,
                article["provider"], article["provider_article_id"],
                article["published_at"], article["title"],
                article["title_normalized"], article["publisher"],
                article.get("publisher_home_url"), article.get("author"),
                article["article_url"], article["canonical_url"],
                article["ticker_breadth"], article["scope"],
                article["category"], article["category_source"],
                article["observed_at"])
            article_id = written["id"]
            stats["articles_inserted" if written["inserted"]
                  else "articles_updated"] += 1

        # Link every ticker we actually track. An article fetched for AAPL that
        # also names MSFT is real MSFT context, and attaching it here costs no
        # extra provider call.
        for symbol in article.get("_tickers") or []:
            if universe is not None and symbol not in universe:
                continue
            await conn.execute(LINK_SQL, article_id, symbol,
                               classify_relevance(article["title"], symbol))
            stats["links_written"] += 1

    return stats


async def record_source_state(conn, status: str, *, symbols_covered: int = 0,
                              articles_written: int = 0, detail: str = "",
                              now: Optional[datetime] = None) -> None:
    """One row in `catalyst_source_state`, so an empty news table can never be
    misread as 'nothing happened'."""
    moment = now or datetime.now(timezone.utc)
    await conn.execute(
        SOURCE_STATE_SQL, SOURCE_COMPANY_NEWS, status, moment,
        moment if status == STATE_OK else None,
        symbols_covered, articles_written, detail[:400] or None)


async def refresh_company_news(conn, client, symbols: Sequence[str], *,
                               now: Optional[datetime] = None,
                               lookback_days: int = DEFAULT_LOOKBACK_DAYS,
                               max_pages: int = MAX_PAGES_PER_SYMBOL,
                               ) -> Dict[str, Any]:
    """One idempotent refresh over a bounded symbol list.

    A provider failure is absorbed into `catalyst_source_state` and reported,
    never raised: a news outage must cost the product its news dimension and
    nothing else.
    """
    moment = now or datetime.now(timezone.utc)
    cleaned = [s.strip().upper() for s in symbols if s and s.strip()]
    universe = set(cleaned)
    summary: Dict[str, Any] = {"symbols": len(cleaned),
                               "source": SOURCE_COMPANY_NEWS}

    try:
        articles = await fetch_company_news(
            client, cleaned, observed_at=moment,
            lookback_days=lookback_days, max_pages=max_pages)
        stats = await upsert_articles(conn, articles, universe=universe)
        await record_source_state(conn, STATE_OK, symbols_covered=len(cleaned),
                                  articles_written=stats["articles_inserted"],
                                  now=moment)
        summary.update({"status": STATE_OK, **stats})
    except NewsSourceUnavailable as exc:
        await record_source_state(conn, STATE_UNAVAILABLE,
                                  symbols_covered=len(cleaned),
                                  detail=f"{exc.reason}: {exc.detail}", now=moment)
        summary.update({"status": STATE_UNAVAILABLE, "reason": exc.reason})
    except Exception as exc:
        await record_source_state(conn, STATE_ERROR, symbols_covered=len(cleaned),
                                  detail=type(exc).__name__, now=moment)
        summary.update({"status": STATE_ERROR, "reason": type(exc).__name__})

    return summary


__all__ = [
    "PROVIDER", "NEWS_PATH", "UNENTITLED_NEWS_PATHS", "SOURCE_COMPANY_NEWS",
    "STATE_OK", "STATE_UNAVAILABLE", "STATE_ERROR", "STATE_NEVER_RUN",
    "PAGE_SIZE", "MAX_PAGES_PER_SYMBOL", "DEFAULT_LOOKBACK_DAYS",
    "NewsSourceUnavailable", "normalize_article", "fetch_company_news",
    "upsert_articles", "record_source_state", "refresh_company_news",
]
