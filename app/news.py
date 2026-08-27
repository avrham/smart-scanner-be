"""Deterministic company-news context for the Smart Scanner product API.

PURE: no DB, no network, no provider. Given already-fetched article rows and a
scan session date, every output is a deterministic function of stored facts.

WHAT THIS IS NOT
----------------
News context sits BESIDE the strategy result. It does not change the candidate
verdict, the Wyckoff evaluation, the attention tier, the ordering or ENTER
eligibility, and it produces no score. The product says "this setup exists, AND
something material was published about this company" — never "this setup is
better/worse because of the news". That second statement needs evidence we do
not have, and no amount of headline reading produces it.

There is deliberately NO sentiment anywhere in this module. The provider ships
`insights[].sentiment` and an AI-written `description` with every article; both
are dropped at ingestion and neither is representable here. A machine opinion
rendered beside a scanner verdict would read as the same kind of fact.

POINT-IN-TIME HONESTY — AND WHY IT INVERTS THE EARNINGS RULE
------------------------------------------------------------
`app.catalyst` withholds a FUTURE earnings date unless we observed it on or
before the session, because a scheduled date is knowledge that did not exist
yet. News is the opposite: publication is a PAST PUBLIC FACT. An article
published on 2026-07-25 was public on 2026-07-29 whether or not our ingestion
had run, so back-filling the archive today and showing it for that session adds
no hindsight.

The gate is therefore `published_at`, never `observed_at`:

    an article is visible to session S  iff  published_at <= close(S)

`close(S)` is 16:00 America/New_York on S — the real session close, DST
included. An article published at 20:30 UTC on session S landed after that
session's close and belongs to the NEXT trading session, which is exactly how
a trader reads it.

SESSION ATTRIBUTION
-------------------
Every article is mapped to the trading session it could first have acted on:
the first trading day whose close is at or after `published_at`. Distances are
then counted in TRADING SESSIONS, so a Friday-evening story is "yesterday" on
Monday rather than "three days ago".
"""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from app.catalyst import trading_sessions_between
from app.prospective_session import is_trading_day

NEWS_CONTEXT_CONTRACT_VERSION = "smart_scanner_news_context.v1"

#: The `catalyst_source_state.source` row this dimension reports through.
SOURCE_COMPANY_NEWS = "provider_company_news"

# ---- market clock ----------------------------------------------------------- #
MARKET_TZ = ZoneInfo("America/New_York")
MARKET_CLOSE = time(16, 0)

# ---- scope ------------------------------------------------------------------ #
# How many companies a story is about. Fixed A PRIORI from product meaning, not
# fitted to anything: a piece that names 25 tickers is a market article that
# happens to mention this company, and calling it a company catalyst would be
# false. The raw count is persisted alongside, so the judgement stays checkable.
SCOPE_COMPANY_SPECIFIC = "company_specific"   # 1-3 tickers
SCOPE_MULTI_COMPANY = "multi_company"         # 4-10 tickers
SCOPE_MARKET_WIDE = "market_wide"             # 11+ tickers
SCOPES = (SCOPE_COMPANY_SPECIFIC, SCOPE_MULTI_COMPANY, SCOPE_MARKET_WIDE)

COMPANY_SPECIFIC_MAX_TICKERS = 3
MULTI_COMPANY_MAX_TICKERS = 10

# ---- per-symbol relevance --------------------------------------------------- #
# A visibility fact, never an importance score.
RELEVANCE_PRIMARY = "primary"       # the title names this company
RELEVANCE_MENTIONED = "mentioned"   # the provider attached the ticker; the
                                    # title does not name the company
RELEVANCES = (RELEVANCE_PRIMARY, RELEVANCE_MENTIONED)

# ---- categories ------------------------------------------------------------- #
CATEGORY_EARNINGS_RESULTS = "earnings_results"
CATEGORY_GUIDANCE = "guidance"
CATEGORY_ANALYST_ACTION = "analyst_action"
CATEGORY_MERGER_ACQUISITION = "merger_acquisition"
CATEGORY_REGULATORY_LEGAL = "regulatory_legal"
CATEGORY_MANAGEMENT = "management"
CATEGORY_PRODUCT_ANNOUNCEMENT = "product_announcement"
CATEGORY_FINANCING_CAPITAL = "financing_capital"
CATEGORY_GENERAL = "general_company_news"
CATEGORIES = (CATEGORY_EARNINGS_RESULTS, CATEGORY_GUIDANCE,
              CATEGORY_ANALYST_ACTION, CATEGORY_MERGER_ACQUISITION,
              CATEGORY_REGULATORY_LEGAL, CATEGORY_MANAGEMENT,
              CATEGORY_PRODUCT_ANNOUNCEMENT, CATEGORY_FINANCING_CAPITAL,
              CATEGORY_GENERAL)

CATEGORY_SOURCE_PROVIDER = "provider"
CATEGORY_SOURCE_DERIVED_TITLE = "derived_title"
CATEGORY_SOURCE_DEFAULT = "default"
CATEGORY_SOURCES = (CATEGORY_SOURCE_PROVIDER, CATEGORY_SOURCE_DERIVED_TITLE,
                    CATEGORY_SOURCE_DEFAULT)

# The entitled feed carries NO category field of its own, so every category we
# publish is derived from the headline by the small table below, and every row
# says so via `category_source`. The patterns are deliberately few and
# high-precision — each one names an event that has to have happened for the
# phrase to be written. Anything that does not match is `general_company_news`,
# which is a true statement; a bigger keyword net would only manufacture
# confidence we have not earned.
_CATEGORY_PATTERNS = (
    (CATEGORY_EARNINGS_RESULTS, re.compile(
        r"\b(q[1-4]\s+(earnings|results)|quarterly\s+(earnings|results)"
        r"|earnings\s+(report|call|beat|miss)|reports?\s+q[1-4]"
        r"|(beats?|misses|missed)\s+(earnings\s+)?estimates)\b")),
    (CATEGORY_GUIDANCE, re.compile(
        r"\b(guidance|full[-\s]year\s+outlook|raises?\s+outlook"
        r"|cuts?\s+outlook|lowers?\s+outlook|forecasts?\s+revenue)\b")),
    (CATEGORY_ANALYST_ACTION, re.compile(
        r"\b(upgrades?|downgrades?|price\s+target|initiates?\s+coverage"
        r"|reiterates?\s+(buy|sell|hold)|analyst\s+rating)\b")),
    (CATEGORY_MERGER_ACQUISITION, re.compile(
        # NOT "to buy X": in this feed that phrase is almost always advice
        # ("3 Dividend Stocks to Buy and Hold"), not a transaction. Measured on
        # the real corpus, it produced false positives and nothing else.
        r"\b(acquires?|acquisition|merger|merges?\s+with|takeover"
        r"|buyout|divests?|spin[-\s]?off)\b")),
    (CATEGORY_REGULATORY_LEGAL, re.compile(
        r"\b(lawsuit|sues?|sued|antitrust|regulators?|investigation"
        r"|settlement|fined?|subpoena|ftc|doj|sec\s+(probe|charges))\b")),
    (CATEGORY_MANAGEMENT, re.compile(
        # A management CHANGE, not any sentence containing "CEO" — quoting a
        # sitting chief executive is not a corporate event.
        r"\b((ceo|cfo|coo|chief\s+executive)\s+(steps?\s+down|resigns?"
        r"|departs?|to\s+retire|out\b)|steps?\s+down\s+as|resigns?\s+as"
        r"|appoints?\s+\w+\s+(ceo|cfo|coo)|names?\s+new\s+(ceo|cfo|coo)"
        r"|succession\s+plan)\b")),
    (CATEGORY_PRODUCT_ANNOUNCEMENT, re.compile(
        r"\b(launches?|unveils?|announces?\s+new|introduces?|debuts?"
        r"|rolls?\s+out|releases?\s+new)\b")),
    (CATEGORY_FINANCING_CAPITAL, re.compile(
        # A capital ACTION. Bare "dividend" matched every dividend-screen
        # opinion piece in the corpus, which is a topic, not an event.
        r"\b(raises?\s+(its\s+)?dividend|dividend\s+(hike|increase|cut)"
        r"|declares?\s+(a\s+)?(special\s+)?dividend|buyback"
        r"|share\s+repurchase|stock\s+split|debt\s+offering"
        r"|issues?\s+notes|secondary\s+offering)\b")),
)

# ---- proximity -------------------------------------------------------------- #
# Windows fixed A PRIORI from product meaning, counted in TRADING SESSIONS, and
# never fitted to outcomes:
#
#   today          published inside the scan session itself
#   recent         1-3 sessions back — still plausibly driving this tape
#   older_context  4-7 sessions back — background, available on the detail
#                  screen and never on the list
#   out_of_window  older than 7 sessions; stored, never surfaced
PROXIMITY_TODAY = "today"
PROXIMITY_RECENT = "recent"
PROXIMITY_OLDER_CONTEXT = "older_context"
PROXIMITY_OUT_OF_WINDOW = "out_of_window"
PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_RECENT, PROXIMITY_OLDER_CONTEXT,
               PROXIMITY_OUT_OF_WINDOW)

RECENT_MAX_SESSIONS = 3
OLDER_CONTEXT_MAX_SESSIONS = 7

#: Proximities the SYMBOL DETAIL screen may show at all.
IN_WINDOW_PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_RECENT,
                         PROXIMITY_OLDER_CONTEXT)
#: Proximities the SCANNER LIST may surface. Ancient articles stay silent.
NOTABLE_PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_RECENT)

#: Bounded payload sizes — the list row carries counts, never articles.
MAX_DETAIL_ITEMS = 8
MAX_ROW_HEADLINE_ITEMS = 1

# ---- availability (same vocabulary as app.catalyst, deliberately) ----------- #
STATUS_AVAILABLE = "available"
STATUS_UNAVAILABLE = "unavailable"
STATUS_STALE = "stale"
NEWS_STATUSES = (STATUS_AVAILABLE, STATUS_UNAVAILABLE, STATUS_STALE)

REASON_SOURCE_UNAVAILABLE = "source_unavailable"
REASON_NEVER_REFRESHED = "never_refreshed"
REASON_STALE_REFRESH = "stale_refresh"

# ---- company identity ------------------------------------------------------- #
# The frozen 25-symbol scanner universe. Used ONLY to decide whether a headline
# names the company — a fixed list for a fixed universe, not a general entity
# resolver. Each entry is the shortest form a headline actually uses.
SYMBOL_COMPANY_NAMES: Dict[str, tuple] = {
    "AAPL": ("apple",),
    "AMD": ("amd", "advanced micro devices"),
    "AMZN": ("amazon",),
    "AVGO": ("broadcom",),
    "BAC": ("bank of america",),
    "CAT": ("caterpillar",),
    "COST": ("costco",),
    "CRM": ("salesforce",),
    "CVX": ("chevron",),
    "GE": ("ge aerospace", "general electric"),
    "GOOGL": ("alphabet", "google"),
    "GS": ("goldman sachs",),
    "HD": ("home depot",),
    "JNJ": ("johnson & johnson", "johnson and johnson"),
    "JPM": ("jpmorgan", "jp morgan"),
    "LLY": ("eli lilly", "lilly"),
    "META": ("meta platforms", "meta", "facebook"),
    "MSFT": ("microsoft",),
    "NFLX": ("netflix",),
    "NVDA": ("nvidia",),
    "ORCL": ("oracle",),
    "TSLA": ("tesla",),
    "UNH": ("unitedhealth", "united health"),
    "WMT": ("walmart",),
    "XOM": ("exxon", "exxonmobil"),
}


# --------------------------------------------------------------------------- #
# normalization — the auditable half of the dedupe rule
# --------------------------------------------------------------------------- #

_PUNCT = re.compile(r"[^a-z0-9\s]+")
_WS = re.compile(r"\s+")


def normalize_title(title: Optional[str]) -> str:
    """Lowercase, drop punctuation, collapse whitespace.

    Enough to recognise the SAME story re-posted with different typography,
    and deliberately not enough to conflate two different stories that merely
    share a subject.
    """
    if not title:
        return ""
    return _WS.sub(" ", _PUNCT.sub(" ", title.lower())).strip()


def canonical_url(url: Optional[str]) -> str:
    """scheme+host+path, lowercased host, query and fragment removed.

    Syndication and tracking parameters (`?source=iedfolrf0000001`) make the
    same article look new on every refresh; stripping them does not.
    """
    if not url:
        return ""
    parts = urlsplit(url.strip())
    host = (parts.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = (parts.path or "").rstrip("/")
    if not host:
        return url.strip().lower()
    return f"{parts.scheme.lower() or 'https'}://{host}{path}"


# --------------------------------------------------------------------------- #
# classification
# --------------------------------------------------------------------------- #

def classify_scope(ticker_breadth: Optional[int]) -> str:
    """How many companies the story is about."""
    n = ticker_breadth or 0
    if n <= COMPANY_SPECIFIC_MAX_TICKERS:
        return SCOPE_COMPANY_SPECIFIC
    if n <= MULTI_COMPANY_MAX_TICKERS:
        return SCOPE_MULTI_COMPANY
    return SCOPE_MARKET_WIDE


def classify_category(title: Optional[str]) -> tuple:
    """(category, category_source) from the headline alone.

    First match wins, in the fixed order of `_CATEGORY_PATTERNS`. Returning
    `general_company_news` is a RESULT, not a failure: "we know something was
    published and we do not claim to know what kind" is true, whereas a forced
    label would not be.
    """
    text = normalize_title(title)
    if not text:
        return CATEGORY_GENERAL, CATEGORY_SOURCE_DEFAULT
    for category, pattern in _CATEGORY_PATTERNS:
        if pattern.search(text):
            return category, CATEGORY_SOURCE_DERIVED_TITLE
    return CATEGORY_GENERAL, CATEGORY_SOURCE_DEFAULT


def title_names_company(title: Optional[str], symbol: str) -> bool:
    """Does the headline itself name this company (ticker or name)?

    Ticker matching is word-bounded and case-sensitive on the raw title, so
    "GE" matches "GE Aerospace" but not the word "get"; name matching runs on
    the normalized title. Symbols outside the frozen universe fall back to the
    ticker test alone rather than silently returning False for everything.
    """
    if not title:
        return False
    if re.search(rf"(?<![A-Za-z0-9]){re.escape(symbol)}(?![A-Za-z0-9])", title):
        return True
    normalized = normalize_title(title)
    for name in SYMBOL_COMPANY_NAMES.get(symbol.upper(), ()):  # noqa: SIM110
        if normalize_title(name) in normalized:
            return True
    return False


def classify_relevance(title: Optional[str], symbol: str) -> str:
    return (RELEVANCE_PRIMARY if title_names_company(title, symbol)
            else RELEVANCE_MENTIONED)


# --------------------------------------------------------------------------- #
# market clock / session attribution
# --------------------------------------------------------------------------- #

def session_close_utc(session: date) -> datetime:
    """16:00 America/New_York on `session`, expressed in UTC (DST-correct)."""
    return datetime.combine(session, MARKET_CLOSE, tzinfo=MARKET_TZ).astimezone(
        timezone.utc)


def effective_session(published_at: datetime, *, cap_days: int = 10) -> Optional[date]:
    """The first trading session this article could have acted on.

    An article published before a trading day's close belongs to that day;
    anything published after the close rolls forward to the next trading day.
    Returns None if no trading day is found inside `cap_days` (a corrupt
    timestamp must never spin).
    """
    moment = _aware(published_at)
    cursor = moment.astimezone(MARKET_TZ).date()
    for _ in range(cap_days + 1):
        if is_trading_day(cursor) and moment <= session_close_utc(cursor):
            return cursor
        cursor = cursor + timedelta(days=1)
    return None


def is_visible_to_session(published_at: datetime, as_of_session: date) -> bool:
    """The point-in-time gate. Publication is a past public fact, so the test
    is the clock — NOT whether our ingestion had happened to run yet."""
    return _aware(published_at) <= session_close_utc(as_of_session)


def classify_proximity(sessions_ago: Optional[int]) -> str:
    """Map a non-negative trading-session distance onto the product vocabulary."""
    if sessions_ago is None or sessions_ago < 0:
        return PROXIMITY_OUT_OF_WINDOW
    if sessions_ago == 0:
        return PROXIMITY_TODAY
    if sessions_ago <= RECENT_MAX_SESSIONS:
        return PROXIMITY_RECENT
    if sessions_ago <= OLDER_CONTEXT_MAX_SESSIONS:
        return PROXIMITY_OLDER_CONTEXT
    return PROXIMITY_OUT_OF_WINDOW


# --------------------------------------------------------------------------- #
# freshness
# --------------------------------------------------------------------------- #

#: A successful refresh older than this makes the dimension `stale`. Tighter
#: than the earnings calendar's 36h: an earnings DATE stays true for weeks,
#: whereas "the latest news" going a day cold is exactly the failure that
#: would let the product present old news as current.
FRESHNESS_MAX_AGE_HOURS = 12


def evaluate_freshness(source_state: Optional[Dict[str, Any]], *,
                       now: datetime) -> Dict[str, Any]:
    """Turn the persisted `catalyst_source_state` row into an explicit verdict.

    Distinguishes three situations an empty table cannot: never refreshed, the
    source is unavailable to us, and a refresh that succeeded but went stale.
    """
    if not source_state:
        return {"status": STATUS_UNAVAILABLE, "reason": REASON_NEVER_REFRESHED,
                "last_refresh_at": None, "last_success_at": None,
                "age_hours": None, "detail": None}

    last_success = source_state.get("last_success_at")
    last_refresh = source_state.get("last_refresh_at")
    detail = source_state.get("detail")

    if source_state.get("status") != "ok" or last_success is None:
        return {"status": STATUS_UNAVAILABLE, "reason": REASON_SOURCE_UNAVAILABLE,
                "last_refresh_at": _iso(last_refresh),
                "last_success_at": _iso(last_success),
                "age_hours": None, "detail": detail}

    age = (now - _aware(last_success)).total_seconds() / 3600.0
    if age > FRESHNESS_MAX_AGE_HOURS:
        return {"status": STATUS_STALE, "reason": REASON_STALE_REFRESH,
                "last_refresh_at": _iso(last_refresh),
                "last_success_at": _iso(last_success),
                "age_hours": round(age, 1), "detail": detail}

    return {"status": STATUS_AVAILABLE, "reason": None,
            "last_refresh_at": _iso(last_refresh),
            "last_success_at": _iso(last_success),
            "age_hours": round(age, 1), "detail": detail}


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return _aware(value).isoformat()
    return None


# --------------------------------------------------------------------------- #
# selection — point-in-time honest, deduplicated, bounded
# --------------------------------------------------------------------------- #

def select_visible_articles(
    articles: Sequence[Dict[str, Any]],
    *,
    as_of_session: date,
    limit: int = MAX_DETAIL_ITEMS,
) -> List[Dict[str, Any]]:
    """Newest-first, point-in-time filtered, near-duplicate suppressed.

    THE NEAR-DUPLICATE RULE, stated in full:
      an article is dropped when an already-selected article has BOTH the same
      publisher AND the same normalized title.

    Same publisher only, on purpose. Two outlets independently covering one
    event are two pieces of evidence that it mattered, and collapsing them
    would hide exactly the signal a reader wants; the same outlet reposting its
    own headline is noise. Provider-id and canonical-URL duplicates never reach
    this function — ingestion refuses to store them at all.
    """
    prepared: List[Dict[str, Any]] = []
    for row in articles:
        published = row.get("published_at")
        if not isinstance(published, datetime):
            continue
        if not is_visible_to_session(published, as_of_session):
            continue
        session = effective_session(published)
        if session is None:
            continue
        sessions_ago = trading_sessions_between(session, as_of_session)
        proximity = classify_proximity(sessions_ago)
        if proximity == PROXIMITY_OUT_OF_WINDOW:
            continue
        prepared.append({**row, "_published": _aware(published),
                         "_session": session, "_sessions_ago": sessions_ago,
                         "_proximity": proximity})

    prepared.sort(key=lambda r: r["_published"], reverse=True)

    seen = set()
    picked: List[Dict[str, Any]] = []
    for row in prepared:
        key = ((row.get("publisher") or "").strip().lower(),
               row.get("title_normalized") or normalize_title(row.get("title")))
        if key in seen:
            continue
        seen.add(key)
        picked.append(row)
        if len(picked) >= limit:
            break
    return picked


def build_news_item(row: Dict[str, Any], *, symbol: str) -> Dict[str, Any]:
    """One product-facing article. Only fields a reader can act on or verify.

    No provider payload, no description, no sentiment, no image — the question
    is "what happened, when, who reported it, what kind of event, where do I
    read it", and nothing else belongs here.
    """
    relevance = row.get("relevance") or classify_relevance(row.get("title"), symbol)
    return {
        "published_at": _iso(row.get("published_at")),
        "session": row["_session"].isoformat() if row.get("_session") else None,
        "sessions_ago": row.get("_sessions_ago"),
        "proximity": row.get("_proximity"),
        "headline": row.get("title"),
        "publisher": row.get("publisher"),
        "url": row.get("article_url"),
        "category": row.get("category") or CATEGORY_GENERAL,
        "category_source": row.get("category_source") or CATEGORY_SOURCE_DEFAULT,
        "scope": row.get("scope") or SCOPE_COMPANY_SPECIFIC,
        "relevance": relevance,
        "notable": is_notable(row.get("_proximity"), scope=row.get("scope"),
                              relevance=relevance),
    }


def is_notable(proximity: Optional[str], *, scope: Optional[str],
               relevance: Optional[str]) -> bool:
    """The SILENCE GATE for the scanner list — not an importance score.

    All three conditions are visibility facts a reader can check: the story is
    about this company (its title names it), it is not a market-wide round-up,
    and it is close enough to this session to bear on it. Nothing here ranks
    one article above another.
    """
    return (proximity in NOTABLE_PROXIMITIES
            and scope != SCOPE_MARKET_WIDE
            and relevance == RELEVANCE_PRIMARY)


# --------------------------------------------------------------------------- #
# the product objects
# --------------------------------------------------------------------------- #

def build_news_context(
    articles: Sequence[Dict[str, Any]],
    *,
    symbol: str,
    as_of_session: Optional[date],
    freshness: Dict[str, Any],
    limit: int = MAX_DETAIL_ITEMS,
) -> Dict[str, Any]:
    """The full per-symbol news block."""
    base: Dict[str, Any] = {
        "contract_version": NEWS_CONTEXT_CONTRACT_VERSION,
        "status": freshness.get("status"),
        "reason": freshness.get("reason"),
        "as_of_session": as_of_session.isoformat() if as_of_session else None,
        "last_refresh_at": freshness.get("last_refresh_at"),
        "last_success_at": freshness.get("last_success_at"),
        "age_hours": freshness.get("age_hours"),
        "detail": freshness.get("detail"),
        "window_sessions": OLDER_CONTEXT_MAX_SESSIONS,
        "in_window_count": 0,
        "notable_count": 0,
        "latest_published_at": None,
        "top_category": None,
        "items": [],
    }

    if freshness.get("status") == STATUS_UNAVAILABLE or as_of_session is None:
        if as_of_session is None:
            base["status"] = STATUS_UNAVAILABLE
            base["reason"] = base["reason"] or REASON_NEVER_REFRESHED
        return base

    picked = select_visible_articles(articles, as_of_session=as_of_session,
                                     limit=limit)
    items = [build_news_item(row, symbol=symbol) for row in picked]
    notable = [i for i in items if i["notable"]]

    base["items"] = items
    base["in_window_count"] = len(items)
    base["notable_count"] = len(notable)
    base["latest_published_at"] = items[0]["published_at"] if items else None
    # The category of the newest NOTABLE item — a label for what is nearby, not
    # a summary of everything. Absent when nothing is notable, so the UI has
    # nothing to print rather than something vague.
    base["top_category"] = notable[0]["category"] if notable else None
    return base


def build_row_news(news_context: Dict[str, Any]) -> Dict[str, Any]:
    """The COMPACT subset a list row carries — counts, never articles.

    `notable_count` is the gate the UI uses to stay silent. A row must never
    carry "no news": the absence of a headline is not a finding, and printing
    it on 25 rows would turn an empty feed into a claim.
    """
    items = news_context.get("items") or []
    notable = [i for i in items if i.get("notable")]
    return {
        "status": news_context.get("status"),
        "reason": news_context.get("reason"),
        "notable_count": news_context.get("notable_count", 0),
        "in_window_count": news_context.get("in_window_count", 0),
        "top_category": news_context.get("top_category"),
        "latest_published_at": (notable[0]["published_at"] if notable
                                else None),
        "latest_proximity": notable[0]["proximity"] if notable else None,
        "latest_headline": (notable[0]["headline"]
                            if notable[:MAX_ROW_HEADLINE_ITEMS] else None),
    }


def empty_news_context(*, symbol: str = "",
                       reason: str = REASON_NEVER_REFRESHED) -> Dict[str, Any]:
    """A fully-unavailable block.

    Used when news loading fails entirely: the scanner keeps working and every
    news field says `unavailable` rather than the request failing.
    """
    return build_news_context(
        [], symbol=symbol, as_of_session=None,
        freshness={"status": STATUS_UNAVAILABLE, "reason": reason,
                   "last_refresh_at": None, "last_success_at": None,
                   "age_hours": None, "detail": None})


__all__ = [
    "NEWS_CONTEXT_CONTRACT_VERSION", "SOURCE_COMPANY_NEWS",
    "SCOPE_COMPANY_SPECIFIC", "SCOPE_MULTI_COMPANY", "SCOPE_MARKET_WIDE",
    "SCOPES", "COMPANY_SPECIFIC_MAX_TICKERS", "MULTI_COMPANY_MAX_TICKERS",
    "RELEVANCE_PRIMARY", "RELEVANCE_MENTIONED", "RELEVANCES",
    "CATEGORIES", "CATEGORY_GENERAL", "CATEGORY_SOURCES",
    "CATEGORY_SOURCE_DERIVED_TITLE", "CATEGORY_SOURCE_DEFAULT",
    "PROXIMITY_TODAY", "PROXIMITY_RECENT", "PROXIMITY_OLDER_CONTEXT",
    "PROXIMITY_OUT_OF_WINDOW", "PROXIMITIES", "NOTABLE_PROXIMITIES",
    "IN_WINDOW_PROXIMITIES", "RECENT_MAX_SESSIONS",
    "OLDER_CONTEXT_MAX_SESSIONS", "MAX_DETAIL_ITEMS",
    "STATUS_AVAILABLE", "STATUS_UNAVAILABLE", "STATUS_STALE", "NEWS_STATUSES",
    "REASON_SOURCE_UNAVAILABLE", "REASON_NEVER_REFRESHED",
    "REASON_STALE_REFRESH", "FRESHNESS_MAX_AGE_HOURS",
    "SYMBOL_COMPANY_NAMES", "MARKET_TZ", "MARKET_CLOSE",
    "normalize_title", "canonical_url", "classify_scope", "classify_category",
    "title_names_company", "classify_relevance", "session_close_utc",
    "effective_session", "is_visible_to_session", "classify_proximity",
    "evaluate_freshness", "select_visible_articles", "build_news_item",
    "is_notable", "build_news_context", "build_row_news", "empty_news_context",
]
