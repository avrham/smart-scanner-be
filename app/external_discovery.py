"""Market-wide discovery candidates — the answer to "what else should we look at?"

Bounded FMP ingestion plus the pure normalisation around it. Shaped like the
other ingestion modules: an open connection in, a bounded summary out, no
scheduling of its own.

WHY THIS EXISTS AT ALL
----------------------
The scanner deliberately holds a frozen 25-symbol universe, which is what
makes the prospective experiment interpretable. The cost of that choice is a
specific blind spot: the scanner cannot tell whether any of its 25 symbols is
in the market's attention cohort today, and it can never surface a symbol it
does not already hold. That is not a bug to fix in the strategy — it is a
question a different data source answers cheaply.

WHY IT IS NOT AN `external_signals` SOURCE
-------------------------------------------
A mover is not a claim. An indicator saying "bullish on the 4H" is somebody's
opinion; "third largest volume today" is a measurement anyone with the whole
tape can make. Feeding movers into the confluence reading would make "it moved
a lot" count as a source agreeing with the scanner, which is not what
agreement is supposed to mean. Separate table, separate module, no confluence.

MEASURED ENTITLEMENT (2026-08-28, against the live key)
--------------------------------------------------------
    /api/v3/*                 403  "Legacy Endpoint" — the base URL this
                                   repository's existing FMP provider still
                                   uses is DEAD.
    /stable/biggest-gainers   200  free tier
    /stable/biggest-losers    200  free tier
    /stable/most-actives      200  free tier
    /stable/company-screener  402  requires a paid tier

Only the three that answer are implemented. The screener is not modelled at
all rather than being written speculatively against an entitlement we do not
hold — an adapter that cannot receive data reads as coverage.

Also measured: most symbol-scoped FMP endpoints on the free tier are limited
to a fixed undisclosed symbol whitelist. The three feeds used here are NOT
symbol-scoped, which is exactly why they work.

LICENCE — WHY THIS DATA STOPS AT THE DATABASE
---------------------------------------------
FMP's individual plans are licensed for personal, non-commercial use and
forbid integrating the data into tools accessible by third parties; displaying
or redistributing it requires a separate agreement. So this module writes to
the database and `ops/analysis` reads it, and NOTHING here is exposed through
the Product API or the UI. Ingesting for our own research is a different act
from publishing, and the code boundary matches the licence boundary rather
than relying on someone remembering the distinction later.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import httpx

from app.news import effective_session

#: The registry source this dimension reports as.
SOURCE_FMP = "fmp"

#: `catalyst_source_state.source`. Namespaced like every external source.
SOURCE_STATE_FMP_DISCOVERY = "external_fmp_discovery"

# ---- lists ------------------------------------------------------------------ #
LIST_TOP_GAINERS = "top_gainers"
LIST_TOP_LOSERS = "top_losers"
LIST_MOST_ACTIVE = "most_active"
LIST_KINDS = (LIST_TOP_GAINERS, LIST_TOP_LOSERS, LIST_MOST_ACTIVE)

#: The CURRENT base. `/api/v3` returns HTTP 403 "Legacy Endpoint" and is not
#: used here — see the module docstring. Deliberately not read from
#: `settings.FMP_BASE_URL`: that value still points at the dead v3 host and is
#: shared with the market-data provider, which this module must not disturb.
FMP_STABLE_BASE_URL = "https://financialmodelingprep.com/stable"

ENDPOINTS: Dict[str, str] = {
    LIST_TOP_GAINERS: "biggest-gainers",
    LIST_TOP_LOSERS: "biggest-losers",
    LIST_MOST_ACTIVE: "most-actives",
}

#: How many ranks to keep per list. The tail of a movers list is sub-dollar
#: microcaps; the product question is "what is the market watching", and the
#: answer lives at the top. Bounded so ingestion can never become an archive.
DEFAULT_LIST_LIMIT = 25

#: Spacing between the three requests. The free tier is a daily-call budget
#: rather than a rate limit, but three requests once a day should still not
#: arrive as a burst.
REQUEST_INTERVAL_SECONDS = 1.0

# ---- source state (same vocabulary as every other ingestion here) ----------- #
STATE_OK = "ok"
STATE_UNAVAILABLE = "unavailable"
STATE_ERROR = "error"
STATE_NEVER_RUN = "never_run"


class DiscoverySourceUnavailable(Exception):
    """The provider could not be read, or we are not entitled to the endpoint.

    Carries a short, secret-free reason. `not_entitled` is a first-class
    outcome rather than an error: it is the honest answer for a plan that does
    not include a feed, and it must not look like an outage.
    """

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


class FmpDiscoveryClient:
    """A deliberately small FMP client for the three entitled movers feeds.

    Separate from `app/workers/fmp_client.py` on purpose. That one is the
    market-data provider path, still pinned to the dead `/api/v3` base, and
    repointing it is a change to the price pipeline that this milestone has no
    business making. This client exists so the discovery path can use the
    current base URL without touching the provider.
    """

    def __init__(self, api_key: str, *,
                 base_url: str = FMP_STABLE_BASE_URL,
                 interval_seconds: float = REQUEST_INTERVAL_SECONDS,
                 timeout: float = 30.0):
        key = (api_key or "").strip()
        if not key:
            # Refused here rather than sent as an empty parameter, so a
            # deployment with no FMP key reports `unavailable` instead of
            # producing a confusing 401 from the provider.
            raise DiscoverySourceUnavailable(
                "missing_api_key", "no FMP credential is configured")
        self._api_key = key
        self.base_url = base_url.rstrip("/")
        self.interval_seconds = interval_seconds
        self.timeout = timeout

    async def get_list(self, path: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params={"apikey": self._api_key})
        if response.status_code == 401:
            raise DiscoverySourceUnavailable(
                "unauthorized", "the FMP credential was rejected")
        if response.status_code == 402:
            # Not an outage. The plan simply does not include this feed, and
            # saying so precisely is what stops someone re-testing it monthly.
            raise DiscoverySourceUnavailable(
                "not_entitled", "the current FMP plan does not include this feed")
        if response.status_code == 403:
            raise DiscoverySourceUnavailable(
                "legacy_endpoint",
                "FMP rejected the endpoint as legacy — check the base URL")
        if response.status_code == 429:
            raise DiscoverySourceUnavailable(
                "rate_limited", "FMP returned HTTP 429")
        if response.status_code != 200:
            raise DiscoverySourceUnavailable(
                "provider_unavailable", f"FMP returned HTTP {response.status_code}")
        payload = response.json()
        if not isinstance(payload, list):
            raise DiscoverySourceUnavailable(
                "unexpected_payload", "the feed was not a list")
        return payload

    async def pause(self) -> None:
        await asyncio.sleep(self.interval_seconds)


# --------------------------------------------------------------------------- #
# normalisation — pure, so it is fully testable without the network
# --------------------------------------------------------------------------- #

def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any, *, limit: int = 128) -> Optional[str]:
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def normalize_symbol(value: Any) -> Optional[str]:
    """Uppercased ticker, or None when it is not shaped like one.

    The pattern matches the DB CHECK constraint exactly. FMP's movers feeds
    include foreign listings and warrants whose symbols carry characters we do
    not model; those are DROPPED rather than mangled into something storable.
    """
    symbol = (_text(value, limit=24) or "").upper()
    if not symbol or not symbol[0].isalpha() or len(symbol) > 16:
        return None
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-")
    return symbol if set(symbol) <= allowed else None


def normalize_candidate(row: Dict[str, Any], *, list_kind: str, rank: int,
                        observed_at: datetime, session_date: date,
                        universe: Optional[Set[str]] = None,
                        ) -> Optional[Dict[str, Any]]:
    """One feed entry -> one storable row, or None if the symbol is unusable."""
    symbol = normalize_symbol(row.get("symbol"))
    if symbol is None:
        return None
    return {
        "source": SOURCE_FMP,
        "list_kind": list_kind,
        "symbol": symbol,
        "company_name": _text(row.get("name")),
        "exchange": _text(row.get("exchange"), limit=32),
        "rank": rank,
        "price": _number(row.get("price")),
        "change_amount": _number(row.get("change")),
        # The feed spells this `changesPercentage` on some endpoints and
        # `changePercentage` on others; both are accepted rather than trusting
        # one spelling and silently storing NULL when it is the other.
        "change_percent": _number(row.get("changesPercentage")
                                  if row.get("changesPercentage") is not None
                                  else row.get("changePercentage")),
        "observed_at": observed_at,
        "session_date": session_date,
        "in_scanner_universe": bool(universe and symbol in universe),
    }


def normalize_list(rows: Sequence[Dict[str, Any]], *, list_kind: str,
                   observed_at: datetime, session_date: date,
                   universe: Optional[Set[str]] = None,
                   limit: int = DEFAULT_LIST_LIMIT) -> List[Dict[str, Any]]:
    """A whole feed -> bounded, ranked, de-duplicated rows.

    Rank is assigned from the STORED position, not the feed index, so dropping
    an unusable symbol does not leave a hole in the ranking.
    """
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for row in rows:
        if len(out) >= limit:
            break
        if not isinstance(row, dict):
            continue
        candidate = normalize_candidate(
            row, list_kind=list_kind, rank=len(out) + 1,
            observed_at=observed_at, session_date=session_date,
            universe=universe)
        if candidate is None or candidate["symbol"] in seen:
            continue
        seen.add(candidate["symbol"])
        out.append(candidate)
    return out


def resolve_session(observed_at: datetime) -> Optional[date]:
    """The trading session this observation belongs to.

    Borrowed from `app.news` so discovery cannot disagree with the catalyst and
    external layers about when a session ends.
    """
    return effective_session(observed_at)


# --------------------------------------------------------------------------- #
# persistence
# --------------------------------------------------------------------------- #

UPSERT_SQL = """
INSERT INTO public.external_discovery_candidates (
    source, list_kind, symbol, company_name, exchange, rank, price,
    change_amount, change_percent, observed_at, session_date,
    in_scanner_universe)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (source, list_kind, symbol, session_date) DO UPDATE SET
    company_name = COALESCE(EXCLUDED.company_name,
                            external_discovery_candidates.company_name),
    exchange = COALESCE(EXCLUDED.exchange,
                        external_discovery_candidates.exchange),
    rank = EXCLUDED.rank,
    price = EXCLUDED.price,
    change_amount = EXCLUDED.change_amount,
    change_percent = EXCLUDED.change_percent,
    observed_at = EXCLUDED.observed_at,
    in_scanner_universe = EXCLUDED.in_scanner_universe
RETURNING (xmax = 0) AS inserted
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


async def upsert_candidates(conn, candidates: Iterable[Dict[str, Any]],
                            ) -> Dict[str, int]:
    """Idempotent write. A re-run within one session updates ranks.

    `inserted` and `updated` are counted separately: a single "written" total
    would hide the difference between a first fetch and a re-fetch, which is
    exactly what tells you whether the cadence is working.
    """
    stats = {"seen": 0, "inserted": 0, "updated": 0}
    for candidate in candidates:
        stats["seen"] += 1
        row = await conn.fetchrow(
            UPSERT_SQL,
            candidate["source"], candidate["list_kind"], candidate["symbol"],
            candidate.get("company_name"), candidate.get("exchange"),
            candidate["rank"], candidate.get("price"),
            candidate.get("change_amount"), candidate.get("change_percent"),
            candidate["observed_at"], candidate["session_date"],
            candidate["in_scanner_universe"])
        stats["inserted" if row["inserted"] else "updated"] += 1
    return stats


async def record_source_state(conn, status: str, *, symbols_covered: int = 0,
                              written: int = 0, detail: str = "",
                              now: Optional[datetime] = None) -> None:
    """One row in `catalyst_source_state`, so an empty discovery table can
    never be misread as 'the market was quiet today'."""
    moment = now or datetime.now(timezone.utc)
    await conn.execute(
        SOURCE_STATE_SQL, SOURCE_STATE_FMP_DISCOVERY, status, moment,
        moment if status == STATE_OK else None,
        symbols_covered, written, detail[:400] or None)


async def refresh_discovery_candidates(
    conn, client: Optional[FmpDiscoveryClient], *,
    universe: Optional[Set[str]] = None,
    now: Optional[datetime] = None,
    limit: int = DEFAULT_LIST_LIMIT,
    list_kinds: Sequence[str] = LIST_KINDS,
) -> Dict[str, Any]:
    """One idempotent refresh over the entitled movers feeds.

    A source failure is absorbed into `catalyst_source_state` and reported,
    never raised: an FMP outage — or, far more commonly, no FMP key at all —
    must cost the product this dimension and nothing else. `client=None` is the
    ordinary no-credential case and is reported as `unavailable`, not an error.

    Each list is fetched independently so one unentitled feed cannot cost the
    other two their refresh.
    """
    moment = now or datetime.now(timezone.utc)
    session = resolve_session(moment)
    summary: Dict[str, Any] = {"source": SOURCE_STATE_FMP_DISCOVERY,
                               "session_date": session.isoformat() if session else None,
                               "lists": {}}

    if client is None:
        await record_source_state(conn, STATE_UNAVAILABLE,
                                  detail="missing_api_key", now=moment)
        summary.update({"status": STATE_UNAVAILABLE, "reason": "missing_api_key"})
        return summary
    if session is None:
        await record_source_state(conn, STATE_ERROR,
                                  detail="unresolved_session", now=moment)
        summary.update({"status": STATE_ERROR, "reason": "unresolved_session"})
        return summary

    total = {"seen": 0, "inserted": 0, "updated": 0}
    failures: List[str] = []
    symbols: Set[str] = set()

    for kind in list_kinds:
        path = ENDPOINTS.get(kind)
        if path is None:
            continue
        try:
            await client.pause()
            rows = await client.get_list(path)
            candidates = normalize_list(
                rows, list_kind=kind, observed_at=moment, session_date=session,
                universe=universe, limit=limit)
            stats = await upsert_candidates(conn, candidates)
            for candidate in candidates:
                symbols.add(candidate["symbol"])
            for key in total:
                total[key] += stats[key]
            summary["lists"][kind] = stats
        except DiscoverySourceUnavailable as exc:
            failures.append(f"{kind}:{exc.reason}")
            summary["lists"][kind] = {"status": STATE_UNAVAILABLE,
                                      "reason": exc.reason}
        except Exception as exc:
            failures.append(f"{kind}:{type(exc).__name__}")
            summary["lists"][kind] = {"status": STATE_ERROR,
                                      "reason": type(exc).__name__}

    if total["seen"] == 0:
        await record_source_state(conn, STATE_UNAVAILABLE,
                                  detail="; ".join(failures) or "no rows",
                                  now=moment)
        summary.update({"status": STATE_UNAVAILABLE, "failures": failures})
        return summary

    # Partial success is reported as success WITH the failures listed, rather
    # than as a failure: two working feeds out of three is genuinely more
    # useful than none, and hiding which one broke would make it undiagnosable.
    await record_source_state(conn, STATE_OK, symbols_covered=len(symbols),
                              written=total["inserted"],
                              detail="; ".join(failures), now=moment)
    summary.update({"status": STATE_OK, "failures": failures,
                    "distinct_symbols": len(symbols), **total})
    return summary


# --------------------------------------------------------------------------- #
# research reads (ops/analysis only — never the Product API; see the docstring)
# --------------------------------------------------------------------------- #

OUTSIDE_UNIVERSE_SQL = """
SELECT symbol,
       count(DISTINCT session_date) AS sessions_seen,
       count(*) AS appearances,
       array_agg(DISTINCT list_kind ORDER BY list_kind) AS lists,
       min(rank) AS best_rank,
       max(session_date) AS last_seen
FROM public.external_discovery_candidates
WHERE in_scanner_universe = false
  AND session_date >= $1
GROUP BY symbol
ORDER BY sessions_seen DESC, appearances DESC, best_rank ASC
LIMIT $2
"""


async def symbols_worth_investigating(conn, *, since: date,
                                      limit: int = 25) -> List[Dict[str, Any]]:
    """Symbols the wider market kept noticing that our universe never scans.

    Ordered by PERSISTENCE first (how many distinct sessions it appeared on),
    not by the size of any single move. A stock that gapped 200% once is noise;
    one that has been in the attention cohort four sessions running is a
    question. This ordering is a research convenience and feeds nothing — no
    scanner path reads this function, and no symbol reaches the frozen universe
    through it.
    """
    rows = await conn.fetch(OUTSIDE_UNIVERSE_SQL, since, limit)
    return [dict(r) for r in rows]


__all__ = [
    "SOURCE_FMP", "SOURCE_STATE_FMP_DISCOVERY",
    "LIST_TOP_GAINERS", "LIST_TOP_LOSERS", "LIST_MOST_ACTIVE", "LIST_KINDS",
    "FMP_STABLE_BASE_URL", "ENDPOINTS", "DEFAULT_LIST_LIMIT",
    "REQUEST_INTERVAL_SECONDS",
    "STATE_OK", "STATE_UNAVAILABLE", "STATE_ERROR", "STATE_NEVER_RUN",
    "DiscoverySourceUnavailable", "FmpDiscoveryClient",
    "normalize_symbol", "normalize_candidate", "normalize_list",
    "resolve_session", "upsert_candidates", "record_source_state",
    "refresh_discovery_candidates", "symbols_worth_investigating",
]
