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

Wave 2 made that boundary explicit rather than merely observed: every row
carries the licence class it was collected under, and the Product API's
database role holds no privilege on this table at all. See
`app/source_licensing.py`.

WAVE 2 ADDITIONS
----------------
`aggregate_discovery` rolls a session up per symbol while keeping EVERY reason
it was noticed — "top gainer AND most active" stays two facts a reader can
check, and deliberately never becomes a 2, then a score, then an ordering
somebody trades. `cross_reference_universe` answers the question this whole
path exists for: of what the market noticed today, how much is inside our
frozen 25, how much is outside it, and how much we could not study even if we
wanted to because we hold no history for it.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import httpx

from app.news import effective_session
from app.source_licensing import LICENSING_INTERNAL_ONLY, resolve_visibility

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

    async def get_list(self, path: str,
                       params: Optional[Dict[str, Any]] = None,
                       ) -> List[Dict[str, Any]]:
        """GET one /stable feed as a list.

        `params` are merged with the credential rather than appended to the
        path, because httpx REPLACES a URL's query string when `params` is
        given — a symbol spelled into the path would have been silently
        discarded and every caller would have got the market-wide feed.
        """
        url = f"{self.base_url}/{path}"
        query = {"apikey": self._api_key}
        query.update({k: v for k, v in (params or {}).items() if v is not None})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params=query)
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


#: The same client, under a name that does not claim discovery owns it.
#: Wave 2's analyst path speaks to the identical base URL with the identical
#: credential and the identical error vocabulary (402 = not entitled, 403 =
#: legacy endpoint), so giving it a second copy would mean two places to fix
#: when FMP next changes an error code.
FmpStableClient = FmpDiscoveryClient


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
                        reference_session_date: Optional[date] = None,
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
        # THE FETCH CLOCK — the only timestamp with an authority behind it.
        "observed_at": observed_at,
        # WHEN IT COULD FIRST BE ACTED UPON. Forward-rolling.
        "session_date": session_date,
        # WHICH SESSION THE NUMBERS DESCRIBE. Inferred, and labelled as
        # inferred. Falls back to the observation itself rather than to
        # `session_date`, so a caller that forgets to pass it can never make
        # the row claim the numbers came from a session that had not happened.
        "reference_session_date": (reference_session_date
                                   if reference_session_date is not None
                                   else infer_reference_session(observed_at)),
        "reference_session_basis": BASIS_INFERRED_FROM_OBSERVATION_TIME,
        "in_scanner_universe": bool(universe and symbol in universe),
        # The provider's own row, bounded. Kept because a rank means nothing
        # without the numbers it was assigned from, and because a feed that
        # quietly changes its field names must be diagnosable after the fact
        # rather than only while someone is watching.
        "source_metadata": bound_source_row(row),
        # The licence class AS IT WAS AT INGESTION. Denormalised from the
        # registry on purpose: if a display licence is ever acquired, the rows
        # collected under the old terms stay identifiable.
        "licensing_visibility": resolve_visibility(SOURCE_FMP),
    }


#: Fields worth keeping from a movers row, and nothing else. A whole raw
#: payload in a JSONB column becomes a place for unbounded provider text to
#: accumulate; this keeps it to the numbers the rank was derived from.
_METADATA_KEYS = ("price", "change", "changesPercentage", "changePercentage",
                  "volume", "avgVolume", "marketCap", "exchange", "name")


def bound_source_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in _METADATA_KEYS:
        value = row.get(key)
        if value is None or isinstance(value, (dict, list)):
            continue
        out[key] = value if isinstance(value, (int, float, bool)) \
            else str(value)[:64]
    return out


def normalize_list(rows: Sequence[Dict[str, Any]], *, list_kind: str,
                   observed_at: datetime, session_date: date,
                   reference_session_date: Optional[date] = None,
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
            reference_session_date=reference_session_date, universe=universe)
        if candidate is None or candidate["symbol"] in seen:
            continue
        seen.add(candidate["symbol"])
        out.append(candidate)
    return out


# --------------------------------------------------------------------------- #
# TWO DATES, TWO QUESTIONS. They are not interchangeable and one cannot be
# derived from the other.
#
#   session_date            "when could this observation FIRST BE ACTED UPON?"
#                           Forward-rolling: a Sunday fetch is actionable on
#                           Monday. This is the conservative anchor.
#
#   reference_session_date  "which market session do these NUMBERS describe?"
#                           Backward-looking: a Sunday fetch is describing
#                           Friday's tape.
#
# They differ by a session whenever the fetch happens outside a live session,
# which is most of the time. Before this existed, only the first was stored,
# and a snapshot of Friday's closing movers was labelled 2026-08-31 — a
# session that had not happened.
# --------------------------------------------------------------------------- #

#: How `reference_session_date` was arrived at. A bounded vocabulary with ONE
#: value today, and that value says exactly what it is: our inference from our
#: own fetch clock against the US equity calendar.
#:
#: It exists because FMP's movers feeds carry NO provider timestamp — measured,
#: and stated in migration 023. Naming the column `describes_session` with no
#: basis beside it would read as though the provider had declared the session,
#: which would be a claim we cannot support.
BASIS_INFERRED_FROM_OBSERVATION_TIME = "inferred_from_observation_time"
REFERENCE_SESSION_BASES = (BASIS_INFERRED_FROM_OBSERVATION_TIME,)


def resolve_session(observed_at: datetime) -> Optional[date]:
    """The first trading session this observation could be ACTED UPON.

    Forward-rolling, and borrowed from `app.news` so discovery cannot disagree
    with the catalyst and external layers about when a session ends. A fetch at
    11:00 on a Sunday returns the following Monday: that is the first moment
    anybody could trade on it, and it is deliberately NOT a claim about which
    session the data describes — see `infer_reference_session` for that.
    """
    return effective_session(observed_at)


def infer_reference_session(observed_at: datetime) -> Optional[date]:
    """The market session these numbers are inferred to describe.

    INFERENCE, not provider metadata. The feeds carry no timestamp, so the
    honest reconstruction is our own fetch clock read against the US equity
    calendar:

        before 09:30 ET on a trading day  -> the latest COMPLETED session
                                             (the tape has not moved yet today)
        09:30-16:00 ET on a trading day   -> that session, in progress
        after 16:00 ET on a trading day   -> that session, just completed
        weekend or market holiday         -> the latest COMPLETED session

    The two boundary cases are the ones that were previously wrong and are the
    ones worth stating: a pre-open fetch describes YESTERDAY, and a weekend
    fetch describes FRIDAY. Both were being labelled with the next session.
    """
    from app.prospective_session import (REGULAR_OPEN, EXCHANGE_TZ,
                                         is_trading_day,
                                         resolve_latest_completed_session)
    from zoneinfo import ZoneInfo

    moment = (observed_at if observed_at.tzinfo
              else observed_at.replace(tzinfo=timezone.utc))
    local = moment.astimezone(ZoneInfo(EXCHANGE_TZ))
    if is_trading_day(local.date()) and local.time() >= REGULAR_OPEN:
        # In progress, or closed a few hours ago. Either way the numbers are
        # this session's, and this session is the one being described.
        return local.date()
    # Pre-open, weekend or holiday: nothing has traded since the last close.
    return resolve_latest_completed_session(moment)


# --------------------------------------------------------------------------- #
# persistence
# --------------------------------------------------------------------------- #

UPSERT_SQL = """
INSERT INTO public.external_discovery_candidates (
    source, list_kind, symbol, company_name, exchange, rank, price,
    change_amount, change_percent, observed_at, session_date,
    reference_session_date, reference_session_basis,
    in_scanner_universe, source_metadata, licensing_visibility)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16)
-- Identity WIDENED by reference_session_date, not changed. Adding a column to
-- a unique key can only ever permit more rows, never merge existing ones — so
-- this cannot lose data, and it restores what migration 023's comment always
-- said the key meant ("a second fetch on the same session updates the rank").
-- Without it, a pre-open fetch describing Thursday and an in-session fetch
-- describing Friday collide on one actionable session and the second silently
-- overwrites the first's provenance.
ON CONFLICT (source, list_kind, symbol, session_date, reference_session_date)
DO UPDATE SET
    company_name = COALESCE(EXCLUDED.company_name,
                            external_discovery_candidates.company_name),
    exchange = COALESCE(EXCLUDED.exchange,
                        external_discovery_candidates.exchange),
    rank = EXCLUDED.rank,
    price = EXCLUDED.price,
    change_amount = EXCLUDED.change_amount,
    change_percent = EXCLUDED.change_percent,
    observed_at = EXCLUDED.observed_at,
    in_scanner_universe = EXCLUDED.in_scanner_universe,
    source_metadata = EXCLUDED.source_metadata,
    licensing_visibility = EXCLUDED.licensing_visibility
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
            candidate["reference_session_date"],
            candidate["reference_session_basis"],
            candidate["in_scanner_universe"],
            json.dumps(candidate.get("source_metadata") or {}),
            candidate.get("licensing_visibility") or LICENSING_INTERNAL_ONLY)
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
    reference = infer_reference_session(moment)
    summary: Dict[str, Any] = {
        "source": SOURCE_STATE_FMP_DISCOVERY,
        # BOTH reported, because a summary that showed one would be the same
        # ambiguity in a log line instead of in a column.
        "session_date": session.isoformat() if session else None,
        "reference_session_date": reference.isoformat() if reference else None,
        "reference_session_basis": BASIS_INFERRED_FROM_OBSERVATION_TIME,
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
                reference_session_date=reference, universe=universe,
                limit=limit)
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

#: Counts MARKET SESSIONS, not actionable sessions. "How many sessions did the
#: market keep noticing this" is a question about the tape; counting
#: `session_date` answered a different one and, on a weekend, counted a session
#: that had not happened.
OUTSIDE_UNIVERSE_SQL = """
SELECT symbol,
       count(DISTINCT reference_session_date) AS sessions_seen,
       count(*) AS appearances,
       array_agg(DISTINCT list_kind ORDER BY list_kind) AS lists,
       min(rank) AS best_rank,
       max(reference_session_date) AS last_seen
FROM public.external_discovery_candidates
WHERE in_scanner_universe = false
  AND reference_session_date IS NOT NULL
  AND reference_session_date >= $1
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


# --------------------------------------------------------------------------- #
# backfill — migration 025
#
# Lives here, not in the migration, because the inference needs the US equity
# trading calendar and that calendar exists exactly once in this repository.
# A second copy written in SQL could drift from the first without anybody
# noticing, and a drifting calendar is precisely how a row ends up labelled
# with a session that did not happen.
# --------------------------------------------------------------------------- #

BACKFILL_SELECT_SQL = """
SELECT id, symbol, list_kind, observed_at, session_date
FROM public.external_discovery_candidates
WHERE reference_session_date IS NULL
ORDER BY observed_at, symbol, list_kind
"""

BACKFILL_UPDATE_SQL = """
UPDATE public.external_discovery_candidates
SET reference_session_date = $2, reference_session_basis = $3
WHERE id = $1 AND reference_session_date IS NULL
"""


async def plan_reference_backfill(conn) -> List[Dict[str, Any]]:
    """Every unfilled row with the session it WOULD be given. Reads only.

    Returned rather than applied so the mapping can be printed and checked
    before anything is written. `unsafe` marks a row whose inference would land
    after its own actionable session — impossible by construction, checked
    anyway, because a silent bad backfill is worse than the missing column.
    """
    rows = []
    for record in await conn.fetch(BACKFILL_SELECT_SQL):
        inferred = infer_reference_session(record["observed_at"])
        rows.append({
            "id": record["id"],
            "symbol": record["symbol"],
            "list_kind": record["list_kind"],
            "observed_at": record["observed_at"],
            "session_date": record["session_date"],
            "reference_session_date": inferred,
            "basis": BASIS_INFERRED_FROM_OBSERVATION_TIME,
            "unsafe": inferred is None or inferred > record["session_date"],
        })
    return rows


async def backfill_reference_sessions(conn, *, dry_run: bool = True,
                                      ) -> Dict[str, Any]:
    """Apply the planned mapping. Idempotent: only ever fills NULLs.

    Refuses to write anything at all if ANY row plans an unsafe value — a
    partial backfill would leave the table in a state where some rows are
    trustworthy and some are not, which is harder to reason about than none.
    """
    plan = await plan_reference_backfill(conn)
    unsafe = [r for r in plan if r["unsafe"]]
    summary: Dict[str, Any] = {
        "candidates": len(plan),
        "unsafe": len(unsafe),
        "dry_run": dry_run,
        "updated": 0,
        "distinct_mappings": sorted({
            (str(r["observed_at"]), str(r["reference_session_date"]),
             str(r["session_date"])) for r in plan}),
    }
    if unsafe or dry_run:
        return summary
    for row in plan:
        await conn.execute(BACKFILL_UPDATE_SQL, row["id"],
                           row["reference_session_date"], row["basis"])
        summary["updated"] += 1
    return summary


# --------------------------------------------------------------------------- #
# A3 — aggregation. One symbol, EVERY reason it was noticed.
# --------------------------------------------------------------------------- #

def aggregate_discovery(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Per-symbol rollup of one session's candidate rows. Pure.

    Every reason is preserved as a list. The temptation here is to turn "top
    gainer AND most active" into a 2, and then into a score, and then into an
    ordering somebody trades — so the rollup deliberately stops at the facts:
    which lists, best rank, largest absolute move. A reader can say "CRM
    appeared in top gainers and most actives" and check it; nothing here says
    what that is worth.

    Ordered by how many distinct lists noticed the symbol, then by best rank.
    That is a display order for a research report, not a ranking of quality,
    and no scanner path consumes it.
    """
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = row.get("symbol")
        if not symbol:
            continue
        entry = grouped.setdefault(symbol, {
            "symbol": symbol,
            "company_name": row.get("company_name"),
            "exchange": row.get("exchange"),
            "in_scanner_universe": False,
            "reasons": [],
            "best_rank": None,
            "max_abs_change_percent": None,
            "observed_at": None,
            # Both dates survive the rollup. Collapsing to one is what the
            # audit found; a summary that keeps only the actionable session
            # would reintroduce it one layer up.
            "reference_session_date": None,
            "first_actionable_session": None,
        })
        kind = row.get("list_kind")
        if kind and kind not in entry["reasons"]:
            entry["reasons"].append(kind)
        entry["in_scanner_universe"] = (entry["in_scanner_universe"]
                                        or bool(row.get("in_scanner_universe")))
        rank = row.get("rank")
        if isinstance(rank, int) and (entry["best_rank"] is None
                                      or rank < entry["best_rank"]):
            entry["best_rank"] = rank
        change = row.get("change_percent")
        if isinstance(change, (int, float)):
            magnitude = abs(float(change))
            if (entry["max_abs_change_percent"] is None
                    or magnitude > entry["max_abs_change_percent"]):
                entry["max_abs_change_percent"] = magnitude
        observed = row.get("observed_at")
        if observed is not None and (entry["observed_at"] is None
                                     or observed > entry["observed_at"]):
            entry["observed_at"] = observed
        entry["company_name"] = entry["company_name"] or row.get("company_name")
        entry["exchange"] = entry["exchange"] or row.get("exchange")
        reference = row.get("reference_session_date")
        if reference is not None and (entry["reference_session_date"] is None
                                      or reference > entry["reference_session_date"]):
            entry["reference_session_date"] = reference
        actionable = row.get("session_date")
        if actionable is not None and (entry["first_actionable_session"] is None
                                       or actionable < entry["first_actionable_session"]):
            entry["first_actionable_session"] = actionable

    out = list(grouped.values())
    for entry in out:
        entry["reasons"].sort()
        entry["reason_count"] = len(entry["reasons"])
    out.sort(key=lambda e: (-e["reason_count"],
                            e["best_rank"] if e["best_rank"] is not None else 999,
                            e["symbol"]))
    return out


CURRENT_DISCOVERY_SQL = """
SELECT reference_session_date, symbol, company_name, exchange,
       in_scanner_universe, reasons, reason_count, best_rank,
       max_abs_change_percent, observed_at, first_actionable_session,
       reference_session_basis, licensing_visibility
FROM public.external_discovery_current
WHERE reference_session_date = $1
ORDER BY reason_count DESC, best_rank ASC, symbol
"""

#: The latest MARKET SESSION any snapshot describes. Deliberately not
#: `max(session_date)`: that is the latest session something became
#: ACTIONABLE in, which on a weekend is a session that has not happened, and
#: reporting it as "the latest discovery session" was the audit's finding.
LATEST_REFERENCE_SESSION_SQL = """
SELECT max(reference_session_date) AS reference_session_date
FROM public.external_discovery_candidates
"""


async def latest_reference_session(conn) -> Optional[date]:
    """The most recent market session we hold a snapshot FOR.

    Never a future date: `reference_session_date` is inferred backwards from
    the fetch clock, so the worst case is the session currently in progress.
    """
    row = await conn.fetchrow(LATEST_REFERENCE_SESSION_SQL)
    return row["reference_session_date"] if row else None


async def current_discovery(conn, *,
                            reference_session_date: Optional[date] = None,
                            ) -> List[Dict[str, Any]]:
    """The deterministic current-discovery view for one MARKET session."""
    session = reference_session_date or await latest_reference_session(conn)
    if session is None:
        return []
    rows = await conn.fetch(CURRENT_DISCOVERY_SQL, session)
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# A4 — cross-reference against the frozen universe
#
# The most important output of the whole discovery path, and the reason it
# exists: it says exactly what the fixed 25-symbol universe is not seeing.
# --------------------------------------------------------------------------- #

#: Local daily bars needed before a discovered symbol could be studied at all
#: with the tools this repository already has. Taken from the control
#: strategy's own hard gate (sma150_bounce requires 200 completed daily bars),
#: not invented here — a symbol below it cannot be evaluated by either arm.
MIN_LOCAL_BARS = 200

LOCAL_HISTORY_SQL = """
SELECT symbol, count(*) AS bars, max(trading_date) AS last_bar
FROM public.daily_bars
WHERE symbol = ANY($1::text[])
GROUP BY symbol
"""


async def cross_reference_universe(conn, *,
                                   reference_session_date: Optional[date] = None,
                                   ) -> Dict[str, Any]:
    """What one discovery session found, sorted against what we actually hold.

    Five buckets, and none of them is an instruction. A symbol appearing here
    with 0 local bars is a research question — "the market noticed this four
    sessions running and we cannot even chart it" — never an enqueue.
    """
    session = reference_session_date or await latest_reference_session(conn)
    if session is None:
        return {"reference_session_date": None, "first_actionable_session": None,
                "discovered": 0, "inside_universe": [],
                "outside_universe": [], "multi_category": [],
                "with_local_history": [], "insufficient_local_history": [],
                "min_local_bars": MIN_LOCAL_BARS}

    rows = await current_discovery(conn, reference_session_date=session)
    symbols = [r["symbol"] for r in rows]
    history: Dict[str, Dict[str, Any]] = {}
    if symbols:
        for record in await conn.fetch(LOCAL_HISTORY_SQL, symbols):
            history[record["symbol"]] = {"bars": record["bars"],
                                         "last_bar": record["last_bar"]}

    inside, outside, multi, has_history, thin = [], [], [], [], []
    for row in rows:
        bars = history.get(row["symbol"], {}).get("bars", 0) or 0
        entry = {**row, "local_bars": bars,
                 "last_local_bar": history.get(row["symbol"], {}).get("last_bar")}
        (inside if row["in_scanner_universe"] else outside).append(entry)
        if (row.get("reason_count") or 0) > 1:
            multi.append(entry)
        (has_history if bars >= MIN_LOCAL_BARS else thin).append(entry)

    actionable = [r["first_actionable_session"] for r in rows
                  if r.get("first_actionable_session") is not None]
    return {
        # The session the snapshot DESCRIBES...
        "reference_session_date": session.isoformat(),
        # ...and the earliest session anybody could have acted on it. Both,
        # always, so a report can never imply they are the same day.
        "first_actionable_session": (min(actionable).isoformat()
                                     if actionable else None),
        "discovered": len(rows),
        "min_local_bars": MIN_LOCAL_BARS,
        "inside_universe": inside,
        "outside_universe": outside,
        "multi_category": multi,
        "with_local_history": has_history,
        "insufficient_local_history": thin,
    }


__all__ = [
    "SOURCE_FMP", "SOURCE_STATE_FMP_DISCOVERY",
    "LIST_TOP_GAINERS", "LIST_TOP_LOSERS", "LIST_MOST_ACTIVE", "LIST_KINDS",
    "FMP_STABLE_BASE_URL", "ENDPOINTS", "DEFAULT_LIST_LIMIT",
    "REQUEST_INTERVAL_SECONDS",
    "STATE_OK", "STATE_UNAVAILABLE", "STATE_ERROR", "STATE_NEVER_RUN",
    "DiscoverySourceUnavailable", "FmpDiscoveryClient", "FmpStableClient",
    "normalize_symbol", "normalize_candidate", "normalize_list",
    "resolve_session", "infer_reference_session",
    "BASIS_INFERRED_FROM_OBSERVATION_TIME", "REFERENCE_SESSION_BASES", "upsert_candidates", "record_source_state",
    "refresh_discovery_candidates", "symbols_worth_investigating",
    "bound_source_row", "aggregate_discovery", "latest_reference_session",
    "current_discovery", "cross_reference_universe", "MIN_LOCAL_BARS",
    "plan_reference_backfill", "backfill_reference_sessions",
]
