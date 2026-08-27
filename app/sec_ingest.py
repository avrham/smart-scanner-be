"""Bounded SEC 8-K ingestion for the Smart Scanner catalyst layer.

Shaped like a job handler: an open connection in, a bounded summary out, no
scheduling of its own. It is the only component here that reaches the SEC, and
the Product API never does — it reads what this writes.

WHY THE SEC DIRECTLY, AND NOT THE EXISTING PROVIDER
---------------------------------------------------
Measured, not assumed. The configured market-data provider does expose SEC
filings at `/v1/reference/sec/filings` and our plan IS entitled to it (HTTP
200), but it cannot answer this product question:

  * `ticker` and `cik` query parameters are ACCEPTED AND IGNORED — asking for
    AAPL returns whichever filings the global feed happens to hold, so there is
    no way to fetch one issuer's filings short of paging the entire market.
  * the records carry NO 8-K item codes, which is the whole point of this
    layer: without them an "event type" could only be guessed from text, which
    is exactly the failure mode News V1 already documented.

The SEC's own structured submissions endpoint answers both, for free, with no
entitlement question and no intermediary that can drift:

    https://data.sec.gov/submissions/CIK##########.json

One request per issuer returns `accessionNumber`, `form`, `acceptanceDateTime`,
`filingDate`, `reportDate`, `items`, `primaryDocument` — every field the model
needs, asserted by EDGAR itself. That is a first-party source, not a second
paid provider.

SEC ACCESS POLICY
-----------------
The SEC requires a declared, identifiable User-Agent and asks that automated
clients stay within roughly 10 requests/second. This module REFUSES to make a
request without an explicit User-Agent rather than sending a default one — a
generic client string would be a policy violation dressed up as a default. The
value comes from configuration and is never committed. Requests are spaced, the
structured JSON endpoints are used instead of scraping HTML, and no access
control is circumvented.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import httpx

# ---- shared vocabulary ------------------------------------------------------ #
# Imported, not redeclared: `app/sec_events.py` owns these words, so what we
# persist and what the product says can never drift apart.
from app.sec_events import (  # noqa: F401
    ACCEPTED_FORM_PREFIX, SEC_TAXONOMY_VERSION, SOURCE_SEC_EDGAR,
    classify_event_types, is_amendment, is_primary_event, parse_item_codes,
)

# ---- source state (same vocabulary as the other catalyst ingestions) -------- #
STATE_OK = "ok"
STATE_UNAVAILABLE = "unavailable"
STATE_ERROR = "error"
STATE_NEVER_RUN = "never_run"

SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"
SEC_FILING_INDEX_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}"

#: SEC asks automated clients to stay near 10 requests/second. We use a tenth of
#: that: the whole universe is 25 issuers, so politeness costs us seconds.
REQUEST_INTERVAL_SECONDS = 1.0

#: How far back to keep filings. Wide enough to cover the product's 20-session
#: window several times over, bounded so ingestion can never become an archive.
DEFAULT_LOOKBACK_DAYS = 180


class SecSourceUnavailable(Exception):
    """The SEC endpoint could not be read (network, throttling, policy, or a
    missing User-Agent). Carries a short, secret-free reason."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


class SecClient:
    """A deliberately small SEC HTTP client.

    It exists so that the User-Agent requirement is enforced in ONE place and
    cannot be forgotten at a call site.
    """

    def __init__(self, user_agent: str, *,
                 interval_seconds: float = REQUEST_INTERVAL_SECONDS,
                 timeout: float = 60.0):
        agent = (user_agent or "").strip()
        if not agent:
            raise SecSourceUnavailable(
                "missing_user_agent",
                "SEC access requires a declared identifiable User-Agent")
        self.user_agent = agent
        self.interval_seconds = interval_seconds
        self.timeout = timeout

    async def get_json(self, url: str) -> Any:
        headers = {"User-Agent": self.user_agent,
                   "Accept-Encoding": "gzip, deflate"}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, headers=headers)
        if response.status_code == 403:
            raise SecSourceUnavailable(
                "sec_access_denied",
                "SEC returned HTTP 403 — check the declared User-Agent")
        if response.status_code == 429:
            raise SecSourceUnavailable(
                "sec_rate_limited", "SEC returned HTTP 429 — request rate too high")
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise SecSourceUnavailable(
                "sec_unavailable", f"SEC returned HTTP {response.status_code}")
        return response.json()

    async def pause(self) -> None:
        await asyncio.sleep(self.interval_seconds)


# --------------------------------------------------------------------------- #
# Normalisation — pure, so it is fully testable without the network
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
    # EDGAR stamps acceptance in UTC and says so with a trailing Z. A value that
    # arrives without an offset is read as UTC rather than as local time, which
    # would silently shift the point-in-time gate by hours.
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def normalize_cik(value: Any) -> Optional[str]:
    """EDGAR CIKs are zero-padded to 10 digits everywhere they are used."""
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(10) if digits else None


def accession_no_dashes(accession: str) -> str:
    return accession.replace("-", "")


def build_filing_url(cik: str, accession: str, document: Optional[str]) -> str:
    """A direct link to the primary document, or to the filing index if EDGAR
    did not name one. Never a search page: the reader should land on the filing.
    """
    numeric_cik = str(int(cik))
    folder = accession_no_dashes(accession)
    if document:
        return SEC_ARCHIVE_URL.format(cik=numeric_cik, accession=folder,
                                      document=document)
    return SEC_FILING_INDEX_URL.format(cik=numeric_cik, accession=folder)


def is_current_report(form: Any) -> bool:
    """8-K, 8-K/A, 8-K12B … and nothing else.

    V1 is about material corporate events, not about every document a company
    files. 10-K/10-Q/Form 4/13D are different product questions.
    """
    return str(form or "").strip().upper().startswith(ACCEPTED_FORM_PREFIX)


def normalize_filing(row: Dict[str, Any], *, cik: str,
                     observed_at: datetime) -> Optional[Dict[str, Any]]:
    """One EDGAR submissions record -> one storable row, or None if unusable.

    A filing with no accession number or no acceptance timestamp is dropped
    rather than stored with an invented part: identity and point-in-time
    honesty both rest on those two fields, and a guess at either would be worse
    than not carrying the filing at all.
    """
    accession = str(row.get("accessionNumber") or "").strip()
    accepted_at = _as_datetime(row.get("acceptanceDateTime"))
    form = str(row.get("form") or "").strip()
    if not accession or accepted_at is None or not form:
        return None
    if not is_current_report(form):
        return None

    filing_date = _as_date(row.get("filingDate")) or accepted_at.date()
    item_codes = parse_item_codes(row.get("items"))
    document = str(row.get("primaryDocument") or "").strip() or None

    return {
        "source": SOURCE_SEC_EDGAR,
        "accession_number": accession,
        "cik": cik,
        "form": form,
        "accepted_at": accepted_at,
        "filing_date": filing_date,
        # The EVENT date. Carried for completeness and never used as the gate.
        "period_of_report": _as_date(row.get("reportDate")),
        "item_codes": item_codes,
        "event_types": classify_event_types(item_codes),
        "taxonomy_version": SEC_TAXONOMY_VERSION,
        "is_primary_event": is_primary_event(item_codes),
        # EDGAR's submissions feed does not carry an explicit pointer from an
        # amendment to the filing it amends, so this stays NULL rather than
        # being guessed from dates. The `8-K/A` form itself is what tells the
        # product an amendment is an amendment, and both rows survive.
        "amends_accession_number": None,
        "primary_document": document,
        "filing_url": build_filing_url(cik, accession, document),
        "observed_at": observed_at,
    }


def extract_recent_filings(submissions: Dict[str, Any], *, cik: str,
                           observed_at: datetime,
                           since: Optional[date] = None) -> List[Dict[str, Any]]:
    """Pull the current reports out of one issuer's submissions document.

    EDGAR returns `filings.recent` as PARALLEL ARRAYS — one array per field,
    indexed together. Zipping them by index is the documented shape; anything
    else would silently mis-pair a form with another filing's timestamp.
    """
    recent = ((submissions or {}).get("filings") or {}).get("recent") or {}
    accessions = recent.get("accessionNumber") or []
    out: List[Dict[str, Any]] = []
    for index in range(len(accessions)):
        row = {key: (values[index] if index < len(values) else None)
               for key, values in recent.items() if isinstance(values, list)}
        record = normalize_filing(row, cik=cik, observed_at=observed_at)
        if record is None:
            continue
        if since and record["filing_date"] < since:
            continue
        out.append(record)
    return out


def extract_symbols(submissions: Dict[str, Any]) -> List[str]:
    """Every ticker EDGAR associates with this issuer (dual-class included)."""
    tickers = (submissions or {}).get("tickers") or []
    return sorted({str(t).strip().upper() for t in tickers if str(t).strip()})


# --------------------------------------------------------------------------- #
# SEC access
# --------------------------------------------------------------------------- #

async def fetch_ticker_cik_map(client: SecClient) -> Dict[str, str]:
    """The SEC's own ticker -> CIK directory. One request, authoritative.

    Resolved live rather than frozen into the repository: a CIK can change when
    an issuer reorganises, and a stale hard-coded map would quietly attach one
    company's filings to another company's symbol.
    """
    payload = await client.get_json(SEC_TICKER_MAP_URL)
    if not isinstance(payload, dict):
        raise SecSourceUnavailable("sec_unavailable",
                                   "ticker directory was not readable")
    out: Dict[str, str] = {}
    for entry in payload.values():
        ticker = str((entry or {}).get("ticker") or "").strip().upper()
        cik = normalize_cik((entry or {}).get("cik_str"))
        if ticker and cik:
            out[ticker] = cik
    return out


async def fetch_filings(client: SecClient, symbols: Sequence[str], *,
                        observed_at: datetime,
                        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
                        cik_map: Optional[Dict[str, str]] = None,
                        ) -> Dict[str, Any]:
    """Bounded per-issuer 8-K fetch over a symbol list.

    Returns filings keyed by accession number plus the symbol links, because
    one issuer's filing legitimately belongs to several tickers and the caller
    must not have to rediscover that.
    """
    resolved = cik_map if cik_map is not None else await fetch_ticker_cik_map(client)
    since = date.fromordinal(max(1, observed_at.date().toordinal() - lookback_days))

    filings: Dict[str, Dict[str, Any]] = {}
    links: Dict[str, Set[str]] = {}
    unresolved: List[str] = []
    seen_ciks: Set[str] = set()

    for symbol in symbols:
        cik = resolved.get(symbol.upper())
        if not cik:
            unresolved.append(symbol)
            continue
        if cik in seen_ciks:
            # Two of our symbols share one issuer (dual class): its filings are
            # already loaded and linking happens below.
            continue
        seen_ciks.add(cik)
        await client.pause()
        submissions = await client.get_json(SEC_SUBMISSIONS_URL.format(cik=cik))
        if submissions is None:
            unresolved.append(symbol)
            continue
        records = extract_recent_filings(submissions, cik=cik,
                                         observed_at=observed_at, since=since)
        issuer_symbols = {s for s in extract_symbols(submissions)
                          if s in {x.upper() for x in symbols}} or {symbol.upper()}
        for record in records:
            filings[record["accession_number"]] = record
            links.setdefault(record["accession_number"], set()).update(issuer_symbols)

    return {"filings": list(filings.values()), "links": links,
            "unresolved": unresolved, "ciks": len(seen_ciks)}


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #

UPSERT_FILING_SQL = """
INSERT INTO public.sec_filings (
    source, accession_number, cik, form, accepted_at, filing_date,
    period_of_report, item_codes, event_types, taxonomy_version,
    is_primary_event, amends_accession_number, primary_document, filing_url,
    observed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (source, accession_number) DO UPDATE SET
    form = EXCLUDED.form,
    accepted_at = EXCLUDED.accepted_at,
    filing_date = EXCLUDED.filing_date,
    period_of_report = EXCLUDED.period_of_report,
    item_codes = EXCLUDED.item_codes,
    event_types = EXCLUDED.event_types,
    taxonomy_version = EXCLUDED.taxonomy_version,
    is_primary_event = EXCLUDED.is_primary_event,
    primary_document = COALESCE(EXCLUDED.primary_document,
                                sec_filings.primary_document),
    filing_url = EXCLUDED.filing_url,
    updated_at = NOW()
RETURNING id, (xmax = 0) AS inserted
"""

LINK_SQL = """
INSERT INTO public.sec_filing_symbols (filing_id, symbol, cik)
VALUES ($1,$2,$3)
ON CONFLICT (symbol, filing_id) DO NOTHING
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


async def upsert_filings(conn, filings: Iterable[Dict[str, Any]], *,
                         links: Optional[Dict[str, Set[str]]] = None,
                         universe: Optional[Set[str]] = None) -> Dict[str, int]:
    """Idempotent write of filings + their issuer/symbol links.

    IDENTITY IS SEC-NATIVE. `(source, accession_number)` is UNIQUE, so a second
    refresh updates rather than inserts, and `inserted` vs `updated` is reported
    separately — a single "written" counter would hide exactly the regression
    this is here to detect.

    AMENDMENTS ARE NOT MERGED. An 8-K/A carries its own accession number, so it
    lands as its own row beside the filing it amends. Both survive; neither
    silently overwrites the other.
    """
    stats = {"filings_seen": 0, "filings_inserted": 0, "filings_updated": 0,
             "links_written": 0}
    link_map = links or {}

    for filing in filings:
        stats["filings_seen"] += 1
        written = await conn.fetchrow(
            UPSERT_FILING_SQL,
            filing["source"], filing["accession_number"], filing["cik"],
            filing["form"], filing["accepted_at"], filing["filing_date"],
            filing.get("period_of_report"), filing["item_codes"],
            filing["event_types"], filing["taxonomy_version"],
            filing["is_primary_event"], filing.get("amends_accession_number"),
            filing.get("primary_document"), filing["filing_url"],
            filing["observed_at"])
        stats["filings_inserted" if written["inserted"]
              else "filings_updated"] += 1

        for symbol in sorted(link_map.get(filing["accession_number"]) or set()):
            if universe is not None and symbol not in universe:
                continue
            await conn.execute(LINK_SQL, written["id"], symbol, filing["cik"])
            stats["links_written"] += 1

    return stats


async def record_source_state(conn, status: str, *, symbols_covered: int = 0,
                              filings_written: int = 0, detail: str = "",
                              now: Optional[datetime] = None) -> None:
    """One row in `catalyst_source_state`, so an empty filings table can never
    be misread as 'no company disclosed anything'."""
    moment = now or datetime.now(timezone.utc)
    await conn.execute(
        SOURCE_STATE_SQL, SOURCE_SEC_EDGAR, status, moment,
        moment if status == STATE_OK else None,
        symbols_covered, filings_written, detail[:400] or None)


async def refresh_sec_filings(conn, client: SecClient, symbols: Sequence[str], *,
                              now: Optional[datetime] = None,
                              lookback_days: int = DEFAULT_LOOKBACK_DAYS,
                              cik_map: Optional[Dict[str, str]] = None,
                              ) -> Dict[str, Any]:
    """One idempotent refresh over a bounded symbol list.

    A source failure is absorbed into `catalyst_source_state` and reported,
    never raised: an EDGAR outage must cost the product its SEC dimension and
    nothing else.
    """
    moment = now or datetime.now(timezone.utc)
    cleaned = [s.strip().upper() for s in symbols if s and s.strip()]
    universe = set(cleaned)
    summary: Dict[str, Any] = {"symbols": len(cleaned), "source": SOURCE_SEC_EDGAR}

    try:
        fetched = await fetch_filings(client, cleaned, observed_at=moment,
                                      lookback_days=lookback_days,
                                      cik_map=cik_map)
        stats = await upsert_filings(conn, fetched["filings"],
                                     links=fetched["links"], universe=universe)
        await record_source_state(conn, STATE_OK, symbols_covered=len(cleaned),
                                  filings_written=stats["filings_inserted"],
                                  now=moment)
        summary.update({"status": STATE_OK, "issuers": fetched["ciks"],
                        "unresolved_symbols": fetched["unresolved"], **stats})
    except SecSourceUnavailable as exc:
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
    "SOURCE_SEC_EDGAR", "STATE_OK", "STATE_UNAVAILABLE", "STATE_ERROR",
    "STATE_NEVER_RUN", "SEC_SUBMISSIONS_URL", "SEC_TICKER_MAP_URL",
    "REQUEST_INTERVAL_SECONDS", "DEFAULT_LOOKBACK_DAYS",
    "SecSourceUnavailable", "SecClient",
    "normalize_cik", "accession_no_dashes", "build_filing_url",
    "is_current_report", "normalize_filing", "extract_recent_filings",
    "extract_symbols", "fetch_ticker_cik_map", "fetch_filings",
    "upsert_filings", "record_source_state", "refresh_sec_filings",
]
