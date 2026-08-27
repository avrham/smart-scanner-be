"""Ingestion for corporate catalyst events (Earnings Catalyst Context V1).

Reuses the EXISTING market-data client (its rate limiting, retries and error
sanitising). No new provider, no new scheduler, no polling loop, and no
credential anywhere near the Product API — this module is only ever run by a
component that already holds the provider key.

PROVIDER REALITY, measured not assumed
--------------------------------------
The configured provider exposes an earnings calendar at `/benzinga/v1/earnings`,
but our plan is not entitled to it:

    HTTP 403 {"error": "You are not entitled to this data. Please upgrade..."}

So a FORWARD earnings calendar is genuinely unavailable to us today. That is
reported as an explicit `unavailable` source state with the reason — never as
"no earnings scheduled", which would be a different and false statement.

What the plan DOES expose is `/vX/reference/financials`, which carries the date
each periodic financial report was FILED. That is a real, dated, already-occurred
corporate event, and it answers the "was there a recent report?" half of the
product question. It is stored as its own event type and must never be presented
as an earnings-announcement date: a 10-Q is typically filed on or just after the
announcement, and conflating them would invent precision we do not have.

LIFECYCLE
---------
`refresh_catalysts()` is a single idempotent entry point taking an open
connection and a symbol list. It is deliberately shaped like a job handler so
the daily pipeline can call it later without modification.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# ---- shared vocabulary ------------------------------------------------------ #
# Imported, not redeclared: `app/catalyst.py` owns these words. Re-exported here
# so ingestion callers need only one import.
from app.catalyst import (  # noqa: E402,F401
    CERTAINTIES, CERTAINTY_CONFIRMED, CERTAINTY_ESTIMATED, CERTAINTY_FILED,
    EVENT_EARNINGS, EVENT_FINANCIAL_REPORT_FILING, EVENT_TYPES,
    SESSION_TIMINGS, TIMING_AFTER_MARKET, TIMING_BEFORE_MARKET,
    TIMING_DURING_MARKET, TIMING_UNKNOWN,
)

# ---- sources ---------------------------------------------------------------- #
SOURCE_EARNINGS_CALENDAR = "provider_earnings_calendar"
SOURCE_FINANCIAL_FILINGS = "provider_financial_report_filings"

# ---- source state ----------------------------------------------------------- #
STATE_OK = "ok"
STATE_UNAVAILABLE = "unavailable"
STATE_ERROR = "error"
STATE_NEVER_RUN = "never_run"

#: Provider path for the (plan-gated) earnings calendar.
EARNINGS_CALENDAR_PATH = "/benzinga/v1/earnings"
#: Provider path for periodic financial reports (available on our plan).
FINANCIALS_PATH = "/vX/reference/financials"

#: How many recent periodic reports to keep per symbol. Bounded on purpose.
FILINGS_PER_SYMBOL = 4


class CatalystSourceUnavailable(Exception):
    """The source exists but this deployment cannot read it (entitlement,
    credential, or removal). Carries a short, secret-free reason."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


# --------------------------------------------------------------------------- #
# Normalisation — pure, so it is fully testable without a provider
# --------------------------------------------------------------------------- #

def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def normalize_earnings_timing(raw: Any) -> str:
    """Map a provider timing token onto our session vocabulary.

    Anything unrecognised becomes `unknown` rather than a guess — the product
    says "timing unknown" instead of implying a session.
    """
    token = (str(raw or "")).strip().lower()
    if token in ("bmo", "before_market", "before-market-open", "premarket", "pre-market"):
        return TIMING_BEFORE_MARKET
    if token in ("amc", "after_market", "after-market-close", "postmarket", "post-market"):
        return TIMING_AFTER_MARKET
    if token in ("dmt", "during_market", "during-market", "intraday"):
        return TIMING_DURING_MARKET
    return TIMING_UNKNOWN


def normalize_earnings_record(symbol: str, row: Dict[str, Any],
                              *, observed_at: datetime) -> Optional[Dict[str, Any]]:
    """One provider earnings row -> one storable event, or None if unusable."""
    event_date = _as_date(row.get("date") or row.get("earnings_date"))
    if event_date is None:
        return None
    confirmed = row.get("date_confirmed")
    certainty = (CERTAINTY_CONFIRMED
                 if confirmed in (True, 1, "1", "true", "True")
                 else CERTAINTY_ESTIMATED)
    return {
        "symbol": symbol.strip().upper(),
        "event_type": EVENT_EARNINGS,
        "event_date": event_date,
        "session_timing": normalize_earnings_timing(row.get("time") or row.get("timing")),
        "certainty": certainty,
        "fiscal_period": _clean(row.get("fiscal_period") or row.get("period")),
        "fiscal_year": _clean(row.get("fiscal_year") or row.get("period_year")),
        "source": SOURCE_EARNINGS_CALENDAR,
        "source_reference": _clean(row.get("id")),
        "observed_at": observed_at,
    }


def normalize_filing_record(row: Dict[str, Any], *, observed_at: datetime,
                            symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """One provider financials row -> one `financial_report_filing` event.

    ONE report routinely covers SEVERAL tickers: every share class plus the
    preferred lines share a filing, so GOOGL's report lists GOOG/GOOGL/GOOGM and
    JPM's lists AMJB/JPM/JPMpC and more. The list order means nothing, so when we
    asked about a specific symbol we attribute the filing to THAT symbol if it is
    covered, and drop the row otherwise — never to whichever ticker happens to
    sort first.

    `certainty` is always `filed`: this is a recorded past fact, neither a
    confirmed future date nor an estimate. `session_timing` stays `unknown`
    even though an acceptance timestamp exists — SEC acceptance time is not the
    earnings-call session, and inferring one from the other would be exactly the
    kind of invented precision this model refuses.
    """
    filing_date = _as_date(row.get("filing_date"))
    covered = [str(t).strip().upper() for t in (row.get("tickers") or []) if t]
    requested = (symbol or "").strip().upper()
    if requested:
        if requested not in covered:
            return None
        resolved = requested
    else:
        resolved = (covered[0] if covered
                    else str(row.get("ticker") or "").strip().upper())
    if filing_date is None or not resolved:
        return None
    return {
        "symbol": resolved,
        "event_type": EVENT_FINANCIAL_REPORT_FILING,
        "event_date": filing_date,
        "session_timing": TIMING_UNKNOWN,
        "certainty": CERTAINTY_FILED,
        "fiscal_period": _clean(row.get("fiscal_period")),
        "fiscal_year": _clean(row.get("fiscal_year")),
        "source": SOURCE_FINANCIAL_FILINGS,
        "source_reference": _clean(row.get("source_filing_url")),
        "observed_at": observed_at,
    }


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text[:400] or None


# --------------------------------------------------------------------------- #
# Provider access
# --------------------------------------------------------------------------- #

async def fetch_earnings_calendar(client, symbols: Sequence[str],
                                  *, observed_at: datetime) -> List[Dict[str, Any]]:
    """Forward earnings calendar. Raises CatalystSourceUnavailable when the
    deployment is not entitled to the feed."""
    events: List[Dict[str, Any]] = []
    for symbol in symbols:
        try:
            payload = await client._request(
                EARNINGS_CALENDAR_PATH, {"ticker": symbol, "limit": 4})
        except Exception as exc:  # provider error object is already sanitised
            status = getattr(exc, "status_code", None)
            if status in (401, 402, 403, 404):
                raise CatalystSourceUnavailable(
                    "provider_not_entitled",
                    f"earnings calendar returned HTTP {status} for this plan",
                ) from exc
            raise
        for row in (payload or {}).get("results") or []:
            record = normalize_earnings_record(symbol, row, observed_at=observed_at)
            if record:
                events.append(record)
    return events


async def fetch_financial_report_filings(client, symbols: Sequence[str],
                                         *, observed_at: datetime) -> List[Dict[str, Any]]:
    """Recent periodic-report FILING dates — an already-occurred fact."""
    events: List[Dict[str, Any]] = []
    for symbol in symbols:
        payload = await client._request(FINANCIALS_PATH, {
            "ticker": symbol, "limit": FILINGS_PER_SYMBOL,
            "timeframe": "quarterly", "order": "desc", "sort": "filing_date",
        })
        for row in (payload or {}).get("results") or []:
            # `symbol=` both attributes and filters: a report that does not
            # cover the symbol we asked about is dropped, not reassigned.
            record = normalize_filing_record(
                row, observed_at=observed_at, symbol=symbol)
            if record:
                events.append(record)
    return events


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #

UPSERT_SQL = """
INSERT INTO public.symbol_catalyst_events (
    symbol, event_type, event_date, session_timing, certainty,
    fiscal_period, fiscal_year, source, source_reference, observed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (symbol, event_type, event_date) DO UPDATE SET
    session_timing = EXCLUDED.session_timing,
    certainty = EXCLUDED.certainty,
    fiscal_period = COALESCE(EXCLUDED.fiscal_period, symbol_catalyst_events.fiscal_period),
    fiscal_year = COALESCE(EXCLUDED.fiscal_year, symbol_catalyst_events.fiscal_year),
    source = EXCLUDED.source,
    source_reference = COALESCE(EXCLUDED.source_reference,
                                symbol_catalyst_events.source_reference),
    observed_at = EXCLUDED.observed_at,
    updated_at = NOW()
"""

SUPERSEDE_SQL = """
DELETE FROM public.symbol_catalyst_events
WHERE symbol = $1 AND event_type = $2
  AND fiscal_period IS NOT DISTINCT FROM $3
  AND fiscal_year IS NOT DISTINCT FROM $4
  AND event_date <> $5
  AND event_date >= $6
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


async def upsert_events(conn, events: Iterable[Dict[str, Any]],
                        *, today: Optional[date] = None) -> int:
    """Idempotent upsert.

    A RESCHEDULED forward event is handled explicitly: after writing the new
    date, any other FUTURE row for the same (symbol, type, fiscal period) is
    removed, so a moved earnings date never leaves two competing futures on the
    board. Past rows are left alone — they are history, not a schedule.
    """
    reference_day = today or datetime.now(timezone.utc).date()
    count = 0
    for e in events:
        await conn.execute(
            UPSERT_SQL, e["symbol"], e["event_type"], e["event_date"],
            e["session_timing"], e["certainty"], e.get("fiscal_period"),
            e.get("fiscal_year"), e["source"], e.get("source_reference"),
            e["observed_at"])
        if e["event_date"] >= reference_day and e.get("fiscal_period"):
            await conn.execute(
                SUPERSEDE_SQL, e["symbol"], e["event_type"],
                e.get("fiscal_period"), e.get("fiscal_year"),
                e["event_date"], reference_day)
        count += 1
    return count


async def record_source_state(conn, source: str, status: str, *,
                              symbols_covered: int = 0, events_upserted: int = 0,
                              detail: str = "", now: Optional[datetime] = None) -> None:
    moment = now or datetime.now(timezone.utc)
    await conn.execute(
        SOURCE_STATE_SQL, source, status, moment,
        moment if status == STATE_OK else None,
        symbols_covered, events_upserted, detail[:400] or None)


async def refresh_catalysts(conn, client, symbols: Sequence[str], *,
                            now: Optional[datetime] = None) -> Dict[str, Any]:
    """One idempotent refresh over a bounded symbol list.

    Shaped like a job handler on purpose: an open connection in, a bounded
    result out, no scheduling of its own. Each source is recorded independently,
    so one source being unavailable never hides the other's result.
    """
    moment = now or datetime.now(timezone.utc)
    symbols = [s.strip().upper() for s in symbols if s and s.strip()]
    summary: Dict[str, Any] = {"symbols": len(symbols), "sources": {}}

    # 1. Forward earnings calendar (plan-gated for this deployment).
    try:
        events = await fetch_earnings_calendar(client, symbols, observed_at=moment)
        written = await upsert_events(conn, events, today=moment.date())
        await record_source_state(conn, SOURCE_EARNINGS_CALENDAR, STATE_OK,
                                  symbols_covered=len(symbols),
                                  events_upserted=written, now=moment)
        summary["sources"][SOURCE_EARNINGS_CALENDAR] = {
            "status": STATE_OK, "events": written}
    except CatalystSourceUnavailable as exc:
        await record_source_state(conn, SOURCE_EARNINGS_CALENDAR, STATE_UNAVAILABLE,
                                  symbols_covered=len(symbols), events_upserted=0,
                                  detail=f"{exc.reason}: {exc.detail}", now=moment)
        summary["sources"][SOURCE_EARNINGS_CALENDAR] = {
            "status": STATE_UNAVAILABLE, "reason": exc.reason}
    except Exception as exc:
        await record_source_state(conn, SOURCE_EARNINGS_CALENDAR, STATE_ERROR,
                                  symbols_covered=len(symbols), events_upserted=0,
                                  detail=type(exc).__name__, now=moment)
        summary["sources"][SOURCE_EARNINGS_CALENDAR] = {
            "status": STATE_ERROR, "reason": type(exc).__name__}

    # 2. Periodic financial-report filing dates (available on our plan).
    try:
        events = await fetch_financial_report_filings(client, symbols, observed_at=moment)
        written = await upsert_events(conn, events, today=moment.date())
        await record_source_state(conn, SOURCE_FINANCIAL_FILINGS, STATE_OK,
                                  symbols_covered=len(symbols),
                                  events_upserted=written, now=moment)
        summary["sources"][SOURCE_FINANCIAL_FILINGS] = {
            "status": STATE_OK, "events": written}
    except Exception as exc:
        await record_source_state(conn, SOURCE_FINANCIAL_FILINGS, STATE_ERROR,
                                  symbols_covered=len(symbols), events_upserted=0,
                                  detail=type(exc).__name__, now=moment)
        summary["sources"][SOURCE_FINANCIAL_FILINGS] = {
            "status": STATE_ERROR, "reason": type(exc).__name__}

    return summary


__all__ = [
    "EVENT_EARNINGS", "EVENT_FINANCIAL_REPORT_FILING", "EVENT_TYPES",
    "TIMING_BEFORE_MARKET", "TIMING_AFTER_MARKET", "TIMING_DURING_MARKET",
    "TIMING_UNKNOWN", "SESSION_TIMINGS",
    "CERTAINTY_CONFIRMED", "CERTAINTY_ESTIMATED", "CERTAINTY_FILED", "CERTAINTIES",
    "SOURCE_EARNINGS_CALENDAR", "SOURCE_FINANCIAL_FILINGS",
    "STATE_OK", "STATE_UNAVAILABLE", "STATE_ERROR", "STATE_NEVER_RUN",
    "EARNINGS_CALENDAR_PATH", "FINANCIALS_PATH", "FILINGS_PER_SYMBOL",
    "CatalystSourceUnavailable",
    "normalize_earnings_timing", "normalize_earnings_record",
    "normalize_filing_record",
    "fetch_earnings_calendar", "fetch_financial_report_filings",
    "upsert_events", "record_source_state", "refresh_catalysts",
]
