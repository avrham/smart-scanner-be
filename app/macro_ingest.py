"""Pull the two entitled macro calendars, normalise them, store them.

    federalreserve.gov/monetarypolicy/fomccalendars.htm  -> FOMC rate decisions
    bea.gov/news/schedule                                -> GDP, PCE

NETWORK + DB. The pure reading layer is `app/macro_calendar.py`; the parsing
functions here are pure too, so the whole normalisation is testable against
saved markup without touching either the network or a database.

WHY THESE TWO AND NOT A CALENDAR PROVIDER
-----------------------------------------
A commercial economic calendar would have been one endpoint instead of two
parsers. It would also have put the least defensible link in the chain — a
vendor's transcription of a government schedule — behind a licence that forbids
showing it. The publishers themselves have neither problem: works of the U.S.
Government are not subject to copyright protection in the United States
(17 U.S.C. 105), there is no key, no plan and no rate limit, and the Fed's own
page is definitionally correct about the Fed's own meetings.

WHAT WAS MEASURED, 2026-08-30
-----------------------------
  federalreserve.gov/monetarypolicy/fomccalendars.htm   200
  bea.gov/news/schedule                                 200
  www.bls.gov  — every path, including /robots.txt, from two independent
                 clients                                403
  FMP /stable/economic-calendar                         402

The BLS result is why CPI, PPI, nonfarm payrolls and the unemployment rate are
NOT in this module. api.bls.gov answers (200) but serves time series only — it
publishes no release calendar — so there is no first-party route to those
schedules from here. They are blocked, not deferred, and inventing them from a
secondary source would be exactly the transcription risk this design avoids.

PARSING PUBLISHED HTML IS THE SUPPORTED INTERFACE HERE
------------------------------------------------------
Neither agency offers a schedule API. Both publish the schedule as a public
page with stable, semantically-classed markup, and both are read at daily
cadence with a single request and an identifying User-Agent. A layout change
breaks the parse into `unavailable` with a named reason — never into silently
wrong dates: every parsed row must yield a real date and a recognised event
type or it is dropped.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx

import app.macro_calendar as mc

# --------------------------------------------------------------------------- #
# transport
# --------------------------------------------------------------------------- #
FOMC_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
BEA_SCHEDULE_URL = "https://www.bea.gov/news/schedule"

#: Identifies us and gives an operator on the other end somebody to contact.
#: Not a disguise: an agency that wants to refuse this traffic must be able to.
USER_AGENT = ("smart-scanner-market-calendar/1.0 "
              "(+internal research; contact: repository owner)")

REQUEST_TIMEOUT_SECONDS = 30.0
REQUEST_INTERVAL_SECONDS = 1.0

STATE_OK = "ok"
STATE_UNAVAILABLE = "unavailable"
STATE_ERROR = "error"
STATE_NEVER_RUN = "never_run"


class MacroSourceUnavailable(Exception):
    """The publisher could not be read, or the page no longer parses.

    Carries a short, secret-free reason. A layout change is `unparseable`, a
    refusal is `forbidden`; keeping them apart is the difference between "fix
    the parser" and "this network is blocked".
    """

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


class MacroCalendarClient:
    """One tiny HTTP client for both publishers.

    Deliberately not shared with the FMP discovery client: that one carries a
    credential and speaks a provider's error vocabulary (402 = not entitled).
    These two carry no credential and cannot return that answer at all.
    """

    def __init__(self, *, timeout: float = REQUEST_TIMEOUT_SECONDS,
                 interval_seconds: float = REQUEST_INTERVAL_SECONDS):
        self.timeout = timeout
        self.interval_seconds = interval_seconds

    async def get_text(self, url: str) -> str:
        try:
            async with httpx.AsyncClient(
                    timeout=self.timeout, follow_redirects=True,
                    headers={"User-Agent": USER_AGENT}) as client:
                response = await client.get(url)
        except Exception as exc:                       # network-level failure
            raise MacroSourceUnavailable(
                "unreachable", type(exc).__name__) from exc
        if response.status_code in (401, 403):
            raise MacroSourceUnavailable(
                "forbidden", f"publisher returned HTTP {response.status_code}")
        if response.status_code == 429:
            raise MacroSourceUnavailable("rate_limited", "HTTP 429")
        if response.status_code != 200:
            raise MacroSourceUnavailable(
                "publisher_unavailable",
                f"HTTP {response.status_code}")
        return response.text

    async def pause(self) -> None:
        await asyncio.sleep(self.interval_seconds)


# --------------------------------------------------------------------------- #
# parsing — pure
# --------------------------------------------------------------------------- #

_MONTHS = {m: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"])}


def _month_number(name: str) -> Optional[int]:
    return _MONTHS.get((name or "").strip().lower())


def _strip_tags(fragment: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", fragment)).strip()


# ---- Federal Reserve ------------------------------------------------------- #

_FOMC_PANEL_RE = re.compile(r'<a id="\d+">(\d{4}) FOMC Meetings</a>')
# The lookahead has to stop at the PANEL FOOTER and at the next plain row as
# well as at the next meeting. Without those, the final meeting of the last
# panel swallows the page footer, whose navigation contains a link that matches
# the press-conference pattern — which would have reported a press conference
# for a meeting the Fed has not yet scheduled one for.
_FOMC_MEETING_RE = re.compile(
    r'<div class="[^"]*fomc-meeting"[^>]*>(.*?)'
    r'(?=<div class="[^"]*fomc-meeting"|<div class="panel|<div class="row">|\Z)',
    re.S)
_FOMC_MONTH_RE = re.compile(
    r'fomc-meeting__month[^>]*>\s*(?:<strong>)?([A-Za-z/ ]+?)(?:</strong>)?\s*</div>')
_FOMC_DATE_RE = re.compile(r'fomc-meeting__date[^>]*>\s*([^<]+?)\s*</div>')
_FOMC_PRESSCONF_RE = re.compile(r'fomcpres{1,2}conf', re.I)
_FOMC_STATEMENT_RE = re.compile(
    r'href="(/newsevents/pressreleases/monetary(\d{8})a\.htm)"')


def parse_fomc_calendar(html: str, *, observed_at: datetime,
                        source_reference: str = FOMC_CALENDAR_URL,
                        ) -> List[Dict[str, Any]]:
    """The Fed's meeting calendar -> storable `fomc_rate_decision` rows.

    The DECISION lands on the last day of the meeting, which is the date the
    statement carries and the date a reader means by "FOMC Wednesday". The
    first day is kept separately because "the meeting starts today" is a
    different true thing.

    A date range spelled `31-1` under `January/February` spans a month boundary
    and, in December, a year boundary. Both are handled explicitly rather than
    left to arithmetic that would silently produce a date in the wrong year.

    Raises `MacroSourceUnavailable('unparseable')` when the page yields no
    meetings at all — a layout change must fail loudly, not quietly return an
    empty calendar that reads as "the Fed has no meetings scheduled".
    """
    parts = _FOMC_PANEL_RE.split(html)
    events: List[Dict[str, Any]] = []
    seen: set = set()

    for idx in range(1, len(parts), 2):
        try:
            panel_year = int(parts[idx])
        except (TypeError, ValueError):
            continue
        body = parts[idx + 1]
        for block in _FOMC_MEETING_RE.findall(body):
            month_match = _FOMC_MONTH_RE.search(block)
            date_match = _FOMC_DATE_RE.search(block)
            if not month_match or not date_match:
                continue
            parsed = _parse_fomc_meeting(
                month_match.group(1), date_match.group(1), panel_year)
            if parsed is None:
                continue
            start_date, end_date, has_projections = parsed
            if end_date in seen:
                continue
            seen.add(end_date)

            statement = _FOMC_STATEMENT_RE.search(block)
            metadata: Dict[str, Any] = {"panel_year": panel_year}
            if statement:
                metadata["statement_url"] = (
                    "https://www.federalreserve.gov" + statement.group(1))

            events.append({
                "source": mc.SOURCE_FEDERAL_RESERVE,
                "event_type": mc.EVENT_FOMC_RATE_DECISION,
                "title": f"FOMC meeting, {start_date.isoformat()} to "
                         f"{end_date.isoformat()}",
                "scheduled_date": end_date,
                "scheduled_start_date": start_date,
                # The Fed publishes no clock for the statement on this page.
                # NULL is the honest answer; 14:00 ET is a convention, not a
                # published fact, and inventing it would be inventing data.
                "scheduled_time_local": None,
                "scheduled_timezone": "America/New_York",
                # A press-conference link exists only once the Fed has posted
                # the page. Its absence for a future meeting means "not yet
                # published", not "no press conference".
                "has_press_conference": (True
                                         if _FOMC_PRESSCONF_RE.search(block)
                                         else None),
                # The asterisk on the date range is the Fed's own marker for a
                # meeting carrying a Summary of Economic Projections.
                "has_projections": has_projections or None,
                "source_reference": source_reference,
                "source_metadata": metadata,
                "observed_at": observed_at,
            })

    if not events:
        raise MacroSourceUnavailable(
            "unparseable", "no FOMC meetings found in the calendar page")
    return events


def _parse_fomc_meeting(month_text: str, date_text: str, panel_year: int,
                        ) -> Optional[Tuple[date, date, bool]]:
    """`('January/February', '31-1*', 2026)` -> (start, end, has_projections)."""
    has_projections = "*" in date_text
    digits = re.findall(r"\d+", date_text)
    if not digits:
        return None
    start_day, end_day = int(digits[0]), int(digits[-1])

    months = [m for m in re.split(r"[/&]", month_text) if m.strip()]
    start_month = _month_number(months[0]) if months else None
    if start_month is None:
        return None
    end_month = (_month_number(months[-1]) if len(months) > 1 else start_month)
    if end_month is None:
        return None

    start_year = panel_year
    # December/January rolls the second half into the next year. The panel year
    # always belongs to the FIRST day of the meeting.
    end_year = panel_year + 1 if end_month < start_month else panel_year
    try:
        start = date(start_year, start_month, start_day)
        end = date(end_year, end_month, end_day)
    except ValueError:
        return None
    if end < start:
        return None
    return start, end, has_projections


# ---- Bureau of Economic Analysis ------------------------------------------- #

_BEA_YEAR_RE = re.compile(r">Year (\d{4})<")
_BEA_ROW_RE = re.compile(
    r'<div class="release-date">([^<]+)</div>\s*'
    r'(?:<small[^>]*>([^<]*)</small>)?.*?'
    r'release-title[^>]*>\s*(.*?)\s*</td>', re.S)
_BEA_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(AM|PM)", re.I)

_BEA_GDP_RE = re.compile(r"^(GDP|Gross Domestic Product)\b", re.I)
_BEA_REGIONAL_RE = re.compile(r"\bby (County|State|Metropolitan|Industry)\b",
                              re.I)
_BEA_PCE_RE = re.compile(r"^Personal Income and Outlays\b", re.I)


def classify_bea_release(title: str) -> Optional[str]:
    """A BEA release title -> one of our event types, or None to ignore it.

    Only two of the BEA's releases move broad US equities, and the schedule is
    full of releases that do not: international transactions, services supplied
    through affiliates, and a family of REGIONAL GDP products whose titles also
    begin with "GDP". The regional ones are excluded on the leading clause,
    which is where the qualifier always appears — "GDP by County and Personal
    Income by County" is not the national accounts release and must not be
    banner-worthy.
    """
    head = (title or "").split(",")[0].strip()
    if not head:
        return None
    if _BEA_PCE_RE.match(head):
        return mc.EVENT_PCE
    if _BEA_GDP_RE.match(head) and not _BEA_REGIONAL_RE.search(head):
        return mc.EVENT_GDP
    return None


def parse_bea_schedule(html: str, *, observed_at: datetime,
                       source_reference: str = BEA_SCHEDULE_URL,
                       ) -> List[Dict[str, Any]]:
    """The BEA release schedule -> storable `gdp` / `pce` rows.

    The page carries the year ONCE, in the table header, and then lists months
    in order. When the list rolls past December the month number goes backwards
    — that is the only signal a new year has started, so it is what we use,
    rather than assuming the table never crosses a boundary.

    An unrecognised release is dropped silently and on purpose: this is a
    filter, not a mirror, and storing every BEA release would fill a banner
    that has room for one line.
    """
    year_match = _BEA_YEAR_RE.search(html)
    if not year_match:
        raise MacroSourceUnavailable(
            "unparseable", "no year header found in the BEA schedule table")
    year = int(year_match.group(1))

    rows = _BEA_ROW_RE.findall(html)
    if not rows:
        raise MacroSourceUnavailable(
            "unparseable", "no release rows found in the BEA schedule table")

    events: List[Dict[str, Any]] = []
    seen: set = set()
    previous_month = 0
    for raw_date, raw_time, raw_title in rows:
        title = _strip_tags(raw_title)
        event_type = classify_bea_release(title)
        parts = _strip_tags(raw_date).split()
        if len(parts) < 2:
            continue
        month = _month_number(parts[0])
        try:
            day = int(re.sub(r"\D", "", parts[1]))
        except ValueError:
            continue
        if month is None:
            continue
        if month < previous_month:
            year += 1
        previous_month = month
        if event_type is None:
            continue
        try:
            scheduled = date(year, month, day)
        except ValueError:
            continue
        key = (event_type, scheduled)
        if key in seen:
            continue
        seen.add(key)
        events.append({
            "source": mc.SOURCE_BEA,
            "event_type": event_type,
            "title": title,
            "scheduled_date": scheduled,
            "scheduled_start_date": None,
            "scheduled_time_local": _parse_clock(raw_time),
            "scheduled_timezone": "America/New_York",
            "has_press_conference": None,
            "has_projections": None,
            "source_reference": source_reference,
            "source_metadata": {"published_time": _strip_tags(raw_time or "")},
            "observed_at": observed_at,
        })
    return events


def _parse_clock(raw: Optional[str]) -> Optional[time]:
    """'8:30 AM' -> time(8, 30). Anything else -> None, never a guess."""
    match = _BEA_TIME_RE.search(raw or "")
    if not match:
        return None
    hour = int(match.group(1)) % 12
    if match.group(3).upper() == "PM":
        hour += 12
    return time(hour, int(match.group(2)))


# --------------------------------------------------------------------------- #
# persistence
# --------------------------------------------------------------------------- #

UPSERT_SQL = """
INSERT INTO public.macro_events (
    source, event_type, title, scheduled_date, scheduled_start_date,
    scheduled_time_local, scheduled_timezone, source_listing,
    has_press_conference, has_projections, source_reference, source_metadata,
    first_observed_at, observed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,'listed',$8,$9,$10,$11::jsonb,$12,$12)
ON CONFLICT (source, event_type, scheduled_date) DO UPDATE SET
    title = EXCLUDED.title,
    scheduled_start_date = COALESCE(EXCLUDED.scheduled_start_date,
                                    macro_events.scheduled_start_date),
    scheduled_time_local = COALESCE(EXCLUDED.scheduled_time_local,
                                    macro_events.scheduled_time_local),
    scheduled_timezone = EXCLUDED.scheduled_timezone,
    source_listing = 'listed',
    has_press_conference = COALESCE(EXCLUDED.has_press_conference,
                                    macro_events.has_press_conference),
    has_projections = COALESCE(EXCLUDED.has_projections,
                               macro_events.has_projections),
    source_reference = EXCLUDED.source_reference,
    source_metadata = EXCLUDED.source_metadata,
    observed_at = EXCLUDED.observed_at,
    updated_at = NOW()
RETURNING (xmax = 0) AS inserted
"""

#: A future event the source has stopped listing. Marked, never deleted: the
#: point-in-time record of what the calendar said on the day we read it is the
#: whole value of storing this, and a DELETE would erase it.
WITHDRAW_SQL = """
UPDATE public.macro_events
SET source_listing = 'withdrawn', updated_at = NOW()
WHERE source = $1
  AND scheduled_date >= $2
  AND observed_at < $3
  AND source_listing = 'listed'
"""

SOURCE_STATE_SQL = """
INSERT INTO public.catalyst_source_state (
    source, status, last_refresh_at, last_success_at,
    symbols_covered, events_upserted, detail, updated_at)
VALUES ($1,$2,$3,$4,0,$5,$6,NOW())
ON CONFLICT (source) DO UPDATE SET
    status = EXCLUDED.status,
    last_refresh_at = EXCLUDED.last_refresh_at,
    last_success_at = COALESCE(EXCLUDED.last_success_at,
                               catalyst_source_state.last_success_at),
    events_upserted = EXCLUDED.events_upserted,
    detail = EXCLUDED.detail,
    updated_at = NOW()
"""


async def upsert_events(conn, events: Iterable[Dict[str, Any]],
                        ) -> Dict[str, int]:
    """Idempotent write. Re-reading an unchanged calendar updates nothing but
    `observed_at`, which is what keeps the withdrawal sweep honest."""
    stats = {"seen": 0, "inserted": 0, "updated": 0}
    for event in events:
        stats["seen"] += 1
        row = await conn.fetchrow(
            UPSERT_SQL,
            event["source"], event["event_type"], event["title"],
            event["scheduled_date"], event.get("scheduled_start_date"),
            event.get("scheduled_time_local"),
            event.get("scheduled_timezone") or "America/New_York",
            event.get("has_press_conference"), event.get("has_projections"),
            event["source_reference"],
            json.dumps(event.get("source_metadata") or {}),
            event["observed_at"])
        stats["inserted" if row["inserted"] else "updated"] += 1
    return stats


async def mark_withdrawn(conn, source: str, *, as_of: date,
                         run_started_at: datetime) -> int:
    result = await conn.execute(WITHDRAW_SQL, source, as_of, run_started_at)
    try:
        return int(str(result).rsplit(" ", 1)[-1])
    except (TypeError, ValueError):
        return 0


async def record_source_state(conn, source: str, status: str, *,
                              written: int = 0, detail: str = "",
                              now: Optional[datetime] = None) -> None:
    moment = now or datetime.now(timezone.utc)
    await conn.execute(
        SOURCE_STATE_SQL, mc.source_state_key(source), status, moment,
        moment if status == STATE_OK else None, written,
        detail[:400] or None)


# --------------------------------------------------------------------------- #
# the refresh
# --------------------------------------------------------------------------- #

FETCHERS = {
    mc.SOURCE_FEDERAL_RESERVE: (FOMC_CALENDAR_URL, parse_fomc_calendar),
    mc.SOURCE_BEA: (BEA_SCHEDULE_URL, parse_bea_schedule),
}


async def refresh_macro_calendar(
    conn, client: Optional[MacroCalendarClient], *,
    now: Optional[datetime] = None,
    sources: Sequence[str] = mc.MACRO_SOURCES,
) -> Dict[str, Any]:
    """One idempotent refresh over the entitled calendars.

    Each publisher is fetched, parsed and written INDEPENDENTLY, and a failure
    is absorbed into `catalyst_source_state` rather than raised. One agency
    changing its page layout must cost the product that agency's events and
    nothing else — not the other calendar, and certainly not the scan.
    """
    moment = now or datetime.now(timezone.utc)
    today = moment.astimezone(timezone.utc).date()
    summary: Dict[str, Any] = {"as_of": today.isoformat(), "sources": {}}

    if client is None:
        for source in sources:
            await record_source_state(conn, source, STATE_UNAVAILABLE,
                                      detail="no_client", now=moment)
            summary["sources"][source] = {"status": STATE_UNAVAILABLE,
                                          "reason": "no_client"}
        summary["status"] = STATE_UNAVAILABLE
        return summary

    any_ok = False
    for source in sources:
        target = FETCHERS.get(source)
        if target is None:
            continue
        url, parser = target
        try:
            await client.pause()
            html = await client.get_text(url)
            events = parser(html, observed_at=moment)
            stats = await upsert_events(conn, events)
            withdrawn = await mark_withdrawn(
                conn, source, as_of=today, run_started_at=moment)
            await record_source_state(conn, source, STATE_OK,
                                      written=stats["inserted"],
                                      detail=f"withdrawn={withdrawn}",
                                      now=moment)
            summary["sources"][source] = {"status": STATE_OK,
                                          "withdrawn": withdrawn, **stats}
            any_ok = True
        except MacroSourceUnavailable as exc:
            await record_source_state(conn, source, STATE_UNAVAILABLE,
                                      detail=exc.reason, now=moment)
            summary["sources"][source] = {"status": STATE_UNAVAILABLE,
                                          "reason": exc.reason}
        except Exception as exc:
            await record_source_state(conn, source, STATE_ERROR,
                                      detail=type(exc).__name__, now=moment)
            summary["sources"][source] = {"status": STATE_ERROR,
                                          "reason": type(exc).__name__}

    summary["status"] = STATE_OK if any_ok else STATE_UNAVAILABLE
    return summary


__all__ = [
    "FOMC_CALENDAR_URL", "BEA_SCHEDULE_URL", "USER_AGENT",
    "STATE_OK", "STATE_UNAVAILABLE", "STATE_ERROR", "STATE_NEVER_RUN",
    "MacroSourceUnavailable", "MacroCalendarClient",
    "parse_fomc_calendar", "parse_bea_schedule", "classify_bea_release",
    "upsert_events", "mark_withdrawn", "record_source_state",
    "refresh_macro_calendar", "FETCHERS",
]
