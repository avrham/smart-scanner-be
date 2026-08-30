"""Deterministic market-calendar context: what SCHEDULED event is nearby.

PURE: no DB, no network, no provider. Given already-fetched macro rows and a
session date, every output is a deterministic function of stored facts and the
calendar.

WHAT THIS IS AND IS NOT
-----------------------
It answers one question — "is a market-wide scheduled event close?" — and
stops. It does not say the event is risky, does not say the market will move,
does not produce a number, and does not touch the strategy, the attention tier,
the ordering or the confluence reading. "FOMC today" is a fact about a
calendar. Everything a trader might infer from it is theirs to infer.

WHY IT IS NOT PART OF MARKET REGIME
-----------------------------------
Market regime is a statement about what PRICE is doing, computed from bars we
hold. A macro event is a statement about what is SCHEDULED, computed from a
publisher's calendar. They are independent, and keeping them independent is the
only way a reader can tell "the tape is weak" from "the tape is weak and CPI is
Thursday" from "CPI is Thursday". No macro event ever changes a regime
classification, and there is a test that says so.

WHY IT IS NOT A PER-SYMBOL FIELD
--------------------------------
The same event is true for every row on the screen. Rendered per row it becomes
twenty-five copies of one fact, which reads as twenty-five findings and drowns
the per-symbol evidence that is actually different between rows. So this block
is scanner-level, and the symbol screen shows the same market-wide block
labelled as market-wide — never as something about that company.

WHAT THE SOURCES ACTUALLY PUBLISH
---------------------------------
A SCHEDULE. Not a consensus, not an actual, not a previous value. The Federal
Reserve publishes meeting dates; the BEA publishes release dates and times.
Neither publishes an expectation, so this module has no field for one. An
`unavailable` reading here means our refresh is broken — never that the
calendar is empty, which is a different and much rarer thing.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from app.news import session_close_utc
from app.source_licensing import product_visible_rows

MARKET_CALENDAR_CONTRACT_VERSION = "smart_scanner_market_calendar_context.v1"

# --------------------------------------------------------------------------- #
# sources
# --------------------------------------------------------------------------- #
SOURCE_FEDERAL_RESERVE = "federal_reserve"
SOURCE_BEA = "bea"
MACRO_SOURCES = (SOURCE_FEDERAL_RESERVE, SOURCE_BEA)

#: `catalyst_source_state` keys. Prefixed like the external sources so one
#: freshness table can hold every dimension without the names colliding.
SOURCE_STATE_PREFIX = "external_macro_"


def source_state_key(source: str) -> str:
    return f"{SOURCE_STATE_PREFIX}{source}"


# --------------------------------------------------------------------------- #
# event vocabulary
#
# Exactly the three types we ingest. CPI, PPI, nonfarm payrolls and the
# unemployment rate are absent because their primary publisher (bls.gov)
# refuses every request from this network — see the migration header. Naming
# them here with no ingestion behind them would read as coverage.
# --------------------------------------------------------------------------- #
EVENT_FOMC_RATE_DECISION = "fomc_rate_decision"
EVENT_GDP = "gdp"
EVENT_PCE = "pce"
EVENT_TYPES = (EVENT_FOMC_RATE_DECISION, EVENT_GDP, EVENT_PCE)

#: Short human labels. Deliberately terse: this text lands in a one-line banner
#: beside a scanner, not in a research note.
EVENT_LABELS: Dict[str, str] = {
    EVENT_FOMC_RATE_DECISION: "FOMC rate decision",
    EVENT_GDP: "GDP",
    EVENT_PCE: "PCE inflation",
}

#: Which types are market-moving enough to headline the banner when several
#: land in the same window. Order is by breadth of effect on US equities, not
#: by any measured impact — we measure nothing here.
EVENT_PRIORITY = {EVENT_FOMC_RATE_DECISION: 0, EVENT_PCE: 1, EVENT_GDP: 2}

LISTING_LISTED = "listed"
LISTING_WITHDRAWN = "withdrawn"

# --------------------------------------------------------------------------- #
# event status — derived, never stored twice
#
# The sources publish forward schedules and no cancellation flag, so:
#   scheduled  the date has not passed and the source still lists it
#   released   the date has passed (the release happened, per the calendar)
#   unknown    the source stopped listing a still-future event. That is "the
#              source no longer says this", NOT "it was cancelled" — we have no
#              standing to make the second claim.
# --------------------------------------------------------------------------- #
STATUS_SCHEDULED = "scheduled"
STATUS_RELEASED = "released"
STATUS_UNKNOWN = "unknown"
EVENT_STATUSES = (STATUS_SCHEDULED, STATUS_RELEASED, STATUS_UNKNOWN)


def event_status(scheduled: Optional[date], *, listing: Optional[str],
                 as_of: Optional[date]) -> str:
    if listing == LISTING_WITHDRAWN:
        return STATUS_UNKNOWN
    if scheduled is None or as_of is None:
        return STATUS_UNKNOWN
    return STATUS_RELEASED if scheduled < as_of else STATUS_SCHEDULED


# --------------------------------------------------------------------------- #
# proximity
#
# CALENDAR days, not trading sessions — unlike every other proximity in this
# repository, and for a specific reason. "CPI tomorrow" has to mean the next
# calendar day or the words are wrong; a Friday event described as "tomorrow"
# on a Thursday and still "tomorrow" on the Saturday would be nonsense. The
# earnings layer counts sessions because it answers "how many chances to trade
# before this"; this one answers "when, on a wall calendar".
# --------------------------------------------------------------------------- #
PROXIMITY_TODAY = "today"
PROXIMITY_TOMORROW = "tomorrow"
PROXIMITY_WITHIN_3_DAYS = "within_3_days"
PROXIMITY_RECENTLY_RELEASED = "recently_released"
PROXIMITY_NONE_NEARBY = "none_nearby"
PROXIMITY_UNAVAILABLE = "unavailable"

PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_TOMORROW, PROXIMITY_WITHIN_3_DAYS,
               PROXIMITY_RECENTLY_RELEASED, PROXIMITY_NONE_NEARBY,
               PROXIMITY_UNAVAILABLE)

#: Ordered by how close the event is. Used to pick ONE headline proximity from
#: several events, never to score anything.
PROXIMITY_ORDER = {PROXIMITY_TODAY: 0, PROXIMITY_TOMORROW: 1,
                   PROXIMITY_WITHIN_3_DAYS: 2,
                   PROXIMITY_RECENTLY_RELEASED: 3,
                   PROXIMITY_NONE_NEARBY: 4, PROXIMITY_UNAVAILABLE: 5}

WITHIN_MAX_DAYS = 3
RECENT_MAX_DAYS = 3

#: How far the block looks in each direction. Forward far enough that a reader
#: planning a week sees the next FOMC; back only far enough to say "that just
#: happened" without the banner becoming a history feed.
WINDOW_FORWARD_DAYS = 21
WINDOW_BACK_DAYS = RECENT_MAX_DAYS

MAX_ITEMS = 6


def classify_proximity(days_until: Optional[int]) -> str:
    """Signed calendar-day distance -> the product vocabulary."""
    if days_until is None:
        return PROXIMITY_UNAVAILABLE
    if days_until == 0:
        return PROXIMITY_TODAY
    if days_until == 1:
        return PROXIMITY_TOMORROW
    if 1 < days_until <= WITHIN_MAX_DAYS:
        return PROXIMITY_WITHIN_3_DAYS
    if days_until < 0 and -days_until <= RECENT_MAX_DAYS:
        return PROXIMITY_RECENTLY_RELEASED
    return PROXIMITY_NONE_NEARBY


def is_nearby(proximity: str) -> bool:
    """Close enough to say out loud in a one-line banner."""
    return proximity in (PROXIMITY_TODAY, PROXIMITY_TOMORROW,
                         PROXIMITY_WITHIN_3_DAYS)


# --------------------------------------------------------------------------- #
# availability — same vocabulary as every other dimension here
# --------------------------------------------------------------------------- #
AVAIL_AVAILABLE = "available"
AVAIL_UNAVAILABLE = "unavailable"
AVAIL_STALE = "stale"
AVAILABILITY_STATUSES = (AVAIL_AVAILABLE, AVAIL_UNAVAILABLE, AVAIL_STALE)

REASON_SOURCE_UNAVAILABLE = "source_unavailable"
REASON_NEVER_REFRESHED = "never_refreshed"
REASON_STALE_REFRESH = "stale_refresh"

#: A published schedule barely changes, so a long window would be tolerant to
#: the point of uselessness. Two days catches a broken daily refresh across a
#: weekend without crying wolf on one missed run.
FRESHNESS_MAX_AGE_HOURS = 48


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return _aware(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return None


def evaluate_freshness(source_state: Optional[Dict[str, Any]], *,
                       now: datetime) -> Dict[str, Any]:
    """One persisted source-state row -> an explicit availability verdict."""
    if not source_state:
        return {"status": AVAIL_UNAVAILABLE, "reason": REASON_NEVER_REFRESHED,
                "last_refresh_at": None, "last_success_at": None,
                "age_hours": None, "detail": None}

    last_success = source_state.get("last_success_at")
    last_refresh = source_state.get("last_refresh_at")
    detail = source_state.get("detail")

    if source_state.get("status") != "ok" or last_success is None:
        return {"status": AVAIL_UNAVAILABLE,
                "reason": REASON_SOURCE_UNAVAILABLE,
                "last_refresh_at": _iso(last_refresh),
                "last_success_at": _iso(last_success),
                "age_hours": None, "detail": detail}

    age = (now - _aware(last_success)).total_seconds() / 3600.0
    if age > FRESHNESS_MAX_AGE_HOURS:
        return {"status": AVAIL_STALE, "reason": REASON_STALE_REFRESH,
                "last_refresh_at": _iso(last_refresh),
                "last_success_at": _iso(last_success),
                "age_hours": round(age, 1), "detail": detail}

    return {"status": AVAIL_AVAILABLE, "reason": None,
            "last_refresh_at": _iso(last_refresh),
            "last_success_at": _iso(last_success),
            "age_hours": round(age, 1), "detail": detail}


def combine_freshness(per_source: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Dimension-level verdict from the per-source verdicts.

    BEST-OF, like the external hub: the question this block answers is "can we
    show a calendar at all". One publisher failing while the other answers is a
    working calendar with a hole in it, and the per-source verdicts travel
    alongside so the hole stays nameable.
    """
    if not per_source:
        return {"status": AVAIL_UNAVAILABLE, "reason": REASON_NEVER_REFRESHED,
                "last_refresh_at": None, "last_success_at": None,
                "age_hours": None, "detail": None, "per_source": {}}
    order = {AVAIL_AVAILABLE: 0, AVAIL_STALE: 1, AVAIL_UNAVAILABLE: 2}
    best = min(per_source.values(),
               key=lambda f: (order.get(f.get("status"), 3),
                              f.get("age_hours") if f.get("age_hours")
                              is not None else float("inf")))
    return {**best, "per_source": per_source}


# --------------------------------------------------------------------------- #
# point in time
# --------------------------------------------------------------------------- #

def is_visible_to_session(first_observed_at: Any,
                          as_of_session: date) -> bool:
    """Could this session have known the event was on the calendar?

    Gated on OUR FIRST OBSERVATION, not on the scheduled date. The schedule is
    public well in advance, but we only know what we actually read — and a
    meeting the Fed added on a Tuesday must not appear in a Monday's context
    just because its date is later.
    """
    if first_observed_at is None:
        return False
    return _aware(first_observed_at) <= session_close_utc(as_of_session)


# --------------------------------------------------------------------------- #
# item building
# --------------------------------------------------------------------------- #

def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else None


def build_event_item(row: Dict[str, Any], *,
                     as_of: Optional[date]) -> Dict[str, Any]:
    """One stored row as the product sees it.

    Carries the source's own title and the exact page it came from, so any
    claim on the screen can be checked against the publisher.
    """
    scheduled = _as_date(row.get("scheduled_date"))
    days_until = (scheduled - as_of).days if (scheduled and as_of) else None
    status = event_status(scheduled, listing=row.get("source_listing"),
                          as_of=as_of)
    local_time = row.get("scheduled_time_local")
    return {
        "event_type": row.get("event_type"),
        "label": EVENT_LABELS.get(row.get("event_type"), row.get("event_type")),
        # The publisher's own words, including the reference period our
        # normalised type drops ("Personal Income and Outlays, August 2026").
        "title": row.get("title"),
        "scheduled_date": _iso(scheduled),
        "scheduled_start_date": _iso(_as_date(row.get("scheduled_start_date"))),
        "scheduled_time_local": (local_time.isoformat()
                                 if isinstance(local_time, time) else None),
        "scheduled_timezone": row.get("scheduled_timezone"),
        "days_until": days_until,
        "proximity": classify_proximity(days_until),
        "status": status,
        # NULL, not false: "the publisher has not said" is different from "no
        # press conference", and only one of them is knowable in advance.
        "has_press_conference": row.get("has_press_conference"),
        "has_projections": row.get("has_projections"),
        "source": row.get("source"),
        "source_reference": row.get("source_reference"),
    }


def _sort_key(item: Dict[str, Any]) -> Any:
    """Nearest first; ties broken by breadth of effect, then by type name.

    Note what this is not: a ranking of importance. It decides which single
    event a one-line banner names when two land on the same day, and nothing
    downstream consumes the order.
    """
    days = item.get("days_until")
    return (abs(days) if days is not None else 10_000,
            0 if (days or 0) >= 0 else 1,
            EVENT_PRIORITY.get(item.get("event_type"), 99),
            item.get("event_type") or "")


def select_visible_events(rows: Sequence[Dict[str, Any]], *,
                          as_of_session: date) -> List[Dict[str, Any]]:
    """Rows this session was allowed to know about, inside the display window."""
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not is_visible_to_session(row.get("first_observed_at"),
                                     as_of_session):
            continue
        scheduled = _as_date(row.get("scheduled_date"))
        if scheduled is None:
            continue
        delta = (scheduled - as_of_session).days
        if delta > WINDOW_FORWARD_DAYS or delta < -WINDOW_BACK_DAYS:
            continue
        out.append(row)
    return out


# --------------------------------------------------------------------------- #
# the product block
# --------------------------------------------------------------------------- #

def live_anchor_session(now: datetime) -> Optional[date]:
    """The session a reader looking at the screen RIGHT NOW is standing in."""
    from app.news import effective_session as news_effective_session
    return news_effective_session(now)


def resolve_anchor_session(scan_session: Optional[date], *, now: datetime,
                           pinned: bool) -> Optional[date]:
    """Which session the calendar's PROXIMITY is counted from.

    Same rule, and the same reasoning, as `external_signals`: the scan runs on
    our schedule and the world does not. Counting "how many days until the
    Fed meets" from a scan that is five days old produces "in six days" for an
    event that is tomorrow — not a cautious answer, a wrong one.

      * pinned (the caller asked for a specific past session) -> that session,
        strictly. A historical view must not be told about a meeting announced
        afterwards.
      * default -> the later of the scan session and the session we are
        currently in.

    The point-in-time gate is untouched: an event is still only visible if we
    had OBSERVED it by the anchor session's close.
    """
    if pinned or scan_session is None:
        return scan_session
    live = live_anchor_session(now)
    return max(scan_session, live) if live else scan_session


def build_market_calendar_context(
    rows: Sequence[Dict[str, Any]],
    *,
    as_of_session: Optional[date],
    freshness: Dict[str, Any],
    sources: Sequence[Dict[str, Any]] = (),
    scan_session: Optional[date] = None,
    limit: int = MAX_ITEMS,
) -> Dict[str, Any]:
    """The market-wide calendar block.

    `applies_to` is in the contract rather than left implicit. The same block
    is served on the symbol screen, and a reader must never be able to take
    "FOMC tomorrow" on a symbol page as a statement about that company.

    `as_of_session` is the DISPLAY anchor (see `resolve_anchor_session`) and
    `scan_session` is the session the scanner result belongs to. Both are
    reported: when they differ, the reader is looking at a current calendar
    beside an older scan, and the screen has to be able to say so.
    """
    base: Dict[str, Any] = {
        "contract_version": MARKET_CALENDAR_CONTRACT_VERSION,
        "applies_to": "market_wide",
        "status": freshness.get("status"),
        "reason": freshness.get("reason"),
        "as_of_session": as_of_session.isoformat() if as_of_session else None,
        "scan_session": scan_session.isoformat() if scan_session else None,
        # False means the calendar is counted from a LATER day than the scan.
        # Without it a reader cannot tell that "FOMC tomorrow" is tomorrow for
        # them and was six days away for the scan sitting beside it.
        "anchor_is_scan_session": (scan_session is None
                                   or as_of_session == scan_session),
        "age_hours": freshness.get("age_hours"),
        "window_forward_days": WINDOW_FORWARD_DAYS,
        "window_back_days": WINDOW_BACK_DAYS,
        "proximity": PROXIMITY_UNAVAILABLE,
        "headline": None,
        "upcoming": [],
        "recent": [],
        "sources": [
            {"source": r.get("source"), "display_name": r.get("display_name"),
             "status": r.get("status"), "notes": r.get("notes"),
             "licensing_visibility": r.get("licensing_visibility"),
             "availability": (freshness.get("per_source") or {}).get(
                 r.get("source"))}
            for r in product_visible_rows(sources)
            if r.get("supports_calendar")
        ],
    }

    if freshness.get("status") == AVAIL_UNAVAILABLE or as_of_session is None:
        base["status"] = AVAIL_UNAVAILABLE
        base["reason"] = base.get("reason") or REASON_NEVER_REFRESHED
        return base

    items = [build_event_item(r, as_of=as_of_session)
             for r in select_visible_events(rows, as_of_session=as_of_session)]
    items.sort(key=_sort_key)

    upcoming = [i for i in items if (i["days_until"] or 0) >= 0][:limit]
    recent = [i for i in items if (i["days_until"] or 0) < 0][:limit]

    base["upcoming"] = upcoming
    base["recent"] = recent

    # ONE headline. A banner that lists everything is a feed, and a feed above
    # a scanner is noise; the rest stays available in `upcoming`.
    if upcoming and is_nearby(upcoming[0]["proximity"]):
        base["headline"] = upcoming[0]
        base["proximity"] = upcoming[0]["proximity"]
    elif recent:
        base["headline"] = recent[0]
        base["proximity"] = PROXIMITY_RECENTLY_RELEASED
    elif upcoming:
        base["headline"] = upcoming[0]
        base["proximity"] = PROXIMITY_NONE_NEARBY
    else:
        base["proximity"] = PROXIMITY_NONE_NEARBY
    return base


def empty_market_calendar_context(*, reason: str = REASON_NEVER_REFRESHED,
                                  ) -> Dict[str, Any]:
    """A fully-unavailable block, for when the calendar load fails entirely."""
    return build_market_calendar_context(
        [], as_of_session=None,
        freshness={"status": AVAIL_UNAVAILABLE, "reason": reason,
                   "last_refresh_at": None, "last_success_at": None,
                   "age_hours": None, "detail": None, "per_source": {}})


__all__ = [
    "MARKET_CALENDAR_CONTRACT_VERSION",
    "SOURCE_FEDERAL_RESERVE", "SOURCE_BEA", "MACRO_SOURCES",
    "SOURCE_STATE_PREFIX", "source_state_key",
    "EVENT_FOMC_RATE_DECISION", "EVENT_GDP", "EVENT_PCE", "EVENT_TYPES",
    "EVENT_LABELS", "EVENT_PRIORITY",
    "LISTING_LISTED", "LISTING_WITHDRAWN",
    "STATUS_SCHEDULED", "STATUS_RELEASED", "STATUS_UNKNOWN", "EVENT_STATUSES",
    "event_status",
    "PROXIMITY_TODAY", "PROXIMITY_TOMORROW", "PROXIMITY_WITHIN_3_DAYS",
    "PROXIMITY_RECENTLY_RELEASED", "PROXIMITY_NONE_NEARBY",
    "PROXIMITY_UNAVAILABLE", "PROXIMITIES", "PROXIMITY_ORDER",
    "WITHIN_MAX_DAYS", "RECENT_MAX_DAYS", "WINDOW_FORWARD_DAYS",
    "WINDOW_BACK_DAYS", "MAX_ITEMS", "classify_proximity", "is_nearby",
    "AVAIL_AVAILABLE", "AVAIL_UNAVAILABLE", "AVAIL_STALE",
    "AVAILABILITY_STATUSES", "REASON_SOURCE_UNAVAILABLE",
    "REASON_NEVER_REFRESHED", "REASON_STALE_REFRESH",
    "FRESHNESS_MAX_AGE_HOURS", "evaluate_freshness", "combine_freshness",
    "is_visible_to_session", "live_anchor_session", "resolve_anchor_session",
    "build_event_item", "select_visible_events",
    "build_market_calendar_context", "empty_market_calendar_context",
]
