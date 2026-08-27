"""Deterministic catalyst context for the Smart Scanner product API.

PURE: no DB, no network, no provider. Given already-fetched event rows and a
scan session date, every output is a deterministic function of stored facts.

WHAT THIS IS NOT
----------------
Catalyst context sits BESIDE the strategy result. It does not change the
candidate verdict, the Wyckoff evaluation, the attention tier, or any gate, and
it produces no score. The product says "this setup exists, AND earnings are
approaching" — never "this setup is better/worse because earnings are
approaching". That second statement needs evidence we do not have.

SESSION SEMANTICS
-----------------
A calendar-date subtraction is not enough. An event dated on the scan session
itself may or may not have happened during that session:

    before_market   it happened BEFORE the session opened
    during_market   it happened inside the session
    after_market    it has NOT happened during that session yet
    unknown         we do not know, and we say so rather than assume

POINT-IN-TIME HONESTY
---------------------
For a historical scan the question is what was KNOWABLE then, not what we know
now. A past event is safe to report historically — it had already occurred and
was public. A FUTURE event is only claimable if we actually observed it on or
before that session (`observed_at <= as_of`); otherwise the answer is an
explicit `no_point_in_time_snapshot`, never a retroactively-applied estimate.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from app.prospective_session import is_trading_day

CATALYST_CONTEXT_CONTRACT_VERSION = "smart_scanner_catalyst_context.v1"

# ---- shared event vocabulary ------------------------------------------------ #
# Defined HERE, in the pure model, and imported by the ingestion layer — so the
# words the product speaks and the words we persist can never drift apart.
#
#   earnings                 an earnings ANNOUNCEMENT (a calendar event)
#   financial_report_filing  the date a periodic report was FILED (a past fact)
# They are not interchangeable and must never be presented as one another.
EVENT_EARNINGS = "earnings"
EVENT_FINANCIAL_REPORT_FILING = "financial_report_filing"
EVENT_FILING = EVENT_FINANCIAL_REPORT_FILING  # short alias, same value
EVENT_TYPES = (EVENT_EARNINGS, EVENT_FINANCIAL_REPORT_FILING)

# Session timing. `unknown` is first-class: we never guess a time of day.
TIMING_BEFORE_MARKET = "before_market"
TIMING_AFTER_MARKET = "after_market"
TIMING_DURING_MARKET = "during_market"
TIMING_UNKNOWN = "unknown"
SESSION_TIMINGS = (TIMING_BEFORE_MARKET, TIMING_AFTER_MARKET,
                   TIMING_DURING_MARKET, TIMING_UNKNOWN)

# How much the date can be trusted. `estimated` is never silently upgraded.
CERTAINTY_CONFIRMED = "confirmed"
CERTAINTY_ESTIMATED = "estimated"
CERTAINTY_FILED = "filed"
CERTAINTIES = (CERTAINTY_CONFIRMED, CERTAINTY_ESTIMATED, CERTAINTY_FILED)

# ---- availability ----------------------------------------------------------- #
STATUS_AVAILABLE = "available"
STATUS_UNAVAILABLE = "unavailable"
STATUS_STALE = "stale"
CATALYST_STATUSES = (STATUS_AVAILABLE, STATUS_UNAVAILABLE, STATUS_STALE)

# ---- proximity -------------------------------------------------------------- #
# Windows are chosen A PRIORI from product meaning, never fitted to outcomes:
#
#   today      the event is dated on the scan session itself; session timing
#              then decides whether it has already happened
#   imminent   1-2 trading sessions away — you are effectively on top of it
#   near       3-7 trading sessions — inside the next week and a half
#   upcoming   8-21 trading sessions — on the horizon, worth knowing
#   distant    more than 21 sessions ahead, or more than 5 sessions behind —
#              a KNOWN event that is simply too far away to change today's
#              read. NOT surfaced; silence is better than noise. Distinct
#              from `none_known`, which means we looked and found nothing.
#   recent     occurred within the last 5 trading sessions
#
PROXIMITY_TODAY = "today"
PROXIMITY_IMMINENT = "imminent"
PROXIMITY_NEAR = "near"
PROXIMITY_UPCOMING = "upcoming"
PROXIMITY_DISTANT = "distant"
PROXIMITY_RECENT = "recent"
PROXIMITY_NONE_KNOWN = "none_known"

PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_IMMINENT, PROXIMITY_NEAR,
               PROXIMITY_UPCOMING, PROXIMITY_DISTANT, PROXIMITY_RECENT,
               PROXIMITY_NONE_KNOWN)

IMMINENT_MAX_SESSIONS = 2
NEAR_MAX_SESSIONS = 7
UPCOMING_MAX_SESSIONS = 21
RECENT_MAX_SESSIONS = 5

#: Proximities the UI is allowed to surface. `distant`/`none_known` stay silent.
NOTABLE_PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_IMMINENT, PROXIMITY_NEAR,
                       PROXIMITY_UPCOMING, PROXIMITY_RECENT)

# ---- same-session resolution ------------------------------------------------ #
SAME_SESSION_BEFORE_OPEN = "occurred_before_open"
SAME_SESSION_INTRADAY = "occurred_intraday"
SAME_SESSION_AFTER_CLOSE = "after_close_not_yet_occurred"
SAME_SESSION_UNKNOWN = "timing_unknown"

# ---- freshness -------------------------------------------------------------- #
#: A successful refresh older than this makes the context `stale` — the product
#: must never present old event dates as current.
FRESHNESS_MAX_AGE_HOURS = 36

# ---- unavailability reasons ------------------------------------------------- #
REASON_SOURCE_UNAVAILABLE = "source_unavailable"
REASON_NEVER_REFRESHED = "never_refreshed"
REASON_STALE_REFRESH = "stale_refresh"
REASON_NO_POINT_IN_TIME = "no_point_in_time_snapshot"


# --------------------------------------------------------------------------- #
# trading-day arithmetic
# --------------------------------------------------------------------------- #

def trading_sessions_between(start: date, end: date, *, cap: int = 400) -> Optional[int]:
    """Count trading sessions strictly after `start` up to and including `end`.

    Negative when `end` precedes `start`. Returns None beyond `cap` sessions so
    a corrupt date can never spin. Uses the repository's own market calendar,
    so weekends and US market holidays are excluded.
    """
    if start == end:
        return 0
    step = 1 if end > start else -1
    sessions = 0
    cursor = start
    guard = 0
    while cursor != end:
        guard += 1
        if guard > cap * 3:
            return None
        cursor = cursor + timedelta(days=step)
        if is_trading_day(cursor):
            sessions += step
        if abs(sessions) > cap:
            return None
    return sessions


def classify_proximity(sessions_until: Optional[int]) -> str:
    """Map a signed trading-session distance onto the product vocabulary."""
    if sessions_until is None:
        return PROXIMITY_NONE_KNOWN
    if sessions_until == 0:
        return PROXIMITY_TODAY
    if sessions_until > 0:
        if sessions_until <= IMMINENT_MAX_SESSIONS:
            return PROXIMITY_IMMINENT
        if sessions_until <= NEAR_MAX_SESSIONS:
            return PROXIMITY_NEAR
        if sessions_until <= UPCOMING_MAX_SESSIONS:
            return PROXIMITY_UPCOMING
        return PROXIMITY_DISTANT
    if -sessions_until <= RECENT_MAX_SESSIONS:
        return PROXIMITY_RECENT
    return PROXIMITY_DISTANT


def resolve_same_session(session_timing: Optional[str]) -> str:
    """For an event dated ON the scan session, did it already happen?"""
    if session_timing == "before_market":
        return SAME_SESSION_BEFORE_OPEN
    if session_timing == "during_market":
        return SAME_SESSION_INTRADAY
    if session_timing == "after_market":
        return SAME_SESSION_AFTER_CLOSE
    return SAME_SESSION_UNKNOWN


def is_notable(proximity: str) -> bool:
    return proximity in NOTABLE_PROXIMITIES


# --------------------------------------------------------------------------- #
# freshness
# --------------------------------------------------------------------------- #

def evaluate_freshness(source_state: Optional[Dict[str, Any]], *,
                       now: datetime) -> Dict[str, Any]:
    """Turn a persisted source-state row into an explicit availability verdict.

    Distinguishes three situations an empty table cannot: never refreshed, the
    source is unavailable to us, and a refresh that succeeded but has gone
    stale.
    """
    if not source_state:
        return {"status": STATUS_UNAVAILABLE, "reason": REASON_NEVER_REFRESHED,
                "last_refresh_at": None, "last_success_at": None,
                "age_hours": None, "detail": None}

    status_token = source_state.get("status")
    last_success = source_state.get("last_success_at")
    last_refresh = source_state.get("last_refresh_at")
    detail = source_state.get("detail")

    if status_token != "ok" or last_success is None:
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
# event selection — point-in-time honest
# --------------------------------------------------------------------------- #

def select_relevant_event(
    events: Sequence[Dict[str, Any]],
    *,
    as_of_session: date,
    event_type: str,
    require_point_in_time: bool = True,
) -> Dict[str, Any]:
    """Pick the one event that matters for this session, and say how sure we are.

    Preference order: an event dated ON the session, then the nearest upcoming
    one, then the most recent past one. A FUTURE event is only offered when we
    actually observed it on or before the session — otherwise reporting it would
    be attaching later knowledge to an earlier scan.
    """
    typed = [e for e in events if e.get("event_type") == event_type
             and isinstance(e.get("event_date"), date)]
    if not typed:
        return {"event": None, "excluded_future_for_lookahead": False}

    same_day = [e for e in typed if e["event_date"] == as_of_session]
    if same_day:
        return {"event": same_day[0], "excluded_future_for_lookahead": False}

    future = sorted((e for e in typed if e["event_date"] > as_of_session),
                    key=lambda e: e["event_date"])
    past = sorted((e for e in typed if e["event_date"] < as_of_session),
                  key=lambda e: e["event_date"], reverse=True)

    excluded = False
    if future and require_point_in_time:
        knowable = [e for e in future if _observed_by(e, as_of_session)]
        excluded = len(knowable) < len(future)
        future = knowable

    if future:
        return {"event": future[0], "excluded_future_for_lookahead": excluded}
    if past:
        return {"event": past[0], "excluded_future_for_lookahead": excluded}
    return {"event": None, "excluded_future_for_lookahead": excluded}


def _observed_by(event: Dict[str, Any], as_of_session: date) -> bool:
    observed = event.get("observed_at")
    if not isinstance(observed, datetime):
        return False
    return _aware(observed).date() <= as_of_session


# --------------------------------------------------------------------------- #
# the product objects
# --------------------------------------------------------------------------- #

def build_event_context(
    events: Sequence[Dict[str, Any]],
    *,
    event_type: str,
    as_of_session: Optional[date],
    freshness: Dict[str, Any],
    require_point_in_time: bool = True,
) -> Dict[str, Any]:
    """One catalyst block: what the event is, when, how sure, and how close."""
    base: Dict[str, Any] = {
        "status": freshness["status"],
        "reason": freshness.get("reason"),
        "event_date": None,
        "timing": None,
        "certainty": None,
        "fiscal_period": None,
        "fiscal_year": None,
        "source": None,
        "source_reference": None,
        "observed_at": None,
        "sessions_until": None,
        "calendar_days_until": None,
        "proximity": PROXIMITY_NONE_KNOWN,
        "same_session": None,
        "notable": False,
    }

    if freshness["status"] == STATUS_UNAVAILABLE or as_of_session is None:
        if as_of_session is None:
            base["status"] = STATUS_UNAVAILABLE
            base["reason"] = base["reason"] or REASON_NEVER_REFRESHED
        return base

    picked = select_relevant_event(
        events, as_of_session=as_of_session, event_type=event_type,
        require_point_in_time=require_point_in_time)
    event = picked["event"]

    if event is None:
        if picked["excluded_future_for_lookahead"]:
            base["status"] = STATUS_UNAVAILABLE
            base["reason"] = REASON_NO_POINT_IN_TIME
        return base

    event_date = event["event_date"]
    sessions = trading_sessions_between(as_of_session, event_date)
    proximity = classify_proximity(sessions)

    base.update({
        "event_date": event_date.isoformat(),
        "timing": event.get("session_timing"),
        "certainty": event.get("certainty"),
        "fiscal_period": event.get("fiscal_period"),
        "fiscal_year": event.get("fiscal_year"),
        "source": event.get("source"),
        "source_reference": event.get("source_reference"),
        "observed_at": _iso(event.get("observed_at")),
        "sessions_until": sessions,
        "calendar_days_until": (event_date - as_of_session).days,
        "proximity": proximity,
        "same_session": (resolve_same_session(event.get("session_timing"))
                         if proximity == PROXIMITY_TODAY else None),
        "notable": is_notable(proximity),
    })
    if picked["excluded_future_for_lookahead"]:
        base["reason"] = REASON_NO_POINT_IN_TIME
    return base


def build_catalyst_context(
    events: Sequence[Dict[str, Any]],
    *,
    as_of_session: Optional[date],
    earnings_freshness: Dict[str, Any],
    filings_freshness: Dict[str, Any],
    require_point_in_time: bool = True,
) -> Dict[str, Any]:
    """The full per-symbol catalyst object.

    `earnings` and `last_financial_report` are separate blocks on purpose: a
    report FILING date is not an earnings-announcement date, and presenting one
    as the other would invent precision the data does not carry.
    """
    return {
        "contract_version": CATALYST_CONTEXT_CONTRACT_VERSION,
        "as_of_session": as_of_session.isoformat() if as_of_session else None,
        "earnings": build_event_context(
            events, event_type="earnings", as_of_session=as_of_session,
            freshness=earnings_freshness,
            require_point_in_time=require_point_in_time),
        "last_financial_report": build_event_context(
            events, event_type="financial_report_filing",
            as_of_session=as_of_session, freshness=filings_freshness,
            require_point_in_time=require_point_in_time),
    }


def build_row_catalyst(catalyst_context: Dict[str, Any]) -> Dict[str, Any]:
    """The COMPACT subset a list row carries.

    Only what identifies a potentially important nearby catalyst. `notable` is
    the gate the UI uses to stay silent when there is nothing to say — a row
    must not carry "no earnings nearby".
    """
    earnings = catalyst_context["earnings"]
    report = catalyst_context["last_financial_report"]
    return {
        "earnings_status": earnings["status"],
        "earnings_proximity": earnings["proximity"],
        "earnings_sessions_until": earnings["sessions_until"],
        "earnings_timing": earnings["timing"],
        "earnings_certainty": earnings["certainty"],
        "earnings_notable": earnings["notable"],
        "last_report_proximity": report["proximity"],
        "last_report_sessions_until": report["sessions_until"],
        "last_report_notable": report["notable"],
    }


def empty_catalyst_context(*, reason: str = REASON_NEVER_REFRESHED) -> Dict[str, Any]:
    """A fully-unavailable context.

    Used when catalyst loading fails entirely: the scanner keeps working and
    every catalyst field says `unavailable` rather than the request failing.
    """
    unavailable = {"status": STATUS_UNAVAILABLE, "reason": reason,
                   "last_refresh_at": None, "last_success_at": None,
                   "age_hours": None, "detail": None}
    return build_catalyst_context(
        [], as_of_session=None,
        earnings_freshness=unavailable, filings_freshness=unavailable)


__all__ = [
    "CATALYST_CONTEXT_CONTRACT_VERSION",
    "EVENT_EARNINGS", "EVENT_FINANCIAL_REPORT_FILING", "EVENT_FILING",
    "EVENT_TYPES", "SESSION_TIMINGS", "CERTAINTIES",
    "TIMING_BEFORE_MARKET", "TIMING_AFTER_MARKET",
    "TIMING_DURING_MARKET", "TIMING_UNKNOWN",
    "CERTAINTY_CONFIRMED", "CERTAINTY_ESTIMATED", "CERTAINTY_FILED",
    "STATUS_AVAILABLE", "STATUS_UNAVAILABLE", "STATUS_STALE", "CATALYST_STATUSES",
    "PROXIMITY_TODAY", "PROXIMITY_IMMINENT", "PROXIMITY_NEAR",
    "PROXIMITY_UPCOMING", "PROXIMITY_DISTANT", "PROXIMITY_RECENT",
    "PROXIMITY_NONE_KNOWN", "PROXIMITIES", "NOTABLE_PROXIMITIES",
    "IMMINENT_MAX_SESSIONS", "NEAR_MAX_SESSIONS", "UPCOMING_MAX_SESSIONS",
    "RECENT_MAX_SESSIONS",
    "SAME_SESSION_BEFORE_OPEN", "SAME_SESSION_INTRADAY",
    "SAME_SESSION_AFTER_CLOSE", "SAME_SESSION_UNKNOWN",
    "FRESHNESS_MAX_AGE_HOURS",
    "REASON_SOURCE_UNAVAILABLE", "REASON_NEVER_REFRESHED",
    "REASON_STALE_REFRESH", "REASON_NO_POINT_IN_TIME",
    "trading_sessions_between", "classify_proximity", "resolve_same_session",
    "is_notable", "evaluate_freshness", "select_relevant_event",
    "build_event_context", "build_catalyst_context", "build_row_catalyst",
    "empty_catalyst_context",
]
