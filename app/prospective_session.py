"""Deterministic latest-fully-completed US regular market session resolver.

Rule-based NYSE calendar (weekends + federal market holidays with Sat→Fri /
Sun→Mon observance + Good Friday). A session is FULLY completed only at/after the
regular 16:00 America/New_York close (early-close days are treated conservatively
as complete only at the regular close — this can only EXCLUDE the current day,
never wrongly include it). Pure; no provider, no DB, no network.

`market_calendar_version = us_market_calendar.v1`. Consistent with the shared
completed-daily-bar policy `ny_session_close.v1` (16:00 ET).
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

MARKET_CALENDAR_VERSION = "us_market_calendar.v1"
EXCHANGE_TZ = "America/New_York"
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)


def _easter(year: int) -> date:
    """Anonymous Gregorian algorithm (for Good Friday)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The nth (1-based) `weekday` (Mon=0) of a month."""
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    d = date(year, month, 28) + timedelta(days=4)  # first day of next month-ish
    d = date(year, month, 1)
    # last day of month
    nxt = date(year + (month == 12), (month % 12) + 1, 1)
    last = nxt - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def _observed(d: date) -> date:
    if d.weekday() == 5:      # Saturday -> Friday
        return d - timedelta(days=1)
    if d.weekday() == 6:      # Sunday -> Monday
        return d + timedelta(days=1)
    return d


def us_market_holidays(year: int) -> set:
    """NYSE full-day holidays (observed) for a year."""
    h = set()
    h.add(_observed(date(year, 1, 1)))              # New Year's Day
    h.add(_nth_weekday(year, 1, 0, 3))              # MLK (3rd Mon Jan)
    h.add(_nth_weekday(year, 2, 0, 3))              # Washington's Birthday (3rd Mon Feb)
    h.add(_easter(year) - timedelta(days=2))        # Good Friday
    h.add(_last_weekday(year, 5, 0))                # Memorial Day (last Mon May)
    if year >= 2022:
        h.add(_observed(date(year, 6, 19)))         # Juneteenth
    h.add(_observed(date(year, 7, 4)))              # Independence Day
    h.add(_nth_weekday(year, 9, 0, 1))              # Labor Day (1st Mon Sep)
    h.add(_nth_weekday(year, 11, 3, 4))             # Thanksgiving (4th Thu Nov)
    h.add(_observed(date(year, 12, 25)))            # Christmas
    return h


def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    return d not in us_market_holidays(d.year)


def _et(now_utc: datetime) -> datetime:
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    return now_utc.astimezone(ZoneInfo(EXCHANGE_TZ))


def resolve_latest_completed_session(now_utc: datetime) -> date:
    """Latest US regular session that is FULLY completed as of `now_utc`."""
    et = _et(now_utc)
    d = et.date()
    # today counts only if it is a trading day AND the regular close has passed
    if not (is_trading_day(d) and et.timetz().replace(tzinfo=None) >= REGULAR_CLOSE):
        d = d - timedelta(days=1)
    while not is_trading_day(d):
        d = d - timedelta(days=1)
    return d


def nth_trading_session_after(session_date: date, n: int) -> date:
    """The Nth US regular trading session STRICTLY AFTER `session_date`.

    Pure calendar arithmetic — no bars, no store, no clock. `n` must be >= 1.

    WHY THIS AND NOT "THE NTH STORED BAR"
    -------------------------------------
    Counting stored bars would let a gap in our own ingestion redefine the
    horizon: with one session's bar missing, the 5th stored bar is the 6th
    trading session and a "5D outcome" would quietly measure six days. It
    would also let the symbol's grid and the benchmark's grid drift apart, so
    a benchmark-relative figure would compare two different windows — the
    2026-08-31 misalignment defect, in a new place.

    The rule calendar here is the same one `resolve_latest_completed_session`
    uses, so a horizon resolved by this function and a completed session
    resolved by that one are always talking about the same grid. The price of
    a rule is that an UNSCHEDULED closure (a national day of mourning, a
    weather closure) is not in it; that shows up as a bar that never arrives
    for the resolved date, which the outcome engine reports as waiting-for-data
    rather than papering over. Honest, and visible.
    """
    if n < 1:
        raise ValueError("n must be >= 1")
    d = session_date
    remaining = int(n)
    # Bounded. 20 sessions is the longest horizon this project asks for, and
    # no run of consecutive non-trading days comes near this ceiling.
    for _ in range(remaining * 12 + 40):
        d = d + timedelta(days=1)
        if is_trading_day(d):
            remaining -= 1
            if remaining == 0:
                return d
    raise ValueError("no Nth trading session found within the bounded search")


def trading_sessions_between(start: date, end: date) -> int:
    """Trading sessions strictly after `start` and at or before `end`.

    Zero when `end` is not after `start`. Lets a grace window be expressed in
    SESSIONS rather than calendar days, so a weekend is never counted as two
    days of a symbol's data being late.
    """
    if end <= start:
        return 0
    count = 0
    d = start + timedelta(days=1)
    while d <= end:
        if is_trading_day(d):
            count += 1
        d = d + timedelta(days=1)
    return count


def session_cutoff_utc(session_date: date) -> datetime:
    """The regular-close instant (16:00 ET) of `session_date`, as UTC."""
    close_local = datetime.combine(session_date, REGULAR_CLOSE, ZoneInfo(EXCHANGE_TZ))
    return close_local.astimezone(timezone.utc)


def resolve_snapshot(now_utc: datetime) -> dict:
    session = resolve_latest_completed_session(now_utc)
    return {
        "snapshot_session_date": session.isoformat(),
        "snapshot_cutoff_at": session_cutoff_utc(session).isoformat(),
        "market_calendar_version": MARKET_CALENDAR_VERSION,
    }


__all__ = [
    "MARKET_CALENDAR_VERSION", "EXCHANGE_TZ", "REGULAR_OPEN", "REGULAR_CLOSE",
    "us_market_holidays", "is_trading_day",
    "resolve_latest_completed_session", "session_cutoff_utc", "resolve_snapshot",
    "nth_trading_session_after", "trading_sessions_between",
]
