"""Analyst grade CHANGE events — ingested for research, never displayed.

    /stable/grades?symbol=NVDA  ->  1138 rows back to 2012, on the current key

WHAT QUESTION THIS ANSWERS
--------------------------
"Who changed their mind about this company, and when." That is a different
question from the three the scanner already answers — what the price did, what
the company announced, and what another chart-reading system claimed — and it
is the only one of the four that is somebody's published, dated, attributable
ACTION.

WHY CHANGES AND NOT RATIOS
--------------------------
The same key also serves consensus ratings, price-target consensus, TTM ratios,
key metrics and growth. None of them is built here, deliberately. A P/E is true
for months; it cannot answer "what is different today", which is the only
question a daily scanner is asking. A downgrade has a date. Static fundamentals
are a fair thing to want and a poor thing to add to a scanner that already has
six evidence dimensions competing for one screen — and with FMP restricted to
internal use, they would have had no product surface to justify the weight.

POINT IN TIME, AND WHY IT GIVES UP A DAY
----------------------------------------
The provider publishes a DATE and no clock. A grade that landed at 06:00 ET was
actionable that session; one that landed at 17:00 ET was not, and nothing in
the payload distinguishes them. So `session_date` is the first trading session
STRICTLY AFTER the event date — always. That deliberately forfeits up to a
day of edge in exchange for a guarantee: no measurement built on these rows can
ever be reading an outcome it could not have traded.

LICENCE — THIS IS THE WHOLE REASON FOR THE BOUNDARY
---------------------------------------------------
FMP's individual plans are personal and non-commercial and forbid integrating
the data into tools accessible by third parties. So this module writes to the
database and stops: no router imports it, the Product API's role holds no
privilege on `analyst_grade_events`, and `app/source_licensing.py` states the
position in one place with a test behind it. Paraphrasing a downgrade into a
product field would breach the same term — the licence restricts the data, not
the spelling.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

from app.external_discovery import (DiscoverySourceUnavailable,
                                    FmpStableClient, normalize_symbol)
from app.prospective_session import is_trading_day
from app.source_licensing import LICENSING_INTERNAL_ONLY

SOURCE_FMP = "fmp"

#: `catalyst_source_state` key. Distinct from the movers key so a working
#: discovery refresh can never make a broken analyst refresh look healthy.
SOURCE_STATE_FMP_GRADES = "external_fmp_analyst_grades"

GRADES_PATH = "grades"

#: How far back a refresh reads. The provider returns the FULL history on every
#: call regardless, so this bounds what we STORE, not what we fetch: a first run
#: can be given a wide window to build the descriptive history, and the daily
#: run a narrow one so it writes only what is new.
DEFAULT_LOOKBACK_DAYS = 30

#: Guard against a pathological payload. The largest real history seen was
#: ~1,150 rows for a mega-cap over fourteen years.
MAX_ROWS_PER_SYMBOL = 5000

STATE_OK = "ok"
STATE_UNAVAILABLE = "unavailable"
STATE_ERROR = "error"
STATE_NEVER_RUN = "never_run"

# --------------------------------------------------------------------------- #
# action vocabulary
# --------------------------------------------------------------------------- #
ACTION_UPGRADE = "upgrade"
ACTION_DOWNGRADE = "downgrade"
ACTION_MAINTAIN = "maintain"
ACTION_INITIALISE = "initialise"
ACTION_OTHER = "other"

ACTIONS = (ACTION_UPGRADE, ACTION_DOWNGRADE, ACTION_MAINTAIN,
           ACTION_INITIALISE, ACTION_OTHER)

#: The two that are genuinely a CHANGE of view. `maintain` is by far the most
#: common row in the feed and is a reaffirmation, not news; keeping it stored
#: but separable is what lets a descriptive study compare the two populations
#: instead of assuming the difference.
DIRECTIONAL_ACTIONS = (ACTION_UPGRADE, ACTION_DOWNGRADE)

_ACTION_MAP = {
    "upgrade": ACTION_UPGRADE, "upgraded": ACTION_UPGRADE,
    "downgrade": ACTION_DOWNGRADE, "downgraded": ACTION_DOWNGRADE,
    "maintain": ACTION_MAINTAIN, "maintains": ACTION_MAINTAIN,
    "maintained": ACTION_MAINTAIN, "reiterate": ACTION_MAINTAIN,
    "reiterated": ACTION_MAINTAIN, "hold": ACTION_MAINTAIN,
    "initialise": ACTION_INITIALISE, "initialize": ACTION_INITIALISE,
    "initialised": ACTION_INITIALISE, "initialized": ACTION_INITIALISE,
    "initiate": ACTION_INITIALISE, "initiated": ACTION_INITIALISE,
    "initiates": ACTION_INITIALISE, "init": ACTION_INITIALISE,
}


def normalize_action(raw: Any) -> str:
    """The provider's word -> ours. Unknown maps to `other`, never guessed.

    `hold` becomes `maintain` because the feed uses it for a reaffirmed rating
    rather than for a move TO a Hold rating — the latter arrives as a downgrade
    or an upgrade with `newGrade` = Hold, which is why the grades themselves are
    stored verbatim beside the action.
    """
    token = str(raw or "").strip().lower()
    return _ACTION_MAP.get(token, ACTION_OTHER)


def _text(value: Any, *, limit: int = 128) -> Optional[str]:
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def parse_event_date(value: Any) -> Optional[date]:
    """`'2026-08-17'` -> date. Anything else -> None, and the row is dropped."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def next_session(event_day: date, *, cap_days: int = 10) -> Optional[date]:
    """The first trading session STRICTLY AFTER `event_day`.

    See the module docstring: the provider gives no clock, so same-session
    actionability cannot be established and is therefore never assumed.
    """
    cursor = event_day
    for _ in range(cap_days):
        cursor = cursor + timedelta(days=1)
        if is_trading_day(cursor):
            return cursor
    return None


def normalize_grade_event(row: Dict[str, Any], *, observed_at: datetime,
                          universe: Optional[Set[str]] = None,
                          symbol_hint: Optional[str] = None,
                          ) -> Optional[Dict[str, Any]]:
    """One feed entry -> one storable row, or None when it is not usable.

    Dropped rather than mangled: an unparseable date or an unshaped ticker
    would produce a row that looks like evidence and is not.
    """
    if not isinstance(row, dict):
        return None
    symbol = normalize_symbol(row.get("symbol") or symbol_hint)
    if symbol is None:
        return None
    event_day = parse_event_date(row.get("date"))
    if event_day is None:
        return None
    session = next_session(event_day)
    if session is None:
        return None
    company = _text(row.get("gradingCompany"))
    if not company:
        return None
    action_raw = _text(row.get("action"), limit=32) or ""
    return {
        "source": SOURCE_FMP,
        "symbol": symbol,
        "event_date": event_day,
        "session_date": session,
        "grading_company": company,
        "previous_grade": _text(row.get("previousGrade"), limit=64),
        "new_grade": _text(row.get("newGrade"), limit=64),
        "action": action_raw or "unspecified",
        "action_normalized": normalize_action(action_raw),
        "in_scanner_universe": bool(universe and symbol in universe),
        "licensing_visibility": LICENSING_INTERNAL_ONLY,
        "observed_at": observed_at,
    }


def normalize_grade_history(rows: Sequence[Dict[str, Any]], *,
                            observed_at: datetime,
                            since: Optional[date] = None,
                            universe: Optional[Set[str]] = None,
                            symbol_hint: Optional[str] = None,
                            limit: int = MAX_ROWS_PER_SYMBOL,
                            ) -> List[Dict[str, Any]]:
    """A whole per-symbol history -> bounded, de-duplicated, storable rows."""
    out: List[Dict[str, Any]] = []
    seen: Set[Any] = set()
    for row in rows[:limit]:
        event = normalize_grade_event(row, observed_at=observed_at,
                                      universe=universe,
                                      symbol_hint=symbol_hint)
        if event is None:
            continue
        if since is not None and event["event_date"] < since:
            continue
        key = (event["symbol"], event["event_date"], event["grading_company"],
               event["action"], event["previous_grade"], event["new_grade"])
        if key in seen:
            continue
        seen.add(key)
        out.append(event)
    return out


# --------------------------------------------------------------------------- #
# persistence
# --------------------------------------------------------------------------- #

INSERT_SQL = """
INSERT INTO public.analyst_grade_events (
    source, symbol, event_date, session_date, grading_company,
    previous_grade, new_grade, action, action_normalized,
    in_scanner_universe, licensing_visibility, observed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (source, symbol, event_date, grading_company, action,
             COALESCE(previous_grade, ''), COALESCE(new_grade, ''))
DO NOTHING
RETURNING id
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


async def insert_events(conn, events: Iterable[Dict[str, Any]],
                        ) -> Dict[str, int]:
    """Append-only. A grade change is a historical fact and never updates.

    `DO NOTHING` rather than `DO UPDATE`: if the provider re-emits a row with
    the same identity but different content, the honest response is to keep the
    version we recorded at the time, not to quietly rewrite history.
    """
    stats = {"seen": 0, "inserted": 0, "duplicate": 0}
    for event in events:
        stats["seen"] += 1
        row = await conn.fetchrow(
            INSERT_SQL,
            event["source"], event["symbol"], event["event_date"],
            event["session_date"], event["grading_company"],
            event.get("previous_grade"), event.get("new_grade"),
            event["action"], event["action_normalized"],
            event["in_scanner_universe"], event["licensing_visibility"],
            event["observed_at"])
        stats["inserted" if row is not None else "duplicate"] += 1
    return stats


async def record_source_state(conn, status: str, *, symbols_covered: int = 0,
                              written: int = 0, detail: str = "",
                              now: Optional[datetime] = None) -> None:
    moment = now or datetime.now(timezone.utc)
    await conn.execute(
        SOURCE_STATE_SQL, SOURCE_STATE_FMP_GRADES, status, moment,
        moment if status == STATE_OK else None,
        symbols_covered, written, detail[:400] or None)


async def refresh_analyst_grades(
    conn, client: Optional[FmpStableClient], *,
    symbols: Sequence[str],
    universe: Optional[Set[str]] = None,
    since: Optional[date] = None,
    now: Optional[datetime] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Dict[str, Any]:
    """One idempotent refresh over the named symbols.

    Per-symbol isolation: one ticker the provider refuses must not cost the
    other twenty-four their refresh, and the failures are named in the summary
    rather than folded into a single count. A missing credential is the ordinary
    case and reports `unavailable`, never an error.

    `symbols` is passed in rather than read here: this module has no opinion
    about which symbols matter, and giving it one would be the first step
    towards it deciding what the scanner looks at.
    """
    moment = now or datetime.now(timezone.utc)
    cutoff = since or (moment.date() - timedelta(days=lookback_days))
    summary: Dict[str, Any] = {
        "source": SOURCE_STATE_FMP_GRADES,
        "since": cutoff.isoformat(),
        "symbols_requested": len(symbols),
        "symbols": {},
    }

    if client is None:
        await record_source_state(conn, STATE_UNAVAILABLE,
                                  detail="missing_api_key", now=moment)
        summary.update({"status": STATE_UNAVAILABLE,
                        "reason": "missing_api_key"})
        return summary
    if not symbols:
        await record_source_state(conn, STATE_UNAVAILABLE,
                                  detail="no_symbols", now=moment)
        summary.update({"status": STATE_UNAVAILABLE, "reason": "no_symbols"})
        return summary

    total = {"seen": 0, "inserted": 0, "duplicate": 0}
    failures: List[str] = []
    covered = 0

    for symbol in symbols:
        try:
            await client.pause()
            rows = await client.get_list(GRADES_PATH, {"symbol": symbol})
            events = normalize_grade_history(
                rows, observed_at=moment, since=cutoff, universe=universe,
                symbol_hint=symbol)
            stats = await insert_events(conn, events)
            for key in total:
                total[key] += stats[key]
            covered += 1
            summary["symbols"][symbol] = stats
        except DiscoverySourceUnavailable as exc:
            failures.append(f"{symbol}:{exc.reason}")
            summary["symbols"][symbol] = {"status": STATE_UNAVAILABLE,
                                          "reason": exc.reason}
        except Exception as exc:
            failures.append(f"{symbol}:{type(exc).__name__}")
            summary["symbols"][symbol] = {"status": STATE_ERROR,
                                          "reason": type(exc).__name__}

    if covered == 0:
        await record_source_state(conn, STATE_UNAVAILABLE,
                                  detail="; ".join(failures)[:400] or "no rows",
                                  now=moment)
        summary.update({"status": STATE_UNAVAILABLE, "failures": failures})
        return summary

    # Partial success reported AS success, with the failures named: twenty-four
    # symbols refreshed is genuinely better than none, and hiding which one
    # broke would make it undiagnosable.
    await record_source_state(conn, STATE_OK, symbols_covered=covered,
                              written=total["inserted"],
                              detail="; ".join(failures)[:400], now=moment)
    summary.update({"status": STATE_OK, "failures": failures, **total})
    return summary


# --------------------------------------------------------------------------- #
# research reads (ops/analysis only — never the Product API)
# --------------------------------------------------------------------------- #

RECENT_CHANGES_SQL = """
SELECT symbol, event_date, session_date, grading_company,
       previous_grade, new_grade, action, action_normalized,
       in_scanner_universe
FROM public.analyst_grade_events
WHERE session_date >= $1
  AND ($2::boolean IS NOT TRUE OR action_normalized = ANY($3::text[]))
ORDER BY event_date DESC, symbol, grading_company
LIMIT $4
"""

CHANGE_COUNTS_SQL = """
SELECT symbol,
       count(*) FILTER (WHERE action_normalized = 'upgrade')   AS upgrades,
       count(*) FILTER (WHERE action_normalized = 'downgrade') AS downgrades,
       count(*) FILTER (WHERE action_normalized = 'maintain')  AS maintains,
       count(*)                                                AS total,
       max(event_date)                                         AS last_event
FROM public.analyst_grade_events
WHERE session_date >= $1
GROUP BY symbol
ORDER BY (count(*) FILTER (WHERE action_normalized IN ('upgrade','downgrade')))
         DESC, symbol
"""


async def recent_changes(conn, *, since: date, directional_only: bool = True,
                         limit: int = 100) -> List[Dict[str, Any]]:
    rows = await conn.fetch(RECENT_CHANGES_SQL, since, directional_only,
                            list(DIRECTIONAL_ACTIONS), limit)
    return [dict(r) for r in rows]


async def change_counts(conn, *, since: date) -> List[Dict[str, Any]]:
    """Per-symbol upgrade/downgrade/maintain counts. A COUNT, not a score.

    Ordered by how many directional actions a symbol drew, which is a statement
    about analyst attention and nothing else. No scanner path reads this.
    """
    rows = await conn.fetch(CHANGE_COUNTS_SQL, since)
    return [dict(r) for r in rows]


__all__ = [
    "SOURCE_FMP", "SOURCE_STATE_FMP_GRADES", "GRADES_PATH",
    "DEFAULT_LOOKBACK_DAYS", "MAX_ROWS_PER_SYMBOL",
    "STATE_OK", "STATE_UNAVAILABLE", "STATE_ERROR", "STATE_NEVER_RUN",
    "ACTION_UPGRADE", "ACTION_DOWNGRADE", "ACTION_MAINTAIN",
    "ACTION_INITIALISE", "ACTION_OTHER", "ACTIONS", "DIRECTIONAL_ACTIONS",
    "normalize_action", "parse_event_date", "next_session",
    "normalize_grade_event", "normalize_grade_history",
    "insert_events", "record_source_state", "refresh_analyst_grades",
    "recent_changes", "change_counts",
]
