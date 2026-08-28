"""Deterministic external-intelligence context for the Smart Scanner product API.

PURE: no DB, no network, no provider. Given already-fetched signal rows and a
scan session date, every output is a deterministic function of stored facts.

WHAT THIS IS NOT
----------------
External signals sit BESIDE the strategy result. They do not change the
candidate verdict, the Wyckoff evaluation, the attention tier, the ordering or
ENTER eligibility, and this module produces no score. The product says "our
scanner reads WATCH, AND an external system reported bullish on the 4H" —
never "therefore the odds are better".

WHY THIS IS A SIBLING OF `catalyst_context`, NOT A MEMBER OF IT
---------------------------------------------------------------
Earnings, news and 8-K all answer "what HAPPENED to this company". They are
events in the world, asserted by someone with standing to assert them — an
exchange calendar, a publisher, a registrant under regulation.

An external signal is a different kind of object entirely: it is another
system's OPINION about the same price series we already hold. Nesting an
opinion inside the catalyst block would let it borrow the authority of a
filing, and a reader scanning a column of "events" would have no way to tell
that one of them is a machine's guess. It gets its own top-level
`external_intelligence` block for exactly that reason.

THE GATE IS ARRIVAL, NOT THE SOURCE'S CLOCK
-------------------------------------------
Every other point-in-time layer here trusts a timestamp from an authority:
EDGAR's acceptance clock, a publisher's publication clock. This one cannot.
The timestamp arrives inside a payload posted by whoever holds the ingress
URL, so a backdated `source_timestamp` — malicious, or just a misconfigured
chart in the wrong timezone — would manufacture lookahead that looks exactly
like a finding.

    observed_at    when the SOURCE says it fired    evidence, displayed
    received_at    when it reached this server      provable
    effective_at   the visibility gate              = received_at

    a signal is visible to session S  iff  effective_at <= close(S)

`close(S)` is 16:00 America/New_York, DST included, borrowed from `app.news`
so no two layers here can drift apart on what a session is.

CONFIDENCE IS NEVER INVENTED
----------------------------
When a source supplies no confidence, `confidence` is None and the product
reports it as unavailable. It is not defaulted to 0.5, not derived from the
signal type, and not back-filled from anything. A number that was never
measured would be read as a measurement.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from app.catalyst import trading_sessions_between
# Re-exported deliberately: the SQL bound in the Product API, the SQL bound in
# the `external_signal_session_links` view and the gate here must all be the
# SAME clock, and importing it through this module makes that hard to break.
from app.news import session_close_utc

EXTERNAL_INTELLIGENCE_CONTRACT_VERSION = "smart_scanner_external_intelligence.v1"

#: The versioned payload contract a TradingView-family alert must declare.
#: Bumped only when the ACCEPTED SHAPE changes, so an old alert configured
#: months ago keeps working until it genuinely cannot.
TRADINGVIEW_CONTRACT_VERSION = "smart_scanner_tradingview_signal.v1"

# ---- sources ---------------------------------------------------------------- #
# The two that can actually receive data today. Everything else lives in the
# `external_signal_sources` registry with an honest status and no adapter —
# an adapter that cannot receive data reads as coverage and is a liability.
SOURCE_TRADINGVIEW = "tradingview"
SOURCE_AI_EDGE = "ai_edge"

#: Sources the webhook gateway will accept a delivery for.
WEBHOOK_SOURCES = (SOURCE_TRADINGVIEW, SOURCE_AI_EDGE)

#: `catalyst_source_state.source` rows this dimension reports freshness through.
SOURCE_STATE_PREFIX = "external_"


def source_state_key(source: str) -> str:
    """The freshness row name for one external source.

    Namespaced so an external source can never collide with a catalyst source
    in the shared `catalyst_source_state` table.
    """
    return f"{SOURCE_STATE_PREFIX}{source}"


# ---- signal type: the source's word, then ours ------------------------------ #
# Normalisation exists so the product can aggregate. It NEVER replaces the raw
# value — both are stored, and an unmapped word becomes `unknown` rather than
# being forced into the nearest bucket.
TYPE_ENTRY = "entry_signal"
TYPE_EXIT = "exit_signal"
TYPE_CLASSIFICATION = "classification"
TYPE_REGIME_FILTER = "regime_filter"
TYPE_TREND = "trend"
TYPE_REVERSAL = "reversal"
TYPE_BREAKOUT = "breakout"
TYPE_SETUP = "setup"
TYPE_ALERT = "alert"
TYPE_UNKNOWN = "unknown"

SIGNAL_TYPES = (
    TYPE_ENTRY, TYPE_EXIT, TYPE_CLASSIFICATION, TYPE_REGIME_FILTER,
    TYPE_TREND, TYPE_REVERSAL, TYPE_BREAKOUT, TYPE_SETUP, TYPE_ALERT,
    TYPE_UNKNOWN,
)

_TYPE_SYNONYMS: Dict[str, str] = {
    "entry": TYPE_ENTRY, "entry_signal": TYPE_ENTRY, "open": TYPE_ENTRY,
    "buy": TYPE_ENTRY, "sell": TYPE_ENTRY, "long": TYPE_ENTRY,
    "short": TYPE_ENTRY, "signal": TYPE_ENTRY,
    "exit": TYPE_EXIT, "exit_signal": TYPE_EXIT, "close": TYPE_EXIT,
    "take_profit": TYPE_EXIT, "stop": TYPE_EXIT, "stop_loss": TYPE_EXIT,
    "classification": TYPE_CLASSIFICATION, "prediction": TYPE_CLASSIFICATION,
    "ml_signal": TYPE_CLASSIFICATION, "lorentzian": TYPE_CLASSIFICATION,
    "regime": TYPE_REGIME_FILTER, "regime_filter": TYPE_REGIME_FILTER,
    "filter": TYPE_REGIME_FILTER, "kernel": TYPE_REGIME_FILTER,
    "trend_filter": TYPE_REGIME_FILTER,
    "trend": TYPE_TREND, "trend_change": TYPE_TREND, "kernel_flip": TYPE_TREND,
    "reversal": TYPE_REVERSAL, "pivot": TYPE_REVERSAL,
    "breakout": TYPE_BREAKOUT, "breakdown": TYPE_BREAKOUT,
    "setup": TYPE_SETUP, "watch": TYPE_SETUP,
    "alert": TYPE_ALERT, "notification": TYPE_ALERT,
}


def normalize_signal_type(raw: Any) -> str:
    """Source word -> our vocabulary. Unmapped becomes `unknown`, never a guess."""
    token = _token(raw)
    if not token:
        return TYPE_UNKNOWN
    if token in SIGNAL_TYPES:
        return token
    return _TYPE_SYNONYMS.get(token, TYPE_UNKNOWN)


# ---- direction -------------------------------------------------------------- #
# There is deliberately NO buy/sell in this vocabulary. Those are execution
# words, and this system records opinions and measures them — it places no
# orders. A source that says "buy" is recorded verbatim in `direction` and
# normalised to `bullish`, which is the claim it is actually making.
DIRECTION_BULLISH = "bullish"
DIRECTION_BEARISH = "bearish"
DIRECTION_NEUTRAL = "neutral"
DIRECTION_UNKNOWN = "unknown"

DIRECTIONS = (DIRECTION_BULLISH, DIRECTION_BEARISH, DIRECTION_NEUTRAL,
              DIRECTION_UNKNOWN)

#: Directions that express a view at all (as opposed to "no view" / "we could
#: not tell"). Used by the confluence reading, never as a score.
DIRECTIONAL = (DIRECTION_BULLISH, DIRECTION_BEARISH)

_DIRECTION_SYNONYMS: Dict[str, str] = {
    "bullish": DIRECTION_BULLISH, "bull": DIRECTION_BULLISH,
    "long": DIRECTION_BULLISH, "buy": DIRECTION_BULLISH,
    "up": DIRECTION_BULLISH, "trend_up": DIRECTION_BULLISH,
    "uptrend": DIRECTION_BULLISH, "positive": DIRECTION_BULLISH,
    "bearish": DIRECTION_BEARISH, "bear": DIRECTION_BEARISH,
    "short": DIRECTION_BEARISH, "sell": DIRECTION_BEARISH,
    "down": DIRECTION_BEARISH, "trend_down": DIRECTION_BEARISH,
    "downtrend": DIRECTION_BEARISH, "negative": DIRECTION_BEARISH,
    "neutral": DIRECTION_NEUTRAL, "flat": DIRECTION_NEUTRAL,
    "none": DIRECTION_NEUTRAL, "range": DIRECTION_NEUTRAL,
    "sideways": DIRECTION_NEUTRAL,
}


def normalize_direction(raw: Any) -> str:
    """Source word -> bullish / bearish / neutral / unknown.

    `unknown` is a real answer and is used whenever the source's word is not
    one this map knows. Forcing an unrecognised word into `neutral` would
    quietly turn "we cannot read this" into "the source saw nothing".
    """
    token = _token(raw)
    if not token:
        return DIRECTION_UNKNOWN
    if token in DIRECTIONS:
        return token
    return _DIRECTION_SYNONYMS.get(token, DIRECTION_UNKNOWN)


# ---- timeframe -------------------------------------------------------------- #
# TradingView's `{{interval}}` emits bare minute counts ("240") and letter codes
# ("D", "W", "M"). Other sources send human forms ("4h", "1d"). All are accepted;
# anything unrecognised normalises to None and the raw string is still stored.
_TIMEFRAME_MAP: Dict[str, str] = {
    "1": "1m", "1m": "1m", "1min": "1m",
    "3": "3m", "3m": "3m",
    "5": "5m", "5m": "5m",
    "15": "15m", "15m": "15m",
    "30": "30m", "30m": "30m",
    "60": "1h", "1h": "1h", "60m": "1h", "hourly": "1h",
    "120": "2h", "2h": "2h",
    "240": "4h", "4h": "4h",
    "d": "1d", "1d": "1d", "daily": "1d", "1day": "1d",
    "w": "1w", "1w": "1w", "weekly": "1w",
    "m": "1M", "1mo": "1M", "monthly": "1M",
}

#: Values the DB CHECK constraint accepts. Anything else must normalise to None.
NORMALIZED_TIMEFRAMES = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h",
                         "1d", "1w", "1M")


def normalize_timeframe(raw: Any) -> Optional[str]:
    """'240' -> '4h', 'D' -> '1d'. Unrecognised -> None (never a guess).

    Note the deliberate asymmetry with the map above: bare 'M' is TradingView's
    MONTHLY code, not minutes, and '1M' is returned for it. Minute intervals are
    always bare digits in TradingView, so nothing is ambiguous in practice.
    """
    token = _token(raw)
    if not token:
        return None
    mapped = _TIMEFRAME_MAP.get(token)
    return mapped if mapped in NORMALIZED_TIMEFRAMES else None


def _token(raw: Any) -> str:
    return str(raw or "").strip().lower().replace(" ", "_").replace("-", "_")


# ---- proximity -------------------------------------------------------------- #
# Windows fixed A PRIORI from product meaning, counted in TRADING SESSIONS, and
# NEVER fitted to outcomes. They are NARROWER than the SEC windows on purpose:
# an 8-K about an acquisition is still the relevant fact a fortnight later,
# whereas an indicator's 4H classification from three weeks ago is describing a
# market that no longer exists.
#
#   this_session      arrived during the scan session itself
#   previous_session  the session before it
#   recent            2-3 sessions back
#   older_context     4-10 sessions back, detail screen only
#   out_of_window     older; stored forever, never surfaced, so an external
#                     badge can never sit on a symbol indefinitely
PROXIMITY_THIS_SESSION = "this_session"
PROXIMITY_PREVIOUS_SESSION = "previous_session"
PROXIMITY_RECENT = "recent"
PROXIMITY_OLDER_CONTEXT = "older_context"
PROXIMITY_OUT_OF_WINDOW = "out_of_window"

PROXIMITIES = (PROXIMITY_THIS_SESSION, PROXIMITY_PREVIOUS_SESSION,
               PROXIMITY_RECENT, PROXIMITY_OLDER_CONTEXT,
               PROXIMITY_OUT_OF_WINDOW)

RECENT_MAX_SESSIONS = 3
OLDER_CONTEXT_MAX_SESSIONS = 10

IN_WINDOW_PROXIMITIES = (PROXIMITY_THIS_SESSION, PROXIMITY_PREVIOUS_SESSION,
                         PROXIMITY_RECENT, PROXIMITY_OLDER_CONTEXT)
#: Proximities the SCANNER LIST may surface. The detail screen shows more.
NOTABLE_PROXIMITIES = (PROXIMITY_THIS_SESSION, PROXIMITY_PREVIOUS_SESSION)

#: Bounded payload sizes — the list row carries counts, never signals.
MAX_DETAIL_ITEMS = 8


def classify_proximity(sessions_ago: Optional[int]) -> str:
    if sessions_ago is None or sessions_ago < 0:
        return PROXIMITY_OUT_OF_WINDOW
    if sessions_ago == 0:
        return PROXIMITY_THIS_SESSION
    if sessions_ago == 1:
        return PROXIMITY_PREVIOUS_SESSION
    if sessions_ago <= RECENT_MAX_SESSIONS:
        return PROXIMITY_RECENT
    if sessions_ago <= OLDER_CONTEXT_MAX_SESSIONS:
        return PROXIMITY_OLDER_CONTEXT
    return PROXIMITY_OUT_OF_WINDOW


def is_notable(proximity: Optional[str], *, direction: Optional[str]) -> bool:
    """The SILENCE GATE for the scanner list — not an importance score.

    Two checkable facts: the source expressed a view at all, and it arrived
    close enough to this session to bear on it. Nothing here ranks one source
    above another, and a `neutral` or `unknown` reading never lights a row.
    """
    return direction in DIRECTIONAL and proximity in NOTABLE_PROXIMITIES


# ---- availability (same vocabulary as app.catalyst / app.news / app.sec) ---- #
STATUS_AVAILABLE = "available"
STATUS_UNAVAILABLE = "unavailable"
STATUS_STALE = "stale"
EXTERNAL_STATUSES = (STATUS_AVAILABLE, STATUS_UNAVAILABLE, STATUS_STALE)

REASON_SOURCE_UNAVAILABLE = "source_unavailable"
REASON_NEVER_REFRESHED = "never_refreshed"
REASON_STALE_REFRESH = "stale_refresh"
#: Specific to pushed sources: nothing is wrong, no one has configured the
#: third-party alert yet. Reporting this as an error would be a lie about our
#: own system, and reporting it as `available` would be a lie about the data.
REASON_NOT_CONFIGURED = "not_configured"

#: A source that has not delivered in this long is reported `stale`.
#:
#: Deliberately much LONGER than the pulled sources' 12h/30h. A pulled source
#: going quiet means our refresh failed. A pushed source going quiet usually
#: means the indicator simply did not fire — which is the normal, correct
#: behaviour of an alert and must not be reported as a fault. Seven days is
#: long enough that an ordinary quiet week never reads as broken, short enough
#: that a genuinely disconnected webhook eventually surfaces.
FRESHNESS_MAX_AGE_HOURS = 24 * 7


# --------------------------------------------------------------------------- #
# point-in-time
# --------------------------------------------------------------------------- #

def is_visible_to_session(effective_at: datetime, as_of_session: date) -> bool:
    """The gate. Arrival against the session close — nothing else."""
    return _aware(effective_at) <= session_close_utc(as_of_session)


def effective_session(effective_at: datetime) -> Optional[date]:
    """The first trading session this signal could have acted on.

    Shares `app.news`'s market clock deliberately: layers disagreeing about
    when a session ends would be a bug nobody could see.
    """
    from app.news import effective_session as news_effective_session
    return news_effective_session(effective_at)


# --------------------------------------------------------------------------- #
# freshness
# --------------------------------------------------------------------------- #

def evaluate_freshness(source_state: Optional[Dict[str, Any]], *,
                       now: datetime,
                       registry_status: Optional[str] = None) -> Dict[str, Any]:
    """Turn the persisted `catalyst_source_state` row into an explicit verdict.

    Distinguishes four situations an empty table cannot: never configured, a
    delivery path that broke, a source that has gone quiet, and a healthy one.
    `registry_status` lets a source the registry knows is awaiting manual setup
    report `not_configured` instead of the misleading `never_refreshed`.
    """
    if not source_state:
        reason = (REASON_NOT_CONFIGURED
                  if registry_status in ("requires_manual_setup",
                                         "available_not_integrated")
                  else REASON_NEVER_REFRESHED)
        return {"status": STATUS_UNAVAILABLE, "reason": reason,
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


def combine_freshness(per_source: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """One dimension-level verdict from the per-source verdicts.

    The rule is BEST-OF, not worst-of, and the direction matters. This block
    answers "can the product show external intelligence at all". One configured
    source delivering while a second was never connected is a working
    dimension with an unconnected source in it — reporting the whole dimension
    as unavailable because of the second would hide the first.

    The per-source verdicts are returned alongside, so the UI can still say
    exactly which source is quiet and why.
    """
    if not per_source:
        return {"status": STATUS_UNAVAILABLE, "reason": REASON_NEVER_REFRESHED,
                "last_refresh_at": None, "last_success_at": None,
                "age_hours": None, "detail": None, "per_source": {}}

    order = {STATUS_AVAILABLE: 0, STATUS_STALE: 1, STATUS_UNAVAILABLE: 2}
    best = min(per_source.values(),
               key=lambda f: (order.get(f.get("status"), 3),
                              f.get("age_hours") if f.get("age_hours")
                              is not None else float("inf")))
    return {**best, "per_source": per_source}


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return _aware(value).isoformat()
    return None


# --------------------------------------------------------------------------- #
# selection
# --------------------------------------------------------------------------- #

def select_visible_signals(
    signals: Sequence[Dict[str, Any]],
    *,
    as_of_session: date,
    limit: int = MAX_DETAIL_ITEMS,
) -> List[Dict[str, Any]]:
    """Newest-first, point-in-time filtered, superseded rows dropped, bounded.

    SUPERSESSION IS DERIVED HERE, not stored. A correction arrives as a new row
    carrying `supersedes_signal_id`; the row it points at is hidden from the
    product while remaining in the database forever. That is what lets the
    ingest role hold INSERT and nothing else — see migration 022.

    A correction only hides its target if the correction ITSELF is visible to
    this session. Otherwise a future correction would retroactively erase what
    we were actually looking at on the day, which is lookahead wearing the
    costume of a data fix.
    """
    visible: List[Dict[str, Any]] = []
    for row in signals:
        effective = row.get("effective_at")
        if not isinstance(effective, datetime):
            continue
        if not is_visible_to_session(effective, as_of_session):
            continue
        visible.append(row)

    superseded = {row.get("supersedes_signal_id") for row in visible
                  if row.get("supersedes_signal_id")}

    prepared: List[Dict[str, Any]] = []
    for row in visible:
        if row.get("id") in superseded:
            continue
        session = effective_session(_aware(row["effective_at"]))
        if session is None:
            continue
        sessions_ago = trading_sessions_between(session, as_of_session)
        proximity = classify_proximity(sessions_ago)
        if proximity == PROXIMITY_OUT_OF_WINDOW:
            continue
        prepared.append({**row, "_effective": _aware(row["effective_at"]),
                         "_session": session, "_sessions_ago": sessions_ago,
                         "_proximity": proximity})

    prepared.sort(key=lambda r: r["_effective"], reverse=True)
    return prepared[:limit]


def build_signal_item(row: Dict[str, Any]) -> Dict[str, Any]:
    """One product-facing signal. The source's claim and its provenance.

    Both the raw and the normalised value of every semantic field are present,
    so a reader can always see what the source actually said rather than only
    what our vocabulary made of it.
    """
    confidence = row.get("confidence")
    return {
        "source": row.get("source"),
        "source_signal_id": row.get("source_signal_id"),
        "symbol": row.get("symbol"),
        # What the source said, verbatim.
        "signal_type": row.get("signal_type"),
        "direction": row.get("direction"),
        "timeframe": row.get("timeframe"),
        # What we made of it.
        "signal_type_normalized": row.get("signal_type_normalized"),
        "direction_normalized": row.get("direction_normalized"),
        "timeframe_normalized": row.get("timeframe_normalized"),
        # Never invented. `available` is an explicit fact about the source.
        "confidence": float(confidence) if confidence is not None else None,
        "confidence_scale": row.get("confidence_scale"),
        "confidence_available": confidence is not None,
        # The three clocks, all reported. `effective_at` is the gate;
        # `observed_at` is the source's claim; the skew between them is a fact
        # about the source and is never silently absorbed.
        "observed_at": _iso(row.get("observed_at")),
        "received_at": _iso(row.get("received_at")),
        "effective_at": _iso(row.get("effective_at")),
        "clock_skew_seconds": row.get("clock_skew_seconds"),
        "session": row["_session"].isoformat() if row.get("_session") else None,
        "sessions_ago": row.get("_sessions_ago"),
        "proximity": row.get("_proximity"),
        # Provenance.
        "indicator": row.get("indicator"),
        "indicator_version": row.get("indicator_version"),
        "alert_id": row.get("alert_id"),
        "contract_version": row.get("contract_version"),
        "source_payload_version": row.get("source_payload_version"),
        "is_correction": bool(row.get("supersedes_signal_id")),
        "notable": is_notable(row.get("_proximity"),
                              direction=row.get("direction_normalized")),
    }


# --------------------------------------------------------------------------- #
# confluence — a DESCRIPTION of agreement, never a score
# --------------------------------------------------------------------------- #
# Phase 12's whole point is that we do not yet know whether confluence has
# value. Emitting "3/4 confirmations" would answer that question by assertion
# instead of measuring it, and the number would immediately be read as a
# probability. So this returns a WORD describing the relationship, the raw
# per-source readings that produced it, and nothing that can be sorted on.
CONFLUENCE_AGREEMENT = "agreement"
CONFLUENCE_DISAGREEMENT = "disagreement"
CONFLUENCE_MIXED = "mixed"
CONFLUENCE_EXTERNAL_ONLY = "external_only"
CONFLUENCE_INTERNAL_ONLY = "internal_only"
CONFLUENCE_NO_EXTERNAL_SIGNAL = "no_external_signal"
CONFLUENCE_UNAVAILABLE = "unavailable"

CONFLUENCE_STATES = (
    CONFLUENCE_AGREEMENT, CONFLUENCE_DISAGREEMENT, CONFLUENCE_MIXED,
    CONFLUENCE_EXTERNAL_ONLY, CONFLUENCE_INTERNAL_ONLY,
    CONFLUENCE_NO_EXTERNAL_SIGNAL, CONFLUENCE_UNAVAILABLE,
)

#: Attention tiers that mean "the internal scanner is pointing at this symbol".
#: Imported by value rather than from `scanner_view` to keep this module pure
#: and importable without the product-view layer; the two are asserted equal
#: by the isolation tests.
INTERNAL_INTERESTED_TIERS = ("high_attention", "developing")


def classify_external_stance(items: Sequence[Dict[str, Any]]) -> str:
    """What the external sources, taken together, are saying in-window.

    Counts DISTINCT directional readings, not signals: a chatty indicator that
    fires ten bullish alerts must not outvote a quiet one, because this is a
    description of who said what, not a poll.
    """
    directions = {i.get("direction_normalized") for i in items
                  if i.get("direction_normalized") in DIRECTIONAL}
    if not directions:
        return "none"
    if directions == {DIRECTION_BULLISH}:
        return DIRECTION_BULLISH
    if directions == {DIRECTION_BEARISH}:
        return DIRECTION_BEARISH
    return "mixed"


def classify_confluence(*, attention: Optional[str],
                        items: Sequence[Dict[str, Any]],
                        status: Optional[str]) -> str:
    """How the internal reading and the external readings relate. A WORD.

    Order of the branches IS the definition. Note what is absent: no count, no
    ratio, no weight and no ordering. The product is currently trying to find
    out whether agreement predicts anything; publishing a number would prejudge
    exactly the question this data exists to answer.
    """
    if status == STATUS_UNAVAILABLE:
        return CONFLUENCE_UNAVAILABLE

    internal_interested = attention in INTERNAL_INTERESTED_TIERS
    external = classify_external_stance(
        [i for i in items if i.get("notable")])

    if external == "none":
        return (CONFLUENCE_INTERNAL_ONLY if internal_interested
                else CONFLUENCE_NO_EXTERNAL_SIGNAL)
    if not internal_interested:
        return CONFLUENCE_EXTERNAL_ONLY
    if external == "mixed":
        return CONFLUENCE_MIXED
    # The internal candidate arm only ever flags an ACCUMULATION reading, so
    # "the scanner is interested" is a bullish-leaning stance. An external
    # bearish reading against it is a genuine disagreement and is reported as
    # one — it is at least as interesting to measure as agreement.
    return (CONFLUENCE_AGREEMENT if external == DIRECTION_BULLISH
            else CONFLUENCE_DISAGREEMENT)


# --------------------------------------------------------------------------- #
# the product objects
# --------------------------------------------------------------------------- #

def build_external_context(
    signals: Sequence[Dict[str, Any]],
    *,
    as_of_session: Optional[date],
    sources: Sequence[Dict[str, Any]],
    freshness: Dict[str, Any],
    attention: Optional[str] = None,
    limit: int = MAX_DETAIL_ITEMS,
) -> Dict[str, Any]:
    """The full per-symbol external-intelligence block."""
    base: Dict[str, Any] = {
        "contract_version": EXTERNAL_INTELLIGENCE_CONTRACT_VERSION,
        "status": freshness.get("status"),
        "reason": freshness.get("reason"),
        "as_of_session": as_of_session.isoformat() if as_of_session else None,
        "last_signal_at": None,
        "age_hours": freshness.get("age_hours"),
        "window_sessions": OLDER_CONTEXT_MAX_SESSIONS,
        "in_window_count": 0,
        "notable_count": 0,
        "sources_present": [],
        "external_stance": "none",
        "confluence": CONFLUENCE_UNAVAILABLE,
        # The registry, so the UI can say "AI Edge: awaiting your alert setup"
        # instead of silently showing nothing for a source that exists.
        "sources": [build_source_entry(s) for s in sources],
        "items": [],
    }

    if freshness.get("status") == STATUS_UNAVAILABLE or as_of_session is None:
        if as_of_session is None:
            base["status"] = STATUS_UNAVAILABLE
            base["reason"] = base["reason"] or REASON_NEVER_REFRESHED
        base["confluence"] = classify_confluence(
            attention=attention, items=[], status=STATUS_UNAVAILABLE)
        return base

    picked = select_visible_signals(signals, as_of_session=as_of_session,
                                   limit=limit)
    items = [build_signal_item(row) for row in picked]
    notable = [i for i in items if i["notable"]]

    base["items"] = items
    base["in_window_count"] = len(items)
    base["notable_count"] = len(notable)
    # Ordered by first appearance (newest first), so the list reads as "who has
    # spoken recently" rather than as a ranking.
    present: List[str] = []
    for item in items:
        if item["source"] and item["source"] not in present:
            present.append(item["source"])
    base["sources_present"] = present
    base["last_signal_at"] = items[0]["effective_at"] if items else None
    base["external_stance"] = classify_external_stance(notable)
    base["confluence"] = classify_confluence(
        attention=attention, items=items, status=base["status"])
    return base


def build_source_entry(row: Dict[str, Any]) -> Dict[str, Any]:
    """One registry row, as the product sees it.

    Capability metadata is surfaced so the UI can distinguish "this source is
    silent" from "this source was never connected" — two states that look
    identical in the data and mean completely different things to a user.
    """
    return {
        "source": row.get("source"),
        "display_name": row.get("display_name"),
        "status": row.get("status"),
        "transports": list(row.get("transports") or []),
        "supports_realtime": bool(row.get("supports_realtime")),
        "supports_historical": bool(row.get("supports_historical")),
        "supports_symbol_scan": bool(row.get("supports_symbol_scan")),
        "supports_signal_events": bool(row.get("supports_signal_events")),
        "emits_signals": bool(row.get("emits_signals")),
        "requires_paid_plan": bool(row.get("requires_paid_plan")),
        "notes": row.get("notes"),
    }


def build_row_external(context: Dict[str, Any]) -> Dict[str, Any]:
    """The COMPACT subset a list row carries — counts and words, never signals.

    `notable_count` is the gate the UI uses to stay SILENT. A row must never
    carry "no external signal": on any given session most symbols will have
    none, and printing that on 25 rows would turn an ordinary quiet day into a
    claim about every symbol.
    """
    items = context.get("items") or []
    notable = [i for i in items if i.get("notable")]
    latest = notable[0] if notable else None
    return {
        "status": context.get("status"),
        "reason": context.get("reason"),
        "notable_count": context.get("notable_count", 0),
        "in_window_count": context.get("in_window_count", 0),
        "sources_present": list(context.get("sources_present") or []),
        "external_stance": context.get("external_stance"),
        "confluence": context.get("confluence"),
        "latest_source": latest["source"] if latest else None,
        "latest_direction": latest["direction_normalized"] if latest else None,
        "latest_timeframe": (latest["timeframe_normalized"] or
                             latest["timeframe"]) if latest else None,
        "latest_effective_at": latest["effective_at"] if latest else None,
        "latest_proximity": latest["proximity"] if latest else None,
    }


def empty_external_context(*, reason: str = REASON_NEVER_REFRESHED,
                           sources: Sequence[Dict[str, Any]] = (),
                           ) -> Dict[str, Any]:
    """A fully-unavailable block.

    Used when external loading fails entirely: the scanner keeps working and
    every external field says `unavailable` rather than the request failing.
    """
    return build_external_context(
        [], as_of_session=None, sources=sources,
        freshness={"status": STATUS_UNAVAILABLE, "reason": reason,
                   "last_refresh_at": None, "last_success_at": None,
                   "age_hours": None, "detail": None})


def summarize_sources(contexts: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Overview-level compact evidence. Counts and names — never a feed.

    The overview screen answers "is any external system talking, and about how
    many of my symbols" so a user knows whether to look. The signals themselves
    live on the symbol detail screen, which is where a claim can be shown with
    its provenance attached.
    """
    sources_present: List[str] = []
    symbols_with_signal = 0
    total_notable = 0
    agreement = disagreement = 0
    for ctx in contexts:
        notable = ctx.get("notable_count", 0) or 0
        if notable:
            symbols_with_signal += 1
        total_notable += notable
        for source in ctx.get("sources_present") or []:
            if source not in sources_present:
                sources_present.append(source)
        if ctx.get("confluence") == CONFLUENCE_AGREEMENT:
            agreement += 1
        elif ctx.get("confluence") == CONFLUENCE_DISAGREEMENT:
            disagreement += 1
    return {
        "external_sources_present": sorted(sources_present),
        "symbols_with_external_signal": symbols_with_signal,
        "recent_signal_count": total_notable,
        # A COUNT OF ROWS, not a score and not an ordering input. It says how
        # many symbols the two readings happened to line up on — nothing about
        # whether that lining up means anything.
        "agreement_symbol_count": agreement,
        "disagreement_symbol_count": disagreement,
    }


__all__ = [
    "EXTERNAL_INTELLIGENCE_CONTRACT_VERSION", "TRADINGVIEW_CONTRACT_VERSION",
    "SOURCE_TRADINGVIEW", "SOURCE_AI_EDGE", "WEBHOOK_SOURCES",
    "SOURCE_STATE_PREFIX", "source_state_key",
    "TYPE_ENTRY", "TYPE_EXIT", "TYPE_CLASSIFICATION", "TYPE_REGIME_FILTER",
    "TYPE_TREND", "TYPE_REVERSAL", "TYPE_BREAKOUT", "TYPE_SETUP",
    "TYPE_ALERT", "TYPE_UNKNOWN", "SIGNAL_TYPES", "normalize_signal_type",
    "DIRECTION_BULLISH", "DIRECTION_BEARISH", "DIRECTION_NEUTRAL",
    "DIRECTION_UNKNOWN", "DIRECTIONS", "DIRECTIONAL", "normalize_direction",
    "NORMALIZED_TIMEFRAMES", "normalize_timeframe",
    "PROXIMITY_THIS_SESSION", "PROXIMITY_PREVIOUS_SESSION", "PROXIMITY_RECENT",
    "PROXIMITY_OLDER_CONTEXT", "PROXIMITY_OUT_OF_WINDOW", "PROXIMITIES",
    "RECENT_MAX_SESSIONS", "OLDER_CONTEXT_MAX_SESSIONS",
    "IN_WINDOW_PROXIMITIES", "NOTABLE_PROXIMITIES", "MAX_DETAIL_ITEMS",
    "classify_proximity", "is_notable",
    "STATUS_AVAILABLE", "STATUS_UNAVAILABLE", "STATUS_STALE",
    "EXTERNAL_STATUSES", "REASON_SOURCE_UNAVAILABLE", "REASON_NEVER_REFRESHED",
    "REASON_STALE_REFRESH", "REASON_NOT_CONFIGURED", "FRESHNESS_MAX_AGE_HOURS",
    "session_close_utc", "is_visible_to_session", "effective_session",
    "evaluate_freshness", "combine_freshness", "select_visible_signals",
    "build_signal_item",
    "CONFLUENCE_AGREEMENT", "CONFLUENCE_DISAGREEMENT", "CONFLUENCE_MIXED",
    "CONFLUENCE_EXTERNAL_ONLY", "CONFLUENCE_INTERNAL_ONLY",
    "CONFLUENCE_NO_EXTERNAL_SIGNAL", "CONFLUENCE_UNAVAILABLE",
    "CONFLUENCE_STATES", "INTERNAL_INTERESTED_TIERS",
    "classify_external_stance", "classify_confluence",
    "build_external_context", "build_source_entry", "build_row_external",
    "empty_external_context", "summarize_sources",
]
