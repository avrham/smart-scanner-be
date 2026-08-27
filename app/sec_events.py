"""Deterministic SEC material-event context for the Smart Scanner product API.

PURE: no DB, no network, no provider. Given already-fetched filing rows and a
scan session date, every output is a deterministic function of stored facts.

WHAT THIS IS NOT
----------------
SEC context sits BESIDE the strategy result. It does not change the candidate
verdict, the Wyckoff evaluation, the attention tier, the ordering or ENTER
eligibility, and it produces no score. The product says "this setup exists, AND
the company formally disclosed a material event" — never "this setup is
better/worse because of it". It also offers no legal reading of any filing:
an item code is reported as the item code it is, with a link to the document.

WHY 8-K
-------
News V1 measured the honest limit of a company-news feed: 61 of 100 historical
symbol-sessions carried a "notable" headline, and most survivors were still
commentary. An 8-K is the opposite kind of object. A registrant does not choose
to be written about — it is REQUIRED to file, under a numbered SEC item, within
a deadline, with a timestamped public acceptance. That gives this layer three
things the news layer structurally cannot have: a canonical identity (the
accession number), a defensible disclosure moment (EDGAR acceptance), and an
event type that is asserted by the filer under regulation rather than inferred
from a headline.

POINT-IN-TIME HONESTY — THE GATE IS ACCEPTANCE, NOT THE EVENT DATE
-------------------------------------------------------------------
Three timestamps travel with every 8-K and only one of them may decide
visibility:

    period_of_report   when the EVENT happened
    filing_date        the EDGAR filing date (a date, no clock)
    accepted_at        when the filing became PUBLIC   <- the only gate

    an 8-K is visible to session S  iff  accepted_at <= close(S)

The failure this prevents is concrete: an event on 27 July disclosed on 30 July
at 08:10 ET was unknowable to a scan on 29 July. Gating on `period_of_report`
would show it, and would be lookahead of the worst kind — the kind that looks
like a finding.

`close(S)` is 16:00 America/New_York, DST included, borrowed from `app.news` so
the two catalyst layers cannot drift apart on what a session is. A filing
accepted after the close belongs to the NEXT trading session, which is how a
trader reads it: Apple's 8-K accepted 2026-07-30T20:30:28Z (16:30 ET) is
Friday's news, not Thursday's.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from app.catalyst import trading_sessions_between
# Re-exported deliberately: the SQL bound in the Product API and the gate
# here must be the SAME clock, and importing it through this module makes
# that impossible to get wrong at a call site.
from app.news import session_close_utc

SEC_EVENTS_CONTRACT_VERSION = "smart_scanner_sec_events.v1"

#: The `catalyst_source_state.source` row this dimension reports through.
SOURCE_SEC_EDGAR = "sec_edgar_8k"

# ---- forms ------------------------------------------------------------------ #
# V1 is centred on the current-report form and its amendment. Everything else
# EDGAR carries (10-K, 10-Q, 4, 13D/G, S-1, DEF 14A) is a different product
# question and is deliberately not ingested.
FORM_8K = "8-K"
FORM_8K_A = "8-K/A"
#: Every form family this layer accepts. `8-K12B` is a successor-issuer variant
#: of the same current report and is kept for completeness.
ACCEPTED_FORM_PREFIX = "8-K"


def is_amendment(form: Optional[str]) -> bool:
    """An 8-K/A amends an earlier filing; it is never the original."""
    return bool(form) and form.strip().upper().endswith("/A")


# ---- taxonomy --------------------------------------------------------------- #
#: Bumped whenever the mapping below changes, and stored on every row, so a
#: taxonomy revision is visible in the data instead of silently rewriting
#: history.
SEC_TAXONOMY_VERSION = "sec_8k_items.v1"

EVENT_MATERIAL_AGREEMENT = "material_agreement"
EVENT_BANKRUPTCY = "bankruptcy_or_receivership"
EVENT_MINE_SAFETY = "mine_safety"
EVENT_CYBERSECURITY = "cybersecurity_incident"
EVENT_ACQUISITION_DISPOSITION = "acquisition_or_disposition"
EVENT_RESULTS = "results_of_operations"
EVENT_FINANCIAL_OBLIGATION = "financial_obligation"
EVENT_RESTRUCTURING = "restructuring_or_impairment"
EVENT_DELISTING_OR_LISTING = "delisting_or_listing"
EVENT_EQUITY_OR_CAPITAL = "equity_or_capital"
EVENT_ACCOUNTANT_CHANGE = "accountant_change"
EVENT_MANAGEMENT_CHANGE = "management_change"
EVENT_CHARTER_OR_GOVERNANCE = "charter_or_governance"
EVENT_SHAREHOLDER_MATTERS = "shareholder_matters"
EVENT_ASSET_BACKED = "asset_backed_securities"
EVENT_REGULATION_FD = "regulation_fd"
EVENT_OTHER_MATERIAL = "other_material_event"
EVENT_EXHIBITS = "financial_statements_and_exhibits"
EVENT_UNKNOWN = "unknown"

EVENT_TYPES = (
    EVENT_MATERIAL_AGREEMENT, EVENT_BANKRUPTCY, EVENT_MINE_SAFETY,
    EVENT_CYBERSECURITY, EVENT_ACQUISITION_DISPOSITION, EVENT_RESULTS,
    EVENT_FINANCIAL_OBLIGATION, EVENT_RESTRUCTURING, EVENT_DELISTING_OR_LISTING,
    EVENT_EQUITY_OR_CAPITAL, EVENT_ACCOUNTANT_CHANGE, EVENT_MANAGEMENT_CHANGE,
    EVENT_CHARTER_OR_GOVERNANCE, EVENT_SHAREHOLDER_MATTERS, EVENT_ASSET_BACKED,
    EVENT_REGULATION_FD, EVENT_OTHER_MATERIAL, EVENT_EXHIBITS, EVENT_UNKNOWN,
)

#: Item code -> event family, taken from the SEC's own 8-K section semantics.
#: Nothing here is inferred from text, and the original codes are always kept
#: alongside so this mapping can be checked (and revised) after the fact.
ITEM_EVENT_TYPES: Dict[str, str] = {
    # 1.xx Registrant's business and operations
    "1.01": EVENT_MATERIAL_AGREEMENT,        # entry into a material agreement
    "1.02": EVENT_MATERIAL_AGREEMENT,        # termination of a material agreement
    "1.03": EVENT_BANKRUPTCY,
    "1.04": EVENT_MINE_SAFETY,
    "1.05": EVENT_CYBERSECURITY,             # material cybersecurity incident
    # 2.xx Financial information
    "2.01": EVENT_ACQUISITION_DISPOSITION,
    "2.02": EVENT_RESULTS,                   # results of operations
    "2.03": EVENT_FINANCIAL_OBLIGATION,
    "2.04": EVENT_FINANCIAL_OBLIGATION,      # triggering event / acceleration
    "2.05": EVENT_RESTRUCTURING,             # exit or disposal costs
    "2.06": EVENT_RESTRUCTURING,             # material impairment
    # 3.xx Securities and trading markets
    "3.01": EVENT_DELISTING_OR_LISTING,
    "3.02": EVENT_EQUITY_OR_CAPITAL,         # unregistered sale of equity
    "3.03": EVENT_EQUITY_OR_CAPITAL,         # modification of security rights
    # 4.xx Matters related to accountants and financial statements
    "4.01": EVENT_ACCOUNTANT_CHANGE,
    "4.02": EVENT_ACCOUNTANT_CHANGE,         # non-reliance on prior statements
    # 5.xx Corporate governance and management
    "5.01": EVENT_MANAGEMENT_CHANGE,         # change in control
    "5.02": EVENT_MANAGEMENT_CHANGE,         # director/officer departure or election
    "5.03": EVENT_CHARTER_OR_GOVERNANCE,
    "5.04": EVENT_SHAREHOLDER_MATTERS,       # benefit-plan trading suspension
    "5.05": EVENT_CHARTER_OR_GOVERNANCE,     # code of ethics
    "5.06": EVENT_CHARTER_OR_GOVERNANCE,     # shell company status change
    "5.07": EVENT_SHAREHOLDER_MATTERS,       # submission of matters to a vote
    "5.08": EVENT_SHAREHOLDER_MATTERS,       # shareholder director nominations
    # 6.xx Asset-backed securities
    "6.01": EVENT_ASSET_BACKED, "6.02": EVENT_ASSET_BACKED,
    "6.03": EVENT_ASSET_BACKED, "6.04": EVENT_ASSET_BACKED,
    "6.05": EVENT_ASSET_BACKED, "6.06": EVENT_ASSET_BACKED,
    # 7.xx Regulation FD
    "7.01": EVENT_REGULATION_FD,
    # 8.xx Other events (registrant's discretion)
    "8.01": EVENT_OTHER_MATERIAL,
    # 9.xx Financial statements and exhibits
    "9.01": EVENT_EXHIBITS,
}

# ---- structural visibility, NOT importance ---------------------------------- #
# The distinction is the SEC's, not ours. These two items describe HOW something
# reached the public — an exhibit attached to a filing, or the Reg FD channel
# used to furnish it — rather than WHAT happened. A filing carrying only these
# is a real filing and is stored, counted and shown on the detail screen; it
# just does not, on its own, mean a material event occurred.
#
# Nothing here ranks one event above another. `2.02` is not "more important"
# than `5.02`; both are primary, and the product presents them as equals.
SUPPORTING_ITEMS = frozenset({"7.01", "9.01"})


def normalize_item_code(raw: Any) -> Optional[str]:
    """'2.02 ' -> '2.02'. Anything not shaped like an item code is dropped."""
    token = str(raw or "").strip()
    if not token:
        return None
    parts = token.split(".")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        return None
    return f"{int(parts[0])}.{parts[1][:2].zfill(2)}"


def parse_item_codes(raw: Any) -> List[str]:
    """EDGAR's comma-separated `items` field -> ordered, de-duplicated codes."""
    if isinstance(raw, (list, tuple)):
        tokens = list(raw)
    else:
        tokens = str(raw or "").split(",")
    out: List[str] = []
    for token in tokens:
        code = normalize_item_code(token)
        if code and code not in out:
            out.append(code)
    return out


def classify_event_types(item_codes: Sequence[str]) -> List[str]:
    """Item codes -> event families, in first-seen order, duplicates collapsed.

    An unmapped code becomes `unknown` rather than being silently dropped: the
    product would otherwise report a filing as having no event type when in fact
    it had one this taxonomy does not know yet.
    """
    out: List[str] = []
    for code in item_codes:
        family = ITEM_EVENT_TYPES.get(code, EVENT_UNKNOWN)
        if family not in out:
            out.append(family)
    return out


def is_primary_event(item_codes: Sequence[str]) -> bool:
    """True when at least one item says WHAT HAPPENED.

    A filing with no item codes at all is treated as primary: we know a formal
    current report was filed, and claiming it was merely supporting would be a
    stronger statement than the data supports.
    """
    if not item_codes:
        return True
    return any(code not in SUPPORTING_ITEMS for code in item_codes)


def primary_event_types(item_codes: Sequence[str]) -> List[str]:
    """Event families from the primary items only — what the row may label."""
    return classify_event_types([c for c in item_codes if c not in SUPPORTING_ITEMS])


# ---- proximity -------------------------------------------------------------- #
# Windows fixed A PRIORI from product meaning, counted in TRADING SESSIONS, and
# never fitted to outcomes. They are WIDER than the news windows on purpose: a
# headline is stale in days, whereas a formally disclosed corporate event —
# an executive departure, an acquisition, a debt issue — is still the relevant
# fact about a company a fortnight later.
#
#   today          accepted inside the scan session itself
#   recent         1-5 sessions back — about a week
#   older_context  6-20 sessions back — about a month, detail screen only
#   out_of_window  older than 20 sessions; stored, never surfaced, so an 8-K
#                  badge can never sit on a symbol indefinitely
PROXIMITY_TODAY = "today"
PROXIMITY_RECENT = "recent"
PROXIMITY_OLDER_CONTEXT = "older_context"
PROXIMITY_OUT_OF_WINDOW = "out_of_window"
PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_RECENT, PROXIMITY_OLDER_CONTEXT,
               PROXIMITY_OUT_OF_WINDOW)

RECENT_MAX_SESSIONS = 5
OLDER_CONTEXT_MAX_SESSIONS = 20

IN_WINDOW_PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_RECENT,
                         PROXIMITY_OLDER_CONTEXT)
#: Proximities the SCANNER LIST may surface.
NOTABLE_PROXIMITIES = (PROXIMITY_TODAY, PROXIMITY_RECENT)

#: Bounded payload sizes — the list row carries counts, never filings.
MAX_DETAIL_ITEMS = 6

# ---- availability (same vocabulary as app.catalyst / app.news) -------------- #
STATUS_AVAILABLE = "available"
STATUS_UNAVAILABLE = "unavailable"
STATUS_STALE = "stale"
SEC_STATUSES = (STATUS_AVAILABLE, STATUS_UNAVAILABLE, STATUS_STALE)

REASON_SOURCE_UNAVAILABLE = "source_unavailable"
REASON_NEVER_REFRESHED = "never_refreshed"
REASON_STALE_REFRESH = "stale_refresh"

#: A successful refresh older than this makes the dimension `stale`.
#:
#: Deliberately NOT the news layer's 12h. The two sources decay differently: a
#: news feed going half a day cold means missed stories, whereas an 8-K is a
#: durable dated disclosure refreshed once per daily pipeline run. 30 hours is
#: one daily cadence plus slack — long enough that a normal day never reads as
#: stale, short enough that ONE missed run does.
FRESHNESS_MAX_AGE_HOURS = 30


# --------------------------------------------------------------------------- #
# point-in-time
# --------------------------------------------------------------------------- #

def is_visible_to_session(accepted_at: datetime, as_of_session: date) -> bool:
    """The gate. EDGAR acceptance against the session close — nothing else."""
    return _aware(accepted_at) <= session_close_utc(as_of_session)


def effective_session(accepted_at: datetime) -> Optional[date]:
    """The first trading session this disclosure could have acted on.

    Shares `app.news`'s market clock deliberately: two catalyst layers
    disagreeing about when a session ends would be a bug nobody could see.
    """
    from app.news import effective_session as news_effective_session
    return news_effective_session(accepted_at)


def classify_proximity(sessions_ago: Optional[int]) -> str:
    if sessions_ago is None or sessions_ago < 0:
        return PROXIMITY_OUT_OF_WINDOW
    if sessions_ago == 0:
        return PROXIMITY_TODAY
    if sessions_ago <= RECENT_MAX_SESSIONS:
        return PROXIMITY_RECENT
    if sessions_ago <= OLDER_CONTEXT_MAX_SESSIONS:
        return PROXIMITY_OLDER_CONTEXT
    return PROXIMITY_OUT_OF_WINDOW


def is_notable(proximity: Optional[str], *, primary: Optional[bool]) -> bool:
    """The SILENCE GATE for the scanner list — not an importance score.

    Two checkable facts: the filing carries at least one item describing what
    happened, and it is close enough to this session to bear on it. Nothing
    here ranks one filing above another.
    """
    return bool(primary) and proximity in NOTABLE_PROXIMITIES


# --------------------------------------------------------------------------- #
# freshness
# --------------------------------------------------------------------------- #

def evaluate_freshness(source_state: Optional[Dict[str, Any]], *,
                       now: datetime) -> Dict[str, Any]:
    """Turn the persisted `catalyst_source_state` row into an explicit verdict.

    Distinguishes three situations an empty table cannot: never refreshed, the
    source is unreachable, and a refresh that succeeded but went stale.
    """
    if not source_state:
        return {"status": STATUS_UNAVAILABLE, "reason": REASON_NEVER_REFRESHED,
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

def select_visible_filings(
    filings: Sequence[Dict[str, Any]],
    *,
    as_of_session: date,
    limit: int = MAX_DETAIL_ITEMS,
) -> List[Dict[str, Any]]:
    """Newest-first, point-in-time filtered, bounded.

    There is no near-duplicate rule here and none is needed: the accession
    number is a true identity, so unlike a syndicated headline a filing simply
    cannot arrive twice wearing a different face. An 8-K/A is a DIFFERENT
    filing with its own accession number and survives alongside its original —
    an amendment is a disclosure in its own right, and hiding it behind the
    thing it amends would suppress the newer fact.
    """
    prepared: List[Dict[str, Any]] = []
    for row in filings:
        accepted = row.get("accepted_at")
        if not isinstance(accepted, datetime):
            continue
        if not is_visible_to_session(accepted, as_of_session):
            continue
        session = effective_session(accepted)
        if session is None:
            continue
        sessions_ago = trading_sessions_between(session, as_of_session)
        proximity = classify_proximity(sessions_ago)
        if proximity == PROXIMITY_OUT_OF_WINDOW:
            continue
        prepared.append({**row, "_accepted": _aware(accepted),
                         "_session": session, "_sessions_ago": sessions_ago,
                         "_proximity": proximity})

    prepared.sort(key=lambda r: r["_accepted"], reverse=True)
    return prepared[:limit]


def build_sec_item(row: Dict[str, Any]) -> Dict[str, Any]:
    """One product-facing filing. Structured evidence and a link — nothing else.

    No summary, no interpretation, no legal reading. The item codes are the
    registrant's own assertion of what was disclosed, and `filing_url` is where
    a reader goes to find out what it says.
    """
    codes = list(row.get("item_codes") or [])
    return {
        "accession_number": row.get("accession_number"),
        "form": row.get("form"),
        "is_amendment": is_amendment(row.get("form")),
        "amends_accession_number": row.get("amends_accession_number"),
        "accepted_at": _iso(row.get("accepted_at")),
        "session": row["_session"].isoformat() if row.get("_session") else None,
        "sessions_ago": row.get("_sessions_ago"),
        "proximity": row.get("_proximity"),
        "filing_date": (row["filing_date"].isoformat()
                        if isinstance(row.get("filing_date"), date) else None),
        # Reported for completeness and clearly labelled as the EVENT date. It
        # is never the visibility gate — see the module docstring.
        "period_of_report": (row["period_of_report"].isoformat()
                             if isinstance(row.get("period_of_report"), date)
                             else None),
        "item_codes": codes,
        "event_types": list(row.get("event_types") or []),
        "primary_event_types": primary_event_types(codes),
        "is_primary_event": bool(row.get("is_primary_event")),
        "taxonomy_version": row.get("taxonomy_version"),
        "source_reference": row.get("filing_url"),
        "notable": is_notable(row.get("_proximity"),
                              primary=row.get("is_primary_event")),
    }


# --------------------------------------------------------------------------- #
# the product objects
# --------------------------------------------------------------------------- #

def build_sec_context(
    filings: Sequence[Dict[str, Any]],
    *,
    as_of_session: Optional[date],
    freshness: Dict[str, Any],
    limit: int = MAX_DETAIL_ITEMS,
) -> Dict[str, Any]:
    """The full per-symbol SEC block."""
    base: Dict[str, Any] = {
        "contract_version": SEC_EVENTS_CONTRACT_VERSION,
        "taxonomy_version": SEC_TAXONOMY_VERSION,
        "status": freshness.get("status"),
        "reason": freshness.get("reason"),
        "as_of_session": as_of_session.isoformat() if as_of_session else None,
        "last_refresh_at": freshness.get("last_refresh_at"),
        "last_success_at": freshness.get("last_success_at"),
        "age_hours": freshness.get("age_hours"),
        "detail": freshness.get("detail"),
        "window_sessions": OLDER_CONTEXT_MAX_SESSIONS,
        "in_window_count": 0,
        "primary_event_count": 0,
        "notable_count": 0,
        "latest_accepted_at": None,
        "top_event_type": None,
        "items": [],
    }

    if freshness.get("status") == STATUS_UNAVAILABLE or as_of_session is None:
        if as_of_session is None:
            base["status"] = STATUS_UNAVAILABLE
            base["reason"] = base["reason"] or REASON_NEVER_REFRESHED
        return base

    picked = select_visible_filings(filings, as_of_session=as_of_session,
                                    limit=limit)
    items = [build_sec_item(row) for row in picked]
    notable = [i for i in items if i["notable"]]

    base["items"] = items
    base["in_window_count"] = len(items)
    base["primary_event_count"] = sum(1 for i in items if i["is_primary_event"])
    base["notable_count"] = len(notable)
    base["latest_accepted_at"] = items[0]["accepted_at"] if items else None
    # The event family of the newest NOTABLE filing's first primary item — a
    # label for what is nearby, not a summary of everything and not a ranking.
    if notable and notable[0]["primary_event_types"]:
        base["top_event_type"] = notable[0]["primary_event_types"][0]
    return base


def build_row_sec(sec_context: Dict[str, Any]) -> Dict[str, Any]:
    """The COMPACT subset a list row carries — counts, never filings.

    `notable_count` is the gate the UI uses to stay silent. A row must never
    carry "no SEC event": most companies file nothing in any given week, and
    printing that on 25 rows would turn a quiet month into a claim.
    """
    items = sec_context.get("items") or []
    notable = [i for i in items if i.get("notable")]
    latest = notable[0] if notable else None
    return {
        "status": sec_context.get("status"),
        "reason": sec_context.get("reason"),
        "notable_count": sec_context.get("notable_count", 0),
        "in_window_count": sec_context.get("in_window_count", 0),
        "primary_event_count": sec_context.get("primary_event_count", 0),
        "top_event_type": sec_context.get("top_event_type"),
        "latest_accepted_at": latest["accepted_at"] if latest else None,
        "latest_proximity": latest["proximity"] if latest else None,
        "latest_form": latest["form"] if latest else None,
        "latest_item_codes": list(latest["item_codes"]) if latest else [],
    }


def empty_sec_context(*, reason: str = REASON_NEVER_REFRESHED) -> Dict[str, Any]:
    """A fully-unavailable block.

    Used when SEC loading fails entirely: the scanner keeps working and every
    SEC field says `unavailable` rather than the request failing.
    """
    return build_sec_context(
        [], as_of_session=None,
        freshness={"status": STATUS_UNAVAILABLE, "reason": reason,
                   "last_refresh_at": None, "last_success_at": None,
                   "age_hours": None, "detail": None})


__all__ = [
    "SEC_EVENTS_CONTRACT_VERSION", "SEC_TAXONOMY_VERSION", "SOURCE_SEC_EDGAR",
    "FORM_8K", "FORM_8K_A", "ACCEPTED_FORM_PREFIX", "is_amendment",
    "EVENT_TYPES", "ITEM_EVENT_TYPES", "SUPPORTING_ITEMS",
    "EVENT_RESULTS", "EVENT_MANAGEMENT_CHANGE", "EVENT_MATERIAL_AGREEMENT",
    "EVENT_EXHIBITS", "EVENT_REGULATION_FD", "EVENT_OTHER_MATERIAL",
    "EVENT_UNKNOWN",
    "normalize_item_code", "parse_item_codes", "classify_event_types",
    "is_primary_event", "primary_event_types",
    "PROXIMITY_TODAY", "PROXIMITY_RECENT", "PROXIMITY_OLDER_CONTEXT",
    "PROXIMITY_OUT_OF_WINDOW", "PROXIMITIES", "NOTABLE_PROXIMITIES",
    "IN_WINDOW_PROXIMITIES", "RECENT_MAX_SESSIONS",
    "OLDER_CONTEXT_MAX_SESSIONS", "MAX_DETAIL_ITEMS",
    "STATUS_AVAILABLE", "STATUS_UNAVAILABLE", "STATUS_STALE", "SEC_STATUSES",
    "REASON_SOURCE_UNAVAILABLE", "REASON_NEVER_REFRESHED",
    "REASON_STALE_REFRESH", "FRESHNESS_MAX_AGE_HOURS",
    "session_close_utc",
    "is_visible_to_session", "effective_session", "classify_proximity",
    "is_notable", "evaluate_freshness", "select_visible_filings",
    "build_sec_item", "build_sec_context", "build_row_sec", "empty_sec_context",
]
