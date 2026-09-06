"""What the ingress refused, kept where an operator can still read it.

WHY THIS MODULE EXISTS
----------------------
The gateway deliberately does NOT write a database row for a request it
refuses before parsing (bad token, wrong route, oversized body, rate limit) —
see `IngressRejected` in app/external_ingest.py. Recording unauthenticated
traffic per request would hand an anonymous caller a way to fill our tables,
and that reasoning is sound.

The consequence was not. It meant the ONLY trace of a refused delivery was a
log line, and Fly retains those for minutes. So when the account owner
reported that many real AI Edge alerts had fired and the tables were empty,
there was no evidence anywhere in the system that could tell these apart:

    A. TradingView never called us at all
    B. TradingView called a path this deployment does not serve
    C. TradingView called the right path with the wrong credential

Those have completely different fixes and the system could not distinguish
them. That is the gap this module closes.

THE SHAPE THAT KEEPS BOTH PROPERTIES
------------------------------------
Counters accumulate in memory and are flushed, at most once per
`flush_interval`, into `catalyst_source_state` under ONE ROW PER REASON CODE:

    external_ingress_refused_unauthorized_no_credential
    external_ingress_refused_unauthorized_bad_credential
    external_ingress_refused_route_not_found
    ...

So:

  * an anonymous caller cannot grow any table: the reason vocabulary is closed
    and capped, so the row count is bounded no matter how much traffic
    arrives;
  * a flood costs at most one UPDATE per reason per interval, not one per
    request;
  * the evidence survives a restart, which a log line and an in-process
    counter do not.

WHY ONE ROW PER REASON RATHER THAN ONE ROW TOTAL
------------------------------------------------
This started as a single aggregate row and that was wrong, which the live
staging deployment showed within a minute of the first probe. The ingress runs
TWO machines. `events_upserted` accumulates, so the TOTAL was right — but
`detail`, the free-text field carrying the per-reason breakdown, is
last-writer-wins. Machine B flushing one `route_not_found` erased machine A's
record that two requests had arrived with a bad credential.

The total was never the interesting number. "How many refusals" does not tell
an operator anything; "which reason" is the entire diagnostic, and it was
precisely the part that two processes could silently overwrite. Giving each
reason its own row moves the count into `events_upserted`, where the database
adds concurrent writes instead of letting one clobber the other. There is no
read-modify-write and therefore no race.

It needs NO migration: `catalyst_source_state` already carries a running
`events_upserted` total, and the ingress role's RLS policy permits exactly the
rows named `external\\_%` (ops/sql/create_smart_scanner_external_ingest.sql).

WHAT IS RECORDED, AND WHAT IS DELIBERATELY NOT
----------------------------------------------
Recorded: a reason code, how the caller presented a credential, and — when a
credential was presented and was wrong — the first 8 hex of its SHA-256. That
last field is what lets an owner check whether the value pasted into a
TradingView alert is the value this deployment expects, by hashing their own
copy locally.

Not recorded, ever: the supplied credential, the expected credential, any hash
or prefix of the EXPECTED credential, the caller's address, or the request
body. A fingerprint of what a stranger sent is bounded and one-way; a
fingerprint of what we expect would be an oracle, so it is never published.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.source_scope import SCOPE_PRODUCT

logger = logging.getLogger(__name__)

#: Prefix for the freshness rows refusals are folded into: one row per reason,
#: named `<prefix>_<reason>`. Inside the `external_` namespace so the ingress
#: role's RLS policy already covers it, and distinct from every registry source
#: so it can never be mistaken for one — the health endpoint's source list is
#: driven by `external_signal_sources`, which has no row by any of these names,
#: and every product read looks a row up by an explicit `source_state_key()`.
REFUSAL_STATE_PREFIX = "external_ingress_refused"


def refusal_state_source(reason: str) -> str:
    """The durable row name for one refusal reason."""
    return f"{REFUSAL_STATE_PREFIX}_{reason}"

#: Refusals happen at most a handful of times a minute in normal life, and a
#: flood must not become a write amplifier. One durable write per 30s is
#: frequent enough that an operator watching a misconfigured alert sees it on
#: the next bar close, and slow enough that abuse costs nothing.
DEFAULT_FLUSH_INTERVAL_SECONDS = 30.0

#: How the caller presented a credential. The distinction is the whole point:
#: "no credential at all" is an internet scanner or a webhook URL missing its
#: `?token=`, while "a credential that did not match" is a stale secret. The
#: HTTP RESPONSE stays identical for both — only this internal record differs.
TRANSPORT_NONE = "none"
TRANSPORT_HEADER = "header"
TRANSPORT_QUERY = "query"

#: Reason codes used only inside the ledger. The wire response keeps saying
#: `unauthorized` for every one of them.
REASON_NO_CREDENTIAL = "unauthorized_no_credential"
REASON_BAD_CREDENTIAL = "unauthorized_bad_credential"
REASON_ROUTE_NOT_FOUND = "route_not_found"

#: Bounds on what one process will hold. Reason codes come from a closed set in
#: the gateway, and route probes come from the internet, so only the latter
#: needs a cap — an unbounded map keyed by attacker-chosen paths would be a
#: memory leak wearing a diagnostic's clothes.
MAX_TRACKED_REASONS = 32
MAX_DETAIL_CHARS = 400


def credential_fingerprint(supplied: Optional[str]) -> Optional[str]:
    """First 8 hex of SHA-256 over a credential a STRANGER sent us.

    One-way and truncated, so it identifies a repeated wrong value across
    deliveries without being a way back to it. Only ever applied to a supplied
    credential — never to the expected one, which would make this an oracle.
    """
    text = (supplied or "").strip()
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


#: Characters that survive a query-string round trip without encoding. A
#: credential outside this set is not wrong, but it CANNOT be pasted raw into a
#: TradingView webhook URL: `+` becomes a space, `&` and `#` end the value, and
#: `/` and `=` are ambiguous depending on the client. The result is a token that
#: works from curl with a header and fails silently from the one caller the
#: endpoint exists for — an outage with no error anywhere.
#:
#: RFC 3986 unreserved, plus the two characters base64url uses, so a token
#: generated either of the documented ways passes.
_URL_SAFE_TOKEN_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789-._~")


def token_is_url_safe(token: Optional[str]) -> bool:
    """Can this credential be pasted into a webhook URL unencoded?

    Reported, never enforced. A deployment whose callers all send headers is
    perfectly fine with any bytes; refusing to boot would turn a warning into
    an outage. What matters is that the condition stops being invisible.
    """
    text = (token or "").strip()
    if not text:
        return False
    return all(c in _URL_SAFE_TOKEN_CHARS for c in text)


class RefusalLedger:
    """In-memory refusal counters with a throttled durable flush.

    Not thread-safe by design: the ingress is a single-process asyncio app, and
    a lock here would buy nothing while suggesting a concurrency model that
    does not exist. A lost increment under some future threading model would
    understate a diagnostic counter, never corrupt a signal.
    """

    def __init__(self, *,
                 flush_interval: float = DEFAULT_FLUSH_INTERVAL_SECONDS):
        self.flush_interval = float(flush_interval)
        self.counts: Dict[str, int] = {}
        #: Counted but not yet written, PER REASON — the unit a flush works in,
        #: because each reason owns its own durable row.
        self.pending_by_reason: Dict[str, int] = {}
        #: The most recent context for each reason, so a flush can say how the
        #: caller presented itself without one reason's detail overwriting
        #: another's.
        self.context: Dict[str, Dict[str, Any]] = {}
        self.total = 0
        self.last_reason: Optional[str] = None
        self.last_transport: Optional[str] = None
        self.last_fingerprint: Optional[str] = None
        self.last_refused_at: Optional[datetime] = None
        self._last_flush_monotonic: Optional[float] = None

    @property
    def pending(self) -> int:
        """Total unwritten refusals, across every reason."""
        return sum(self.pending_by_reason.values())

    # -- recording ---------------------------------------------------------- #

    def record(self, reason: str, *, transport: str = TRANSPORT_NONE,
               fingerprint: Optional[str] = None,
               now: Optional[datetime] = None) -> str:
        """Count one refusal. Never raises, never touches the network.

        Returns the reason key it was counted under, which is not always the
        reason passed in — see the cap below.
        """
        key = str(reason or "unknown")[:64]
        if key not in self.counts and len(self.counts) >= MAX_TRACKED_REASONS:
            # A closed vocabulary in practice; the cap exists so a future
            # caller that passes something attacker-influenced cannot grow this
            # map — or the number of durable rows — without bound.
            key = "other"
        moment = now or datetime.now(timezone.utc)
        self.counts[key] = self.counts.get(key, 0) + 1
        self.pending_by_reason[key] = self.pending_by_reason.get(key, 0) + 1
        self.context[key] = {"transport": transport,
                             "fingerprint": fingerprint, "at": moment}
        self.total += 1
        self.last_reason = key
        self.last_transport = transport
        self.last_fingerprint = fingerprint
        self.last_refused_at = moment
        return key

    # -- flushing ----------------------------------------------------------- #

    def due(self, *, monotonic: Optional[float] = None) -> bool:
        """Is there something to write, and has the throttle window passed?"""
        if self.pending <= 0:
            return False
        if self._last_flush_monotonic is None:
            return True
        moment = time.monotonic() if monotonic is None else monotonic
        return (moment - self._last_flush_monotonic) >= self.flush_interval

    def detail(self, reason: str) -> str:
        """A compact, secret-free note for ONE reason's row.

        Scoped to a single reason on purpose. A cross-reason summary here would
        be written by whichever process flushed last and would erase what the
        others had seen — the exact bug this shape replaced.
        """
        context = self.context.get(reason) or {}
        parts = [reason]
        transport = context.get("transport")
        if transport:
            parts.append(f"via {transport}")
        fingerprint = context.get("fingerprint")
        if fingerprint:
            # The caller's value, hashed and truncated. Never ours.
            parts.append(f"supplied_fp={fingerprint}")
        moment = context.get("at")
        if isinstance(moment, datetime):
            parts.append(f"at {moment.isoformat()}")
        return " ".join(parts)[:MAX_DETAIL_CHARS]

    def snapshot(self) -> Dict[str, Any]:
        """What the health endpoint reports about THIS process."""
        return {
            "since_boot": dict(sorted(self.counts.items())),
            "since_boot_total": self.total,
            "pending_flush": self.pending,
            "last_reason": self.last_reason,
            "last_credential_transport": self.last_transport,
            "last_supplied_credential_fingerprint": self.last_fingerprint,
            "last_refused_at": (self.last_refused_at.isoformat()
                                if self.last_refused_at else None),
        }

    def take_pending(self) -> Dict[str, int]:
        """Claim the pending counts, leaving the ledger empty of them.

        Claimed BEFORE the write and restored on failure (see `flush_refusals`)
        so a refusal is never counted twice and never silently dropped.
        """
        claimed = dict(self.pending_by_reason)
        self.pending_by_reason = {}
        return claimed

    def restore_pending(self, claimed: Dict[str, int]) -> None:
        for reason, count in claimed.items():
            self.pending_by_reason[reason] = (
                self.pending_by_reason.get(reason, 0) + count)

    def mark_flushed(self, *, monotonic: Optional[float] = None) -> None:
        self._last_flush_monotonic = (time.monotonic() if monotonic is None
                                      else monotonic)


#: One ledger per process, constructed at import so the counters survive across
#: requests (a per-request ledger would count nothing at all).
LEDGER = RefusalLedger()


UPSERT_REFUSAL_SQL = """
INSERT INTO public.catalyst_source_state (
    source, status, last_refresh_at, last_success_at,
    symbols_covered, events_upserted, detail, scope, updated_at)
VALUES ($1,'error',$2,NULL,0,$3,$4,$5,NOW())
ON CONFLICT (source, scope) DO UPDATE SET
    status = 'error',
    last_refresh_at = EXCLUDED.last_refresh_at,
    events_upserted = catalyst_source_state.events_upserted
                      + EXCLUDED.events_upserted,
    detail = EXCLUDED.detail,
    updated_at = NOW()
"""

READ_REFUSALS_SQL = """
SELECT source, events_upserted, last_refresh_at, detail
FROM public.catalyst_source_state
WHERE source LIKE $1 AND scope = $2
"""


async def flush_refusals(conn, *, ledger: Optional[RefusalLedger] = None,
                         force: bool = False,
                         now: Optional[datetime] = None) -> int:
    """Fold the pending refusals into their durable rows. Returns rows written.

    Best effort by contract: a failure here must never change the response to
    the caller. A refusal that could not be recorded is a lost diagnostic, and
    a refusal that took the endpoint down would be an outage — so the counts
    are put BACK on failure and retried by the next flush rather than dropped.

    `ledger` defaults to the process ledger RESOLVED AT CALL TIME rather than
    bound as a default argument: a default is evaluated once at import, which
    would silently keep writing the original ledger after a caller replaced the
    module-level one — the exact way a diagnostic quietly stops diagnosing.
    """
    ledger = LEDGER if ledger is None else ledger
    if not (force and ledger.pending > 0) and not ledger.due():
        return 0
    moment = now or datetime.now(timezone.utc)
    claimed = ledger.take_pending()
    written = 0
    failed: Dict[str, int] = {}
    for reason, count in claimed.items():
        try:
            await conn.execute(
                UPSERT_REFUSAL_SQL, refusal_state_source(reason), moment,
                count, ledger.detail(reason), SCOPE_PRODUCT)
            written += 1
        except Exception:
            failed[reason] = count
    if failed:
        ledger.restore_pending(failed)
        logger.warning("external ingress: refusal ledger flush failed",
                       extra={"extra_data": {
                           "event": "external_refusal_flush_error",
                           "reasons": sorted(failed)}}, exc_info=False)
    ledger.mark_flushed()
    return written


async def read_refusals(conn) -> Dict[str, Any]:
    """The DURABLE refusal counts, which outlive this process AND its peers.

    Per reason, because that is the diagnostic. The total is derived rather
    than stored: a stored total is one more thing two machines can disagree
    about, and it was never the number anyone needed.

    Returns a bounded dict even on failure: the health endpoint must degrade to
    "unknown" rather than 500, because an operator reads it precisely when
    something is already wrong.
    """
    try:
        rows = await conn.fetch(READ_REFUSALS_SQL,
                                f"{REFUSAL_STATE_PREFIX}\\_%", SCOPE_PRODUCT)
    except Exception:
        logger.warning("external ingress: refusal ledger unreadable",
                       exc_info=False)
        return {"available": False}

    by_reason: Dict[str, int] = {}
    last_at: Optional[datetime] = None
    last_detail: Optional[str] = None
    for row in rows:
        reason = str(row["source"])[len(REFUSAL_STATE_PREFIX) + 1:]
        by_reason[reason] = int(row["events_upserted"] or 0)
        moment = row["last_refresh_at"]
        if moment is not None and (last_at is None or moment > last_at):
            last_at, last_detail = moment, row["detail"]
    return {
        "available": True,
        # Empty rather than absent when nothing has ever been refused: "never
        # refused" and "cannot read" are different answers and the whole point
        # is not to conflate them.
        "by_reason": dict(sorted(by_reason.items())),
        "recorded_total": sum(by_reason.values()),
        "last_refused_at": last_at.isoformat() if last_at else None,
        "last_detail": last_detail,
    }


__all__ = [
    "REFUSAL_STATE_PREFIX", "refusal_state_source",
    "DEFAULT_FLUSH_INTERVAL_SECONDS",
    "TRANSPORT_NONE", "TRANSPORT_HEADER", "TRANSPORT_QUERY",
    "REASON_NO_CREDENTIAL", "REASON_BAD_CREDENTIAL", "REASON_ROUTE_NOT_FOUND",
    "MAX_TRACKED_REASONS", "MAX_DETAIL_CHARS",
    "credential_fingerprint", "token_is_url_safe", "RefusalLedger", "LEDGER",
    "flush_refusals", "read_refusals",
]
