"""Catalyst evidence for research SURVIVORS only, written to the research cohort.

WHAT CHANGED SINCE LAST MILESTONE
---------------------------------
This stage existed but fetched nothing. The reason was real: refreshing EDGAR
for one discovered symbol wrote `catalyst_source_state` under `sec_edgar` — the
single row the Product API reads to decide whether its SEC coverage is fresh
FOR THE FROZEN 25. Row-level security refused the write, which is the only
reason it was a design note and not an incident.

Migration 028 gave that table a `scope`, so "EDGAR is fresh for research" and
"EDGAR is fresh for the product" are now two rows and two facts. With that in
place the stage can do its job.

WHO GETS ENRICHED
-----------------
Only `candidate_state = 'research_candidate'`. Not the discovered pool, not the
admission-passed set, not `scanned_not_candidate`. Enrichment is the expensive
tail of the funnel and it is spent on symbols that already survived a
deterministic screen — which is also why the bound (`MAX_ENRICHED_SYMBOLS`) is
small: it is sized to the number of survivors a person actually reads, not to
the size of the pool.

CATALYSTS ARE EVIDENCE, NOT A VERDICT
-------------------------------------
This module NEVER writes `research_symbols`. It cannot promote a
`scanned_not_candidate` symbol, cannot alter `candidate_state`, and produces no
score and no ordering. A symbol with an 8-K and no structure is still not a
candidate; the filing is context for a human reading a symbol that already
passed, and nothing else. That is enforced by a test, and by this module
holding no UPDATE against `research_symbols` at all.

POINT-IN-TIME
-------------
Every write carries `observed_at = now`, so a filing we learn about today can
never be presented as having been knowable last week. The catalyst, news and
SEC readers already gate on `observed_at <= as_of`; this stage's only duty is
to record honestly WHEN we learned something, which it does by not backdating.

PER-SOURCE ISOLATION
--------------------
Each source is attempted independently and its outcome recorded independently.
EDGAR being down must not cost the run its news, and a missing provider
credential is an ordinary `unavailable`, not an error. No source failure
propagates out of this function — enrichment is the last stage, and a run that
produced a candidate has already succeeded.

PROVIDER COST, HONESTLY SPLIT
-----------------------------
SEC is free of the market-data budget entirely: EDGAR is a public endpoint with
its own rate limit, which `SecClient` already enforces. News and earnings go
through the SAME provider whose request budget the history warmup needs, so
they get their own small ceiling, spent AFTER warmup, and counted separately in
the summary. A reader must be able to see which requests bought history and
which bought context.

LICENSING
---------
Analyst grades are NOT here. They are FMP, `internal_research_only`, entitled
per symbol, and enriching a research symbol with them would spend a restricted
entitlement to produce a field nothing may display. See app/source_licensing.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import app.research_universe as ru
from app.source_scope import SCOPE_RESEARCH

logger = logging.getLogger(__name__)

RESEARCH_ENRICHMENT_CONTRACT_VERSION = "smart_scanner_research_enrichment.v1"

#: Hard cap on symbols enriched per lifecycle run. Derived from the observed
#: candidate volume (one per run so far), not from the pool size: a bound that
#: tracks the pool would grow with discovery noise, which is the opposite of
#: what a survivor stage is for. Ten is already more than a person reviews in
#: a sitting.
MAX_ENRICHED_SYMBOLS = 10

#: Market-data requests this stage may spend, separately from the history
#: warmup budget. Two per candidate at the current cap, and never allowed to
#: borrow from the warmup ceiling — buying context for today's survivor must
#: not cost tomorrow's symbol its history.
MAX_ENRICHMENT_PROVIDER_REQUESTS = 4

SOURCE_SEC = "sec_filings"
SOURCE_NEWS = "company_news"
SOURCE_EARNINGS = "earnings"
ENRICHMENT_SOURCES = (SOURCE_SEC, SOURCE_NEWS, SOURCE_EARNINGS)

#: FMP, internal_research_only, entitled per symbol. Named here so its absence
#: is a decision on the record rather than an omission someone has to notice.
EXCLUDED_SOURCES = {
    "analyst_grades": "fmp_internal_research_only_and_entitled_per_symbol",
}

STATUS_OK = "ok"
STATUS_UNAVAILABLE = "unavailable"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"

REASON_NO_CANDIDATES = "no_research_candidates"
REASON_NO_CREDENTIAL = "provider_credential_absent"
REASON_NO_USER_AGENT = "sec_user_agent_absent"
REASON_BUDGET = "enrichment_provider_budget_exhausted"


CANDIDATE_SQL = """
SELECT symbol
FROM public.research_symbols
WHERE candidate_state = $1
ORDER BY latest_reference_session DESC, symbol
LIMIT $2
"""


async def candidate_symbols(conn, *, limit: int = MAX_ENRICHED_SYMBOLS
                            ) -> List[str]:
    """The survivors, and only the survivors.

    The filter is on `candidate_state`, the column that records what the SCREEN
    found — never on `discovery_reasons`, which records only why we looked.
    """
    rows = await conn.fetch(CANDIDATE_SQL, ru.CANDIDATE_RESEARCH_CANDIDATE,
                            max(0, min(int(limit), MAX_ENRICHED_SYMBOLS)))
    return [r["symbol"] for r in rows]


async def _enrich_sec(conn, symbols: Sequence[str], *, user_agent: str,
                      now: datetime) -> Dict[str, Any]:
    """EDGAR. Arbitrary symbols are genuinely safe here: filings are looked up
    by CIK from the public ticker map, the endpoint is free, and `SecClient`
    enforces the SEC's own request interval. Costs zero market-data budget."""
    import app.sec_ingest as si
    if not user_agent:
        return {"status": STATUS_UNAVAILABLE, "reason": REASON_NO_USER_AGENT,
                "provider_requests": 0}
    client = si.SecClient(user_agent)
    try:
        result = await si.refresh_sec_filings(
            conn, client, symbols, now=now, scope=SCOPE_RESEARCH)
    finally:
        close = getattr(client, "aclose", None)
        if close is not None:
            await close()
    return {"status": result.get("status", STATUS_ERROR),
            "reason": result.get("reason"),
            "filings_inserted": result.get("filings_inserted"),
            "unresolved_symbols": result.get("unresolved_symbols"),
            "scope": result.get("scope"),
            # EDGAR is not the market-data provider; its requests do not come
            # out of the budget the history warmup needs.
            "provider_requests": 0}


async def _enrich_news(conn, symbols: Sequence[str], *, api_key: str,
                       now: datetime, budget: int) -> Dict[str, Any]:
    """Company news through the market-data provider. Budgeted separately."""
    import app.news_ingest as ni
    if not api_key:
        return {"status": STATUS_UNAVAILABLE, "reason": REASON_NO_CREDENTIAL,
                "provider_requests": 0}
    if budget <= 0:
        return {"status": STATUS_SKIPPED, "reason": REASON_BUDGET,
                "provider_requests": 0}
    from app.workers.massive_client import MassiveClient
    client = MassiveClient(api_key=api_key)
    # One page per symbol: enough to see whether anything was published, and
    # bounded by the same budget the summary reports.
    capped = list(symbols)[:budget]
    try:
        result = await ni.refresh_company_news(
            conn, client, capped, now=now, max_pages=1, scope=SCOPE_RESEARCH)
    finally:
        close = getattr(client, "aclose", None)
        if close is not None:
            await close()
    return {"status": result.get("status", STATUS_ERROR),
            "reason": result.get("reason"),
            "articles_inserted": result.get("articles_inserted"),
            "scope": result.get("scope"),
            "symbols": len(capped),
            "provider_requests": len(capped)}


async def _enrich_earnings(conn, symbols: Sequence[str], *, api_key: str,
                           now: datetime, budget: int) -> Dict[str, Any]:
    """Earnings / report-filing dates. Plan-gated on this deployment; an
    `unavailable` here is the expected answer, not a failure."""
    import app.catalyst_ingest as ci
    if not api_key:
        return {"status": STATUS_UNAVAILABLE, "reason": REASON_NO_CREDENTIAL,
                "provider_requests": 0}
    if budget <= 0:
        return {"status": STATUS_SKIPPED, "reason": REASON_BUDGET,
                "provider_requests": 0}
    from app.workers.massive_client import MassiveClient
    client = MassiveClient(api_key=api_key)
    capped = list(symbols)[:budget]
    try:
        result = await ci.refresh_catalysts(conn, client, capped, now=now,
                                            scope=SCOPE_RESEARCH)
    finally:
        close = getattr(client, "aclose", None)
        if close is not None:
            await close()
    sources = result.get("sources") or {}
    # Two sub-sources behind one stage. `ok` only if at least one produced.
    statuses = {k: v.get("status") for k, v in sources.items()}
    overall = (STATUS_OK if STATUS_OK in statuses.values()
               else (STATUS_UNAVAILABLE if STATUS_UNAVAILABLE in statuses.values()
                     else STATUS_ERROR))
    return {"status": overall, "sub_sources": statuses,
            "scope": result.get("scope"), "symbols": len(capped),
            "provider_requests": len(capped)}


async def enrich_research_candidates(
        conn, *, now: Optional[datetime] = None,
        limit: int = MAX_ENRICHED_SYMBOLS,
        provider_budget: int = MAX_ENRICHMENT_PROVIDER_REQUESTS,
        massive_api_key: str = "", sec_user_agent: str = "",
        enable_sec: bool = True, enable_news: bool = True,
        enable_earnings: bool = True) -> Dict[str, Any]:
    """Bounded enrichment of the survivors. Never raises.

    Each source is isolated: an exception in one is caught, recorded against
    that source, and the next is still attempted. Enrichment is the last stage
    of a run that has already produced its result, so it must not be able to
    turn a successful run into a failed one.
    """
    moment = now or datetime.now(timezone.utc)
    symbols = await candidate_symbols(conn, limit=limit)
    summary: Dict[str, Any] = {
        "contract_version": RESEARCH_ENRICHMENT_CONTRACT_VERSION,
        "scope": SCOPE_RESEARCH,
        "eligible_symbols": symbols,
        "enriched": 0,
        "provider_requests": 0,
        "provider_budget": int(provider_budget),
        "sources": {},
        "excluded_sources": dict(EXCLUDED_SOURCES),
    }
    if not symbols:
        for name in ENRICHMENT_SOURCES:
            summary["sources"][name] = {"status": STATUS_SKIPPED,
                                        "reason": REASON_NO_CANDIDATES,
                                        "provider_requests": 0}
        return summary

    remaining = int(provider_budget)
    plan = [
        (SOURCE_SEC, enable_sec,
         lambda: _enrich_sec(conn, symbols, user_agent=sec_user_agent,
                             now=moment)),
        (SOURCE_NEWS, enable_news,
         lambda: _enrich_news(conn, symbols, api_key=massive_api_key,
                              now=moment, budget=remaining)),
        (SOURCE_EARNINGS, enable_earnings,
         lambda: _enrich_earnings(conn, symbols, api_key=massive_api_key,
                                  now=moment, budget=remaining)),
    ]

    for name, enabled, run in plan:
        if not enabled:
            summary["sources"][name] = {"status": STATUS_SKIPPED,
                                        "reason": "disabled_by_caller",
                                        "provider_requests": 0}
            continue
        try:
            result = await run()
        except Exception as exc:                      # noqa: BLE001
            # Bounded and secret-free: the class name, never the message, which
            # on an HTTP client can carry a URL with a key in it.
            logger.warning("research enrichment source %s failed", name,
                           exc_info=True)
            result = {"status": STATUS_ERROR, "reason": type(exc).__name__,
                      "provider_requests": 0}
        spent = int(result.get("provider_requests") or 0)
        remaining = max(0, remaining - spent)
        summary["provider_requests"] += spent
        summary["sources"][name] = result

    summary["enriched"] = len(symbols)
    summary["provider_budget_remaining"] = remaining
    return summary


__all__ = [
    "RESEARCH_ENRICHMENT_CONTRACT_VERSION", "MAX_ENRICHED_SYMBOLS",
    "MAX_ENRICHMENT_PROVIDER_REQUESTS", "ENRICHMENT_SOURCES",
    "EXCLUDED_SOURCES", "SOURCE_SEC", "SOURCE_NEWS", "SOURCE_EARNINGS",
    "STATUS_OK", "STATUS_UNAVAILABLE", "STATUS_ERROR", "STATUS_SKIPPED",
    "candidate_symbols", "enrich_research_candidates",
]
