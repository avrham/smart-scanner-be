"""Persisted research lifecycle runs — so the funnel can be measured, not recalled.

WHY
---
The first research cohort produced one candidate out of seven scans. That
number is not yet information: it could be the normal rate, an unusually good
session, or an artefact of which five symbols happened to be admitted first.
Telling those apart needs several sessions of the SAME measurement, and stdout
is not a measurement.

WHAT IS PERSISTED
-----------------
One parent row per run (the counters, the context it was pinned to, the
provider cost in both directions) and one child row per symbol per run (the
single lifecycle state that symbol was in). The child rows are not a
convenience: `PRIMARY KEY (run_id, symbol)` is what makes exactly-once
accounting a property of the database rather than a claim by the writer.

CONSERVATION IS RECORDED, NOT ENFORCED BY OMISSION
--------------------------------------------------
A run whose funnel does not conserve is still written, with
`funnel_conserved = false`. Refusing to store it would delete the evidence of
the bug it proves. The lifecycle raises AFTER the row is safely on disk.

IDEMPOTENCY
-----------
`run_key` is unique and derived from the dispatch identity. A leased task that
is retried, or a scheduler that fires twice for one occurrence, updates the
existing run instead of inventing a second history. A deliberate manual re-run
gets its own key, because it genuinely is another run.

WHAT THIS DOES NOT DO
---------------------
No thresholds are tuned from these numbers, no predictive claim is made, and
there is no outcome column. `research_candidate` means "survived the screen".
Whether surviving the screen is worth anything is a question that needs far
more than one candidate, and pretending otherwise here would make the data
worse, not better.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import app.research_funnel as rf

logger = logging.getLogger(__name__)

RESEARCH_RUN_CONTRACT_VERSION = "smart_scanner_research_lifecycle_run.v1"

RUN_STATUS_RUNNING = "running"
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_DRY_RUN = "dry_run"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_BLOCKED_STALE = "blocked_stale_core_history"
RUN_STATUS_BLOCKED_CONFIG = "blocked_canonical_config_unavailable"

RUN_STATUSES = (RUN_STATUS_RUNNING, RUN_STATUS_COMPLETED, RUN_STATUS_DRY_RUN,
                RUN_STATUS_FAILED, RUN_STATUS_BLOCKED_STALE,
                RUN_STATUS_BLOCKED_CONFIG)

#: Statuses that mean the run got far enough for its funnel to be meaningful.
#: A blocked run has real counters too (the pool did not change), but it did no
#: work, so including it in a conversion rate would dilute the measurement with
#: sessions in which nothing was attempted.
MEASURABLE_STATUSES = (RUN_STATUS_COMPLETED,)

#: Hard cap on the stored summary, so a future stage that returns a large
#: structure cannot turn the audit table into a payload store.
MAX_SUMMARY_BYTES = 60_000


# --------------------------------------------------------------------------- #
# writing
# --------------------------------------------------------------------------- #

START_SQL = """
INSERT INTO public.research_lifecycle_runs (
    run_key, contract_version, started_at, status, target_session)
VALUES ($1, $2, $3, 'running', $4)
ON CONFLICT (run_key) DO UPDATE SET
    -- A retry re-opens the SAME run rather than creating a second one. The
    -- original started_at is kept: the run began when it began.
    status = 'running', updated_at = NOW()
RETURNING id, started_at
"""


async def start_run(conn, *, run_key: str, target_session: Optional[date],
                    now: Optional[datetime] = None) -> Dict[str, Any]:
    """Open (or re-open) a run. Written BEFORE any work, so a crash leaves a
    `running` row rather than no evidence that anything was attempted."""
    moment = now or datetime.now(timezone.utc)
    row = await conn.fetchrow(START_SQL, run_key, RESEARCH_RUN_CONTRACT_VERSION,
                              moment, target_session)
    return {"id": str(row["id"]), "run_key": run_key,
            "started_at": row["started_at"]}


FINISH_SQL = """
UPDATE public.research_lifecycle_runs SET
    completed_at = $2, duration_seconds = $3, status = $4,
    failure_summary = $5,
    target_session = COALESCE($6, target_session),
    discovery_reference_session = $7,
    canonical_history_fresh = $8, canonical_config_hash = $9,
    canonical_min_price = $10,
    symbols_considered = $11, symbols_selected = $12,
    admission_passed = $13, admission_rejected = $14,
    admission_unknown = $15, admission_pending = $16,
    history_warmups_attempted = $17, history_ready = $18,
    history_unavailable = $19, history_failed = $20,
    research_scanned = $21, research_candidates = $22,
    provider_calls_used = $23, provider_calls_avoided = $24,
    provider_budget = $25, provider_budget_exhausted = $26,
    bars_inserted = $27,
    enrichment_symbols = $28, enrichment_sources_ok = $29,
    enrichment_sources_failed = $30,
    funnel_conserved = $31, summary = $32::jsonb, updated_at = NOW()
WHERE id = $1
"""

SYMBOL_SQL = """
INSERT INTO public.research_lifecycle_run_symbols (
    run_id, symbol, lifecycle_state, admission_tier, warmed,
    provider_calls, bars_inserted)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (run_id, symbol) DO UPDATE SET
    lifecycle_state = EXCLUDED.lifecycle_state,
    admission_tier = EXCLUDED.admission_tier,
    warmed = EXCLUDED.warmed,
    provider_calls = EXCLUDED.provider_calls,
    bars_inserted = EXCLUDED.bars_inserted
"""


def _bounded_summary(summary: Dict[str, Any]) -> str:
    """Serialise the summary, dropping the per-symbol list if it would blow the
    cap — the child table already holds that, verbatim and queryable."""
    payload = dict(summary)
    text = json.dumps(payload, default=str)
    if len(text) <= MAX_SUMMARY_BYTES:
        return text
    funnel = payload.get("funnel")
    if isinstance(funnel, dict) and "per_symbol" in funnel:
        funnel = dict(funnel)
        funnel.pop("per_symbol", None)
        payload["funnel"] = funnel
        text = json.dumps(payload, default=str)
    if len(text) <= MAX_SUMMARY_BYTES:
        return text
    # Last resort: keep the shape, say plainly what was dropped. Never a
    # truncated JSON string that a reader would have to guess at.
    return json.dumps({"summary_omitted": "exceeded_max_summary_bytes",
                       "status": payload.get("status"),
                       "bytes": len(text)}, default=str)


async def finish_run(conn, run_id: str, *, summary: Dict[str, Any],
                     funnel: Optional[Dict[str, Any]] = None,
                     warm_detail: Optional[Dict[str, Dict[str, int]]] = None,
                     now: Optional[datetime] = None) -> Dict[str, Any]:
    """Close the run: counters from the FUNNEL (never recounted here), the
    per-symbol rows, and the bounded summary.

    Every counter is read out of the funnel partition rather than derived a
    second time. Two independent count paths is how the previous report came to
    disagree with itself.
    """
    moment = now or datetime.now(timezone.utc)
    funnel = funnel or summary.get("funnel") or {}
    states = funnel.get("states") or {}
    admission = funnel.get("admission") or {}
    provider = funnel.get("provider") or {}
    warm = summary.get("warmup") or {}
    enrich = summary.get("enrichment") or {}
    sources = enrich.get("sources") or {}
    conservation = funnel.get("conservation") or {}

    ok_sources = sum(1 for s in sources.values()
                     if isinstance(s, dict) and s.get("status") == "ok")
    failed_sources = sum(1 for s in sources.values()
                         if isinstance(s, dict)
                         and s.get("status") in ("error", "unavailable"))

    started = summary.get("started_at")
    await conn.execute(
        FINISH_SQL, run_id, moment,
        summary.get("duration_seconds"),
        summary.get("status") or RUN_STATUS_COMPLETED,
        (summary.get("blocked_detail") or summary.get("failure_summary")
         or None),
        _as_date(summary.get("target_completed_session")),
        _as_date((summary.get("discovery_refresh") or {})
                 .get("reference_session_date")),
        (summary.get("core_freshness") or {}).get("fresh"),
        (summary.get("canonical_config") or {}).get("config_hash"),
        (summary.get("canonical_config") or {}).get("min_price"),
        int((summary.get("admission_pool") or {}).get("considered") or 0),
        int(funnel.get("selected_for_research") or 0),
        int(admission.get("passed") or 0),
        int(admission.get("rejected") or 0),
        int(admission.get("unknown") or 0),
        int(admission.get("pending") or 0),
        _count(warm.get("warmups_attempted", warm.get("selected"))),
        int(states.get(rf.LIFECYCLE_SCAN_PENDING, 0)
            + (funnel.get("scanned") or 0)),
        int(states.get(rf.LIFECYCLE_HISTORY_UNAVAILABLE, 0)),
        int(states.get(rf.LIFECYCLE_HISTORY_FAILED, 0)),
        int(funnel.get("scanned") or 0),
        int(funnel.get("research_candidates") or 0),
        int(provider.get("calls_used")
            or summary.get("provider_requests_used") or 0),
        int(provider.get("calls_avoided")
            or summary.get("provider_requests_avoided_by_admission") or 0),
        _int_or_none(summary.get("provider_budget")),
        bool(warm.get("budget_exhausted") or False),
        int(warm.get("bars_inserted") or 0),
        int(enrich.get("enriched") or 0), ok_sources, failed_sources,
        bool(conservation.get("ok", True)),
        _bounded_summary(summary),
    )

    warm_detail = warm_detail or {}
    for entry in (funnel.get("per_symbol") or []):
        symbol = entry.get("symbol")
        if not symbol:
            continue
        detail = warm_detail.get(symbol, {})
        await conn.execute(
            SYMBOL_SQL, run_id, symbol, entry["lifecycle_state"],
            entry["admission_tier"], bool(detail.get("warmed", False)),
            int(detail.get("provider_calls", 0)),
            int(detail.get("bars_inserted", 0)))

    return {"run_id": run_id, "symbols_recorded": len(funnel.get("per_symbol") or []),
            "funnel_conserved": bool(conservation.get("ok", True)),
            "started_at": started}


async def fail_run(conn, run_id: str, *, reason: str,
                   now: Optional[datetime] = None) -> None:
    """Record a run that died. A bounded reason string — a class name or a code,
    never an exception payload, which could carry a URL or a key."""
    await conn.execute(
        "UPDATE public.research_lifecycle_runs SET status='failed', "
        "completed_at=$2, failure_summary=$3, updated_at=NOW() WHERE id=$1",
        run_id, now or datetime.now(timezone.utc), str(reason)[:400])


def _as_date(value: Any) -> Optional[date]:
    if value is None or isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None


def _count(value: Any) -> int:
    """A COUNT, from either a number or the collection it describes.

    `warmup.selected` is the list of symbols a human reads in the summary, and
    an earlier version of this writer passed it straight to an INTEGER column.
    `int([...])` raises, the whole `finish_run` was swallowed by the caller's
    `except`, and the run row sat at `running` for ever while its task reported
    success — a lost audit for a run that had actually completed.

    The lesson is not "remember to call len()". It is that the audit writer must
    not be able to lose a run to a field's shape, so it accepts either and the
    lifecycle also emits an explicit `warmups_attempted` scalar beside the list.
    """
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return len(value)
    except TypeError:
        return 0


def _int_or_none(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# P8 — reading across sessions
#
# Every question in the measurement brief, as queries over the two tables. No
# rollup tables: a derived table is a second copy of the truth that goes stale
# in a different way from the first.
# --------------------------------------------------------------------------- #

RECENT_RUNS_SQL = """
SELECT id, run_key, started_at, completed_at, duration_seconds, status,
       target_session, discovery_reference_session, canonical_history_fresh,
       canonical_config_hash, canonical_min_price,
       symbols_selected, admission_passed, admission_rejected,
       admission_unknown, admission_pending,
       history_warmups_attempted, history_ready, history_unavailable,
       history_failed, research_scanned, research_candidates,
       provider_calls_used, provider_calls_avoided, provider_budget,
       provider_budget_exhausted, bars_inserted,
       enrichment_symbols, enrichment_sources_ok, enrichment_sources_failed,
       funnel_conserved
FROM public.research_lifecycle_runs
ORDER BY started_at DESC
LIMIT $1
"""


async def recent_runs(conn, *, limit: int = 20) -> List[Dict[str, Any]]:
    """"Show me the last N research lifecycle runs" — without parsing a log."""
    return [dict(r) for r in await conn.fetch(RECENT_RUNS_SQL, max(1, min(limit, 200)))]


#: Cross-run aggregates. Restricted to MEASURABLE runs so a blocked session
#: cannot dilute a rate with work that was never attempted.
AGGREGATE_SQL = """
SELECT count(*)::int                                AS runs,
       min(started_at)                              AS first_run_at,
       max(started_at)                              AS last_run_at,
       sum(symbols_selected)::int                   AS symbols_selected,
       sum(admission_passed)::int                   AS admission_passed,
       sum(admission_rejected)::int                 AS admission_rejected,
       sum(admission_unknown)::int                  AS admission_unknown,
       sum(research_scanned)::int                   AS research_scanned,
       sum(research_candidates)::int                AS research_candidates,
       sum(provider_calls_used)::int                AS provider_calls_used,
       sum(provider_calls_avoided)::int             AS provider_calls_avoided,
       sum(bars_inserted)::int                      AS bars_inserted,
       count(*) FILTER (WHERE NOT funnel_conserved)::int AS runs_not_conserving,
       count(*) FILTER (WHERE provider_budget_exhausted)::int AS runs_budget_exhausted
FROM public.research_lifecycle_runs
WHERE status = ANY($1::text[])
"""

#: Median, not mean. One run that spent twelve requests on a symbol the
#: provider then refused would drag a mean somewhere no run actually was.
MEDIAN_CALLS_SQL = """
SELECT percentile_cont(0.5) WITHIN GROUP (
         ORDER BY provider_calls_used::numeric / NULLIF(research_candidates,0))
       AS median_calls_per_candidate
FROM public.research_lifecycle_runs
WHERE status = ANY($1::text[]) AND research_candidates > 0
"""

#: Symbol-level persistence across runs: seen once, seen repeatedly, and the
#: ones that were a candidate more than once. "Discovered again" and "survived
#: again" are different facts and are counted separately.
SYMBOL_PERSISTENCE_SQL = """
WITH per_symbol AS (
  SELECT s.symbol,
         count(DISTINCT s.run_id)::int AS runs_seen,
         count(DISTINCT s.run_id) FILTER (
           WHERE s.lifecycle_state = 'research_candidate')::int AS runs_candidate
  FROM public.research_lifecycle_run_symbols s
  JOIN public.research_lifecycle_runs r ON r.id = s.run_id
  WHERE r.status = ANY($1::text[])
  GROUP BY s.symbol)
SELECT count(*)::int                                       AS unique_symbols,
       count(*) FILTER (WHERE runs_seen > 1)::int          AS repeated_symbols,
       count(*) FILTER (WHERE runs_candidate > 0)::int     AS symbols_ever_candidate,
       count(*) FILTER (WHERE runs_candidate > 1)::int     AS repeat_candidates
FROM per_symbol
"""

#: Conversion by DISCOVERY REASON. The one cut that could change how discovery
#: is read — "top gainer" and "most active" may not survive the screen at the
#: same rate. Descriptive only: it is not fed back into prioritisation.
BY_REASON_SQL = """
SELECT reason,
       count(DISTINCT s.symbol)::int AS symbols,
       count(DISTINCT s.symbol) FILTER (
         WHERE s.lifecycle_state = 'research_candidate')::int AS candidates
FROM public.research_lifecycle_run_symbols s
JOIN public.research_lifecycle_runs r ON r.id = s.run_id
JOIN public.research_symbols rs ON rs.symbol = s.symbol
CROSS JOIN LATERAL unnest(rs.discovery_reasons) AS reason
WHERE r.status = ANY($1::text[])
GROUP BY reason
ORDER BY symbols DESC
"""

#: Where symbols stop, across every measurable run.
STATE_DISTRIBUTION_SQL = """
SELECT s.lifecycle_state, count(*)::int AS occurrences,
       count(DISTINCT s.symbol)::int AS symbols
FROM public.research_lifecycle_run_symbols s
JOIN public.research_lifecycle_runs r ON r.id = s.run_id
WHERE r.status = ANY($1::text[])
GROUP BY s.lifecycle_state
ORDER BY occurrences DESC
"""


async def measurement(conn, *, statuses: Optional[List[str]] = None,
                      limit: int = 20) -> Dict[str, Any]:
    """The multi-session picture. Descriptive, and labelled as such.

    Rates carry their denominators here for the same reason they do in
    `research_funnel`: an aggregate percentage with an unnamed population is
    the kind of number that gets quoted back as a claim.
    """
    scope = list(statuses or MEASURABLE_STATUSES)
    agg = dict(await conn.fetchrow(AGGREGATE_SQL, scope))
    med = await conn.fetchval(MEDIAN_CALLS_SQL, scope)
    persistence = dict(await conn.fetchrow(SYMBOL_PERSISTENCE_SQL, scope))
    by_reason = [dict(r) for r in await conn.fetch(BY_REASON_SQL, scope)]
    states = [dict(r) for r in await conn.fetch(STATE_DISTRIBUTION_SQL, scope)]

    for row in by_reason:
        row["candidate_rate"] = rf.rate(row["candidates"], row["symbols"],
                                        of="symbols_discovered_for_reason")

    return {
        "contract_version": RESEARCH_RUN_CONTRACT_VERSION,
        "measured_statuses": scope,
        "runs": agg,
        "rates": {
            "admission_pass_rate": rf.rate(
                agg["admission_passed"] or 0, agg["symbols_selected"] or 0,
                of="symbols_selected_across_runs"),
            "candidate_conversion_rate": rf.rate(
                agg["research_candidates"] or 0, agg["research_scanned"] or 0,
                of="symbols_scanned_across_runs"),
        },
        "median_provider_calls_per_candidate":
            float(med) if med is not None else None,
        "symbol_persistence": persistence,
        "by_discovery_reason": by_reason,
        "lifecycle_state_distribution": states,
        "recent_runs": await recent_runs(conn, limit=limit),
        # Said in the payload, not only in a docstring, because this structure
        # is what a future reader will quote.
        "interpretation": (
            "Descriptive funnel quality only. No threshold in this system is "
            "derived from these numbers, and none of them is evidence that a "
            "research candidate outperforms anything."),
    }


__all__ = [
    "RESEARCH_RUN_CONTRACT_VERSION", "RUN_STATUSES", "MEASURABLE_STATUSES",
    "RUN_STATUS_RUNNING", "RUN_STATUS_COMPLETED", "RUN_STATUS_DRY_RUN",
    "RUN_STATUS_FAILED", "RUN_STATUS_BLOCKED_STALE",
    "RUN_STATUS_BLOCKED_CONFIG", "MAX_SUMMARY_BYTES",
    "start_run", "finish_run", "fail_run", "recent_runs", "measurement",
    "_count",
]
