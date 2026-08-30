"""Admit discovered symbols into research, and warm their history — bounded.

DB + PROVIDER. The pure layer is `app/research_universe.py`; everything here
touches a connection or the market-data provider.

WHAT IT REUSES RATHER THAN REBUILDS
-----------------------------------
The provider abstraction (`app.providers.get_market_data_provider`), the bar
normaliser (`normalize_daily_bars`) and the canonical upsert
(`upsert_daily_bars`) are the SAME ones the frozen universe's warmup uses.
There is one way daily bars enter this database and this is not a second one —
a research bar and a frozen-universe bar are the same row shape written by the
same code, which is what makes the research scan comparable to a real scan at
all.

WHAT IT DELIBERATELY DOES NOT REUSE
-----------------------------------
`history_warmup_universes`. That model is immutable by database trigger the
moment a universe leaves `draft`, which is correct for a frozen cohort and
wrong for a set that grows. See migration 026.

WHY EVERY LIMIT IS SMALL
------------------------
Massive Basic allows five requests a minute, and this repository already paces
warmup at ONE symbol per 75 seconds behind a machine-wide advisory lock. Those
are the real constraints; a limit that ignores them is decoration. Five
symbols per run is therefore about six minutes of provider wall time, and the
provider-request budget is counted and enforced rather than hoped for.

FAILURE IS PER SYMBOL
---------------------
One symbol the provider will not serve must never cost the other four their
warmup, and it must never strand a queue. Each symbol is attempted
independently, its error is mapped through the existing bounded classifier
(`map_provider_error`), and a terminal answer parks it as `unavailable` rather
than retrying forever.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Set

import app.research_universe as ru
from app.config import settings
from app.history_warmup_execute import (HISTORY_WARMUP_ADVISORY_LOCK_KEY,
                                        map_provider_error, normalize_daily_bars,
                                        upsert_daily_bars)

logger = logging.getLogger(__name__)

SOURCE_STATE_RESEARCH = "external_research_warmup"

#: The frozen candidate universe. Read ONLY, and only to know which discovered
#: symbols are already inside it — a symbol in the 25 is not a research symbol,
#: because it is already being scanned properly.
SCANNER_UNIVERSE_CODE = "WYCKOFF-HISTORY-WARMUP-QUALIFICATION"


# --------------------------------------------------------------------------- #
# reads
# --------------------------------------------------------------------------- #

FROZEN_UNIVERSE_SQL = """
SELECT s.symbol
FROM public.history_warmup_universe_symbols s
JOIN public.history_warmup_universes u ON u.id = s.universe_id
WHERE u.universe_code = ANY($1::text[])
"""

#: Discovery candidates OUTSIDE the frozen universe, aggregated across every
#: observation we hold — so `discovery_observation_count` is a count of
#: separate market sessions, not of rows.
DISCOVERY_POOL_SQL = """
SELECT c.symbol,
       array_agg(DISTINCT c.list_kind ORDER BY c.list_kind)   AS reasons,
       count(DISTINCT c.reference_session_date)               AS observation_count,
       min(c.observed_at)                                     AS first_observed_at,
       max(c.observed_at)                                     AS latest_observed_at,
       min(c.reference_session_date)                          AS first_reference_session,
       max(c.reference_session_date)                          AS latest_reference_session,
       min(c.session_date)                                    AS first_actionable_session,
       min(c.rank)                                            AS best_rank,
       min(c.source)                                          AS discovery_source
FROM public.external_discovery_candidates c
WHERE c.in_scanner_universe = false
  AND c.reference_session_date IS NOT NULL
  AND c.reference_session_date >= $1
GROUP BY c.symbol
"""

#: Period GROUPS, not just a bar count. The canonical readiness gate is 24
#: COMPLETED MONTHS; a daily count cannot answer it, and the provider caps
#: history at two years, so a symbol can hold ~500 bars and be either side of
#: the gate depending on where its listing starts.
BAR_COUNTS_SQL = """
SELECT symbol, count(*)::int AS bars,
       min(trading_date) AS first_session, max(trading_date) AS latest_session,
       count(DISTINCT date_trunc('month', trading_date))::int AS month_groups,
       count(DISTINCT date_trunc('week', trading_date))::int  AS week_groups
FROM public.daily_bars
WHERE symbol = ANY($1::text[])
GROUP BY symbol
"""


async def frozen_universe_symbols(conn, codes: Sequence[str] = (
        SCANNER_UNIVERSE_CODE, "SMART-SCANNER-REFERENCE-MARKET-V1")) -> Set[str]:
    """Symbols we already hold properly: the frozen 25 and the reference market.

    Both are excluded from research admission. The 25 because they are already
    scanned for real; the reference ETFs because they are market context, not
    subjects — a benchmark that became a research candidate would be comparing
    itself to itself.
    """
    rows = await conn.fetch(FROZEN_UNIVERSE_SQL, list(codes))
    return {str(r["symbol"]).upper() for r in rows}


async def bar_counts(conn, symbols: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    rows = await conn.fetch(BAR_COUNTS_SQL, list(dict.fromkeys(symbols)))
    return {r["symbol"]: {"bars": r["bars"], "first_session": r["first_session"],
                          "latest_session": r["latest_session"],
                          "month_groups": r["month_groups"],
                          "week_groups": r["week_groups"]} for r in rows}


# --------------------------------------------------------------------------- #
# admission
# --------------------------------------------------------------------------- #

UPSERT_RESEARCH_SYMBOL_SQL = """
INSERT INTO public.research_symbols (
    symbol, discovery_source, discovery_reasons, discovery_observation_count,
    first_observed_at, latest_observed_at, first_reference_session,
    latest_reference_session, first_actionable_session, best_rank,
    state, history_daily_bars, history_first_session, history_latest_session,
    licensing_visibility)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (symbol) DO UPDATE SET
    -- Re-discovery ENRICHES the row; it never resets progress. `first_*` stay
    -- first, the reason set unions, and the warmup counters are untouched —
    -- otherwise a symbol rediscovered daily would restart its own history.
    discovery_reasons = (
        SELECT array_agg(DISTINCT r ORDER BY r)
        FROM unnest(research_symbols.discovery_reasons
                    || EXCLUDED.discovery_reasons) AS r),
    discovery_observation_count = GREATEST(
        research_symbols.discovery_observation_count,
        EXCLUDED.discovery_observation_count),
    latest_observed_at = GREATEST(research_symbols.latest_observed_at,
                                  EXCLUDED.latest_observed_at),
    latest_reference_session = GREATEST(research_symbols.latest_reference_session,
                                        EXCLUDED.latest_reference_session),
    best_rank = LEAST(COALESCE(research_symbols.best_rank, 9999),
                      COALESCE(EXCLUDED.best_rank, 9999)),
    updated_at = NOW()
RETURNING (xmax = 0) AS inserted
"""


async def admit_from_discovery(conn, *, since: date,
                               limit: int = ru.MAX_NEW_RESEARCH_SYMBOLS_PER_RUN,
                               now: Optional[datetime] = None,
                               ) -> Dict[str, Any]:
    """Admit up to `limit` discovered symbols into the research domain.

    Admission is NOT universe membership and cannot become it: this writes one
    table, `research_symbols`, and holds no privilege on any universe relation.

    Already-admitted symbols are refreshed rather than re-admitted, and they do
    not consume the new-symbol budget — otherwise a symbol the market notices
    every day would crowd out every symbol it noticed once.
    """
    moment = now or datetime.now(timezone.utc)
    pool = [dict(r) for r in await conn.fetch(DISCOVERY_POOL_SQL, since)]
    if not pool:
        return {"considered": 0, "admitted": 0, "refreshed": 0, "selected": [],
                "excluded_already_held": 0}

    held = await frozen_universe_symbols(conn)
    # `in_scanner_universe` is already false on these rows, but it was computed
    # at INGESTION time against the universe as it stood then. Re-checking here
    # means a symbol can never drift into research because a snapshot aged.
    pool = [r for r in pool if r["symbol"] not in held]

    existing = {r["symbol"] for r in await conn.fetch(
        "SELECT symbol FROM public.research_symbols")}
    counts = await bar_counts(conn, [r["symbol"] for r in pool])
    for row in pool:
        row["daily_bars"] = counts.get(row["symbol"], {}).get("bars", 0)

    fresh = [r for r in pool if r["symbol"] not in existing]
    already = [r for r in pool if r["symbol"] in existing]
    selected = ru.prioritise(fresh, limit=limit)

    admitted = refreshed = 0
    for row in selected + already:
        local = counts.get(row["symbol"], {})
        state = ru.classify_history_state(
            symbol=row["symbol"], daily_bars=local.get("bars", 0),
            week_groups=local.get("week_groups"),
            month_groups=local.get("month_groups"))
        result = await conn.fetchrow(
            UPSERT_RESEARCH_SYMBOL_SQL,
            row["symbol"], row["discovery_source"], list(row["reasons"]),
            int(row["observation_count"]), row["first_observed_at"],
            row["latest_observed_at"], row["first_reference_session"],
            row["latest_reference_session"], row["first_actionable_session"],
            int(row["best_rank"]) if row.get("best_rank") is not None else None,
            state, int(local.get("bars", 0) or 0),
            local.get("first_session"), local.get("latest_session"),
            "internal_research_only")
        if result["inserted"]:
            admitted += 1
        else:
            refreshed += 1

    return {
        "considered": len(pool),
        "admitted": admitted,
        "refreshed": refreshed,
        "excluded_already_held": len(held),
        "selected": [
            {"symbol": r["symbol"], "reasons": list(r["reasons"]),
             "observations": int(r["observation_count"]),
             "local_bars": r["daily_bars"],
             "why": ru.explain_priority(r)}
            for r in selected],
    }


# --------------------------------------------------------------------------- #
# state refresh — recomputed, never trusted
# --------------------------------------------------------------------------- #

async def refresh_states(conn, *, symbols: Optional[Sequence[str]] = None,
                         now: Optional[datetime] = None) -> Dict[str, Any]:
    """Recompute every research symbol's state from what we actually hold.

    Called before selection and after warmup. A stored state is a record of the
    last decision; this is the decision. A process that dies between fetching
    bars and updating a row therefore self-corrects on the next pass instead of
    leaving a symbol stuck in `history_warming` forever.
    """
    rows = [dict(r) for r in await conn.fetch(
        "SELECT symbol, state, warmup_attempts, warmup_last_error_class, "
        "       history_daily_bars, research_scanned_at "
        "FROM public.research_symbols"
        + (" WHERE symbol = ANY($1::text[])" if symbols else ""),
        *( [list(symbols)] if symbols else [] ))]
    if not rows:
        return {"updated": 0, "states": {}}

    counts = await bar_counts(conn, [r["symbol"] for r in rows])
    tally: Dict[str, int] = {}
    updated = 0
    for row in rows:
        local = counts.get(row["symbol"], {})
        bars = int(local.get("bars", 0) or 0)
        state = ru.classify_history_state(
            symbol=row["symbol"], daily_bars=bars,
            week_groups=local.get("week_groups"),
            month_groups=local.get("month_groups"),
            attempts=int(row["warmup_attempts"] or 0),
            last_error_class=row["warmup_last_error_class"])
        # A symbol that has been scanned stays scanned as long as it is still
        # ready — the scan is a fact about the past and re-deriving it away
        # would lose it.
        if state == ru.STATE_RESEARCH_READY and row["research_scanned_at"]:
            state = ru.STATE_RESEARCH_SCANNED
        tally[state] = tally.get(state, 0) + 1
        # Compare against what the ROW currently stores, not against the value
        # just computed — comparing a number to itself is always false, and the
        # bar count then never persisted at all.
        if state != row["state"] or bars != int(row["history_daily_bars"] or 0):
            await conn.execute(
                "UPDATE public.research_symbols SET state=$2, history_daily_bars=$3, "
                "history_first_session=$4, history_latest_session=$5, updated_at=NOW() "
                "WHERE symbol=$1",
                row["symbol"], state, bars, local.get("first_session"),
                local.get("latest_session"))
            updated += 1
    return {"updated": updated, "states": tally}


# --------------------------------------------------------------------------- #
# bounded warmup
# --------------------------------------------------------------------------- #

WARMUP_SELECT_SQL = """
SELECT symbol, discovery_reasons AS reasons, discovery_observation_count AS observation_count,
       history_daily_bars AS daily_bars, latest_reference_session, best_rank,
       warmup_attempts, warmup_cooldown_until
FROM public.research_symbols
WHERE state IN ('discovered', 'history_required', 'history_warming')
  AND warmup_attempts < $1
"""


async def select_warmup_batch(conn, *, limit: int = ru.MAX_WARMUP_SYMBOLS_PER_RUN,
                              now: Optional[datetime] = None,
                              ) -> List[Dict[str, Any]]:
    """The next symbols to warm, in the documented lexicographic order.

    Symbols in cooldown are skipped rather than counted against the batch: a
    parked symbol must not silently consume the budget of a healthy one.
    """
    moment = now or datetime.now(timezone.utc)
    rows = [dict(r) for r in await conn.fetch(
        WARMUP_SELECT_SQL, ru.MAX_WARMUP_ATTEMPTS)]
    eligible = [r for r in rows
                if not ru.is_in_cooldown(r["warmup_cooldown_until"], now=moment)]
    return ru.prioritise(eligible, limit=limit)


async def warm_symbol(conn, provider, symbol: str, *,
                      target_sessions: int = ru.RESEARCH_FETCH_TARGET_SESSIONS,
                      now: Optional[datetime] = None) -> Dict[str, Any]:
    """One symbol, one bounded provider window. Never a loop over the market.

    The calendar window is `target_sessions * 1.75` days, the same ratio the
    frozen universe's own warmup uses — trading days are roughly 70% of
    calendar days and the margin absorbs holidays without a second request,
    which at one symbol per 75 seconds is the cost that matters.

    Returns bounded telemetry and RAISES nothing: a provider failure is mapped
    to a safe code and recorded on the row, because one symbol must not end a
    batch.
    """
    moment = now or datetime.now(timezone.utc)
    before = (await bar_counts(conn, [symbol])).get(symbol, {}).get("bars", 0)
    result: Dict[str, Any] = {
        "symbol": symbol, "bars_before": before, "bars_after": before,
        "provider_requests": 0, "error_code": None, "error_class": None,
        "inserted": 0, "updated": 0, "unchanged": 0,
    }

    frm = moment.date() - timedelta(days=int(target_sessions * 1.75))
    to = moment.date()
    provider_name = getattr(provider, "name", None) or "unknown"

    attempt_number = int(await conn.fetchval(
        "UPDATE public.research_symbols SET state=$2, "
        "warmup_attempts=warmup_attempts+1, warmup_last_attempt_at=$3, "
        "updated_at=NOW() WHERE symbol=$1 RETURNING warmup_attempts",
        symbol, ru.STATE_HISTORY_WARMING, moment) or 1)

    try:
        raw = await provider.get_daily_bars(symbol, str(frm), str(to))
        result["provider_requests"] = 1
        stats = await upsert_daily_bars(
            conn, normalize_daily_bars(raw, now=moment), source=provider_name)
        result.update({k: stats.get(k, 0)
                       for k in ("inserted", "updated", "unchanged")})
    except Exception as exc:                       # noqa: BLE001 — mapped below
        code, klass = map_provider_error(exc)
        result["error_code"], result["error_class"] = code, klass
        # The count still increments: a failed call cost a request, and a
        # budget that only counts successes is not a budget.
        result["provider_requests"] = 1
        cooldown = ru.cooldown_until(moment)
        await conn.execute(
            "UPDATE public.research_symbols SET warmup_last_error_code=$2, "
            "warmup_last_error_class=$3, warmup_cooldown_until=$4, "
            "warmup_provider_requests=warmup_provider_requests+1, updated_at=NOW() "
            "WHERE symbol=$1", symbol, code, klass, cooldown)
        logger.warning("research warmup failed symbol=%s code=%s class=%s",
                       symbol, code, klass)
        return result

    after = (await bar_counts(conn, [symbol])).get(symbol, {}).get("bars", 0)
    result["bars_after"] = after
    # A symbol the provider serves but cannot fill is `unavailable`, not
    # `failed`: retrying will not conjure history that does not exist. Two
    # shapes of that, both terminal:
    #   * it returned almost nothing at all (a very recent listing);
    #   * it returned NOTHING NEW on a repeat call, which is the provider
    #     saying it has given us everything it holds. Without this second
    #     case a symbol whose listing is younger than the 24-month gate burns
    #     all three attempts and lands in `failed`, which reads as our fault.
    exhausted = attempt_number > 1 and after == before and after > 0
    terminal = after < ru.RESEARCH_MIN_USABLE_BARS or exhausted
    await conn.execute(
        "UPDATE public.research_symbols SET warmup_last_error_code=$2, "
        "warmup_last_error_class=$3, warmup_cooldown_until=NULL, "
        "warmup_provider_requests=warmup_provider_requests+1, updated_at=NOW() "
        "WHERE symbol=$1",
        symbol,
        ("provider_history_exhausted" if exhausted
         else "insufficient_provider_history") if terminal else None,
        "terminal" if terminal else None)
    return result


async def run_warmup(conn, provider, *,
                     limit: int = ru.MAX_WARMUP_SYMBOLS_PER_RUN,
                     max_requests: int = ru.MAX_PROVIDER_REQUESTS_PER_RUN,
                     spacing_seconds: Optional[int] = None,
                     now: Optional[datetime] = None) -> Dict[str, Any]:
    """Warm a bounded batch, sequentially, counting every provider call.

    Sequential and not concurrent on purpose: the existing warmup path takes a
    machine-wide advisory lock, and Massive Basic allows five requests a
    minute. A second worker would spend its life colliding with the first.
    """
    moment = now or datetime.now(timezone.utc)
    spacing = (spacing_seconds if spacing_seconds is not None
               else int(settings.HISTORY_WARMUP_PROVIDER_REQUEST_SPACING_SECONDS))
    summary: Dict[str, Any] = {
        "selected": [], "max_requests": max_requests, "provider_requests": 0,
        "warmed": [], "failed": [], "budget_exhausted": False, "locked": False,
    }
    if provider is None:
        summary["reason"] = "no_provider"
        return summary

    # THE SAME machine-wide lock the frozen universe's warmup takes, not a new
    # one. Research and the frozen universe share one provider and one rate
    # limit, so they must share one gate — two independent warmers would spend
    # their budget colliding. `try` and not a wait: a run that cannot get the
    # lock reports that and exits, rather than queueing behind work it cannot
    # see. The lock is session-scoped, so a crash releases it.
    acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1)",
                                   HISTORY_WARMUP_ADVISORY_LOCK_KEY)
    if not acquired:
        summary["locked"] = True
        summary["reason"] = "history_warmup_execution_in_progress"
        return summary

    try:
        batch = await select_warmup_batch(conn, limit=limit, now=moment)
        summary["selected"] = [r["symbol"] for r in batch]
        for index, row in enumerate(batch):
            if summary["provider_requests"] >= max_requests:
                # Stated, not silent. A run that stopped early because it ran
                # out of budget must say so, or the missing symbols read as
                # "nothing to do".
                summary["budget_exhausted"] = True
                break
            if index and spacing:
                await asyncio.sleep(spacing)
            outcome = await warm_symbol(conn, provider, row["symbol"], now=moment)
            summary["provider_requests"] += outcome["provider_requests"]
            (summary["failed"] if outcome["error_code"] else summary["warmed"]
             ).append(outcome)
        await refresh_states(conn, now=moment)
    finally:
        await conn.fetchval("SELECT pg_advisory_unlock($1)",
                            HISTORY_WARMUP_ADVISORY_LOCK_KEY)
    return summary


__all__ = [
    "SOURCE_STATE_RESEARCH", "SCANNER_UNIVERSE_CODE",
    "frozen_universe_symbols", "bar_counts", "admit_from_discovery",
    "refresh_states", "select_warmup_batch", "warm_symbol", "run_warmup",
]
