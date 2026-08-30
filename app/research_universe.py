"""The research domain: discovered symbols we may STUDY but never trade beside.

PURE: no DB, no network, no provider. Given already-fetched rows, every output
is a deterministic function of stored facts and the market calendar.

THE PROBLEM THIS EXISTS TO SOLVE
--------------------------------
Wave 2 proved the scanner's blind spot with a number: of 68 symbols the market
noticed in one session, exactly ONE was inside the frozen 25, and 67 had too
little local history to analyse at all. We could see what we were missing and
could do nothing with it. This layer makes those 67 studiable.

THE LINE IT MUST NOT CROSS
--------------------------
A research symbol is NOT a universe member. It never becomes one, and the
guarantee is structural rather than remembered:

  * it lives in its own tables, never in `history_warmup_universe_symbols`;
  * `research_scan_results` is not `strategy_shadow_evaluations` and no join
    exists between them;
  * nothing here creates an experiment pair, a canonical outcome row, an
    attention tier, or an ENTER eligibility;
  * the frozen universe's own table is never written by any code path here.

WHY IT IS NOT A `history_warmup_universes` ROW
----------------------------------------------
That model is deliberately IMMUTABLE — a database trigger refuses membership
changes the moment a universe leaves `draft`, and the warmup machinery pins a
hash at freeze. That is exactly right for a cohort whose interpretability
depends on never changing, and exactly wrong for a research set that must grow
as the market surfaces new symbols. Reusing it would mean either freezing a set
we need to extend, or leaving a universe permanently in `draft` and quietly
losing the immutability guarantee for everybody. So research gets its own
small table, and the frozen model keeps meaning what it says.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.prospective_readiness import (CANDIDATE_MIN_DAILY_BARS,
                                       CANDIDATE_MIN_MONTHLY_PERIODS,
                                       CONTROL_MIN_DAILY_BARS,
                                       IMPLIED_DAILY_SESSIONS_FOR_MONTHLY)

RESEARCH_CONTRACT_VERSION = "smart_scanner_research_symbol.v1"
RESEARCH_SCAN_CONTRACT_VERSION = "smart_scanner_research_scan.v1"

# --------------------------------------------------------------------------- #
# lifecycle
#
# Deterministic, and computed from stored facts every time rather than trusted
# from the column. The column records what we last decided; the function
# decides. A state machine that can only be advanced by the thing that advanced
# it last is a state machine that gets stuck.
# --------------------------------------------------------------------------- #
STATE_DISCOVERED = "discovered"
STATE_HISTORY_REQUIRED = "history_required"
STATE_HISTORY_WARMING = "history_warming"
STATE_RESEARCH_READY = "research_ready"
STATE_RESEARCH_SCANNED = "research_scanned"
#: The provider cannot give us this symbol — delisted, wrong exchange, an
#: instrument we do not model. Distinct from a failure, because retrying will
#: not change it.
STATE_UNAVAILABLE = "unavailable"
#: Warmup failed enough times to stop. Retryable in principle, parked in fact.
STATE_FAILED = "failed"

RESEARCH_STATES = (STATE_DISCOVERED, STATE_HISTORY_REQUIRED,
                   STATE_HISTORY_WARMING, STATE_RESEARCH_READY,
                   STATE_RESEARCH_SCANNED, STATE_UNAVAILABLE, STATE_FAILED)

TERMINAL_STATES = (STATE_UNAVAILABLE, STATE_FAILED)

# --------------------------------------------------------------------------- #
# how much history a research symbol needs
#
# VERIFIED FROM CODE, not assumed. The binding gate is not a daily bar count at
# all: `CANDIDATE_MIN_MONTHLY_PERIODS = 24` binds, and
# `IMPLIED_DAILY_SESSIONS_FOR_MONTHLY = 504` is the practical daily floor that
# satisfies it. 175 (candidate daily) and 200 (control daily) are both well
# below it and never bind on their own.
#
# The frozen 25 hold 521 completed sessions, which is consistent with 504 plus
# the sessions since they were warmed — that agreement is the check that this
# number is the real one.
# --------------------------------------------------------------------------- #
#: The FETCH target — how much history to ask the provider for. It is NOT the
#: readiness gate; see `classify_history_state`, which delegates that to the
#: canonical evaluator. Keeping them separate matters because the provider plan
#: caps history at two years (~500 sessions) and a bar-count gate of 504 would
#: therefore have declared every freshly discovered symbol permanently
#: not-ready, while the actual rule — 24 COMPLETED MONTHS — is satisfiable.
RESEARCH_FETCH_TARGET_SESSIONS = IMPLIED_DAILY_SESSIONS_FOR_MONTHLY   # 504

#: Kept as the name the rest of the code reads for the fetch window.
RESEARCH_MIN_DAILY_BARS = RESEARCH_FETCH_TARGET_SESSIONS

#: Fetch margin. The provider counts calendar coverage, we count completed
#: sessions, and holidays make those differ; asking for a little more costs one
#: request and avoids a second round trip that would cost 75 seconds.
RESEARCH_FETCH_MARGIN_BARS = 40

#: Below this, the symbol is not worth a second request — it is a recent
#: listing, an illiquid instrument, or something the provider does not carry.
#: Deliberately generous: the point is to stop retrying, not to filter.
RESEARCH_MIN_USABLE_BARS = 60


# --------------------------------------------------------------------------- #
# bounded operation
#
# Every number here is derived from the ONE hard external constraint, which is
# measured and configured, not guessed: Massive Basic allows
# MASSIVE_REQUESTS_PER_MINUTE = 5, and this repository's own warmup pacing is
# HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH = 1 every
# HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS = 75. One symbol per 75 seconds is
# therefore the real throughput, and a "limit" that ignores it is decoration.
# --------------------------------------------------------------------------- #

#: New symbols admitted per discovery run. Five, because five symbols is
#: already ~6 minutes of provider wall time at the pacing above, and because a
#: run that admits more than a human will look at in a day is just a queue that
#: grows. 67 outside-universe symbols in one session is exactly the explosion
#: this bounds.
MAX_NEW_RESEARCH_SYMBOLS_PER_RUN = 5

#: Symbols warmed per warmup run.
MAX_WARMUP_SYMBOLS_PER_RUN = 5

#: Hard ceiling on provider calls in one run, counted and enforced rather than
#: hoped for. One daily fetch per symbol plus headroom for a single retry each.
MAX_PROVIDER_REQUESTS_PER_RUN = 12

#: Concurrency is ONE, and not because of caution. The existing warmup path
#: takes a machine-wide advisory lock (HISTORY_WARMUP_ADVISORY_LOCK_KEY), so a
#: second concurrent warmer would spend its life colliding with the first and
#: reporting 409s. Parallelism here would be a slower way to do the same work.
MAX_CONCURRENT_WARMUPS = 1

#: Attempts before a symbol is parked in `failed`. Three matches
#: JOB_MAX_ATTEMPTS_DEFAULT so operators reason about one number.
MAX_WARMUP_ATTEMPTS = 3

#: How long a failed symbol waits before it may be retried. An hour is longer
#: than any transient provider condition and short enough that a fixed problem
#: recovers within a session.
WARMUP_COOLDOWN_MINUTES = 60


def cooldown_until(now: datetime,
                   minutes: int = WARMUP_COOLDOWN_MINUTES) -> datetime:
    return now + timedelta(minutes=minutes)


def is_in_cooldown(cooldown_at: Optional[datetime], *, now: datetime) -> bool:
    if cooldown_at is None:
        return False
    moment = (cooldown_at if cooldown_at.tzinfo
              else cooldown_at.replace(tzinfo=timezone.utc))
    return moment > now


# --------------------------------------------------------------------------- #
# history state
# --------------------------------------------------------------------------- #

def readiness_verdict(*, symbol: str, daily_bars: int,
                      week_groups: Optional[int], month_groups: Optional[int],
                      oldest: Any = None, latest: Any = None) -> Dict[str, Any]:
    """The CANONICAL readiness evaluation, unmodified.

    `app.prospective_readiness.evaluate_symbol` is the function the frozen
    universe's own readiness is judged by. A research symbol is judged by the
    same one, so "ready" means exactly what it means on the 25 — including the
    part that actually binds, which is 24 completed MONTHS rather than any
    daily bar count.
    """
    from app.prospective_readiness import evaluate_symbol
    return evaluate_symbol({
        "symbol": symbol, "daily_bars": daily_bars,
        "oldest": oldest, "latest": latest,
        "week_groups": week_groups, "month_groups": month_groups})


def is_research_ready(daily_bars: Optional[int], *,
                      week_groups: Optional[int] = None,
                      month_groups: Optional[int] = None,
                      symbol: str = "") -> bool:
    """Ready when the canonical evaluator says both arms have their history.

    Period counts are REQUIRED. Without them the answer is "not yet", not a
    guess from a bar count: the provider caps history at two years, so a
    symbol can sit on ~500 daily bars and still be either side of the 24-month
    gate depending on where its listing starts.
    """
    if week_groups is None or month_groups is None:
        return False
    verdict = readiness_verdict(
        symbol=symbol, daily_bars=int(daily_bars or 0),
        week_groups=week_groups, month_groups=month_groups)
    return bool(verdict.get("candidate_overall_ready")
                and (daily_bars or 0) >= CONTROL_MIN_DAILY_BARS)


def classify_history_state(*, daily_bars: Optional[int],
                           week_groups: Optional[int] = None,
                           month_groups: Optional[int] = None,
                           symbol: str = "",
                           attempts: int = 0,
                           last_error_class: Optional[str] = None,
                           ) -> str:
    """Where a symbol stands, computed from what we hold — never from a flag.

    `unavailable` is reserved for a provider verdict we should not retry
    (the symbol does not exist for it). Running out of attempts is `failed`,
    which is a different sentence: we could not get it, not there is nothing
    to get.
    """
    if last_error_class == "terminal":
        return STATE_UNAVAILABLE
    bars = daily_bars or 0
    if is_research_ready(bars, week_groups=week_groups,
                         month_groups=month_groups, symbol=symbol):
        return STATE_RESEARCH_READY
    if attempts >= MAX_WARMUP_ATTEMPTS:
        return STATE_FAILED
    if bars > 0:
        return STATE_HISTORY_WARMING
    return STATE_HISTORY_REQUIRED


def readiness_gap(*, daily_bars: Optional[int], week_groups: Optional[int],
                  month_groups: Optional[int], symbol: str = "") -> List[str]:
    """What is still missing, in the canonical evaluator's own words."""
    if week_groups is None or month_groups is None:
        return ["period_counts_unknown"]
    return list(readiness_verdict(
        symbol=symbol, daily_bars=int(daily_bars or 0),
        week_groups=week_groups,
        month_groups=month_groups).get("blocking_reasons") or [])


# --------------------------------------------------------------------------- #
# prioritisation — LEXICOGRAPHIC, never a score
#
# Which discovered symbol gets the next 75 seconds of provider time. The
# ordering is a list of independent questions asked in a fixed order, so a
# reader can always say WHY one symbol came before another by naming the first
# question where they differed. A weighted number could not be explained that
# way, and the number would immediately become the thing people sort on.
#
# Every dimension below is one we actually hold and are allowed to use. Note
# what is ABSENT: price, market cap and volume all exist in the FMP payload and
# none is used, because they are the restricted provider's values and this
# ordering is not a place to launder them.
# --------------------------------------------------------------------------- #

PRIORITY_DIMENSIONS = (
    ("reason_count", "appears in more discovery categories at once"),
    ("observation_count", "seen across more separate discovery observations"),
    ("has_partial_history", "already partly cached, so cheaper to finish"),
    ("latest_reference_session", "more recent market session"),
    ("best_rank", "higher position in the list that surfaced it"),
    ("symbol", "alphabetical, so the order is total and reproducible"),
)


def priority_key(row: Dict[str, Any]) -> Tuple:
    """The ordering, as a tuple. Lower sorts first.

    The final `symbol` term exists so the order is TOTAL: two symbols equal on
    every real dimension still have a stable, reproducible position, and a
    re-run picks the same five.
    """
    reasons = row.get("reasons") or row.get("discovery_reasons") or []
    bars = row.get("daily_bars") or row.get("history_daily_bars") or 0
    reference = row.get("latest_reference_session") or row.get("reference_session_date")
    return (
        -int(len(reasons)),
        -int(row.get("observation_count") or row.get("discovery_observation_count") or 0),
        # Partly-cached first: finishing a symbol we already started costs
        # fewer provider calls than starting a new one.
        0 if bars > 0 else 1,
        -(reference.toordinal() if isinstance(reference, date) else 0),
        int(row.get("best_rank") or 9999),
        str(row.get("symbol") or ""),
    )


def prioritise(rows: Sequence[Dict[str, Any]], *,
               limit: int = MAX_NEW_RESEARCH_SYMBOLS_PER_RUN,
               ) -> List[Dict[str, Any]]:
    """Deterministic ordering, then a hard cut. No score is produced."""
    return sorted(rows, key=priority_key)[:max(0, limit)]


def explain_priority(row: Dict[str, Any]) -> List[str]:
    """The dimensions that put this row where it is, in words.

    Returned so a report can say "chosen because it appeared in three lists"
    rather than "chosen because 0.82".
    """
    out: List[str] = []
    reasons = row.get("reasons") or row.get("discovery_reasons") or []
    if len(reasons) > 1:
        out.append(f"in {len(reasons)} discovery categories")
    observations = row.get("observation_count") or row.get(
        "discovery_observation_count") or 0
    if observations > 1:
        out.append(f"seen on {observations} discovery observations")
    if (row.get("daily_bars") or row.get("history_daily_bars") or 0) > 0:
        out.append("partly cached already")
    return out or ["first by the tie-break order"]


# --------------------------------------------------------------------------- #
# research candidates
#
# THE DISTINCTION THIS SECTION EXISTS TO ENFORCE
# ----------------------------------------------
# The first cohort reported three symbols as "worth a human look" while their
# canonical verdict was AVOID / price_below_minimum. That was wrong, and it was
# wrong in a specific way: discovery strength was being used to answer a
# question only strategy evidence can answer.
#
#   DISCOVERY REASONS  explain WHY WE LOOKED.        (it moved, on three lists)
#   SCAN EVIDENCE      decides WHETHER IT SURVIVED.  (the strategy's own read)
#
# They never mix. A symbol on five mover lists that the strategy hard-rejects is
# `scanned_not_candidate`, and the discovery reasons remain attached to explain
# why we spent anything on it at all.
#
# Being a candidate is ALSO not ENTER or WATCH. It means the research screen did
# not disqualify it and there is something to read.
# --------------------------------------------------------------------------- #
CANDIDATE_RESEARCH_CANDIDATE = "research_candidate"
CANDIDATE_SCANNED_NOT_CANDIDATE = "scanned_not_candidate"
CANDIDATE_INSUFFICIENT_DATA = "insufficient_data"
CANDIDATE_UNAVAILABLE = "unavailable"

CANDIDATE_STATES = (CANDIDATE_RESEARCH_CANDIDATE,
                    CANDIDATE_SCANNED_NOT_CANDIDATE,
                    CANDIDATE_INSUFFICIENT_DATA, CANDIDATE_UNAVAILABLE)

# ---- why we looked (discovery), kept separate on purpose ------------------- #
LOOKED_MULTIPLE_LISTS = "discovered_multiple_lists"
LOOKED_REPEATEDLY = "discovered_repeatedly"
LOOKED_RECENTLY = "discovery_recent"
LOOKED_REASONS = (LOOKED_MULTIPLE_LISTS, LOOKED_REPEATEDLY, LOOKED_RECENTLY)

# ---- what the research screen found --------------------------------------- #
SCREEN_STRUCTURE_PRESENT = "research_structure_present"
SCREEN_SETUP_PRESENT = "research_setup_present"
SCREEN_BENCHMARK_LEADING = "benchmark_outperforming"
SCREEN_HARD_DISQUALIFIED = "hard_disqualified"
SCREEN_NO_EVIDENCE = "no_research_evidence"
SCREEN_NOT_SCANNED = "not_scanned"
SCREEN_REASONS = (SCREEN_STRUCTURE_PRESENT, SCREEN_SETUP_PRESENT,
                  SCREEN_BENCHMARK_LEADING, SCREEN_HARD_DISQUALIFIED,
                  SCREEN_NO_EVIDENCE, SCREEN_NOT_SCANNED)

#: How recent a discovery still counts as current, in market sessions.
RECENT_DISCOVERY_MAX_SESSIONS = 3

#: Setup states the strategy itself treats as present (see
#: `prospective_campaign.candidate_signal_fields`, which reads
#: `setup_state not in (None, "absent", "none")`). Restated here as a tuple for
#: readability only — the membership rule is the strategy's, not ours.
_SETUP_ABSENT = (None, "", "absent", "none")


def looked_because(row: Dict[str, Any], *,
                   latest_reference_session: Optional[date] = None,
                   ) -> List[str]:
    """Why this symbol was looked at. DISCOVERY facts only.

    Never an argument that it is interesting — only that it is the reason we
    spent a provider request. Kept apart from the screen so the two can never
    be summed into one impression.
    """
    out: List[str] = []
    reasons = row.get("discovery_reasons") or row.get("reasons") or []
    if len(reasons) > 1:
        out.append(LOOKED_MULTIPLE_LISTS)
    if (row.get("discovery_observation_count") or 0) > 1:
        out.append(LOOKED_REPEATEDLY)
    seen = row.get("latest_reference_session")
    if (isinstance(seen, date) and isinstance(latest_reference_session, date)
            and (latest_reference_session - seen).days
            <= RECENT_DISCOVERY_MAX_SESSIONS):
        out.append(LOOKED_RECENTLY)
    return out


def screen_findings(row: Dict[str, Any]) -> List[str]:
    """What the RESEARCH SCAN found. Strategy evidence only.

    A `rejection_reason` is the strategy declining the symbol on one of its own
    hard gates, and it ends the matter — `hard_disqualified` is returned alone,
    because listing "structure present" beside "rejected on price" would invite
    somebody to weigh one against the other.
    """
    if row.get("rejection_reason"):
        return [SCREEN_HARD_DISQUALIFIED]
    out: List[str] = []
    structure = row.get("structure_state")
    if structure and structure not in ("none", "absent"):
        out.append(SCREEN_STRUCTURE_PRESENT)
    if row.get("setup_state") not in _SETUP_ABSENT:
        out.append(SCREEN_SETUP_PRESENT)
    if row.get("benchmark_relative") == "outperforming":
        out.append(SCREEN_BENCHMARK_LEADING)
    return out or [SCREEN_NO_EVIDENCE]


def classify_candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    """The candidate verdict, with both halves reported separately.

    Deterministic, no score, and no path by which discovery strength alone can
    produce `research_candidate`.
    """
    state = row.get("state")
    if state in TERMINAL_STATES:
        return {"candidate_state": CANDIDATE_UNAVAILABLE,
                "screen": [], "reason": state}
    if state != STATE_RESEARCH_SCANNED:
        return {"candidate_state": CANDIDATE_INSUFFICIENT_DATA,
                "screen": [SCREEN_NOT_SCANNED], "reason": SCREEN_NOT_SCANNED}

    findings = screen_findings(row)
    if SCREEN_HARD_DISQUALIFIED in findings:
        return {"candidate_state": CANDIDATE_SCANNED_NOT_CANDIDATE,
                "screen": findings,
                # The strategy's own words for WHY, kept verbatim.
                "reason": row.get("rejection_reason")}
    if findings == [SCREEN_NO_EVIDENCE]:
        return {"candidate_state": CANDIDATE_SCANNED_NOT_CANDIDATE,
                "screen": findings, "reason": SCREEN_NO_EVIDENCE}
    return {"candidate_state": CANDIDATE_RESEARCH_CANDIDATE,
            "screen": findings, "reason": None}


def is_research_candidate(row: Dict[str, Any], *,
                          latest_reference_session: Optional[date] = None,
                          ) -> bool:
    """Survived the research screen. NOT a recommendation, and NOT ENTER.

    `latest_reference_session` is accepted for call-compatibility and is
    deliberately unused: recency explains why we looked, and can never by
    itself make something a candidate.
    """
    return (classify_candidate(row)["candidate_state"]
            == CANDIDATE_RESEARCH_CANDIDATE)


# --------------------------------------------------------------------------- #
# reference/sector handling for symbols the registry has never heard of
# --------------------------------------------------------------------------- #
SECTOR_KNOWN = "sector_known"
SECTOR_UNKNOWN = "sector_unknown"
REFERENCE_UNAVAILABLE = "reference_unavailable"
SECTOR_STATES = (SECTOR_KNOWN, SECTOR_UNKNOWN, REFERENCE_UNAVAILABLE)


def classify_sector_state(symbol: str, *, benchmark_available: bool) -> str:
    """A discovered symbol usually has no sector mapping, and we never guess.

    The registry is a hand-made GICS-sector-to-SPDR-ETF mapping for the frozen
    25. A symbol it has never seen gets `sector_unknown` — NOT a nearest match,
    NOT the market average. Benchmark-relative context can still be computed
    against SPY, because that comparison needs no mapping at all; only the
    sector-relative half is unavailable.
    """
    from app.reference_market import sector_benchmark_for
    if not benchmark_available:
        return REFERENCE_UNAVAILABLE
    return SECTOR_KNOWN if sector_benchmark_for(symbol) else SECTOR_UNKNOWN


__all__ = [
    "RESEARCH_CONTRACT_VERSION", "RESEARCH_SCAN_CONTRACT_VERSION",
    "STATE_DISCOVERED", "STATE_HISTORY_REQUIRED", "STATE_HISTORY_WARMING",
    "STATE_RESEARCH_READY", "STATE_RESEARCH_SCANNED", "STATE_UNAVAILABLE",
    "STATE_FAILED", "RESEARCH_STATES", "TERMINAL_STATES",
    "RESEARCH_MIN_DAILY_BARS", "RESEARCH_FETCH_TARGET_SESSIONS",
    "RESEARCH_FETCH_MARGIN_BARS", "readiness_verdict", "readiness_gap",
    "RESEARCH_MIN_USABLE_BARS",
    "MAX_NEW_RESEARCH_SYMBOLS_PER_RUN", "MAX_WARMUP_SYMBOLS_PER_RUN",
    "MAX_PROVIDER_REQUESTS_PER_RUN", "MAX_CONCURRENT_WARMUPS",
    "MAX_WARMUP_ATTEMPTS", "WARMUP_COOLDOWN_MINUTES",
    "cooldown_until", "is_in_cooldown",
    "classify_history_state", "is_research_ready",
    "PRIORITY_DIMENSIONS", "priority_key", "prioritise", "explain_priority",
    "CANDIDATE_RESEARCH_CANDIDATE", "CANDIDATE_SCANNED_NOT_CANDIDATE",
    "CANDIDATE_INSUFFICIENT_DATA", "CANDIDATE_UNAVAILABLE", "CANDIDATE_STATES",
    "LOOKED_MULTIPLE_LISTS", "LOOKED_REPEATEDLY", "LOOKED_RECENTLY",
    "LOOKED_REASONS", "SCREEN_STRUCTURE_PRESENT", "SCREEN_SETUP_PRESENT",
    "SCREEN_BENCHMARK_LEADING", "SCREEN_HARD_DISQUALIFIED",
    "SCREEN_NO_EVIDENCE", "SCREEN_NOT_SCANNED", "SCREEN_REASONS",
    "RECENT_DISCOVERY_MAX_SESSIONS", "looked_because", "screen_findings",
    "classify_candidate", "is_research_candidate",
    "SECTOR_KNOWN", "SECTOR_UNKNOWN", "REFERENCE_UNAVAILABLE",
    "SECTOR_STATES", "classify_sector_state",
]
