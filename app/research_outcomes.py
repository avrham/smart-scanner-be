"""What happened next: session-based forward outcomes for research scans.

THE PROBLEM THIS EXISTS TO CLOSE
--------------------------------
The research lifecycle produces predictions and stores none of their results.
Every session that passes without a measurement is an observation that can
never be recovered, because the labelling opportunity is the horizon itself —
the bars arrive whether or not anybody wrote down what to compare them to.
`research_lifecycle_runs` measures the FUNNEL (how many symbols reached which
stage). Nothing measured the MARKET.

WHAT AN OUTCOME IS HERE, PRECISELY
----------------------------------
One row per (scan, horizon):

    return  = 100 * (close[horizon_session] / close[scan_session] - 1)

for the symbol, the same arithmetic for the benchmark over the SAME TWO
SESSIONS, and their difference. That is all. It is a MARKET-PATH OBSERVATION,
not a trade: no side (research cannot produce ENTER at all — migration 026's
CHECK refuses it), no stop, no target, no simulated R, no position.

The reference is the close of the scan's own session because that is the last
bar the scan could see. `research_scan._local_bars` bounds its frame with
`trading_date <= scan_session`, and `run_research_scans` will not scan a symbol
unless `history_latest_session = scan_session`, so this price is exactly "the
market when we formed the view" — and it is derived rather than stored, because
nothing about a research scan is a decision to buy at it.

EVERY SCAN IS MEASURED, NOT EVERY CANDIDATE  (the brief's O1)
-------------------------------------------------------------
`scanned_not_candidate` scans get outcomes too. Without them there is no
denominator: "research candidates returned +2% at 5D" says nothing at all until
it sits beside what the symbols the screen REJECTED did over the same sessions.
The screen is the thing under test, and a test with only positives is a
description.

THE FIVE THINGS THIS MODULE REFUSES TO DO
-----------------------------------------
  1. It never calls a provider. Bars come from the local `daily_bars` store or
     the outcome is not measured. A measurement that could fetch its own data
     would be a measurement that could fetch the WRONG data, later, silently.
  2. It never counts stored bars to find a horizon. The Nth trading session is
     resolved from the market calendar and persisted; the bar for exactly that
     date must exist. See `prospective_session.nth_trading_session_after`.
  3. It never measures the symbol against a benchmark on a different date.
     Both endpoints, both instruments, both exact sessions, or nothing.
  4. It never writes a number before the horizon session has COMPLETED.
  5. It never rewrites a measured outcome. A later pass that disagrees records
     the disagreement; the frozen numbers do not move, and the database
     enforces that with a trigger rather than trusting this file.

RELATIONSHIP TO THE CANONICAL EXPERIMENT
----------------------------------------
It reuses the pure math (`app.workers.outcomes.calculator`, `outcome.v1`) and
the market calendar, and shares nothing else. It does not read, write, join or
reference `strategy_shadow_pair_outcomes`, `strategy_shadow_pairs`,
`prospective_campaign_registrations` or any other frozen-25 relation — the
research role holds no privilege on any of them, by omission.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import app.research_funnel as rf
from app.prospective_session import (MARKET_CALENDAR_VERSION,
                                     nth_trading_session_after,
                                     resolve_latest_completed_session,
                                     trading_sessions_between)
from app.reference_market import PRIMARY_BENCHMARK
from app.workers.outcomes.calculator import (CALCULATION_VERSION, LONG,
                                             compute_mfe_mae,
                                             signed_return_pct, window_label)

logger = logging.getLogger(__name__)

RESEARCH_OUTCOME_CONTRACT_VERSION = "research_scan_outcome.v1"

#: The horizons, in COMPLETED TRADING SESSIONS. Identical to the canonical
#: experiment's `HOLDING_WINDOWS` and restated here rather than imported as a
#: mutable list, because this tuple is half a primary key and a database CHECK
#: enumerates the same five values.
HORIZONS: Tuple[int, ...] = (1, 3, 5, 10, 20)

#: MFE/MAE provenance. Daily bars give real per-session high and low but say
#: nothing about intrabar ORDER, so these are excursion EXTREMES over the
#: window and never a claim that a level was reached before another one.
EXCURSION_BASIS_DAILY = "daily_high_low.v1"
EXCURSION_BASIS_INCOMPLETE = "incomplete_window"

# ---- statuses (the brief's O13) -------------------------------------------- #
STATUS_NOT_YET_ELIGIBLE = "not_yet_eligible"
STATUS_WAITING_FOR_DATA = "waiting_for_data"
STATUS_MEASURED = "measured"
STATUS_FAILED_TERMINAL = "failed_terminal"
OUTCOME_STATUSES: Tuple[str, ...] = (STATUS_NOT_YET_ELIGIBLE,
                                     STATUS_WAITING_FOR_DATA,
                                     STATUS_MEASURED,
                                     STATUS_FAILED_TERMINAL)

# ---- bounded, secret-free status reason codes ------------------------------ #
REASON_HORIZON_NOT_COMPLETE = "horizon_session_not_completed"
REASON_MISSING_SYMBOL_ENTRY = "missing_symbol_entry_bar"
REASON_MISSING_SYMBOL_EXIT = "missing_symbol_exit_bar"
REASON_MISSING_BENCHMARK_ENTRY = "missing_benchmark_entry_bar"
REASON_MISSING_BENCHMARK_EXIT = "missing_benchmark_exit_bar"
REASON_NON_POSITIVE_PRICE = "non_positive_reference_price"
REASON_GRACE_EXCEEDED = "bars_unavailable_within_grace"
REASON_MEASURED = "measured"

#: How long a horizon may sit waiting for a bar before the automatic path gives
#: up on it. Sixty TRADING SESSIONS — roughly three months.
#:
#: WHY SO LONG
#: -----------
#: A research symbol's forward bars do not arrive on a schedule. They arrive
#: when `research_ingest.select_warmup_batch`'s freshness top-up reaches that
#: symbol, and that queue serves five symbols a run against a pool that is
#: currently 80 and growing. A symbol therefore waits on the order of
#: ceil(N/5) runs — about sixteen sessions today — for each refresh, and the
#: whole pool cycles more slowly still while cold bootstrap shares the same
#: budget. A grace of one or two weeks would abandon outcomes the system was
#: about to be able to measure, which is the one failure mode worse than
#: waiting: throwing away a real observation and calling it terminal.
#:
#: Sixty is deliberately several multiples of the measured cycle, so reaching
#: it means the data is genuinely not coming — the symbol left the pool, was
#: delisted, or its history was never completed — rather than that the queue
#: was busy. A terminal row is still visible, still carries its reason code,
#: and can still be revisited by an explicit operator pass (`include_terminal`).
MISSING_DATA_GRACE_SESSIONS = 60

#: Bounded work per run. Both are ceilings the schedule may lower and never
#: raise (see `app.jobs.research_outcomes.task_payload_from_template`).
DEFAULT_SCAN_LIMIT = 200
DEFAULT_OBSERVATION_LIMIT = 400

#: A measured row never grows more than this many divergence notes. Detection
#: must not become an unbounded write amplifier on a table that is evidence.
MAX_REVISION_NOTES = 20


# --------------------------------------------------------------------------- #
# planning: which observations SHOULD exist
# --------------------------------------------------------------------------- #

def horizon_session_for(scan_session: date, horizon: int) -> date:
    """The session that closes `horizon` completed trading sessions after the
    scan. Pure calendar; see `nth_trading_session_after` for why not bars."""
    return nth_trading_session_after(scan_session, horizon)


def plan_observations(scan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The five observations one scan owes, as plain dicts.

    Planned EAGERLY — including horizons that are years from maturing — and
    that is the point. `not_yet_eligible` has to be a row somebody can select,
    or the brief's O13 ("for every scan/horizon we should be able to determine
    the status without logs") is answered by absence, and absence is
    ambiguous: it cannot distinguish "the horizon has not arrived" from "the
    engine has never looked at this scan". Five rows per scan is a price worth
    paying to remove that ambiguity permanently.
    """
    scan_session = scan["scan_session"]
    classification = rf.scan_classification(scan)
    rows: List[Dict[str, Any]] = []
    for horizon in HORIZONS:
        rows.append({
            "scan_id": scan["id"],
            "symbol": scan["symbol"],
            "scan_session": scan_session,
            "horizon_sessions": horizon,
            "horizon_label": window_label(horizon),
            "horizon_session": horizon_session_for(scan_session, horizon),
            "contract_version": RESEARCH_OUTCOME_CONTRACT_VERSION,
            "calculation_version": CALCULATION_VERSION,
            "market_calendar_version": MARKET_CALENDAR_VERSION,
            # ---- the attribution SNAPSHOT (O11). Copied, never joined:
            # `research_scan.UPSERT_SCAN_SQL` rewrites these columns in place
            # on a re-scan of the same (symbol, scan_session), so a join would
            # let a later re-scan change what an earlier outcome was about.
            "strategy_code": scan["strategy_code"],
            "strategy_version": scan["strategy_version"],
            "config_hash": scan["config_hash"],
            "scan_classification": classification,
            "scan_verdict": scan.get("verdict"),
            "scan_structure_state": scan.get("structure_state"),
            "scan_setup_state": scan.get("setup_state"),
            "scan_reason_code": scan.get("reason_code"),
            "scan_rejection_reason": scan.get("rejection_reason"),
            "scan_benchmark_relative": scan.get("benchmark_relative"),
            "scan_scanned_at": scan["scanned_at"],
            "benchmark_symbol": PRIMARY_BENCHMARK,
        })
    return rows


# --------------------------------------------------------------------------- #
# measurement: pure, so it can be proven without a database
# --------------------------------------------------------------------------- #

class Bar:
    """One daily bar, as the engine needs it. Deliberately not a dict: a
    missing key on a dict is a KeyError three frames away from the cause."""

    __slots__ = ("trading_date", "open", "high", "low", "close")

    def __init__(self, trading_date: date, open_: float, high: float,
                 low: float, close: float) -> None:
        self.trading_date = trading_date
        self.open = float(open_)
        self.high = float(high)
        self.low = float(low)
        self.close = float(close)


def bars_hash(entry: Bar, exit_bar: Bar, bench_entry: Bar, bench_exit: Bar,
              window: Sequence[Bar]) -> str:
    """SHA-256 over the EXACT bars a measurement consumed.

    Lets a later pass detect a corrected close without this table keeping a
    second copy of the series. Deterministic: fixed field order, fixed
    formatting, sorted window. Prices are rendered at 6 decimal places, which
    is finer than any equity tick and coarse enough that float round-tripping
    cannot flip the hash on its own.
    """
    def fmt(bar: Bar, kind: str) -> str:
        return (f"{kind}|{bar.trading_date.isoformat()}|{bar.open:.6f}|"
                f"{bar.high:.6f}|{bar.low:.6f}|{bar.close:.6f}")

    parts = [
        "research_scan_outcome_bars.v1",
        fmt(entry, "entry"), fmt(exit_bar, "exit"),
        fmt(bench_entry, "bench_entry"), fmt(bench_exit, "bench_exit"),
    ]
    parts.extend(fmt(b, "win")
                 for b in sorted(window, key=lambda b: b.trading_date))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def measure(*, entry: Optional[Bar], exit_bar: Optional[Bar],
            bench_entry: Optional[Bar], bench_exit: Optional[Bar],
            window: Sequence[Bar], horizon_sessions: int) -> Dict[str, Any]:
    """One outcome, from four endpoint bars and the window between them.

    PURE. No connection, no clock, no configuration. Returns either a
    `measured` result carrying every number, or a `waiting_for_data` result
    carrying a bounded reason code and NO numbers at all — never a partial
    one, because the schema refuses a non-measured row that carries a value
    and this function must not be the thing that discovers that.

    THE BENCHMARK USES THE SAME TWO SESSIONS  (the brief's O5)
    ----------------------------------------------------------
    Not "the benchmark's nearest bar", not "the benchmark at or before the
    horizon". The same two dates or nothing. On 2026-08-31 the research scan
    reported excess returns in which the symbol half was three days older than
    the benchmark half, and IBIT's excess moved by 1.75 points between two
    scans in which IBIT gained no bars at all. Requiring the exact dates makes
    that arithmetic impossible rather than checked for.
    """
    missing = []
    if entry is None:
        missing.append(REASON_MISSING_SYMBOL_ENTRY)
    if exit_bar is None:
        missing.append(REASON_MISSING_SYMBOL_EXIT)
    if bench_entry is None:
        missing.append(REASON_MISSING_BENCHMARK_ENTRY)
    if bench_exit is None:
        missing.append(REASON_MISSING_BENCHMARK_EXIT)
    if missing:
        # The FIRST missing thing, in a fixed order, so the reason code is
        # deterministic rather than dependent on dict iteration.
        return {"status": STATUS_WAITING_FOR_DATA, "status_reason": missing[0],
                "missing": missing}

    if entry.close <= 0 or bench_entry.close <= 0:
        # A zero or negative close is a data fault, and a return computed from
        # one is arithmetic on a fault. Not measured, and said so.
        return {"status": STATUS_WAITING_FOR_DATA,
                "status_reason": REASON_NON_POSITIVE_PRICE,
                "missing": [REASON_NON_POSITIVE_PRICE]}

    # LONG for both, and the word is doing no work: research has no side, so
    # this is the plain percentage change of a price series, and the benchmark
    # is subtracted from it rather than traded against it.
    symbol_return = signed_return_pct(entry.close, exit_bar.close, LONG)
    benchmark_return = signed_return_pct(bench_entry.close, bench_exit.close,
                                         LONG)

    # EXCURSIONS ARE OPTIONAL AND THE RETURN IS NOT.
    # The return needs two closes; the excursion needs every session in the
    # window. Blocking a measurable return on a missing INTERIOR bar would
    # discard a fact we hold because of one we do not — so a short window
    # yields NULL excursions with the basis saying why, and the return is
    # written anyway.
    ordered = sorted(window, key=lambda b: b.trading_date)
    expected = int(horizon_sessions)
    present = len(ordered)
    if present >= expected:
        mfe, mae = compute_mfe_mae(entry.close,
                                   [b.high for b in ordered],
                                   [b.low for b in ordered],
                                   LONG, window=expected)
        basis = EXCURSION_BASIS_DAILY
    else:
        mfe, mae, basis = None, None, EXCURSION_BASIS_INCOMPLETE

    return {
        "status": STATUS_MEASURED,
        "status_reason": REASON_MEASURED,
        "entry_close": entry.close,
        "exit_close": exit_bar.close,
        "benchmark_entry_close": bench_entry.close,
        "benchmark_exit_close": bench_exit.close,
        "symbol_return_pct": symbol_return,
        "benchmark_return_pct": benchmark_return,
        "excess_return_pct": symbol_return - benchmark_return,
        "mfe_pct": mfe,
        "mae_pct": mae,
        "excursion_basis": basis,
        "window_sessions_expected": expected,
        "window_sessions_present": present,
        "bars_hash": bars_hash(entry, exit_bar, bench_entry, bench_exit,
                               ordered),
    }


# --------------------------------------------------------------------------- #
# persistence
# --------------------------------------------------------------------------- #

_PLAN_COLUMNS = (
    "scan_id", "symbol", "scan_session", "horizon_sessions", "horizon_label",
    "horizon_session", "contract_version", "calculation_version",
    "market_calendar_version", "strategy_code", "strategy_version",
    "config_hash", "scan_classification", "scan_verdict",
    "scan_structure_state", "scan_setup_state", "scan_reason_code",
    "scan_rejection_reason", "scan_benchmark_relative", "scan_scanned_at",
    "benchmark_symbol",
)

#: Planning is idempotent by the identity that matters. A second planning pass
#: over the same scan hits the conflict and does NOTHING — deliberately not a
#: DO UPDATE, because everything in the plan is either immutable (the horizon)
#: or a snapshot that must not be refreshed (the attribution). Re-planning a
#: re-scanned symbol must not silently re-point an existing observation at the
#: new classification; that is exactly what the snapshot exists to prevent.
INSERT_PLAN_SQL = f"""
INSERT INTO public.research_scan_outcomes ({', '.join(_PLAN_COLUMNS)}, status)
VALUES ({', '.join(f'${i}' for i in range(1, len(_PLAN_COLUMNS) + 1))},
        '{STATUS_NOT_YET_ELIGIBLE}')
ON CONFLICT (scan_id, horizon_sessions) DO NOTHING
RETURNING id
"""

#: The measurement write. The `WHERE` on the conflict target is a second lock
#: on top of the database trigger: even a caller holding a stale row cannot
#: overwrite an outcome that has already been measured. Belt and braces, and
#: the braces are the trigger.
UPDATE_MEASURED_SQL = """
UPDATE public.research_scan_outcomes SET
    status = $2, status_reason = $3,
    entry_close = $4, exit_close = $5,
    benchmark_entry_close = $6, benchmark_exit_close = $7,
    symbol_return_pct = $8, benchmark_return_pct = $9, excess_return_pct = $10,
    mfe_pct = $11, mae_pct = $12, excursion_basis = $13,
    window_sessions_expected = $14, window_sessions_present = $15,
    bars_hash = $16,
    measured_at = $17,
    attempt_count = attempt_count + 1, last_attempt_at = $17
WHERE id = $1 AND status <> 'measured'
RETURNING id
"""

UPDATE_PENDING_SQL = """
UPDATE public.research_scan_outcomes SET
    status = $2, status_reason = $3,
    attempt_count = attempt_count + 1, last_attempt_at = $4
WHERE id = $1 AND status <> 'measured'
RETURNING id
"""

#: The divergence write. It touches ONLY the columns the freeze trigger leaves
#: writable on a measured row, and it appends rather than replaces — bounded to
#: MAX_REVISION_NOTES so evidence cannot become a log.
RECORD_REVISION_SQL = """
UPDATE public.research_scan_outcomes SET
    revision_detected = TRUE,
    revision_notes = (
        CASE WHEN jsonb_array_length(revision_notes) >= $3 THEN revision_notes
             ELSE revision_notes || $2::jsonb END),
    attempt_count = attempt_count + 1, last_attempt_at = $4
WHERE id = $1
RETURNING id
"""

#: Scans that do not yet own their five observations. Bounded, and ordered
#: oldest-first so a backlog drains in the order it accumulated rather than
#: newest-first, which would starve the oldest scans — the same fairness
#: argument `research_ingest.select_warmup_batch` makes for warmup.
#:
#: The existence probe is the LAST horizon this module plans (20D), not the
#: first. Planning inserts the five rows in ascending order without a
#: transaction, so a process that died between them would leave a scan holding
#: a 1D row and nothing else — and a probe on 1D would then consider that scan
#: planned forever. Probing the last one written means a partial plan is
#: selected again, and the inserts that already succeeded are absorbed by the
#: ON CONFLICT DO NOTHING.
SELECT_UNPLANNED_SCANS_SQL = """
SELECT s.id, s.symbol, s.scan_session, s.scanned_at, s.strategy_code,
       s.strategy_version, s.config_hash, s.verdict, s.structure_state,
       s.setup_state, s.reason_code, s.rejection_reason, s.benchmark_relative
FROM public.research_scan_results s
WHERE NOT EXISTS (
    SELECT 1 FROM public.research_scan_outcomes o
    WHERE o.scan_id = s.id AND o.horizon_sessions = $2)
ORDER BY s.scan_session ASC, s.symbol ASC
LIMIT $1
"""

#: The worklist. `horizon_session <= as_of` is the whole eligibility test and
#: it is applied in SQL, so a row whose horizon has not completed is never even
#: fetched — the no-lookahead rule is a WHERE clause, not a branch somebody has
#: to remember to write.
SELECT_DUE_SQL = """
SELECT * FROM public.research_scan_outcomes
WHERE horizon_session <= $1
  AND status IN ('not_yet_eligible', 'waiting_for_data')
ORDER BY horizon_session ASC, scan_session ASC, symbol ASC,
         horizon_sessions ASC
LIMIT $2
"""

#: The same worklist plus already-measured and terminal rows, for an explicit
#: operator recheck. Measured rows are re-measured only to DETECT divergence —
#: the trigger makes acting on it impossible.
SELECT_DUE_INCLUDING_SETTLED_SQL = """
SELECT * FROM public.research_scan_outcomes
WHERE horizon_session <= $1
ORDER BY horizon_session ASC, scan_session ASC, symbol ASC,
         horizon_sessions ASC
LIMIT $2
"""


async def plan_missing_observations(conn, *, limit: int = DEFAULT_SCAN_LIMIT,
                                    ) -> Dict[str, Any]:
    """Give every scan the five observation rows it owes. Idempotent.

    ONE TRANSACTION PER SCAN, AND THE REASON IS PROVENANCE
    ------------------------------------------------------
    The attribution snapshot is read ONCE per scan, in `plan_observations`, and
    all five horizons copy from that one in-memory dict — so within a single
    pass they cannot disagree. The hazard is a pass that does not finish.

    Without a transaction the five INSERTs are five statements. A worker that
    died after two of them would leave a scan holding 1D and 3D from revision A;
    the next pass re-reads `research_scan_results`, and if the symbol has since
    been re-scanned for the same session — which `research_scan.UPSERT_SCAN_SQL`
    does in place — it would write 5D, 10D and 20D from revision B. One scan,
    five horizons, two different classifications, and nothing in the row to say
    so. That is exactly the failure the snapshot exists to prevent, arriving
    through the back door.

    A transaction makes the five rows all-or-nothing, so a scan is either
    entirely unplanned (and re-selected, since the existence probe is the LAST
    horizon written) or entirely planned from one revision.

    The transaction is per SCAN, not per pass: a single long transaction over
    two hundred scans would hold row locks for the whole pass and turn a
    bounded read-mostly job into a blocker for the lifecycle that shares this
    connection's role.
    """
    scans = [dict(r) for r in await conn.fetch(
        SELECT_UNPLANNED_SCANS_SQL, max(0, int(limit)), HORIZONS[-1])]
    inserted = 0
    for scan in scans:
        plans = plan_observations(scan)
        async with conn.transaction():
            for plan in plans:
                row = await conn.fetchrow(
                    INSERT_PLAN_SQL, *[plan[c] for c in _PLAN_COLUMNS])
                if row is not None:
                    inserted += 1
    return {"scans_considered": len(scans), "observations_planned": inserted,
            "truncated_by_limit": len(scans) >= max(0, int(limit))}


async def _bar(conn, symbol: str, session: date) -> Optional[Bar]:
    """The bar for EXACTLY this symbol and EXACTLY this session, or None.

    Not "at or before". A tolerant lookup here is how a horizon silently
    becomes a different horizon.
    """
    row = await conn.fetchrow(
        "SELECT trading_date, open, high, low, close FROM public.daily_bars "
        "WHERE symbol = $1 AND trading_date = $2", symbol, session)
    if row is None:
        return None
    return Bar(row["trading_date"], float(row["open"]), float(row["high"]),
               float(row["low"]), float(row["close"]))


async def _window_bars(conn, symbol: str, start: date, end: date) -> List[Bar]:
    """Every stored bar strictly after `start` and up to `end`, oldest first."""
    rows = await conn.fetch(
        "SELECT trading_date, open, high, low, close FROM public.daily_bars "
        "WHERE symbol = $1 AND trading_date > $2 AND trading_date <= $3 "
        "ORDER BY trading_date ASC", symbol, start, end)
    return [Bar(r["trading_date"], float(r["open"]), float(r["high"]),
                float(r["low"]), float(r["close"])) for r in rows]


async def measure_observation(conn, observation: Dict[str, Any], *,
                              as_of_session: date,
                              now: Optional[datetime] = None,
                              ) -> Dict[str, Any]:
    """Measure ONE observation and persist the result. Never raises for data.

    Returns a small dict describing what happened, in the same vocabulary the
    row now carries.
    """
    moment = now or datetime.now(timezone.utc)
    oid = observation["id"]
    symbol = observation["symbol"]
    scan_session = observation["scan_session"]
    horizon_session = observation["horizon_session"]
    horizon = int(observation["horizon_sessions"])
    benchmark = observation["benchmark_symbol"]
    already_measured = observation["status"] == STATUS_MEASURED

    if horizon_session > as_of_session:
        # Reachable only through the include-settled selection, whose bound is
        # the same `as_of` — kept as an explicit guard because "no future bar
        # may influence an earlier measurement" must not depend on a caller.
        return {"id": oid, "status": STATUS_NOT_YET_ELIGIBLE,
                "status_reason": REASON_HORIZON_NOT_COMPLETE}

    entry = await _bar(conn, symbol, scan_session)
    exit_bar = await _bar(conn, symbol, horizon_session)
    bench_entry = await _bar(conn, benchmark, scan_session)
    bench_exit = await _bar(conn, benchmark, horizon_session)
    window = await _window_bars(conn, symbol, scan_session, horizon_session)

    result = measure(entry=entry, exit_bar=exit_bar, bench_entry=bench_entry,
                     bench_exit=bench_exit, window=window,
                     horizon_sessions=horizon)

    if already_measured:
        # A recheck of frozen evidence. The ONLY thing that may happen is a
        # note. See the module header, rule 5.
        return await _record_divergence_if_any(conn, observation, result,
                                               now=moment)

    if result["status"] == STATUS_MEASURED:
        row = await conn.fetchrow(
            UPDATE_MEASURED_SQL, oid, STATUS_MEASURED, REASON_MEASURED,
            result["entry_close"], result["exit_close"],
            result["benchmark_entry_close"], result["benchmark_exit_close"],
            result["symbol_return_pct"], result["benchmark_return_pct"],
            result["excess_return_pct"], result["mfe_pct"], result["mae_pct"],
            result["excursion_basis"], result["window_sessions_expected"],
            result["window_sessions_present"], result["bars_hash"], moment)
        if row is None:
            # Somebody measured it between our SELECT and our UPDATE. That is
            # the concurrent-run case and it is not an error: the row is
            # measured, which is the outcome we wanted.
            return {"id": oid, "status": STATUS_MEASURED,
                    "status_reason": REASON_MEASURED, "already": True}
        return {"id": oid, **{k: result[k] for k in (
            "status", "status_reason", "symbol_return_pct",
            "benchmark_return_pct", "excess_return_pct", "mfe_pct", "mae_pct",
            "excursion_basis", "bars_hash")}}

    # Still missing data. Abandon only after a grace measured in SESSIONS.
    waited = trading_sessions_between(horizon_session, as_of_session)
    if waited >= MISSING_DATA_GRACE_SESSIONS:
        status, reason = STATUS_FAILED_TERMINAL, REASON_GRACE_EXCEEDED
    else:
        status, reason = STATUS_WAITING_FOR_DATA, result["status_reason"]
    await conn.execute(UPDATE_PENDING_SQL, oid, status, reason, moment)
    return {"id": oid, "status": status, "status_reason": reason,
            "sessions_waited": waited}


async def _record_divergence_if_any(conn, observation: Dict[str, Any],
                                    result: Dict[str, Any], *,
                                    now: datetime) -> Dict[str, Any]:
    """Compare a recheck against frozen evidence; record, never repair.

    The comparison is on `bars_hash`, not on the returns: a corrected close
    that happens to leave the percentage unchanged after rounding is still a
    different bar, and a policy that only noticed when the number moved would
    be a policy that notices selectively.
    """
    oid = observation["id"]
    if result["status"] != STATUS_MEASURED:
        # The store no longer holds bars it once did. Worth recording, never
        # worth acting on: the frozen numbers were computed from bars that
        # existed when they were computed.
        note = {"kind": "bars_no_longer_available",
                "reason": result.get("status_reason"),
                "observed_at": now.isoformat()}
    elif result["bars_hash"] == observation.get("bars_hash"):
        return {"id": oid, "status": STATUS_MEASURED, "unchanged": True}
    else:
        note = {"kind": "bars_revised",
                "frozen_bars_hash": observation.get("bars_hash"),
                "recomputed_bars_hash": result["bars_hash"],
                "frozen_symbol_return_pct":
                    _as_float(observation.get("symbol_return_pct")),
                "recomputed_symbol_return_pct": result["symbol_return_pct"],
                "frozen_excess_return_pct":
                    _as_float(observation.get("excess_return_pct")),
                "recomputed_excess_return_pct": result["excess_return_pct"],
                "observed_at": now.isoformat()}
    await conn.execute(RECORD_REVISION_SQL, oid, json.dumps([note]),
                       MAX_REVISION_NOTES, now)
    return {"id": oid, "status": STATUS_MEASURED, "revision_detected": True,
            "revision_kind": note["kind"]}


def _as_float(value: Any) -> Optional[float]:
    return None if value is None else float(value)


# --------------------------------------------------------------------------- #
# the run
# --------------------------------------------------------------------------- #

MODE_SCHEDULED = "scheduled"
MODE_BACKFILL = "backfill"
MODE_DRY_RUN = "dry_run"


async def run_maturation(conn, *, now: Optional[datetime] = None,
                         scan_limit: int = DEFAULT_SCAN_LIMIT,
                         observation_limit: int = DEFAULT_OBSERVATION_LIMIT,
                         dry_run: bool = False,
                         include_settled: bool = False,
                         mode: str = MODE_SCHEDULED) -> Dict[str, Any]:
    """One bounded maturation pass. Plan, then measure what is due.

    Writes NOTHING when `dry_run` is true — not even the plan rows — because
    the brief asks a dry run to REPORT what a real run would do, and a dry run
    that leaves a hundred rows behind has already done part of it.

    `include_settled` re-reads measured and terminal rows too. It exists for
    the operator recheck and never for the schedule: rechecking frozen evidence
    on every run would spend the whole bound re-deriving answers that cannot
    change.
    """
    moment = now or datetime.now(timezone.utc)
    as_of = resolve_latest_completed_session(moment)

    if dry_run:
        return await _dry_run(conn, as_of=as_of, scan_limit=scan_limit,
                              observation_limit=observation_limit,
                              moment=moment)

    planned = await plan_missing_observations(conn, limit=scan_limit)

    sql = (SELECT_DUE_INCLUDING_SETTLED_SQL if include_settled
           else SELECT_DUE_SQL)
    due = [dict(r) for r in await conn.fetch(sql, as_of,
                                             max(0, int(observation_limit)))]

    tally = {STATUS_MEASURED: 0, STATUS_WAITING_FOR_DATA: 0,
             STATUS_NOT_YET_ELIGIBLE: 0, STATUS_FAILED_TERMINAL: 0}
    revisions = 0
    reasons: Dict[str, int] = {}
    measured_sample: List[Dict[str, Any]] = []

    for observation in due:
        outcome = await measure_observation(conn, observation,
                                            as_of_session=as_of, now=moment)
        status = outcome["status"]
        tally[status] = tally.get(status, 0) + 1
        if outcome.get("revision_detected"):
            revisions += 1
        reason = outcome.get("status_reason")
        if reason and status != STATUS_MEASURED:
            reasons[reason] = reasons.get(reason, 0) + 1
        if status == STATUS_MEASURED and len(measured_sample) < 10 \
                and not outcome.get("already") and not outcome.get("unchanged"):
            measured_sample.append({
                "symbol": observation["symbol"],
                "scan_session": observation["scan_session"].isoformat(),
                "horizon": observation["horizon_label"],
                "symbol_return_pct": outcome.get("symbol_return_pct"),
                "excess_return_pct": outcome.get("excess_return_pct")})

    return {
        "contract_version": RESEARCH_OUTCOME_CONTRACT_VERSION,
        "mode": mode,
        "as_of_session": as_of.isoformat(),
        "scans_considered": planned["scans_considered"],
        "observations_planned": planned["observations_planned"],
        "observations_due": len(due),
        "measured": tally[STATUS_MEASURED],
        "waiting_for_data": tally[STATUS_WAITING_FOR_DATA],
        "not_yet_eligible": tally[STATUS_NOT_YET_ELIGIBLE],
        "failed_terminal": tally[STATUS_FAILED_TERMINAL],
        "revisions_detected": revisions,
        "waiting_reasons": reasons,
        "measured_sample": measured_sample,
        # A run that stopped at its bound with work still due must never read
        # as a run that finished the work.
        "truncated_by_limit": bool(
            planned["truncated_by_limit"]
            or len(due) >= max(0, int(observation_limit))),
    }


async def _dry_run(conn, *, as_of: date, scan_limit: int,
                   observation_limit: int, moment: datetime) -> Dict[str, Any]:
    """What a real run WOULD do, computed without writing anything.

    Planned-but-unwritten observations are evaluated in memory against the same
    calendar and the same bar predicates the real path uses, so the counts are
    a prediction of the real run rather than a different program's opinion.
    """
    existing = [dict(r) for r in await conn.fetch(
        "SELECT * FROM public.research_scan_outcomes "
        "ORDER BY scan_session ASC, symbol ASC, horizon_sessions ASC")]
    unplanned_scans = [dict(r) for r in await conn.fetch(
        SELECT_UNPLANNED_SCANS_SQL, max(0, int(scan_limit)), HORIZONS[-1])]

    candidates: List[Dict[str, Any]] = []
    for row in existing:
        if row["status"] in (STATUS_NOT_YET_ELIGIBLE, STATUS_WAITING_FOR_DATA):
            candidates.append(row)
    for scan in unplanned_scans:
        candidates.extend(plan_observations(scan))

    eligible = not_yet = 0
    measurable = 0
    missing: Dict[str, int] = {}
    by_horizon: Dict[str, Dict[str, int]] = {}
    for row in candidates:
        label = row["horizon_label"]
        bucket = by_horizon.setdefault(
            label, {"eligible": 0, "not_yet_eligible": 0, "measurable": 0,
                    "missing_data": 0})
        if row["horizon_session"] > as_of:
            not_yet += 1
            bucket["not_yet_eligible"] += 1
            continue
        eligible += 1
        bucket["eligible"] += 1
        entry = await _bar(conn, row["symbol"], row["scan_session"])
        exit_bar = await _bar(conn, row["symbol"], row["horizon_session"])
        bench_entry = await _bar(conn, row["benchmark_symbol"],
                                 row["scan_session"])
        bench_exit = await _bar(conn, row["benchmark_symbol"],
                                row["horizon_session"])
        result = measure(entry=entry, exit_bar=exit_bar,
                         bench_entry=bench_entry, bench_exit=bench_exit,
                         window=[], horizon_sessions=int(row["horizon_sessions"]))
        if result["status"] == STATUS_MEASURED:
            measurable += 1
            bucket["measurable"] += 1
        else:
            reason = result["status_reason"]
            missing[reason] = missing.get(reason, 0) + 1
            bucket["missing_data"] += 1

    settled = {"measured": 0, "failed_terminal": 0}
    for row in existing:
        if row["status"] in settled:
            settled[row["status"]] += 1

    return {
        "contract_version": RESEARCH_OUTCOME_CONTRACT_VERSION,
        "mode": MODE_DRY_RUN,
        "as_of_session": as_of.isoformat(),
        "scans_unplanned": len(unplanned_scans),
        "observations_existing": len(existing),
        "observations_would_plan": len(unplanned_scans) * len(HORIZONS),
        "eligible": eligible,
        "not_yet_eligible": not_yet,
        "measurable": measurable,
        "missing_data": eligible - measurable,
        "missing_data_reasons": missing,
        "by_horizon": by_horizon,
        "already_measured": settled["measured"],
        "already_failed_terminal": settled["failed_terminal"],
        "truncated_by_limit": len(unplanned_scans) >= max(0, int(scan_limit)),
    }


# --------------------------------------------------------------------------- #
# the durable RUN: one row per pass, opened first and closed in a `finally`
# --------------------------------------------------------------------------- #

STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_DRY_RUN = "dry_run"
STATUS_FAILED = "failed"

OPEN_RUN_SQL = """
INSERT INTO public.research_outcome_runs (
    run_key, contract_version, status, mode, as_of_session)
VALUES ($1, $2, 'running', $3, $4)
ON CONFLICT (run_key) DO UPDATE SET updated_at = NOW()
RETURNING id, status
"""

CLOSE_RUN_SQL = """
UPDATE public.research_outcome_runs SET
    status = $2, failure_summary = $3,
    completed_at = $4,
    duration_seconds = EXTRACT(EPOCH FROM ($4 - started_at)),
    as_of_session = COALESCE($5, as_of_session),
    scans_considered = $6, observations_planned = $7, observations_due = $8,
    measured = $9, waiting_for_data = $10, not_yet_eligible = $11,
    failed_terminal = $12, revisions_detected = $13,
    truncated_by_limit = $14, summary = $15::jsonb, updated_at = NOW()
WHERE id = $1
"""


async def run_outcome_maturation(conn, *, run_key: str,
                                 mode: str = MODE_SCHEDULED,
                                 scan_limit: int = DEFAULT_SCAN_LIMIT,
                                 observation_limit: int = DEFAULT_OBSERVATION_LIMIT,
                                 include_settled: bool = False,
                                 dry_run: bool = False,
                                 now: Optional[datetime] = None,
                                 ) -> Dict[str, Any]:
    """One maturation run, with its durable record. The ONE entry point.

    The operator CLI and the queue handler both call this, with the same
    arguments, which is what keeps the manual proof and the scheduled run one
    program rather than two.

    THE RUN ROW IS WRITTEN IN A `finally`
    -------------------------------------
    The same shape `research_lifecycle.run_lifecycle` uses, and for the same
    reason: a worker that dies after doing the work but before finalising its
    task must have left the evidence behind, so the queue's crash-reconcile
    probe can recognise a completed run instead of repeating it. A run that
    FAILED is recorded as failed and is explicitly NOT durable output — a
    recorded failure reconciled to `succeeded` is how a continuation gets
    destroyed (T16).

    A DRY RUN OPENS NO ROW AT ALL. It writes nothing anywhere, which is what
    makes it safe to point at production data before a decision.
    """
    moment = now or datetime.now(timezone.utc)
    if dry_run:
        summary = await run_maturation(conn, now=moment, scan_limit=scan_limit,
                                       observation_limit=observation_limit,
                                       dry_run=True, mode=MODE_DRY_RUN)
        summary["run_key"] = run_key
        summary["status"] = STATUS_DRY_RUN
        return summary

    as_of = resolve_latest_completed_session(moment)
    row = await conn.fetchrow(OPEN_RUN_SQL, run_key,
                              RESEARCH_OUTCOME_CONTRACT_VERSION, mode, as_of)
    run_id = row["id"]

    status = STATUS_FAILED
    failure: Optional[str] = None
    summary: Dict[str, Any] = {}
    try:
        summary = await run_maturation(
            conn, now=moment, scan_limit=scan_limit,
            observation_limit=observation_limit, dry_run=False,
            include_settled=include_settled, mode=mode)
        status = STATUS_COMPLETED
        return {"run_key": run_key, "run_id": str(run_id),
                "status": status, **summary}
    except Exception as exc:                              # noqa: BLE001
        # Bounded and secret-free: an exception CLASS, never a payload, never a
        # DSN, never a provider message.
        failure = type(exc).__name__[:200]
        logger.exception("research outcome maturation failed run_key=%s",
                         run_key)
        raise
    finally:
        finished = datetime.now(timezone.utc)
        try:
            await conn.execute(
                CLOSE_RUN_SQL, run_id, status, failure, finished, as_of,
                int(summary.get("scans_considered", 0) or 0),
                int(summary.get("observations_planned", 0) or 0),
                int(summary.get("observations_due", 0) or 0),
                int(summary.get("measured", 0) or 0),
                int(summary.get("waiting_for_data", 0) or 0),
                int(summary.get("not_yet_eligible", 0) or 0),
                int(summary.get("failed_terminal", 0) or 0),
                int(summary.get("revisions_detected", 0) or 0),
                bool(summary.get("truncated_by_limit", False)),
                json.dumps(summary, default=str))
        except Exception:                                 # noqa: BLE001
            # Never let the bookkeeping mask the work's own outcome.
            logger.warning("could not finalise research outcome run row",
                           exc_info=False)


# --------------------------------------------------------------------------- #
# reading the ledger (the brief's O13, answerable without a log)
# --------------------------------------------------------------------------- #

STATUS_SUMMARY_SQL = """
SELECT horizon_label, horizon_sessions, scan_classification, status,
       count(*) AS observations,
       count(*) FILTER (WHERE excess_return_pct > 0) AS beat_benchmark,
       avg(symbol_return_pct)   AS mean_symbol_return_pct,
       avg(benchmark_return_pct) AS mean_benchmark_return_pct,
       avg(excess_return_pct)   AS mean_excess_return_pct
FROM public.research_scan_outcomes
GROUP BY 1, 2, 3, 4
ORDER BY horizon_sessions, scan_classification, status
"""


async def outcome_status(conn) -> Dict[str, Any]:
    """The whole ledger, partitioned by horizon, classification and status.

    Descriptive only. No hit rate is presented without the sample size beside
    it and no aggregate is presented for a status other than `measured`,
    because a mean over rows that hold no number is not a small number — it is
    a category error.
    """
    rows = [dict(r) for r in await conn.fetch(STATUS_SUMMARY_SQL)]
    by_status: Dict[str, int] = {}
    cells: List[Dict[str, Any]] = []
    for row in rows:
        by_status[row["status"]] = by_status.get(row["status"], 0) + int(
            row["observations"])
        cell = {"horizon": row["horizon_label"],
                "classification": row["scan_classification"],
                "status": row["status"],
                "observations": int(row["observations"])}
        if row["status"] == STATUS_MEASURED:
            cell.update({
                "beat_benchmark": int(row["beat_benchmark"] or 0),
                "mean_symbol_return_pct":
                    _as_float(row["mean_symbol_return_pct"]),
                "mean_benchmark_return_pct":
                    _as_float(row["mean_benchmark_return_pct"]),
                "mean_excess_return_pct":
                    _as_float(row["mean_excess_return_pct"])})
        cells.append(cell)
    total = sum(by_status.values())
    return {"contract_version": RESEARCH_OUTCOME_CONTRACT_VERSION,
            "observations": total,
            "by_status": {s: by_status.get(s, 0) for s in OUTCOME_STATUSES},
            "cells": cells}


__all__ = [
    "RESEARCH_OUTCOME_CONTRACT_VERSION", "HORIZONS", "OUTCOME_STATUSES",
    "STATUS_NOT_YET_ELIGIBLE", "STATUS_WAITING_FOR_DATA", "STATUS_MEASURED",
    "STATUS_FAILED_TERMINAL", "MISSING_DATA_GRACE_SESSIONS",
    "DEFAULT_SCAN_LIMIT", "DEFAULT_OBSERVATION_LIMIT", "MAX_REVISION_NOTES",
    "EXCURSION_BASIS_DAILY", "EXCURSION_BASIS_INCOMPLETE",
    "REASON_MISSING_SYMBOL_ENTRY", "REASON_MISSING_SYMBOL_EXIT",
    "REASON_MISSING_BENCHMARK_ENTRY", "REASON_MISSING_BENCHMARK_EXIT",
    "REASON_NON_POSITIVE_PRICE", "REASON_GRACE_EXCEEDED", "REASON_MEASURED",
    "REASON_HORIZON_NOT_COMPLETE",
    "MODE_SCHEDULED", "MODE_BACKFILL", "MODE_DRY_RUN",
    "Bar", "bars_hash", "measure", "horizon_session_for", "plan_observations",
    "plan_missing_observations", "measure_observation", "run_maturation",
    "run_outcome_maturation", "outcome_status",
    "STATUS_RUNNING", "STATUS_COMPLETED", "STATUS_DRY_RUN", "STATUS_FAILED",
]
