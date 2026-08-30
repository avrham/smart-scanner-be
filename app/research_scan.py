"""Run the CANONICAL strategy on a research symbol, and persist it nowhere near
the experiment.

WHY THIS FILE CONTAINS NO STRATEGY MATH
---------------------------------------
Because a second Wyckoff implementation would be a second set of answers, and
the day they disagreed nobody would know which one was the product. So this
module resolves the SAME arm, builds the SAME canonical frame and calls the
SAME evaluator the prospective experiment calls:

    app.workers.shadow.runner._resolve_arm       the real strategy + frozen config
    app.workers.shadow.frames.build_canonical_frame
    app.workers.shadow.runner._evaluate_arm      the actual evaluation

Those are underscore-prefixed because they are internal to the shadow runner,
not because they are private to it — `run_shadow_comparison` is the only other
caller and it differs from this one in exactly one respect: it PERSISTS to
`strategy_shadow_runs` / `_pairs` / `_evaluations`. That is the experiment, and
a research symbol may never enter it.

WHAT THIS DELIBERATELY DOES NOT DO
----------------------------------
No `create_shadow_run`. No pair. No arm row. No outcome. No attention tier. No
ordering. And no ENTER: the strategy runs with the same `allow_enter=false`
configuration the experiment uses, and the database additionally REFUSES a row
whose verdict is ENTER (migration 026), so "research cannot trade" is a
constraint rather than an intention.

NO 4H FRAME, AND THAT IS HONEST RATHER THAN MISSING
---------------------------------------------------
The warmup fetches DAILY bars only, so a research symbol has no 4H history and
the candidate's trigger analysis reports that absence in its own vocabulary —
the same way it does for any symbol whose 4H frame is unavailable. Nothing is
fabricated to fill the gap. It costs nothing that matters here: the 4H frame
feeds the ENTER trigger, `enable_4h_trigger` is false, and a research scan may
not produce ENTER in any case. What the daily frame does give — structure
state, setup state, reason code — is the part a human is actually reading.

READS LOCAL BARS ONLY, ON THE CALLER'S CONNECTION
-------------------------------------------------
The lookahead barrier is one predicate — `trading_date <= scan_session` — and
it is the same one `LocalHistoryProvider` applies for the prospective campaign.
It is applied here directly rather than through that shim for a concrete
reason: the shim acquires its own connection from the global pool, which in an
operator-run context is a different database from the one the caller is holding.
A research scan makes no provider call at all and cannot see a bar the pinned
session could not have seen.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import app.market_context as mc
import app.research_universe as ru
from app.reference_market import PRIMARY_BENCHMARK, sector_benchmark_for

logger = logging.getLogger(__name__)

#: The candidate arm, resolved from the same constants the campaign uses so a
#: strategy rename cannot leave research evaluating something else.
from app.prospective_campaign import (CANDIDATE_ARM_CODE,
                                      CANDIDATE_STRATEGY_CODE,
                                      CONTROL_ARM_CODE,
                                      CONTROL_STRATEGY_CODE)

#: The EXPERIMENT DECLARATION, not a hand-assembled copy of it. It carries the
#: arm codes, the per-arm history-depth functions, the candidate's data-meta
#: extras and its config overrides. Assembling those by hand here is exactly
#: how a research scan would drift into evaluating something subtly different
#: from what the 25 are evaluated by — and the first symptom was a KeyError on
#: `sma_window`, because the DEFAULT experiment's depth function belongs to a
#: different pair of strategies entirely.
RESEARCH_EXPERIMENT_CODE = "wyckoff_v2_vs_baseline"

#: Bars pulled from the local store for context maths. Enough for the longest
#: relative-strength horizon with room to spare; nothing here is tuned.
CONTEXT_BARS = 90


def _market_close_utc(session: date) -> datetime:
    from app.news import session_close_utc
    return session_close_utc(session)


async def _local_bars(conn, symbol: str, session: date,
                      limit: int = CONTEXT_BARS) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        "SELECT trading_date, open, high, low, close, volume FROM daily_bars "
        "WHERE symbol=$1 AND trading_date <= $2 ORDER BY trading_date DESC LIMIT $3",
        symbol, session, limit)
    return [{"trading_date": r["trading_date"], "open": float(r["open"]),
             "high": float(r["high"]), "low": float(r["low"]),
             "close": float(r["close"]), "volume": float(r["volume"])}
            for r in reversed(rows)]


async def build_context(conn, symbol: str, *, session: date) -> Dict[str, Any]:
    """Benchmark-relative context, and an explicit answer about the sector.

    A discovered symbol is almost never in the hand-made sector registry, and
    we do not guess: `sector_unknown` is the answer, and the sector-relative
    half is simply unavailable. The BENCHMARK half still works, because
    comparing to SPY needs no mapping at all — so a research symbol gets real
    market context rather than none.
    """
    own = await _local_bars(conn, symbol, session)
    bench = await _local_bars(conn, PRIMARY_BENCHMARK, session)
    benchmark = mc.build_reference_relative_strength(
        own, bench, reference_symbol=PRIMARY_BENCHMARK if bench else None,
        reference_kind="broad_market",
        unavailable_reason=None if bench else "no_benchmark_bars_stored")

    sector_symbol = sector_benchmark_for(symbol)
    sector_state = ru.classify_sector_state(
        symbol, benchmark_available=bool(bench))
    sector: Dict[str, Any] = {
        "status": mc.STATUS_UNAVAILABLE,
        "reason": "symbol_not_in_sector_registry",
        "reference_symbol": None, "category": None,
    }
    if sector_symbol:
        sector_bars = await _local_bars(conn, sector_symbol, session)
        sector = mc.build_reference_relative_strength(
            own, sector_bars, reference_symbol=sector_symbol,
            reference_kind="sector",
            unavailable_reason=None if sector_bars else "no_sector_bars_stored")

    return {"benchmark": benchmark, "sector": sector,
            "sector_state": sector_state, "sector_symbol": sector_symbol,
            "volume": mc.build_volume_context(own)}


async def evaluate_research_symbol(conn, symbol: str, *, session: date,
                                   now: Optional[datetime] = None,
                                   ) -> Dict[str, Any]:
    """One research scan. Canonical evaluation, zero experiment writes.

    Mirrors `run_shadow_comparison`'s per-symbol sequence exactly — resolve
    arms, one local fetch shared by both, canonical frame, evaluate each arm —
    and stops before the part that creates a run.
    """
    from app.workers.patterns.config import bound_config_connection
    from app.workers.shadow.experiments import get_experiment
    from app.workers.shadow.frames import (FrameRejection, build_canonical_frame,
                                           shared_required_history_bars)
    from app.workers.shadow.runner import _evaluate_arm, _resolve_arm

    cutoff = _market_close_utc(session)
    moment = now or datetime.now(timezone.utc)
    scan_id = str(uuid.uuid4())

    experiment = get_experiment(RESEARCH_EXPERIMENT_CODE)
    # BOUND to this connection and FAIL-CLOSED, without altering the canonical
    # execution layer. Previously the config was read through the global pool,
    # which in an operator-run process points at a different database; the read
    # failed, the resolver fell back to strategy defaults, and the resulting
    # hash HAPPENED to equal the experiment's because staging stores no
    # override for this pattern. An accidental equality is not an invariant.
    with bound_config_connection(conn, require_db=True):
        candidate = await _resolve_arm(
            experiment.candidate_pattern_code, experiment.candidate_arm_code,
            config_overrides=experiment.candidate_config_overrides)
        control = await _resolve_arm(experiment.control_pattern_code,
                                     experiment.control_arm_code)
    # Depth from the EXPERIMENT's own per-arm functions. The default pair
    # belongs to a different experiment and expects config keys these
    # strategies do not have.
    requested = shared_required_history_bars(
        control["config"], candidate["config"],
        control_fn=experiment.control_history_bars,
        candidate_fn=experiment.candidate_history_bars)

    # The FMP-shaped payload the canonical frame builder consumes, from local
    # bars at or before the pinned session. Identical in shape and in barrier
    # to `LocalHistoryProvider.get_daily_history`.
    bars = await _local_bars(conn, symbol, session, limit=requested + 40)
    payload = {"symbol": symbol, "historical": [
        {"date": b["trading_date"].isoformat(), "open": b["open"],
         "high": b["high"], "low": b["low"], "close": b["close"],
         "volume": b["volume"]}
        for b in bars]}

    out: Dict[str, Any] = {
        "symbol": symbol, "scan_session": session, "scanned_at": moment,
        "contract_version": ru.RESEARCH_SCAN_CONTRACT_VERSION,
        "experiment_code": RESEARCH_EXPERIMENT_CODE,
        "strategy_code": candidate["strategy_code"],
        "strategy_version": candidate["strategy_version"],
        "config_hash": candidate["config_hash"],
        "frame_hash": None, "bars_evaluated": None,
        "verdict": None, "score": None, "reason": None,
        "rejection_reason": None, "evidence": {},
        "control_verdict": None, "control_reason": None,
        "local_bars_read": len(bars),
    }

    try:
        frame = build_canonical_frame(symbol, payload, max_bars=requested,
                                      now_utc=cutoff + timedelta(hours=8))
    except FrameRejection as rejection:
        # An honest data rejection, in the strategy's own vocabulary. Not a
        # failure of the scan and never a fabricated verdict.
        out["rejection_reason"] = rejection.reason_code
        return out

    out["frame_hash"] = frame.frame_hash
    out["bars_evaluated"] = frame.bar_count

    # The candidate's own data-meta vocabulary, from the declaration. No 4H
    # frame is supplied: a research symbol has daily bars only, and the
    # trigger analysis reports that absence in its own words rather than
    # having it fabricated.
    extras = (experiment.candidate_data_meta_extras(frame)
              if experiment.candidate_data_meta_extras is not None else None)
    evaluation = _evaluate_arm(candidate, frame, scan_id,
                               latest_bar_completed=True,
                               now_utc=cutoff + timedelta(hours=8),
                               data_meta_extras=extras)
    details = evaluation.get("details") or {}
    out.update({
        "verdict": evaluation.get("verdict"),
        "score": evaluation.get("score"),
        "reason": evaluation.get("reason"),
        "rejection_reason": evaluation.get("rejection_reason"),
        "evidence": details,
    })

    # The control arm needs its own daily depth; when a research symbol is
    # short of it, that is reported rather than substituted.
    try:
        control_eval = _evaluate_arm(control, frame, scan_id,
                                     latest_bar_completed=True,
                                     now_utc=cutoff + timedelta(hours=8))
        out["control_verdict"] = control_eval.get("verdict")
        out["control_reason"] = control_eval.get("reason")
    except Exception:                              # noqa: BLE001
        logger.warning("research control arm unavailable for %s", symbol,
                       exc_info=False)

    return out


# --------------------------------------------------------------------------- #
# persistence — its own table, with none of an experiment row's columns
# --------------------------------------------------------------------------- #

UPSERT_SCAN_SQL = """
INSERT INTO public.research_scan_results (
    symbol, scan_session, scanned_at, contract_version, strategy_code,
    strategy_version, config_hash, frame_hash, bars_evaluated, verdict, score,
    reason, rejection_reason, structure_state, setup_state, reason_code,
    evidence, benchmark_symbol, benchmark_relative, benchmark_excess_pct,
    sector_state, sector_symbol, control_verdict, control_reason,
    licensing_visibility)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb,
        $18,$19,$20,$21,$22,$23,$24,$25)
ON CONFLICT (symbol, scan_session) DO UPDATE SET
    scanned_at = EXCLUDED.scanned_at,
    verdict = EXCLUDED.verdict, score = EXCLUDED.score,
    reason = EXCLUDED.reason, rejection_reason = EXCLUDED.rejection_reason,
    structure_state = EXCLUDED.structure_state,
    setup_state = EXCLUDED.setup_state, reason_code = EXCLUDED.reason_code,
    evidence = EXCLUDED.evidence, frame_hash = EXCLUDED.frame_hash,
    bars_evaluated = EXCLUDED.bars_evaluated,
    benchmark_relative = EXCLUDED.benchmark_relative,
    benchmark_excess_pct = EXCLUDED.benchmark_excess_pct,
    sector_state = EXCLUDED.sector_state,
    control_verdict = EXCLUDED.control_verdict,
    control_reason = EXCLUDED.control_reason
RETURNING (xmax = 0) AS inserted
"""


async def persist_research_scan(conn, scan: Dict[str, Any],
                                context: Dict[str, Any]) -> bool:
    """Store one research scan, reading its semantics with the CANONICAL
    extractors rather than re-deriving them.

    `candidate_signal_fields` is the same function the prospective campaign
    uses to read a wyckoff_mtf_v2 evaluation, so `setup_state` here means
    exactly what it means on the 25. A second reader would be a second
    definition.
    """
    import app.scanner_view as sv
    from app.prospective_campaign import candidate_signal_fields
    details = scan.get("evidence") or {}
    signal = candidate_signal_fields(details)
    benchmark = context.get("benchmark") or {}
    row = await conn.fetchrow(
        UPSERT_SCAN_SQL,
        scan["symbol"], scan["scan_session"], scan["scanned_at"],
        scan["contract_version"], scan["strategy_code"],
        scan["strategy_version"], scan["config_hash"], scan.get("frame_hash"),
        scan.get("bars_evaluated"), scan.get("verdict"), scan.get("score"),
        scan.get("reason"), scan.get("rejection_reason"),
        sv.structure_state(details), signal.get("setup_state"),
        sv.reason_code(details),
        json.dumps(details, default=str),
        benchmark.get("reference_symbol"), benchmark.get("category"),
        benchmark.get("relative_return_pct"),
        context.get("sector_state") or ru.SECTOR_UNKNOWN,
        context.get("sector_symbol"),
        scan.get("control_verdict"), scan.get("control_reason"),
        "internal_research_only")
    return bool(row["inserted"])


CANDIDATE_ROW_SQL = """
SELECT r.symbol, r.state, r.discovery_reasons, r.discovery_observation_count,
       r.latest_reference_session,
       s.verdict, s.rejection_reason, s.structure_state, s.setup_state,
       s.benchmark_relative
FROM public.research_symbols r
LEFT JOIN LATERAL (
    SELECT * FROM public.research_scan_results x
    WHERE x.symbol = r.symbol ORDER BY x.scan_session DESC LIMIT 1
) s ON true
WHERE r.symbol = $1
"""

CANDIDATE_UPDATE_SQL = """
UPDATE public.research_symbols
SET candidate_state = $2, candidate_reason = $3, looked_because = $4,
    screen_findings = $5, updated_at = NOW()
WHERE symbol = $1
"""


async def classify_and_store_candidate(conn, symbol: str, *,
                                       latest_reference_session=None,
                                       now: Optional[datetime] = None,
                                       ) -> Dict[str, Any]:
    """Decide, and store, whether this symbol survived the research screen.

    The two halves are written to two columns. Discovery strength explains why
    we looked; only the scan's own evidence decides whether it survived, and
    the first cohort proved what happens when one column carries both.
    """
    row = await conn.fetchrow(CANDIDATE_ROW_SQL, symbol)
    if row is None:
        return {"symbol": symbol, "candidate_state": None}
    data = dict(row)
    verdict = ru.classify_candidate(data)
    looked = ru.looked_because(
        data, latest_reference_session=latest_reference_session
        or data.get("latest_reference_session"))
    await conn.execute(CANDIDATE_UPDATE_SQL, symbol,
                       verdict["candidate_state"], verdict["reason"],
                       looked, verdict["screen"])
    return {"symbol": symbol, **verdict, "looked_because": looked}


async def reclassify_candidates(conn, *, now: Optional[datetime] = None,
                                ) -> Dict[str, Any]:
    """Recompute every symbol's candidate state from stored evidence.

    Cheap, local, and idempotent — the classification is a pure function of
    rows we already hold, so it is recomputed rather than trusted, exactly like
    the history state.
    """
    rows = await conn.fetch("SELECT symbol FROM public.research_symbols")
    tally: Dict[str, int] = {}
    for row in rows:
        verdict = await classify_and_store_candidate(conn, row["symbol"],
                                                     now=now)
        state = verdict.get("candidate_state")
        if state:
            tally[state] = tally.get(state, 0) + 1
    return {"reclassified": len(rows), "states": tally}


async def run_research_scans(conn, *, session: date,
                             limit: int = ru.MAX_WARMUP_SYMBOLS_PER_RUN,
                             now: Optional[datetime] = None) -> Dict[str, Any]:
    """Scan every research-ready symbol, one at a time, failing per symbol."""
    moment = now or datetime.now(timezone.utc)
    rows = [dict(r) for r in await conn.fetch(
        "SELECT symbol FROM public.research_symbols "
        "WHERE state IN ('research_ready','research_scanned') "
        "ORDER BY latest_reference_session DESC, symbol LIMIT $1", limit)]
    summary: Dict[str, Any] = {"session": session.isoformat(),
                               "scanned": [], "failed": []}
    for row in rows:
        symbol = row["symbol"]
        try:
            scan = await evaluate_research_symbol(conn, symbol, session=session,
                                                  now=moment)
            context = await build_context(conn, symbol, session=session)
            await persist_research_scan(conn, scan, context)
            await conn.execute(
                "UPDATE public.research_symbols SET state=$2, "
                "research_scanned_at=$3, updated_at=NOW() WHERE symbol=$1",
                symbol, ru.STATE_RESEARCH_SCANNED, moment)
            await classify_and_store_candidate(conn, symbol, now=moment)
            summary["scanned"].append({
                "symbol": symbol, "verdict": scan.get("verdict"),
                "rejection_reason": scan.get("rejection_reason"),
                "bars": scan.get("bars_evaluated"),
                "benchmark": (context.get("benchmark") or {}).get("category"),
                "sector_state": context.get("sector_state")})
        except Exception as exc:                   # noqa: BLE001
            logger.warning("research scan failed symbol=%s", symbol,
                           exc_info=False)
            summary["failed"].append({"symbol": symbol,
                                      "error": type(exc).__name__})
    return summary


__all__ = [
    "CONTEXT_BARS", "build_context", "evaluate_research_symbol",
    "persist_research_scan", "run_research_scans",
    "classify_and_store_candidate", "reclassify_candidates",
]
