"""prospective_outcome_maturation.v1 — mature ONE frozen pair's shared
market-path outcome from LOCAL history only.

Reuses the existing pure outcome.v1 math and the pair-level
`strategy_shadow_pair_outcomes` schema UNCHANGED (Concept A: one outcome per
pair, shared by both arms — never an arm-specific return). NO formula,
horizon, threshold or price convention is changed here; every calculation
function is imported verbatim from app.workers.shadow.outcomes.calculator /
persistence. The ONLY new code is the LOCAL-ONLY forward-bar read
(app/jobs/prospective_outcome_local_reader.py), which replaces the existing
service's provider-backed `_fetch_daily_range` — this worker never
constructs or calls Massive/FMP/any provider.

Idempotency: `upsert_pair_outcome` write-once-merges under a row lock
(FOR UPDATE + ON CONFLICT DO NOTHING) — replaying this task for a pair whose
outcome is already `complete` is a no-op merge (frozen horizons never
regress); replaying while `partial` simply re-observes whatever forward bars
exist locally now and may advance (never retreat) the horizons resolved.

CPU isolation mirrors handlers/prospective.py: `run_prospective_outcome_task`
is the picklable, module-level child-process entrypoint (own event loop, own
DB pool); `evaluate_prospective_outcome` is the async core; `probe_
prospective_outcome_durable_output` is the parent-side reconcile probe used
on child death / lease expiry.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import asyncpg

from app.jobs import contracts as C
from app.jobs.contracts import ProspectiveOutcomePayload, TerminalJobError
from app.jobs.prospective_outcome_local_reader import (
    local_session_dates,
    read_local_forward_bars,
    read_local_snapshot_bar,
)
from app.workers.outcomes.calculator import HOLDING_WINDOWS
from app.workers.shadow.outcomes.calculator import (
    build_forward_sequence,
    check_reference_revision,
    compute_outcome_values,
    resolve_reference_price,
    status_for_bar_count,
)
from app.workers.shadow.outcomes.constants import (
    CALCULATION_VERSION,
    FORWARD_FRAME_VERSION,
    OUTCOME_COVERAGE_VERSION,
    OUTCOME_FINGERPRINT_VERSION,
    REASON_PROVIDER_MISMATCH,
    REASON_SNAPSHOT_BAR_MISSING,
    REFERENCE_PRICE_ROLE,
)
from app.workers.shadow.outcomes.fingerprints import (
    compute_forward_bars_hash,
    compute_outcome_fingerprint,
)
from app.workers.shadow.outcomes.persistence import (
    select_pairs_for_outcomes,
    upsert_pair_outcome,
)
from app.workers.shadow.outcomes.eligibility import (
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_MATURED,
    classify_maturation_eligibility,
    completed_forward_sessions,
)

# The only provider identity a local-only worker ever produces: the same
# "local-history" tag every campaign pair already carries as its own
# `provider` (LocalHistoryProvider.name) — satisfying the existing
# "forward data MUST come from the frozen pair's provider" continuity rule
# without inventing a new provider name.
LOCAL_FORWARD_PROVIDER = "local-history"


# --------------------------------------------------------------------------
# child-process entrypoint (picklable) + in-process async core
# --------------------------------------------------------------------------
def run_prospective_outcome_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """CHILD PROCESS entrypoint. Owns its event loop + DB pool. Returns a
    bounded result dict; NEVER raises across the process boundary."""
    import asyncio
    return asyncio.run(_child_main(payload))


async def _child_main(payload: Dict[str, Any]) -> Dict[str, Any]:
    from app.deps import init_db_pool, close_db_pool
    await init_db_pool()
    try:
        return await evaluate_prospective_outcome(payload)
    finally:
        try:
            await close_db_pool()
        except Exception:
            pass


async def evaluate_prospective_outcome(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Async core (a DB pool must already be initialized). Always returns a
    dict: {"ok": True, "reconciled": bool, "result": {...}} or
    {"ok": False, "error_class": ..., "safe_error_code": ..., "message": ...}."""
    from app.workers.persistence import get_db_connection, release_db_connection
    try:
        p = ProspectiveOutcomePayload.from_dict(payload)
    except C.JobError as e:
        return _err(e)
    try:
        conn = await get_db_connection()
        try:
            await _load_and_validate_registration(conn, p)
        finally:
            await release_db_connection(conn)

        # include_recalc=True + pending=False (default) drops the default
        # "(o.id IS NULL OR status IN pending/partial)" predicate entirely —
        # an explicit pair_ids selector must see the pair regardless of its
        # current outcome status (including already-complete), otherwise an
        # idempotent replay of a completed pair would look like "not found".
        pairs = await select_pairs_for_outcomes(pair_ids=[p.pair_id], include_recalc=True, limit=1)
        if not pairs:
            raise TerminalJobError("invalid_pair", "pair not found for outcome selection")
        pair = pairs[0]
        if pair["symbol"] != p.symbol:
            raise TerminalJobError("invalid_task_payload", "symbol/pair mismatch")
        if pair["existing_status"] == "complete":
            return {"ok": True, "reconciled": True,
                    "result": _bounded_result(pair, outcome_status="complete",
                                              already_applied=True)}

        record = await _mature_one_pair(pair)
        result = await upsert_pair_outcome(record)
        return {"ok": True, "reconciled": False,
                "result": _bounded_result(pair, outcome_status=result["outcome_status"],
                                          outcome_id=result.get("outcome_id"))}
    except C.JobError as e:
        return _err(e)
    except asyncpg.PostgresError as e:
        return {"ok": False, "error_class": C.ERR_RETRYABLE,
                "safe_error_code": "database_error", "message": type(e).__name__[:120]}
    except Exception as e:
        return {"ok": False, "error_class": C.ERR_RETRYABLE,
                "safe_error_code": "unexpected_worker_error", "message": type(e).__name__[:120]}


def _bounded_result(pair: Dict[str, Any], *, outcome_status: str,
                    outcome_id: Optional[str] = None,
                    already_applied: bool = False) -> Dict[str, Any]:
    return {
        "pair_id": pair["pair_id"], "symbol": pair["symbol"],
        "outcome_status": outcome_status, "outcome_id": outcome_id,
        "already_applied": already_applied,
        "provider_called": False, "provider_constructed": False,
    }


def _err(e: C.JobError) -> Dict[str, Any]:
    return {"ok": False, "error_class": e.error_class,
            "safe_error_code": e.safe_error_code, "message": str(e)[:200]}


# --------------------------------------------------------------------------
# validation — re-derive identity from the immutable registration
# --------------------------------------------------------------------------
async def _load_and_validate_registration(conn: asyncpg.Connection,
                                          p: ProspectiveOutcomePayload) -> Dict[str, Any]:
    reg = await conn.fetchrow(
        "SELECT * FROM prospective_campaign_registrations WHERE id=$1", p.registration_id)
    if reg is None:
        raise TerminalJobError("invalid_registration", "registration not found")
    if reg["registration_identity"] != p.registration_identity:
        raise TerminalJobError("stale_registration_identity", "identity drift")
    if str(reg["campaign_id"]) != p.campaign_id:
        raise TerminalJobError("invalid_task_payload", "campaign_id mismatch")
    if str(reg["campaign_run_id"]) != p.campaign_run_id:
        raise TerminalJobError("invalid_task_payload", "campaign_run_id mismatch")
    if reg["status"] != "completed":
        # Maturation only ever runs against a COMPLETED campaign — never a
        # still-executing one (its pairs could still be mid-write).
        raise TerminalJobError("campaign_not_completed", f"status={reg['status']}")
    return dict(reg)


# --------------------------------------------------------------------------
# the pure single-pair maturation (existing formulas, local-only read)
# --------------------------------------------------------------------------
async def _mature_one_pair(pair: Dict[str, Any]) -> Dict[str, Any]:
    from app.jobs.contracts import RetryableJobError

    if pair.get("provider") != LOCAL_FORWARD_PROVIDER:
        # Provider continuity is required in shadow_pair_outcomes.v1 (never
        # mix providers, never silently substitute) — unchanged rule, just
        # applied against this worker's own fixed local-history identity.
        return _error_record(pair, REASON_PROVIDER_MISMATCH,
                             f"pair provider '{pair.get('provider')}' != "
                             f"'{LOCAL_FORWARD_PROVIDER}'")

    reference_price = resolve_reference_price(
        frame_last_bar=pair["frame_last_bar"], frame_bar_count=pair["frame_bar_count"],
        snapshot_date=pair["snapshot_date"], frame_last_date=pair["frame_last_date"])

    snapshot_bar = await read_local_snapshot_bar(pair["symbol"], pair["snapshot_date"])
    if snapshot_bar is None:
        return _error_record(pair, REASON_SNAPSHOT_BAR_MISSING,
                             "local daily_bars has no bar on snapshot_date; "
                             "reference continuity unconfirmed")
    revision_detected, revision_note = check_reference_revision(
        reference_price, snapshot_bar, provider=LOCAL_FORWARD_PROVIDER)
    if revision_detected:
        from app.workers.shadow.outcomes.constants import REASON_REFERENCE_REVISION
        from datetime import datetime, timezone
        return _error_record(
            pair, REASON_REFERENCE_REVISION,
            "local snapshot-date close diverged from the frozen reference; "
            "forward price scale incompatible",
            reference_revision_detected=True,
            revision_notes=[{**revision_note, "detected_at":
                            datetime.now(timezone.utc).isoformat()}])

    raw_bars = await read_local_forward_bars(pair["symbol"], pair["snapshot_date"])
    try:
        sequence = build_forward_sequence(raw_bars, pair["snapshot_date"],
                                          explicit_completed=True)
    except Exception as exc:
        # A malformed locally-stored bar is a data-integrity condition, not a
        # transient one — bounded, non-fatal to the pair (frozen horizons, if
        # any, are preserved by the merge layer); a future warmup correction
        # can repair it (upsert_daily_bars, fingerprint-guarded).
        raise RetryableJobError("local_forward_bar_malformed", type(exc).__name__)

    forward_bars = sequence["forward_bars"]
    values = compute_outcome_values(reference_price, forward_bars)

    return {
        "pair_id": pair["pair_id"],
        "outcome_fingerprint": compute_outcome_fingerprint(
            pair_fingerprint=pair["pair_fingerprint"],
            pair_fingerprint_version=pair["pair_fingerprint_version"]),
        "outcome_fingerprint_version": OUTCOME_FINGERPRINT_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
        "forward_frame_version": FORWARD_FRAME_VERSION,
        "reference_price": reference_price,
        "reference_price_role": REFERENCE_PRICE_ROLE,
        "forward_provider": LOCAL_FORWARD_PROVIDER,
        "forward_data_as_of": values["last_forward_date"] or None,
        "available_forward_bars": values["available_forward_bars"],
        "first_forward_date": values["first_forward_date"],
        "last_forward_date": values["last_forward_date"],
        "forward_bars_hash": compute_forward_bars_hash(
            symbol=pair["symbol"], provider=LOCAL_FORWARD_PROVIDER,
            snapshot_date=pair["snapshot_date"], forward_bars=forward_bars),
        "max_favorable_excursion": values["max_favorable_excursion"],
        "max_adverse_excursion": values["max_adverse_excursion"],
        "mfe_mae_bar_count": values["mfe_mae_bar_count"],
        # Benchmark returns require local SPY/QQQ history, which is outside
        # this campaign's frozen 25-symbol universe. Consistent with the
        # existing service's own rule ("a benchmark failure NEVER fails the
        # pair"), this is left unset (best-effort) rather than fabricated or
        # blocking — see history-readiness audit in the phase report.
        "benchmark_returns": None,
        "reference_revision_detected": False,
        "revision_notes": [],
        "outcome_status": status_for_bar_count(values["available_forward_bars"]),
        "error_code": None,
        "error_message": None,
        **{f"ret_{w}d": values["ret_by_window"][w] for w in HOLDING_WINDOWS},
    }


def _error_record(pair: Dict[str, Any], error_code: str, error_message: str,
                  *, reference_revision_detected: bool = False,
                  revision_notes: Optional[list] = None) -> Dict[str, Any]:
    from app.workers.shadow.outcomes.constants import STATUS_ERROR
    return {
        "pair_id": pair["pair_id"],
        "outcome_fingerprint": compute_outcome_fingerprint(
            pair_fingerprint=pair["pair_fingerprint"],
            pair_fingerprint_version=pair["pair_fingerprint_version"]),
        "outcome_fingerprint_version": OUTCOME_FINGERPRINT_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
        "forward_frame_version": FORWARD_FRAME_VERSION,
        "reference_price": None,
        "reference_price_role": REFERENCE_PRICE_ROLE,
        "forward_provider": LOCAL_FORWARD_PROVIDER,
        "outcome_status": STATUS_ERROR,
        "error_code": error_code,
        "error_message": error_message,
        "available_forward_bars": 0,
        "reference_revision_detected": bool(reference_revision_detected),
        "revision_notes": list(revision_notes or []),
    }


# --------------------------------------------------------------------------
# parent-side reconcile probe (used on child death / lease expiry)
# --------------------------------------------------------------------------
async def probe_prospective_outcome_durable_output(conn: asyncpg.Connection,
                                                    payload: Dict[str, Any]
                                                    ) -> Optional[Dict[str, Any]]:
    """Return a bounded result dict if this pair's outcome is already
    `complete` (→ reconcile to succeeded WITHOUT recompute), else None
    (→ retryable). A `partial` outcome is NOT reconciled here — partial is a
    legitimate, non-terminal state that a fresh claim should re-observe
    (more forward bars may exist now), so it must go through a real attempt,
    not be silently treated as done."""
    try:
        p = ProspectiveOutcomePayload.from_dict(payload)
    except C.JobError:
        return None
    row = await conn.fetchrow(
        "SELECT p.id AS pair_id, p.symbol, o.id AS outcome_id, o.outcome_status "
        "FROM strategy_shadow_pairs p LEFT JOIN strategy_shadow_pair_outcomes o "
        "ON o.pair_id = p.id WHERE p.id = $1", p.pair_id)
    if row is None or row["outcome_status"] != "complete":
        return None
    return {"pair_id": str(row["pair_id"]), "symbol": row["symbol"],
            "outcome_status": "complete", "outcome_id": str(row["outcome_id"]),
            "already_applied": True, "reconciled": True,
            "provider_called": False, "provider_constructed": False}


__all__ = [
    "run_prospective_outcome_task",
    "evaluate_prospective_outcome",
    "probe_prospective_outcome_durable_output",
    "LOCAL_FORWARD_PROVIDER",
]
