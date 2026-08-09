"""prospective_symbol_evaluation.v1 — evaluate exactly ONE frozen-universe symbol.

Reuses the PURE shadow runner unchanged: a single call to
``run_shadow_comparison(provider, [symbol], run_id=<shared campaign run>, ...)``
produces one strategy_shadow_runs row (idempotent create), one pair and two
evaluations (candidate + control) persisted ATOMICALLY by ``persist_shadow_pair``
(pair + both arms in one transaction — never a pair without both arms). NO
strategy math lives here; NO provider is constructed; NO outcome is created.

Crash safety:
  * The whole per-symbol persistence is atomic (one transaction inside the
    reused runner), so a partial pair (one arm) can never exist.
  * If a complete pair + both arms already exist for this symbol under the
    campaign run, the task RECONCILES to succeeded WITHOUT recomputation
    (crash-after-persist, or an exact task replay).
  * All validation re-derives every immutable identity from the registration —
    the payload is only the addressing of which frozen symbol to evaluate.

CPU isolation: ``run_prospective_symbol_task`` is the picklable, module-level
child-process entrypoint (own event loop + own DB pool). ``evaluate_prospective_
symbol`` is the async core (assumes a pool is already up) so tests can call it
in-process. ``probe_prospective_durable_output`` is the parent-side reconcile
probe used on child death / lease expiry.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

import asyncpg

import app.prospective_campaign as pc
from app.jobs import contracts as C
from app.jobs.contracts import ProspectiveSymbolPayload, TerminalJobError
from app.jobs import prospective_support as PS


# --------------------------------------------------------------------------
# child-process entrypoint (picklable) + in-process async core
# --------------------------------------------------------------------------
def run_prospective_symbol_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """CHILD PROCESS entrypoint. Owns its event loop + DB pool. Returns a bounded
    result dict; NEVER raises across the process boundary."""
    import asyncio
    return asyncio.run(_child_main(payload))


async def _child_main(payload: Dict[str, Any]) -> Dict[str, Any]:
    from app.deps import init_db_pool, close_db_pool
    await init_db_pool()
    try:
        return await evaluate_prospective_symbol(payload)
    finally:
        try:
            await close_db_pool()
        except Exception:
            pass


async def evaluate_prospective_symbol(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Async core (a DB pool must already be initialized). Always returns a dict:
    {"ok": True, "reconciled": bool, "result": {...}} or
    {"ok": False, "error_class": ..., "safe_error_code": ..., "message": ...}."""
    from app.workers.persistence import get_db_connection, release_db_connection
    try:
        p = ProspectiveSymbolPayload.from_dict(payload)
    except C.JobError as e:
        return _err(e)
    try:
        conn = await get_db_connection()
        try:
            reg = await _load_and_validate_registration(conn, p)
            await _validate_symbol_membership(conn, reg, p)
            # Recognise an already-evaluated symbol FIRST (idempotent replay),
            # so a prior campaign whose outcomes were later matured still
            # reconciles cleanly. The run-scoped guard then only protects a
            # genuinely fresh evaluation (the campaign's own pairs must carry no
            # outcomes); OTHER campaigns' matured outcomes are expected.
            existing = await _existing_symbol_result(conn, reg, p)
            if existing is not None:
                return {"ok": True, "reconciled": True, "result": existing}
            await PS.assert_no_outcomes_for_run(conn, reg["campaign_run_id"])
        finally:
            await release_db_connection(conn)

        # Pure single-symbol evaluation (runner manages its own connections).
        await _run_single_symbol(reg, p)

        conn = await get_db_connection()
        try:
            result = await _existing_symbol_result(conn, reg, p)
        finally:
            await release_db_connection(conn)
        if result is None:
            # preflight said all-ready; a missing pair here means the symbol was
            # rejected by the runner (a data-integrity condition) — terminal.
            raise TerminalJobError("symbol_evaluation_incomplete",
                                   "no complete pair persisted for symbol")
        result["reconciled"] = False
        return {"ok": True, "reconciled": False, "result": result}
    except C.JobError as e:
        return _err(e)
    except asyncpg.PostgresError as e:  # transient DB conditions → retryable
        return {"ok": False, "error_class": C.ERR_RETRYABLE,
                "safe_error_code": "database_error", "message": type(e).__name__[:120]}
    except Exception as e:  # unexpected → retryable (bounded by max_attempts)
        return {"ok": False, "error_class": C.ERR_RETRYABLE,
                "safe_error_code": "unexpected_worker_error", "message": type(e).__name__[:120]}


def _err(e: C.JobError) -> Dict[str, Any]:
    return {"ok": False, "error_class": e.error_class,
            "safe_error_code": e.safe_error_code, "message": str(e)[:200]}


# --------------------------------------------------------------------------
# validation — re-derive every identity from the immutable registration
# --------------------------------------------------------------------------
async def _load_and_validate_registration(conn: asyncpg.Connection,
                                          p: ProspectiveSymbolPayload) -> Dict[str, Any]:
    reg = await conn.fetchrow(
        "SELECT * FROM prospective_campaign_registrations WHERE id=$1", p.registration_id)
    if reg is None:
        raise TerminalJobError("invalid_registration", "registration not found")
    if reg["registration_identity"] != p.registration_identity:
        raise TerminalJobError("stale_registration_identity", "identity drift")
    if reg["status"] not in ("registered", "executing", "completed"):
        raise TerminalJobError("registration_not_executable", f"status={reg['status']}")
    if reg["universe_hash"] != p.universe_hash:
        raise TerminalJobError("stale_universe", "universe hash drift")
    if reg["history_readiness_manifest_hash"] != p.history_readiness_manifest_hash:
        raise TerminalJobError("stale_history_manifest", "history manifest drift")
    if reg["snapshot_session_date"].isoformat() != p.snapshot_session_date:
        raise TerminalJobError("invalid_snapshot_session", "snapshot session drift")
    if not _same_instant(reg["snapshot_cutoff_at"], p.snapshot_cutoff_at):
        raise TerminalJobError("invalid_snapshot_session", "snapshot cutoff drift")
    if (reg["candidate_strategy_code"] != pc.CANDIDATE_STRATEGY_CODE
            or reg["candidate_strategy_version"] != pc.CANDIDATE_STRATEGY_VERSION
            or reg["candidate_signal_definition"] != pc.CANDIDATE_SIGNAL_DEFINITION):
        raise TerminalJobError("stale_candidate_identity", "candidate identity drift")
    if bool(reg["candidate_allow_enter"]) is not False:
        raise TerminalJobError("candidate_allow_enter_violation", "allow_enter must be false")
    if (reg["control_strategy_code"] != pc.CONTROL_STRATEGY_CODE
            or reg["control_strategy_version"] != pc.CONTROL_STRATEGY_VERSION):
        raise TerminalJobError("stale_control_identity", "control identity drift")
    if p.candidate_strategy_identity != PS.candidate_identity_string():
        raise TerminalJobError("stale_candidate_identity", "candidate identity string drift")
    if p.control_strategy_identity != PS.control_identity_string():
        raise TerminalJobError("stale_control_identity", "control identity string drift")
    if p.candidate_signal_definition != pc.CANDIDATE_SIGNAL_DEFINITION:
        raise TerminalJobError("strategy_contract_mismatch", "signal definition drift")
    if not reg["campaign_run_id"]:
        raise TerminalJobError("campaign_not_initialized", "campaign_run_id unset")
    return dict(reg)


def _same_instant(dt: datetime, iso: str) -> bool:
    try:
        other = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return False
    a = dt if dt.tzinfo else dt.replace(tzinfo=other.tzinfo)
    return a == other


async def _validate_symbol_membership(conn: asyncpg.Connection, reg: Dict[str, Any],
                                      p: ProspectiveSymbolPayload) -> None:
    universe = await PS.load_frozen_universe(conn, reg["universe_id"])
    if universe["universe_hash"] != p.universe_hash:
        raise TerminalJobError("stale_universe", "universe hash drift at membership")
    symbols = universe["symbols"]
    if p.ordinal < 0 or p.ordinal >= len(symbols):
        raise TerminalJobError("invalid_task_payload", f"ordinal {p.ordinal} out of range")
    if symbols[p.ordinal] != p.symbol:
        raise TerminalJobError("invalid_task_payload",
                               f"symbol/ordinal mismatch at {p.ordinal}")


# --------------------------------------------------------------------------
# durable-output inspection (reconcile / verify)
# --------------------------------------------------------------------------
async def _existing_symbol_result(conn: asyncpg.Connection, reg: Dict[str, Any],
                                  p: ProspectiveSymbolPayload) -> Optional[Dict[str, Any]]:
    """Return a complete bounded result dict when a pair + BOTH arms already
    exist for this symbol under the campaign run; else None. persist_shadow_pair
    is atomic, so a pair always has both arms — a pair with a single arm never
    occurs."""
    rows = await conn.fetch(
        "SELECT p.id AS pair_id, e.arm_code, e.verdict, e.score, e.reason, e.details_snapshot "
        "FROM strategy_shadow_run_pairs rp "
        "JOIN strategy_shadow_pairs p ON p.id = rp.pair_id "
        "JOIN strategy_shadow_evaluations e ON e.pair_id = p.id "
        "WHERE rp.run_id = $1 AND p.symbol = $2",
        reg["campaign_run_id"], p.symbol)
    if not rows:
        return None
    by_arm = {r["arm_code"]: r for r in rows}
    cand = by_arm.get(pc.CANDIDATE_ARM_CODE)
    ctrl = by_arm.get(pc.CONTROL_ARM_CODE)
    if cand is None or ctrl is None:
        return None
    pair_id = str(cand["pair_id"])
    return {
        "symbol": p.symbol,
        "ordinal": p.ordinal,
        "pair_id": pair_id,
        "candidate": {
            "arm": pc.CANDIDATE_ARM_CODE,
            "verdict": cand["verdict"],
            "score": _num(cand["score"]),
            "signal": pc.candidate_signal_fields(_details(cand["details_snapshot"])),
        },
        "control": {
            "arm": pc.CONTROL_ARM_CODE,
            "verdict": ctrl["verdict"],
            "score": _num(ctrl["score"]),
        },
        "provider_called": False,
        "provider_constructed": False,
    }


def _details(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            d = json.loads(v)
            return d if isinstance(d, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def _num(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# the pure single-symbol evaluation (reused runner, unchanged)
# --------------------------------------------------------------------------
async def _run_single_symbol(reg: Dict[str, Any], p: ProspectiveSymbolPayload) -> None:
    from app.prospective_local_provider import LocalHistoryProvider
    from app.workers.shadow.runner import run_shadow_comparison
    from app.workers.shadow.experiments import get_experiment

    shim = LocalHistoryProvider(
        snapshot_session_date=reg["snapshot_session_date"],
        snapshot_cutoff_at=reg["snapshot_cutoff_at"])
    experiment = get_experiment(reg["experiment_code"])
    telemetry_extras = {
        "campaign": {
            "campaign_id": str(reg["campaign_id"]) if reg["campaign_id"] else None,
            "registration_id": str(reg["id"]),
            "symbol": p.symbol,
            "ordinal": p.ordinal,
            "source": "durable_worker",
        }
    }
    summary = await run_shadow_comparison(
        shim, [p.symbol],
        run_id=str(reg["campaign_run_id"]),
        experiment=experiment,
        as_of_date=reg["snapshot_session_date"],
        now_utc=reg["snapshot_cutoff_at"],
        telemetry_extras=telemetry_extras,
    )
    # A hard runner failure (not a per-symbol rejection) is retryable.
    if isinstance(summary, dict) and summary.get("status") == "failed" and not summary.get("pairs"):
        raise C.RetryableJobError("shadow_run_failed",
                                  summary.get("error_code", "shadow_run_failed"))


# --------------------------------------------------------------------------
# parent-side reconcile probe (used on child death / lease expiry)
# --------------------------------------------------------------------------
async def probe_prospective_durable_output(conn: asyncpg.Connection,
                                          payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a complete result dict if this symbol's pair + both arms already
    exist (→ reconcile to succeeded WITHOUT recompute), else None (→ retryable)."""
    try:
        p = ProspectiveSymbolPayload.from_dict(payload)
    except C.JobError:
        return None
    reg = await conn.fetchrow(
        "SELECT id, campaign_id, campaign_run_id FROM prospective_campaign_registrations WHERE id=$1",
        payload.get("registration_id"))
    if reg is None or not reg["campaign_run_id"]:
        return None
    result = await _existing_symbol_result(conn, dict(reg), p)
    if result is not None:
        result["reconciled"] = True
    return result


__all__ = [
    "run_prospective_symbol_task",
    "evaluate_prospective_symbol",
    "probe_prospective_durable_output",
]
