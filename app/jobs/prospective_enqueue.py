"""Canonical prospective-campaign enqueue service + campaign-state sync.

Creates exactly ONE parent job and 25 durable tasks (one per frozen-universe
symbol, deterministic ordinals) ATOMICALLY, initializes the campaign marker on
the immutable registration, and is fully idempotent (exact replay → same job,
no new tasks; completed replay → already_applied). NO strategy evaluation and NO
provider construction happen here — the API request only enqueues.

Also owns the prospective campaign/registration lifecycle mirror
(``sync_prospective_campaign``): registration → executing when work starts,
→ completed only when the job succeeds with the full 25 pairs + 50 evaluations,
and left recoverable (never completed) when the job fails or is cancelled.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

import asyncpg

import app.prospective_campaign as pc
from app.config import settings
from app.jobs import contracts as C
from app.jobs import identity as ident
from app.jobs import prospective_support as PS
from app.jobs import queue as Q
from app.jobs.contracts import TerminalJobError


async def _load_registration(conn: asyncpg.Connection, registration_id: str,
                             registration_identity: str) -> Dict[str, Any]:
    reg = await conn.fetchrow(
        "SELECT * FROM prospective_campaign_registrations WHERE id=$1", registration_id)
    if reg is None:
        raise TerminalJobError("invalid_registration", "registration not found")
    if reg["registration_identity"] != registration_identity:
        raise TerminalJobError("stale_registration_identity", "identity mismatch")
    if reg["status"] not in ("registered", "executing", "completed"):
        raise TerminalJobError("registration_not_executable", f"status={reg['status']}")
    if bool(reg["candidate_allow_enter"]) is not False:
        raise TerminalJobError("candidate_allow_enter_violation", "allow_enter must be false")
    return dict(reg)


def _execution_identity(reg: Dict[str, Any]) -> str:
    return pc.campaign_execution_identity(
        registration_identity_value=reg["registration_identity"],
        universe_hash=reg["universe_hash"],
        history_readiness_manifest_hash=reg["history_readiness_manifest_hash"],
        snapshot_session_date=reg["snapshot_session_date"].isoformat())


def _task_payload(reg: Dict[str, Any], symbol: str, ordinal: int) -> Dict[str, Any]:
    return {
        "registration_id": str(reg["id"]),
        "registration_identity": reg["registration_identity"],
        "universe_id": str(reg["universe_id"]),
        "universe_hash": reg["universe_hash"],
        "history_readiness_manifest_hash": reg["history_readiness_manifest_hash"],
        "snapshot_session_date": reg["snapshot_session_date"].isoformat(),
        "snapshot_cutoff_at": reg["snapshot_cutoff_at"].isoformat(),
        "symbol": symbol,
        "ordinal": ordinal,
        "candidate_strategy_identity": PS.candidate_identity_string(),
        "control_strategy_identity": PS.control_identity_string(),
        "candidate_signal_definition": pc.CANDIDATE_SIGNAL_DEFINITION,
    }


async def enqueue_prospective_campaign(conn: asyncpg.Connection, *, registration_id: str,
                                       registration_identity: str,
                                       requested_by: Optional[str] = None) -> Dict[str, Any]:
    """Idempotently enqueue the campaign. Returns a response dict with status
    ``queued`` | ``already_queued`` | ``already_applied``."""
    reg = await _load_registration(conn, registration_id, registration_identity)

    # Validate the frozen universe + pinned hashes BEFORE any write.
    universe = await PS.load_frozen_universe(conn, reg["universe_id"])
    if universe["universe_hash"] != reg["universe_hash"]:
        raise TerminalJobError("stale_universe", "universe hash drift")
    symbols: List[str] = universe["symbols"]
    if not symbols:
        raise TerminalJobError("empty_universe", "frozen universe has no symbols")
    await PS.assert_no_outcomes(conn)

    exec_id = _execution_identity(reg)
    job_key = ident.job_idempotency_key(
        job_type=C.JOB_TYPE_PROSPECTIVE_CAMPAIGN,
        registration_identity=reg["registration_identity"],
        campaign_execution_identity=exec_id)

    existing = await conn.fetchrow(
        "SELECT * FROM job_runs WHERE idempotency_key=$1", job_key)
    if existing is not None:
        counts = await _job_counts(conn, existing["id"])
        status = "already_applied" if reg["status"] == "completed" else "already_queued"
        return _response(status, existing, reg, counts)

    async with conn.transaction():
        # (Re)initialize the campaign marker on the registration. Generate the
        # campaign/run identities ONCE; a replay re-uses the stored values.
        campaign_id = reg["campaign_id"] or uuid.uuid4()
        campaign_run_id = reg["campaign_run_id"] or uuid.uuid4()
        await conn.execute(
            "UPDATE prospective_campaign_registrations SET campaign_id=$2, campaign_run_id=$3,"
            " campaign_execution_identity = COALESCE(campaign_execution_identity, $4),"
            " updated_at=NOW() WHERE id=$1",
            reg["id"], campaign_id, campaign_run_id, exec_id)
        reg["campaign_id"] = campaign_id
        reg["campaign_run_id"] = campaign_run_id

        job = await conn.fetchrow(
            "INSERT INTO job_runs (job_type, job_contract_version, queue_name, idempotency_key,"
            " status, priority, registration_id, campaign_id, requested_by, total_task_count,"
            " queued_task_count) "
            "VALUES ($1,$2,$3,$4,'queued',100,$5,$6,$7,$8,$8) "
            "ON CONFLICT (idempotency_key) DO NOTHING RETURNING *",
            C.JOB_TYPE_PROSPECTIVE_CAMPAIGN, C.PROSPECTIVE_CAMPAIGN_ENQUEUE_CONTRACT,
            C.PROSPECTIVE_QUEUE, job_key, reg["id"], campaign_id, requested_by, len(symbols))
        if job is None:
            # A concurrent enqueue won the race — return its job.
            job = await conn.fetchrow("SELECT * FROM job_runs WHERE idempotency_key=$1", job_key)
            counts = await _job_counts(conn, job["id"])
            return _response("already_queued", job, reg, counts)

        inserted = 0
        for ordinal, symbol in enumerate(symbols):
            payload = _task_payload(reg, symbol, ordinal)
            task_key = symbol
            task_idem = ident.prospective_task_idempotency_key(
                registration_identity=reg["registration_identity"],
                campaign_execution_identity=exec_id,
                symbol=symbol,
                snapshot_session_date=reg["snapshot_session_date"].isoformat(),
                snapshot_cutoff_at=reg["snapshot_cutoff_at"].isoformat(),
                candidate_strategy_identity=PS.candidate_identity_string(),
                control_strategy_identity=PS.control_identity_string())
            row = await conn.fetchrow(
                "INSERT INTO job_tasks (job_id, queue_name, task_type, task_contract_version,"
                " task_key, ordinal, payload, payload_hash, idempotency_key, status, priority,"
                " max_attempts) "
                "VALUES ($1,$2,$3,$3,$4,$5,$6::jsonb,$7,$8,'queued',100,$9) "
                "ON CONFLICT DO NOTHING RETURNING id",
                job["id"], C.PROSPECTIVE_QUEUE, C.PROSPECTIVE_SYMBOL_EVALUATION_TASK,
                task_key, ordinal, json.dumps(payload), ident.payload_hash(payload),
                task_idem, int(getattr(settings, "JOB_MAX_ATTEMPTS_DEFAULT", 3)))
            if row is not None:
                inserted += 1
        await Q.recompute_job_counters(conn, job["id"])
        await Q.record_event(conn, job_id=job["id"], event_type="job_created",
                             safe_message=requested_by or None,
                             metadata={"total_tasks": len(symbols), "inserted": inserted,
                                       "campaign_id": str(campaign_id)})
        job = await conn.fetchrow("SELECT * FROM job_runs WHERE id=$1", job["id"])
        counts = await _job_counts(conn, job["id"])
        return _response("queued", job, reg, counts)


async def _job_counts(conn: asyncpg.Connection, job_id: str) -> Dict[str, int]:
    row = await conn.fetchrow(
        "SELECT count(*)::int AS total,"
        " count(*) FILTER (WHERE status='queued')::int AS queued,"
        " count(*) FILTER (WHERE status IN ('leased','running'))::int AS running,"
        " count(*) FILTER (WHERE status='retryable')::int AS retryable,"
        " count(*) FILTER (WHERE status='succeeded')::int AS succeeded,"
        " count(*) FILTER (WHERE status='failed')::int AS failed,"
        " count(*) FILTER (WHERE status='cancelled')::int AS cancelled "
        "FROM job_tasks WHERE job_id=$1", job_id)
    return {k: int(row[k]) for k in row.keys()}


def _response(status: str, job: Any, reg: Dict[str, Any], counts: Dict[str, int]) -> Dict[str, Any]:
    job_id = str(job["id"])
    return {
        "contract_version": C.PROSPECTIVE_CAMPAIGN_ENQUEUE_CONTRACT,
        "status": status,
        "job_id": job_id,
        "registration_id": str(reg["id"]),
        "campaign_id": str(reg["campaign_id"]) if reg.get("campaign_id") else None,
        "total_task_count": counts["total"],
        "job_status": job["status"],
        "progress_url": f"/api/admin/jobs/{job_id}",
        "tasks_url": f"/api/admin/jobs/{job_id}/tasks",
        "audit_url": "/api/admin/prospective/audit",
    }


# --------------------------------------------------------------------------
# campaign/registration lifecycle mirror (called by the worker after settle)
# --------------------------------------------------------------------------
async def sync_prospective_campaign(conn: asyncpg.Connection, job_id: str) -> None:
    """Mirror the job state onto the immutable registration:
      * any progress          → registration 'executing' (campaign executing)
      * job succeeded          → 'completed' + counts + finished_at
      * job failed / cancelled → left recoverable (never 'completed').
    Idempotent; safe to call after every task settle."""
    job = await conn.fetchrow(
        "SELECT id, status, registration_id, campaign_id FROM job_runs WHERE id=$1", job_id)
    if job is None or job["registration_id"] is None:
        return
    reg_id = job["registration_id"]
    if job["status"] == C.JOB_SUCCEEDED:
        run_id = await conn.fetchval(
            "SELECT campaign_run_id FROM prospective_campaign_registrations WHERE id=$1", reg_id)
        counts = await _campaign_counts(conn, run_id) if run_id else {"pairs": 0, "cand": 0, "ctrl": 0}
        await conn.execute(
            "UPDATE prospective_campaign_registrations SET status='completed',"
            " pair_count=$2, candidate_evaluation_count=$3, control_evaluation_count=$4,"
            " execution_lease_expires_at=NULL, finished_at=COALESCE(finished_at, NOW()),"
            " telemetry=$5::jsonb, updated_at=NOW() WHERE id=$1 AND status<>'completed'",
            reg_id, counts["pairs"], counts["cand"], counts["ctrl"],
            json.dumps({"source": "durable_worker", "job_id": str(job_id),
                        "campaign_id": str(job["campaign_id"]) if job["campaign_id"] else None}))
    elif job["status"] in (C.JOB_RUNNING, C.JOB_CANCEL_REQUESTED, C.JOB_FAILED, C.JOB_CANCELLED):
        # keep recoverable: mark executing (never completed) unless already completed
        await conn.execute(
            "UPDATE prospective_campaign_registrations SET status='executing', updated_at=NOW() "
            "WHERE id=$1 AND status IN ('registered','executing')", reg_id)


async def _campaign_counts(conn: asyncpg.Connection, run_id: Any) -> Dict[str, int]:
    row = await conn.fetchrow(
        "SELECT count(DISTINCT p.id)::int AS pairs,"
        " count(*) FILTER (WHERE e.arm_code=$2)::int AS cand,"
        " count(*) FILTER (WHERE e.arm_code=$3)::int AS ctrl "
        "FROM strategy_shadow_run_pairs rp "
        "JOIN strategy_shadow_pairs p ON p.id=rp.pair_id "
        "LEFT JOIN strategy_shadow_evaluations e ON e.pair_id=p.id "
        "WHERE rp.run_id=$1", run_id, pc.CANDIDATE_ARM_CODE, pc.CONTROL_ARM_CODE)
    return {"pairs": int(row["pairs"]), "cand": int(row["cand"]), "ctrl": int(row["ctrl"])}


__all__ = [
    "enqueue_prospective_campaign", "sync_prospective_campaign",
]
