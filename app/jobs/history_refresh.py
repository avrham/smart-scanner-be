"""Durable history-incremental-refresh job: enqueue + status (Root Cause A).

Makes the daily-pipeline ``history_refresh`` stage self-advancing WITHOUT giving
the pipeline driver a provider credential. When the frozen universe's daily/4H
history is stale, the driver (running ``advance_daily_pipeline_service``)
enqueues — or RECOGNIZES — exactly ONE bounded refresh job for the frozen
universe + resolved session: one job_runs row on the ``history_incremental_refresh``
queue with one task per universe symbol. A dedicated history-refresh worker (the
ONLY automated component besides the history-warmup HTTP app that holds the
Massive credential) claims each task and runs the provider-backed
``history_incremental_refresh_execute_service`` per symbol. The pipeline records
the child job id and WAITS (defers) until the child is terminal, then re-checks
readiness and advances. Both the job and the per-symbol tasks are idempotent, so
a replay never creates a duplicate provider batch, and a crash/restart recognizes
the same child job.

NO strategy math, NO provider construction, and NO bar writes happen here — this
module only enqueues/reads durable queue rows. The provider work lives entirely
in the history-refresh worker's child callable.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import asyncpg

from app.config import settings
from app.jobs import contracts as C
from app.jobs import identity as ident
from app.jobs import queue as Q

# --- durable-queue identifiers ---------------------------------------------
HISTORY_REFRESH_JOB_TYPE = "history_incremental_refresh"
HISTORY_REFRESH_QUEUE = "history_incremental_refresh"
HISTORY_REFRESH_TASK = "history_incremental_refresh_symbol.v1"
HISTORY_REFRESH_JOB_CONTRACT = "history_incremental_refresh_job.v1"
# Per-symbol provider refresh: a rate-limit / transient provider error is
# retryable; keep the bounded global two-retry budget (default schedule).
HISTORY_REFRESH_MAX_ATTEMPTS = int(getattr(settings, "JOB_MAX_ATTEMPTS_DEFAULT", 3))

# the refresh contract the worker runs (daily + 4H in one bounded unit)
HISTORY_REFRESH_CONTRACT_VERSION_V2 = "history_incremental_refresh.v2"


def _task_payload(*, symbol: str, universe_id: str, universe_hash: str,
                  resolved_session_date: str, contract_version: str) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "universe_id": str(universe_id),
        "universe_hash": str(universe_hash),
        "resolved_session_date": str(resolved_session_date),
        "contract_version": contract_version,
    }


async def enqueue_history_incremental_refresh(
        conn: asyncpg.Connection, *, universe_id: str, universe_hash: str,
        symbols: List[str], resolved_session_date: str,
        contract_version: str = HISTORY_REFRESH_CONTRACT_VERSION_V2,
        requested_by: Optional[str] = None) -> Dict[str, Any]:
    """Idempotently enqueue (or recognize) ONE bounded refresh job for the frozen
    universe + resolved session. One task per symbol; a symbol already current is
    a provider-free no-op in the worker. Returns ``{job_id, status, task_count}``
    with status ``queued`` | ``already_queued`` | ``already_applied``."""
    if not symbols:
        raise C.TerminalJobError("empty_universe", "frozen universe has no symbols")
    job_key = ident.history_refresh_job_idempotency_key(
        universe_hash=universe_hash, resolved_session_date=resolved_session_date,
        contract_version=contract_version)

    existing = await conn.fetchrow("SELECT * FROM job_runs WHERE idempotency_key=$1", job_key)
    if existing is not None:
        status = "already_applied" if existing["status"] == C.JOB_SUCCEEDED else "already_queued"
        return {"job_id": str(existing["id"]), "status": status,
                "job_status": existing["status"], "task_count": int(existing["total_task_count"] or 0)}

    async with conn.transaction():
        job = await conn.fetchrow(
            "INSERT INTO job_runs (job_type, job_contract_version, queue_name, idempotency_key,"
            " status, priority, requested_by, total_task_count, queued_task_count) "
            "VALUES ($1,$2,$3,$4,'queued',100,$5,$6,$6) "
            "ON CONFLICT (idempotency_key) DO NOTHING RETURNING *",
            HISTORY_REFRESH_JOB_TYPE, HISTORY_REFRESH_JOB_CONTRACT, HISTORY_REFRESH_QUEUE,
            job_key, requested_by, len(symbols))
        if job is None:  # concurrent enqueue won the race — recognize its job
            job = await conn.fetchrow("SELECT * FROM job_runs WHERE idempotency_key=$1", job_key)
            return {"job_id": str(job["id"]), "status": "already_queued",
                    "job_status": job["status"], "task_count": int(job["total_task_count"] or 0)}

        inserted = 0
        for ordinal, symbol in enumerate(symbols):
            payload = _task_payload(symbol=symbol, universe_id=universe_id,
                                    universe_hash=universe_hash,
                                    resolved_session_date=resolved_session_date,
                                    contract_version=contract_version)
            task_idem = ident.history_refresh_task_idempotency_key(
                universe_hash=universe_hash, resolved_session_date=resolved_session_date,
                symbol=symbol, contract_version=contract_version)
            row = await conn.fetchrow(
                "INSERT INTO job_tasks (job_id, queue_name, task_type, task_contract_version,"
                " task_key, ordinal, payload, payload_hash, idempotency_key, status, priority,"
                " max_attempts) "
                "VALUES ($1,$2,$3,$3,$4,$5,$6::jsonb,$7,$8,'queued',100,$9) "
                "ON CONFLICT DO NOTHING RETURNING id",
                job["id"], HISTORY_REFRESH_QUEUE, HISTORY_REFRESH_TASK, symbol, ordinal,
                json.dumps(payload), ident.payload_hash(payload), task_idem,
                HISTORY_REFRESH_MAX_ATTEMPTS)
            if row is not None:
                inserted += 1
        await Q.recompute_job_counters(conn, job["id"])
        await Q.record_event(conn, job_id=job["id"], event_type="job_created",
                             safe_message=requested_by or None,
                             metadata={"total_tasks": len(symbols), "inserted": inserted,
                                       "resolved_session_date": str(resolved_session_date)})
        return {"job_id": str(job["id"]), "status": "queued",
                "job_status": "queued", "task_count": len(symbols)}


async def history_refresh_job_status(conn: asyncpg.Connection, job_id: str) -> Optional[str]:
    """Current status of a history-refresh job (None if unknown)."""
    row = await conn.fetchrow(
        "SELECT status FROM job_runs WHERE id=$1 AND job_type=$2", job_id, HISTORY_REFRESH_JOB_TYPE)
    return row["status"] if row is not None else None


__all__ = [
    "HISTORY_REFRESH_JOB_TYPE", "HISTORY_REFRESH_QUEUE", "HISTORY_REFRESH_TASK",
    "HISTORY_REFRESH_JOB_CONTRACT", "HISTORY_REFRESH_MAX_ATTEMPTS",
    "HISTORY_REFRESH_CONTRACT_VERSION_V2",
    "enqueue_history_incremental_refresh", "history_refresh_job_status",
]
