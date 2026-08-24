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

BOUNDED RECOVERY: when a history-refresh job FAILS with retry-exhausted
*retryable* work (e.g. every remaining task hitting the shared cooldown 409),
``enqueue_history_incremental_refresh`` creates exactly ONE bounded recovery
SUCCESSOR — a NEW durable job with a DISTINCT generation-scoped idempotency
identity (``recovery_generation`` >= 1), covering ONLY the symbols the
predecessor did not complete, carrying predecessor lineage in its
``result_summary``. The predecessor job/task/attempt rows are NEVER mutated
(immutable evidence). Recovery is capped at ONE generation; a failed successor is
not auto-recovered (the pipeline then fails closed). Terminal / operator /
cancelled failures are never auto-recovered. This is the automatic, evidence-
preserving alternative to DB surgery on the failed rows.
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
# Per-symbol provider refresh keeps the bounded global two-retry budget (default
# schedule) for GENUINE provider errors (e.g. a 429 rate-limit). It is NOT relied
# on for the shared history-warmup execution cooldown / lock: that KNOWN transient
# 409 is absorbed by a bounded in-task wait in history_refresh_worker (see
# HISTORY_REFRESH_TASK_MAX_WAIT_SECONDS), so a 25-symbol job never exhausts these
# attempts just because symbols wake into the same cooldown window.
HISTORY_REFRESH_MAX_ATTEMPTS = int(getattr(settings, "JOB_MAX_ATTEMPTS_DEFAULT", 3))

# the refresh contract the worker runs (daily + 4H in one bounded unit)
HISTORY_REFRESH_CONTRACT_VERSION_V2 = "history_incremental_refresh.v2"


# Bounded automatic recovery: at most ONE recovery successor generation for an
# initial (generation-0) history-refresh job. A failed generation-1 successor is
# NOT auto-recovered — the pipeline then fails closed and needs intervention.
MAX_HISTORY_REFRESH_RECOVERY_GENERATION = 1


def _task_payload(*, symbol: str, universe_id: str, universe_hash: str,
                  resolved_session_date: str, contract_version: str,
                  recovery_generation: int = 0) -> Dict[str, Any]:
    p = {
        "symbol": symbol,
        "universe_id": str(universe_id),
        "universe_hash": str(universe_hash),
        "resolved_session_date": str(resolved_session_date),
        "contract_version": contract_version,
    }
    if int(recovery_generation):
        p["recovery_generation"] = int(recovery_generation)
    return p


def _job_key(universe_hash, resolved_session_date, contract_version, generation):
    return ident.history_refresh_job_idempotency_key(
        universe_hash=universe_hash, resolved_session_date=resolved_session_date,
        contract_version=contract_version, recovery_generation=generation)


def _response(job, status, *, recoverable=None, extra=None):
    out = {"job_id": str(job["id"]), "status": status, "job_status": job["status"],
           "task_count": int(job["total_task_count"] or 0)}
    if recoverable is not None:
        out["recoverable"] = recoverable
    if extra:
        out.update(extra)
    return out


async def _create_job_and_tasks(conn, *, job_key, universe_id, universe_hash, symbols,
                                resolved_session_date, contract_version, requested_by,
                                generation, lineage):
    """INSERT one job (with bounded lineage in result_summary) + one task per
    symbol, task keys scoped by ``generation`` so a successor never collides with
    a predecessor. Idempotent: a concurrent creator is recognized via ON CONFLICT."""
    job = await conn.fetchrow(
        "INSERT INTO job_runs (job_type, job_contract_version, queue_name, idempotency_key,"
        " status, priority, requested_by, total_task_count, queued_task_count, result_summary) "
        "VALUES ($1,$2,$3,$4,'queued',100,$5,$6,$6,$7::jsonb) "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING *",
        HISTORY_REFRESH_JOB_TYPE, HISTORY_REFRESH_JOB_CONTRACT, HISTORY_REFRESH_QUEUE,
        job_key, requested_by, len(symbols), json.dumps(lineage))
    if job is None:  # concurrent creator won the race — recognize it (exactly one)
        job = await conn.fetchrow("SELECT * FROM job_runs WHERE idempotency_key=$1", job_key)
        return job, False
    for ordinal, symbol in enumerate(symbols):
        payload = _task_payload(symbol=symbol, universe_id=universe_id, universe_hash=universe_hash,
                                resolved_session_date=resolved_session_date,
                                contract_version=contract_version, recovery_generation=generation)
        task_idem = ident.history_refresh_task_idempotency_key(
            universe_hash=universe_hash, resolved_session_date=resolved_session_date,
            symbol=symbol, contract_version=contract_version, recovery_generation=generation)
        await conn.execute(
            "INSERT INTO job_tasks (job_id, queue_name, task_type, task_contract_version,"
            " task_key, ordinal, payload, payload_hash, idempotency_key, status, priority, max_attempts) "
            "VALUES ($1,$2,$3,$3,$4,$5,$6::jsonb,$7,$8,'queued',100,$9) ON CONFLICT DO NOTHING",
            job["id"], HISTORY_REFRESH_QUEUE, HISTORY_REFRESH_TASK, symbol, ordinal,
            json.dumps(payload), ident.payload_hash(payload), task_idem, HISTORY_REFRESH_MAX_ATTEMPTS)
    await Q.recompute_job_counters(conn, job["id"])
    await Q.record_event(conn, job_id=job["id"], event_type="job_created",
                         safe_message=requested_by or None,
                         metadata={"total_tasks": len(symbols), "generation": generation,
                                   "predecessor_history_job_id": lineage.get("predecessor_history_job_id"),
                                   "resolved_session_date": str(resolved_session_date)})
    return job, True


async def _recovery_eligibility(conn, predecessor_job_id):
    """A failed history-refresh job is auto-recoverable ONLY if every non-succeeded
    task failed with a RETRYABLE class (retry-exhausted retryable work). Terminal /
    operator / cancelled failures are NOT auto-recovered. Returns
    (recoverable, symbols_needing_refresh, reason)."""
    rows = await conn.fetch(
        "SELECT task_key, status, error_class FROM job_tasks WHERE job_id=$1", predecessor_job_id)
    non_succeeded = [r["task_key"] for r in rows if r["status"] != C.TASK_SUCCEEDED]
    if not non_succeeded:
        return False, [], "nothing_to_recover"
    blocking = [r for r in rows if r["status"] != C.TASK_SUCCEEDED
                and r["error_class"] != C.ERR_RETRYABLE]
    if blocking:
        return False, non_succeeded, "non_recoverable_failure"
    return True, sorted(non_succeeded), "retryable_exhausted"


async def enqueue_history_incremental_refresh(
        conn: asyncpg.Connection, *, universe_id: str, universe_hash: str,
        symbols: List[str], resolved_session_date: str,
        contract_version: str = HISTORY_REFRESH_CONTRACT_VERSION_V2,
        requested_by: Optional[str] = None) -> Dict[str, Any]:
    """Enqueue-or-recognize the bounded refresh work for the frozen universe +
    resolved session, generation-aware and idempotent. Behaviour by the state of
    the highest existing generation for this logical identity:

    * none exist            → create generation-0 (one task per symbol) → ``queued``
    * succeeded             → ``already_applied``
    * queued/running        → ``already_queued`` (in flight — the pipeline waits)
    * failed + recoverable  → create/recognize the next generation SUCCESSOR
      (tasks ONLY for symbols the predecessor did not complete) → ``queued`` /
      ``already_queued`` with ``recoverable=True`` + lineage
    * failed + not recoverable / recovery budget exhausted / cancelled →
      ``not_recoverable`` / ``recovery_exhausted`` with ``recoverable=False``

    The predecessor job/task/attempt rows are NEVER mutated — a successor is a
    NEW durable job with a DISTINCT generation-scoped idempotency identity.
    """
    if not symbols:
        raise C.TerminalJobError("empty_universe", "frozen universe has no symbols")

    logical = {"universe_hash": str(universe_hash),
               "resolved_session_date": str(resolved_session_date),
               "contract_version": contract_version}

    # highest existing generation for this logical identity (probe 0..MAX)
    current = None
    current_gen = -1
    for gen in range(0, MAX_HISTORY_REFRESH_RECOVERY_GENERATION + 1):
        row = await conn.fetchrow(
            "SELECT * FROM job_runs WHERE idempotency_key=$1",
            _job_key(universe_hash, resolved_session_date, contract_version, gen))
        if row is not None:
            current, current_gen = row, gen

    if current is None:
        async with conn.transaction():
            job, _created = await _create_job_and_tasks(
                conn, job_key=_job_key(universe_hash, resolved_session_date, contract_version, 0),
                universe_id=universe_id, universe_hash=universe_hash, symbols=list(symbols),
                resolved_session_date=resolved_session_date, contract_version=contract_version,
                requested_by=requested_by, generation=0,
                lineage={"recovery_generation": 0, "logical_identity": logical})
        return _response(job, "queued" if _created else "already_queued", recoverable=True,
                         extra={"recovery_generation": 0})

    if current["status"] == C.JOB_SUCCEEDED:
        return _response(current, "already_applied", recoverable=True,
                         extra={"recovery_generation": current_gen})
    if current["status"] in (C.JOB_QUEUED, C.JOB_RUNNING):
        return _response(current, "already_queued", recoverable=True,
                         extra={"recovery_generation": current_gen})

    # current is terminal-failed (or cancelled) → consider a bounded successor.
    if current["status"] != C.JOB_FAILED:
        return _response(current, "not_recoverable", recoverable=False,
                         extra={"recovery_generation": current_gen, "reason": str(current["status"])})
    if current_gen >= MAX_HISTORY_REFRESH_RECOVERY_GENERATION:
        return _response(current, "recovery_exhausted", recoverable=False,
                         extra={"recovery_generation": current_gen})
    recoverable, needing, reason = await _recovery_eligibility(conn, current["id"])
    if not recoverable:
        return _response(current, "not_recoverable", recoverable=False,
                         extra={"recovery_generation": current_gen, "reason": reason})

    succ_gen = current_gen + 1
    async with conn.transaction():
        job, created = await _create_job_and_tasks(
            conn, job_key=_job_key(universe_hash, resolved_session_date, contract_version, succ_gen),
            universe_id=universe_id, universe_hash=universe_hash, symbols=needing,
            resolved_session_date=resolved_session_date, contract_version=contract_version,
            requested_by=requested_by, generation=succ_gen,
            lineage={"recovery_generation": succ_gen,
                     "predecessor_history_job_id": str(current["id"]),
                     "logical_identity": logical, "recovery_reason": reason})
    return _response(job, "queued" if created else "already_queued", recoverable=True,
                     extra={"recovery_generation": succ_gen,
                            "predecessor_history_job_id": str(current["id"])})


async def history_refresh_job_status(conn: asyncpg.Connection, job_id: str) -> Optional[str]:
    """Current status of a history-refresh job (None if unknown)."""
    row = await conn.fetchrow(
        "SELECT status FROM job_runs WHERE id=$1 AND job_type=$2", job_id, HISTORY_REFRESH_JOB_TYPE)
    return row["status"] if row is not None else None


__all__ = [
    "HISTORY_REFRESH_JOB_TYPE", "HISTORY_REFRESH_QUEUE", "HISTORY_REFRESH_TASK",
    "HISTORY_REFRESH_JOB_CONTRACT", "HISTORY_REFRESH_MAX_ATTEMPTS",
    "HISTORY_REFRESH_CONTRACT_VERSION_V2", "MAX_HISTORY_REFRESH_RECOVERY_GENERATION",
    "enqueue_history_incremental_refresh", "history_refresh_job_status",
]
