"""Generic durable-queue service — NO task-specific strategy logic.

PostgreSQL is the source of truth. Task claiming is one SHORT transaction using
SELECT ... FOR UPDATE SKIP LOCKED; the CPU work runs OUTSIDE any transaction.
All functions take an asyncpg connection so callers control transaction scope
and the connection's role (least-privilege). Every state transition writes a
bounded, secret-free job_events row.

This module knows about job_runs / job_tasks / job_task_attempts / job_events /
job_workers only. Campaign/registration coupling lives in the prospective layer.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import asyncpg

from app.jobs import contracts as C


# --------------------------------------------------------------------------
# events + counters
# --------------------------------------------------------------------------
async def record_event(conn: asyncpg.Connection, *, event_type: str,
                       job_id: Optional[str] = None, task_id: Optional[str] = None,
                       safe_message: Optional[str] = None,
                       metadata: Optional[Dict[str, Any]] = None) -> None:
    """Append a bounded, secret-free state-transition event."""
    await conn.execute(
        "INSERT INTO job_events (job_id, task_id, event_type, safe_message, metadata) "
        "VALUES ($1, $2, $3, $4, $5::jsonb)",
        job_id, task_id, event_type[:60],
        (safe_message or None) if safe_message is None else safe_message[:500],
        json.dumps(metadata) if metadata is not None else None,
    )


async def recompute_job_counters(conn: asyncpg.Connection, job_id: str) -> Dict[str, Any]:
    """Recount tasks from the durable table and derive the parent job status.

    Idempotent: safe to call after every task transition. Never marks a job
    ``succeeded`` while a task is non-terminal; a job with any terminally-failed
    task settles to ``failed`` once all non-cancelled work is done; a
    cancel-requested job settles to ``cancelled`` once all tasks are terminal.
    """
    row = await conn.fetchrow(
        "SELECT "
        " count(*) FILTER (WHERE status='queued')      AS queued,"
        " count(*) FILTER (WHERE status='leased')      AS leased,"
        " count(*) FILTER (WHERE status='running')     AS running,"
        " count(*) FILTER (WHERE status='retryable')   AS retryable,"
        " count(*) FILTER (WHERE status='succeeded')   AS succeeded,"
        " count(*) FILTER (WHERE status='failed')      AS failed,"
        " count(*) FILTER (WHERE status='cancelled')   AS cancelled,"
        " count(*)                                      AS total "
        "FROM job_tasks WHERE job_id=$1", job_id)
    c = {k: int(row[k]) for k in row.keys()}
    job = await conn.fetchrow(
        "SELECT status, cancel_requested_at, started_at FROM job_runs WHERE id=$1", job_id)
    active = c["queued"] + c["leased"] + c["running"] + c["retryable"]
    terminal = c["succeeded"] + c["failed"] + c["cancelled"]

    cancel_requested = job["cancel_requested_at"] is not None
    if cancel_requested:
        new_status = C.JOB_CANCELLED if active == 0 else C.JOB_CANCEL_REQUESTED
    elif c["total"] == 0:
        new_status = C.JOB_QUEUED
    elif active == 0:
        new_status = C.JOB_FAILED if c["failed"] > 0 else C.JOB_SUCCEEDED
    elif c["leased"] + c["running"] + c["succeeded"] + c["failed"] + c["cancelled"] > 0:
        new_status = C.JOB_RUNNING
    else:
        new_status = C.JOB_QUEUED

    finished_clause = ""
    if new_status in C.JOB_TERMINAL:
        finished_clause = ", finished_at = COALESCE(finished_at, NOW())"
    started_clause = ""
    if new_status == C.JOB_RUNNING and job["started_at"] is None:
        started_clause = ", started_at = COALESCE(started_at, NOW())"
    await conn.execute(
        "UPDATE job_runs SET queued_task_count=$2, running_task_count=$3,"
        " succeeded_task_count=$4, retryable_task_count=$5, failed_task_count=$6,"
        " cancelled_task_count=$7, status=$8, updated_at=NOW()"
        + finished_clause + started_clause +
        " WHERE id=$1",
        job_id, c["queued"], c["leased"] + c["running"], c["succeeded"],
        c["retryable"], c["failed"], c["cancelled"], new_status)
    c["status"] = new_status
    return c


# --------------------------------------------------------------------------
# claiming (FOR UPDATE SKIP LOCKED) — one short transaction
# --------------------------------------------------------------------------
async def claim_next_task(conn: asyncpg.Connection, *, queue_name: str,
                          worker_id: str, lease_seconds: int) -> Optional[Dict[str, Any]]:
    """Atomically claim exactly one claimable task from ``queue_name``.

    In ONE short transaction: SELECT ... FOR UPDATE SKIP LOCKED the highest-
    priority ready task, increment its attempt, set it leased with a lease
    owner+expiry, append the attempt row, and refresh the parent job counters.
    The caller performs CPU work AFTER this returns (never inside a txn).
    Returns the claimed task as a dict, or None when the queue has no ready work.
    """
    async with conn.transaction():
        row = await conn.fetchrow(
            "SELECT id FROM job_tasks "
            "WHERE queue_name = $1 AND status IN ('queued','retryable') "
            "  AND available_at <= NOW() "
            "ORDER BY priority DESC, ordinal ASC, created_at ASC "
            "FOR UPDATE SKIP LOCKED LIMIT 1",
            queue_name)
        if row is None:
            return None
        task_id = row["id"]
        task = await conn.fetchrow(
            "UPDATE job_tasks SET "
            " status='leased', attempt_count = attempt_count + 1,"
            " lease_owner=$2, lease_acquired_at=NOW(),"
            " lease_expires_at = NOW() + ($3 || ' seconds')::interval,"
            " heartbeat_at=NOW(), started_at = COALESCE(started_at, NOW()),"
            " updated_at=NOW() "
            "WHERE id=$1 RETURNING *", task_id, worker_id, str(int(lease_seconds)))
        await conn.execute(
            "INSERT INTO job_task_attempts (task_id, attempt_number, worker_id, status, started_at) "
            "VALUES ($1, $2, $3, 'leased', NOW()) "
            "ON CONFLICT (task_id, attempt_number) DO NOTHING",
            task_id, int(task["attempt_count"]), worker_id)
        await conn.execute(
            "UPDATE job_workers SET status='busy', current_task_id=$2, last_heartbeat_at=NOW() "
            "WHERE worker_id=$1", worker_id, task_id)
        await recompute_job_counters(conn, task["job_id"])
        await record_event(conn, job_id=task["job_id"], task_id=task_id,
                           event_type="task_leased",
                           safe_message=f"attempt {task['attempt_count']}",
                           metadata={"worker_id": worker_id,
                                     "attempt": int(task["attempt_count"])})
        return dict(task)


async def mark_running(conn: asyncpg.Connection, *, task_id: str, worker_id: str) -> bool:
    """Transition a leased task owned by ``worker_id`` to running. Returns False
    if the lease was lost (owner changed / reclaimed)."""
    row = await conn.fetchrow(
        "UPDATE job_tasks SET status='running', heartbeat_at=NOW(), updated_at=NOW() "
        "WHERE id=$1 AND lease_owner=$2 AND status IN ('leased','running') RETURNING id",
        task_id, worker_id)
    return row is not None


# --------------------------------------------------------------------------
# lease renewal + heartbeat (called by the parent while CPU work runs)
# --------------------------------------------------------------------------
async def renew_lease(conn: asyncpg.Connection, *, task_id: str, worker_id: str,
                      lease_seconds: int) -> bool:
    """Extend the lease + task heartbeat. Returns False if the lease was lost
    (another worker reclaimed it after expiry) — the caller must abandon."""
    row = await conn.fetchrow(
        "UPDATE job_tasks SET heartbeat_at=NOW(),"
        " lease_expires_at = NOW() + ($3 || ' seconds')::interval, updated_at=NOW() "
        "WHERE id=$1 AND lease_owner=$2 AND status IN ('leased','running') RETURNING id",
        task_id, worker_id, str(int(lease_seconds)))
    return row is not None


async def worker_heartbeat(conn: asyncpg.Connection, *, worker_id: str,
                           status: Optional[str] = None,
                           current_task_id: Optional[str] = None,
                           draining: Optional[bool] = None) -> None:
    sets = ["last_heartbeat_at=NOW()"]
    args: List[Any] = [worker_id]
    if status is not None:
        args.append(status); sets.append(f"status=${len(args)}")
    if current_task_id is not None:
        args.append(current_task_id); sets.append(f"current_task_id=${len(args)}")
    if draining is not None:
        args.append(draining); sets.append(f"draining=${len(args)}")
    await conn.execute(
        f"UPDATE job_workers SET {', '.join(sets)} WHERE worker_id=$1", *args)


# --------------------------------------------------------------------------
# task finalization
# --------------------------------------------------------------------------
async def _record_attempt_end(conn: asyncpg.Connection, *, task_id: str,
                              attempt_number: int, status: str, safe_error_code: Optional[str],
                              duration_ms: Optional[int], lease_lost: bool,
                              result_summary: Optional[Dict[str, Any]]) -> None:
    await conn.execute(
        "UPDATE job_task_attempts SET status=$3, finished_at=NOW(), safe_error_code=$4,"
        " duration_ms=$5, lease_lost=$6, result_summary=$7::jsonb "
        "WHERE task_id=$1 AND attempt_number=$2",
        task_id, attempt_number, status, safe_error_code, duration_ms, lease_lost,
        json.dumps(result_summary) if result_summary is not None else None)


async def complete_task_succeeded(conn: asyncpg.Connection, *, task_id: str, worker_id: str,
                                  result_summary: Dict[str, Any],
                                  duration_ms: Optional[int] = None) -> Dict[str, Any]:
    async with conn.transaction():
        task = await conn.fetchrow(
            "UPDATE job_tasks SET status='succeeded', finished_at=NOW(), heartbeat_at=NOW(),"
            " safe_error_code=NULL, error_class=NULL, result_summary=$2::jsonb, updated_at=NOW() "
            "WHERE id=$1 RETURNING job_id, attempt_count",
            task_id, json.dumps(result_summary))
        await _record_attempt_end(conn, task_id=task_id, attempt_number=int(task["attempt_count"]),
                                  status='succeeded', safe_error_code=None, duration_ms=duration_ms,
                                  lease_lost=False, result_summary=result_summary)
        await conn.execute(
            "UPDATE job_workers SET status='idle', current_task_id=NULL, last_heartbeat_at=NOW() "
            "WHERE worker_id=$1", worker_id)
        counters = await recompute_job_counters(conn, task["job_id"])
        await record_event(conn, job_id=task["job_id"], task_id=task_id,
                           event_type="task_succeeded",
                           metadata={"duration_ms": duration_ms})
        counters["job_id"] = str(task["job_id"])
        return counters


async def settle_task_failure(conn: asyncpg.Connection, *, task_id: str, worker_id: str,
                              safe_error_code: str, error_class: str,
                              backoff_seconds_value: Optional[int],
                              lease_lost: bool = False,
                              duration_ms: Optional[int] = None) -> Dict[str, Any]:
    """Route a failed attempt: retryable (with backoff) while attempts remain and
    the class is retryable; otherwise terminal (failed). Records the attempt end
    + a bounded event. ``operator_error`` is terminal-now but marked operator-
    retry-eligible so an operator can re-drive it later."""
    async with conn.transaction():
        cur = await conn.fetchrow(
            "SELECT t.job_id, t.attempt_count, t.max_attempts, j.cancel_requested_at "
            "FROM job_tasks t JOIN job_runs j ON j.id=t.job_id WHERE t.id=$1", task_id)
        job_id = cur["job_id"]
        attempt = int(cur["attempt_count"])
        # A cancel-requested job cancels rather than retries outstanding work.
        if cur["cancel_requested_at"] is not None:
            await conn.execute(
                "UPDATE job_tasks SET status='cancelled', finished_at=NOW(), heartbeat_at=NOW(),"
                " safe_error_code=$2, error_class='cancelled', updated_at=NOW() WHERE id=$1",
                task_id, safe_error_code[:80])
            await _record_attempt_end(conn, task_id=task_id, attempt_number=attempt,
                                      status='cancelled', safe_error_code=safe_error_code[:80],
                                      duration_ms=duration_ms, lease_lost=lease_lost,
                                      result_summary=None)
            event = "task_cancelled"
            new_status = C.TASK_CANCELLED
        else:
            can_retry = (error_class == C.ERR_RETRYABLE
                         and attempt < int(cur["max_attempts"])
                         and backoff_seconds_value is not None)
            if can_retry:
                await conn.execute(
                    "UPDATE job_tasks SET status='retryable', lease_owner=NULL,"
                    " lease_expires_at=NULL, heartbeat_at=NOW(), safe_error_code=$2,"
                    " error_class=$3, available_at = NOW() + ($4 || ' seconds')::interval,"
                    " updated_at=NOW() WHERE id=$1",
                    task_id, safe_error_code[:80], error_class, str(int(backoff_seconds_value)))
                await _record_attempt_end(conn, task_id=task_id, attempt_number=attempt,
                                          status='retryable', safe_error_code=safe_error_code[:80],
                                          duration_ms=duration_ms, lease_lost=lease_lost,
                                          result_summary=None)
                event = "task_retry_scheduled"
                new_status = C.TASK_RETRYABLE
            else:
                operator_eligible = error_class in (C.ERR_OPERATOR,)
                await conn.execute(
                    "UPDATE job_tasks SET status='failed', finished_at=NOW(), heartbeat_at=NOW(),"
                    " lease_owner=NULL, lease_expires_at=NULL, safe_error_code=$2, error_class=$3,"
                    " operator_retry_eligible=$4, updated_at=NOW() WHERE id=$1",
                    task_id, safe_error_code[:80], error_class, operator_eligible)
                await _record_attempt_end(conn, task_id=task_id, attempt_number=attempt,
                                          status='failed', safe_error_code=safe_error_code[:80],
                                          duration_ms=duration_ms, lease_lost=lease_lost,
                                          result_summary=None)
                event = "task_failed"
                new_status = C.TASK_FAILED
        await conn.execute(
            "UPDATE job_workers SET status='idle', current_task_id=NULL, last_heartbeat_at=NOW() "
            "WHERE worker_id=$1", worker_id)
        counters = await recompute_job_counters(conn, job_id)
        await record_event(conn, job_id=job_id, task_id=task_id, event_type=event,
                           safe_message=safe_error_code[:80],
                           metadata={"error_class": error_class, "attempt": attempt,
                                     "lease_lost": lease_lost,
                                     "backoff_seconds": backoff_seconds_value})
        counters["task_status"] = new_status
        counters["job_id"] = str(job_id)
        return counters


# --------------------------------------------------------------------------
# expired-lease reconciliation (crash recovery)
# --------------------------------------------------------------------------
async def find_expired_lease_tasks(conn: asyncpg.Connection, *, queue_name: str,
                                   limit: int = 25) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        "SELECT * FROM job_tasks WHERE queue_name=$1 AND status IN ('leased','running') "
        "AND lease_expires_at IS NOT NULL AND lease_expires_at < NOW() "
        "ORDER BY lease_expires_at ASC LIMIT $2", queue_name, int(limit))
    return [dict(r) for r in rows]


async def reconcile_task_to_retryable(conn: asyncpg.Connection, *, task_id: str,
                                      safe_error_code: str = "lease_expired",
                                      backoff_seconds_value: Optional[int] = 0) -> None:
    """An expired-lease task with NO complete durable output → retryable (or
    failed if attempts are exhausted). Records a lease-lost attempt end."""
    async with conn.transaction():
        cur = await conn.fetchrow(
            "SELECT job_id, attempt_count, max_attempts FROM job_tasks WHERE id=$1", task_id)
        if cur is None:
            return
        attempt = int(cur["attempt_count"])
        retry = attempt < int(cur["max_attempts"])
        if retry:
            await conn.execute(
                "UPDATE job_tasks SET status='retryable', lease_owner=NULL, lease_expires_at=NULL,"
                " safe_error_code=$2, error_class='retryable',"
                " available_at = NOW() + ($3 || ' seconds')::interval, updated_at=NOW() "
                "WHERE id=$1 AND status IN ('leased','running')",
                task_id, safe_error_code[:80], str(int(backoff_seconds_value or 0)))
            status = 'retryable'
        else:
            await conn.execute(
                "UPDATE job_tasks SET status='failed', finished_at=NOW(), lease_owner=NULL,"
                " lease_expires_at=NULL, safe_error_code=$2, error_class='retryable',"
                " operator_retry_eligible=TRUE, updated_at=NOW() "
                "WHERE id=$1 AND status IN ('leased','running')",
                task_id, safe_error_code[:80])
            status = 'failed'
        await _record_attempt_end(conn, task_id=task_id, attempt_number=attempt,
                                  status=status, safe_error_code=safe_error_code[:80],
                                  duration_ms=None, lease_lost=True, result_summary=None)
        await recompute_job_counters(conn, cur["job_id"])
        await record_event(conn, job_id=cur["job_id"], task_id=task_id,
                           event_type="lease_expired_reconciled",
                           safe_message=safe_error_code[:80],
                           metadata={"reconciled_to": status, "lease_lost": True})


async def reconcile_task_to_succeeded(conn: asyncpg.Connection, *, task_id: str,
                                      result_summary: Dict[str, Any]) -> None:
    """An expired-lease task whose durable output is already COMPLETE (pair +
    both arms) → succeeded by reconciliation, WITHOUT any recomputation."""
    async with conn.transaction():
        cur = await conn.fetchrow(
            "SELECT job_id, attempt_count FROM job_tasks WHERE id=$1", task_id)
        if cur is None:
            return
        await conn.execute(
            "UPDATE job_tasks SET status='succeeded', finished_at=NOW(), lease_owner=NULL,"
            " lease_expires_at=NULL, safe_error_code=NULL, error_class=NULL,"
            " result_summary=$2::jsonb, updated_at=NOW() WHERE id=$1",
            task_id, json.dumps(result_summary))
        await _record_attempt_end(conn, task_id=task_id, attempt_number=int(cur["attempt_count"]),
                                  status='succeeded', safe_error_code=None, duration_ms=None,
                                  lease_lost=True, result_summary=result_summary)
        await recompute_job_counters(conn, cur["job_id"])
        await record_event(conn, job_id=cur["job_id"], task_id=task_id,
                           event_type="lease_expired_reconciled",
                           safe_message="reconciled_to_succeeded",
                           metadata={"reconciled_to": "succeeded", "recomputed": False})


# --------------------------------------------------------------------------
# cancellation + operator retry
# --------------------------------------------------------------------------
async def request_cancel(conn: asyncpg.Connection, *, job_id: str,
                         requested_by: Optional[str] = None) -> Dict[str, Any]:
    """Set cancel_requested, stop claiming, and cancel remaining queued/retryable
    tasks. A currently-running task is allowed to finish its atomic unit; its
    persisted output is never deleted."""
    async with conn.transaction():
        job = await conn.fetchrow("SELECT id, status FROM job_runs WHERE id=$1 FOR UPDATE", job_id)
        if job is None:
            return {"found": False}
        if job["status"] in C.JOB_TERMINAL:
            return {"found": True, "status": job["status"], "already_terminal": True}
        await conn.execute(
            "UPDATE job_runs SET cancel_requested_at = COALESCE(cancel_requested_at, NOW()),"
            " status='cancel_requested', updated_at=NOW() WHERE id=$1", job_id)
        cancelled = await conn.fetch(
            "UPDATE job_tasks SET status='cancelled', finished_at=NOW(), lease_owner=NULL,"
            " lease_expires_at=NULL, error_class='cancelled', safe_error_code='job_cancelled',"
            " updated_at=NOW() WHERE job_id=$1 AND status IN ('queued','retryable') RETURNING id",
            job_id)
        for r in cancelled:
            await record_event(conn, job_id=job_id, task_id=r["id"], event_type="task_cancelled",
                               safe_message="job_cancelled")
        counters = await recompute_job_counters(conn, job_id)
        await record_event(conn, job_id=job_id, event_type="job_cancel_requested",
                           safe_message=requested_by or None,
                           metadata={"cancelled_pending": len(cancelled)})
        counters["cancelled_pending"] = len(cancelled)
        return {"found": True, **counters}


async def retry_failed_tasks(conn: asyncpg.Connection, *, job_id: str,
                             requested_by: Optional[str] = None) -> Dict[str, Any]:
    """Operator retry: re-queue ONLY terminally-failed tasks explicitly marked
    operator-retry-eligible. Immutable payload is preserved; attempt history is
    preserved (a new attempt row is created when the task is next claimed);
    max_attempts is bumped so the retry has a budget."""
    async with conn.transaction():
        job = await conn.fetchrow("SELECT id, status FROM job_runs WHERE id=$1 FOR UPDATE", job_id)
        if job is None:
            return {"found": False}
        rows = await conn.fetch(
            "UPDATE job_tasks SET status='retryable', available_at=NOW(), lease_owner=NULL,"
            " lease_expires_at=NULL, safe_error_code=NULL, error_class=NULL,"
            " max_attempts = attempt_count + 1, finished_at=NULL, updated_at=NOW() "
            "WHERE job_id=$1 AND status='failed' AND operator_retry_eligible=TRUE RETURNING id",
            job_id)
        for r in rows:
            await record_event(conn, job_id=job_id, task_id=r["id"],
                               event_type="task_operator_retry",
                               safe_message=requested_by or None)
        counters = await recompute_job_counters(conn, job_id)
        await record_event(conn, job_id=job_id, event_type="job_retry_failed",
                           metadata={"retried": len(rows)})
        counters["retried"] = len(rows)
        counters["found"] = True
        return counters


__all__ = [
    "record_event", "recompute_job_counters", "claim_next_task", "mark_running",
    "renew_lease", "worker_heartbeat", "complete_task_succeeded", "settle_task_failure",
    "find_expired_lease_tasks", "reconcile_task_to_retryable", "reconcile_task_to_succeeded",
    "request_cancel", "retry_failed_tasks",
]
