"""Durable-queue identity for research outcome maturation.

WHY A SEPARATE JOB AND NOT A LIFECYCLE STAGE
--------------------------------------------
The research lifecycle looks FORWARD: discover, admit, warm, scan, classify.
Outcome maturation looks BACKWARD: score what an earlier session predicted.
They share a pool and nothing else, and welding them together would put the
older work in a position to stop the newer.

That is not hypothetical here. The lifecycle already blocks on core-history
freshness, already takes the machine-wide warmup lock, and already defers
itself up to three times waiting for a refresh it asked for. Adding a stage
that reads twenty-session-old bars for eighty symbols would give a missing 10D
bar the power to defer today's discovery — the exact inversion the brief
forbids in one line: *outcome maturation should not prevent today's research
run from completing*.

As a separate job it cannot do that — but only in one of the two senses of
"isolation", and the distinction is worth writing down because the first cut of
this file blurred them.

FAILURE ISOLATION IS COMPLETE. Own job_run, own task, own attempt budget, own
schedule, own run row. A total failure of every outcome attempt leaves
`research_lifecycle_runs` untouched and its counters correct; there is no code
path from an outcome error to a lifecycle status, and no shared row to corrupt.

RESOURCE ISOLATION IS NOT, AND CANNOT BE HERE. The research worker runs at
concurrency 1 and the parent renews a running task's lease indefinitely
(`app/jobs/worker.py` heartbeats every JOB_TASK_HEARTBEAT_SECONDS with no child
wall clock), so whichever task is executing holds the single executor until it
returns. An outcome pass therefore DELAYS a concurrent lifecycle task by its own
duration. A separate queue would not change that — same worker, same executor —
and the queue is an identity boundary rather than a scheduling one.

What bounds the delay is the WORK, and it is bounded by construction: at most
`scan_limit` (<= 200) scans x 5 inserts plus `observation_limit` (<= 400)
observations x 6 small point queries, every one of them capped at 120 s by the
role's `statement_timeout`. Measured in staging on 2026-09-06: 0.68 s and
1.23 s for the full pass. The schedules are three hours apart, so the ordinary
case is no overlap at all; the residual is a lifecycle RETRY (30-minute backoff)
landing on the same minute, which waits one outcome pass.

WHY IT RIDES THE `research_lifecycle` QUEUE ANYWAY
--------------------------------------------------
Because the queue is the EXECUTION BOUNDARY — which identity may claim this
work — and this work needs exactly the identity the lifecycle already has:
read `research_scan_results`, read `daily_bars` (including the benchmark,
which the role's SELECT policy already permits in full), write research
tables, touch nothing canonical, hold no provider credential it will use.

A second queue would mean a second RLS predicate on `job_runs`/`job_tasks`, a
second entry in the worker's `JOB_WORKER_QUEUES`, and a configuration redeploy
— all to express a boundary that is already drawn in the right place. It would
also buy no isolation: the research worker runs at concurrency 1, so two queues
on one worker still means one task at a time. Isolation comes from the job, not
from the lane.

WHY ONE TASK AND NOT ONE PER OBSERVATION
----------------------------------------
The work is local reads and small writes with no provider budget to spend and
no ordering between observations. Fanning it out per (scan, horizon) would
create hundreds of tasks whose combined runtime is less than the queue
overhead of creating them, and would give up the one thing a single task
provides for free: a single bounded run row that says what the whole pass did.

IDEMPOTENCY
-----------
The task key is derived from (schedule, occurrence) — or, for a manual run,
from an operator label plus the wall clock — and becomes the run key. Two fires
of one occurrence produce one run. And the measurement itself is idempotent
underneath that: a measured outcome is frozen by a database trigger, so even a
task that ran twice in full cannot produce a second, different answer.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg

from app.jobs import identity as ident
from app.jobs import queue as Q
from app.jobs import research_lifecycle as RL
from app.research_outcomes import (DEFAULT_OBSERVATION_LIMIT,
                                   DEFAULT_SCAN_LIMIT)

RESEARCH_OUTCOMES_JOB_TYPE = "smart_scanner_research_outcomes.v1"
RESEARCH_OUTCOMES_JOB_CONTRACT = "smart_scanner_research_outcomes.v1"
RESEARCH_OUTCOMES_TASK = "smart_scanner_research_outcome_maturation.v1"
RESEARCH_OUTCOMES_SCHEDULE_CODE = "SMART-SCANNER-RESEARCH-OUTCOMES"

#: The SAME queue and the SAME worker type as the lifecycle. See the header:
#: the queue is an identity boundary, and this work belongs to that identity.
RESEARCH_OUTCOMES_QUEUE = RL.RESEARCH_LIFECYCLE_QUEUE
RESEARCH_OUTCOMES_WORKER_TYPE = RL.RESEARCH_LIFECYCLE_WORKER_TYPE

#: Three, and a short backoff, because what an outcome run waits for is not a
#: prerequisite it can request. The lifecycle gets four long deferrals because
#: it enqueues its own core-history refresh and needs to outlast it. This run
#: has nothing to outlast: if a bar is missing it will still be missing in
#: thirty minutes, and the row already records that honestly as
#: `waiting_for_data`. The attempts here exist for transport faults — a dropped
#: connection, a worker that died mid-pass — which resolve in seconds.
RESEARCH_OUTCOMES_MAX_ATTEMPTS = 3
RESEARCH_OUTCOMES_BACKOFF_SECONDS = [60, 300]


def run_key_for_occurrence(*, schedule_code: str, schedule_version: int,
                           occurrence_iso: str) -> str:
    """Stable per-occurrence identity. Two fires of one occurrence share a run."""
    return "roc:" + ident.schedule_occurrence_idempotency_key(
        schedule_code=schedule_code, schedule_version=int(schedule_version),
        occurrence_iso=occurrence_iso)


def manual_run_key(*, label: str, now: Optional[datetime] = None) -> str:
    """Identity for an operator-invoked run.

    Carries the wall clock to the minute for the same two reasons the
    lifecycle's does: a deliberate re-run is genuinely a new run, and a manual
    run can never collide with a scheduled occurrence's key and overwrite its
    audit.
    """
    moment = now or datetime.now(timezone.utc)
    stamp = moment.strftime("%Y%m%dT%H%M")
    safe = "".join(c for c in (label or "manual") if c.isalnum() or c in "-_")[:40]
    return f"roc:manual:{safe or 'manual'}:{stamp}"


def task_payload_from_template(template: Optional[Dict[str, Any]], *,
                               run_key: str,
                               mode: str = "scheduled") -> Dict[str, Any]:
    """Bounded run parameters, read from the schedule row rather than compiled in.

    Every limit is CLAMPED here, so the schedule may lower a bound and never
    raise it past what this module considers safe. That is the same rule
    `research_lifecycle.task_payload_from_template` applies to the provider
    budget, and it matters for the same reason: a mistyped template must not be
    able to widen the blast radius of a scheduled run.
    """
    tmpl = template or {}
    if isinstance(tmpl, str):
        try:
            tmpl = json.loads(tmpl)
        except (ValueError, TypeError):
            tmpl = {}

    def bounded(key: str, default: int, ceiling: int) -> int:
        try:
            value = int(tmpl.get(key, default))
        except (TypeError, ValueError):
            value = default
        return max(0, min(value, ceiling))

    return {
        "run_key": run_key,
        "mode": mode,
        "scan_limit": bounded("scan_limit", DEFAULT_SCAN_LIMIT,
                              DEFAULT_SCAN_LIMIT),
        "observation_limit": bounded("observation_limit",
                                     DEFAULT_OBSERVATION_LIMIT,
                                     DEFAULT_OBSERVATION_LIMIT),
        # NEVER settable from a schedule template. Re-reading frozen evidence
        # is an operator action with an operator's reason, and a schedule that
        # did it every night would spend its whole bound re-deriving answers
        # that a database trigger guarantees cannot change.
        "include_settled": False,
        "contract_version": RESEARCH_OUTCOMES_JOB_CONTRACT,
    }


async def enqueue_research_outcomes(
        conn: asyncpg.Connection, *, run_key: str,
        payload: Optional[Dict[str, Any]] = None,
        requested_by: str = "operator") -> Dict[str, Any]:
    """Create ONE job + ONE task for this run key, or recognise the existing one.

    The only way an outcome run is dispatched. The scheduler calls it and so
    does the operator CLI, which is what makes "the backfill went through the
    production runtime path" a fact about the call graph rather than a claim.
    """
    body = dict(payload or {})
    body["run_key"] = run_key
    key = f"rocjob:{run_key}"
    row = await conn.fetchrow(
        "INSERT INTO job_runs (job_type, job_contract_version, queue_name,"
        " idempotency_key, status, requested_by) "
        "VALUES ($1,$2,$3,$4,'queued',$5) "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id",
        RESEARCH_OUTCOMES_JOB_TYPE, RESEARCH_OUTCOMES_JOB_CONTRACT,
        RESEARCH_OUTCOMES_QUEUE, key, requested_by)
    if row is None:
        existing = await conn.fetchrow(
            "SELECT id, status FROM job_runs WHERE idempotency_key=$1", key)
        return {"status": "already_queued",
                "job_id": str(existing["id"]) if existing else None,
                "run_key": run_key}

    job_id = row["id"]
    await Q.record_event(conn, job_id=job_id, event_type="job_scheduled",
                         safe_message=RESEARCH_OUTCOMES_JOB_TYPE,
                         metadata={"run_key": run_key})
    await conn.execute(
        "INSERT INTO job_tasks (job_id, queue_name, task_type,"
        " task_contract_version, task_key, ordinal, payload, payload_hash,"
        " idempotency_key, status, priority, max_attempts) "
        "VALUES ($1,$2,$3,$3,'outcomes',0,$4::jsonb,$5,$6,'queued',100,$7) "
        "ON CONFLICT DO NOTHING",
        job_id, RESEARCH_OUTCOMES_QUEUE, RESEARCH_OUTCOMES_TASK,
        json.dumps(body), ident.payload_hash(body), f"roctask:{run_key}",
        RESEARCH_OUTCOMES_MAX_ATTEMPTS)
    await Q.recompute_job_counters(conn, job_id)
    return {"status": "queued", "job_id": str(job_id), "run_key": run_key}


__all__ = [
    "RESEARCH_OUTCOMES_JOB_TYPE", "RESEARCH_OUTCOMES_JOB_CONTRACT",
    "RESEARCH_OUTCOMES_QUEUE", "RESEARCH_OUTCOMES_TASK",
    "RESEARCH_OUTCOMES_SCHEDULE_CODE", "RESEARCH_OUTCOMES_WORKER_TYPE",
    "RESEARCH_OUTCOMES_MAX_ATTEMPTS", "RESEARCH_OUTCOMES_BACKOFF_SECONDS",
    "run_key_for_occurrence", "manual_run_key", "task_payload_from_template",
    "enqueue_research_outcomes",
]
