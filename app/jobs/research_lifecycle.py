"""Durable-queue identity for the research lifecycle.

WHY A DEDICATED QUEUE AND A DEDICATED WORKER TYPE
-------------------------------------------------
The research lifecycle spends provider requests, writes research tables, and
refreshes research-scoped catalyst freshness. None of that belongs to any
existing component:

  * the pipeline-driver's role cannot see a research table, deliberately;
  * the history-refresh worker holds the provider credential but exists to
    refresh the FROZEN universe's bars and nothing else;
  * the Product API holds no credential and writes nothing at all.

So this is its own queue (`research_lifecycle`), its own task type, its own
worker type, and — in ops/sql — its own least-privilege role. A component that
cannot execute research work should not be able to claim a research task, and
after this it structurally cannot.

WHY ONE TASK AND NOT ONE PER SYMBOL
-----------------------------------
The stages are ordered and share state: admission must run after discovery and
before warmup, the scan must run after readiness, and the freshness gate must
be able to stop everything downstream. Fanning that out per symbol would turn a
sequence into a coordination problem, and the provider allows one symbol per 75
seconds anyway — so the parallelism would buy nothing and cost the ordering.

IDEMPOTENCY
-----------
The task key is derived from (schedule, occurrence) — or, for a manual run,
from an operator-supplied label — and becomes the lifecycle's `run_key`. A
scheduler that fires twice for the same occurrence produces one run; a leased
task that is retried re-opens the same run rather than inventing a second
history of the same execution.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg

from app.config import settings
from app.jobs import identity as ident
from app.jobs import queue as Q

RESEARCH_LIFECYCLE_JOB_TYPE = "smart_scanner_research_lifecycle.v1"
RESEARCH_LIFECYCLE_JOB_CONTRACT = "smart_scanner_research_lifecycle.v1"
RESEARCH_LIFECYCLE_QUEUE = "research_lifecycle"
RESEARCH_LIFECYCLE_TASK = "smart_scanner_research_lifecycle_run.v1"
RESEARCH_LIFECYCLE_SCHEDULE_CODE = "SMART-SCANNER-RESEARCH-LIFECYCLE"

#: The worker type allowed to materialise and to execute this schedule.
RESEARCH_LIFECYCLE_WORKER_TYPE = "research_lifecycle"

#: Two attempts, not three. The lifecycle is bounded, idempotent and cheap to
#: repeat tomorrow; a third attempt inside one occurrence would spend provider
#: requests re-doing work whose only likely blocker (stale core bars, a held
#: warmup lock) will not have changed within the retry window.
RESEARCH_LIFECYCLE_MAX_ATTEMPTS = 2

#: Bounded defaults, all overridable from the schedule's payload template.
DEFAULT_ADMIT_LIMIT = 5
DEFAULT_WARM_LIMIT = 5
DEFAULT_PROVIDER_BUDGET = 12
DEFAULT_DISCOVERY_DAYS = 14


def run_key_for_occurrence(*, schedule_code: str, schedule_version: int,
                           occurrence_iso: str) -> str:
    """Stable per-occurrence identity. Two fires of the same occurrence share
    one run; tomorrow's occurrence is a different run."""
    return "rlc:" + ident.schedule_occurrence_idempotency_key(
        schedule_code=schedule_code, schedule_version=int(schedule_version),
        occurrence_iso=occurrence_iso)


def manual_run_key(*, label: str, now: Optional[datetime] = None) -> str:
    """Identity for an operator-invoked run.

    Carries the wall clock to the minute so a deliberate re-run is genuinely a
    new run — and so a manual run can never collide with a scheduled
    occurrence's key, which would silently overwrite the scheduled run's audit.
    """
    moment = now or datetime.now(timezone.utc)
    stamp = moment.strftime("%Y%m%dT%H%M")
    safe = "".join(c for c in (label or "manual") if c.isalnum() or c in "-_")[:40]
    return f"rlc:manual:{safe or 'manual'}:{stamp}"


def task_payload_from_template(template: Optional[Dict[str, Any]], *,
                               run_key: str) -> Dict[str, Any]:
    """Bounded run parameters, read from the schedule row rather than compiled in.

    Every limit is clamped here, so a mistyped template cannot widen the
    provider budget: the schedule may lower a bound, never raise it past what
    this module considers safe.
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
        "admit_limit": bounded("admit_limit", DEFAULT_ADMIT_LIMIT,
                               DEFAULT_ADMIT_LIMIT),
        "warm_limit": bounded("warm_limit", DEFAULT_WARM_LIMIT,
                              DEFAULT_WARM_LIMIT),
        "provider_budget": bounded("provider_budget", DEFAULT_PROVIDER_BUDGET,
                                   DEFAULT_PROVIDER_BUDGET),
        "discovery_days": bounded("discovery_days", DEFAULT_DISCOVERY_DAYS, 60),
        "refresh_discovery": bool(tmpl.get("refresh_discovery", True)),
        "enrich": bool(tmpl.get("enrich", True)),
        "contract_version": RESEARCH_LIFECYCLE_JOB_CONTRACT,
    }


async def enqueue_research_lifecycle(
        conn: asyncpg.Connection, *, run_key: str,
        payload: Optional[Dict[str, Any]] = None,
        requested_by: str = "operator") -> Dict[str, Any]:
    """Create ONE job + ONE task for this run key, or recognise the existing one.

    This is the ONLY way a lifecycle is dispatched — the scheduler calls it and
    so does the operator CLI. That is what makes "run it manually through the
    exact same dispatcher used by scheduling" a fact rather than an intention.
    """
    body = dict(payload or {})
    body["run_key"] = run_key
    key = f"rlcjob:{run_key}"
    row = await conn.fetchrow(
        "INSERT INTO job_runs (job_type, job_contract_version, queue_name,"
        " idempotency_key, status, requested_by) "
        "VALUES ($1,$2,$3,$4,'queued',$5) "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id",
        RESEARCH_LIFECYCLE_JOB_TYPE, RESEARCH_LIFECYCLE_JOB_CONTRACT,
        RESEARCH_LIFECYCLE_QUEUE, key, requested_by)
    if row is None:
        existing = await conn.fetchrow(
            "SELECT id, status FROM job_runs WHERE idempotency_key=$1", key)
        return {"status": "already_queued",
                "job_id": str(existing["id"]) if existing else None,
                "run_key": run_key}

    job_id = row["id"]
    await Q.record_event(conn, job_id=job_id, event_type="job_scheduled",
                         safe_message=RESEARCH_LIFECYCLE_JOB_TYPE,
                         metadata={"run_key": run_key})
    await conn.execute(
        "INSERT INTO job_tasks (job_id, queue_name, task_type,"
        " task_contract_version, task_key, ordinal, payload, payload_hash,"
        " idempotency_key, status, priority, max_attempts) "
        "VALUES ($1,$2,$3,$3,'lifecycle',0,$4::jsonb,$5,$6,'queued',100,$7) "
        "ON CONFLICT DO NOTHING",
        job_id, RESEARCH_LIFECYCLE_QUEUE, RESEARCH_LIFECYCLE_TASK,
        json.dumps(body), ident.payload_hash(body), f"rlctask:{run_key}",
        RESEARCH_LIFECYCLE_MAX_ATTEMPTS)
    await Q.recompute_job_counters(conn, job_id)
    return {"status": "queued", "job_id": str(job_id), "run_key": run_key}


__all__ = [
    "RESEARCH_LIFECYCLE_JOB_TYPE", "RESEARCH_LIFECYCLE_JOB_CONTRACT",
    "RESEARCH_LIFECYCLE_QUEUE", "RESEARCH_LIFECYCLE_TASK",
    "RESEARCH_LIFECYCLE_SCHEDULE_CODE", "RESEARCH_LIFECYCLE_WORKER_TYPE",
    "RESEARCH_LIFECYCLE_MAX_ATTEMPTS", "DEFAULT_ADMIT_LIMIT",
    "DEFAULT_WARM_LIMIT", "DEFAULT_PROVIDER_BUDGET", "DEFAULT_DISCOVERY_DAYS",
    "run_key_for_occurrence", "manual_run_key", "task_payload_from_template",
    "enqueue_research_lifecycle",
]
