"""Durable daily-pipeline orchestrator foundation (smart_scanner_daily_pipeline.v1).

Represents ONE pipeline occurrence as a single ``job_runs`` row (job_type
``smart_scanner_daily_pipeline``, queue_name ``daily_pipeline`` — a distinct
queue NAME within the SAME durable-queue tables, never a second queue
system). Stage progress is tracked in that row's ``result_summary`` JSONB
(bounded, matches the existing ``job_runs.result_summary`` <= 16384-byte
constraint from migration 018). No new table, no new broker.

Occurrence identity is deterministic over exactly the fields specified for
this pipeline: schedule_code, schedule_version, resolved_session_date,
frozen_universe_hash, pipeline_contract_version. A repeated call for the same
identity returns the SAME occurrence row (idempotent create), so the same
logical pipeline is always resumed rather than duplicated; a later resolved
session naturally produces a NEW occurrence.

Stage EXECUTION is intentionally NOT performed by this module — it stays a
pure, DB-durable state machine. The actual work for a stage (calling the
history-warmup incremental-refresh endpoint, the prospective register/enqueue
endpoints, the outcome-maturation enqueue endpoint, or the audit endpoint) is
supplied by the caller as a stage RESULT via ``record_stage_result``. This
keeps the state machine testable without live HTTP/DB-role dependencies and
mirrors this repo's existing separation between queue mechanics
(app/jobs/queue.py) and task handlers (app/jobs/handlers/*).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import asyncpg

from app.jobs import identity as ident

PIPELINE_CONTRACT_VERSION = "smart_scanner_daily_pipeline.v1"
# v2 separates PIPELINE-level completion from OUTCOME-level maturity: the
# outcome stage matures ALL eligible prior campaigns (bounded sweep) and records
# the CURRENT campaign's maturity truthfully (deferred when it has no completed
# forward session yet) instead of blocking the whole occurrence on the current
# campaign's own — chronologically impossible — same-day maturation. v1
# occurrence history is never rewritten; a v2 occurrence has a DISTINCT
# idempotency identity (contract version participates in the key).
PIPELINE_CONTRACT_VERSION_V2 = "smart_scanner_daily_pipeline.v2"
PIPELINE_CONTRACT_VERSIONS = (PIPELINE_CONTRACT_VERSION, PIPELINE_CONTRACT_VERSION_V2)
PIPELINE_JOB_TYPE = "smart_scanner_daily_pipeline"
PIPELINE_QUEUE = "daily_pipeline"
# The durable driver that makes scheduler -> occurrence -> advance fully
# automatic. The scheduler materialises ONE driver TASK (this task type, on this
# queue) per fired occurrence; a dedicated pipeline-driver worker claims it and
# calls advance_daily_pipeline_service repeatedly (bounded progress, defer while
# async child work runs) until the occurrence is terminal. The driver never
# duplicates the state machine — it only invokes the existing service.
DAILY_PIPELINE_ADVANCE_TASK = "smart_scanner_daily_pipeline_advance.v1"
DAILY_PIPELINE_DRIVER_QUEUE = "daily_pipeline_driver"
# job_tasks caps max_attempts at 10 (migration 018 CHECK). The driver does NOT
# rely on many retries to span a slow campaign — it WAITS INTERNALLY (sleep+poll,
# lease auto-renewed) up to DAILY_PIPELINE_DRIVER_MAX_WAIT_SECONDS within one
# claim; the attempts are a safety valve for a genuinely transient failure or a
# wait that exceeds that ceiling.
DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS = 10

STAGE_HISTORY_REFRESH = "history_refresh"
STAGE_PROSPECTIVE_CAMPAIGN = "prospective_campaign"
STAGE_OUTCOME_MATURATION = "outcome_maturation"
STAGE_AUDIT_REPORT = "audit_report"
STAGE_DONE = "done"

# Order matters: this IS the dependency chain. "resolve_session" is not a
# stage of its own — the caller resolves it once (via the existing canonical
# app.prospective_session.resolve_latest_completed_session) to compute the
# occurrence identity in the first place, and "readiness verification" /
# "quality audit" / "reporting" are folded into history_refresh's own
# preflight and audit_report respectively (matching this repo's existing
# reuse-don't-duplicate posture).
STAGE_ORDER: List[str] = [
    STAGE_HISTORY_REFRESH,
    STAGE_PROSPECTIVE_CAMPAIGN,
    STAGE_OUTCOME_MATURATION,
    STAGE_AUDIT_REPORT,
]

STAGE_STATE_PENDING = "pending"
STAGE_STATE_IN_PROGRESS = "in_progress"
STAGE_STATE_COMPLETED = "completed"
STAGE_STATE_BLOCKED = "blocked"
STAGE_STATE_RETRYABLE_FAILURE = "retryable_failure"
STAGE_STATE_TERMINAL_FAILURE = "terminal_failure"

_TERMINAL_STAGE_STATES = (STAGE_STATE_TERMINAL_FAILURE,)
_RESULT_SUMMARY_MAX_BYTES = 16384


def pipeline_occurrence_identity(*, schedule_code: str, schedule_version: int,
                                 resolved_session_date: str, frozen_universe_hash: str,
                                 pipeline_contract_version: str = PIPELINE_CONTRACT_VERSION) -> str:
    return ident.pipeline_occurrence_identity(
        schedule_code=schedule_code, schedule_version=schedule_version,
        resolved_session_date=resolved_session_date,
        frozen_universe_hash=frozen_universe_hash,
        pipeline_contract_version=pipeline_contract_version)


def _initial_result_summary(*, resolved_session_date: str, frozen_universe_hash: str,
                            universe_id: str,
                            pipeline_contract_version: str = PIPELINE_CONTRACT_VERSION
                            ) -> Dict[str, Any]:
    summary = {
        "contract_version": pipeline_contract_version,
        "resolved_session_date": resolved_session_date,
        "frozen_universe_hash": frozen_universe_hash,
        "universe_id": universe_id,
        "current_stage": STAGE_ORDER[0],
        "stages": {s: {"state": STAGE_STATE_PENDING} for s in STAGE_ORDER},
        "campaign_registration_id": None,
        "campaign_job_id": None,
        "outcome_job_id": None,
    }
    if pipeline_contract_version == PIPELINE_CONTRACT_VERSION_V2:
        # v2 top-level bookkeeping: the current campaign's truthful maturity and
        # the bounded prior-campaign maturation sweep are tracked separately from
        # (and never conflated with) pipeline-stage completion.
        summary["current_campaign_maturity"] = None
        summary["prior_maturation"] = None
    return summary


def _load_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("result_summary")
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return {}
    return dict(raw)


async def ensure_pipeline_occurrence(
        conn: asyncpg.Connection, *, schedule_code: str, schedule_version: int,
        resolved_session_date: str, frozen_universe_hash: str, universe_id: str,
        requested_by: str = "daily_pipeline_operator",
        pipeline_contract_version: str = PIPELINE_CONTRACT_VERSION) -> Dict[str, Any]:
    """Idempotently create-or-fetch the occurrence for this exact
    (session, universe, contract-version) tuple. Never creates a second
    occurrence for the same identity, even under concurrent callers (UNIQUE
    idempotency_key). A v2 occurrence has a distinct key from a v1 one for the
    same (session, universe), so v1 history is never rewritten."""
    if pipeline_contract_version not in PIPELINE_CONTRACT_VERSIONS:
        raise ValueError(f"unknown pipeline contract version {pipeline_contract_version!r}")
    key = pipeline_occurrence_identity(
        schedule_code=schedule_code, schedule_version=schedule_version,
        resolved_session_date=resolved_session_date,
        frozen_universe_hash=frozen_universe_hash,
        pipeline_contract_version=pipeline_contract_version)
    summary = _initial_result_summary(
        resolved_session_date=resolved_session_date,
        frozen_universe_hash=frozen_universe_hash, universe_id=universe_id,
        pipeline_contract_version=pipeline_contract_version)
    row = await conn.fetchrow(
        "INSERT INTO job_runs (job_type, job_contract_version, queue_name, idempotency_key,"
        " status, requested_by, result_summary) "
        "VALUES ($1,$2,$3,$4,'running',$5,$6::jsonb) "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING *",
        PIPELINE_JOB_TYPE, pipeline_contract_version, PIPELINE_QUEUE, key,
        requested_by, json.dumps(summary))
    if row is None:
        row = await conn.fetchrow("SELECT * FROM job_runs WHERE idempotency_key=$1", key)
    return dict(row)


async def get_pipeline_occurrence(conn: asyncpg.Connection, occurrence_id: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        "SELECT * FROM job_runs WHERE id=$1 AND job_type=$2", occurrence_id, PIPELINE_JOB_TYPE)
    return dict(row) if row is not None else None


async def latest_pipeline_occurrence(conn: asyncpg.Connection) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        "SELECT * FROM job_runs WHERE job_type=$1 ORDER BY created_at DESC LIMIT 1",
        PIPELINE_JOB_TYPE)
    return dict(row) if row is not None else None


def current_stage(occurrence: Dict[str, Any]) -> str:
    return _load_summary(occurrence).get("current_stage", STAGE_ORDER[0])


def pipeline_summary(occurrence: Dict[str, Any]) -> Dict[str, Any]:
    """Public accessor for an occurrence's parsed result_summary — e.g. to
    read ``campaign_registration_id``/``campaign_job_id``/``outcome_job_id``
    set by an earlier stage."""
    return _load_summary(occurrence)


async def record_stage_result(conn: asyncpg.Connection, occurrence_id: str, *,
                              stage: str, result: Dict[str, Any]) -> Dict[str, Any]:
    """Persist ONE stage's outcome and advance (or halt) the state machine.

    ``result`` must include ``state`` (one of the STAGE_STATE_* values above)
    and may include stage-specific bounded fields (e.g. campaign_registration_id,
    campaign_job_id, outcome_job_id, completed_forward_sessions). Advancing
    past the last stage marks the occurrence ``succeeded``; a
    STAGE_STATE_TERMINAL_FAILURE marks it ``failed`` and freezes current_stage
    at the blocked stage (never silently skips to the next one)."""
    if stage not in STAGE_ORDER:
        raise ValueError(f"unknown stage {stage!r}")
    row = await conn.fetchrow("SELECT * FROM job_runs WHERE id=$1", occurrence_id)
    if row is None:
        raise ValueError("unknown occurrence")
    summary = _load_summary(dict(row))
    if summary.get("current_stage") != stage:
        # Stale/out-of-order write (e.g. a retried caller for an already-
        # advanced occurrence) — ignore rather than corrupt state.
        return dict(row)
    state = result.get("state")
    summary["stages"][stage] = {k: v for k, v in result.items()}
    job_status = "running"
    finished = None
    if state == STAGE_STATE_COMPLETED:
        idx = STAGE_ORDER.index(stage)
        if idx + 1 < len(STAGE_ORDER):
            summary["current_stage"] = STAGE_ORDER[idx + 1]
        else:
            summary["current_stage"] = STAGE_DONE
            job_status = "succeeded"
            finished = "NOW()"
    elif state in _TERMINAL_STAGE_STATES:
        job_status = "failed"
        finished = "NOW()"
    # STAGE_STATE_IN_PROGRESS / BLOCKED / RETRYABLE_FAILURE: stay on this
    # stage, job status stays 'running' — a later call resumes it.
    for key in ("campaign_registration_id", "campaign_job_id", "outcome_job_id",
                "current_campaign_maturity", "prior_maturation"):
        if key in result:
            summary[key] = result[key]
    payload = json.dumps(summary)
    if len(payload.encode("utf-8")) > _RESULT_SUMMARY_MAX_BYTES:
        # Never let an oversized stage result exceed the DB constraint —
        # degrade to a bounded marker rather than fail the whole occurrence.
        summary["stages"][stage] = {"state": state, "truncated": True}
        payload = json.dumps(summary)
    if finished == "NOW()":
        updated = await conn.fetchrow(
            "UPDATE job_runs SET result_summary=$2::jsonb, status=$3, finished_at=NOW(),"
            " updated_at=NOW() WHERE id=$1 RETURNING *", occurrence_id, payload, job_status)
    else:
        updated = await conn.fetchrow(
            "UPDATE job_runs SET result_summary=$2::jsonb, status=$3, updated_at=NOW()"
            " WHERE id=$1 RETURNING *", occurrence_id, payload, job_status)
    return dict(updated)


def build_status_view(occurrence: Dict[str, Any]) -> Dict[str, Any]:
    """Operator-visible status view. No secrets, no DSNs — only IDs, stage
    states, and timestamps."""
    summary = _load_summary(occurrence)
    stages = summary.get("stages", {})
    completed = [s for s in STAGE_ORDER if stages.get(s, {}).get("state") == STAGE_STATE_COMPLETED]
    blocked = next((s for s in STAGE_ORDER if stages.get(s, {}).get("state") == STAGE_STATE_BLOCKED), None)
    retryable = next((s for s in STAGE_ORDER
                      if stages.get(s, {}).get("state") == STAGE_STATE_RETRYABLE_FAILURE), None)
    terminal = next((s for s in STAGE_ORDER
                     if stages.get(s, {}).get("state") == STAGE_STATE_TERMINAL_FAILURE), None)
    active_job_ids = {
        "campaign_job_id": summary.get("campaign_job_id"),
        "outcome_job_id": summary.get("outcome_job_id"),
    }
    view = {
        "contract_version": summary.get("contract_version", PIPELINE_CONTRACT_VERSION),
        "occurrence_id": str(occurrence["id"]),
        "resolved_session_date": summary.get("resolved_session_date"),
        "universe_hash": summary.get("frozen_universe_hash"),
        "universe_id": summary.get("universe_id"),
        "occurrence_status": occurrence.get("status"),
        "current_stage": summary.get("current_stage"),
        "stage_states": {s: stages.get(s, {}).get("state", STAGE_STATE_PENDING) for s in STAGE_ORDER},
        "completed_stages": completed,
        "blocked_stage": blocked,
        "retryable_failure_stage": retryable,
        "terminal_failure_stage": terminal,
        "campaign_registration_id": summary.get("campaign_registration_id"),
        "campaign_job_id": active_job_ids["campaign_job_id"],
        "outcome_job_id": active_job_ids["outcome_job_id"],
        "started_at": occurrence["created_at"].isoformat() if occurrence.get("created_at") else None,
        "completed_at": occurrence["finished_at"].isoformat() if occurrence.get("finished_at") else None,
    }
    if summary.get("contract_version") == PIPELINE_CONTRACT_VERSION_V2:
        # v2 surfaces the two separated concepts distinctly from stage state.
        view["current_campaign_maturity"] = summary.get("current_campaign_maturity")
        view["prior_maturation"] = summary.get("prior_maturation")
    return view


__all__ = [
    "PIPELINE_CONTRACT_VERSION", "PIPELINE_CONTRACT_VERSION_V2", "PIPELINE_CONTRACT_VERSIONS",
    "PIPELINE_JOB_TYPE", "PIPELINE_QUEUE",
    "DAILY_PIPELINE_ADVANCE_TASK", "DAILY_PIPELINE_DRIVER_QUEUE", "DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS",
    "STAGE_HISTORY_REFRESH", "STAGE_PROSPECTIVE_CAMPAIGN", "STAGE_OUTCOME_MATURATION",
    "STAGE_AUDIT_REPORT", "STAGE_DONE", "STAGE_ORDER",
    "STAGE_STATE_PENDING", "STAGE_STATE_IN_PROGRESS", "STAGE_STATE_COMPLETED",
    "STAGE_STATE_BLOCKED", "STAGE_STATE_RETRYABLE_FAILURE", "STAGE_STATE_TERMINAL_FAILURE",
    "pipeline_occurrence_identity", "ensure_pipeline_occurrence", "get_pipeline_occurrence",
    "latest_pipeline_occurrence", "current_stage", "pipeline_summary", "record_stage_result",
    "build_status_view",
]
