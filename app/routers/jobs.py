"""Generic durable-queue management API (job + schedule operators).

Bounded, authenticated, cursor-paginated. No unbounded list endpoint. These are
generic (job-type agnostic); the prospective enqueue route lives in the
prospective section of the admin router. All routes require the worker token and
operate on whichever least-privilege connection the deployment mode selected.
"""

from __future__ import annotations

import json
import uuid as _uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg
from fastapi import APIRouter, Body, Depends, HTTPException, Query

from app.config import settings
from app.deps import get_db, get_worker_token
from app.jobs import queue as Q

router = APIRouter()

_MAX_LIMIT = 100


def _uuid_or_422(value: str, field: str = "id") -> str:
    try:
        return str(_uuid.UUID(str(value)))
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(status_code=422, detail={"error": f"invalid_{field}"})


def _row(r: asyncpg.Record) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in dict(r).items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, _uuid.UUID):
            out[k] = str(v)
        elif isinstance(v, (dict, list)) or v is None or isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = str(v)
    return out


# --------------------------------------------------------------------------
# jobs
# --------------------------------------------------------------------------
@router.get("/jobs")
async def list_jobs(_: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db),
                    job_type: Optional[str] = None, status: Optional[str] = None,
                    created_after: Optional[str] = None,
                    limit: int = Query(default=25, ge=1, le=_MAX_LIMIT),
                    cursor: Optional[str] = None):
    """Bounded, keyset-paginated job listing (newest first)."""
    conds: List[str] = []
    args: List[Any] = []
    if job_type:
        args.append(job_type[:80]); conds.append(f"job_type=${len(args)}")
    if status:
        args.append(status[:40]); conds.append(f"status=${len(args)}")
    if created_after:
        try:
            args.append(datetime.fromisoformat(created_after.replace("Z", "+00:00")))
            conds.append(f"created_at >= ${len(args)}")
        except ValueError:
            raise HTTPException(status_code=422, detail={"error": "invalid_created_after"})
    if cursor:
        try:
            args.append(datetime.fromisoformat(cursor.replace("Z", "+00:00")))
            conds.append(f"created_at < ${len(args)}")
        except ValueError:
            raise HTTPException(status_code=422, detail={"error": "invalid_cursor"})
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    args.append(int(limit) + 1)
    rows = await db.fetch(
        f"SELECT id, job_type, job_contract_version, queue_name, status, priority,"
        f" registration_id, campaign_id, schedule_id, total_task_count, queued_task_count,"
        f" running_task_count, succeeded_task_count, retryable_task_count, failed_task_count,"
        f" cancelled_task_count, safe_error_code, created_at, started_at, finished_at "
        f"FROM job_runs{where} ORDER BY created_at DESC LIMIT ${len(args)}", *args)
    items = [_row(r) for r in rows[:limit]]
    next_cursor = items[-1]["created_at"] if len(rows) > limit and items else None
    return {"items": items, "count": len(items), "next_cursor": next_cursor}


@router.get("/jobs/workers")
async def list_workers(_: str = Depends(get_worker_token), db: asyncpg.Connection = Depends(get_db),
                       limit: int = Query(default=25, ge=1, le=_MAX_LIMIT)):
    stale = int(getattr(settings, "JOB_WORKER_STALE_SECONDS", 90))
    rows = await db.fetch(
        "SELECT worker_id, worker_type, queue_names, deployed_git_sha, hostname, status,"
        " started_at, last_heartbeat_at, current_task_id, draining,"
        " (EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at)))::int AS heartbeat_age_seconds,"
        " (NOW() - last_heartbeat_at) > ($1 || ' seconds')::interval AS stale "
        "FROM job_workers ORDER BY last_heartbeat_at DESC LIMIT $2", str(stale), int(limit))
    return {"items": [_row(r) for r in rows], "count": len(rows), "stale_threshold_seconds": stale}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, _: str = Depends(get_worker_token),
                  db: asyncpg.Connection = Depends(get_db)):
    jid = _uuid_or_422(job_id, "job_id")
    row = await db.fetchrow("SELECT * FROM job_runs WHERE id=$1", jid)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_job"})
    return _row(row)


@router.get("/jobs/{job_id}/tasks")
async def get_job_tasks(job_id: str, _: str = Depends(get_worker_token),
                        db: asyncpg.Connection = Depends(get_db),
                        status: Optional[str] = None,
                        limit: int = Query(default=50, ge=1, le=_MAX_LIMIT)):
    jid = _uuid_or_422(job_id, "job_id")
    args: List[Any] = [jid]
    cond = ""
    if status:
        args.append(status[:40]); cond = f" AND status=${len(args)}"
    args.append(int(limit))
    rows = await db.fetch(
        f"SELECT id, ordinal, task_key, task_type, status, priority, attempt_count, max_attempts,"
        f" operator_retry_eligible, available_at, lease_owner, lease_expires_at, heartbeat_at,"
        f" started_at, finished_at, safe_error_code, error_class, result_summary, created_at "
        f"FROM job_tasks WHERE job_id=$1{cond} ORDER BY ordinal ASC LIMIT ${len(args)}", *args)
    return {"items": [_row(r) for r in rows], "count": len(rows)}


@router.get("/jobs/{job_id}/events")
async def get_job_events(job_id: str, _: str = Depends(get_worker_token),
                         db: asyncpg.Connection = Depends(get_db),
                         limit: int = Query(default=100, ge=1, le=_MAX_LIMIT)):
    jid = _uuid_or_422(job_id, "job_id")
    rows = await db.fetch(
        "SELECT id, task_id, event_type, safe_message, metadata, created_at "
        "FROM job_events WHERE job_id=$1 ORDER BY created_at ASC, id ASC LIMIT $2", jid, int(limit))
    return {"items": [_row(r) for r in rows], "count": len(rows)}


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, _: str = Depends(get_worker_token),
                     db: asyncpg.Connection = Depends(get_db)):
    jid = _uuid_or_422(job_id, "job_id")
    res = await Q.request_cancel(db, job_id=jid, requested_by="operator")
    if not res.get("found"):
        raise HTTPException(status_code=404, detail={"error": "unknown_job"})
    return {"job_id": jid, **res}


@router.post("/jobs/{job_id}/retry-failed")
async def retry_failed(job_id: str, _: str = Depends(get_worker_token),
                       db: asyncpg.Connection = Depends(get_db)):
    jid = _uuid_or_422(job_id, "job_id")
    res = await Q.retry_failed_tasks(db, job_id=jid, requested_by="operator")
    if not res.get("found"):
        raise HTTPException(status_code=404, detail={"error": "unknown_job"})
    return {"job_id": jid, **res}


# --------------------------------------------------------------------------
# schedules
# --------------------------------------------------------------------------
_ALLOWED_SCHEDULE_TYPES = ("cron", "market_daily")


@router.get("/job-schedules")
async def list_schedules(_: str = Depends(get_worker_token),
                         db: asyncpg.Connection = Depends(get_db),
                         limit: int = Query(default=50, ge=1, le=_MAX_LIMIT)):
    rows = await db.fetch(
        "SELECT * FROM job_schedules ORDER BY schedule_code ASC, schedule_version DESC LIMIT $1",
        int(limit))
    return {"items": [_row(r) for r in rows], "count": len(rows)}


@router.post("/job-schedules")
async def create_schedule(_: str = Depends(get_worker_token),
                          db: asyncpg.Connection = Depends(get_db), body: Any = Body(...)):
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail={"error": "body_must_be_object"})
    code = str(body.get("schedule_code", "")).strip()
    stype = str(body.get("schedule_type", "")).strip()
    if not code or len(code) > 80:
        raise HTTPException(status_code=422, detail={"error": "invalid_schedule_code"})
    if stype not in _ALLOWED_SCHEDULE_TYPES:
        raise HTTPException(status_code=422, detail={"error": "invalid_schedule_type"})
    if not body.get("job_type") or not body.get("job_contract_version"):
        raise HTTPException(status_code=422, detail={"error": "job_type_and_contract_required"})
    cron_expr = body.get("cron_expression")
    delay = body.get("market_close_delay_minutes")
    if stype == "cron":
        if not cron_expr:
            raise HTTPException(status_code=422, detail={"error": "cron_expression_required"})
        from app.jobs.scheduler import parse_cron
        try:
            parse_cron(cron_expr)
        except ValueError:
            raise HTTPException(status_code=422, detail={"error": "invalid_cron_expression"})
    if stype == "market_daily" and delay is None:
        raise HTTPException(status_code=422, detail={"error": "market_close_delay_minutes_required"})
    template = body.get("payload_template")
    if template is not None and len(json.dumps(template)) > 8192:
        raise HTTPException(status_code=422, detail={"error": "payload_template_too_large"})
    # Schedules are created DISABLED and unpaused by default (safety); an
    # operator must explicitly enable via PATCH once handlers are wired.
    row = await db.fetchrow(
        "INSERT INTO job_schedules (schedule_code, schedule_version, schedule_type, timezone,"
        " cron_expression, market_close_delay_minutes, job_type, job_contract_version,"
        " payload_template, enabled, paused, idempotency_scope) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb, COALESCE($10,FALSE), FALSE,"
        " COALESCE($11,'occurrence')) "
        "ON CONFLICT (schedule_code, schedule_version) DO NOTHING RETURNING *",
        code, int(body.get("schedule_version", 1)), stype,
        str(body.get("timezone") or "America/New_York"), cron_expr,
        (int(delay) if delay is not None else None), str(body["job_type"])[:80],
        str(body["job_contract_version"])[:80],
        json.dumps(template) if template is not None else None,
        bool(body.get("enabled", False)), body.get("idempotency_scope"))
    if row is None:
        raise HTTPException(status_code=409, detail={"error": "schedule_already_exists"})
    return _row(row)


@router.patch("/job-schedules/{schedule_id}")
async def patch_schedule(schedule_id: str, _: str = Depends(get_worker_token),
                         db: asyncpg.Connection = Depends(get_db), body: Any = Body(...)):
    sid = _uuid_or_422(schedule_id, "schedule_id")
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail={"error": "body_must_be_object"})
    sets: List[str] = []
    args: List[Any] = [sid]
    for field in ("enabled", "paused"):
        if field in body:
            args.append(bool(body[field])); sets.append(f"{field}=${len(args)}")
    if "market_close_delay_minutes" in body and body["market_close_delay_minutes"] is not None:
        args.append(int(body["market_close_delay_minutes"]))
        sets.append(f"market_close_delay_minutes=${len(args)}")
    if "cron_expression" in body and body["cron_expression"]:
        from app.jobs.scheduler import parse_cron
        try:
            parse_cron(body["cron_expression"])
        except ValueError:
            raise HTTPException(status_code=422, detail={"error": "invalid_cron_expression"})
        args.append(str(body["cron_expression"])[:120]); sets.append(f"cron_expression=${len(args)}")
    if "next_run_at" in body and body["next_run_at"]:
        try:
            args.append(datetime.fromisoformat(str(body["next_run_at"]).replace("Z", "+00:00")))
        except ValueError:
            raise HTTPException(status_code=422, detail={"error": "invalid_next_run_at"})
        sets.append(f"next_run_at=${len(args)}")
    if "payload_template" in body:
        template = body["payload_template"]
        if template is not None and len(json.dumps(template)) > 8192:
            raise HTTPException(status_code=422, detail={"error": "payload_template_too_large"})
        args.append(json.dumps(template) if template is not None else None)
        sets.append(f"payload_template=${len(args)}::jsonb")
    if not sets:
        raise HTTPException(status_code=422, detail={"error": "no_mutable_fields"})
    row = await db.fetchrow(
        f"UPDATE job_schedules SET {', '.join(sets)}, updated_at=NOW() WHERE id=$1 RETURNING *", *args)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_schedule"})
    return _row(row)


@router.post("/job-schedules/{schedule_id}/pause")
async def pause_schedule(schedule_id: str, _: str = Depends(get_worker_token),
                         db: asyncpg.Connection = Depends(get_db)):
    sid = _uuid_or_422(schedule_id, "schedule_id")
    row = await db.fetchrow(
        "UPDATE job_schedules SET paused=TRUE, updated_at=NOW() WHERE id=$1 RETURNING *", sid)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_schedule"})
    return _row(row)


@router.post("/job-schedules/{schedule_id}/resume")
async def resume_schedule(schedule_id: str, _: str = Depends(get_worker_token),
                          db: asyncpg.Connection = Depends(get_db)):
    sid = _uuid_or_422(schedule_id, "schedule_id")
    row = await db.fetchrow(
        "UPDATE job_schedules SET paused=FALSE, updated_at=NOW() WHERE id=$1 RETURNING *", sid)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_schedule"})
    return _row(row)


@router.get("/job-schedules/{schedule_id}/preview")
async def preview_schedule(schedule_id: str, _: str = Depends(get_worker_token),
                           db: asyncpg.Connection = Depends(get_db),
                           count: int = Query(default=5, ge=1, le=20)):
    sid = _uuid_or_422(schedule_id, "schedule_id")
    row = await db.fetchrow("SELECT * FROM job_schedules WHERE id=$1", sid)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_schedule"})
    from app.jobs.scheduler import preview_occurrences
    now = datetime.now(timezone.utc)
    try:
        occ = preview_occurrences(dict(row), now, count=count)
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "cannot_compute_occurrence",
                                                     "reason": str(e)[:120]})
    return {"schedule_id": sid, "schedule_code": row["schedule_code"],
            "schedule_type": row["schedule_type"], "computed_at": now.isoformat(),
            "next_occurrences": occ}


__all__ = ["router"]
