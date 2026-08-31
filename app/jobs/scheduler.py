"""Scheduler foundation — runs INSIDE the worker parent process.

Exactly one leader acts at a time (Postgres advisory lock). The leader inspects
enabled, non-paused, due schedules and creates jobs through the canonical job
path with a DETERMINISTIC per-occurrence idempotency key, so the same
occurrence can never be enqueued twice (even if two ticks race). It NEVER runs
task logic and NEVER enqueues a disabled or paused schedule.

Two schedule types:
  * cron          — minimal 5-field cron (min hour dom mon dow), lists/ranges/steps.
  * market_daily  — the latest fully-completed NYSE session close + a configured
                    delay, America/New_York, holiday & early-close aware (reuses
                    app.prospective_session).

No schedule is enabled in this task; a disabled template documents the intended
future daily pipeline. Enqueuing concrete tasks per job_type is wired per handler
as those handlers adopt the queue (documented in the runbook).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import asyncpg

from app.config import settings
from app.jobs import contracts as C
from app.jobs import identity as ident
from app.jobs import queue as Q

logger = logging.getLogger("app.jobs.scheduler")

_MAX_CRON_SEARCH_MINUTES = 366 * 24 * 60  # one year bound


# --------------------------------------------------------------------------
# minimal cron
# --------------------------------------------------------------------------
def _parse_field(spec: str, lo: int, hi: int) -> set:
    values: set = set()
    for part in spec.split(","):
        part = part.strip()
        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            step = int(step_s)
        else:
            base = part
        if base in ("*", ""):
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(base)
        if start < lo or end > hi or start > end or step < 1:
            raise ValueError(f"invalid cron field {spec!r}")
        values.update(range(start, end + 1, step))
    return values


def parse_cron(expr: str) -> Dict[str, set]:
    fields = str(expr).split()
    if len(fields) != 5:
        raise ValueError("cron expression must have 5 fields")
    return {
        "minute": _parse_field(fields[0], 0, 59),
        "hour": _parse_field(fields[1], 0, 23),
        "dom": _parse_field(fields[2], 1, 31),
        "month": _parse_field(fields[3], 1, 12),
        "dow": _parse_field(fields[4], 0, 6),  # 0 = Sunday
    }


def next_cron_occurrence(expr: str, after: datetime, tz: str) -> datetime:
    """First minute strictly after ``after`` matching ``expr`` in tz. Bounded
    search (one year) so a pathological expression can never spin forever."""
    fields = parse_cron(expr)
    zone = ZoneInfo(tz)
    cur = after.astimezone(zone).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(_MAX_CRON_SEARCH_MINUTES):
        dow = (cur.weekday() + 1) % 7  # Python Mon=0 → cron Sun=0
        if (cur.minute in fields["minute"] and cur.hour in fields["hour"]
                and cur.day in fields["dom"] and cur.month in fields["month"]
                and dow in fields["dow"]):
            return cur.astimezone(timezone.utc)
        cur += timedelta(minutes=1)
    raise ValueError("no cron occurrence found within one year")


# --------------------------------------------------------------------------
# market_daily
# --------------------------------------------------------------------------
def next_market_daily_occurrence(after: datetime, delay_minutes: int) -> datetime:
    """The next (close + delay) instant strictly after ``after``, walking forward
    through NYSE trading days (holiday/early-close aware via the completed-session
    resolver). Uses the regular 16:00 ET close + delay."""
    from app.prospective_session import (is_trading_day, session_cutoff_utc,
                                          resolve_latest_completed_session)
    delay = timedelta(minutes=int(delay_minutes))
    # start from the latest completed session; its close+delay is the earliest
    # candidate, then step forward day-by-day to find the first future one.
    session = resolve_latest_completed_session(after)
    candidate = session_cutoff_utc(session) + delay
    guard = 0
    while candidate <= after and guard < 400:
        session = session + timedelta(days=1)
        while not is_trading_day(session):
            session = session + timedelta(days=1)
        candidate = session_cutoff_utc(session) + delay
        guard += 1
    return candidate


# --------------------------------------------------------------------------
# next-run computation + preview
# --------------------------------------------------------------------------
def compute_next_run_at(schedule: Dict[str, Any], after: datetime) -> datetime:
    stype = schedule["schedule_type"]
    tz = schedule.get("timezone") or "America/New_York"
    if stype == "cron":
        return next_cron_occurrence(schedule["cron_expression"], after, tz)
    if stype == "market_daily":
        return next_market_daily_occurrence(after, schedule.get("market_close_delay_minutes") or 0)
    raise ValueError(f"unknown schedule_type {stype!r}")


def preview_occurrences(schedule: Dict[str, Any], after: datetime, count: int = 5) -> List[str]:
    out: List[str] = []
    cursor = after
    for _ in range(max(1, min(count, 20))):
        nxt = compute_next_run_at(schedule, cursor)
        out.append(nxt.isoformat())
        cursor = nxt
    return out


# --------------------------------------------------------------------------
# leader tick
# --------------------------------------------------------------------------
async def run_scheduler_tick(*, worker_id: str, now: Optional[datetime] = None) -> Dict[str, Any]:
    """One scheduler pass. Acquires the leader advisory lock; if leader, enqueues
    all due occurrences (idempotently) and advances next_run_at. Returns a small
    summary. A non-leader returns immediately."""
    from app.workers.persistence import get_db_connection, release_db_connection
    now = now or datetime.now(timezone.utc)
    lock_key = int(settings.JOB_SCHEDULER_ADVISORY_LOCK_KEY)
    conn = await get_db_connection()
    try:
        is_leader = await conn.fetchval("SELECT pg_try_advisory_lock($1)", lock_key)
        if not is_leader:
            return {"leader": False, "enqueued": 0}
        try:
            return await _tick_as_leader(conn, worker_id=worker_id, now=now)
        finally:
            await conn.fetchval("SELECT pg_advisory_unlock($1)", lock_key)
    finally:
        await release_db_connection(conn)


def _template(schedule: Dict[str, Any]) -> Dict[str, Any]:
    """The schedule's payload template as a dict, whatever the driver returned."""
    tmpl = schedule.get("payload_template") or {}
    if isinstance(tmpl, str):
        try:
            tmpl = json.loads(tmpl)
        except (ValueError, TypeError):
            tmpl = {}
    return tmpl if isinstance(tmpl, dict) else {}


def _schedule_is_ownable(schedule: Dict[str, Any]) -> bool:
    """May THIS leader materialise this schedule?

    A schedule may declare `payload_template.scheduler_owner`, naming the worker
    type responsible for it. A schedule WITHOUT the key is unowned and any
    leader may materialise it — so every schedule that existed before this
    existed behaves exactly as it did, and no deployed app needs to change.

    WHY OWNERSHIP IS ON THE SCHEDULE AND NOT IN EACH APP'S ENV
    ---------------------------------------------------------
    Because it is a property of the work. The research lifecycle must be
    materialised only by the research worker: the pipeline-driver's role is
    deliberately unable to touch a research table, so a research task it
    created would be a task it could never explain, and its insert would be
    refused by RLS on every tick — a warning loop, not a schedule. Putting the
    fact in the schedule row means one place states it, rather than every app
    having to carry a list of the schedules it must ignore.
    """
    owner = (_template(schedule).get("scheduler_owner") or "").strip()
    if not owner:
        return True
    mine = ((settings.JOB_SCHEDULER_OWNER or "").strip()
            or (getattr(settings, "JOB_WORKER_TYPE", "") or "").strip())
    return owner == mine


async def _tick_as_leader(conn: asyncpg.Connection, *, worker_id: str,
                          now: datetime) -> Dict[str, Any]:
    due = await conn.fetch(
        "SELECT * FROM job_schedules WHERE enabled=TRUE AND paused=FALSE "
        "AND (next_run_at IS NULL OR next_run_at <= $1) ORDER BY next_run_at ASC NULLS FIRST",
        now)
    enqueued = 0
    skipped_not_owned = 0
    for sched in due:
        s = dict(sched)
        if not _schedule_is_ownable(s):
            # Not ours. Leave next_run_at alone so the owning leader still sees
            # it as due — skipping must not consume somebody else's occurrence.
            skipped_not_owned += 1
            continue
        try:
            occurrence = s["next_run_at"] or compute_next_run_at(s, now)
            job_id = await _create_scheduled_job(conn, s, occurrence)
            if job_id is not None:
                enqueued += 1
            next_run = compute_next_run_at(s, occurrence)
            await conn.execute(
                "UPDATE job_schedules SET next_run_at=$2, last_enqueued_at=NOW(),"
                " last_job_id=COALESCE($3, last_job_id), updated_at=NOW() WHERE id=$1",
                s["id"], next_run, job_id)
        except Exception:
            logger.warning("schedule tick failed for %s", s.get("schedule_code"), exc_info=True)
    return {"leader": True, "enqueued": enqueued, "due": len(due),
            "skipped_not_owned": skipped_not_owned}


def _research_lifecycle_spec(schedule: Dict[str, Any],
                             occurrence: datetime) -> Optional[Dict[str, Any]]:
    """The durable task spec for the research lifecycle schedule, else None.

    The run key is derived from the OCCURRENCE, so a scheduler that fires twice
    for one occurrence produces one lifecycle run rather than two competing
    histories of the same session.
    """
    from app.jobs import research_lifecycle as RL
    if schedule.get("job_type") != RL.RESEARCH_LIFECYCLE_JOB_TYPE:
        return None
    run_key = RL.run_key_for_occurrence(
        schedule_code=schedule["schedule_code"],
        schedule_version=int(schedule["schedule_version"]),
        occurrence_iso=occurrence.isoformat())
    payload = RL.task_payload_from_template(_template(schedule),
                                            run_key=run_key)
    payload.update({"schedule_code": schedule["schedule_code"],
                    "schedule_version": int(schedule["schedule_version"]),
                    "occurrence_scheduled_at": occurrence.isoformat()})
    return {"task_type": RL.RESEARCH_LIFECYCLE_TASK,
            "queue": RL.RESEARCH_LIFECYCLE_QUEUE,
            "max_attempts": int(RL.RESEARCH_LIFECYCLE_MAX_ATTEMPTS),
            "payload": payload,
            "task_key_prefix": "rlctask:",
            "task_key": f"rlctask:{run_key}"}


def _pipeline_driver_spec(schedule: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the durable-driver task spec for a daily-pipeline schedule whose
    payload_template carries the frozen ``universe_id`` (and optional hash), else
    None. Without a configured universe the scheduler stays backward-compatible:
    it materialises only the inert parent marker (no driver task), exactly as
    before this handler existed."""
    from app.jobs import daily_pipeline as DP
    if schedule.get("job_type") != DP.PIPELINE_JOB_TYPE:
        return None
    tmpl = schedule.get("payload_template") or {}
    if isinstance(tmpl, str):
        try:
            tmpl = json.loads(tmpl)
        except (ValueError, TypeError):
            tmpl = {}
    universe_id = tmpl.get("universe_id")
    if not universe_id:
        return None
    return {
        "task_type": DP.DAILY_PIPELINE_ADVANCE_TASK,
        "queue": DP.DAILY_PIPELINE_DRIVER_QUEUE,
        "max_attempts": int(DP.DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS),
        "payload": {
            "universe_id": str(universe_id),
            "universe_hash": tmpl.get("universe_hash"),
            "pipeline_contract_version": tmpl.get("contract_version") or DP.PIPELINE_CONTRACT_VERSION_V2,
        },
    }


async def _create_scheduled_job(conn: asyncpg.Connection, schedule: Dict[str, Any],
                                occurrence: datetime) -> Optional[str]:
    """Create ONE parent occurrence per fire (idempotent via the occurrence key)
    AND — for a daily-pipeline schedule configured with a frozen universe — ONE
    durable driver task (smart_scanner_daily_pipeline_advance.v1) on the driver
    queue so a pipeline-driver worker advances it automatically. Both are
    idempotent: a duplicate scheduler fire for the same occurrence creates
    neither a second parent nor a second driver task."""
    key = ident.schedule_occurrence_idempotency_key(
        schedule_code=schedule["schedule_code"],
        schedule_version=int(schedule["schedule_version"]),
        occurrence_iso=occurrence.isoformat())
    driver = _pipeline_driver_spec(schedule)
    if driver is not None:
        # Existing behaviour, byte for byte: the driver payload gains the
        # schedule identity here and the task key is "dpadv:" + occurrence key.
        driver = dict(driver)
        driver["payload"] = {**driver["payload"],
                             "schedule_code": schedule["schedule_code"],
                             "schedule_version": int(schedule["schedule_version"]),
                             "occurrence_scheduled_at": occurrence.isoformat()}
        driver["task_key"] = "dpadv:" + key
    else:
        driver = _research_lifecycle_spec(schedule, occurrence)
    marker_queue = (driver["queue"] if driver is not None
                    else (schedule.get("queue_name") or C.PROSPECTIVE_QUEUE))
    row = await conn.fetchrow(
        "INSERT INTO job_runs (job_type, job_contract_version, queue_name, idempotency_key,"
        " status, schedule_id, requested_by) "
        "VALUES ($1,$2,$3,$4,'queued',$5,'scheduler') "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id",
        schedule["job_type"], schedule["job_contract_version"],
        marker_queue, key, schedule["id"])
    if row is None:
        return None  # duplicate occurrence — never enqueued twice
    await Q.record_event(conn, job_id=row["id"], event_type="job_scheduled",
                         safe_message=schedule["schedule_code"],
                         metadata={"occurrence": occurrence.isoformat(),
                                   "schedule_id": str(schedule["id"])})
    if driver is not None:
        task_payload = driver["payload"]
        await conn.execute(
            "INSERT INTO job_tasks (job_id, queue_name, task_type, task_contract_version,"
            " task_key, ordinal, payload, payload_hash, idempotency_key, status, priority,"
            " max_attempts) "
            "VALUES ($1,$2,$3,$3,'driver',0,$4::jsonb,$5,$6,'queued',100,$7) "
            "ON CONFLICT DO NOTHING",
            row["id"], driver["queue"], driver["task_type"], json.dumps(task_payload),
            ident.payload_hash(task_payload), driver["task_key"],
            driver["max_attempts"])
        await Q.recompute_job_counters(conn, row["id"])
    return str(row["id"])


__all__ = [
    "parse_cron", "next_cron_occurrence", "next_market_daily_occurrence",
    "compute_next_run_at", "preview_occurrences", "run_scheduler_tick",
    "_schedule_is_ownable",
]
