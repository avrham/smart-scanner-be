"""Durable market-data jobs (Phase 7A).

Persistence + orchestration for the `market_data_jobs` table (migration 006).
Initially supports one job type: market-cap enrichment.

Design:
  * Duplicate protection is DURABLE — the partial unique index
    `market_data_jobs_active_uniq` rejects a second queued/running job for the
    same (job_type, provider, trading_date). No in-memory lock is trusted.
  * State transitions are enforced in SQL WHERE clauses (queued -> running,
    running -> completed/failed), so a lost race can never double-transition.
  * Stale-job recovery: a `running` job whose updated_at is older than the
    configured timeout is marked failed, so a process restart cannot leave a
    phantom running job blocking new work forever.
  * All timestamps are timezone-aware UTC.
  * Errors are sanitized before persistence: type + capped message, secrets
    scrubbed, never a traceback.
"""

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import asyncpg

from app.workers.persistence import get_db_connection, release_db_connection


logger = logging.getLogger(__name__)

JOB_TYPE_ENRICHMENT = "market_cap_enrichment"
ACTIVE_STATUSES = ("queued", "running")
JOB_STATUSES = ("queued", "running", "completed", "failed", "cancelled")

# Bounds persisted payloads (defense in depth; enrichment already caps at 25).
MAX_PERSISTED_SYMBOLS = 25
MAX_ERROR_LENGTH = 300

# Patterns that must never reach the database or logs.
_SECRET_PATTERNS = [
    re.compile(r"apiKey=[^&\s\"']+", re.IGNORECASE),
    # Header-style values may contain spaces (e.g. "Bearer <token>") — mask to
    # the end of the line.
    re.compile(r"(authorization|x-worker-token)\s*[:=]\s*[^\n]+", re.IGNORECASE),
    re.compile(r"(api[_-]?key|token|secret|password)\s*[:=]\s*\S+", re.IGNORECASE),
]


class DuplicateActiveJobError(Exception):
    """A queued or running job already exists for this type/provider/date."""


def utcnow() -> datetime:
    """Timezone-aware UTC now (jobs never use naive datetimes)."""
    return datetime.now(timezone.utc)


def sanitize_error(exc: BaseException) -> str:
    """Safe, bounded error text: exception type + scrubbed message.

    Never includes a traceback; API keys / tokens / auth headers are masked.
    """
    text = f"{type(exc).__name__}: {exc}"
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(lambda m: m.group(0).split("=")[0].split(":")[0] + "=***", text)
    return text[:MAX_ERROR_LENGTH]


# --------------------------------------------------------------------------- #
# SQL (module constants so tests can match statements exactly)
# --------------------------------------------------------------------------- #

SQL_CREATE_JOB = """
    INSERT INTO market_data_jobs (
        job_type, status, provider, trading_date, requested_limit,
        created_at, updated_at
    )
    VALUES ($1, 'queued', $2, $3, $4, $5, $5)
    RETURNING id
"""

SQL_GET_JOB = """
    SELECT id, job_type, status, provider, trading_date, requested_limit,
           selection_strategy, selected_symbols, progress, result, error,
           created_at, started_at, finished_at, updated_at
    FROM market_data_jobs
    WHERE id = $1
"""

SQL_LIST_JOBS = """
    SELECT id, job_type, status, provider, trading_date, requested_limit,
           selection_strategy, selected_symbols, progress, result, error,
           created_at, started_at, finished_at, updated_at
    FROM market_data_jobs
    WHERE ($1::text IS NULL OR job_type = $1)
      AND ($2::text IS NULL OR status = $2)
      AND ($3::text IS NULL OR provider = $3)
      AND ($4::date IS NULL OR trading_date = $4)
    ORDER BY created_at DESC
    LIMIT $5
"""

SQL_MARK_RUNNING = """
    UPDATE market_data_jobs
    SET status = 'running', started_at = $2, updated_at = $2
    WHERE id = $1 AND status = 'queued'
"""

SQL_UPDATE_PROGRESS = """
    UPDATE market_data_jobs
    SET progress = $2::jsonb, updated_at = $3
    WHERE id = $1 AND status = 'running'
"""

SQL_COMPLETE_JOB = """
    UPDATE market_data_jobs
    SET status = 'completed', result = $2::jsonb, selected_symbols = $3::jsonb,
        selection_strategy = $4, finished_at = $5, updated_at = $5
    WHERE id = $1 AND status = 'running'
"""

SQL_FAIL_JOB = """
    UPDATE market_data_jobs
    SET status = 'failed', error = $2, finished_at = $3, updated_at = $3
    WHERE id = $1 AND status IN ('queued', 'running')
"""

# Fixed, safe error messages for recovered jobs. The leading token is the
# stable error code (queued_job_timeout | stale_job_timeout).
QUEUED_TIMEOUT_ERROR = "queued_job_timeout: queued job never started; recovered"
RUNNING_TIMEOUT_ERROR = "stale_job_timeout: recovered after process restart or hang"

SQL_RECOVER_STALE = """
    UPDATE market_data_jobs
    SET status = 'failed',
        error = CASE
            WHEN status = 'queued'
                THEN 'queued_job_timeout: queued job never started; recovered'
            ELSE 'stale_job_timeout: recovered after process restart or hang'
        END,
        finished_at = $2, updated_at = $2
    WHERE status IN ('queued', 'running') AND updated_at < $1
"""


def _updated_count(result: Any) -> int:
    """Parse asyncpg's 'UPDATE n' command tag."""
    try:
        return int(str(result).split()[-1])
    except (ValueError, IndexError):
        return 0


def _parse_json_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """asyncpg returns JSONB as str by default; normalize to Python objects."""
    out = dict(row)
    for key in ("selected_symbols", "progress", "result"):
        value = out.get(key)
        if isinstance(value, str):
            try:
                out[key] = json.loads(value)
            except (ValueError, TypeError):
                pass
    if out.get("id") is not None:
        out["id"] = str(out["id"])
    if out.get("trading_date") is not None:
        out["trading_date"] = str(out["trading_date"])
    return out


# --------------------------------------------------------------------------- #
# Repository
# --------------------------------------------------------------------------- #

async def create_job(
    job_type: str,
    provider: str,
    trading_date: Optional[date],
    requested_limit: Optional[int],
) -> str:
    """Insert a queued job. Raises DuplicateActiveJobError when an active
    (queued/running) job already exists for the same type/provider/date.

    market_cap_enrichment REQUIRES a resolved trading_date: PostgreSQL unique
    indexes treat NULLs as distinct, so a NULL date would bypass duplicate
    protection. Enforced here and by the migration-006 CHECK constraint.
    """
    if job_type == JOB_TYPE_ENRICHMENT and trading_date is None:
        raise ValueError(
            "market_cap_enrichment jobs require a resolved trading_date "
            "(NULL would bypass duplicate protection)"
        )
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(
            SQL_CREATE_JOB, job_type, provider, trading_date, requested_limit, utcnow()
        )
        return str(row["id"])
    except asyncpg.exceptions.UniqueViolationError:
        raise DuplicateActiveJobError(
            f"An active {job_type} job already exists for provider={provider}, "
            f"trading_date={trading_date}"
        )
    finally:
        await release_db_connection(conn)


async def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(SQL_GET_JOB, job_id)
        return _parse_json_fields(dict(row)) if row else None
    finally:
        await release_db_connection(conn)


async def list_jobs(
    job_type: Optional[str] = None,
    status: Optional[str] = None,
    provider: Optional[str] = None,
    trading_date: Optional[date] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Bounded, filtered job listing (newest first). Filters compose with AND
    semantics; provider is an exact normalized (lowercase) match."""
    bounded_limit = max(1, min(int(limit), 200))
    normalized_provider = provider.strip().lower() if provider else None
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(
            SQL_LIST_JOBS, job_type, status, normalized_provider, trading_date,
            bounded_limit,
        )
        return [_parse_json_fields(dict(r)) for r in rows]
    finally:
        await release_db_connection(conn)


async def mark_running(job_id: str) -> bool:
    """queued -> running. Returns False if the job was not in 'queued'."""
    conn = await get_db_connection()
    try:
        result = await conn.execute(SQL_MARK_RUNNING, job_id, utcnow())
        return _updated_count(result) == 1
    finally:
        await release_db_connection(conn)


async def update_progress(job_id: str, progress: Dict[str, Any]) -> None:
    """Persist bounded progress counters. Best-effort: never raises."""
    try:
        conn = await get_db_connection()
        try:
            await conn.execute(
                SQL_UPDATE_PROGRESS, job_id, json.dumps(progress, default=str), utcnow()
            )
        finally:
            await release_db_connection(conn)
    except Exception as exc:
        logger.warning("progress update failed for job %s: %s", job_id, type(exc).__name__)


async def complete_job(
    job_id: str,
    result: Dict[str, Any],
    selected_symbols: Optional[List[str]] = None,
    selection_strategy: Optional[str] = None,
) -> bool:
    """running -> completed with the safe result summary."""
    symbols = (selected_symbols or [])[:MAX_PERSISTED_SYMBOLS]
    conn = await get_db_connection()
    try:
        cmd = await conn.execute(
            SQL_COMPLETE_JOB,
            job_id,
            json.dumps(result, default=str),
            json.dumps(symbols),
            selection_strategy,
            utcnow(),
        )
        return _updated_count(cmd) == 1
    finally:
        await release_db_connection(conn)


async def fail_job(job_id: str, error: str) -> bool:
    """queued/running -> failed with a sanitized, bounded error message."""
    conn = await get_db_connection()
    try:
        cmd = await conn.execute(SQL_FAIL_JOB, job_id, error[:MAX_ERROR_LENGTH], utcnow())
        return _updated_count(cmd) == 1
    finally:
        await release_db_connection(conn)


async def recover_stale_jobs(timeout_minutes: int) -> int:
    """Fail active jobs (queued OR running) whose updated_at is older than the
    timeout. Both states block the partial unique index, so both must be
    recoverable: queued jobs get error code `queued_job_timeout`, running jobs
    get `stale_job_timeout`. One timeout (MARKET_DATA_JOB_STALE_MINUTES) covers
    both states.

    Called before creating a new job so a crashed process can never block new
    work indefinitely. Returns the number of jobs recovered."""
    cutoff = utcnow() - timedelta(minutes=timeout_minutes)
    conn = await get_db_connection()
    try:
        cmd = await conn.execute(SQL_RECOVER_STALE, cutoff, utcnow())
        recovered = _updated_count(cmd)
        if recovered:
            logger.warning("recovered %d stale running market-data job(s)", recovered)
        return recovered
    finally:
        await release_db_connection(conn)


# --------------------------------------------------------------------------- #
# Enrichment job runner
# --------------------------------------------------------------------------- #

async def run_enrichment_job(
    job_id: str,
    provider: Any,
    trading_date: date,
    max_detail_calls: int,
) -> None:
    """Execute a market-cap enrichment job with durable state transitions.

    queued -> running -> completed (or failed). Enrichment behavior itself is
    unchanged: deterministic prioritization, rate limiter, fresh-profile
    skipping and the max_detail_calls bound all live in the provider. Partial
    results remain valid — ticker profiles are written per symbol, so a
    mid-run failure keeps everything enriched so far.
    """
    if not await mark_running(job_id):
        logger.warning("job %s not in 'queued' state; skipping run", job_id)
        return

    async def on_progress(progress: Dict[str, Any]) -> None:
        await update_progress(job_id, progress)

    try:
        summary = await provider.enrich_market_caps(
            trading_date,
            max_detail_calls=max_detail_calls,
            progress_callback=on_progress,
        )
        await complete_job(
            job_id,
            result=summary,
            selected_symbols=summary.get("selected_symbols"),
            selection_strategy=summary.get("selection_strategy"),
        )
        logger.info("enrichment job %s completed", job_id)
    except Exception as exc:
        safe = sanitize_error(exc)
        await fail_job(job_id, safe)
        logger.error("enrichment job %s failed: %s", job_id, safe)
