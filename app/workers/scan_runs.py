"""Canonical scan-run identity (Phase 7B).

pattern_runs is the single durable scan-run table (migration 007 extends it).
The UUID returned by POST /api/admin/scan/start — the same one the scan
WebSocket subscribes to — is now the pattern_runs row id, created at scan
START (status='running') and finalized at scan end. Persisted signals link to
it via signal_provenance.scan_run_id (real FK).

No second scan identity exists: legacy `log_pattern_run` rows (random id,
written only at scan end) remain readable but new scans go through
create/finalize here.
"""

import json
import logging
import uuid as uuid_lib
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional

from app.workers.persistence import get_db_connection, release_db_connection


logger = logging.getLogger(__name__)


async def create_scan_run(
    scan_run_id: str,
    pattern_code: str,
    scanner_mode: str,
    provider: Optional[str] = None,
    dry_run: bool = False,
    requested_limit: Optional[int] = None,
    scan_date: Optional[date] = None,
    run_started_at: Optional[datetime] = None,
) -> str:
    """Create the canonical scan-run row at scan START (status='running').

    Idempotent on id (ON CONFLICT DO NOTHING) so a retried start cannot create
    a duplicate identity.
    """
    if run_started_at is None:
        run_started_at = datetime.now(timezone.utc)

    conn = await get_db_connection()
    try:
        await conn.execute(
            """
            INSERT INTO pattern_runs (
                id, pattern_code, run_started_at, scanned_count, enter_count,
                rejected_count, scanner_mode, status, provider, dry_run,
                requested_limit, scan_date, created_at, updated_at
            )
            VALUES ($1, $2, $3, 0, 0, 0, $4, 'running', $5, $6, $7, $8, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            uuid_lib.UUID(str(scan_run_id)),
            pattern_code,
            run_started_at,
            scanner_mode,
            provider,
            dry_run,
            requested_limit,
            scan_date,
        )
        logger.info(
            "Created scan run %s (%s/%s, provider=%s, dry_run=%s)",
            scan_run_id, scanner_mode, pattern_code, provider, dry_run,
        )
        return str(scan_run_id)
    finally:
        await release_db_connection(conn)


def sanitize_scan_error(message: Optional[str], max_len: int = 500) -> Optional[str]:
    """Bound and scrub an error message before persisting it on the scan run.

    Reuses the market-jobs sanitizer (masks API keys, tokens, DSNs, auth
    headers) so a scan failure can never leak a secret into pattern_runs.
    """
    if not message:
        return None
    from app.workers.market_jobs import _SECRET_PATTERNS

    text = str(message)
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(
            lambda m: m.group(0).split("=")[0].split(":")[0] + "=***", text
        )
    return text[:max_len]


async def finalize_scan_run(
    scan_run_id: str,
    status: str,
    scanned_count: int,
    enter_count: int,
    rejected_count: int,
    telemetry: Optional[Dict[str, Any]] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Finalize the canonical scan-run row at scan end (completed OR failed).

    Writes counts, status, finished_at, safe error identity (failed runs) and
    the structured telemetry (JSONB column + legacy notes TEXT for the
    pattern-runs API). Handled failures must always pass status='failed' so no
    handled exception path leaves a scan stuck in 'running'.
    """
    conn = await get_db_connection()
    try:
        telemetry_json = json.dumps(telemetry) if telemetry is not None else None
        await conn.execute(
            """
            UPDATE pattern_runs
            SET status = $2,
                scanned_count = $3,
                enter_count = $4,
                rejected_count = $5,
                telemetry = $6,
                notes = COALESCE($6, notes),
                error_code = $7,
                error_message = $8,
                finished_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            """,
            uuid_lib.UUID(str(scan_run_id)),
            status,
            scanned_count,
            enter_count,
            rejected_count,
            telemetry_json,
            error_code,
            sanitize_scan_error(error_message),
        )
    finally:
        await release_db_connection(conn)
