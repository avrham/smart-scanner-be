"""Read-only maturity preflight + durable enqueue for outcome maturation.

Mirrors app/jobs/prospective_enqueue.py's shape (one parent job + bounded
per-unit tasks, deterministic idempotency, replay-safe) but for maturing the
shared pair-level (Concept A) market-path outcome of an already-COMPLETED
campaign's 25 frozen pairs. Reuses the existing eligibility classifier
(app.workers.shadow.outcomes.eligibility) UNCHANGED — session-based
maturity, never calendar days — fed with a LOCAL trading-session calendar
derived from the campaign's own frozen universe (no SPY dependency: this
universe does not include SPY/QQQ). NO strategy evaluation, NO provider
construction, NO outcome calculation happens in preflight or enqueue — only
selection and durable task creation.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

import asyncpg

from app.jobs import contracts as C
from app.jobs import identity as ident
from app.jobs import queue as Q
from app.jobs.contracts import TerminalJobError
from app.jobs.prospective_outcome_local_reader import local_session_dates
from app.workers.shadow.outcomes.eligibility import (
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_MATURED,
    ELIGIBILITY_MISSING_SESSION_DATA,
    ELIGIBILITY_NOT_YET,
    ELIGIBILITY_RETRYABLE,
    ELIGIBILITY_TERMINAL,
    ELIGIBILITY_UNKNOWN,
    FULL_MATURATION_SESSIONS,
    MIN_MATURATION_SESSIONS,
    classify_maturation_eligibility,
    completed_forward_sessions,
)
from app.workers.outcomes.calculator import HOLDING_WINDOWS, window_label

ENQUEUE_ELIGIBLE_STATES = (ELIGIBILITY_ELIGIBLE, ELIGIBILITY_RETRYABLE)


async def _load_registration(conn: asyncpg.Connection, registration_id: str,
                             registration_identity: str) -> Dict[str, Any]:
    reg = await conn.fetchrow(
        "SELECT * FROM prospective_campaign_registrations WHERE id=$1", registration_id)
    if reg is None:
        raise TerminalJobError("invalid_registration", "registration not found")
    if reg["registration_identity"] != registration_identity:
        raise TerminalJobError("stale_registration_identity", "identity mismatch")
    if reg["status"] != "completed":
        raise TerminalJobError("campaign_not_completed", f"status={reg['status']}")
    if not reg["campaign_run_id"]:
        raise TerminalJobError("campaign_not_initialized", "campaign_run_id unset")
    return dict(reg)


async def _pairs_with_outcome_state(conn: asyncpg.Connection, run_id: str
                                    ) -> List[Dict[str, Any]]:
    """Every frozen pair under this campaign run + its current outcome state
    (status/error_code), unfiltered by eligibility — the preflight/enqueue
    classifier decides what to do with each, not the SQL."""
    rows = await conn.fetch(
        "SELECT p.id AS pair_id, p.symbol, p.provider, p.snapshot_date, "
        "p.pair_fingerprint, p.pair_fingerprint_version, "
        "o.outcome_status, o.error_code, o.available_forward_bars "
        "FROM strategy_shadow_run_pairs rp "
        "JOIN strategy_shadow_pairs p ON p.id = rp.pair_id "
        "LEFT JOIN strategy_shadow_pair_outcomes o ON o.pair_id = p.id "
        "WHERE rp.run_id = $1 ORDER BY p.symbol", run_id)
    return [dict(r) for r in rows]


async def _classify_all(conn: asyncpg.Connection, reg: Dict[str, Any]
                        ) -> Dict[str, Any]:
    pairs = await _pairs_with_outcome_state(conn, str(reg["campaign_run_id"]))
    symbols = sorted({p["symbol"] for p in pairs})
    min_snapshot = min((p["snapshot_date"] for p in pairs), default=None)
    session_dates = (await local_session_dates(symbols, after=min_snapshot)
                     if symbols and min_snapshot else [])
    latest_completed_session = max(session_dates) if session_dates else None

    classified: List[Dict[str, Any]] = []
    for p in pairs:
        cfs = completed_forward_sessions(
            session_dates, p["snapshot_date"],
            latest_completed_session=latest_completed_session)
        eligibility = classify_maturation_eligibility(
            outcome_status=p["outcome_status"], error_code=p["error_code"],
            completed_forward_sessions=cfs)
        classified.append({**p, "completed_forward_sessions": cfs,
                           "eligibility": eligibility})
    return {"pairs": classified, "session_dates": session_dates,
            "latest_completed_session": latest_completed_session}


def _manifest_hash(reg: Dict[str, Any], classified: List[Dict[str, Any]]) -> str:
    """Stable identity over the CURRENT maturity classification — changes
    exactly when eligibility state changes (new session data or a status
    transition), never on unrelated fields."""
    payload = {
        "registration_identity": reg["registration_identity"],
        "campaign_execution_identity": reg["campaign_execution_identity"],
        "horizons": list(HOLDING_WINDOWS),
        "pairs": sorted(
            [{"pair_id": str(p["pair_id"]), "symbol": p["symbol"],
              "eligibility": p["eligibility"],
              "completed_forward_sessions": p["completed_forward_sessions"]}
             for p in classified],
            key=lambda x: x["pair_id"]),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        .encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


async def build_outcome_maturity_preflight(conn: asyncpg.Connection, *,
                                           registration_id: str,
                                           registration_identity: str) -> Dict[str, Any]:
    """Read-only maturity/preflight report. NO provider construction, NO
    outcome calculation, NO write of any kind."""
    reg = await _load_registration(conn, registration_id, registration_identity)
    state = await _classify_all(conn, reg)
    pairs = state["pairs"]
    counts: Dict[str, int] = {}
    for p in pairs:
        counts[p["eligibility"]] = counts.get(p["eligibility"], 0) + 1
    manifest_hash = _manifest_hash(reg, pairs)
    return {
        "contract_version": "prospective_outcome_maturity_preflight.v1",
        "registration_id": str(reg["id"]),
        "registration_identity": reg["registration_identity"],
        "campaign_id": str(reg["campaign_id"]) if reg["campaign_id"] else None,
        "campaign_run_id": str(reg["campaign_run_id"]),
        "pair_count": len(pairs),
        "configured_horizons": [window_label(w) for w in HOLDING_WINDOWS],
        "min_maturation_sessions": MIN_MATURATION_SESSIONS,
        "full_maturation_sessions": FULL_MATURATION_SESSIONS,
        "matured_count": counts.get(ELIGIBILITY_MATURED, 0),
        "eligible_count": counts.get(ELIGIBILITY_ELIGIBLE, 0),
        "not_yet_eligible_count": counts.get(ELIGIBILITY_NOT_YET, 0),
        "retryable_count": counts.get(ELIGIBILITY_RETRYABLE, 0),
        "terminal_count": counts.get(ELIGIBILITY_TERMINAL, 0),
        "missing_market_session_data_count": counts.get(ELIGIBILITY_MISSING_SESSION_DATA, 0),
        "eligibility_unknown_count": counts.get(ELIGIBILITY_UNKNOWN, 0),
        "history_cutoff_used": (state["latest_completed_session"].isoformat()
                                if state["latest_completed_session"] else None),
        "local_session_calendar_source": "campaign_universe_symbols",
        "local_session_dates_count": len(state["session_dates"]),
        "maturity_manifest_hash": manifest_hash,
        "enqueue_available_count": counts.get(ELIGIBILITY_ELIGIBLE, 0) + counts.get(ELIGIBILITY_RETRYABLE, 0),
        "provider_called": False,
        "provider_constructed": False,
    }


async def enqueue_outcome_maturation(conn: asyncpg.Connection, *, registration_id: str,
                                     registration_identity: str,
                                     requested_by: Optional[str] = None) -> Dict[str, Any]:
    """Idempotently enqueue outcome-maturation tasks for MATURE-ELIGIBLE pairs
    only. Returns ``status`` in {queued, already_queued, no_eligible_work}.
    A replay after zero-eligible-work re-evaluates current eligibility (new
    local history may have arrived) rather than caching a negative result."""
    reg = await _load_registration(conn, registration_id, registration_identity)
    state = await _classify_all(conn, reg)
    eligible = [p for p in state["pairs"] if p["eligibility"] in ENQUEUE_ELIGIBLE_STATES]

    if not eligible:
        return {
            "contract_version": C.PROSPECTIVE_OUTCOME_MATURATION_ENQUEUE_CONTRACT,
            "status": "no_eligible_work", "job_id": None,
            "registration_id": str(reg["id"]),
            "campaign_id": str(reg["campaign_id"]) if reg["campaign_id"] else None,
            "eligible_pair_count": 0, "total_task_count": 0,
        }

    # BATCH-scoped key (not just campaign-scoped): outcome maturation is
    # multi-round — an exact replay of the SAME eligible set maps to the same
    # job, but a genuinely later round (a different now-eligible set, e.g.
    # more forward sessions arrived) must get its OWN job, never collide with
    # an earlier round's already-succeeded job.
    job_key = ident.prospective_outcome_job_idempotency_key(
        job_type=C.JOB_TYPE_PROSPECTIVE_OUTCOME_MATURATION,
        registration_identity=reg["registration_identity"],
        campaign_execution_identity=reg["campaign_execution_identity"],
        eligible_pairs=[(p["pair_id"], p["completed_forward_sessions"] or 0) for p in eligible])

    existing = await conn.fetchrow("SELECT * FROM job_runs WHERE idempotency_key=$1", job_key)
    if existing is not None:
        counts = await _job_counts(conn, existing["id"])
        return _response("already_queued" if existing["status"] != C.JOB_SUCCEEDED
                         else "already_applied", existing, reg, counts)

    async with conn.transaction():
        job = await conn.fetchrow(
            "INSERT INTO job_runs (job_type, job_contract_version, queue_name, idempotency_key,"
            " status, priority, registration_id, campaign_id, requested_by, total_task_count,"
            " queued_task_count) "
            "VALUES ($1,$2,$3,$4,'queued',100,$5,$6,$7,$8,$8) "
            "ON CONFLICT (idempotency_key) DO NOTHING RETURNING *",
            C.JOB_TYPE_PROSPECTIVE_OUTCOME_MATURATION, C.PROSPECTIVE_OUTCOME_MATURATION_TASK,
            C.PROSPECTIVE_OUTCOME_QUEUE, job_key, reg["id"],
            reg["campaign_id"], requested_by, len(eligible))
        if job is None:
            job = await conn.fetchrow("SELECT * FROM job_runs WHERE idempotency_key=$1", job_key)
            counts = await _job_counts(conn, job["id"])
            return _response("already_queued", job, reg, counts)

        inserted = 0
        for ordinal, pair in enumerate(eligible):
            payload = {
                "registration_id": str(reg["id"]),
                "registration_identity": reg["registration_identity"],
                "campaign_id": str(reg["campaign_id"]),
                "campaign_run_id": str(reg["campaign_run_id"]),
                "pair_id": str(pair["pair_id"]),
                "symbol": pair["symbol"],
            }
            task_key = pair["symbol"]
            task_idem = ident.prospective_outcome_task_idempotency_key(
                job_idempotency_key=job_key, pair_id=str(pair["pair_id"]))
            row = await conn.fetchrow(
                "INSERT INTO job_tasks (job_id, queue_name, task_type, task_contract_version,"
                " task_key, ordinal, payload, payload_hash, idempotency_key, status, priority,"
                " max_attempts) "
                "VALUES ($1,$2,$3,$3,$4,$5,$6::jsonb,$7,$8,'queued',100,$9) "
                "ON CONFLICT DO NOTHING RETURNING id",
                job["id"], C.PROSPECTIVE_OUTCOME_QUEUE, C.PROSPECTIVE_OUTCOME_MATURATION_TASK,
                task_key, ordinal, json.dumps(payload), ident.payload_hash(payload),
                task_idem, 3)
            if row is not None:
                inserted += 1
        await Q.recompute_job_counters(conn, job["id"])
        await Q.record_event(conn, job_id=job["id"], event_type="job_created",
                             safe_message=requested_by or None,
                             metadata={"total_tasks": len(eligible), "inserted": inserted,
                                       "campaign_id": str(reg["campaign_id"])})
        job = await conn.fetchrow("SELECT * FROM job_runs WHERE id=$1", job["id"])
        counts = await _job_counts(conn, job["id"])
        return _response("queued", job, reg, counts)


async def _job_counts(conn: asyncpg.Connection, job_id: str) -> Dict[str, int]:
    row = await conn.fetchrow(
        "SELECT count(*)::int AS total,"
        " count(*) FILTER (WHERE status='queued')::int AS queued,"
        " count(*) FILTER (WHERE status IN ('leased','running'))::int AS running,"
        " count(*) FILTER (WHERE status='retryable')::int AS retryable,"
        " count(*) FILTER (WHERE status='succeeded')::int AS succeeded,"
        " count(*) FILTER (WHERE status='failed')::int AS failed,"
        " count(*) FILTER (WHERE status='cancelled')::int AS cancelled "
        "FROM job_tasks WHERE job_id=$1", job_id)
    return {k: int(row[k]) for k in row.keys()}


def _response(status: str, job: Any, reg: Dict[str, Any], counts: Dict[str, int]) -> Dict[str, Any]:
    job_id = str(job["id"])
    return {
        "contract_version": C.PROSPECTIVE_OUTCOME_MATURATION_ENQUEUE_CONTRACT,
        "status": status,
        "job_id": job_id,
        "registration_id": str(reg["id"]),
        "campaign_id": str(reg["campaign_id"]) if reg.get("campaign_id") else None,
        "total_task_count": counts["total"],
        "job_status": job["status"],
        "progress_url": f"/api/admin/jobs/{job_id}",
        "tasks_url": f"/api/admin/jobs/{job_id}/tasks",
    }


__all__ = [
    "build_outcome_maturity_preflight", "enqueue_outcome_maturation",
    "ENQUEUE_ELIGIBLE_STATES",
]
