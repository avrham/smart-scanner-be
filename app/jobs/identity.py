"""Deterministic, collision-resistant idempotency-key derivation for the queue.

Keys are sha256 over canonical (sorted-key, compact) JSON of only stable,
server-pinned fields — so an exact replay derives the SAME key and the UNIQUE
constraints in migration 018 make enqueue/task creation naturally idempotent.
Pure and dependency-free.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict


def _canonical(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(prefix: str, obj: Dict[str, Any]) -> str:
    digest = hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


def payload_hash(payload: Dict[str, Any]) -> str:
    """Content hash of a task payload (binds the payload to its attempts)."""
    return _sha256("pph", payload)


def job_idempotency_key(*, job_type: str, registration_identity: str,
                        campaign_execution_identity: str) -> str:
    """One parent job per (job_type, registration identity, campaign execution
    identity). An exact enqueue replay derives the same key."""
    return _sha256("job", {
        "job_type": job_type,
        "registration_identity": registration_identity,
        "campaign_execution_identity": campaign_execution_identity,
    })


def prospective_task_idempotency_key(*, registration_identity: str,
                                     campaign_execution_identity: str,
                                     symbol: str,
                                     snapshot_session_date: str,
                                     snapshot_cutoff_at: str,
                                     candidate_strategy_identity: str,
                                     control_strategy_identity: str) -> str:
    """One task per (registration identity, campaign execution identity, symbol,
    snapshot identity, candidate identity, control identity) — exactly the
    binding required by the task spec so a replay never double-creates."""
    return _sha256("task", {
        "registration_identity": registration_identity,
        "campaign_execution_identity": campaign_execution_identity,
        "symbol": symbol.upper(),
        "snapshot_session_date": snapshot_session_date,
        "snapshot_cutoff_at": snapshot_cutoff_at,
        "candidate_strategy_identity": candidate_strategy_identity,
        "control_strategy_identity": control_strategy_identity,
    })


def prospective_outcome_job_idempotency_key(*, job_type: str, registration_identity: str,
                                            campaign_execution_identity: str,
                                            eligible_pairs: list) -> str:
    """One outcome-maturation job per (campaign, BATCH). Unlike the campaign
    evaluation job (exactly one job for its whole lifetime), outcome
    maturation is inherently multi-round — a later enqueue call may target
    the SAME pairs with MORE forward sessions now available (still-partial
    pairs remain eligible as history advances), a different now-eligible set
    (a retryable error became eligible again), or both — and must NOT
    collide with an earlier round's already-succeeded job. ``eligible_pairs``
    is an iterable of (pair_id, completed_forward_sessions) tuples: the key
    changes whenever EITHER the eligible set OR any pair's observed session
    count changes, and stays identical for a true exact replay. Mirrors this
    repo's own established batch-hash pattern (progressive locked
    shadow-outcome maturation) rather than inventing a new idiom."""
    return _sha256("ojob", {
        "job_type": job_type,
        "registration_identity": registration_identity,
        "campaign_execution_identity": campaign_execution_identity,
        "eligible_pairs": sorted(
            [str(pid), int(sessions)] for pid, sessions in eligible_pairs),
    })


def prospective_outcome_task_idempotency_key(*, job_idempotency_key: str, pair_id: str) -> str:
    """One outcome-maturation task per (BATCH, frozen pair id) — scoped by
    the parent job's own (round-aware) idempotency key, not just the pair,
    because outcome maturation is multi-round: the same pair legitimately
    gets a NEW task in a later round (more forward sessions arrived). Task
    uniqueness must track the job's round scoping exactly, or a second
    round's INSERT ... ON CONFLICT DO NOTHING would silently collide with an
    earlier round's already-succeeded task for the same pair and insert
    nothing — a job with the correct total_task_count but zero real rows.
    Deliberately excludes strategy identity — a pair's shared market-path
    outcome (Concept A) does not depend on which arm's strategy version
    produced the pair, only on the pair itself."""
    return _sha256("otask", {
        "job_idempotency_key": job_idempotency_key,
        "pair_id": str(pair_id),
    })


def schedule_occurrence_idempotency_key(*, schedule_code: str, schedule_version: int,
                                        occurrence_iso: str) -> str:
    """One job per scheduled occurrence — prevents an occurrence being enqueued
    twice even if two scheduler ticks race."""
    return _sha256("sch", {
        "schedule_code": schedule_code,
        "schedule_version": schedule_version,
        "occurrence": occurrence_iso,
    })


def pipeline_occurrence_identity(*, schedule_code: str, schedule_version: int,
                                 resolved_session_date: str, frozen_universe_hash: str,
                                 pipeline_contract_version: str) -> str:
    """One daily-pipeline occurrence per (schedule, resolved completed session,
    frozen universe, pipeline contract). A repeated tick for the same resolved
    session and the same frozen universe always resumes the SAME occurrence;
    a later completed session, or a genuinely different universe, is a new one."""
    return _sha256("dpo", {
        "schedule_code": schedule_code,
        "schedule_version": int(schedule_version),
        "resolved_session_date": str(resolved_session_date),
        "frozen_universe_hash": str(frozen_universe_hash),
        "pipeline_contract_version": pipeline_contract_version,
    })


__all__ = [
    "payload_hash",
    "job_idempotency_key",
    "prospective_task_idempotency_key",
    "prospective_outcome_job_idempotency_key",
    "prospective_outcome_task_idempotency_key",
    "schedule_occurrence_idempotency_key",
    "pipeline_occurrence_identity",
]
