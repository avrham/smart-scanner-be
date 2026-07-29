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


def schedule_occurrence_idempotency_key(*, schedule_code: str, schedule_version: int,
                                        occurrence_iso: str) -> str:
    """One job per scheduled occurrence — prevents an occurrence being enqueued
    twice even if two scheduler ticks race."""
    return _sha256("sch", {
        "schedule_code": schedule_code,
        "schedule_version": schedule_version,
        "occurrence": occurrence_iso,
    })


__all__ = [
    "payload_hash",
    "job_idempotency_key",
    "prospective_task_idempotency_key",
    "schedule_occurrence_idempotency_key",
]
