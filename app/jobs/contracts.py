"""Job-queue contracts: versions, statuses, error taxonomy, backoff, payloads.

Pure, dependency-free constants and helpers (no DB, no strategy math) so both
the queue service and the worker share exactly one definition of the state
machine. Everything here is safe to import from tests and from the web app.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# --- contract versions -----------------------------------------------------
PROSPECTIVE_CAMPAIGN_ENQUEUE_CONTRACT = "prospective_campaign_enqueue.v1"
PROSPECTIVE_SYMBOL_EVALUATION_TASK = "prospective_symbol_evaluation.v1"

# job_type for the prospective campaign parent job.
JOB_TYPE_PROSPECTIVE_CAMPAIGN = "prospective_campaign"
PROSPECTIVE_QUEUE = "prospective"

# --- prospective outcome maturation (Concept A shared market-path only) ----
# Reuses the existing pair-level `strategy_shadow_pair_outcomes` schema and
# pure outcome.v1 formulas UNCHANGED; forward bars are read exclusively from
# the LOCAL daily_bars cache (no provider construction) — see
# app/jobs/prospective_outcome_local_reader.py.
PROSPECTIVE_OUTCOME_MATURATION_ENQUEUE_CONTRACT = "prospective_outcome_maturation_enqueue.v1"
PROSPECTIVE_OUTCOME_MATURATION_TASK = "prospective_outcome_maturation.v1"
JOB_TYPE_PROSPECTIVE_OUTCOME_MATURATION = "prospective_outcome_maturation"
PROSPECTIVE_OUTCOME_QUEUE = "prospective_outcomes"

# --- job states ------------------------------------------------------------
JOB_QUEUED = "queued"
JOB_RUNNING = "running"
JOB_SUCCEEDED = "succeeded"
JOB_FAILED = "failed"
JOB_CANCEL_REQUESTED = "cancel_requested"
JOB_CANCELLED = "cancelled"
JOB_STATUSES = (
    JOB_QUEUED, JOB_RUNNING, JOB_SUCCEEDED, JOB_FAILED,
    JOB_CANCEL_REQUESTED, JOB_CANCELLED,
)
JOB_TERMINAL = (JOB_SUCCEEDED, JOB_FAILED, JOB_CANCELLED)

# --- task states -----------------------------------------------------------
TASK_QUEUED = "queued"
TASK_LEASED = "leased"
TASK_RUNNING = "running"
TASK_RETRYABLE = "retryable"
TASK_SUCCEEDED = "succeeded"
TASK_FAILED = "failed"
TASK_CANCELLED = "cancelled"
TASK_STATUSES = (
    TASK_QUEUED, TASK_LEASED, TASK_RUNNING, TASK_RETRYABLE,
    TASK_SUCCEEDED, TASK_FAILED, TASK_CANCELLED,
)
TASK_CLAIMABLE = (TASK_QUEUED, TASK_RETRYABLE)
TASK_ACTIVE = (TASK_LEASED, TASK_RUNNING)
TASK_TERMINAL = (TASK_SUCCEEDED, TASK_FAILED, TASK_CANCELLED)

# --- campaign states (mirrored onto prospective_campaign_registrations) -----
CAMPAIGN_QUEUED = "queued"
CAMPAIGN_EXECUTING = "executing"
CAMPAIGN_COMPLETED = "completed"
CAMPAIGN_FAILED = "failed"
CAMPAIGN_CANCELLED = "cancelled"

# --- error taxonomy --------------------------------------------------------
ERR_RETRYABLE = "retryable"
ERR_TERMINAL = "terminal"
ERR_OPERATOR = "operator_error"
ERR_CANCELLED = "cancelled"
ERROR_CLASSES = (ERR_RETRYABLE, ERR_TERMINAL, ERR_OPERATOR, ERR_CANCELLED)


class JobError(Exception):
    """Base class carrying a bounded, secret-free ``safe_error_code`` and an
    error class that drives retry vs terminal handling."""

    error_class = ERR_TERMINAL

    def __init__(self, safe_error_code: str, message: str = "") -> None:
        self.safe_error_code = (safe_error_code or "unknown_error")[:80]
        super().__init__(message or self.safe_error_code)


class RetryableJobError(JobError):
    """Transient failure — DB blip, worker death, lease lost before durable
    completion, transient persistence conflict. Retried within policy."""

    error_class = ERR_RETRYABLE


class TerminalJobError(JobError):
    """Permanent failure that must NOT be retried indefinitely — invalid
    registration, stale immutable identity, history not ready, future-bar
    leakage, strategy-contract mismatch, invalid payload, config/auth error."""

    error_class = ERR_TERMINAL


class OperatorRetryableError(TerminalJobError):
    """Terminal now, but explicitly eligible for an operator-initiated retry."""

    error_class = ERR_OPERATOR


# --- retry / backoff -------------------------------------------------------
def backoff_seconds(attempt_count: int, schedule: Optional[List[int]] = None,
                    jitter_seconds: int = 0) -> Optional[int]:
    """Delay before the NEXT attempt after ``attempt_count`` completed attempts.

    schedule defaults to [60, 300]: after attempt 1 -> 60s, after attempt 2 ->
    300s, after attempt 3 -> None (terminal, no further attempt). Jitter is 0 by
    default (deterministic for tests); a caller may pass a controlled, bounded
    jitter. Returns None when no further attempt is allowed.
    """
    sched = list(schedule if schedule is not None else (60, 300))
    idx = attempt_count - 1
    if idx < 0 or idx >= len(sched):
        return None
    base = sched[idx]
    return max(0, base + max(0, jitter_seconds))


# --- typed prospective task payload ----------------------------------------
PROSPECTIVE_PAYLOAD_FIELDS = (
    "registration_id",
    "registration_identity",
    "universe_id",
    "universe_hash",
    "history_readiness_manifest_hash",
    "snapshot_session_date",
    "snapshot_cutoff_at",
    "symbol",
    "ordinal",
    "candidate_strategy_identity",
    "control_strategy_identity",
    "candidate_signal_definition",
)


@dataclass(frozen=True)
class ProspectiveSymbolPayload:
    """Server-pinned references only. The worker re-loads and re-validates every
    identity against the immutable registration — it never trusts these values
    as authoritative, only as the addressing of which frozen symbol to evaluate."""

    registration_id: str
    registration_identity: str
    universe_id: str
    universe_hash: str
    history_readiness_manifest_hash: str
    snapshot_session_date: str
    snapshot_cutoff_at: str
    symbol: str
    ordinal: int
    candidate_strategy_identity: str
    control_strategy_identity: str
    candidate_signal_definition: str

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ProspectiveSymbolPayload":
        missing = [k for k in PROSPECTIVE_PAYLOAD_FIELDS if k not in d]
        if missing:
            raise TerminalJobError(
                "invalid_task_payload",
                f"prospective payload missing fields: {missing}")
        try:
            return cls(
                registration_id=str(d["registration_id"]),
                registration_identity=str(d["registration_identity"]),
                universe_id=str(d["universe_id"]),
                universe_hash=str(d["universe_hash"]),
                history_readiness_manifest_hash=str(d["history_readiness_manifest_hash"]),
                snapshot_session_date=str(d["snapshot_session_date"]),
                snapshot_cutoff_at=str(d["snapshot_cutoff_at"]),
                symbol=str(d["symbol"]).upper(),
                ordinal=int(d["ordinal"]),
                candidate_strategy_identity=str(d["candidate_strategy_identity"]),
                control_strategy_identity=str(d["control_strategy_identity"]),
                candidate_signal_definition=str(d["candidate_signal_definition"]),
            )
        except (TypeError, ValueError) as exc:
            raise TerminalJobError("invalid_task_payload", str(exc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "registration_id": self.registration_id,
            "registration_identity": self.registration_identity,
            "universe_id": self.universe_id,
            "universe_hash": self.universe_hash,
            "history_readiness_manifest_hash": self.history_readiness_manifest_hash,
            "snapshot_session_date": self.snapshot_session_date,
            "snapshot_cutoff_at": self.snapshot_cutoff_at,
            "symbol": self.symbol,
            "ordinal": self.ordinal,
            "candidate_strategy_identity": self.candidate_strategy_identity,
            "control_strategy_identity": self.control_strategy_identity,
            "candidate_signal_definition": self.candidate_signal_definition,
        }


# --- typed outcome-maturation task payload ---------------------------------
OUTCOME_PAYLOAD_FIELDS = (
    "registration_id",
    "registration_identity",
    "campaign_id",
    "campaign_run_id",
    "pair_id",
    "symbol",
)


@dataclass(frozen=True)
class ProspectiveOutcomePayload:
    """Server-pinned references only, re-validated against the immutable
    registration/pair before any read or write — the payload only addresses
    which frozen pair to mature, exactly like ProspectiveSymbolPayload does
    for evaluation tasks."""

    registration_id: str
    registration_identity: str
    campaign_id: str
    campaign_run_id: str
    pair_id: str
    symbol: str

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ProspectiveOutcomePayload":
        missing = [k for k in OUTCOME_PAYLOAD_FIELDS if k not in d]
        if missing:
            raise TerminalJobError(
                "invalid_task_payload",
                f"outcome payload missing fields: {missing}")
        try:
            return cls(
                registration_id=str(d["registration_id"]),
                registration_identity=str(d["registration_identity"]),
                campaign_id=str(d["campaign_id"]),
                campaign_run_id=str(d["campaign_run_id"]),
                pair_id=str(d["pair_id"]),
                symbol=str(d["symbol"]).upper(),
            )
        except (TypeError, ValueError) as exc:
            raise TerminalJobError("invalid_task_payload", str(exc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "registration_id": self.registration_id,
            "registration_identity": self.registration_identity,
            "campaign_id": self.campaign_id,
            "campaign_run_id": self.campaign_run_id,
            "pair_id": self.pair_id,
            "symbol": self.symbol,
        }


__all__ = [
    "PROSPECTIVE_CAMPAIGN_ENQUEUE_CONTRACT",
    "PROSPECTIVE_SYMBOL_EVALUATION_TASK",
    "JOB_TYPE_PROSPECTIVE_CAMPAIGN",
    "PROSPECTIVE_QUEUE",
    "PROSPECTIVE_OUTCOME_MATURATION_ENQUEUE_CONTRACT",
    "PROSPECTIVE_OUTCOME_MATURATION_TASK",
    "JOB_TYPE_PROSPECTIVE_OUTCOME_MATURATION",
    "PROSPECTIVE_OUTCOME_QUEUE",
    "ProspectiveOutcomePayload", "OUTCOME_PAYLOAD_FIELDS",
    "JOB_STATUSES", "JOB_TERMINAL",
    "JOB_QUEUED", "JOB_RUNNING", "JOB_SUCCEEDED", "JOB_FAILED",
    "JOB_CANCEL_REQUESTED", "JOB_CANCELLED",
    "TASK_STATUSES", "TASK_CLAIMABLE", "TASK_ACTIVE", "TASK_TERMINAL",
    "TASK_QUEUED", "TASK_LEASED", "TASK_RUNNING", "TASK_RETRYABLE",
    "TASK_SUCCEEDED", "TASK_FAILED", "TASK_CANCELLED",
    "CAMPAIGN_QUEUED", "CAMPAIGN_EXECUTING", "CAMPAIGN_COMPLETED",
    "CAMPAIGN_FAILED", "CAMPAIGN_CANCELLED",
    "ERROR_CLASSES", "ERR_RETRYABLE", "ERR_TERMINAL", "ERR_OPERATOR", "ERR_CANCELLED",
    "JobError", "RetryableJobError", "TerminalJobError", "OperatorRetryableError",
    "backoff_seconds",
    "ProspectiveSymbolPayload", "PROSPECTIVE_PAYLOAD_FIELDS",
]
