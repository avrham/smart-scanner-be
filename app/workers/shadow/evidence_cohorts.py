"""Explicit versioned cohort analysis for shadow evidence review (9F2).

Contract: shadow_evidence_cohorts.v1.

Cohorts are OVERLAPPING by design (a trigger-confirmed record is also an
evaluated record, usually a daily-ready record, and may be rollout-blocked);
nothing here pretends they partition the data. Every membership rule is an
explicit pure predicate reusing the SAME classification helpers the metrics
layer uses. All functions are pure (no I/O).
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from app.workers.shadow.evidence_review import (
    outcome_maturity,
    readiness_state,
)
from app.workers.shadow.strategy_metrics import (
    TRIGGER_CLASS_ABSENT,
    TRIGGER_CLASS_CONFIRMED,
    TRIGGER_CLASS_CONTRADICTED,
    TRIGGER_CLASS_INSUFFICIENT,
    TRIGGER_CLASS_NOT_EVALUATED,
    TRIGGER_CLASS_WAITING,
    classify_trigger_state,
    is_rollout_blocked,
)


COHORT_CONTRACT_VERSION = "shadow_evidence_cohorts.v1"


def _policy(record: Dict[str, Any]) -> Dict[str, Any]:
    policy = record.get("policy")
    return policy if isinstance(policy, dict) else {}


def _frame_state(record: Dict[str, Any]) -> Optional[str]:
    meta = record.get("four_hour_frame_meta")
    return meta.get("state") if isinstance(meta, dict) else None


# ---- explicit membership predicates (versioned with the contract) --------- #

def is_setup_present(record: Dict[str, Any]) -> bool:
    return _policy(record).get("setup_state") == "valid"


def is_setup_rejected(record: Dict[str, Any]) -> bool:
    return (
        record.get("verdict") == "AVOID"
        and bool(record.get("rejection_reason"))
    )


COHORT_PREDICATES: Dict[str, Callable[[Dict[str, Any]], bool]] = {
    "evaluated": lambda r: True,
    "daily_ready": lambda r: readiness_state(r) == "ready",
    "daily_insufficient": lambda r: readiness_state(r) == "not_ready",
    "daily_readiness_unknown": lambda r: readiness_state(r) == "unknown",
    "four_hour_ready": lambda r: classify_trigger_state(r) in (
        TRIGGER_CLASS_CONFIRMED,
        TRIGGER_CLASS_WAITING,
        TRIGGER_CLASS_CONTRADICTED,
    ),
    "four_hour_insufficient": lambda r: (
        classify_trigger_state(r) == TRIGGER_CLASS_INSUFFICIENT
    ),
    "setup_present": is_setup_present,
    "setup_rejected": is_setup_rejected,
    "trigger_confirmed": lambda r: (
        classify_trigger_state(r) == TRIGGER_CLASS_CONFIRMED
    ),
    "trigger_waiting": lambda r: (
        classify_trigger_state(r) == TRIGGER_CLASS_WAITING
    ),
    "trigger_absent": lambda r: (
        classify_trigger_state(r) == TRIGGER_CLASS_ABSENT
    ),
    "trigger_contradicted": lambda r: (
        classify_trigger_state(r) == TRIGGER_CLASS_CONTRADICTED
    ),
    "trigger_not_evaluated": lambda r: (
        classify_trigger_state(r) == TRIGGER_CLASS_NOT_EVALUATED
    ),
    "rollout_blocked": lambda r: is_rollout_blocked(r) is True,
    # Pair outcomes are verdict-neutral market-path observations: every
    # persisted evaluation's pair is outcome-eligible by contract.
    "outcome_eligible": lambda r: True,
    "matured_outcome": lambda r: outcome_maturity(r) == "matured",
    "pending_outcome": lambda r: outcome_maturity(r) == "pending",
    "missing_outcome": lambda r: outcome_maturity(r) == "missing",
    "provider_failure_4h": lambda r: _frame_state(r) in (
        "fetch_error", "unsupported_provider",
    ),
    "frame_rejected_4h": lambda r: _frame_state(r) == "frame_rejected",
}


def _distribution(values: List[Optional[str]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        if value is None:
            continue
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    iso = getattr(value, "isoformat", None)
    return iso() if callable(iso) else str(value)


def _cohort_summary(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    symbols = sorted({str(r.get("symbol")) for r in records if r.get("symbol")})
    sessions = sorted({
        _iso(r.get("snapshot_date")) for r in records
        if r.get("snapshot_date") is not None
    })
    campaigns = sorted({
        str(c)
        for r in records
        for c in (r.get("campaign_ids") or [])
    })
    created = sorted(
        _iso(r.get("created_at")) for r in records
        if r.get("created_at") is not None
    )
    with_outcome = sum(
        1 for r in records if outcome_maturity(r) != "missing"
    )
    matured = sum(1 for r in records if outcome_maturity(r) == "matured")
    n = len(records)
    reasons: List[Optional[str]] = []
    for r in records:
        if r.get("rejection_reason"):
            reasons.append(str(r["rejection_reason"]))
        trigger = r.get("four_hour_trigger")
        if isinstance(trigger, dict):
            reasons.extend(
                str(code) for code in (trigger.get("reason_codes") or [])
            )
    return {
        "record_count": n,
        "unique_symbol_count": len(symbols),
        "unique_session_count": len(sessions),
        "unique_campaign_count": len(campaigns),
        "first_session": sessions[0] if sessions else None,
        "last_session": sessions[-1] if sessions else None,
        "first_evaluated_at": created[0] if created else None,
        "last_evaluated_at": created[-1] if created else None,
        "strategy_version_distribution": _distribution(
            [r.get("strategy_version") for r in records]
        ),
        "decision_policy_version_distribution": _distribution(
            [r.get("decision_policy_version") for r in records]
        ),
        "config_hash_distribution": _distribution(
            [r.get("config_hash") for r in records]
        ),
        "daily_frame_contract_distribution": _distribution(
            [r.get("daily_frame_contract_version") for r in records]
        ),
        "four_hour_frame_contract_distribution": _distribution([
            (r.get("four_hour_frame_meta") or {}).get("contract_version")
            if isinstance(r.get("four_hour_frame_meta"), dict) else None
            for r in records
        ]),
        "with_outcome_count": with_outcome,
        "matured_outcome_count": matured,
        "missing_outcome_count": n - with_outcome,
        "outcome_coverage": (with_outcome / n) if n else None,
        "reason_distribution": _distribution(reasons),
    }


def build_cohorts(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build every declared cohort over one bounded record set.

    Cohorts OVERLAP; `evaluated` is the universe. Deterministic: cohort
    names sorted, all distributions sorted, counts derived from pure
    predicates only.
    """
    cohorts: Dict[str, Any] = {}
    for name in sorted(COHORT_PREDICATES):
        predicate = COHORT_PREDICATES[name]
        members = [r for r in records if predicate(r)]
        cohorts[name] = _cohort_summary(members)
    return {
        "contract_version": COHORT_CONTRACT_VERSION,
        "cohorts_overlap": True,
        "evaluated_count": len(records),
        "cohorts": cohorts,
    }


def cohort_members(
    records: List[Dict[str, Any]], cohort: str
) -> List[Dict[str, Any]]:
    """Members of one declared cohort (raises KeyError for unknown names)."""
    return [r for r in records if COHORT_PREDICATES[cohort](r)]


def build_failure_distributions(
    records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Explicit failure / waiting / readiness / frame-state distributions.

    Pure and deterministic; every vocabulary stays separate — rejection
    reasons are never pooled with trigger reasons or waiting reasons.
    """
    waiting: List[Optional[str]] = []
    trigger_reasons: List[Optional[str]] = []
    for r in records:
        policy_waiting = _policy(r).get("waiting_reasons")
        if isinstance(policy_waiting, list):
            waiting.extend(str(w) for w in policy_waiting)
        trigger = r.get("four_hour_trigger")
        if isinstance(trigger, dict):
            trigger_reasons.extend(
                str(code) for code in (trigger.get("reason_codes") or [])
            )
    return {
        "contract_version": COHORT_CONTRACT_VERSION,
        "evaluated_count": len(records),
        "rejection_reason_distribution": _distribution(
            [r.get("rejection_reason") for r in records]
        ),
        "waiting_reason_distribution": _distribution(waiting),
        "trigger_reason_distribution": _distribution(trigger_reasons),
        "trigger_state_distribution": _distribution(
            [classify_trigger_state(r) for r in records]
        ),
        "readiness_status_distribution": _distribution(
            [r.get("readiness_status") for r in records]
        ),
        "four_hour_frame_state_distribution": _distribution(
            [_frame_state(r) for r in records]
        ),
        "verdict_distribution": _distribution(
            [r.get("verdict") for r in records]
        ),
    }
