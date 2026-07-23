"""Generic strategy-filtered shadow evidence review (Phase 9F1).

Read-only retrieval layer for the operator evidence-review surfaces: it
normalizes bounded filters, fetches frozen evaluation records through the
existing persistence reads, and applies the DERIVED filters (trigger class,
readiness state, rollout blocking, outcome maturity) using exactly the same
classification helpers the metrics layer uses — evidence review never
reclassifies records with private rules.

Hard rules:
  * read-only: no writes, no configuration mutation, no strategy execution;
  * no provider construction and no live market-data fetch;
  * bounded: the record limit is validated and hard-capped;
  * deterministic: SQL ordering (created_at DESC) is preserved and every
    derived filter is a pure predicate;
  * generic: nothing here is Wyckoff-specific — the strategy code is a
    filter, and strategy-specific fields stay inside the frozen records.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from app.workers.shadow.strategy_metrics import (
    classify_trigger_state,
    is_rollout_blocked,
)


EVIDENCE_REVIEW_CONTRACT_VERSION = "shadow_evidence_review.v1"

DEFAULT_RECORD_LIMIT = 1000
MAX_RECORD_LIMIT = 2000

# Derived-filter vocabularies (closed; unknown values reject).
TRIGGER_STATE_FILTERS = (
    "confirmed", "waiting", "contradicted", "absent",
    "four_hour_insufficient", "disabled", "side_unknown",
    "unknown_other", "not_evaluated",
)
READINESS_STATE_FILTERS = ("ready", "not_ready", "unknown")
OUTCOME_MATURITY_FILTERS = ("matured", "pending", "missing")


class EvidenceFilterError(ValueError):
    """Invalid evidence-review filter (unknown vocabulary / bad bound)."""


def outcome_maturity(record: Dict[str, Any]) -> str:
    """matured / pending / missing for one evaluation record.

    matured  -> the pair outcome row is complete;
    pending  -> an outcome row exists but is not complete (including the
                honest error state — it is NOT matured);
    missing  -> no outcome row exists (never represented as a zero return).
    """
    if not record.get("has_outcome"):
        return "missing"
    if record.get("outcome_status") == "complete":
        return "matured"
    return "pending"


def readiness_state(record: Dict[str, Any]) -> str:
    """ready / not_ready / unknown for one record's frozen daily readiness."""
    status = record.get("readiness_status")
    if status is None:
        return "unknown"
    return "ready" if status == "ready" else "not_ready"


def _parse_date(value: Any, name: str) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        raise EvidenceFilterError(f"{name} must be a YYYY-MM-DD date")


def normalize_evidence_filters(
    *,
    strategy_code: str = "wyckoff_mtf_v2",
    experiment_code: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    symbol: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Any = None,
    max_snapshot_date: Any = None,
    trigger_state: Optional[str] = None,
    readiness: Optional[str] = None,
    rollout_blocked: Optional[bool] = None,
    outcome_maturity_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Validate and normalize one bounded evidence-review filter set."""
    if not (strategy_code or "").strip():
        raise EvidenceFilterError("strategy_code is required")
    if trigger_state is not None and trigger_state not in TRIGGER_STATE_FILTERS:
        raise EvidenceFilterError(
            f"trigger_state must be one of {list(TRIGGER_STATE_FILTERS)}"
        )
    if readiness is not None and readiness not in READINESS_STATE_FILTERS:
        raise EvidenceFilterError(
            f"readiness must be one of {list(READINESS_STATE_FILTERS)}"
        )
    if (
        outcome_maturity_filter is not None
        and outcome_maturity_filter not in OUTCOME_MATURITY_FILTERS
    ):
        raise EvidenceFilterError(
            "outcome_maturity must be one of "
            f"{list(OUTCOME_MATURITY_FILTERS)}"
        )
    effective_limit = DEFAULT_RECORD_LIMIT if limit is None else int(limit)
    if effective_limit < 1 or effective_limit > MAX_RECORD_LIMIT:
        raise EvidenceFilterError(
            f"limit must be between 1 and {MAX_RECORD_LIMIT}"
        )
    return {
        "contract_version": EVIDENCE_REVIEW_CONTRACT_VERSION,
        "strategy_code": strategy_code.strip(),
        "experiment_code": experiment_code,
        "strategy_version": strategy_version,
        "decision_policy_version": decision_policy_version,
        "config_hash": config_hash,
        "symbol": symbol.strip().upper() if symbol else None,
        "campaign_id": campaign_id,
        "min_snapshot_date": _parse_date(min_snapshot_date, "min_snapshot_date"),
        "max_snapshot_date": _parse_date(max_snapshot_date, "max_snapshot_date"),
        "trigger_state": trigger_state,
        "readiness": readiness,
        "rollout_blocked": rollout_blocked,
        "outcome_maturity": outcome_maturity_filter,
        "limit": effective_limit,
    }


def filters_for_response(filters: Dict[str, Any]) -> Dict[str, Any]:
    """JSON-safe echo of the normalized filters."""
    echoed = dict(filters)
    for key in ("min_snapshot_date", "max_snapshot_date"):
        if echoed.get(key) is not None:
            echoed[key] = echoed[key].isoformat()
    return echoed


def apply_derived_filters(
    records: List[Dict[str, Any]], filters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Apply the pure derived predicates (trigger class, readiness state,
    rollout blocking, outcome maturity) to already-fetched records."""
    result = records
    if filters.get("trigger_state") is not None:
        wanted = filters["trigger_state"]
        result = [r for r in result if classify_trigger_state(r) == wanted]
    if filters.get("readiness") is not None:
        wanted = filters["readiness"]
        result = [r for r in result if readiness_state(r) == wanted]
    if filters.get("rollout_blocked") is not None:
        wanted = bool(filters["rollout_blocked"])
        result = [
            r for r in result if is_rollout_blocked(r) is wanted
        ]
    if filters.get("outcome_maturity") is not None:
        wanted = filters["outcome_maturity"]
        result = [r for r in result if outcome_maturity(r) == wanted]
    return result


async def fetch_evidence_records(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Bounded read-only evidence records for one normalized filter set.

    SQL-representable filters run in the persistence read; derived filters
    apply as pure predicates afterwards. Never writes, never constructs a
    provider, never executes a strategy.
    """
    from app.workers.shadow.persistence import (
        fetch_strategy_shadow_evaluations,
    )

    records = await fetch_strategy_shadow_evaluations(
        strategy_code=filters["strategy_code"],
        symbol=filters["symbol"],
        strategy_version=filters["strategy_version"],
        decision_policy_version=filters["decision_policy_version"],
        experiment_code=filters["experiment_code"],
        config_hash=filters["config_hash"],
        campaign_id=filters["campaign_id"],
        min_snapshot_date=filters["min_snapshot_date"],
        max_snapshot_date=filters["max_snapshot_date"],
        limit=filters["limit"],
    )
    return apply_derived_filters(records, filters)
