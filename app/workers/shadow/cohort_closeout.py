"""Historical cohort closeout audit (shadow_cohort_closeout.v1).

Read-only, PURE aggregation that summarizes an EXISTING historical shadow
evidence cohort (the campaigns an operator ran with historical `as_of_date`
values, operationally called "Phase 9G") so it can be closed out and audited.
It answers one question honestly: what is still unresolved, and what is simply
not yet eligible for maturation?

It composes the existing machinery — never a parallel implementation:
  * `aggregate_strategy_shadow_metrics` for decision-state coverage;
  * `build_quality_audit` for versioned honesty issues;
  * `summarize_eligibility` (trading-session maturation eligibility);
  * the joined outcome rows for per-horizon / per-status counts.

Because the live cohort cannot be inspected here, this module reports only
what its inputs contain; the endpoint that feeds it is read-only and performs
no maturation. Maturation itself remains the existing bounded endpoint
`POST /api/admin/shadow/outcomes/calculate`.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from app.workers.outcomes.calculator import HOLDING_WINDOWS, window_label
from app.workers.shadow.outcomes.eligibility import (
    UNRESOLVED_STATES,
    classify_maturation_eligibility,
    completed_forward_sessions,
    summarize_eligibility,
)
from app.workers.shadow.quality_audit import build_quality_audit
from app.workers.shadow.strategy_metrics import aggregate_strategy_shadow_metrics


COHORT_CLOSEOUT_CONTRACT_VERSION = "shadow_cohort_closeout.v1"

PROVIDER_FAILURE_ERROR_CODES = frozenset({
    "provider_mismatch",
    "provider_range_unsupported",
})
FORWARD_FETCH_ERROR_CODE = "forward_fetch_error"

# Max per-pair unresolved rows echoed back (the summary counts are exact; the
# list is bounded so a large cohort can never grow the response without bound).
MAX_UNRESOLVED_LISTED = 200


def _as_date(value: Any) -> Optional[date]:
    if value is None or isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _outcome(row: Dict[str, Any]) -> Dict[str, Any]:
    outcome = row.get("outcome")
    return outcome if isinstance(outcome, dict) else {}


def build_cohort_closeout_audit(
    records: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
    *,
    session_dates: Optional[List[date]] = None,
    latest_completed_session: Optional[date] = None,
    campaign_runs: Optional[List[Dict[str, Any]]] = None,
    campaign_ids: Optional[List[str]] = None,
    strategy_discovery: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Summarize an existing cohort's maturation state. PURE — no I/O."""
    session_dates = session_dates or []

    # ---- outcome-row level: status, per-horizon maturation, failures ------ #
    outcome_status_distribution: Dict[str, int] = {}
    matured_by_horizon: Dict[str, int] = {window_label(w): 0 for w in HOLDING_WINDOWS}
    provider_failure_rows: List[Dict[str, Any]] = []
    forward_fetch_error_rows: List[Dict[str, Any]] = []
    error_code_by_pair: Dict[str, Optional[str]] = {}
    pair_ids_seen: Dict[str, int] = {}

    for row in outcome_rows:
        outcome = _outcome(row)
        pair = row.get("pair") or {}
        pair_id = str(pair.get("pair_id")) if pair.get("pair_id") else None
        status = outcome.get("outcome_status")
        error_code = outcome.get("error_code")
        if pair_id is not None:
            pair_ids_seen[pair_id] = pair_ids_seen.get(pair_id, 0) + 1
            error_code_by_pair[pair_id] = error_code
        if status is not None:
            outcome_status_distribution[str(status)] = (
                outcome_status_distribution.get(str(status), 0) + 1
            )
        for w in HOLDING_WINDOWS:
            label = window_label(w)
            if (outcome.get("returns") or {}).get(label) is not None:
                matured_by_horizon[label] += 1
        if error_code in PROVIDER_FAILURE_ERROR_CODES:
            provider_failure_rows.append({
                "pair_id": pair_id, "symbol": pair.get("symbol"),
                "error_code": error_code,
            })
        if error_code == FORWARD_FETCH_ERROR_CODE:
            forward_fetch_error_rows.append({
                "pair_id": pair_id, "symbol": pair.get("symbol"),
                "snapshot_date": pair.get("snapshot_date"),
            })

    # Duplicate outcome rows for one pair (the table is 1:1 per pair, so >1 is
    # a real anomaly worth surfacing).
    duplicate_outcome_pairs = {
        pid: n for pid, n in pair_ids_seen.items() if n > 1
    }

    # ---- evaluation-record level: eligibility per pair -------------------- #
    eligibility_inputs: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []
    symbol_session_pairs: Dict[str, int] = {}
    for record in records:
        pair_id = str(record.get("pair_id")) if record.get("pair_id") else None
        snapshot = _as_date(record.get("snapshot_date"))
        outcome_status = record.get("outcome_status")
        error_code = (
            error_code_by_pair.get(pair_id) if outcome_status == "error"
            else None
        )
        sessions = (
            completed_forward_sessions(
                session_dates, snapshot,
                latest_completed_session=latest_completed_session,
            )
            if snapshot is not None else None
        )
        eligibility_inputs.append({
            "outcome_status": outcome_status,
            "error_code": error_code,
            "completed_forward_sessions": sessions,
        })
        state = classify_maturation_eligibility(
            outcome_status=outcome_status,
            error_code=error_code,
            completed_forward_sessions=sessions,
        )
        if state in UNRESOLVED_STATES and len(unresolved) < MAX_UNRESOLVED_LISTED:
            unresolved.append({
                "pair_id": pair_id,
                "symbol": record.get("symbol"),
                "snapshot_date": record.get("snapshot_date"),
                "outcome_status": outcome_status,
                "error_code": error_code,
                "completed_forward_sessions": sessions,
                "eligibility": state,
            })
        # duplicate candidate evaluations for one (symbol, session).
        key = f"{str(record.get('symbol') or '').upper()}|{record.get('snapshot_date')}"
        symbol_session_pairs[key] = symbol_session_pairs.get(key, 0) + 1

    eligibility = summarize_eligibility(eligibility_inputs)
    duplicate_symbol_sessions = {
        k: n for k, n in symbol_session_pairs.items() if n > 1
    }

    # ---- campaign ids included -------------------------------------------- #
    campaign_id_set = set(str(c) for c in (campaign_ids or []) if c)
    for record in records:
        for cid in record.get("campaign_ids") or []:
            if cid:
                campaign_id_set.add(str(cid))

    # ---- reuse existing coverage + quality machinery ---------------------- #
    metrics = aggregate_strategy_shadow_metrics(records)
    quality = build_quality_audit(
        records,
        campaign_runs=campaign_runs,
        outcome_rows=outcome_rows,
        strategy_discovery=strategy_discovery,
    )
    evaluated = len(records)
    with_outcome = sum(1 for r in records if r.get("has_outcome"))

    return {
        "closeout_contract_version": COHORT_CLOSEOUT_CONTRACT_VERSION,
        "campaign_ids": sorted(campaign_id_set),
        "campaign_count": len(campaign_id_set),
        # totals
        "total_evaluations": evaluated,
        "total_outcome_rows": len(outcome_rows),
        "outcome_status_distribution": dict(sorted(
            outcome_status_distribution.items()
        )),
        "matured_outcomes_by_horizon": matured_by_horizon,
        # maturation eligibility (trading sessions)
        "eligibility": eligibility,
        "eligible_not_yet_matured_count": eligibility["counts"]["eligible"],
        "not_yet_eligible_count": eligibility["counts"]["not_yet_eligible"],
        "unresolved_action_required_count": (
            eligibility["unresolved_action_required_count"]
        ),
        "unresolved_sample": unresolved,
        "unresolved_sample_truncated": (
            eligibility["unresolved_action_required_count"] > len(unresolved)
        ),
        # failures
        "provider_failure_count": len(provider_failure_rows),
        "provider_failure_rows": provider_failure_rows,
        "forward_fetch_error_count": len(forward_fetch_error_rows),
        "forward_fetch_error_rows": forward_fetch_error_rows,
        # coverage
        "outcome_coverage": (with_outcome / evaluated) if evaluated else None,
        "with_outcome_count": with_outcome,
        "missing_outcome_count": evaluated - with_outcome,
        # duplicate detection
        "duplicate_outcome_pair_count": len(duplicate_outcome_pairs),
        "duplicate_outcome_pairs": dict(sorted(duplicate_outcome_pairs.items())),
        "duplicate_symbol_session_count": len(duplicate_symbol_sessions),
        "duplicate_symbol_sessions": dict(sorted(
            duplicate_symbol_sessions.items()
        )),
        # reused evidence machinery
        "decision_metrics": metrics,
        "quality_audit": quality,
    }


__all__ = [
    "COHORT_CLOSEOUT_CONTRACT_VERSION",
    "PROVIDER_FAILURE_ERROR_CODES",
    "FORWARD_FETCH_ERROR_CODE",
    "build_cohort_closeout_audit",
]
