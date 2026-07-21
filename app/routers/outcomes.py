"""Public (read-only) outcome + metrics endpoints (Phase 2).

  GET /api/outcomes          - list computed signal outcomes (filterable)
  GET /api/outcomes/metrics  - aggregated, baseline-aware metrics

Admin (write) calculation lives in app/routers/admin.py behind the worker token:
  POST /api/admin/outcomes/calculate
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.workers.outcomes.calculator import HOLDING_WINDOWS
from app.workers.outcomes.metrics import (
    aggregate_all_windows,
    aggregate_outcomes,
    group_and_aggregate,
)
from app.workers.outcomes.persistence import fetch_outcomes


router = APIRouter()

_VALID_VERDICTS = ("ENTER", "WATCH", "ALL")


def _validated_verdict(verdict: str) -> str:
    """Reject malformed verdict values safely (no query is executed)."""
    value = (verdict or "").strip().upper()
    if value not in _VALID_VERDICTS:
        raise HTTPException(
            status_code=422,
            detail=f"verdict must be one of {list(_VALID_VERDICTS)}",
        )
    return value


@router.get("/outcomes")
async def list_outcomes(
    pattern_code: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    side: Optional[str] = Query(None, description="LONG or SHORT"),
    status: Optional[str] = Query(
        None, description="pending|calculated|insufficient_data|error"
    ),
    verdict: str = Query(
        "ENTER",
        description=(
            "ENTER | WATCH | ALL. Defaults to ENTER so existing consumers "
            "that expect trade-entry outcomes never silently receive WATCH "
            "candidate observations. ENTER includes legacy rows persisted "
            "before migration 009 (those were ENTER-only by construction)."
        ),
    ),
    strategy_code: Optional[str] = Query(None),
    strategy_version: Optional[str] = Query(None, description="e.g. sma150.v3"),
    decision_policy_version: Optional[str] = Query(None),
    config_hash: Optional[str] = Query(None),
    outcome_coverage_version: Optional[str] = Query(None),
    reference_price_role: Optional[str] = Query(
        None, description="entry_reference | candidate_observation"
    ),
    limit: int = Query(100, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    """Return computed outcome rows (most recent first). Filters AND-compose.

    Phase 8.1A: rows expose signal_verdict, reference_price_role and
    outcome_coverage_version. A WATCH outcome is a candidate observation
    (what happened after the candidate was seen), never an executed trade.
    """
    return await fetch_outcomes(
        pattern_code=pattern_code,
        symbol=symbol,
        side=side,
        status=status,
        verdict=_validated_verdict(verdict),
        strategy_code=strategy_code,
        strategy_version=strategy_version,
        decision_policy_version=decision_policy_version,
        config_hash=config_hash,
        outcome_coverage_version=outcome_coverage_version,
        reference_price_role=reference_price_role,
        limit=limit,
    )


@router.get("/outcomes/metrics")
async def outcome_metrics(
    pattern_code: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    side: Optional[str] = Query(None, description="LONG or SHORT"),
    verdict: str = Query(
        "ENTER",
        description=(
            "ENTER | WATCH | ALL. Defaults to ENTER (existing trade metrics "
            "unchanged). WATCH/ALL aggregates report positive_return_rate; "
            "win_rate is trade terminology and is only emitted for samples "
            "containing no WATCH rows."
        ),
    ),
    strategy_code: Optional[str] = Query(None),
    strategy_version: Optional[str] = Query(None, description="e.g. sma150.v3"),
    decision_policy_version: Optional[str] = Query(None),
    config_hash: Optional[str] = Query(None),
    outcome_coverage_version: Optional[str] = Query(None),
    window: Optional[int] = Query(
        None, description=f"Holding window in days; one of {HOLDING_WINDOWS}"
    ),
    group_by: Optional[str] = Query(
        None,
        description=(
            "Comma-separated keys to group by, e.g. 'pattern_code,side' or "
            "'signal_verdict,strategy_version,decision_policy_version,"
            "config_hash'"
        ),
    ),
    limit: int = Query(1000, ge=1, le=5000),
) -> Dict[str, Any]:
    """Aggregate outcomes into honest, baseline-aware metrics.

    Always reports sample sizes alongside every stat. When `window` is
    omitted, returns metrics for all holding windows. Filters AND-compose,
    so the API can answer e.g. sma150.v3 WATCH outcomes for one config_hash,
    or ENTER versus WATCH for the same strategy_version (verdict=ALL +
    group_by=signal_verdict).
    """
    outcomes = await fetch_outcomes(
        pattern_code=pattern_code,
        symbol=symbol,
        side=side,
        status="calculated",
        verdict=_validated_verdict(verdict),
        strategy_code=strategy_code,
        strategy_version=strategy_version,
        decision_policy_version=decision_policy_version,
        config_hash=config_hash,
        outcome_coverage_version=outcome_coverage_version,
        limit=limit,
    )

    response: Dict[str, Any] = {
        "filters": {
            "pattern_code": pattern_code,
            "symbol": symbol,
            "side": side,
            "verdict": verdict,
            "strategy_code": strategy_code,
            "strategy_version": strategy_version,
            "decision_policy_version": decision_policy_version,
            "config_hash": config_hash,
            "outcome_coverage_version": outcome_coverage_version,
            "window": window,
            "group_by": group_by,
        },
        "total_outcomes": len(outcomes),
    }

    if group_by:
        keys = [k.strip() for k in group_by.split(",") if k.strip()]
        win = window or HOLDING_WINDOWS[2]  # default 5D for grouped view
        response["window"] = win
        response["groups"] = group_and_aggregate(outcomes, keys, win)
        return response

    if window:
        response["metrics"] = aggregate_outcomes(outcomes, window)
    else:
        response["metrics_by_window"] = aggregate_all_windows(outcomes)

    return response
