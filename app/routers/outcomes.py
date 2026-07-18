"""Public (read-only) outcome + metrics endpoints (Phase 2).

  GET /api/outcomes          - list computed signal outcomes (filterable)
  GET /api/outcomes/metrics  - aggregated, baseline-aware metrics

Admin (write) calculation lives in app/routers/admin.py behind the worker token:
  POST /api/admin/outcomes/calculate
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query

from app.workers.outcomes.calculator import HOLDING_WINDOWS
from app.workers.outcomes.metrics import (
    aggregate_all_windows,
    aggregate_outcomes,
    group_and_aggregate,
)
from app.workers.outcomes.persistence import fetch_outcomes


router = APIRouter()


@router.get("/outcomes")
async def list_outcomes(
    pattern_code: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    side: Optional[str] = Query(None, description="LONG or SHORT"),
    status: Optional[str] = Query(
        None, description="pending|calculated|insufficient_data|error"
    ),
    limit: int = Query(100, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    """Return computed outcome rows (most recent first)."""
    return await fetch_outcomes(
        pattern_code=pattern_code,
        symbol=symbol,
        side=side,
        status=status,
        limit=limit,
    )


@router.get("/outcomes/metrics")
async def outcome_metrics(
    pattern_code: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    side: Optional[str] = Query(None, description="LONG or SHORT"),
    window: Optional[int] = Query(
        None, description=f"Holding window in days; one of {HOLDING_WINDOWS}"
    ),
    group_by: Optional[str] = Query(
        None,
        description="Comma-separated keys to group by, e.g. 'pattern_code,side'",
    ),
    limit: int = Query(1000, ge=1, le=5000),
) -> Dict[str, Any]:
    """Aggregate outcomes into honest, baseline-aware metrics.

    Always reports sample_size alongside every stat. When `window` is omitted,
    returns metrics for all holding windows.
    """
    outcomes = await fetch_outcomes(
        pattern_code=pattern_code,
        symbol=symbol,
        side=side,
        status="calculated",
        limit=limit,
    )

    response: Dict[str, Any] = {
        "filters": {
            "pattern_code": pattern_code,
            "symbol": symbol,
            "side": side,
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
