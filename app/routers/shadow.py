"""Read-only inspection APIs for Phase 8.1B1 shadow evaluations.

Bounded views over strategy_shadow_runs / strategy_shadow_pairs /
strategy_shadow_evaluations. List responses never include the full frame or
details snapshots; the pair-detail endpoint exposes the bounded frozen data
for one specific pair. Shadow data is experiment evidence, never candidates.
"""

import logging
import uuid as uuid_lib
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.workers.shadow.constants import (
    AGREEMENT_CATEGORIES,
    DISAGREEMENT_CATEGORIES,
)
from app.workers.shadow.persistence import (
    fetch_shadow_pair_detail,
    fetch_shadow_pairs,
    fetch_shadow_run,
)


logger = logging.getLogger(__name__)

router = APIRouter()

_VALID_VERDICTS = ("ENTER", "WATCH", "AVOID")
_VALID_CATEGORIES = AGREEMENT_CATEGORIES + DISAGREEMENT_CATEGORIES


def _validated_uuid(value: str, name: str) -> str:
    try:
        return str(uuid_lib.UUID(value))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail=f"{name} not found")


def _validated_verdict(value: Optional[str], name: str) -> Optional[str]:
    if value is None:
        return None
    upper = value.upper()
    if upper not in _VALID_VERDICTS:
        raise HTTPException(
            status_code=422,
            detail=f"{name} must be one of {list(_VALID_VERDICTS)}",
        )
    return upper


@router.get("/shadow/runs/{run_id}")
async def get_shadow_run(run_id: str):
    """One shadow run with its bounded telemetry (no pair payloads)."""
    run = await fetch_shadow_run(_validated_uuid(run_id, "run"))
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@router.get("/shadow/pairs")
async def list_shadow_pairs(
    run_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    control_verdict: Optional[str] = Query(None),
    candidate_verdict: Optional[str] = Query(None),
    agreement: Optional[bool] = Query(None),
    disagreement_category: Optional[str] = Query(None),
    control_strategy_version: Optional[str] = Query(None),
    candidate_strategy_version: Optional[str] = Query(None),
    control_config_hash: Optional[str] = Query(None),
    candidate_config_hash: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    """Bounded pair summaries; all filters AND-compose.

    Never returns the full frame_snapshot or details_snapshot — use
    GET /api/shadow/pairs/{pair_id} for the frozen detail.
    """
    if disagreement_category is not None and (
        disagreement_category not in _VALID_CATEGORIES
    ):
        raise HTTPException(
            status_code=422,
            detail=f"disagreement_category must be one of {list(_VALID_CATEGORIES)}",
        )

    pairs = await fetch_shadow_pairs(
        run_id=_validated_uuid(run_id, "run") if run_id is not None else None,
        symbol=symbol,
        control_verdict=_validated_verdict(control_verdict, "control_verdict"),
        candidate_verdict=_validated_verdict(
            candidate_verdict, "candidate_verdict"
        ),
        agreement=agreement,
        disagreement_category_filter=disagreement_category,
        control_strategy_version=control_strategy_version,
        candidate_strategy_version=candidate_strategy_version,
        control_config_hash=control_config_hash,
        candidate_config_hash=candidate_config_hash,
        limit=limit,
    )
    return {"count": len(pairs), "pairs": pairs}


@router.get("/shadow/pairs/{pair_id}")
async def get_shadow_pair(pair_id: str):
    """Full bounded inspection of one immutable pair: the canonical frame,
    both frozen evaluation snapshots, and its run occurrences."""
    detail = await fetch_shadow_pair_detail(_validated_uuid(pair_id, "pair"))
    if detail is None:
        raise HTTPException(status_code=404, detail="pair not found")
    return detail
