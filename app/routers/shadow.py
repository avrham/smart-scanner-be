"""Read-only inspection APIs for Phase 8.1B shadow evaluations and outcomes.

Bounded views over strategy_shadow_runs / strategy_shadow_pairs /
strategy_shadow_evaluations (B1) and strategy_shadow_pair_outcomes (B2).
List responses never include the full frame or details snapshots; the
detail endpoints expose the bounded frozen data for one specific pair.
Shadow data is experiment evidence, never candidates, and the metrics
endpoint returns NEUTRAL grouped evidence only — never a better/winner/
improvement/regression/pass/fail/promote/disable label and never a
parameter recommendation. No automatic v3 rollout gate exists.
"""

import logging
import uuid as uuid_lib
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.workers.shadow.constants import (
    AGREEMENT_CATEGORIES,
    DISAGREEMENT_CATEGORIES,
)
from app.workers.shadow.outcomes.constants import (
    METRICS_CONTRACT_VERSION,
    OUTCOME_STATUSES,
)
from app.workers.shadow.outcomes.metrics import aggregate_pair_outcome_metrics
from app.workers.shadow.outcomes.persistence import (
    fetch_pair_outcome_detail,
    fetch_pair_outcomes,
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


# --------------------------------------------------------------------------- #
# Phase 8.1B2: shared pair outcomes (one market-path outcome per pair)
# --------------------------------------------------------------------------- #

def _outcome_common_filters(
    pair_id: Optional[str],
    run_id: Optional[str],
    control_verdict: Optional[str],
    candidate_verdict: Optional[str],
    outcome_status: Optional[str],
    disagreement_category: Optional[str],
) -> dict:
    """Shared validation for the outcome list/metrics endpoints."""
    if outcome_status is not None and outcome_status not in OUTCOME_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"outcome_status must be one of {list(OUTCOME_STATUSES)}",
        )
    if disagreement_category is not None and (
        disagreement_category not in _VALID_CATEGORIES
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                "disagreement_category must be one of "
                f"{list(_VALID_CATEGORIES)}"
            ),
        )
    return {
        "pair_id": _validated_uuid(pair_id, "pair") if pair_id else None,
        "run_id": _validated_uuid(run_id, "run") if run_id else None,
        "control_verdict": _validated_verdict(control_verdict, "control_verdict"),
        "candidate_verdict": _validated_verdict(
            candidate_verdict, "candidate_verdict"
        ),
        "outcome_status": outcome_status,
        "disagreement_category_filter": disagreement_category,
    }


@router.get("/shadow/outcomes")
async def list_shadow_outcomes(
    pair_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
    outcome_status: Optional[str] = Query(None),
    forward_provider: Optional[str] = Query(None),
    control_verdict: Optional[str] = Query(None),
    candidate_verdict: Optional[str] = Query(None),
    disagreement_category: Optional[str] = Query(None),
    control_strategy_version: Optional[str] = Query(None),
    candidate_strategy_version: Optional[str] = Query(None),
    control_decision_policy_version: Optional[str] = Query(None),
    candidate_decision_policy_version: Optional[str] = Query(None),
    control_config_hash: Optional[str] = Query(None),
    candidate_config_hash: Optional[str] = Query(None),
    min_snapshot_date: Optional[date] = Query(None),
    max_snapshot_date: Optional[date] = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    """Bounded joined outcome rows; all filters AND-compose.

    Each row contains the pair summary, both frozen evaluation identities
    and verdicts, the SHARED pair outcome and the agreement/disagreement
    classification. Never returns the full B1 frame_snapshot — use
    GET /api/shadow/outcomes/{pair_id} for the bounded detail.
    """
    filters = _outcome_common_filters(
        pair_id, run_id, control_verdict, candidate_verdict,
        outcome_status, disagreement_category,
    )
    outcomes = await fetch_pair_outcomes(
        **filters,
        symbol=symbol,
        forward_provider=forward_provider,
        control_strategy_version=control_strategy_version,
        candidate_strategy_version=candidate_strategy_version,
        control_decision_policy_version=control_decision_policy_version,
        candidate_decision_policy_version=candidate_decision_policy_version,
        control_config_hash=control_config_hash,
        candidate_config_hash=candidate_config_hash,
        min_snapshot_date=min_snapshot_date,
        max_snapshot_date=max_snapshot_date,
        limit=limit,
    )
    return {"count": len(outcomes), "outcomes": outcomes}


@router.get("/shadow/outcomes/metrics")
async def shadow_outcome_metrics(
    symbol: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
    outcome_status: Optional[str] = Query(None),
    forward_provider: Optional[str] = Query(None),
    control_verdict: Optional[str] = Query(None),
    candidate_verdict: Optional[str] = Query(None),
    disagreement_category: Optional[str] = Query(None),
    control_strategy_version: Optional[str] = Query(None),
    candidate_strategy_version: Optional[str] = Query(None),
    control_config_hash: Optional[str] = Query(None),
    candidate_config_hash: Optional[str] = Query(None),
    min_snapshot_date: Optional[date] = Query(None),
    max_snapshot_date: Optional[date] = Query(None),
    limit: int = Query(1000, ge=1, le=5000),
):
    """Neutral grouped evidence for shadow pair outcomes.

    Groups are keyed by the FULL mandatory identity (experiment, both arm
    strategy/policy/config identities and verdicts, disagreement category,
    calculation/coverage/forward-frame versions, forward provider) — rows
    are never pooled across any of these. Every group reports sample counts
    next to every statistic. positive_return_rate is the canonical neutral
    term; win_rate is never emitted; no superiority labels exist.
    """
    filters = _outcome_common_filters(
        None, run_id, control_verdict, candidate_verdict,
        outcome_status, disagreement_category,
    )
    filters.pop("pair_id")
    rows = await fetch_pair_outcomes(
        **filters,
        symbol=symbol,
        forward_provider=forward_provider,
        control_strategy_version=control_strategy_version,
        candidate_strategy_version=candidate_strategy_version,
        control_config_hash=control_config_hash,
        candidate_config_hash=candidate_config_hash,
        min_snapshot_date=min_snapshot_date,
        max_snapshot_date=max_snapshot_date,
        limit=limit,
    )
    return {
        "metrics_contract_version": METRICS_CONTRACT_VERSION,
        "total_outcomes": len(rows),
        "groups": aggregate_pair_outcome_metrics(rows),
    }


@router.get("/shadow/outcomes/{pair_id}")
async def get_shadow_outcome(pair_id: str):
    """One pair's shared outcome: pair identity, frame SUMMARY (never the
    full snapshot), both frozen evaluation identities and decisions, the
    full outcome row and its bounded revision diagnostics."""
    detail = await fetch_pair_outcome_detail(_validated_uuid(pair_id, "pair"))
    if detail is None:
        raise HTTPException(status_code=404, detail="pair not found")
    return detail
