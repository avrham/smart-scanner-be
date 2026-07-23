"""Deterministic campaign PLAN generator — never an executor (Phase 9F8).

Contract: shadow_campaign_plan.v1.

Produces the exact bounded sequence of admin campaign payloads an operator
would submit to close the remaining evidence gap, WITHOUT executing
anything: no provider call, no database row, no background work, no
scheduling. Every plan carries executed=false and the operational warnings
(migration 013, Massive requirement) verbatim.

All functions are pure (no I/O).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from app.workers.shadow.campaigns import (
    CAMPAIGN_CHUNK_SIZE,
    MAX_CAMPAIGN_SYMBOLS,
)
from app.workers.shadow.experiments import get_experiment


CAMPAIGN_PLAN_CONTRACT_VERSION = "shadow_campaign_plan.v1"

# Bounded planning inputs: an explicit candidate universe slice only.
MAX_PLAN_SYMBOLS = 500
MAX_PLAN_SESSIONS = 20

_SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_HORIZON_SESSIONS = {"1D": 1, "3D": 3, "5D": 5, "10D": 10, "20D": 20}

WARNING_MIGRATION_013 = (
    "migration 013_wyckoff_v2_shadow_arms.sql must be applied manually "
    "before wyckoff_v2 shadow pairs can persist"
)
WARNING_MASSIVE_REQUIRED = (
    "MARKET_DATA_PROVIDER=massive is required for honest bounded 4H "
    "trigger evidence; FMP returns a typed unsupported_provider state"
)


class CampaignPlanError(ValueError):
    """Invalid campaign-plan request (unbounded / malformed)."""


def _normalize_symbols(symbols: Any) -> List[str]:
    if not isinstance(symbols, list) or not symbols:
        raise CampaignPlanError(
            "candidate_symbols must be an explicit non-empty list — plans "
            "never select an implicit universe"
        )
    if len(symbols) > MAX_PLAN_SYMBOLS:
        raise CampaignPlanError(
            f"candidate_symbols is capped at {MAX_PLAN_SYMBOLS}"
        )
    normalized: List[str] = []
    seen = set()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        if not _SYMBOL_RE.match(symbol):
            raise CampaignPlanError(f"malformed symbol {symbol!r}")
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise CampaignPlanError("candidate_symbols contains no valid ticker")
    return sorted(normalized)


def _normalize_sessions(sessions: Any) -> List[str]:
    if not isinstance(sessions, list) or not sessions:
        raise CampaignPlanError(
            "as_of_sessions must be an explicit non-empty list of "
            "YYYY-MM-DD dates"
        )
    if len(sessions) > MAX_PLAN_SESSIONS:
        raise CampaignPlanError(
            f"as_of_sessions is capped at {MAX_PLAN_SESSIONS}"
        )
    from datetime import date

    normalized = []
    for raw in sessions:
        try:
            normalized.append(date.fromisoformat(str(raw)).isoformat())
        except ValueError:
            raise CampaignPlanError(
                f"as_of_sessions contains a non-date value {raw!r}"
            )
    return sorted(set(normalized))


def build_campaign_plan(
    *,
    experiment_code: str,
    candidate_symbols: Any,
    as_of_sessions: Any,
    max_symbols_per_campaign: Any,
    target_unique_symbols: Optional[int] = None,
    target_trigger_confirmed: Optional[int] = None,
    target_matured_outcomes: Optional[int] = None,
    target_horizon: str = "20D",
    existing_evaluated_symbols: Optional[List[str]] = None,
    existing_unique_symbols: int = 0,
    existing_trigger_confirmed: int = 0,
    existing_matured_outcomes: int = 0,
) -> Dict[str, Any]:
    """Build one deterministic, bounded, NON-EXECUTING campaign plan."""
    experiment = get_experiment(str(experiment_code or ""))

    if max_symbols_per_campaign is None:
        raise CampaignPlanError(
            "max_symbols_per_campaign is required — plans must carry the "
            "same explicit safety bound campaigns require"
        )
    try:
        per_campaign = int(max_symbols_per_campaign)
    except (TypeError, ValueError):
        raise CampaignPlanError("max_symbols_per_campaign must be an integer")
    if per_campaign < 1 or per_campaign > MAX_CAMPAIGN_SYMBOLS:
        raise CampaignPlanError(
            f"max_symbols_per_campaign must be between 1 and "
            f"{MAX_CAMPAIGN_SYMBOLS}"
        )
    if target_horizon not in _HORIZON_SESSIONS:
        raise CampaignPlanError(
            f"target_horizon must be one of {sorted(_HORIZON_SESSIONS)}"
        )

    symbols = _normalize_symbols(candidate_symbols)
    sessions = _normalize_sessions(as_of_sessions)
    covered = {
        str(s or "").strip().upper()
        for s in (existing_evaluated_symbols or [])
    }
    remaining_symbols = [s for s in symbols if s not in covered]

    batches: List[Dict[str, Any]] = []
    payloads: List[Dict[str, Any]] = []
    for session in sessions:
        for start in range(0, len(remaining_symbols), per_campaign):
            chunk = remaining_symbols[start:start + per_campaign]
            if not chunk:
                continue
            batch = {
                "as_of_date": session,
                "batch_index": len(batches),
                "symbol_count": len(chunk),
                "symbols": chunk,
            }
            batches.append(batch)
            payloads.append({
                "method": "POST",
                "path": "/api/admin/shadow-campaigns",
                "body": {
                    "experiment_code": experiment.experiment_code,
                    "symbols": chunk,
                    "max_symbols": per_campaign,
                    "as_of_date": session,
                },
            })

    gap = {
        "unique_symbols": (
            max(0, int(target_unique_symbols) - int(existing_unique_symbols))
            if target_unique_symbols is not None else None
        ),
        "trigger_confirmed": (
            max(0, int(target_trigger_confirmed)
                - int(existing_trigger_confirmed))
            if target_trigger_confirmed is not None else None
        ),
        "matured_outcomes": (
            max(0, int(target_matured_outcomes)
                - int(existing_matured_outcomes))
            if target_matured_outcomes is not None else None
        ),
    }

    return {
        "plan_contract_version": CAMPAIGN_PLAN_CONTRACT_VERSION,
        "executed": False,
        "experiment_code": experiment.experiment_code,
        "experiment_version": experiment.experiment_version,
        "candidate_symbol_count": len(symbols),
        "already_covered_symbol_count": len(symbols) - len(remaining_symbols),
        "planned_symbol_count": len(remaining_symbols),
        "planned_symbols": remaining_symbols,
        "as_of_sessions": sessions,
        "max_symbols_per_campaign": per_campaign,
        "runner_chunk_size": CAMPAIGN_CHUNK_SIZE,
        "expected_campaign_count": len(batches),
        "batches": batches,
        "campaign_payloads": payloads,
        "remaining_evidence_gap": gap,
        "target_horizon": target_horizon,
        # Outcomes at the target horizon need this many completed forward
        # trading sessions AFTER the latest planned as-of session.
        "required_maturation_trading_sessions": (
            _HORIZON_SESSIONS[target_horizon]
        ),
        "maturation_after_session": sessions[-1] if sessions else None,
        "warnings": [
            WARNING_MIGRATION_013,
            WARNING_MASSIVE_REQUIRED,
        ],
    }
