"""Bounded operator shadow-campaign tooling (Phase 9E6).

A campaign is an EXPLICIT, bounded, operator-triggered sequence of canonical
shadow runs over a declared experiment: the requested symbol list is
validated against an explicit safety bound, deterministically ordered and
chunked into the existing per-run symbol cap, and each chunk executes
through run_shadow_comparison unchanged. Nothing is scheduled, nothing
touches production ranking/watches/cards/alerts, and no new persistence is
introduced: every chunk is a normal strategy_shadow_runs row whose frozen
telemetry carries the campaign block, so campaign status reads reuse the
existing run/pair/outcome tables.

Concurrency is bounded BY CONSTRUCTION: chunks execute sequentially and the
runner evaluates one symbol at a time — a campaign can never fan out
unbounded provider load. Retries are idempotent through the existing pair
fingerprint dedupe (same experiment + same frames -> the same immutable
pair, linked, never duplicated).

This module prepares and validates campaign tooling only; executing a real
campaign requires an explicitly authorized operator call with a live
provider and the applied migration 013.
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import date
from typing import Any, Dict, List, Optional

from app.workers.shadow.constants import MAX_SHADOW_SYMBOLS
from app.workers.shadow.experiments import get_experiment
from app.workers.shadow.runner import run_shadow_comparison


logger = logging.getLogger(__name__)


CAMPAIGN_CONTRACT_VERSION = "shadow_campaign.v1"

# Hard campaign-level ceiling (4 chunks of the existing 25-symbol run cap).
# There is NO implicit full-universe mode: symbols are always explicit.
MAX_CAMPAIGN_SYMBOLS = 100
CAMPAIGN_CHUNK_SIZE = MAX_SHADOW_SYMBOLS

_SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,12}$")

# Per-symbol typed statuses.
SYMBOL_STATUS_EVALUATED = "evaluated"
SYMBOL_STATUS_REJECTED = "rejected"
SYMBOL_STATUS_RUN_FAILED = "run_failed"


class CampaignRequestError(ValueError):
    """Invalid campaign request (unbounded / malformed / oversized)."""


def plan_shadow_campaign(
    *,
    experiment_code: Any,
    symbols: Any,
    max_symbols: Any,
    as_of_date: Any = None,
    campaign_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate and deterministically plan one bounded campaign.

    Rules:
      * the experiment must be a DECLARED registry entry;
      * `max_symbols` is REQUIRED — a campaign must state its own explicit
        safety bound (capped at MAX_CAMPAIGN_SYMBOLS);
      * `symbols` is an explicit non-empty list — there is no implicit
        universe selector and no silent truncation: a list larger than the
        declared bound is rejected, never trimmed;
      * symbols are normalized (upper/strip/dedupe) and SORTED so a retried
        campaign covers exactly the same chunks in the same order;
      * `as_of_date` (optional) pins every chunk to the same session.
    """
    experiment = get_experiment(str(experiment_code or ""))

    if max_symbols is None:
        raise CampaignRequestError(
            "max_symbols is required — a campaign must declare an explicit "
            "safety bound"
        )
    try:
        bound = int(max_symbols)
    except (TypeError, ValueError):
        raise CampaignRequestError("max_symbols must be an integer")
    if bound < 1 or bound > MAX_CAMPAIGN_SYMBOLS:
        raise CampaignRequestError(
            f"max_symbols must be between 1 and {MAX_CAMPAIGN_SYMBOLS}"
        )

    if not isinstance(symbols, list) or not symbols:
        raise CampaignRequestError(
            "symbols must be an explicit non-empty list — campaigns never "
            "run an implicit universe"
        )
    normalized: List[str] = []
    seen = set()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        if not _SYMBOL_RE.match(symbol):
            raise CampaignRequestError(f"malformed symbol {symbol!r}")
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise CampaignRequestError("symbols must contain at least one ticker")
    if len(normalized) > bound:
        raise CampaignRequestError(
            f"{len(normalized)} symbols exceed the declared max_symbols "
            f"bound of {bound} — campaigns are never silently truncated"
        )
    normalized = sorted(normalized)

    parsed_as_of: Optional[date] = None
    if as_of_date is not None:
        if isinstance(as_of_date, date):
            parsed_as_of = as_of_date
        else:
            try:
                parsed_as_of = date.fromisoformat(str(as_of_date))
            except ValueError:
                raise CampaignRequestError(
                    "as_of_date must be a YYYY-MM-DD date"
                )

    chunks = [
        normalized[i:i + CAMPAIGN_CHUNK_SIZE]
        for i in range(0, len(normalized), CAMPAIGN_CHUNK_SIZE)
    ]
    return {
        "campaign_contract_version": CAMPAIGN_CONTRACT_VERSION,
        "campaign_id": campaign_id or str(uuid.uuid4()),
        "experiment_code": experiment.experiment_code,
        "experiment_version": experiment.experiment_version,
        "as_of_date": parsed_as_of.isoformat() if parsed_as_of else None,
        "max_symbols": bound,
        "symbols": normalized,
        "requested_count": len(normalized),
        "chunk_count": len(chunks),
        "chunks": chunks,
    }


def _campaign_telemetry_block(
    plan: Dict[str, Any], chunk_index: int
) -> Dict[str, Any]:
    """Frozen campaign block embedded in each chunk run's telemetry."""
    return {
        "campaign": {
            "campaign_contract_version": plan["campaign_contract_version"],
            "campaign_id": plan["campaign_id"],
            "experiment_code": plan["experiment_code"],
            "chunk_index": chunk_index,
            "chunk_count": plan["chunk_count"],
            "as_of_date": plan["as_of_date"],
            "requested_count": plan["requested_count"],
            "max_symbols": plan["max_symbols"],
        }
    }


async def run_shadow_campaign(
    provider: Any,
    plan: Dict[str, Any],
    *,
    now_utc: Any = None,
) -> Dict[str, Any]:
    """Execute one planned campaign: sequential bounded chunks, per-symbol
    typed statuses, partial-failure tolerance.

    One chunk failing never aborts the remaining chunks; its symbols are
    reported with status 'run_failed'. Returns the bounded campaign summary.
    """
    experiment = get_experiment(plan["experiment_code"])
    as_of = (
        date.fromisoformat(plan["as_of_date"])
        if plan.get("as_of_date") else None
    )

    runs: List[Dict[str, Any]] = []
    symbol_statuses: Dict[str, Dict[str, Any]] = {}
    evaluated = 0
    rejected = 0
    failed_chunks = 0

    for chunk_index, chunk in enumerate(plan["chunks"]):
        run_id = str(uuid.uuid4())
        try:
            summary = await run_shadow_comparison(
                provider,
                chunk,
                run_id=run_id,
                now_utc=now_utc,
                experiment=experiment,
                as_of_date=as_of,
                telemetry_extras=_campaign_telemetry_block(plan, chunk_index),
            )
        except Exception as exc:
            logger.error(
                "Campaign %s chunk %s failed: %s",
                plan["campaign_id"], chunk_index, type(exc).__name__,
            )
            summary = {"run_id": run_id, "status": "failed",
                       "error_code": f"chunk_{type(exc).__name__}"}

        status = summary.get("status")
        if status != "completed":
            failed_chunks += 1
            for symbol in chunk:
                symbol_statuses[symbol] = {
                    "status": SYMBOL_STATUS_RUN_FAILED,
                    "run_id": run_id,
                    "chunk_index": chunk_index,
                }
            runs.append({
                "run_id": run_id,
                "chunk_index": chunk_index,
                "status": status,
                "symbols": list(chunk),
            })
            continue

        telemetry = summary.get("telemetry") or {}
        for pair in summary.get("pairs") or []:
            evaluated += 1
            symbol_statuses[pair["symbol"]] = {
                "status": SYMBOL_STATUS_EVALUATED,
                "run_id": run_id,
                "chunk_index": chunk_index,
                "pair_id": pair["pair_id"],
                "created_new_pair": pair["created_new_pair"],
                "control_verdict": pair["control_verdict"],
                "candidate_verdict": pair["candidate_verdict"],
            }
        for reason, syms in (telemetry.get("rejected_symbols") or {}).items():
            for symbol in syms:
                rejected += 1
                symbol_statuses[symbol] = {
                    "status": SYMBOL_STATUS_REJECTED,
                    "reason_code": reason,
                    "run_id": run_id,
                    "chunk_index": chunk_index,
                }
        runs.append({
            "run_id": run_id,
            "chunk_index": chunk_index,
            "status": status,
            "symbols": list(chunk),
            "pair_count": telemetry.get("pair_count"),
            "pairs_created": telemetry.get("pairs_created"),
            "pairs_deduplicated": telemetry.get("pairs_deduplicated"),
        })

    if failed_chunks == 0:
        campaign_status = "completed"
    elif failed_chunks < plan["chunk_count"]:
        campaign_status = "completed_with_failures"
    else:
        campaign_status = "failed"

    return {
        "campaign_contract_version": plan["campaign_contract_version"],
        "campaign_id": plan["campaign_id"],
        "experiment_code": plan["experiment_code"],
        "experiment_version": plan["experiment_version"],
        "as_of_date": plan["as_of_date"],
        "status": campaign_status,
        "requested_count": plan["requested_count"],
        "chunk_count": plan["chunk_count"],
        "failed_chunk_count": failed_chunks,
        "evaluated_count": evaluated,
        "rejected_count": rejected,
        "unresolved_count": (
            plan["requested_count"] - evaluated - rejected
        ),
        "runs": runs,
        "symbol_statuses": symbol_statuses,
    }
