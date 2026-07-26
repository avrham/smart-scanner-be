"""Prospective-campaign preflight (shadow_prospective_preflight.v1).

Read-only, PURE helpers that make the manual prospective workflow safe BEFORE
any campaign is created:

  * resolve the latest COMPLETED trading session from the local trading
    calendar (daily_bars session dates) plus the frozen completion policy
    (ny_session_close.v1) — never a bare `MAX(trading_date)` that could be an
    in-progress partial bar;
  * classify whether an equivalent campaign already exists (same experiment,
    same resolved session, same universe hash) so the operator resumes instead
    of duplicating, and a same-date-different-membership case is reported as a
    configuration mismatch rather than silently treated as safe.

Nothing here schedules, creates, mutates or calls a provider. The endpoint that
uses it is read-only.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from app.workers.shadow.universe_identity import (
    compute_universe_hash,
    normalize_campaign_symbols,
)


PREFLIGHT_CONTRACT_VERSION = "shadow_prospective_preflight.v1"

# Existing-campaign match outcomes (closed vocabulary).
MATCH_NONE = "no_matching_campaign"
MATCH_COMPLETED = "matching_completed_campaign"
MATCH_RESUMABLE = "matching_resumable_campaign"
MATCH_MEMBERSHIP_MISMATCH = "same_session_membership_mismatch"
MATCH_MEMBERSHIP_UNVERIFIABLE = "matching_session_membership_unverifiable"

# Session-completion resolution reasons.
SESSION_RESOLVED_LATEST_COMPLETED = "latest_bar_completed"
SESSION_RESOLVED_PRIOR = "latest_bar_partial_used_prior_session"
SESSION_UNRESOLVED = "no_completed_session"


def resolve_latest_completed_session(
    *,
    latest_bar_date: Optional[date],
    latest_bar_completion_state: Optional[str],
    reference_session_dates: List[date],
) -> Dict[str, Any]:
    """Resolve the latest COMPLETED trading session, PURE.

    `latest_bar_completion_state` is the frozen ny_session_close.v1 verdict for
    `latest_bar_date` ('completed' / 'partial' / 'unknown'). When the latest
    bar is only partial (session in progress), the resolved session steps back
    to the prior real session in `reference_session_dates` — never a calendar
    day. A future/unknown latest bar resolves to nothing (the caller refuses).
    """
    ref = sorted({d for d in reference_session_dates if d is not None})
    if latest_bar_date is None:
        return {
            "resolved_session": None,
            "resolution_reason": SESSION_UNRESOLVED,
            "is_valid_trading_session": False,
        }
    if latest_bar_completion_state == "completed":
        resolved = latest_bar_date
        reason = SESSION_RESOLVED_LATEST_COMPLETED
    elif latest_bar_completion_state == "partial":
        prior = [d for d in ref if d < latest_bar_date]
        resolved = prior[-1] if prior else None
        reason = (
            SESSION_RESOLVED_PRIOR if resolved is not None
            else SESSION_UNRESOLVED
        )
    else:
        resolved = None
        reason = SESSION_UNRESOLVED
    return {
        "resolved_session": resolved.isoformat() if resolved else None,
        "resolution_reason": reason,
        "is_valid_trading_session": bool(resolved is not None and resolved in ref),
    }


def _campaign_symbol_set(runs: List[Dict[str, Any]]) -> List[str]:
    raw: List[str] = []
    for run in runs:
        for sym in run.get("requested_symbols") or []:
            token = str(sym or "").strip().upper()
            if token:
                raw.append(token)
    if not raw:
        return []
    try:
        return normalize_campaign_symbols(sorted(set(raw)))
    except Exception:
        return sorted(set(raw))


def _group_campaigns(campaign_runs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Group chunk runs by campaign_id with union symbols + rolled-up status."""
    grouped: Dict[str, Dict[str, Any]] = {}
    for run in campaign_runs:
        block = run.get("campaign") or {}
        cid = str(block.get("campaign_id") or "unknown")
        entry = grouped.setdefault(cid, {
            "campaign_id": cid,
            "as_of_date": block.get("as_of_date"),
            "experiment_code": run.get("experiment_code"),
            "runs": [],
            "statuses": [],
        })
        entry["runs"].append(run)
        entry["statuses"].append(str(run.get("status")))
    for entry in grouped.values():
        entry["symbols"] = _campaign_symbol_set(entry["runs"])
        entry["universe_hash"] = (
            compute_universe_hash(entry["symbols"]) if entry["symbols"] else None
        )
        entry["all_completed"] = all(
            s == "completed" for s in entry["statuses"]
        )
    return grouped


def classify_existing_campaigns(
    campaign_runs: List[Dict[str, Any]],
    *,
    experiment_code: str,
    session_date: Optional[str],
    universe_hash: Optional[str],
) -> Dict[str, Any]:
    """Classify whether an equivalent campaign already exists for this session.

    Matches on (experiment_code, as_of_date). Among those, compares the frozen
    universe hash. PURE — reads only the supplied campaign records.
    """
    grouped = _group_campaigns(campaign_runs)
    same_session = [
        e for e in grouped.values()
        if e["experiment_code"] == experiment_code
        and e["as_of_date"] == session_date
    ]
    if not same_session:
        return {"match": MATCH_NONE, "campaign_id": None,
                "safe_to_create": True, "matches": []}

    # Prefer a same-membership match; then flag mismatches / unverifiable.
    same_membership = [
        e for e in same_session if e["universe_hash"] == universe_hash
    ]
    if same_membership:
        entry = same_membership[0]
        if entry["all_completed"]:
            match = MATCH_COMPLETED
        else:
            match = MATCH_RESUMABLE
        return {
            "match": match,
            "campaign_id": entry["campaign_id"],
            "safe_to_create": False,
            "matches": [entry["campaign_id"] for entry in same_membership],
        }

    unverifiable = [e for e in same_session if e["universe_hash"] is None]
    if unverifiable and universe_hash is not None:
        return {
            "match": MATCH_MEMBERSHIP_UNVERIFIABLE,
            "campaign_id": unverifiable[0]["campaign_id"],
            "safe_to_create": False,
            "matches": [e["campaign_id"] for e in unverifiable],
        }

    # Same session, different membership hash → configuration mismatch.
    return {
        "match": MATCH_MEMBERSHIP_MISMATCH,
        "campaign_id": same_session[0]["campaign_id"],
        "safe_to_create": False,
        "matches": [e["campaign_id"] for e in same_session],
    }


def build_prospective_preflight(
    *,
    experiment_code: str,
    symbol_report: Dict[str, Any],
    session: Dict[str, Any],
    campaign_match: Dict[str, Any],
    expected_count: Optional[int] = 50,
) -> Dict[str, Any]:
    """Compose the read-only preflight verdict. PURE."""
    reasons: List[str] = []
    if not symbol_report.get("ok"):
        reasons.extend(f"symbols:{p}" for p in symbol_report.get("problems", []))
    if session.get("resolved_session") is None:
        reasons.append(f"session:{session.get('resolution_reason')}")
    elif not session.get("is_valid_trading_session"):
        reasons.append("session:not_a_valid_trading_session")
    if not campaign_match.get("safe_to_create"):
        reasons.append(f"campaign:{campaign_match.get('match')}")

    creation_safe = not reasons
    return {
        "preflight_contract_version": PREFLIGHT_CONTRACT_VERSION,
        "experiment_code": experiment_code,
        "resolved_session": session.get("resolved_session"),
        "resolution_reason": session.get("resolution_reason"),
        "is_valid_trading_session": session.get("is_valid_trading_session"),
        "normalized_symbol_count": symbol_report.get("unique_count"),
        "expected_count": expected_count,
        "universe_hash": symbol_report.get("universe_hash"),
        "symbol_report": symbol_report,
        "existing_campaign_match": campaign_match.get("match"),
        "matching_campaign_id": campaign_match.get("campaign_id"),
        "creation_safe": creation_safe,
        "reasons": reasons,
    }


__all__ = [
    "PREFLIGHT_CONTRACT_VERSION",
    "MATCH_NONE",
    "MATCH_COMPLETED",
    "MATCH_RESUMABLE",
    "MATCH_MEMBERSHIP_MISMATCH",
    "MATCH_MEMBERSHIP_UNVERIFIABLE",
    "SESSION_RESOLVED_LATEST_COMPLETED",
    "SESSION_RESOLVED_PRIOR",
    "SESSION_UNRESOLVED",
    "resolve_latest_completed_session",
    "classify_existing_campaigns",
    "build_prospective_preflight",
]
