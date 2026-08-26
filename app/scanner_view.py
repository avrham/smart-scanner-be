"""PURE UI-facing presentation builders for the Smart Scanner product API.

Translates the existing prospective-campaign / shadow-evaluation persistence
shapes (a "campaign" IS a strategy_shadow_runs row carrying a `telemetry.campaign`
block; a "result row" is one strategy_shadow_pairs row + its control/candidate
strategy_shadow_evaluations) into a small, stable, frontend-consumable contract.

No provider, no DB, no network — deterministic given already-fetched rows.
Reuses app.prospective_campaign.candidate_signal_fields (the existing
deterministic extraction of the candidate's pre-rollout signal semantics from
its persisted details_snapshot) rather than re-deriving strategy semantics.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.prospective_campaign import candidate_signal_fields

OVERVIEW_CONTRACT_VERSION = "smart_scanner_overview.v1"
SYMBOL_DETAIL_CONTRACT_VERSION = "smart_scanner_symbol_detail.v1"
SCAN_LIST_CONTRACT_VERSION = "smart_scanner_scan_list.v1"

# ---- scanner-level state -------------------------------------------------- #
SCANNER_STATE_NO_CAMPAIGN_YET = "no_campaign_yet"
SCANNER_STATE_RUNNING = "running"
SCANNER_STATE_FRESH = "fresh"
SCANNER_STATE_STALE = "stale"
SCANNER_STATE_FAILED = "failed"

SCANNER_STATES = (
    SCANNER_STATE_NO_CAMPAIGN_YET, SCANNER_STATE_RUNNING,
    SCANNER_STATE_FRESH, SCANNER_STATE_STALE, SCANNER_STATE_FAILED,
)

# ---- per-symbol result state ----------------------------------------------- #
SYMBOL_STATE_VALID_RESULT = "valid_result"
SYMBOL_STATE_NOT_READY = "not_ready_insufficient_data"
SYMBOL_STATE_NO_SIGNAL = "no_signal"
SYMBOL_STATE_STALE = "stale"
SYMBOL_STATE_FAILED = "failed"

SYMBOL_STATES = (
    SYMBOL_STATE_VALID_RESULT, SYMBOL_STATE_NOT_READY,
    SYMBOL_STATE_NO_SIGNAL, SYMBOL_STATE_STALE, SYMBOL_STATE_FAILED,
)

_SIGNAL_VERDICTS = ("ENTER", "WATCH")


def classify_scanner_state(
    *, campaign_status: Optional[str], campaign_as_of_date: Optional[str],
    latest_completed_session: str,
) -> str:
    """Scanner-level state the UI renders at the top of the main screen.

    ``campaign_status`` is None when no campaign has ever completed;
    otherwise one of the strategy_shadow_runs values ('running','completed',
    'failed'). A completed campaign is FRESH only when its resolved session
    equals the latest fully-completed US market session; otherwise STALE
    (the pipeline has not yet produced today's scan).
    """
    if campaign_status is None:
        return SCANNER_STATE_NO_CAMPAIGN_YET
    if campaign_status == "running":
        return SCANNER_STATE_RUNNING
    if campaign_status == "failed":
        return SCANNER_STATE_FAILED
    # completed
    if campaign_as_of_date == latest_completed_session:
        return SCANNER_STATE_FRESH
    return SCANNER_STATE_STALE


def classify_symbol_state(
    *, scanner_state: str, has_candidate_result: bool,
    candidate_verdict: Optional[str],
) -> str:
    """Per-symbol state (mission item 10): valid_result / not_ready /
    no_signal / stale / failed. Never inferred from an HTTP status — always
    an explicit field the UI can switch on."""
    if scanner_state == SCANNER_STATE_FAILED:
        return SYMBOL_STATE_FAILED if not has_candidate_result else (
            SYMBOL_STATE_VALID_RESULT if candidate_verdict in _SIGNAL_VERDICTS
            else SYMBOL_STATE_NO_SIGNAL
        )
    if scanner_state in (SCANNER_STATE_NO_CAMPAIGN_YET, SCANNER_STATE_RUNNING):
        return SYMBOL_STATE_NOT_READY
    if not has_candidate_result:
        return SYMBOL_STATE_NOT_READY
    if scanner_state == SCANNER_STATE_STALE:
        return SYMBOL_STATE_STALE
    return (
        SYMBOL_STATE_VALID_RESULT if candidate_verdict in _SIGNAL_VERDICTS
        else SYMBOL_STATE_NO_SIGNAL
    )


def build_overview_row(row: Dict[str, Any], *, scanner_state: str) -> Dict[str, Any]:
    """One result row for the main scanner screen. `row` has the raw SQL
    columns: symbol, candidate_verdict, candidate_score, candidate_details,
    control_verdict, control_score (candidate_details may be None if that
    arm's evaluation is missing — a failed/partial campaign)."""
    candidate_details = row.get("candidate_details") or {}
    sig = candidate_signal_fields(candidate_details) if candidate_details else None
    has_candidate = row.get("candidate_verdict") is not None
    symbol_state = classify_symbol_state(
        scanner_state=scanner_state, has_candidate_result=has_candidate,
        candidate_verdict=row.get("candidate_verdict"),
    )
    return {
        "symbol": row["symbol"],
        "symbol_state": symbol_state,
        "candidate_verdict": row.get("candidate_verdict"),
        "candidate_score": row.get("candidate_score"),
        "control_verdict": row.get("control_verdict"),
        "control_score": row.get("control_score"),
        "agreement": (
            row.get("candidate_verdict") == row.get("control_verdict")
            if has_candidate and row.get("control_verdict") is not None else None
        ),
        "setup_present": sig["setup_present"] if sig else None,
        "trigger_confirmed": sig["trigger_confirmed"] if sig else None,
    }


def summarize_results(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {state: 0 for state in SYMBOL_STATES}
    summary["total"] = len(rows)
    for r in rows:
        summary[r["symbol_state"]] = summary.get(r["symbol_state"], 0) + 1
    return summary


def build_symbol_evidence(candidate_details: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The deterministic 'why' block for the symbol-detail screen — never
    invents explanations beyond what candidate_signal_fields already
    extracts from the strategy's own persisted details_snapshot."""
    if not candidate_details:
        return None
    return candidate_signal_fields(candidate_details)


__all__ = [
    "OVERVIEW_CONTRACT_VERSION", "SYMBOL_DETAIL_CONTRACT_VERSION",
    "SCAN_LIST_CONTRACT_VERSION",
    "SCANNER_STATE_NO_CAMPAIGN_YET", "SCANNER_STATE_RUNNING",
    "SCANNER_STATE_FRESH", "SCANNER_STATE_STALE", "SCANNER_STATE_FAILED",
    "SCANNER_STATES",
    "SYMBOL_STATE_VALID_RESULT", "SYMBOL_STATE_NOT_READY",
    "SYMBOL_STATE_NO_SIGNAL", "SYMBOL_STATE_STALE", "SYMBOL_STATE_FAILED",
    "SYMBOL_STATES",
    "classify_scanner_state", "classify_symbol_state",
    "build_overview_row", "summarize_results", "build_symbol_evidence",
]
