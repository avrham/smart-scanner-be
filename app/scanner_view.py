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

OVERVIEW_CONTRACT_VERSION = "smart_scanner_overview.v2"
SYMBOL_DETAIL_CONTRACT_VERSION = "smart_scanner_symbol_detail.v2"
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

# ---- attention tiers ------------------------------------------------------- #
# The product question is "what should I inspect first?", which is NOT the same
# as the strategy's verdict. These tiers are derived ONLY from deterministic
# fields the arms already persist. Justification for each input is recorded in
# ops/analysis/scanner_signal_forensics.py output; the short version:
#
#   * candidate_score is NOT used for tiering. compute_ranking() returns None
#     whenever ANY quality component is None, so 75/100 real evaluations have no
#     score at all, and among the 25 that do the WATCH and AVOID ranges OVERLAP
#     ([0.4048,0.5822] vs [0.3854,0.4303]). The strategy's own docstring calls it
#     "Ranking-only components. Never a gate." Sorting by it is false precision.
#   * setup_state IS used: it is a real 3-way outcome from policy.py —
#     'valid' (every structural gate passed), 'invalid' (structure was read and
#     disqualified) and 'unknown' (structure could not be read at all). Those
#     last two mean very different things to a person and currently look alike.
#   * the baseline arm is used only to surface DISAGREEMENT, never as a second
#     opinion that outranks the candidate.
ATTENTION_HIGH = "high_attention"
ATTENTION_DEVELOPING = "developing"
ATTENTION_LOW = "low_attention"
ATTENTION_NO_READ = "no_read"
ATTENTION_NOT_READY = "not_ready"

ATTENTION_TIERS = (
    ATTENTION_HIGH, ATTENTION_DEVELOPING, ATTENTION_LOW,
    ATTENTION_NO_READ, ATTENTION_NOT_READY,
)

# Lower sorts first. Stable, total, and independent of any float.
ATTENTION_ORDER = {tier: i for i, tier in enumerate(ATTENTION_TIERS)}

# ---- how the two arms related on this session ------------------------------ #
# `agreement` (verdict string equality) is close to meaningless here: the
# candidate arm only ever emits WATCH/AVOID and the baseline only ever emits
# ENTER/AVOID, so the only value both can produce is AVOID. "Agreement" was
# therefore just "both said AVOID" (65/100 rows). This reports the relationship
# explicitly instead.
CROSS_ARM_BOTH_FLAGGED = "both_flagged"
CROSS_ARM_CANDIDATE_ONLY = "candidate_only"
CROSS_ARM_BASELINE_ONLY = "baseline_only"
CROSS_ARM_NEITHER = "neither_flagged"
CROSS_ARM_NOT_COMPARABLE = "not_comparable"


def classify_cross_arm(
    *, candidate_verdict: Optional[str], control_verdict: Optional[str]
) -> str:
    """How the candidate and baseline readings of the SAME session relate."""
    if candidate_verdict is None or control_verdict is None:
        return CROSS_ARM_NOT_COMPARABLE
    cand = candidate_verdict in _SIGNAL_VERDICTS
    ctrl = control_verdict in _SIGNAL_VERDICTS
    if cand and ctrl:
        return CROSS_ARM_BOTH_FLAGGED
    if cand:
        return CROSS_ARM_CANDIDATE_ONLY
    if ctrl:
        return CROSS_ARM_BASELINE_ONLY
    return CROSS_ARM_NEITHER


def classify_attention(
    *,
    has_candidate_result: bool,
    candidate_verdict: Optional[str],
    setup_state: Optional[str],
    readiness_status: Optional[str],
    control_verdict: Optional[str],
) -> str:
    """Which attention tier a symbol belongs to. Order of the branches IS the
    definition — each is checked before the next and never re-entered."""
    # 1. Nothing was evaluated, or the inputs themselves were not usable.
    if not has_candidate_result:
        return ATTENTION_NOT_READY
    if readiness_status is not None and readiness_status != "ready":
        return ATTENTION_NOT_READY
    # 2. The candidate arm passed every structural gate it applies.
    if candidate_verdict in _SIGNAL_VERDICTS:
        return ATTENTION_HIGH
    # 3. It did not, but the independent baseline reading of the same session
    #    did — the two disagree, which is worth understanding.
    if control_verdict in _SIGNAL_VERDICTS:
        return ATTENTION_DEVELOPING
    # 4. The structure was read and actively disqualified.
    if setup_state == "invalid":
        return ATTENTION_LOW
    # 5. The structure could not be read at all — no opinion, not a negative one.
    return ATTENTION_NO_READ


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


def structure_state(candidate_details: Optional[Dict[str, Any]]) -> Optional[str]:
    """'recognized' | 'ambiguous' | 'unknown' — the structure classifier's own
    read, persisted verbatim by the strategy."""
    return ((candidate_details or {}).get("structure") or {}).get("state")


def reason_code(candidate_details: Optional[Dict[str, Any]]) -> Optional[str]:
    """The policy's single machine reason for this verdict (e.g.
    'watch_setup_valid', 'unknown_structure', 'htf_direction_conflict'). This is
    the most granular differentiator the arms actually produce."""
    return ((candidate_details or {}).get("policy") or {}).get("reason_code")


def build_overview_row(row: Dict[str, Any], *, scanner_state: str) -> Dict[str, Any]:
    """One result row for the main scanner screen. `row` has the raw SQL
    columns: symbol, candidate_verdict, candidate_score, candidate_details,
    control_verdict, control_score (candidate_details may be None if that
    arm's evaluation is missing — a failed/partial campaign)."""
    candidate_details = row.get("candidate_details") or {}
    sig = candidate_signal_fields(candidate_details) if candidate_details else None
    has_candidate = row.get("candidate_verdict") is not None
    candidate_verdict = row.get("candidate_verdict")
    control_verdict = row.get("control_verdict")
    setup = sig["setup_state"] if sig else None
    symbol_state = classify_symbol_state(
        scanner_state=scanner_state, has_candidate_result=has_candidate,
        candidate_verdict=candidate_verdict,
    )
    return {
        "symbol": row["symbol"],
        "symbol_state": symbol_state,
        "attention": classify_attention(
            has_candidate_result=has_candidate,
            candidate_verdict=candidate_verdict,
            setup_state=setup,
            readiness_status=sig["readiness_status"] if sig else None,
            control_verdict=control_verdict,
        ),
        "candidate_verdict": candidate_verdict,
        "candidate_score": row.get("candidate_score"),
        "control_verdict": control_verdict,
        "control_score": row.get("control_score"),
        "cross_arm": classify_cross_arm(
            candidate_verdict=candidate_verdict, control_verdict=control_verdict
        ),
        # The structural read, which is what actually separates symbols:
        # 'valid' / 'invalid' / 'unknown' plus the classifier's own state.
        "setup_state": setup,
        "structure_state": structure_state(candidate_details),
        "reason_code": reason_code(candidate_details),
        "trigger_confirmed": sig["trigger_confirmed"] if sig else None,
        # DEPRECATED, retained for contract compatibility only. Derived from
        # `setup_state not in (None,'absent','none')`, and this strategy emits
        # only valid/invalid/unknown — so it is True for every evaluated symbol
        # (100/100 across all four real campaigns) and differentiates nothing.
        # Use `setup_state`.
        "setup_present": sig["setup_present"] if sig else None,
        "agreement": (
            candidate_verdict == control_verdict
            if has_candidate and control_verdict is not None else None
        ),
    }


def summarize_results(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {state: 0 for state in SYMBOL_STATES}
    summary["total"] = len(rows)
    for r in rows:
        summary[r["symbol_state"]] = summary.get(r["symbol_state"], 0) + 1
    return summary


def summarize_attention(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Counts per attention tier. Unlike results_summary (which collapses every
    row to `stale` the moment a scan is one session behind), this stays
    informative about the scan's CONTENT."""
    summary = {tier: 0 for tier in ATTENTION_TIERS}
    summary["total"] = len(rows)
    for r in rows:
        tier = r.get("attention")
        if tier in summary:
            summary[tier] += 1
    return summary


def attention_sort_key(row: Dict[str, Any]) -> Any:
    """Deterministic 'inspect first' ordering. Tier first, then the structural
    read, then symbol — never the candidate score, which is absent for 75% of
    real evaluations and whose WATCH/AVOID ranges overlap."""
    setup_rank = {"valid": 0, "invalid": 1, "unknown": 2}.get(
        row.get("setup_state") or "", 3
    )
    return (
        ATTENTION_ORDER.get(row.get("attention"), len(ATTENTION_TIERS)),
        setup_rank,
        row.get("symbol") or "",
    )


# ---- gate progression ------------------------------------------------------ #
# The strategy evaluates a symbol in a fixed order (structure -> setup -> 4H
# trigger -> rollout gate). Reporting WHERE it stopped is the deterministic
# answer to "why is this symbol here, and what would have to change?". Nothing
# below infers anything the arms did not already record.
GATE_STRUCTURE = "structure"
GATE_SETUP = "setup"
GATE_TRIGGER = "trigger"
GATE_ROLLOUT = "rollout"

GATE_ORDER = (GATE_STRUCTURE, GATE_SETUP, GATE_TRIGGER, GATE_ROLLOUT)

GATE_PASSED = "passed"
GATE_BLOCKED = "blocked"
GATE_UNKNOWN = "unknown"

# Which gate each recorded waiting reason belongs to. Unmapped tokens are
# attributed to the trigger gate only if they name it, else to the setup gate —
# never invented into a gate that did not record them.
_WAITING_REASON_GATE = {
    "entry_reference_unavailable": GATE_TRIGGER,
    "four_hour_data_missing": GATE_TRIGGER,
    "four_hour_trigger_unknown": GATE_TRIGGER,
    "four_hour_trigger_missing": GATE_TRIGGER,
    "four_hour_trigger_contradicted": GATE_TRIGGER,
    "unknown_phase": GATE_SETUP,
    "phase_not_enter_eligible": GATE_SETUP,
    "enter_disabled_shadow_only": GATE_ROLLOUT,
}


def build_gate_progress(
    candidate_details: Optional[Dict[str, Any]], *, allow_enter: bool
) -> Optional[List[Dict[str, Any]]]:
    """Ordered structure -> setup -> trigger -> rollout progression.

    Each entry is {gate, status, code} where `code` is the arm's own recorded
    token (or None). Status is passed/blocked/unknown — 'unknown' meaning the
    scanner could not form a read, which is deliberately NOT the same as a
    negative read.
    """
    if not candidate_details:
        return None
    sig = candidate_signal_fields(candidate_details)
    struct = structure_state(candidate_details)
    code = reason_code(candidate_details)
    setup = sig["setup_state"]

    if struct == "recognized":
        structure_entry = {"gate": GATE_STRUCTURE, "status": GATE_PASSED, "code": None}
    elif struct in ("ambiguous",):
        structure_entry = {"gate": GATE_STRUCTURE, "status": GATE_BLOCKED, "code": code}
    else:
        structure_entry = {"gate": GATE_STRUCTURE, "status": GATE_UNKNOWN, "code": code}

    if setup == "valid":
        setup_entry = {"gate": GATE_SETUP, "status": GATE_PASSED, "code": None}
    elif setup == "invalid":
        setup_entry = {"gate": GATE_SETUP, "status": GATE_BLOCKED, "code": code}
    else:
        setup_entry = {"gate": GATE_SETUP, "status": GATE_UNKNOWN, "code": code}

    trigger_codes = [
        w for w in sig["waiting_reasons"]
        if _WAITING_REASON_GATE.get(w) == GATE_TRIGGER
    ]
    if sig["trigger_confirmed"]:
        trigger_entry = {"gate": GATE_TRIGGER, "status": GATE_PASSED, "code": None}
    else:
        trigger_entry = {
            "gate": GATE_TRIGGER,
            "status": GATE_UNKNOWN if sig["trigger_state"] in (None, "unknown")
                      else GATE_BLOCKED,
            "code": trigger_codes[0] if trigger_codes else sig["trigger_state"],
        }

    rollout_entry = {
        "gate": GATE_ROLLOUT,
        "status": GATE_PASSED if allow_enter else GATE_BLOCKED,
        "code": None if allow_enter else "enter_disabled_shadow_only",
    }
    return [structure_entry, setup_entry, trigger_entry, rollout_entry]


def build_blockers(
    candidate_details: Optional[Dict[str, Any]], *, allow_enter: bool
) -> List[Dict[str, Any]]:
    """Every gate that is not passed, in evaluation order — i.e. exactly what
    would have to change for this symbol to classify more strongly."""
    progress = build_gate_progress(candidate_details, allow_enter=allow_enter)
    if progress is None:
        return []
    return [g for g in progress if g["status"] != GATE_PASSED]


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
    "ATTENTION_HIGH", "ATTENTION_DEVELOPING", "ATTENTION_LOW",
    "ATTENTION_NO_READ", "ATTENTION_NOT_READY", "ATTENTION_TIERS",
    "ATTENTION_ORDER", "classify_attention", "summarize_attention",
    "attention_sort_key",
    "CROSS_ARM_BOTH_FLAGGED", "CROSS_ARM_CANDIDATE_ONLY",
    "CROSS_ARM_BASELINE_ONLY", "CROSS_ARM_NEITHER",
    "CROSS_ARM_NOT_COMPARABLE", "classify_cross_arm",
    "GATE_STRUCTURE", "GATE_SETUP", "GATE_TRIGGER", "GATE_ROLLOUT",
    "GATE_ORDER", "GATE_PASSED", "GATE_BLOCKED", "GATE_UNKNOWN",
    "build_gate_progress", "build_blockers",
    "structure_state", "reason_code",
    "SCANNER_STATE_NO_CAMPAIGN_YET", "SCANNER_STATE_RUNNING",
    "SCANNER_STATE_FRESH", "SCANNER_STATE_STALE", "SCANNER_STATE_FAILED",
    "SCANNER_STATES",
    "SYMBOL_STATE_VALID_RESULT", "SYMBOL_STATE_NOT_READY",
    "SYMBOL_STATE_NO_SIGNAL", "SYMBOL_STATE_STALE", "SYMBOL_STATE_FAILED",
    "SYMBOL_STATES",
    "classify_scanner_state", "classify_symbol_state",
    "build_overview_row", "summarize_results", "build_symbol_evidence",
]
