"""Single prospective-campaign post-run audit (shadow_campaign_audit.v1).

Read-only, PURE classification of ONE bounded shadow campaign into neutral
metrics plus an explicit verdict. It reuses the existing decision-state
aggregation (`aggregate_strategy_shadow_metrics`) verbatim and adds only the
campaign-identity, membership and side-effect invariants — it never re-derives
returns, never mutates a row and never enables anything.

Verdict vocabulary (closed):

    valid                    terminal-successful, exact expected membership,
                             no duplicates, no side effects, no systemic
                             provider/pair failure (ZERO confirmed triggers is
                             a perfectly valid result)
    invalid                  a hard invariant was violated (duplicates,
                             membership drift, watches/cards created,
                             allow_enter=true, systemic provider/pair failure)
    incomplete               still resumable — some chunk run has not reached
                             a terminal successful state, with no systemic
                             failure (re-submitting the same payload resumes it)
    membership_unverifiable  neither a persisted campaign symbol set nor an
                             explicit expected symbol list was available, so
                             exact membership could not be proven (a bare count
                             match is NOT accepted as membership)

Expected-membership precedence (strongest first):
    1. the campaign's persisted requested symbols (union of its chunk runs);
    2. an explicitly supplied expected symbol list (the frozen universe file);
    3. an expected count only — a WEAK assertion that can never prove
       membership, so it yields membership_unverifiable.

Provider-failure policy: a SYSTEMIC provider failure invalidates the campaign —
defined as every evaluation recording `unsupported_provider` (e.g. the whole
run used FMP, which cannot produce honest 4H evidence). An ISOLATED, bounded
provider failure (a handful of typed `four_hour` fetch_errors on otherwise
healthy evaluations) does NOT invalidate the campaign — those are honest typed
per-symbol states, surfaced in `provider_failure_count` for review, and the
affected 4H-bearing pairs can be re-collected under a new fingerprint later. A
`pair_error` rejection is treated as systemic (the paired comparison could not
persist) and always invalidates.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.workers.shadow.strategy_metrics import aggregate_strategy_shadow_metrics
from app.workers.shadow.universe_identity import (
    compute_universe_hash,
    normalize_campaign_symbols,
    universe_identity,
)


CAMPAIGN_AUDIT_CONTRACT_VERSION = "shadow_campaign_audit.v1"

VERDICT_VALID = "valid"
VERDICT_INVALID = "invalid"
VERDICT_INCOMPLETE = "incomplete"
VERDICT_MEMBERSHIP_UNVERIFIABLE = "membership_unverifiable"

MEMBERSHIP_SOURCE_PERSISTED = "persisted_campaign_symbols"
MEMBERSHIP_SOURCE_EXPLICIT = "explicit_expected_symbols"
MEMBERSHIP_SOURCE_COUNT_ONLY = "expected_count_only"
MEMBERSHIP_SOURCE_NONE = "none"

# Rejected-symbol reason codes that indicate a systemic pipeline failure (the
# whole comparison could not be persisted) rather than a per-symbol data gap.
SYSTEMIC_PAIR_REASONS = frozenset({"pair_error"})

# Additive per-record metric fields summed across identity groups to yield
# flat campaign-level totals.
_ADDITIVE_METRIC_FIELDS = (
    "evaluated_count",
    "daily_ready_count",
    "daily_insufficient_count",
    "readiness_unknown_count",
    "four_hour_ready_count",
    "four_hour_frames_built_count",
    "four_hour_fetch_error_count",
    "four_hour_unsupported_provider_count",
    "setup_present_count",
    "trigger_confirmed_count",
    "trigger_waiting_count",
    "trigger_contradicted_count",
    "matured_outcome_count",
    "rollout_blocked_count",
    "with_outcome_count",
    "missing_outcome_count",
)


def _sum_metric_field(metrics: Dict[str, Any], field: str) -> int:
    return sum(int(g.get(field) or 0) for g in metrics.get("groups", []))


def _merge_distribution(metrics: Dict[str, Any], field: str) -> Dict[str, int]:
    merged: Dict[str, int] = {}
    for group in metrics.get("groups", []):
        for key, value in (group.get(field) or {}).items():
            merged[key] = merged.get(key, 0) + int(value)
    return dict(sorted(merged.items()))


def _persisted_symbols(campaign_runs: List[Dict[str, Any]]) -> List[str]:
    """Union of every chunk run's persisted requested_symbols, normalized.

    Malformed/legacy tokens are skipped defensively (never raised) so the
    audit degrades to a weaker membership source instead of crashing.
    """
    raw: List[str] = []
    for run in campaign_runs or []:
        for sym in run.get("requested_symbols") or []:
            token = str(sym or "").strip().upper()
            if token:
                raw.append(token)
    if not raw:
        return []
    try:
        return normalize_campaign_symbols(sorted(set(raw)))
    except Exception:
        # Degrade gracefully: deterministic sorted-unique without validation.
        return sorted(set(raw))


def _campaign_status(campaign_runs: List[Dict[str, Any]]) -> str:
    """Overall status derived from chunk-run statuses (mirrors the runner)."""
    statuses = [str(r.get("status")) for r in campaign_runs]
    if not statuses:
        return "unknown"
    if all(s == "completed" for s in statuses):
        return "completed"
    if all(s != "completed" for s in statuses):
        return "failed"
    return "completed_with_failures"


def build_campaign_audit(
    records: List[Dict[str, Any]],
    campaign_runs: List[Dict[str, Any]],
    *,
    campaign_id: Optional[str] = None,
    expected_symbols: Optional[List[str]] = None,
    expected_count: Optional[int] = None,
    watches_created: int = 0,
    decision_cards_created: int = 0,
) -> Dict[str, Any]:
    """Audit one campaign's candidate-arm evaluation records + chunk runs.

    `records` are the candidate-arm shadow evaluations scoped to this campaign
    (persistence read shape). `campaign_runs` are the campaign's chunk runs.
    `watches_created` / `decision_cards_created` are passed by the caller from
    the production side (always 0 in the shadow pipeline, which has no such
    creation path); accepting them keeps the invariant explicit and testable.
    """
    block = (campaign_runs[0].get("campaign") if campaign_runs else None) or {}
    resolved_campaign_id = campaign_id or block.get("campaign_id")
    session_date = block.get("as_of_date")

    metrics = aggregate_strategy_shadow_metrics(records)
    totals = {f: _sum_metric_field(metrics, f) for f in _ADDITIVE_METRIC_FIELDS}
    evaluated = totals["evaluated_count"]

    # ---- membership -------------------------------------------------------- #
    evaluated_symbols = sorted({
        str(r.get("symbol")).upper() for r in records if r.get("symbol")
    })
    # Per-symbol candidate-evaluation counts: a clean campaign has exactly one
    # candidate evaluation per symbol.
    symbol_counts: Dict[str, int] = {}
    for r in records:
        sym = str(r.get("symbol") or "").upper()
        if sym:
            symbol_counts[sym] = symbol_counts.get(sym, 0) + 1
    duplicate_evaluations = {s: c for s, c in symbol_counts.items() if c > 1}

    persisted = _persisted_symbols(campaign_runs)
    explicit = (
        normalize_campaign_symbols(expected_symbols)
        if expected_symbols else []
    )

    if persisted:
        membership_source = MEMBERSHIP_SOURCE_PERSISTED
        expected_set = persisted
    elif explicit:
        membership_source = MEMBERSHIP_SOURCE_EXPLICIT
        expected_set = explicit
    elif expected_count is not None:
        membership_source = MEMBERSHIP_SOURCE_COUNT_ONLY
        expected_set = None
    else:
        membership_source = MEMBERSHIP_SOURCE_NONE
        expected_set = None

    # Cross-check the frozen file against the persisted set when BOTH exist.
    explicit_vs_persisted_mismatch = bool(
        persisted and explicit and set(persisted) != set(explicit)
    )

    if expected_set is not None:
        missing_symbols = sorted(set(expected_set) - set(evaluated_symbols))
        unexpected_symbols = sorted(set(evaluated_symbols) - set(expected_set))
        expected_symbol_count = len(expected_set)
    else:
        missing_symbols = []
        unexpected_symbols = []
        expected_symbol_count = expected_count

    # ---- side effects & allow_enter --------------------------------------- #
    allow_enter_true = [
        r for r in records
        if isinstance((r.get("policy") or {}).get("allow_enter"), bool)
        and (r.get("policy") or {}).get("allow_enter") is True
    ]
    allow_enter_true_count = len(allow_enter_true)

    # ---- provider / pair failures ----------------------------------------- #
    pair_error_count = 0
    provider_reject_count = 0
    for run in campaign_runs:
        for reason, syms in (run.get("rejected_symbols") or {}).items():
            n = len(syms or [])
            if reason in SYSTEMIC_PAIR_REASONS:
                pair_error_count += n
            elif "fetch" in str(reason) or "provider" in str(reason):
                provider_reject_count += n
    failed_run_error_codes = sorted({
        str(r.get("error_code")) for r in campaign_runs
        if r.get("status") != "completed" and r.get("error_code")
    })
    provider_failure_count = (
        totals["four_hour_fetch_error_count"] + provider_reject_count
    )
    systemic_provider_failure = bool(
        evaluated > 0
        and totals["four_hour_unsupported_provider_count"] == evaluated
    )

    campaign_status = _campaign_status(campaign_runs)
    terminal_success = campaign_status == "completed"

    # ---- verdict ----------------------------------------------------------- #
    invalid_reasons: List[str] = []
    if duplicate_evaluations:
        invalid_reasons.append("duplicate_evaluations")
    if watches_created > 0:
        invalid_reasons.append("watches_created")
    if decision_cards_created > 0:
        invalid_reasons.append("decision_cards_created")
    if allow_enter_true_count > 0:
        invalid_reasons.append("allow_enter_true")
    if pair_error_count > 0:
        invalid_reasons.append("systemic_pair_failure")
    if systemic_provider_failure:
        invalid_reasons.append("systemic_provider_failure")
    if expected_set is not None and missing_symbols:
        invalid_reasons.append("missing_expected_symbols")
    if expected_set is not None and unexpected_symbols:
        invalid_reasons.append("unexpected_symbols")
    if explicit_vs_persisted_mismatch:
        invalid_reasons.append("membership_hash_mismatch")

    incomplete_reasons: List[str] = []
    if not terminal_success:
        incomplete_reasons.append("non_terminal_campaign_status")

    if invalid_reasons:
        verdict = VERDICT_INVALID
    elif incomplete_reasons:
        verdict = VERDICT_INCOMPLETE
    elif expected_set is None:
        verdict = VERDICT_MEMBERSHIP_UNVERIFIABLE
    else:
        verdict = VERDICT_VALID

    verdict_reasons = list(invalid_reasons or incomplete_reasons)
    if verdict == VERDICT_MEMBERSHIP_UNVERIFIABLE:
        verdict_reasons = ["no_expected_symbol_set_available"]

    return {
        "audit_contract_version": CAMPAIGN_AUDIT_CONTRACT_VERSION,
        "campaign_id": resolved_campaign_id,
        "session_date": session_date,
        "experiment_code": (
            campaign_runs[0].get("experiment_code") if campaign_runs else None
        ),
        "campaign_status": campaign_status,
        "verdict": verdict,
        "verdict_reasons": verdict_reasons,
        "terminal_success": terminal_success,
        # membership
        "membership_source": membership_source,
        "expected_symbol_count": expected_symbol_count,
        "evaluation_count": evaluated,
        "unique_symbol_count": len(evaluated_symbols),
        "missing_symbols": missing_symbols,
        "unexpected_symbols": unexpected_symbols,
        "duplicate_evaluations": dict(sorted(duplicate_evaluations.items())),
        "evaluated_universe_hash": (
            compute_universe_hash(evaluated_symbols) if evaluated_symbols
            else None
        ),
        "expected_universe": (
            universe_identity(expected_set) if expected_set else None
        ),
        "explicit_vs_persisted_mismatch": explicit_vs_persisted_mismatch,
        # decision-state metrics (from the reused aggregation)
        "daily_ready_count": totals["daily_ready_count"],
        "daily_not_ready_count": evaluated - totals["daily_ready_count"],
        "four_hour_ready_count": totals["four_hour_ready_count"],
        "four_hour_not_ready_count": evaluated - totals["four_hour_ready_count"],
        "setup_present_count": totals["setup_present_count"],
        "setup_absent_count": evaluated - totals["setup_present_count"],
        "trigger_waiting_count": totals["trigger_waiting_count"],
        "trigger_confirmed_count": totals["trigger_confirmed_count"],
        "early_failure_reason_distribution": _merge_distribution(
            metrics, "failure_reason_distribution"
        ),
        "matured_outcome_count": totals["matured_outcome_count"],
        "with_outcome_count": totals["with_outcome_count"],
        "missing_outcome_count": totals["missing_outcome_count"],
        # side effects (structurally zero in the shadow pipeline)
        "watches_created": watches_created,
        "decision_cards_created": decision_cards_created,
        "allow_enter_true_count": allow_enter_true_count,
        # provider / pair failures
        "provider_failure_count": provider_failure_count,
        "pair_error_count": pair_error_count,
        "systemic_provider_failure": systemic_provider_failure,
        "failed_run_error_codes": failed_run_error_codes,
        # identity grouping (mixed identities surface as >1 group)
        "identity_group_count": len(metrics.get("groups", [])),
    }


__all__ = [
    "CAMPAIGN_AUDIT_CONTRACT_VERSION",
    "VERDICT_VALID",
    "VERDICT_INVALID",
    "VERDICT_INCOMPLETE",
    "VERDICT_MEMBERSHIP_UNVERIFIABLE",
    "MEMBERSHIP_SOURCE_PERSISTED",
    "MEMBERSHIP_SOURCE_EXPLICIT",
    "MEMBERSHIP_SOURCE_COUNT_ONLY",
    "MEMBERSHIP_SOURCE_NONE",
    "build_campaign_audit",
]
