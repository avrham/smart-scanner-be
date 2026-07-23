"""Versioned evidence-quality and coverage audit (Phase 9F4).

Contract: shadow_evidence_quality.v1.

Pure classification over frozen evidence: evaluation records, campaign
chunk runs, joined outcome rows and the (optional) strategy discovery
snapshot. Nothing is repaired, mutated or re-derived — the audit only
REPORTS honesty gaps so an operator can weigh them before any rollout
discussion. Severity vocabulary (closed, versioned):

    blocking       evidence cannot support a rollout discussion at all
    warning        materially weakens the evidence; review required
    informational  expected/explained states worth surfacing
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.workers.shadow.evidence_review import outcome_maturity
from app.workers.shadow.strategy_metrics import (
    TRIGGER_CLASS_CONFIRMED,
    classify_trigger_state,
)


QUALITY_AUDIT_CONTRACT_VERSION = "shadow_evidence_quality.v1"

SEVERITY_BLOCKING = "blocking"
SEVERITY_WARNING = "warning"
SEVERITY_INFORMATIONAL = "informational"
SEVERITIES = (SEVERITY_BLOCKING, SEVERITY_WARNING, SEVERITY_INFORMATIONAL)


def _frame_meta(record: Dict[str, Any]) -> Dict[str, Any]:
    meta = record.get("four_hour_frame_meta")
    return meta if isinstance(meta, dict) else {}


def _trigger(record: Dict[str, Any]) -> Dict[str, Any]:
    trigger = record.get("four_hour_trigger")
    return trigger if isinstance(trigger, dict) else {}


def _distinct(records: List[Dict[str, Any]], field: str) -> List[str]:
    return sorted({
        str(r.get(field)) for r in records if r.get(field) is not None
    })


def _issue(
    code: str,
    severity: str,
    affected: int,
    detail: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    issue = {
        "code": code,
        "severity": severity,
        "affected_record_count": affected,
    }
    if detail:
        issue["detail"] = detail
    return issue


def build_quality_audit(
    records: List[Dict[str, Any]],
    *,
    campaign_runs: Optional[List[Dict[str, Any]]] = None,
    outcome_rows: Optional[List[Dict[str, Any]]] = None,
    strategy_discovery: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Classify every detectable evidence-quality issue. PURE — no writes,
    no repairs, no snapshot mutation."""
    issues: List[Dict[str, Any]] = []
    n = len(records)

    # ---- configuration ---------------------------------------------------- #
    if strategy_discovery is not None:
        if not strategy_discovery.get("db_configured", False):
            issues.append(_issue(
                "missing_db_pattern_row", SEVERITY_BLOCKING, 0,
                {"config_status": strategy_discovery.get("config_status")},
            ))

    # ---- 4H acquisition --------------------------------------------------- #
    unsupported = [
        r for r in records
        if _frame_meta(r).get("state") == "unsupported_provider"
    ]
    if unsupported:
        issues.append(_issue(
            "unsupported_provider_4h", SEVERITY_WARNING, len(unsupported)
        ))
    fetch_errors = [
        r for r in records if _frame_meta(r).get("state") == "fetch_error"
    ]
    if fetch_errors:
        issues.append(_issue(
            "four_hour_fetch_error", SEVERITY_WARNING, len(fetch_errors),
            {"reasons": sorted({
                str(_frame_meta(r).get("reason_code")) for r in fetch_errors
            })},
        ))
    frame_rejected = [
        r for r in records if _frame_meta(r).get("state") == "frame_rejected"
    ]
    if frame_rejected:
        reasons = sorted({
            str(_frame_meta(r).get("reason_code")) for r in frame_rejected
        })
        issues.append(_issue(
            "four_hour_frame_rejected", SEVERITY_WARNING,
            len(frame_rejected), {"reasons": reasons},
        ))
        duplicates = [
            r for r in frame_rejected
            if _frame_meta(r).get("reason_code") == "duplicate_bar_start"
        ]
        if duplicates:
            issues.append(_issue(
                "duplicate_4h_bars_rejected", SEVERITY_WARNING,
                len(duplicates),
            ))

    # ---- history sufficiency and staleness -------------------------------- #
    insufficient_daily = [
        r for r in records
        if r.get("readiness_status") is not None
        and r.get("readiness_status") != "ready"
    ]
    if insufficient_daily:
        issues.append(_issue(
            "insufficient_daily_history", SEVERITY_INFORMATIONAL,
            len(insufficient_daily),
        ))
    trigger_reasons = [
        (r, set(_trigger(r).get("reason_codes") or ())) for r in records
    ]
    insufficient_4h = [
        r for r, reasons in trigger_reasons
        if reasons & {"insufficient_4h_history",
                      "unconfirmed_4h_bar_completion"}
    ]
    if insufficient_4h:
        issues.append(_issue(
            "insufficient_four_hour_history", SEVERITY_INFORMATIONAL,
            len(insufficient_4h),
        ))
    stale_4h = [
        r for r, reasons in trigger_reasons
        if reasons & {"four_hour_trigger_stale", "unconfirmed_4h_freshness"}
    ]
    if stale_4h:
        issues.append(_issue(
            "stale_four_hour_data", SEVERITY_INFORMATIONAL, len(stale_4h),
        ))

    # ---- trigger evidence honesty ----------------------------------------- #
    missing_trigger_evidence = [
        r for r in records
        if (r.get("policy") or {}).get("setup_state") == "valid"
        and _frame_meta(r).get("state") == "built"
        and not _trigger(r)
    ]
    if missing_trigger_evidence:
        issues.append(_issue(
            "missing_trigger_evidence", SEVERITY_WARNING,
            len(missing_trigger_evidence),
        ))
    confirmed_without_price = [
        r for r in records
        if classify_trigger_state(r) == TRIGGER_CLASS_CONFIRMED
        and _trigger(r).get("trigger_price") is None
    ]
    if confirmed_without_price:
        issues.append(_issue(
            "confirmed_trigger_missing_price", SEVERITY_BLOCKING,
            len(confirmed_without_price),
        ))

    # ---- outcome coverage -------------------------------------------------- #
    missing_outcomes = [
        r for r in records if outcome_maturity(r) == "missing"
    ]
    if missing_outcomes:
        issues.append(_issue(
            "missing_outcomes", SEVERITY_WARNING, len(missing_outcomes),
        ))
    pending_outcomes = [
        r for r in records if outcome_maturity(r) == "pending"
    ]
    if pending_outcomes:
        issues.append(_issue(
            "pending_outcomes", SEVERITY_INFORMATIONAL,
            len(pending_outcomes),
        ))

    # ---- version / config consistency ------------------------------------- #
    for field, code in (
        ("strategy_version", "mixed_strategy_versions"),
        ("decision_policy_version", "mixed_policy_versions"),
        ("config_hash", "mixed_config_hashes"),
        ("daily_frame_contract_version", "mixed_daily_frame_contracts"),
    ):
        distinct = _distinct(records, field)
        if len(distinct) > 1:
            issues.append(_issue(
                code, SEVERITY_WARNING, n, {"values": distinct},
            ))
    contract_4h = sorted({
        str(_frame_meta(r).get("contract_version"))
        for r in records if _frame_meta(r).get("contract_version") is not None
    })
    if len(contract_4h) > 1:
        issues.append(_issue(
            "mixed_four_hour_frame_contracts", SEVERITY_WARNING, n,
            {"values": contract_4h},
        ))

    # ---- outcome-row level ------------------------------------------------- #
    if outcome_rows:
        provider_mismatch = [
            row for row in outcome_rows
            if (row.get("outcome") or {}).get("error_code")
            == "provider_mismatch"
        ]
        if provider_mismatch:
            issues.append(_issue(
                "provider_mismatch_outcomes", SEVERITY_WARNING,
                len(provider_mismatch),
            ))
        benchmark_missing = [
            row for row in outcome_rows
            if (row.get("outcome") or {}).get("outcome_status")
            in ("partial", "complete")
            and not (row.get("outcome") or {}).get("benchmark_returns")
        ]
        if benchmark_missing:
            issues.append(_issue(
                "benchmark_snapshot_missing", SEVERITY_INFORMATIONAL,
                len(benchmark_missing),
            ))

    # ---- campaign coverage ------------------------------------------------- #
    if campaign_runs:
        failed_runs = [
            run for run in campaign_runs
            if run.get("status") not in ("completed",)
        ]
        if failed_runs:
            issues.append(_issue(
                "campaign_partial_failures", SEVERITY_WARNING,
                len(failed_runs),
                {"run_ids": sorted(str(r.get("run_id")) for r in failed_runs)},
            ))
        gaps = []
        for run in campaign_runs:
            requested = run.get("requested_symbols") or []
            pair_count = run.get("pair_count")
            rejected = run.get("rejected_symbols") or {}
            rejected_count = sum(len(v) for v in rejected.values())
            if (
                run.get("status") == "completed"
                and isinstance(pair_count, int)
                and pair_count + rejected_count < len(requested)
            ):
                gaps.append(run)
        if gaps:
            issues.append(_issue(
                "campaign_symbol_count_gaps", SEVERITY_WARNING, len(gaps),
                {"run_ids": sorted(str(r.get("run_id")) for r in gaps)},
            ))

    counts = {severity: 0 for severity in SEVERITIES}
    for issue in issues:
        counts[issue["severity"]] += 1

    return {
        "contract_version": QUALITY_AUDIT_CONTRACT_VERSION,
        "severity_vocabulary": list(SEVERITIES),
        "evaluated_count": n,
        "issue_count": len(issues),
        "blocking_count": counts[SEVERITY_BLOCKING],
        "warning_count": counts[SEVERITY_WARNING],
        "informational_count": counts[SEVERITY_INFORMATIONAL],
        "issues": sorted(issues, key=lambda i: (i["severity"], i["code"])),
    }
