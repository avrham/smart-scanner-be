"""Wyckoff v2 decision policy — wyckoff_mtf.policy.v1 (Phase 9C1).

Authority order:
  A. Data readiness
  B. Daily setup and structure (including minimum price)
  C. HTF policy checks
  D. Phase and trigger eligibility
  E. Rollout gate (allow_enter)
  F. Ranking never decides

Verdicts: ENTER | WATCH | AVOID. Never REJECT.
Pure functions only — no I/O.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.workers.strategies.wyckoff_v2.constants import (
    DECISION_POLICY_VERSION,
    INVALIDATION_VERSION,
    RANKING_VERSION,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import (
    EventCandidate,
    EventDetectionResult,
    FourHourTriggerResult,
    HTFContextResult,
    InvalidationResult,
    PhaseClassificationResult,
    PolicyDecisionResult,
    RangeCandidate,
    RankingResult,
    ReadinessResult,
    StructureClassificationResult,
)


class PolicyError(ValueError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


PHASE_ORDINAL = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}


def _finite_positive(value: Optional[float]) -> bool:
    if value is None:
        return False
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f) and f > 0


def entry_reference_available(trigger: Optional[FourHourTriggerResult]) -> bool:
    """ENTER always requires a confirmed completed-4H trigger_price > 0."""
    if trigger is None or trigger.state != "confirmed":
        return False
    return _finite_positive(trigger.trigger_price)


def compute_invalidation(
    *,
    structure: Optional[StructureClassificationResult],
    selected_range: Optional[RangeCandidate],
    phase_result: Optional[PhaseClassificationResult],
    as_of: str,
    config: Optional[Dict[str, Any]] = None,
) -> InvalidationResult:
    """Daily-structure invalidation. No stop/target."""
    cfg = resolve_config(config)
    buffer_mult = float(cfg["event_invalidation_buffer_atr_multiple"])

    if (
        structure is None
        or selected_range is None
        or structure.state != "recognized"
        or structure.classification not in ("accumulation", "distribution")
    ):
        return InvalidationResult(
            invalidation_version=INVALIDATION_VERSION,
            rule_code=None,
            level=None,
            source_range_id=None if selected_range is None else selected_range.candidate_id,
            source_event_ids=(),
            zone=None,
            atr=None,
            buffer_atr_multiple=buffer_mult,
            timeframe="1d",
            as_of=as_of,
            reason="invalidation_not_computable",
            available=False,
        )

    atr = selected_range.atr
    if atr is None or atr <= 0 or not math.isfinite(float(atr)):
        return InvalidationResult(
            invalidation_version=INVALIDATION_VERSION,
            rule_code=None,
            level=None,
            source_range_id=selected_range.candidate_id,
            source_event_ids=(),
            zone=None,
            atr=atr,
            buffer_atr_multiple=buffer_mult,
            timeframe="1d",
            as_of=as_of,
            reason="invalidation_not_computable",
            available=False,
        )

    source_ids: Tuple[str, ...] = ()
    if phase_result is not None:
        ids: List[str] = []
        for cand in phase_result.candidates:
            if not cand.sequence_valid:
                continue
            if (
                phase_result.selected_phase is not None
                and PHASE_ORDINAL.get(cand.phase, 0)
                > PHASE_ORDINAL.get(phase_result.selected_phase, 0)
            ):
                continue
            ids.extend(cand.supporting_candidate_ids)
        source_ids = tuple(sorted(set(ids)))

    if structure.classification == "accumulation":
        rule = "daily_close_below_support_zone"
        zone = selected_range.support_zone.to_dict()
        level = selected_range.support_zone.lo - buffer_mult * float(atr)
    else:
        rule = "daily_close_above_resistance_zone"
        zone = selected_range.resistance_zone.to_dict()
        level = selected_range.resistance_zone.hi + buffer_mult * float(atr)

    available = _finite_positive(level)
    return InvalidationResult(
        invalidation_version=INVALIDATION_VERSION,
        rule_code=rule if available else None,
        level=float(level) if available else None,
        source_range_id=selected_range.candidate_id,
        source_event_ids=source_ids,
        zone=zone if available else None,
        atr=float(atr),
        buffer_atr_multiple=buffer_mult,
        timeframe="1d",
        as_of=as_of,
        reason=None if available else "invalidation_not_computable",
        available=available,
    )


def compute_ranking(
    *,
    structure: Optional[StructureClassificationResult],
    selected_range: Optional[RangeCandidate],
    phase_result: Optional[PhaseClassificationResult],
    htf: Optional[HTFContextResult],
    trigger: Optional[FourHourTriggerResult],
    config: Optional[Dict[str, Any]] = None,
) -> RankingResult:
    """Ranking-only components. Never a gate."""
    cfg = resolve_config(config)
    full_types = float(cfg["structure_quality_full_event_types"])

    htf_q: Optional[float] = None
    if structure is not None and structure.state == "recognized" and htf is not None:
        alignment = htf.htf_alignment
        classification = structure.classification
        if alignment == "unknown":
            htf_q = None
        elif alignment == "contradiction":
            htf_q = 0.0
        elif classification == "accumulation":
            if alignment == "aligned_up":
                htf_q = 1.0
            elif alignment == "aligned_down":
                htf_q = 0.0
            elif alignment == "mixed":
                htf_q = 0.5
            else:
                htf_q = None
        elif classification == "distribution":
            if alignment == "aligned_down":
                htf_q = 1.0
            elif alignment == "aligned_up":
                htf_q = 0.0
            elif alignment == "mixed":
                htf_q = 0.5
            else:
                htf_q = None

    range_q = None if selected_range is None else selected_range.range_quality

    structure_q: Optional[float] = None
    if structure is not None and structure.state == "recognized":
        if structure.classification == "accumulation":
            n = structure.accumulation_confirmed_type_count
        else:
            n = structure.distribution_confirmed_type_count
        structure_q = min(1.0, float(n) / full_types)

    phase_q: Optional[float] = None
    if structure is None or structure.state != "recognized":
        phase_q = None
    elif phase_result is None or phase_result.selected_phase is None:
        phase_q = 0.0
    else:
        phase_q = {
            "A": 0.2,
            "B": 0.4,
            "C": 0.6,
            "D": 0.8,
            "E": 1.0,
        }.get(phase_result.selected_phase, 0.0)

    trigger_q: Optional[float] = None
    if trigger is None or not trigger.enabled:
        trigger_q = None
    elif trigger.state == "confirmed":
        trigger_q = 1.0
    elif trigger.state in ("missing", "contradicted"):
        trigger_q = 0.0
    else:
        trigger_q = None

    vol_q = None if selected_range is None else selected_range.volume_coverage

    components = {
        "htf_alignment_quality": htf_q,
        "range_quality": range_q,
        "structure_evidence_quality": structure_q,
        "phase_completeness": phase_q,
        "trigger_quality": trigger_q,
        "volume_coverage_quality": vol_q,
    }
    vals = list(components.values())
    if any(v is None for v in vals):
        score = None
    else:
        score = float(sum(float(v) for v in vals) / len(vals))  # type: ignore[arg-type]

    return RankingResult(
        ranking_version=RANKING_VERSION,
        components=components,
        ranking_score=score,
    )


def _side_from_structure(structure: Optional[StructureClassificationResult]) -> str:
    if structure is None or structure.state != "recognized":
        return "UNKNOWN"
    if structure.classification == "accumulation":
        return "LONG"
    if structure.classification == "distribution":
        return "SHORT"
    return "UNKNOWN"


def _usable_events(events: Sequence[EventCandidate]) -> List[EventCandidate]:
    return [
        e
        for e in events
        if e.usable_for_structure
        and e.status == "confirmed"
        and "supporting_candidate_truncated" not in e.reason_codes
    ]


def evaluate_policy(
    *,
    readiness: Optional[ReadinessResult],
    selected_range: Optional[RangeCandidate],
    structure: Optional[StructureClassificationResult],
    phase_result: Optional[PhaseClassificationResult],
    htf: Optional[HTFContextResult],
    trigger: Optional[FourHourTriggerResult],
    invalidation: Optional[InvalidationResult],
    event_result: Optional[EventDetectionResult] = None,
    last_close: Optional[float] = None,
    config: Optional[Dict[str, Any]] = None,
) -> PolicyDecisionResult:
    """Evaluate wyckoff_mtf.policy.v1. Ranking is never consulted."""
    cfg = resolve_config(config)
    allow_enter = bool(cfg["allow_enter"])
    require_trigger = bool(cfg["require_4h_trigger_for_enter"])
    avoid_htf = bool(cfg["avoid_on_htf_contradiction"])
    eligible_phases = set(cfg["enter_eligible_phases"])
    min_price = float(cfg["min_price"])

    gates: Dict[str, bool] = {}
    waiting: List[str] = []

    side = _side_from_structure(structure)
    selected_phase = None if phase_result is None else phase_result.selected_phase
    selected_phase_status = (
        None if phase_result is None else phase_result.selected_phase_status
    )
    trigger_state = "unknown" if trigger is None else trigger.state
    trigger_confirmed = bool(trigger is not None and trigger.state == "confirmed")
    inv_available = bool(
        invalidation is not None
        and invalidation.available
        and _finite_positive(invalidation.level)
    )

    def _avoid(reason: str, *, setup_state: str) -> PolicyDecisionResult:
        return PolicyDecisionResult(
            decision_policy_version=DECISION_POLICY_VERSION,
            verdict="AVOID",
            side=side,
            setup_state=setup_state,
            trigger_state=trigger_state,
            reason_code=reason,
            blocking_reasons=(reason,),
            waiting_reasons=(),
            required_gate_results=dict(gates),
            allow_enter=allow_enter,
            enter_eligible_without_rollout_gate=False,
            selected_phase=selected_phase,
            selected_phase_status=selected_phase_status,
            invalidation_available=inv_available,
            trigger_required=require_trigger,
            trigger_confirmed=trigger_confirmed,
        )

    # A. Readiness
    ready = bool(readiness is not None and readiness.ready)
    gates["readiness_ready"] = ready
    if not ready:
        return _avoid(
            "readiness_not_ready" if readiness is not None else "readiness_missing",
            setup_state="unknown",
        )

    if readiness is not None and "unconfirmed_bar_completion" in readiness.reason_codes:
        gates["daily_bar_completed"] = False
        return _avoid("unconfirmed_completed_daily_bar", setup_state="unknown")
    gates["daily_bar_completed"] = True

    # Minimum-price hard filter (latest completed daily close).
    if last_close is None or not math.isfinite(float(last_close)):
        gates["minimum_price"] = False
        return _avoid("price_below_minimum", setup_state="invalid")
    if float(last_close) < min_price:
        gates["minimum_price"] = False
        return _avoid("price_below_minimum", setup_state="invalid")
    gates["minimum_price"] = True

    # B. Setup / structure
    gates["valid_selected_range"] = selected_range is not None and selected_range.valid
    if selected_range is None or not selected_range.valid:
        return _avoid("no_valid_selected_range", setup_state="invalid")

    if structure is None:
        return _avoid("structure_unavailable", setup_state="unknown")
    if structure.state == "ambiguous":
        gates["structure_recognized"] = False
        return _avoid("ambiguous_structure", setup_state="invalid")
    if structure.state != "recognized" or structure.classification == "unknown":
        gates["structure_recognized"] = False
        return _avoid("unknown_structure", setup_state="unknown")
    gates["structure_recognized"] = True

    if side == "UNKNOWN":
        return _avoid("structure_side_unavailable", setup_state="invalid")

    if event_result is not None:
        _ = _usable_events(event_result.candidates)

    gates["invalidation_available"] = inv_available
    if not inv_available:
        return _avoid("invalidation_unavailable", setup_state="invalid")

    # C. HTF policy
    if htf is not None:
        if avoid_htf and htf.htf_alignment == "contradiction":
            gates["htf_no_contradiction"] = False
            return _avoid("htf_contradiction", setup_state="invalid")
        gates["htf_no_contradiction"] = True
        if structure.classification == "accumulation" and htf.htf_alignment == "aligned_down":
            gates["htf_direction_compatible"] = False
            return _avoid("htf_direction_conflict", setup_state="invalid")
        if structure.classification == "distribution" and htf.htf_alignment == "aligned_up":
            gates["htf_direction_compatible"] = False
            return _avoid("htf_direction_conflict", setup_state="invalid")
        gates["htf_direction_compatible"] = True
    else:
        gates["htf_no_contradiction"] = True
        gates["htf_direction_compatible"] = True

    setup_state = "valid"

    # D. Phase eligibility
    phase_ok = (
        selected_phase is not None
        and selected_phase in eligible_phases
        and phase_result is not None
        and phase_result.phase_state != "UNKNOWN_PHASE"
    )
    gates["phase_enter_eligible"] = bool(phase_ok)

    if selected_phase is None or (
        phase_result is not None and phase_result.phase_state == "UNKNOWN_PHASE"
    ):
        waiting.append("unknown_phase")
    elif selected_phase not in eligible_phases:
        waiting.append("phase_not_enter_eligible")

    # Trigger confirmation gate (config-dependent).
    trigger_confirmation_ok = True
    if require_trigger:
        if trigger is None or not trigger.enabled:
            trigger_confirmation_ok = False
            waiting.append("four_hour_data_missing")
        elif trigger.state == "unknown":
            trigger_confirmation_ok = False
            if "four_hour_data_missing" in trigger.reason_codes:
                waiting.append("four_hour_data_missing")
            else:
                waiting.append("four_hour_trigger_unknown")
        elif trigger.state == "missing":
            trigger_confirmation_ok = False
            waiting.append("four_hour_trigger_missing")
        elif trigger.state == "contradicted":
            trigger_confirmation_ok = False
            waiting.append("four_hour_trigger_contradicted")
        elif trigger.state == "confirmed":
            trigger_confirmation_ok = True
    gates["trigger_confirmed_or_not_required"] = bool(trigger_confirmation_ok)

    # Entry reference is ALWAYS required for ENTER (4H trigger_price only).
    entry_ok = entry_reference_available(trigger)
    gates["entry_reference_available"] = entry_ok
    if not entry_ok:
        waiting.append("entry_reference_unavailable")

    enter_structurally = bool(
        phase_ok
        and trigger_confirmation_ok
        and entry_ok
        and inv_available
        and ready
        and selected_range is not None
        and structure.state == "recognized"
        and side in ("LONG", "SHORT")
    )

    # E. Rollout gate — applied after enter_eligible_without_rollout_gate is known.
    gates["allow_enter"] = allow_enter
    if enter_structurally and not allow_enter:
        waiting.append("enter_disabled_shadow_only")

    if enter_structurally and allow_enter:
        return PolicyDecisionResult(
            decision_policy_version=DECISION_POLICY_VERSION,
            verdict="ENTER",
            side=side,
            setup_state=setup_state,
            trigger_state=trigger_state,
            reason_code="enter_all_gates_passed",
            blocking_reasons=(),
            waiting_reasons=(),
            required_gate_results=dict(gates),
            allow_enter=allow_enter,
            enter_eligible_without_rollout_gate=True,
            selected_phase=selected_phase,
            selected_phase_status=selected_phase_status,
            invalidation_available=inv_available,
            trigger_required=require_trigger,
            trigger_confirmed=trigger_confirmed,
        )

    return PolicyDecisionResult(
        decision_policy_version=DECISION_POLICY_VERSION,
        verdict="WATCH",
        side=side,
        setup_state=setup_state,
        trigger_state=trigger_state,
        reason_code="watch_setup_valid",
        blocking_reasons=(),
        waiting_reasons=tuple(sorted(set(waiting))),
        required_gate_results=dict(gates),
        allow_enter=allow_enter,
        enter_eligible_without_rollout_gate=enter_structurally,
        selected_phase=selected_phase,
        selected_phase_status=selected_phase_status,
        invalidation_available=inv_available,
        trigger_required=require_trigger,
        trigger_confirmed=trigger_confirmed,
    )
