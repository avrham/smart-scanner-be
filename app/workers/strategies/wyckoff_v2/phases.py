"""Structure and cumulative Phase A–E classification — wyckoff_phases.v1 (Phase 9B).

Uses only usable_for_structure=true confirmed event candidates. Confidence is
ranking evidence only and never authorizes a phase. HTF context may be
recorded as contradiction evidence but never forces classification.

Phases are cumulative: lower phases coexist with higher phases; the highest
supported phase is selected. No AMBIGUOUS_PHASE — ambiguous/unknown structure
yields UNKNOWN_PHASE.

Family isolation is strict: accumulation prerequisites never consume
distribution candidates (and vice versa), including shared codes AR/ST.

Pure functions only — no I/O.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from app.workers.provenance import _sha256, canonical_json
from app.workers.strategies.wyckoff_v2.constants import (
    GATE_PHASE_E_HOLD_ABOVE_RESISTANCE,
    GATE_PHASE_E_HOLD_BELOW_SUPPORT,
    PHASE_CANDIDATE_VERSION,
    PHASE_CLASSIFICATION_VERSION,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import (
    EventCandidate,
    EventDetectionResult,
    HTFContextResult,
    PhaseCandidate,
    PhaseClassificationResult,
    RangeCandidate,
    StructureClassificationResult,
)


class PhaseClassificationError(ValueError):
    """Deterministic rejection of malformed phase-classification input."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


_ACC_SIGNATURE = frozenset({"SC", "Spring", "SOS"})
_DIST_SIGNATURE = frozenset({"BC", "UT", "UTAD", "SOW"})

_PHASE_ORDINAL = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}

# Deterministic supporting-sequence selection:
# among usable family-scoped candidates for a code after a prerequisite index,
# choose min(index, candidate_id). Alternatives (Spring/Test, SOS/LPS, UT/UTAD)
# use the same key across the union of allowed codes.


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return pd.Timestamp(value).date()


def _truncate(daily: pd.DataFrame, as_of_date: Any) -> pd.DataFrame:
    pinned = _as_date(as_of_date)
    return daily.loc[daily["date"].map(_as_date) <= pinned].copy().reset_index(drop=True)


def _usable(events: Sequence[EventCandidate], family: str) -> List[EventCandidate]:
    return [
        e
        for e in events
        if e.family == family and e.usable_for_structure and e.status == "confirmed"
    ]


def _phase_candidate_id(
    *,
    structure: str,
    phase: str,
    as_of_date: str,
    range_candidate_id: str,
    supporting: Sequence[str],
) -> str:
    payload = {
        "phase_candidate_version": PHASE_CANDIDATE_VERSION,
        "structure": structure,
        "phase": phase,
        "as_of_date": as_of_date,
        "range_candidate_id": range_candidate_id,
        "supporting_candidate_ids": sorted(str(x) for x in supporting),
    }
    return _sha256(canonical_json(payload))


def _min_confidence(components: Dict[str, Optional[float]]) -> Optional[float]:
    vals = list(components.values())
    if any(v is None for v in vals):
        return None
    return float(min(float(v) for v in vals))  # type: ignore[arg-type]


def classify_structure(
    event_result: EventDetectionResult,
    *,
    as_of_date: Any,
    config: Optional[Dict[str, Any]] = None,
    htf_context: Optional[HTFContextResult] = None,
) -> StructureClassificationResult:
    """Classify accumulation / distribution / unknown from usable events."""
    cfg = resolve_config(config)
    pinned = _as_date(as_of_date).isoformat()
    min_types = int(cfg["min_structure_confirmed_event_types"])

    events = event_result.candidates
    acc = _usable(events, "accumulation")
    dist = _usable(events, "distribution")

    acc_types = tuple(sorted({e.event_code for e in acc}))
    dist_types = tuple(sorted({e.event_code for e in dist}))
    acc_ids = tuple(e.candidate_id for e in sorted(acc, key=lambda x: (x.date, x.index, x.candidate_id)))
    dist_ids = tuple(
        e.candidate_id for e in sorted(dist, key=lambda x: (x.date, x.index, x.candidate_id))
    )

    acc_sigs = tuple(sorted(t for t in acc_types if t in _ACC_SIGNATURE))
    dist_sigs = tuple(sorted(t for t in dist_types if t in _DIST_SIGNATURE))

    acc_qualifies = len(acc_types) >= min_types and len(acc_sigs) >= 1
    dist_qualifies = len(dist_types) >= min_types and len(dist_sigs) >= 1
    acc_incomplete = bool(acc_types) and not acc_qualifies
    dist_incomplete = bool(dist_types) and not dist_qualifies

    reason_codes: List[str] = []
    contradiction_codes: List[str] = []

    if htf_context is not None and htf_context.htf_alignment == "contradiction":
        contradiction_codes.append("htf_alignment_contradiction")
        reason_codes.append("htf_context_recorded_not_forcing")

    if acc_qualifies and not dist_qualifies:
        classification = "accumulation"
        state = "recognized"
        if dist_incomplete:
            contradiction_codes.append("distribution_incomplete_evidence")
            reason_codes.append("opposite_structure_incomplete_evidence")
    elif dist_qualifies and not acc_qualifies:
        classification = "distribution"
        state = "recognized"
        if acc_incomplete:
            contradiction_codes.append("accumulation_incomplete_evidence")
            reason_codes.append("opposite_structure_incomplete_evidence")
    elif acc_qualifies and dist_qualifies:
        classification = "unknown"
        state = "ambiguous"
        reason_codes.append("both_structures_qualify")
        contradiction_codes.append("accumulation_distribution_both_qualify")
    else:
        classification = "unknown"
        state = "unknown"
        if (acc_types or dist_types) and not (acc_qualifies or dist_qualifies):
            reason_codes.append("contradictory_incomplete_structure_evidence")
        else:
            reason_codes.append("insufficient_confirmed_structure_events")

    return StructureClassificationResult(
        phase_classification_version=PHASE_CLASSIFICATION_VERSION,
        as_of_date=pinned,
        range_candidate_id=event_result.range_candidate_id,
        classification=classification,
        state=state,
        accumulation_event_types=acc_types,
        distribution_event_types=dist_types,
        accumulation_candidate_ids=acc_ids,
        distribution_candidate_ids=dist_ids,
        accumulation_confirmed_type_count=len(acc_types),
        distribution_confirmed_type_count=len(dist_types),
        accumulation_signature_events=acc_sigs,
        distribution_signature_events=dist_sigs,
        contradiction_codes=tuple(sorted(set(contradiction_codes))),
        reason_codes=tuple(sorted(set(reason_codes))),
    )


def _event_quality(e: EventCandidate) -> Optional[float]:
    return e.confidence


def _build_phase(
    *,
    structure: str,
    phase: str,
    as_of: str,
    range_id: str,
    required: Sequence[str],
    supporting: Sequence[EventCandidate],
    missing: Sequence[str],
    sequence_valid: bool,
    reason_codes: Sequence[str],
    contradicting: Sequence[str] = (),
    required_gate_codes: Sequence[str] = (),
    passed_gate_codes: Sequence[str] = (),
    missing_gate_codes: Sequence[str] = (),
    failed_gate_codes: Sequence[str] = (),
) -> PhaseCandidate:
    supporting_ids = tuple(e.candidate_id for e in supporting)
    # Family isolation assertion: all supporting events share structure family.
    for e in supporting:
        if e.family != structure:
            raise PhaseClassificationError(
                "cross_family_support",
                f"supporting event {e.candidate_id} family {e.family} != {structure}",
            )

    qualities = [_event_quality(e) for e in supporting]
    if not supporting:
        supporting_event_quality: Optional[float] = None
    elif any(q is None for q in qualities):
        supporting_event_quality = None
    else:
        supporting_event_quality = float(sum(qualities) / len(qualities))  # type: ignore[arg-type]

    if missing:
        seq_completeness = float(max(0.0, 1.0 - len(missing) / max(len(required), 1)))
    else:
        seq_completeness = 1.0 if sequence_valid else 0.0

    contradiction_quality = 1.0 if not contradicting else 0.0
    components = {
        "sequence_completeness": seq_completeness,
        "supporting_event_quality": supporting_event_quality,
        "contradiction_quality": contradiction_quality,
    }

    gates_ok = (
        not missing_gate_codes
        and not failed_gate_codes
        and set(passed_gate_codes) >= set(required_gate_codes)
    )
    events_ok = sequence_valid and not missing
    status = "confirmed" if events_ok and gates_ok else (
        "unknown" if not sequence_valid else "candidate"
    )

    return PhaseCandidate(
        phase_candidate_version=PHASE_CANDIDATE_VERSION,
        candidate_id=_phase_candidate_id(
            structure=structure,
            phase=phase,
            as_of_date=as_of,
            range_candidate_id=range_id,
            supporting=supporting_ids,
        ),
        structure=structure,
        phase=phase,
        ordinal=_PHASE_ORDINAL[phase],
        status=status,
        as_of_date=as_of,
        required_event_codes=tuple(required),
        supporting_candidate_ids=supporting_ids,
        contradicting_candidate_ids=tuple(contradicting),
        missing_event_codes=tuple(missing),
        required_gate_codes=tuple(required_gate_codes),
        passed_gate_codes=tuple(passed_gate_codes),
        missing_gate_codes=tuple(missing_gate_codes),
        failed_gate_codes=tuple(failed_gate_codes),
        sequence_valid=bool(events_ok and gates_ok),
        confidence_components=components,
        confidence=_min_confidence(components),
        reason_codes=tuple(reason_codes),
    )


def _first_after(
    events: Sequence[EventCandidate],
    code: str,
    after_index: int,
) -> Optional[EventCandidate]:
    """Strict chronology: supporting event index must be > prerequisite index."""
    matches = [e for e in events if e.event_code == code and e.index > after_index]
    if not matches:
        return None
    return min(matches, key=lambda e: (e.index, e.candidate_id))


def _first_after_any(
    events: Sequence[EventCandidate],
    codes: Sequence[str],
    after_index: int,
) -> Optional[EventCandidate]:
    matches = [
        e for e in events if e.event_code in codes and e.index > after_index
    ]
    if not matches:
        return None
    return min(matches, key=lambda e: (e.index, e.candidate_id))


def _eval_hold_above(
    frame: pd.DataFrame,
    selected_range: RangeCandidate,
    *,
    after_index: int,
    hold_n: int,
) -> Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]:
    gate = GATE_PHASE_E_HOLD_ABOVE_RESISTANCE
    required = (gate,)
    # Post-range completed bars only, and strictly after Phase D event.
    start = max(selected_range.end_index + 1, after_index + 1)
    post = frame.iloc[start:]
    if len(post) == 0:
        return required, (), (gate,), ()
    closes = post["close"].astype(float)
    hold_count = int((closes > selected_range.resistance_zone.hi).sum())
    violated = bool((closes < selected_range.resistance_zone.lo).any())
    if violated:
        return required, (), (), (gate,)
    if hold_count < hold_n:
        return required, (), (gate,), ()
    return required, (gate,), (), ()


def _eval_hold_below(
    frame: pd.DataFrame,
    selected_range: RangeCandidate,
    *,
    after_index: int,
    hold_n: int,
) -> Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]:
    gate = GATE_PHASE_E_HOLD_BELOW_SUPPORT
    required = (gate,)
    start = max(selected_range.end_index + 1, after_index + 1)
    post = frame.iloc[start:]
    if len(post) == 0:
        return required, (), (gate,), ()
    closes = post["close"].astype(float)
    hold_count = int((closes < selected_range.support_zone.lo).sum())
    violated = bool((closes > selected_range.support_zone.hi).any())
    if violated:
        return required, (), (), (gate,)
    if hold_count < hold_n:
        return required, (), (gate,), ()
    return required, (gate,), (), ()


def _accumulation_phases(
    events: Sequence[EventCandidate],
    selected_range: RangeCandidate,
    frame: pd.DataFrame,
    as_of: str,
    cfg: Dict[str, Any],
) -> List[PhaseCandidate]:
    usable = _usable(events, "accumulation")
    out: List[PhaseCandidate] = []
    range_id = selected_range.candidate_id

    scs = [e for e in usable if e.event_code == "SC"]
    if not scs:
        return out
    sc = min(scs, key=lambda e: (e.index, e.candidate_id))
    ar = _first_after(usable, "AR", sc.index)
    ps = next(
        (e for e in usable if e.event_code == "PS" and e.index < sc.index),
        None,
    )

    if ar is None:
        return out
    supporting_a = [sc, ar] + ([ps] if ps else [])
    out.append(
        _build_phase(
            structure="accumulation",
            phase="A",
            as_of=as_of,
            range_id=range_id,
            required=("SC", "AR"),
            supporting=supporting_a,
            missing=(),
            sequence_valid=True,
            reason_codes=(),
        )
    )

    st = _first_after(usable, "ST", ar.index)
    min_bars = int(cfg["phase_b_min_range_bars"])
    if st is None or selected_range.bar_count < min_bars:
        return out
    out.append(
        _build_phase(
            structure="accumulation",
            phase="B",
            as_of=as_of,
            range_id=range_id,
            required=("SC", "AR", "ST"),
            supporting=[sc, ar, st],
            missing=(),
            sequence_valid=True,
            reason_codes=(),
        )
    )

    phase_c_event = _first_after_any(usable, ("Spring", "Test"), st.index)
    if phase_c_event is None:
        return out
    out.append(
        _build_phase(
            structure="accumulation",
            phase="C",
            as_of=as_of,
            range_id=range_id,
            required=("SC", "AR", "ST", phase_c_event.event_code),
            supporting=[sc, ar, st, phase_c_event],
            missing=(),
            sequence_valid=True,
            reason_codes=(),
        )
    )

    phase_d_event = _first_after_any(
        usable, ("SOS", "LPS"), phase_c_event.index
    )
    if phase_d_event is None:
        return out
    out.append(
        _build_phase(
            structure="accumulation",
            phase="D",
            as_of=as_of,
            range_id=range_id,
            required=(
                "SC",
                "AR",
                "ST",
                phase_c_event.event_code,
                phase_d_event.event_code,
            ),
            supporting=[sc, ar, st, phase_c_event, phase_d_event],
            missing=(),
            sequence_valid=True,
            reason_codes=(),
        )
    )

    hold_n = int(cfg["phase_e_hold_bars"])
    req_g, pass_g, miss_g, fail_g = _eval_hold_above(
        frame,
        selected_range,
        after_index=phase_d_event.index,
        hold_n=hold_n,
    )
    out.append(
        _build_phase(
            structure="accumulation",
            phase="E",
            as_of=as_of,
            range_id=range_id,
            required=(
                "SC",
                "AR",
                "ST",
                phase_c_event.event_code,
                phase_d_event.event_code,
            ),
            supporting=[sc, ar, st, phase_c_event, phase_d_event],
            missing=(),
            sequence_valid=True,
            reason_codes=(),
            required_gate_codes=req_g,
            passed_gate_codes=pass_g,
            missing_gate_codes=miss_g,
            failed_gate_codes=fail_g,
        )
    )
    return out


def _distribution_phases(
    events: Sequence[EventCandidate],
    selected_range: RangeCandidate,
    frame: pd.DataFrame,
    as_of: str,
    cfg: Dict[str, Any],
) -> List[PhaseCandidate]:
    usable = _usable(events, "distribution")
    out: List[PhaseCandidate] = []
    range_id = selected_range.candidate_id

    bcs = [e for e in usable if e.event_code == "BC"]
    if not bcs:
        return out
    bc = min(bcs, key=lambda e: (e.index, e.candidate_id))
    ar = _first_after(usable, "AR", bc.index)
    psy = next(
        (e for e in usable if e.event_code == "PSY" and e.index < bc.index),
        None,
    )

    if ar is None:
        return out
    supporting_a = [bc, ar] + ([psy] if psy else [])
    out.append(
        _build_phase(
            structure="distribution",
            phase="A",
            as_of=as_of,
            range_id=range_id,
            required=("BC", "AR"),
            supporting=supporting_a,
            missing=(),
            sequence_valid=True,
            reason_codes=(),
        )
    )

    st = _first_after(usable, "ST", ar.index)
    min_bars = int(cfg["phase_b_min_range_bars"])
    if st is None or selected_range.bar_count < min_bars:
        return out
    out.append(
        _build_phase(
            structure="distribution",
            phase="B",
            as_of=as_of,
            range_id=range_id,
            required=("BC", "AR", "ST"),
            supporting=[bc, ar, st],
            missing=(),
            sequence_valid=True,
            reason_codes=(),
        )
    )

    phase_c_event = _first_after_any(usable, ("UT", "UTAD"), st.index)
    if phase_c_event is None:
        return out
    out.append(
        _build_phase(
            structure="distribution",
            phase="C",
            as_of=as_of,
            range_id=range_id,
            required=("BC", "AR", "ST", phase_c_event.event_code),
            supporting=[bc, ar, st, phase_c_event],
            missing=(),
            sequence_valid=True,
            reason_codes=(),
        )
    )

    phase_d_event = _first_after_any(
        usable, ("SOW", "LPSY"), phase_c_event.index
    )
    if phase_d_event is None:
        return out
    out.append(
        _build_phase(
            structure="distribution",
            phase="D",
            as_of=as_of,
            range_id=range_id,
            required=(
                "BC",
                "AR",
                "ST",
                phase_c_event.event_code,
                phase_d_event.event_code,
            ),
            supporting=[bc, ar, st, phase_c_event, phase_d_event],
            missing=(),
            sequence_valid=True,
            reason_codes=(),
        )
    )

    hold_n = int(cfg["phase_e_hold_bars"])
    req_g, pass_g, miss_g, fail_g = _eval_hold_below(
        frame,
        selected_range,
        after_index=phase_d_event.index,
        hold_n=hold_n,
    )
    out.append(
        _build_phase(
            structure="distribution",
            phase="E",
            as_of=as_of,
            range_id=range_id,
            required=(
                "BC",
                "AR",
                "ST",
                phase_c_event.event_code,
                phase_d_event.event_code,
            ),
            supporting=[bc, ar, st, phase_c_event, phase_d_event],
            missing=(),
            sequence_valid=True,
            reason_codes=(),
            required_gate_codes=req_g,
            passed_gate_codes=pass_g,
            missing_gate_codes=miss_g,
            failed_gate_codes=fail_g,
        )
    )
    return out


def classify_phases(
    daily: pd.DataFrame,
    selected_range: RangeCandidate,
    event_result: EventDetectionResult,
    *,
    as_of_date: Any,
    config: Optional[Dict[str, Any]] = None,
    htf_context: Optional[HTFContextResult] = None,
    structure: Optional[StructureClassificationResult] = None,
) -> PhaseClassificationResult:
    """Build cumulative Phase A–E candidates and select the highest supported."""
    cfg = resolve_config(config)
    pinned = _as_date(as_of_date)
    as_of_s = pinned.isoformat()
    frame = _truncate(daily, pinned)

    if structure is None:
        structure = classify_structure(
            event_result,
            as_of_date=pinned,
            config=cfg,
            htf_context=htf_context,
        )

    reason_codes: List[str] = list(structure.reason_codes)
    config_used = {
        "phase_b_min_range_bars": cfg["phase_b_min_range_bars"],
        "phase_e_hold_bars": cfg["phase_e_hold_bars"],
        "min_structure_confirmed_event_types": cfg[
            "min_structure_confirmed_event_types"
        ],
    }

    if structure.state == "ambiguous":
        return PhaseClassificationResult(
            phase_classification_version=PHASE_CLASSIFICATION_VERSION,
            as_of_date=as_of_s,
            structure_classification=structure,
            selected_phase=None,
            selected_phase_status=None,
            phase_state="UNKNOWN_PHASE",
            candidates=(),
            reason_codes=tuple(
                sorted(set(reason_codes + ["ambiguous_structure_blocks_phase"]))
            ),
            config_used=config_used,
        )

    if structure.state == "unknown" or structure.classification == "unknown":
        return PhaseClassificationResult(
            phase_classification_version=PHASE_CLASSIFICATION_VERSION,
            as_of_date=as_of_s,
            structure_classification=structure,
            selected_phase=None,
            selected_phase_status=None,
            phase_state="UNKNOWN_PHASE",
            candidates=(),
            reason_codes=tuple(
                sorted(set(reason_codes + ["unknown_structure_blocks_phase"]))
            ),
            config_used=config_used,
        )

    candidates: List[PhaseCandidate] = []
    if structure.classification == "accumulation":
        candidates = _accumulation_phases(
            event_result.candidates, selected_range, frame, as_of_s, cfg
        )
    elif structure.classification == "distribution":
        candidates = _distribution_phases(
            event_result.candidates, selected_range, frame, as_of_s, cfg
        )

    # Confidence never gates selection — only sequence_valid (events + gates).
    valid = [c for c in candidates if c.sequence_valid]
    if not valid:
        return PhaseClassificationResult(
            phase_classification_version=PHASE_CLASSIFICATION_VERSION,
            as_of_date=as_of_s,
            structure_classification=structure,
            selected_phase=None,
            selected_phase_status=None,
            phase_state="UNKNOWN_PHASE",
            candidates=tuple(candidates),
            reason_codes=tuple(
                sorted(set(reason_codes + ["no_valid_phase_candidate"]))
            ),
            config_used=config_used,
        )

    selected = max(valid, key=lambda c: (c.ordinal, c.candidate_id))
    return PhaseClassificationResult(
        phase_classification_version=PHASE_CLASSIFICATION_VERSION,
        as_of_date=as_of_s,
        structure_classification=structure,
        selected_phase=selected.phase,
        selected_phase_status=selected.status,
        phase_state=f"PHASE_{selected.phase}",
        candidates=tuple(candidates),
        reason_codes=tuple(sorted(set(reason_codes))),
        config_used=config_used,
    )
