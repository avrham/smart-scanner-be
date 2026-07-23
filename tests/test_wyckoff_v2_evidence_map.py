"""Phase 9C1: evidence.v1 mapping and bounding tests."""

from __future__ import annotations

import json
from typing import List, Tuple

import pytest

from app.workers.provenance import (
    MAX_EVIDENCE_BYTES,
    build_evidence_snapshot,
    canonical_json,
)
from app.workers.strategies.wyckoff_v2.constants import resolve_config
from app.workers.strategies.wyckoff_v2.evidence_map import (
    EvidenceMappingError,
    bound_event_candidates,
    build_evidence_bundle,
    decision_relevant_candidate_ids,
)
from app.workers.strategies.wyckoff_v2.models import (
    EventCandidate,
    EventDetectionResult,
    FourHourTriggerResult,
    HTFContextResult,
    InvalidationResult,
    PhaseCandidate,
    PhaseClassificationResult,
    PolicyDecisionResult,
    PriceZone,
    RangeCandidate,
    RankingResult,
    ReadinessResult,
    StructureClassificationResult,
)


def _ec(
    *,
    family: str,
    code: str,
    index: int,
    date: str,
    status: str = "confirmed",
    usable: bool = True,
    supporting: Tuple[str, ...] = (),
    reasons: Tuple[str, ...] = (),
) -> EventCandidate:
    cid = f"{family}_{code}_{index}"
    return EventCandidate(
        event_candidate_version="wyckoff_events.v1",
        candidate_id=cid,
        range_candidate_id="range_1",
        family=family,
        event_code=code,
        event_label=code.lower(),
        date=date,
        index=index,
        timeframe="1d",
        as_of_date="2024-06-28",
        price=100.0,
        level=100.0,
        direction="down",
        status=status,
        confirmation_status=status,
        confirmation_end_date=None,
        range_relationship="test",
        effort_result={},
        required_gate_results={},
        confidence_components={},
        confidence=0.5,
        supporting_candidate_ids=supporting,
        contradicting_candidate_ids=(),
        reason_codes=reasons,
        usable_for_structure=usable,
        metadata={},
    )


def _ready() -> ReadinessResult:
    return ReadinessResult(
        readiness_version="wyckoff_readiness.v1",
        ready=True,
        status="ready",
        reason_codes=(),
        latest_bar_completion={"state": "completed"},
        evaluation_time_utc="2024-06-28T20:00:00Z",
        market_data_as_of="2024-06-28",
        desired_history_bars=600,
        requested_history_bars=600,
        available_input_bars=600,
        available_completed_bars=600,
        history_depth_capped=False,
        history_depth_complete=True,
        required_monthly_periods=24,
        available_completed_monthly_periods=30,
        required_weekly_periods=26,
        available_completed_weekly_periods=40,
        required_daily_structure_bars=120,
        usable_volume_bars=600,
        required_volume_bars=100,
        volume_coverage=1.0,
        excluded_partial_daily_bar_date=None,
        missing_fields=(),
    )


def _range() -> RangeCandidate:
    s = PriceZone(lo=98.0, hi=100.0)
    r = PriceZone(lo=110.0, hi=112.0)
    return RangeCandidate(
        range_candidate_version="wyckoff_range.v1",
        candidate_id="range_1",
        as_of_date="2024-06-28",
        start_date="2024-01-02",
        end_date="2024-03-01",
        start_index=10,
        end_index=49,
        post_range_bar_count=5,
        bar_count=40,
        support_zone=s,
        resistance_zone=r,
        support=s.midpoint,
        resistance=r.midpoint,
        midpoint=105.0,
        width=14.0,
        atr=2.0,
        width_atr_multiple=7.0,
        support_interactions=(),
        resistance_interactions=(),
        support_touch_cluster_count=2,
        resistance_touch_cluster_count=2,
        containment_fraction=0.9,
        breakout_contamination_fraction=0.05,
        volume_coverage=1.0,
        quality_components={},
        range_quality=0.8,
        valid=True,
        rejection_reasons=(),
    )


def _structure(ids: Tuple[str, ...]) -> StructureClassificationResult:
    return StructureClassificationResult(
        phase_classification_version="wyckoff_phases.v1",
        as_of_date="2024-06-28",
        range_candidate_id="range_1",
        classification="accumulation",
        state="recognized",
        accumulation_event_types=("SC", "Spring"),
        distribution_event_types=(),
        accumulation_candidate_ids=ids,
        distribution_candidate_ids=(),
        accumulation_confirmed_type_count=2,
        distribution_confirmed_type_count=0,
        accumulation_signature_events=("Spring",),
        distribution_signature_events=(),
        contradiction_codes=(),
        reason_codes=(),
    )


def _phase(ids: Tuple[str, ...]) -> PhaseClassificationResult:
    structure = _structure(ids)
    cand = PhaseCandidate(
        phase_candidate_version="wyckoff_phases.v1",
        candidate_id="phase_C",
        structure="accumulation",
        phase="C",
        ordinal=3,
        status="confirmed",
        as_of_date="2024-06-28",
        required_event_codes=("SC", "Spring"),
        supporting_candidate_ids=ids,
        contradicting_candidate_ids=(),
        missing_event_codes=(),
        required_gate_codes=(),
        passed_gate_codes=(),
        missing_gate_codes=(),
        failed_gate_codes=(),
        sequence_valid=True,
        confidence_components={},
        confidence=0.5,
        reason_codes=(),
    )
    return PhaseClassificationResult(
        phase_classification_version="wyckoff_phases.v1",
        as_of_date="2024-06-28",
        structure_classification=structure,
        selected_phase="C",
        selected_phase_status="confirmed",
        phase_state="PHASE_C",
        candidates=(cand,),
        reason_codes=(),
        config_used={},
    )


def _htf() -> HTFContextResult:
    return HTFContextResult(
        htf_context_version="wyckoff_htf_context.v1",
        as_of_date="2024-06-28",
        monthly_bias="up",
        monthly_sma=100.0,
        monthly_slope_pct=1.0,
        monthly_trend_quality=0.8,
        monthly_window_structure="hh",
        monthly_window_raw={},
        weekly_bias="up",
        weekly_sma=100.0,
        weekly_slope_pct=0.5,
        weekly_trend_quality=0.7,
        weekly_window_structure="hh",
        weekly_window_raw={},
        htf_alignment="aligned_up",
        contradiction_codes=(),
        missing_data=(),
        config_used={},
    )


def _trigger() -> FourHourTriggerResult:
    return FourHourTriggerResult(
        trigger_version="wyckoff_4h_trigger.v1",
        enabled=True,
        state="missing",
        reason_codes=("four_hour_trigger_missing",),
        side="LONG",
        evaluation_time_utc="2024-06-28T20:00:00Z",
        daily_market_data_as_of="2024-06-28",
        available_input_bars=20,
        available_completed_bars=20,
        required_completed_bars=11,
        excluded_incomplete_bar_count=0,
        latest_completed_4h_start="2024-06-28T12:00:00Z",
        latest_completed_4h_end="2024-06-28T16:00:00Z",
        latest_completed_4h_session_date="2024-06-28",
        staleness_sessions=0,
        local_high=101.0,
        local_low=99.0,
        trigger_level=101.0,
        contradiction_level=99.0,
        current_close=100.5,
        trigger_price=None,
        triggered=False,
        contradicted=False,
        missing_data=(),
        config_used={},
    )


def _inv(ids: Tuple[str, ...]) -> InvalidationResult:
    return InvalidationResult(
        invalidation_version="wyckoff_invalidation.v1",
        rule_code="daily_close_below_support_zone",
        level=97.8,
        source_range_id="range_1",
        source_event_ids=ids,
        zone={"lo": 98.0, "hi": 100.0},
        atr=2.0,
        buffer_atr_multiple=0.1,
        timeframe="1d",
        as_of="2024-06-28",
        reason=None,
        available=True,
    )


def _policy() -> PolicyDecisionResult:
    return PolicyDecisionResult(
        decision_policy_version="wyckoff_mtf.policy.v1",
        verdict="WATCH",
        side="LONG",
        setup_state="valid",
        trigger_state="missing",
        reason_code="watch_setup_valid",
        blocking_reasons=(),
        waiting_reasons=("four_hour_trigger_missing",),
        required_gate_results={"readiness_ready": True},
        allow_enter=False,
        enter_eligible_without_rollout_gate=False,
        selected_phase="C",
        selected_phase_status="confirmed",
        invalidation_available=True,
        trigger_required=True,
        trigger_confirmed=False,
    )


def _ranking() -> RankingResult:
    comps = {
        "htf_alignment_quality": 1.0,
        "range_quality": 0.8,
        "structure_evidence_quality": 0.5,
        "phase_completeness": 0.6,
        "trigger_quality": 0.0,
        "volume_coverage_quality": 1.0,
    }
    return RankingResult(
        ranking_version="wyckoff_mtf.v2.rank.v1",
        components=comps,
        ranking_score=sum(comps.values()) / len(comps),
    )


def _events(n_optional: int = 20) -> EventDetectionResult:
    relevant = [
        _ec(family="accumulation", code="SC", index=10, date="2024-02-01"),
        _ec(family="accumulation", code="Spring", index=20, date="2024-02-15"),
    ]
    optional: List[EventCandidate] = []
    for i in range(n_optional):
        optional.append(
            _ec(
                family="accumulation",
                code="ST",
                index=30 + i,
                date=f"2024-03-{(i % 28) + 1:02d}",
                status="confirmation_pending" if i % 2 else "confirmed",
            )
        )
    all_c = relevant + optional
    by = {}
    for c in all_c:
        key = f"{c.family}:{c.event_code}"
        by.setdefault(key, []).append(c)
    return EventDetectionResult(
        event_detection_version="wyckoff_events.v1",
        as_of_date="2024-06-28",
        range_candidate_id="range_1",
        candidates=tuple(all_c),
        candidates_by_code={k: tuple(v) for k, v in by.items()},
        rejection_reason_counts={},
        candidates_truncated=False,
        config_used={},
    )


class TestEvidenceMapping:
    def test_unique_identities_and_json(self):
        ids = ("accumulation_SC_10", "accumulation_Spring_20")
        events = _events(10)
        bundle, meta, included = build_evidence_bundle(
            symbol="TEST",
            policy=_policy(),
            readiness=_ready(),
            aggregation=None,
            htf=_htf(),
            selected_range=_range(),
            event_result=events,
            structure=_structure(ids),
            phase_result=_phase(ids),
            trigger=_trigger(),
            invalidation=_inv(ids),
            ranking=_ranking(),
            market_data_as_of="2024-06-28",
            last_close=100.0,
        )
        keys = [i.identity_key() for i in bundle.items]
        assert len(keys) == len(set(keys))
        payload = bundle.to_dict()
        json.dumps(payload, allow_nan=False, sort_keys=True)
        assert all(i.source_type != "external" for i in bundle.items)

    def test_chronological_and_family_keys(self):
        ids = ("accumulation_SC_10", "accumulation_Spring_20")
        events = _events(5)
        bundle, _, included = build_evidence_bundle(
            symbol="TEST",
            policy=_policy(),
            readiness=_ready(),
            aggregation=None,
            htf=_htf(),
            selected_range=_range(),
            event_result=events,
            structure=_structure(ids),
            phase_result=_phase(ids),
            trigger=_trigger(),
            invalidation=_inv(ids),
            ranking=_ranking(),
            market_data_as_of="2024-06-28",
            last_close=100.0,
        )
        seq_item = next(i for i in bundle.items if i.code == "bounded_event_sequence")
        dates = [c["date"] for c in seq_item.raw_value]
        assert dates == sorted(dates)
        summary = next(i for i in bundle.items if i.code == "event_detection_summary")
        assert "accumulation:SC" in summary.raw_value["candidates_by_code"]

    def test_decision_relevant_never_omitted(self):
        ids = ("accumulation_SC_10", "accumulation_Spring_20")
        events = _events(40)
        decision = decision_relevant_candidate_ids(
            structure=_structure(ids),
            phase_result=_phase(ids),
            invalidation=_inv(ids),
            event_result=events,
        )
        included, meta = bound_event_candidates(
            events, decision_relevant=decision, max_candidates=16
        )
        included_ids = {c.candidate_id for c in included}
        assert ids[0] in included_ids and ids[1] in included_ids
        assert meta["included_decision_relevant_count"] >= 2

    def test_decision_evidence_limit_exceeded(self):
        cands = []
        for i in range(40):
            cands.append(
                _ec(
                    family="accumulation",
                    code="SC",
                    index=i,
                    date="2024-02-01",
                )
            )
        # unique ids already from family_code_index
        many = {c.candidate_id for c in cands}
        events = EventDetectionResult(
            event_detection_version="wyckoff_events.v1",
            as_of_date="2024-06-28",
            range_candidate_id="range_1",
            candidates=tuple(cands),
            candidates_by_code={"accumulation:SC": tuple(cands)},
            rejection_reason_counts={},
            candidates_truncated=False,
            config_used={},
        )
        with pytest.raises(EvidenceMappingError) as exc:
            bound_event_candidates(
                events, decision_relevant=many, max_candidates=16
            )
        assert exc.value.reason_code == "decision_evidence_limit_exceeded"

    def test_evidence_size_and_provenance(self):
        ids = ("accumulation_SC_10", "accumulation_Spring_20")
        events = _events(40)
        bundle, _, _ = build_evidence_bundle(
            symbol="TEST",
            policy=_policy(),
            readiness=_ready(),
            aggregation=None,
            htf=_htf(),
            selected_range=_range(),
            event_result=events,
            structure=_structure(ids),
            phase_result=_phase(ids),
            trigger=_trigger(),
            invalidation=_inv(ids),
            ranking=_ranking(),
            market_data_as_of="2024-06-28",
            last_close=100.0,
            config=resolve_config({"max_event_candidates_in_evidence": 32}),
        )
        evid = bundle.to_dict()
        size = len(canonical_json(evid).encode("utf-8"))
        assert size < MAX_EVIDENCE_BYTES
        details = {
            "evidence": evid,
            "setup_state": "valid",
            "trigger_state": "missing",
            "score_components": {"range_width": 14.0},
            "invalidation": _inv(ids).to_dict(),
            "ranking": _ranking().to_dict(),
            "rejection_reason": None,
            "waiting_reason": "four_hour_trigger_missing",
            "snapshot_date": "2024-06-28",
        }
        snap, meta = build_evidence_snapshot(details, details["score_components"])
        assert meta["evidence_original_size_bytes"] > 0

    def test_config_key_order_invariant(self):
        ids = ("accumulation_SC_10", "accumulation_Spring_20")
        events = _events(5)
        cfg_a = resolve_config({"max_event_candidates_in_evidence": 32})
        cfg_b = resolve_config(
            {k: cfg_a[k] for k in reversed(list(cfg_a.keys()))}
        )
        kwargs = dict(
            symbol="TEST",
            policy=_policy(),
            readiness=_ready(),
            aggregation=None,
            htf=_htf(),
            selected_range=_range(),
            event_result=events,
            structure=_structure(ids),
            phase_result=_phase(ids),
            trigger=_trigger(),
            invalidation=_inv(ids),
            ranking=_ranking(),
            market_data_as_of="2024-06-28",
            last_close=100.0,
        )
        a, _, _ = build_evidence_bundle(**kwargs, config=cfg_a)
        b, _, _ = build_evidence_bundle(**kwargs, config=cfg_b)
        assert a.to_dict() == b.to_dict()
