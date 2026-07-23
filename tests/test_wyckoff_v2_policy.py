"""Phase 9C1: wyckoff_mtf.policy.v1 decision policy tests."""

from __future__ import annotations

from typing import Optional, Tuple

from app.workers.strategies.wyckoff_v2.constants import default_config, resolve_config
from app.workers.strategies.wyckoff_v2.models import (
    EventCandidate,
    EventDetectionResult,
    FourHourTriggerResult,
    HTFContextResult,
    InvalidationResult,
    PhaseCandidate,
    PhaseClassificationResult,
    PriceZone,
    RangeCandidate,
    ReadinessResult,
    StructureClassificationResult,
)
from app.workers.strategies.wyckoff_v2.policy import (
    compute_invalidation,
    compute_ranking,
    evaluate_policy,
)


def _ready(ready: bool = True, reasons: Tuple[str, ...] = ()) -> ReadinessResult:
    return ReadinessResult(
        readiness_version="wyckoff_readiness.v1",
        ready=ready,
        status="ready" if ready else "insufficient_history",
        reason_codes=reasons,
        latest_bar_completion={"state": "completed" if ready else "unknown"},
        evaluation_time_utc="2024-06-28T20:00:00Z",
        market_data_as_of="2024-06-28",
        desired_history_bars=600,
        requested_history_bars=600,
        available_input_bars=600 if ready else 10,
        available_completed_bars=600 if ready else 10,
        history_depth_capped=False,
        history_depth_complete=ready,
        required_monthly_periods=24,
        available_completed_monthly_periods=30 if ready else 2,
        required_weekly_periods=26,
        available_completed_weekly_periods=40 if ready else 2,
        required_daily_structure_bars=120,
        usable_volume_bars=600 if ready else 10,
        required_volume_bars=100,
        volume_coverage=1.0 if ready else 0.1,
        excluded_partial_daily_bar_date=None,
        missing_fields=(),
    )


def _range(valid: bool = True) -> RangeCandidate:
    support = PriceZone(lo=98.0, hi=100.0)
    resistance = PriceZone(lo=110.0, hi=112.0)
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
        support_zone=support,
        resistance_zone=resistance,
        support=support.midpoint,
        resistance=resistance.midpoint,
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
        valid=valid,
        rejection_reasons=() if valid else ("too_narrow",),
    )


def _structure(
    classification: str = "accumulation",
    state: str = "recognized",
) -> StructureClassificationResult:
    return StructureClassificationResult(
        phase_classification_version="wyckoff_phases.v1",
        as_of_date="2024-06-28",
        range_candidate_id="range_1",
        classification=classification,
        state=state,
        accumulation_event_types=("SC", "Spring") if classification == "accumulation" else (),
        distribution_event_types=("BC", "UT") if classification == "distribution" else (),
        accumulation_candidate_ids=("a1", "a2") if classification == "accumulation" else (),
        distribution_candidate_ids=("d1", "d2") if classification == "distribution" else (),
        accumulation_confirmed_type_count=2 if classification == "accumulation" else 0,
        distribution_confirmed_type_count=2 if classification == "distribution" else 0,
        accumulation_signature_events=("Spring",) if classification == "accumulation" else (),
        distribution_signature_events=("UT",) if classification == "distribution" else (),
        contradiction_codes=(),
        reason_codes=(),
    )


def _phase(
    selected: Optional[str] = "C",
    status: str = "confirmed",
) -> PhaseClassificationResult:
    structure = _structure()
    if selected is None:
        return PhaseClassificationResult(
            phase_classification_version="wyckoff_phases.v1",
            as_of_date="2024-06-28",
            structure_classification=structure,
            selected_phase=None,
            selected_phase_status=None,
            phase_state="UNKNOWN_PHASE",
            candidates=(),
            reason_codes=("no_valid_phase_candidate",),
            config_used={},
        )
    cand = PhaseCandidate(
        phase_candidate_version="wyckoff_phases.v1",
        candidate_id=f"phase_{selected}",
        structure="accumulation",
        phase=selected,
        ordinal={"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}[selected],
        status=status,
        as_of_date="2024-06-28",
        required_event_codes=("SC",),
        supporting_candidate_ids=("a1", "a2"),
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
        selected_phase=selected,
        selected_phase_status=status,
        phase_state=f"PHASE_{selected}",
        candidates=(cand,),
        reason_codes=(),
        config_used={},
    )


def _htf(alignment: str = "aligned_up") -> HTFContextResult:
    return HTFContextResult(
        htf_context_version="wyckoff_htf_context.v1",
        as_of_date="2024-06-28",
        monthly_bias="up" if "up" in alignment else "down",
        monthly_sma=100.0,
        monthly_slope_pct=1.0,
        monthly_trend_quality=0.8,
        monthly_window_structure="higher_highs",
        monthly_window_raw={},
        weekly_bias="up" if "up" in alignment else "down",
        weekly_sma=100.0,
        weekly_slope_pct=0.5,
        weekly_trend_quality=0.7,
        weekly_window_structure="higher_highs",
        weekly_window_raw={},
        htf_alignment=alignment,
        contradiction_codes=("htf_bias_conflict",) if alignment == "contradiction" else (),
        missing_data=(),
        config_used={},
    )


def _trigger(
    state: str = "confirmed",
    *,
    enabled: bool = True,
    price: Optional[float] = 105.0,
    reasons: Tuple[str, ...] = (),
) -> FourHourTriggerResult:
    return FourHourTriggerResult(
        trigger_version="wyckoff_4h_trigger.v1",
        enabled=enabled,
        state=state,
        reason_codes=reasons,
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
        current_close=price,
        trigger_price=price if state == "confirmed" else None,
        triggered=state == "confirmed",
        contradicted=state == "contradicted",
        missing_data=(),
        config_used={},
    )


def _inv(available: bool = True, level: Optional[float] = 97.8) -> InvalidationResult:
    return InvalidationResult(
        invalidation_version="wyckoff_invalidation.v1",
        rule_code="daily_close_below_support_zone" if available else None,
        level=level if available else None,
        source_range_id="range_1",
        source_event_ids=("a1", "a2"),
        zone={"lo": 98.0, "hi": 100.0} if available else None,
        atr=2.0 if available else None,
        buffer_atr_multiple=0.1,
        timeframe="1d",
        as_of="2024-06-28",
        reason=None if available else "invalidation_not_computable",
        available=available,
    )


def _watch_base(**overrides):
    kwargs = dict(
        readiness=_ready(),
        selected_range=_range(),
        structure=_structure(),
        phase_result=_phase("C"),
        htf=_htf("aligned_up"),
        trigger=_trigger("missing", reasons=("four_hour_trigger_missing",)),
        invalidation=_inv(),
        last_close=100.0,
        config=resolve_config(),
    )
    kwargs.update(overrides)
    return evaluate_policy(**kwargs)


class TestAvoidSemantics:
    def test_readiness_not_ready(self):
        r = evaluate_policy(
            readiness=_ready(False, ("insufficient_history",)),
            selected_range=None,
            structure=None,
            phase_result=None,
            htf=None,
            trigger=None,
            invalidation=None,
        )
        assert r.verdict == "AVOID"
        assert r.reason_code == "readiness_not_ready"
        assert r.setup_state == "unknown"

    def test_readiness_missing(self):
        r = evaluate_policy(
            readiness=None,
            selected_range=None,
            structure=None,
            phase_result=None,
            htf=None,
            trigger=None,
            invalidation=None,
        )
        assert r.reason_code == "readiness_missing"

    def test_unconfirmed_daily_bar(self):
        ready = _ready(True, ("unconfirmed_bar_completion",))
        r = evaluate_policy(
            readiness=ready,
            selected_range=_range(),
            structure=_structure(),
            phase_result=_phase(),
            htf=_htf(),
            trigger=_trigger(),
            invalidation=_inv(),
            last_close=100.0,
        )
        assert r.reason_code == "unconfirmed_completed_daily_bar"

    def test_price_below_minimum(self):
        r = evaluate_policy(
            readiness=_ready(),
            selected_range=_range(),
            structure=_structure(),
            phase_result=_phase(),
            htf=_htf(),
            trigger=_trigger(),
            invalidation=_inv(),
            last_close=4.99,
            config=resolve_config(),
        )
        assert r.reason_code == "price_below_minimum"
        assert r.setup_state == "invalid"
        assert r.verdict == "AVOID"

    def test_price_at_minimum_passes_gate(self):
        r = _watch_base(last_close=5.0)
        assert r.verdict != "AVOID" or r.reason_code != "price_below_minimum"
        assert r.required_gate_results.get("minimum_price") is True

    def test_price_override_threshold(self):
        r = evaluate_policy(
            readiness=_ready(),
            selected_range=_range(),
            structure=_structure(),
            phase_result=_phase(),
            htf=_htf(),
            trigger=_trigger(),
            invalidation=_inv(),
            last_close=9.99,
            config=resolve_config({"min_price": 10.0}),
        )
        assert r.reason_code == "price_below_minimum"

    def test_no_valid_range(self):
        r = _watch_base(selected_range=None)
        assert r.reason_code == "no_valid_selected_range"

    def test_ambiguous_structure(self):
        r = _watch_base(structure=_structure(state="ambiguous", classification="unknown"))
        assert r.reason_code == "ambiguous_structure"
        assert r.setup_state == "invalid"

    def test_unknown_structure(self):
        r = _watch_base(structure=_structure(state="unknown", classification="unknown"))
        assert r.reason_code == "unknown_structure"

    def test_invalidation_unavailable(self):
        r = _watch_base(invalidation=_inv(False))
        assert r.reason_code == "invalidation_unavailable"

    def test_htf_contradiction(self):
        r = _watch_base(htf=_htf("contradiction"))
        assert r.reason_code == "htf_contradiction"

    def test_htf_direction_conflict_accumulation(self):
        r = _watch_base(htf=_htf("aligned_down"))
        assert r.reason_code == "htf_direction_conflict"

    def test_htf_direction_conflict_distribution(self):
        r = evaluate_policy(
            readiness=_ready(),
            selected_range=_range(),
            structure=_structure("distribution"),
            phase_result=_phase("C"),
            htf=_htf("aligned_up"),
            trigger=_trigger(),
            invalidation=_inv(),
            last_close=100.0,
        )
        assert r.reason_code == "htf_direction_conflict"


class TestWatchAndEnter:
    def test_default_allow_enter_false(self):
        assert default_config()["allow_enter"] is False
        assert isinstance(default_config()["allow_enter"], bool)

    def test_unknown_phase_watch(self):
        r = _watch_base(phase_result=_phase(None), trigger=_trigger("missing"))
        assert r.verdict == "WATCH"
        assert "unknown_phase" in r.waiting_reasons

    def test_phase_a_not_enter_eligible(self):
        r = _watch_base(phase_result=_phase("A"))
        assert r.verdict == "WATCH"
        assert "phase_not_enter_eligible" in r.waiting_reasons

    def test_four_hour_missing_states(self):
        for state, waiting in (
            ("missing", "four_hour_trigger_missing"),
            ("contradicted", "four_hour_trigger_contradicted"),
            ("unknown", "four_hour_trigger_unknown"),
        ):
            r = _watch_base(
                trigger=_trigger(state, reasons=(waiting,)),
                phase_result=_phase("C"),
            )
            assert r.verdict == "WATCH"
            assert waiting in r.waiting_reasons

    def test_no_4h_data_watch(self):
        r = _watch_base(trigger=None, phase_result=_phase("C"))
        assert r.verdict == "WATCH"
        assert "four_hour_data_missing" in r.waiting_reasons

    def test_confirmed_still_watch_when_enter_disabled(self):
        r = _watch_base(
            trigger=_trigger("confirmed"),
            phase_result=_phase("C"),
            config=resolve_config({"enable_4h_trigger": True}),
        )
        assert r.verdict == "WATCH"
        assert "enter_disabled_shadow_only" in r.waiting_reasons
        assert r.enter_eligible_without_rollout_gate is True

    def test_enter_when_allow_enter_true(self):
        r = _watch_base(
            trigger=_trigger("confirmed"),
            phase_result=_phase("C"),
            config=resolve_config(
                {"allow_enter": True, "enable_4h_trigger": True}
            ),
        )
        assert r.verdict == "ENTER"
        assert r.reason_code == "enter_all_gates_passed"

    def test_never_reject(self):
        r = _watch_base(selected_range=None)
        assert r.verdict != "REJECT"

    def test_ranking_not_consulted(self):
        # evaluate_policy has no ranking parameter — structural proof
        import inspect

        sig = inspect.signature(evaluate_policy)
        assert "ranking" not in sig.parameters

    def test_invalidation_compute_accumulation(self):
        inv = compute_invalidation(
            structure=_structure("accumulation"),
            selected_range=_range(),
            phase_result=_phase("C"),
            as_of="2024-06-28",
        )
        assert inv.available is True
        assert inv.rule_code == "daily_close_below_support_zone"
        assert inv.level == 98.0 - 0.1 * 2.0
        assert inv.to_dict().get("stop_price") is None

    def test_ranking_null_when_component_unknown(self):
        ranking = compute_ranking(
            structure=_structure(),
            selected_range=_range(),
            phase_result=_phase("C"),
            htf=_htf("unknown"),
            trigger=_trigger("unknown"),
        )
        assert ranking.ranking_score is None


class TestEntryReferenceAndAllowEnter:
    def test_require_trigger_false_no_4h_watch_entry_ref(self):
        r = _watch_base(
            trigger=None,
            phase_result=_phase("C"),
            config=resolve_config(
                {
                    "require_4h_trigger_for_enter": False,
                    "allow_enter": True,
                    "enable_4h_trigger": True,
                }
            ),
        )
        assert r.verdict == "WATCH"
        assert "entry_reference_unavailable" in r.waiting_reasons

    def test_require_trigger_false_missing_trigger_watch(self):
        r = _watch_base(
            trigger=_trigger("missing"),
            phase_result=_phase("C"),
            config=resolve_config(
                {
                    "require_4h_trigger_for_enter": False,
                    "allow_enter": True,
                }
            ),
        )
        assert r.verdict == "WATCH"
        assert "entry_reference_unavailable" in r.waiting_reasons

    def test_allow_enter_true_no_trigger_price_watch(self):
        r = _watch_base(
            trigger=_trigger("confirmed", price=None),
            phase_result=_phase("C"),
            config=resolve_config({"allow_enter": True, "enable_4h_trigger": True}),
        )
        # confirmed with null price → not a valid entry reference
        assert r.verdict == "WATCH"
        assert "entry_reference_unavailable" in r.waiting_reasons

    def test_allow_enter_true_alone_cannot_bypass_phase(self):
        r = _watch_base(
            trigger=_trigger("confirmed"),
            phase_result=_phase("A"),
            config=resolve_config({"allow_enter": True, "enable_4h_trigger": True}),
        )
        assert r.verdict == "WATCH"
        assert r.verdict != "ENTER"

    def test_allow_enter_recorded_false(self):
        r = _watch_base(trigger=_trigger("confirmed"))
        assert r.allow_enter is False


class TestPolicyTruthTable:
    def test_truth_table_combinations(self):
        cases = [
            # setup invalid → AVOID
            dict(
                structure=_structure(state="unknown", classification="unknown"),
                expect="AVOID",
            ),
            # phase ineligible + confirmed + allow false → WATCH
            dict(
                phase_result=_phase("A"),
                trigger=_trigger("confirmed"),
                expect="WATCH",
            ),
            # phase eligible + missing trigger + require true → WATCH
            dict(
                phase_result=_phase("C"),
                trigger=_trigger("missing"),
                expect="WATCH",
            ),
            # phase eligible + confirmed + allow false → WATCH (rollout)
            dict(
                phase_result=_phase("C"),
                trigger=_trigger("confirmed"),
                config=resolve_config({"enable_4h_trigger": True}),
                expect="WATCH",
                waiting="enter_disabled_shadow_only",
            ),
            # phase eligible + confirmed + allow true → ENTER
            dict(
                phase_result=_phase("C"),
                trigger=_trigger("confirmed"),
                config=resolve_config({"allow_enter": True, "enable_4h_trigger": True}),
                expect="ENTER",
            ),
            # require false + confirmed + allow true → ENTER
            dict(
                phase_result=_phase("C"),
                trigger=_trigger("confirmed"),
                config=resolve_config(
                    {
                        "allow_enter": True,
                        "require_4h_trigger_for_enter": False,
                        "enable_4h_trigger": True,
                    }
                ),
                expect="ENTER",
            ),
            # require false + no trigger + allow true → WATCH entry ref
            dict(
                phase_result=_phase("C"),
                trigger=None,
                config=resolve_config(
                    {
                        "allow_enter": True,
                        "require_4h_trigger_for_enter": False,
                    }
                ),
                expect="WATCH",
                waiting="entry_reference_unavailable",
            ),
            # contradicted + allow true → WATCH
            dict(
                phase_result=_phase("C"),
                trigger=_trigger("contradicted"),
                config=resolve_config({"allow_enter": True}),
                expect="WATCH",
            ),
        ]
        for case in cases:
            expect = case.pop("expect")
            waiting = case.pop("waiting", None)
            r = _watch_base(**case)
            assert r.verdict == expect, (case, r.verdict, r.reason_code, r.waiting_reasons)
            if waiting:
                assert waiting in r.waiting_reasons
            assert "ranking" not in r.required_gate_results
            assert r.verdict != "REJECT"


class TestMinPriceConfigValidation:
    def test_rejects_zero_negative_bool_nan(self):
        from app.workers.strategies.wyckoff_v2.constants import Phase9AConfigError

        for bad in (0, -1, True, False, float("nan"), float("inf")):
            try:
                resolve_config({"min_price": bad})
                raised = False
            except Phase9AConfigError:
                raised = True
            assert raised, bad

    def test_default_min_price_is_five(self):
        assert default_config()["min_price"] == 5.0
        assert isinstance(default_config()["min_price"], float)
