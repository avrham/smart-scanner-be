"""Phase 9C1: WyckoffMTFV2Strategy orchestration tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional, Tuple
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from app.workers.strategies.base import StrategyContext, StrategyDecision
from app.workers.strategies.wyckoff_v2.constants import default_config, resolve_config
from app.workers.strategies.wyckoff_v2.models import (
    CompletedAggregationResult,
    EventCandidate,
    EventDetectionResult,
    FourHourTriggerResult,
    HTFContextResult,
    PhaseCandidate,
    PhaseClassificationResult,
    PriceZone,
    RangeCandidate,
    RangeDetectionResult,
    ReadinessResult,
    StructureClassificationResult,
)
from app.workers.strategies.wyckoff_v2.strategy import WyckoffMTFV2Strategy


def _make_daily(n: int, end: str = "2024-06-28") -> pd.DataFrame:
    end_ts = pd.Timestamp(end)
    dates = []
    cur = end_ts
    while len(dates) < n:
        if cur.weekday() < 5:
            dates.append(cur)
        cur -= pd.Timedelta(days=1)
    dates = list(reversed(dates))
    rng = np.random.default_rng(0)
    closes = 100.0 + np.cumsum(rng.normal(0.0, 0.2, size=n))
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": closes + 0.5,
            "low": closes - 0.5,
            "close": closes,
            "volume": np.full(n, 1e6),
        }
    )


def _ready_result(frame: pd.DataFrame) -> ReadinessResult:
    as_of = pd.Timestamp(frame["date"].iloc[-1]).date().isoformat()
    return ReadinessResult(
        readiness_version="wyckoff_readiness.v1",
        ready=True,
        status="ready",
        reason_codes=(),
        latest_bar_completion={"state": "completed", "bar_date": as_of},
        evaluation_time_utc="2024-06-28T21:00:00Z",
        market_data_as_of=as_of,
        desired_history_bars=600,
        requested_history_bars=600,
        available_input_bars=len(frame),
        available_completed_bars=len(frame),
        history_depth_capped=False,
        history_depth_complete=True,
        required_monthly_periods=24,
        available_completed_monthly_periods=30,
        required_weekly_periods=26,
        available_completed_weekly_periods=40,
        required_daily_structure_bars=120,
        usable_volume_bars=len(frame),
        required_volume_bars=100,
        volume_coverage=1.0,
        excluded_partial_daily_bar_date=None,
        missing_fields=(),
        completed_daily_frame=frame,
    )


def _range(as_of: str = "2024-06-28") -> RangeCandidate:
    s = PriceZone(lo=98.0, hi=100.0)
    r = PriceZone(lo=110.0, hi=112.0)
    return RangeCandidate(
        range_candidate_version="wyckoff_range.v1",
        candidate_id="range_1",
        as_of_date=as_of,
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
        accumulation_event_types=("SC", "Spring"),
        distribution_event_types=(),
        accumulation_candidate_ids=("accumulation_SC_10", "accumulation_Spring_20"),
        distribution_candidate_ids=(),
        accumulation_confirmed_type_count=4,
        distribution_confirmed_type_count=0,
        accumulation_signature_events=("Spring",),
        distribution_signature_events=(),
        contradiction_codes=(),
        reason_codes=(),
    )


def _phase(selected: Optional[str] = "C") -> PhaseClassificationResult:
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
        status="confirmed",
        as_of_date="2024-06-28",
        required_event_codes=("SC", "Spring"),
        supporting_candidate_ids=("accumulation_SC_10", "accumulation_Spring_20"),
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
        selected_phase_status="confirmed",
        phase_state=f"PHASE_{selected}",
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


def _events() -> EventDetectionResult:
    cands = [
        EventCandidate(
            event_candidate_version="wyckoff_events.v1",
            candidate_id="accumulation_SC_10",
            range_candidate_id="range_1",
            family="accumulation",
            event_code="SC",
            event_label="sc",
            date="2024-02-01",
            index=10,
            timeframe="1d",
            as_of_date="2024-06-28",
            price=99.0,
            level=99.0,
            direction="down",
            status="confirmed",
            confirmation_status="confirmed",
            confirmation_end_date=None,
            range_relationship="test",
            effort_result={},
            required_gate_results={},
            confidence_components={},
            confidence=0.5,
            supporting_candidate_ids=(),
            contradicting_candidate_ids=(),
            reason_codes=(),
            usable_for_structure=True,
            metadata={},
        ),
        EventCandidate(
            event_candidate_version="wyckoff_events.v1",
            candidate_id="accumulation_Spring_20",
            range_candidate_id="range_1",
            family="accumulation",
            event_code="Spring",
            event_label="spring",
            date="2024-02-15",
            index=20,
            timeframe="1d",
            as_of_date="2024-06-28",
            price=98.0,
            level=98.0,
            direction="down",
            status="confirmed",
            confirmation_status="confirmed",
            confirmation_end_date=None,
            range_relationship="test",
            effort_result={},
            required_gate_results={},
            confidence_components={},
            confidence=0.5,
            supporting_candidate_ids=(),
            contradicting_candidate_ids=(),
            reason_codes=(),
            usable_for_structure=True,
            metadata={},
        ),
    ]
    return EventDetectionResult(
        event_detection_version="wyckoff_events.v1",
        as_of_date="2024-06-28",
        range_candidate_id="range_1",
        candidates=tuple(cands),
        candidates_by_code={
            "accumulation:SC": (cands[0],),
            "accumulation:Spring": (cands[1],),
        },
        rejection_reason_counts={},
        candidates_truncated=False,
        config_used={},
    )


def _trigger(state: str = "confirmed", enabled: bool = True) -> FourHourTriggerResult:
    return FourHourTriggerResult(
        trigger_version="wyckoff_4h_trigger.v1",
        enabled=enabled,
        state=state,
        reason_codes=() if state == "confirmed" else (f"four_hour_trigger_{state}",),
        side="LONG",
        evaluation_time_utc="2024-06-28T21:00:00Z",
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
        current_close=105.0 if state == "confirmed" else 100.0,
        trigger_price=105.0 if state == "confirmed" else None,
        triggered=state == "confirmed",
        contradicted=state == "contradicted",
        missing_data=(),
        config_used={},
    )


def _agg(frame: pd.DataFrame) -> CompletedAggregationResult:
    return CompletedAggregationResult(
        aggregation_version="wyckoff_aggregation.v1",
        monthly_frame=frame.copy(),
        weekly_frame=frame.copy(),
        monthly_completed_periods=30,
        weekly_completed_periods=40,
        excluded_partial_month_period=None,
        excluded_partial_week_period=None,
        latest_completed_daily_date="2024-06-28",
        evaluation_session_date="2024-06-28",
    )


def _patch_pipeline(
    monkeypatch,
    *,
    structure=None,
    phase=None,
    trigger=None,
    selected_range=None,
    event_spy=None,
):
    import app.workers.strategies.wyckoff_v2.strategy as strat_mod

    frame = _make_daily(300)
    as_of = "2024-06-28"
    rng = selected_range if selected_range is not None else _range(as_of)
    structure = structure if structure is not None else _structure()
    phase = phase if phase is not None else _phase("C")
    trigger = trigger if trigger is not None else _trigger("missing")

    monkeypatch.setattr(
        strat_mod, "assess_data_readiness", lambda *a, **k: _ready_result(frame)
    )
    monkeypatch.setattr(
        strat_mod, "aggregate_completed_timeframes", lambda *a, **k: _agg(frame)
    )

    def _detect_ranges(*a, **k):
        return RangeDetectionResult(
            range_detection_version="wyckoff_range.v1",
            as_of_date=as_of,
            evaluated_candidate_count=1,
            valid_candidate_count=1 if rng and rng.valid else 0,
            selected_range=rng,
            rejection_reason_counts={},
            post_range_segment=(),
            config_used={},
        )

    monkeypatch.setattr(strat_mod, "detect_trading_ranges", _detect_ranges)
    monkeypatch.setattr(strat_mod, "measure_htf_context", lambda *a, **k: _htf())

    def _detect_events(*a, **k):
        if event_spy is not None:
            event_spy(*a, **k)
        return _events()

    monkeypatch.setattr(strat_mod, "detect_event_candidates", _detect_events)
    monkeypatch.setattr(strat_mod, "classify_structure", lambda *a, **k: structure)
    monkeypatch.setattr(strat_mod, "classify_phases", lambda *a, **k: phase)
    monkeypatch.setattr(strat_mod, "analyze_4h_trigger", lambda *a, **k: trigger)
    return frame


class TestStrategyOrchestration:
    def test_early_readiness_avoid(self):
        strat = WyckoffMTFV2Strategy()
        df = _make_daily(10)
        ctx = StrategyContext(
            symbol="AAA",
            pattern_code=strat.pattern_code,
            config=default_config(),
            data_meta={
                "evaluation_time_utc": "2024-06-28T21:00:00Z",
                "as_of_date": "2024-06-28",
                "explicit_completed": True,
            },
        )
        result = strat.evaluate(df, ctx)
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason is not None
        assert result.details["evidence"]["evidence_version"] == "evidence.v1"
        assert result.details["trading_range"] is None
        assert result.stop_price is None and result.target_price is None

    def test_no_range_avoid(self, monkeypatch):
        import app.workers.strategies.wyckoff_v2.strategy as strat_mod

        frame = _make_daily(300)
        monkeypatch.setattr(
            strat_mod, "assess_data_readiness", lambda *a, **k: _ready_result(frame)
        )
        monkeypatch.setattr(
            strat_mod, "aggregate_completed_timeframes", lambda *a, **k: _agg(frame)
        )
        monkeypatch.setattr(
            strat_mod,
            "detect_trading_ranges",
            lambda *a, **k: RangeDetectionResult(
                range_detection_version="wyckoff_range.v1",
                as_of_date="2024-06-28",
                evaluated_candidate_count=0,
                valid_candidate_count=0,
                selected_range=None,
                rejection_reason_counts={},
                post_range_segment=(),
                config_used={},
            ),
        )
        monkeypatch.setattr(strat_mod, "measure_htf_context", lambda *a, **k: _htf())
        strat = WyckoffMTFV2Strategy()
        result = strat.evaluate(
            frame,
            StrategyContext(
                symbol="AAA",
                pattern_code=strat.pattern_code,
                config=default_config(),
                data_meta={
                    "evaluation_time_utc": "2024-06-28T21:00:00Z",
                    "as_of_date": "2024-06-28",
                },
            ),
        )
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason == "no_valid_selected_range"

    def test_unknown_and_ambiguous_structure(self, monkeypatch):
        frame = _patch_pipeline(
            monkeypatch, structure=_structure(state="unknown", classification="unknown")
        )
        strat = WyckoffMTFV2Strategy()
        ctx = StrategyContext(
            symbol="AAA",
            pattern_code=strat.pattern_code,
            config=default_config(),
            data_meta={
                "evaluation_time_utc": "2024-06-28T21:00:00Z",
                "as_of_date": "2024-06-28",
            },
        )
        r = strat.evaluate(frame, ctx)
        assert r.decision == StrategyDecision.AVOID

        frame = _patch_pipeline(
            monkeypatch,
            structure=_structure(state="ambiguous", classification="unknown"),
        )
        r2 = strat.evaluate(frame, ctx)
        assert r2.rejection_reason == "ambiguous_structure"

    def test_watch_paths_and_enter(self, monkeypatch):
        strat = WyckoffMTFV2Strategy()
        meta = {
            "evaluation_time_utc": "2024-06-28T21:00:00Z",
            "as_of_date": "2024-06-28",
        }

        # unknown phase
        frame = _patch_pipeline(monkeypatch, phase=_phase(None))
        r = strat.evaluate(
            frame,
            StrategyContext("AAA", strat.pattern_code, default_config(), data_meta=meta),
        )
        assert r.decision == StrategyDecision.WATCH

        # Phase A
        frame = _patch_pipeline(monkeypatch, phase=_phase("A"))
        r = strat.evaluate(
            frame,
            StrategyContext("AAA", strat.pattern_code, default_config(), data_meta=meta),
        )
        assert r.decision == StrategyDecision.WATCH

        # Phase C + confirmed + allow_enter false
        frame = _patch_pipeline(
            monkeypatch, phase=_phase("C"), trigger=_trigger("confirmed")
        )
        cfg = resolve_config({"enable_4h_trigger": True})
        r = strat.evaluate(
            frame,
            StrategyContext("AAA", strat.pattern_code, cfg, data_meta=meta),
        )
        assert r.decision == StrategyDecision.WATCH
        assert "enter_disabled_shadow_only" in (r.details["waiting_reason"] or "")
        assert r.entry_price is None

        # ENTER
        frame = _patch_pipeline(
            monkeypatch, phase=_phase("C"), trigger=_trigger("confirmed")
        )
        cfg = resolve_config({"enable_4h_trigger": True, "allow_enter": True})
        r = strat.evaluate(
            frame,
            StrategyContext("AAA", strat.pattern_code, cfg, data_meta=meta),
        )
        assert r.decision == StrategyDecision.ENTER
        assert r.entry_price == 105.0
        assert r.stop_price is None and r.target_price is None
        assert r.invalidation == pytest.approx(97.8)
        assert r.side.value == "LONG"

    def test_short_side(self, monkeypatch):
        structure = _structure("distribution")
        phase = _phase("D")
        # fix phase structure field for distribution
        object.__setattr__  # keep frozen; rebuild
        phase = PhaseClassificationResult(
            phase_classification_version="wyckoff_phases.v1",
            as_of_date="2024-06-28",
            structure_classification=structure,
            selected_phase="D",
            selected_phase_status="confirmed",
            phase_state="PHASE_D",
            candidates=(),
            reason_codes=(),
            config_used={},
        )
        # need sequence_valid candidate for invalidation source; empty ok
        cand = PhaseCandidate(
            phase_candidate_version="wyckoff_phases.v1",
            candidate_id="phase_D",
            structure="distribution",
            phase="D",
            ordinal=4,
            status="confirmed",
            as_of_date="2024-06-28",
            required_event_codes=(),
            supporting_candidate_ids=(),
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
        phase = PhaseClassificationResult(
            phase_classification_version="wyckoff_phases.v1",
            as_of_date="2024-06-28",
            structure_classification=structure,
            selected_phase="D",
            selected_phase_status="confirmed",
            phase_state="PHASE_D",
            candidates=(cand,),
            reason_codes=(),
            config_used={},
        )
        htf = _htf()
        htf = HTFContextResult(
            **{
                **htf.to_dict(),
                "htf_alignment": "aligned_down",
                "monthly_bias": "down",
                "weekly_bias": "down",
                "contradiction_codes": (),
                "missing_data": (),
                "config_used": {},
                "monthly_window_raw": {},
                "weekly_window_raw": {},
            }
        )
        import app.workers.strategies.wyckoff_v2.strategy as strat_mod

        frame = _patch_pipeline(
            monkeypatch,
            structure=structure,
            phase=phase,
            trigger=_trigger("missing"),
        )
        monkeypatch.setattr(strat_mod, "measure_htf_context", lambda *a, **k: htf)
        strat = WyckoffMTFV2Strategy()
        r = strat.evaluate(
            frame,
            StrategyContext(
                "AAA",
                strat.pattern_code,
                default_config(),
                data_meta={
                    "evaluation_time_utc": "2024-06-28T21:00:00Z",
                    "as_of_date": "2024-06-28",
                },
            ),
        )
        assert r.side.value == "SHORT"
        assert r.decision == StrategyDecision.WATCH

    def test_determinism_and_no_dataframe(self, monkeypatch):
        frame = _patch_pipeline(monkeypatch)
        strat = WyckoffMTFV2Strategy()
        ctx = StrategyContext(
            "AAA",
            strat.pattern_code,
            default_config(),
            data_meta={
                "evaluation_time_utc": "2024-06-28T21:00:00Z",
                "as_of_date": "2024-06-28",
            },
        )
        a = strat.evaluate(frame, ctx)
        b = strat.evaluate(frame, ctx)
        assert json.dumps(a.details["evidence"], sort_keys=True, allow_nan=False) == (
            json.dumps(b.details["evidence"], sort_keys=True, allow_nan=False)
        )
        for v in a.details.values():
            assert not isinstance(v, pd.DataFrame)

    def test_frozen_range_reuse_false(self, monkeypatch):
        calls = {}

        def spy(*a, **k):
            calls["allow_frozen_range_reuse"] = k.get("allow_frozen_range_reuse")
            return _events()

        frame = _patch_pipeline(monkeypatch, event_spy=spy)
        strat = WyckoffMTFV2Strategy()
        strat.evaluate(
            frame,
            StrategyContext(
                "AAA",
                strat.pattern_code,
                default_config(),
                data_meta={
                    "evaluation_time_utc": "2024-06-28T21:00:00Z",
                    "as_of_date": "2024-06-28",
                },
            ),
        )
        assert calls["allow_frozen_range_reuse"] is False

    def test_same_as_of_mismatch_raises(self, monkeypatch):
        bad = _range("2024-06-01")
        frame = _patch_pipeline(monkeypatch, selected_range=bad)
        strat = WyckoffMTFV2Strategy()
        with pytest.raises(ValueError, match="as_of_date"):
            strat.evaluate(
                frame,
                StrategyContext(
                    "AAA",
                    strat.pattern_code,
                    default_config(),
                    data_meta={
                        "evaluation_time_utc": "2024-06-28T21:00:00Z",
                        "as_of_date": "2024-06-28",
                    },
                ),
            )

    def test_ranking_cannot_override_avoid(self, monkeypatch):
        import app.workers.strategies.wyckoff_v2.strategy as strat_mod
        from app.workers.strategies.wyckoff_v2.models import RankingResult

        frame = _patch_pipeline(
            monkeypatch,
            structure=_structure(state="unknown", classification="unknown"),
        )

        def perfect_ranking(**kwargs):
            comps = {
                "htf_alignment_quality": 1.0,
                "range_quality": 1.0,
                "structure_evidence_quality": 1.0,
                "phase_completeness": 1.0,
                "trigger_quality": 1.0,
                "volume_coverage_quality": 1.0,
            }
            return RankingResult(
                ranking_version="wyckoff_mtf.v2.rank.v1",
                components=comps,
                ranking_score=1.0,
            )

        monkeypatch.setattr(strat_mod, "compute_ranking", perfect_ranking)
        strat = WyckoffMTFV2Strategy()
        r = strat.evaluate(
            frame,
            StrategyContext(
                "AAA",
                strat.pattern_code,
                default_config(),
                data_meta={
                    "evaluation_time_utc": "2024-06-28T21:00:00Z",
                    "as_of_date": "2024-06-28",
                },
            ),
        )
        assert r.decision == StrategyDecision.AVOID
        assert r.score == 1.0

    def test_trigger_integration_matrix(self, monkeypatch):
        strat = WyckoffMTFV2Strategy()
        meta = {
            "evaluation_time_utc": "2024-06-28T21:00:00Z",
            "as_of_date": "2024-06-28",
        }
        cases = [
            (_trigger("unknown", enabled=False), default_config(), StrategyDecision.WATCH),
            (_trigger("unknown"), resolve_config({"enable_4h_trigger": True}), StrategyDecision.WATCH),
            (_trigger("missing"), resolve_config({"enable_4h_trigger": True}), StrategyDecision.WATCH),
            (
                _trigger("contradicted"),
                resolve_config({"enable_4h_trigger": True}),
                StrategyDecision.WATCH,
            ),
            (
                _trigger("confirmed"),
                resolve_config({"enable_4h_trigger": True}),
                StrategyDecision.WATCH,
            ),
            (
                _trigger("confirmed"),
                resolve_config({"enable_4h_trigger": True, "allow_enter": True}),
                StrategyDecision.ENTER,
            ),
        ]
        for trig, cfg, expected in cases:
            frame = _patch_pipeline(monkeypatch, trigger=trig, phase=_phase("C"))
            r = strat.evaluate(
                frame,
                StrategyContext("AAA", strat.pattern_code, cfg, data_meta=meta),
            )
            assert r.decision == expected, (trig.state, cfg.get("allow_enter"), r.decision)


class TestMinPriceAndEarlyShortCircuit:
    def test_price_below_minimum_avoids_and_skips_events(self, monkeypatch):
        import app.workers.strategies.wyckoff_v2.strategy as strat_mod

        frame = _make_daily(300)
        frame["close"] = 4.99
        frame["open"] = 4.99
        frame["high"] = 5.1
        frame["low"] = 4.8
        called = {"events": 0, "phases": 0, "trigger": 0, "ranking": 0}

        monkeypatch.setattr(
            strat_mod, "assess_data_readiness", lambda *a, **k: _ready_result(frame)
        )

        def boom_events(*a, **k):
            called["events"] += 1
            raise AssertionError("events should not run")

        def boom_phases(*a, **k):
            called["phases"] += 1
            raise AssertionError("phases should not run")

        def boom_trigger(*a, **k):
            called["trigger"] += 1
            raise AssertionError("trigger should not run")

        def boom_ranking(*a, **k):
            called["ranking"] += 1
            raise AssertionError("ranking should not run")

        monkeypatch.setattr(strat_mod, "detect_event_candidates", boom_events)
        monkeypatch.setattr(strat_mod, "classify_phases", boom_phases)
        monkeypatch.setattr(strat_mod, "analyze_4h_trigger", boom_trigger)
        monkeypatch.setattr(strat_mod, "compute_ranking", boom_ranking)
        # aggregation/range may still be skipped entirely by price gate
        monkeypatch.setattr(
            strat_mod,
            "aggregate_completed_timeframes",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("agg")),
        )

        strat = WyckoffMTFV2Strategy()
        r = strat.evaluate(
            frame,
            StrategyContext(
                "AAA",
                strat.pattern_code,
                default_config(),
                data_meta={
                    "evaluation_time_utc": "2024-06-28T21:00:00Z",
                    "as_of_date": "2024-06-28",
                },
            ),
        )
        assert r.decision == StrategyDecision.AVOID
        assert r.rejection_reason == "price_below_minimum"
        assert r.entry_price is None
        assert r.stop_price is None and r.target_price is None
        assert called["events"] == 0
        assert "evidence" in r.details
        min_item = next(
            i
            for i in r.details["evidence"]["items"]
            if i["code"] == "minimum_price"
        )
        assert min_item["state"] == "fail"
        assert min_item["threshold"] == 5.0

    def test_readiness_failure_skips_events(self, monkeypatch):
        import app.workers.strategies.wyckoff_v2.strategy as strat_mod

        called = {"events": 0}

        def boom(*a, **k):
            called["events"] += 1
            raise AssertionError("events")

        monkeypatch.setattr(strat_mod, "detect_event_candidates", boom)
        strat = WyckoffMTFV2Strategy()
        r = strat.evaluate(
            _make_daily(10),
            StrategyContext(
                "AAA",
                strat.pattern_code,
                default_config(),
                data_meta={
                    "evaluation_time_utc": "2024-06-28T21:00:00Z",
                    "as_of_date": "2024-06-28",
                    "explicit_completed": True,
                },
            ),
        )
        assert r.decision == StrategyDecision.AVOID
        assert called["events"] == 0

    def test_entry_only_on_enter(self, monkeypatch):
        frame = _patch_pipeline(
            monkeypatch, phase=_phase("C"), trigger=_trigger("confirmed")
        )
        strat = WyckoffMTFV2Strategy()
        meta = {
            "evaluation_time_utc": "2024-06-28T21:00:00Z",
            "as_of_date": "2024-06-28",
        }
        watch = strat.evaluate(
            frame,
            StrategyContext(
                "AAA",
                strat.pattern_code,
                resolve_config({"enable_4h_trigger": True}),
                data_meta=meta,
            ),
        )
        assert watch.decision == StrategyDecision.WATCH
        assert watch.entry_price is None

        enter = strat.evaluate(
            frame,
            StrategyContext(
                "AAA",
                strat.pattern_code,
                resolve_config({"enable_4h_trigger": True, "allow_enter": True}),
                data_meta=meta,
            ),
        )
        assert enter.decision == StrategyDecision.ENTER
        assert enter.entry_price == 105.0
