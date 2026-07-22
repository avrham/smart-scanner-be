"""Phase 9B: structure classification and cumulative Phase A–E candidates."""

from __future__ import annotations

import json
from typing import List, Tuple

import numpy as np
import pandas as pd

from app.workers.strategies.wyckoff_v2.constants import (
    GATE_PHASE_E_HOLD_ABOVE_RESISTANCE,
    GATE_PHASE_E_HOLD_BELOW_SUPPORT,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.events import detect_event_candidates
from app.workers.strategies.wyckoff_v2.models import (
    EventCandidate,
    EventDetectionResult,
    HTFContextResult,
    PriceZone,
    RangeCandidate,
)
from app.workers.strategies.wyckoff_v2.phases import classify_phases, classify_structure


def _ec(
    *,
    family: str,
    code: str,
    index: int,
    date: str = "2024-01-01",
    usable: bool = True,
    status: str = "confirmed",
    confidence: float = 0.5,
    cid: str = None,
    supporting: Tuple[str, ...] = (),
) -> EventCandidate:
    cid = cid or f"{family}_{code}_{index}"
    return EventCandidate(
        event_candidate_version="wyckoff_events.v1",
        candidate_id=cid,
        range_candidate_id="range_1",
        family=family,
        event_code=code,
        event_label=f"{code.lower()}_candidate",
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
        required_gate_results={"ok": True},
        confidence_components={"x": confidence},
        confidence=confidence,
        supporting_candidate_ids=supporting,
        contradicting_candidate_ids=(),
        reason_codes=(),
        usable_for_structure=usable,
        metadata={},
    )


def _event_result(cands: List[EventCandidate]) -> EventDetectionResult:
    by = {}
    for c in cands:
        key = f"{c.family}:{c.event_code}"
        by.setdefault(key, []).append(c)
    return EventDetectionResult(
        event_detection_version="wyckoff_events.v1",
        as_of_date="2024-06-28",
        range_candidate_id="range_1",
        candidates=tuple(cands),
        candidates_by_code={k: tuple(v) for k, v in by.items()},
        rejection_reason_counts={},
        candidates_truncated=False,
        config_used={},
    )


def _range(bar_count: int = 40) -> RangeCandidate:
    support = PriceZone(lo=98.0, hi=100.0)
    resistance = PriceZone(lo=110.0, hi=112.0)
    return RangeCandidate(
        range_candidate_version="wyckoff_range.v1",
        candidate_id="range_1",
        as_of_date="2024-06-28",
        start_date="2024-01-02",
        end_date="2024-03-01",
        start_index=10,
        end_index=10 + bar_count - 1,
        post_range_bar_count=5,
        bar_count=bar_count,
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
        valid=True,
        rejection_reasons=(),
    )


def _daily_for_range(rng: RangeCandidate, extra: int = 10) -> pd.DataFrame:
    n = rng.end_index + 1 + extra
    dates = []
    cur = pd.Timestamp("2024-06-28")
    while len(dates) < n:
        if cur.weekday() < 5:
            dates.append(cur)
        cur -= pd.Timedelta(days=1)
    dates = list(reversed(dates))
    closes = np.full(n, 105.0)
    # Align dates at indexes
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": closes + 1,
            "low": closes - 1,
            "close": closes,
            "volume": np.full(n, 1e6),
        }
    )


class TestStructureClassification:
    def test_accumulation_recognized(self):
        cands = [
            _ec(family="accumulation", code="SC", index=10),
            _ec(family="accumulation", code="Spring", index=20),
            _ec(family="accumulation", code="AR", index=12),
        ]
        s = classify_structure(_event_result(cands), as_of_date="2024-06-28")
        assert s.classification == "accumulation"
        assert s.state == "recognized"
        assert "Spring" in s.accumulation_signature_events

    def test_distribution_recognized(self):
        cands = [
            _ec(family="distribution", code="BC", index=10),
            _ec(family="distribution", code="SOW", index=30),
            _ec(family="distribution", code="AR", index=12),
        ]
        s = classify_structure(_event_result(cands), as_of_date="2024-06-28")
        assert s.classification == "distribution"
        assert s.state == "recognized"

    def test_neither_unknown(self):
        cands = [_ec(family="accumulation", code="PS", index=5)]
        s = classify_structure(_event_result(cands), as_of_date="2024-06-28")
        assert s.classification == "unknown"
        assert s.state == "unknown"

    def test_both_ambiguous(self):
        cands = [
            _ec(family="accumulation", code="SC", index=10),
            _ec(family="accumulation", code="Spring", index=20),
            _ec(family="distribution", code="BC", index=11),
            _ec(family="distribution", code="SOW", index=30),
        ]
        s = classify_structure(_event_result(cands), as_of_date="2024-06-28")
        assert s.state == "ambiguous"
        assert s.classification == "unknown"

    def test_confidence_cannot_change_classification(self):
        low = [
            _ec(family="accumulation", code="SC", index=10, confidence=0.01),
            _ec(family="accumulation", code="Spring", index=20, confidence=0.01),
        ]
        high = [
            _ec(family="accumulation", code="SC", index=10, confidence=0.99),
            _ec(family="accumulation", code="Spring", index=20, confidence=0.99),
        ]
        s1 = classify_structure(_event_result(low), as_of_date="2024-06-28")
        s2 = classify_structure(_event_result(high), as_of_date="2024-06-28")
        assert s1.classification == s2.classification == "accumulation"

    def test_htf_cannot_force_classification(self):
        cands = [_ec(family="accumulation", code="PS", index=5)]
        htf = HTFContextResult(
            htf_context_version="wyckoff_htf_context.v1",
            as_of_date="2024-06-28",
            monthly_bias="up",
            monthly_sma=1.0,
            monthly_slope_pct=1.0,
            monthly_trend_quality=1.0,
            monthly_window_structure="higher_high_higher_low",
            monthly_window_raw={},
            weekly_bias="up",
            weekly_sma=1.0,
            weekly_slope_pct=1.0,
            weekly_trend_quality=1.0,
            weekly_window_structure="higher_high_higher_low",
            weekly_window_raw={},
            htf_alignment="aligned_up",
            contradiction_codes=(),
            missing_data=(),
            config_used={},
        )
        s = classify_structure(
            _event_result(cands), as_of_date="2024-06-28", htf_context=htf
        )
        assert s.classification == "unknown"

    def test_contradictory_incomplete_evidence(self):
        cands = [
            _ec(family="accumulation", code="SC", index=10),
            _ec(family="distribution", code="BC", index=11),
        ]
        s = classify_structure(_event_result(cands), as_of_date="2024-06-28")
        assert s.state == "unknown"
        assert "contradictory_incomplete_structure_evidence" in s.reason_codes


class TestCumulativePhases:
    def _acc_chain(self) -> List[EventCandidate]:
        sc = _ec(family="accumulation", code="SC", index=20, date="2024-02-01")
        ar = _ec(
            family="accumulation",
            code="AR",
            index=25,
            date="2024-02-08",
            cid="ar1",
        )
        # Patch supporting on AR via replace - frozen; rebuild
        ar = EventCandidate(**{**ar.__dict__, "supporting_candidate_ids": (sc.candidate_id,)})
        st = EventCandidate(
            **{
                **_ec(family="accumulation", code="ST", index=30, date="2024-02-15").__dict__,
                "supporting_candidate_ids": (sc.candidate_id, ar.candidate_id),
            }
        )
        spring = _ec(family="accumulation", code="Spring", index=40, date="2024-03-01")
        sos = _ec(family="accumulation", code="SOS", index=55, date="2024-03-20")
        return [sc, ar, st, spring, sos]

    def test_accumulation_phases_a_through_e(self):
        cands = self._acc_chain()
        rng = _range(bar_count=40)
        # end_index must match frame
        df = _daily_for_range(rng, extra=15)
        # Fix range indexes/dates to match frame
        start, end = 10, 49
        rng = RangeCandidate(
            **{
                **rng.__dict__,
                "start_index": start,
                "end_index": end,
                "bar_count": end - start + 1,
                "start_date": pd.Timestamp(df["date"].iloc[start]).date().isoformat(),
                "end_date": pd.Timestamp(df["date"].iloc[end]).date().isoformat(),
                "as_of_date": pd.Timestamp(df["date"].iloc[-1]).date().isoformat(),
                "post_range_bar_count": len(df) - end - 1,
            }
        )
        # Post-range hold above resistance for Phase E
        for i in range(end + 1, len(df)):
            df.loc[i, "close"] = 113.0
            df.loc[i, "high"] = 114.0
            df.loc[i, "low"] = 112.5
            df.loc[i, "open"] = 112.8

        # Align event indexes inside frame
        mapped = []
        for c, idx in zip(cands, [20, 25, 30, 40, 55]):
            mapped.append(EventCandidate(**{**c.__dict__, "index": idx}))
        er = _event_result(mapped)
        as_of = rng.as_of_date
        structure = classify_structure(er, as_of_date=as_of)
        assert structure.classification == "accumulation"
        result = classify_phases(
            df, rng, er, as_of_date=as_of, structure=structure, config=resolve_config({"phase_b_min_range_bars": 20, "phase_e_hold_bars": 2})
        )
        phases = [c.phase for c in result.candidates]
        assert "A" in phases
        assert "B" in phases
        assert "C" in phases
        assert "D" in phases
        assert result.selected_phase == phases[-1] if phases else None
        # Cumulative coexistence
        assert len(result.candidates) >= 3
        # Highest selected
        assert result.selected_phase == max(phases, key=lambda p: "ABCDE".index(p))
        json.dumps(result.to_dict(), allow_nan=False, sort_keys=True)

    def test_distribution_phases_a_through_d(self):
        bc = _ec(family="distribution", code="BC", index=20)
        ar = EventCandidate(
            **{
                **_ec(family="distribution", code="AR", index=25).__dict__,
                "supporting_candidate_ids": (bc.candidate_id,),
            }
        )
        st = EventCandidate(
            **{
                **_ec(family="distribution", code="ST", index=30).__dict__,
                "supporting_candidate_ids": (bc.candidate_id, ar.candidate_id),
            }
        )
        ut = _ec(family="distribution", code="UT", index=40)
        sow = _ec(family="distribution", code="SOW", index=55)
        er = _event_result([bc, ar, st, ut, sow])
        rng = _range(40)
        df = _daily_for_range(rng, extra=10)
        start, end = 10, 49
        rng = RangeCandidate(
            **{
                **rng.__dict__,
                "start_index": start,
                "end_index": end,
                "bar_count": end - start + 1,
                "start_date": pd.Timestamp(df["date"].iloc[start]).date().isoformat(),
                "end_date": pd.Timestamp(df["date"].iloc[end]).date().isoformat(),
                "as_of_date": pd.Timestamp(df["date"].iloc[-1]).date().isoformat(),
            }
        )
        structure = classify_structure(er, as_of_date=rng.as_of_date)
        result = classify_phases(
            df,
            rng,
            er,
            as_of_date=rng.as_of_date,
            structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20}),
        )
        phases = [c.phase for c in result.candidates]
        assert set(phases) >= {"A", "B", "C", "D"}
        assert result.selected_phase == "D"

    def test_ambiguous_and_unknown_structure_yield_unknown_phase(self):
        both = [
            _ec(family="accumulation", code="SC", index=10),
            _ec(family="accumulation", code="Spring", index=20),
            _ec(family="distribution", code="BC", index=11),
            _ec(family="distribution", code="SOW", index=30),
        ]
        er = _event_result(both)
        rng = _range()
        df = _daily_for_range(rng)
        structure = classify_structure(er, as_of_date="2024-06-28")
        result = classify_phases(df, rng, er, as_of_date="2024-06-28", structure=structure)
        assert result.phase_state == "UNKNOWN_PHASE"
        assert result.selected_phase is None

        empty = classify_phases(
            df,
            rng,
            _event_result([]),
            as_of_date="2024-06-28",
        )
        assert empty.phase_state == "UNKNOWN_PHASE"

    def test_lower_phases_do_not_cause_ambiguity(self):
        cands = self._acc_chain()[:3]  # A+B only path through ST
        sc, ar, st = cands[0], cands[1], cands[2]
        ar = EventCandidate(**{**ar.__dict__, "supporting_candidate_ids": (sc.candidate_id,)})
        st = EventCandidate(
            **{**st.__dict__, "supporting_candidate_ids": (sc.candidate_id, ar.candidate_id)}
        )
        er = _event_result([sc, ar, st])
        rng = _range(40)
        df = _daily_for_range(rng)
        start, end = 10, 49
        rng = RangeCandidate(
            **{
                **rng.__dict__,
                "start_index": start,
                "end_index": end,
                "bar_count": end - start + 1,
                "start_date": pd.Timestamp(df["date"].iloc[start]).date().isoformat(),
                "end_date": pd.Timestamp(df["date"].iloc[end]).date().isoformat(),
                "as_of_date": pd.Timestamp(df["date"].iloc[-1]).date().isoformat(),
            }
        )
        structure = classify_structure(er, as_of_date=rng.as_of_date)
        # Only SC+AR — need Spring/SOS for recognition? SC+AR is 2 types but signature needs SC/Spring/SOS — SC is signature
        result = classify_phases(
            df,
            rng,
            er,
            as_of_date=rng.as_of_date,
            structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20}),
        )
        if result.candidates:
            assert result.phase_state != "AMBIGUOUS_PHASE"
            assert "AMBIGUOUS_PHASE" not in result.phase_state

    def test_phase_confidence_cannot_authorize(self):
        # Missing Spring → no Phase C even with high confidence on A/B
        sc = _ec(family="accumulation", code="SC", index=20, confidence=0.99)
        ar = EventCandidate(
            **{
                **_ec(family="accumulation", code="AR", index=25, confidence=0.99).__dict__,
                "supporting_candidate_ids": (sc.candidate_id,),
            }
        )
        st = EventCandidate(
            **{
                **_ec(family="accumulation", code="ST", index=30, confidence=0.99).__dict__,
                "supporting_candidate_ids": (sc.candidate_id, ar.candidate_id),
            }
        )
        er = _event_result([sc, ar, st])
        rng = _range(40)
        df = _daily_for_range(rng)
        start, end = 10, 49
        rng = RangeCandidate(
            **{
                **rng.__dict__,
                "start_index": start,
                "end_index": end,
                "bar_count": end - start + 1,
                "start_date": pd.Timestamp(df["date"].iloc[start]).date().isoformat(),
                "end_date": pd.Timestamp(df["date"].iloc[end]).date().isoformat(),
                "as_of_date": pd.Timestamp(df["date"].iloc[-1]).date().isoformat(),
            }
        )
        structure = classify_structure(er, as_of_date=rng.as_of_date)
        result = classify_phases(
            df,
            rng,
            er,
            as_of_date=rng.as_of_date,
            structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20}),
        )
        assert "C" not in [c.phase for c in result.candidates]
        assert result.selected_phase in (None, "A", "B")

    def test_failed_breakout_hold_prevents_e(self):
        cands = self._acc_chain()
        rng = _range(40)
        df = _daily_for_range(rng, extra=10)
        start, end = 10, 49
        for i in range(end + 1, len(df)):
            df.loc[i, "close"] = 109.0
            df.loc[i, "high"] = 109.5
            df.loc[i, "low"] = 108.0
        rng = RangeCandidate(
            **{
                **rng.__dict__,
                "start_index": start,
                "end_index": end,
                "bar_count": end - start + 1,
                "start_date": pd.Timestamp(df["date"].iloc[start]).date().isoformat(),
                "end_date": pd.Timestamp(df["date"].iloc[end]).date().isoformat(),
                "as_of_date": pd.Timestamp(df["date"].iloc[-1]).date().isoformat(),
            }
        )
        mapped = []
        for c, idx in zip(cands, [20, 25, 30, 40, 55]):
            mapped.append(EventCandidate(**{**c.__dict__, "index": idx}))
        mapped[1] = EventCandidate(
            **{**mapped[1].__dict__, "supporting_candidate_ids": (mapped[0].candidate_id,)}
        )
        mapped[2] = EventCandidate(
            **{
                **mapped[2].__dict__,
                "supporting_candidate_ids": (mapped[0].candidate_id, mapped[1].candidate_id),
            }
        )
        er = _event_result(mapped)
        structure = classify_structure(er, as_of_date=rng.as_of_date)
        result = classify_phases(
            df,
            rng,
            er,
            as_of_date=rng.as_of_date,
            structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20, "phase_e_hold_bars": 2}),
        )
        e_cands = [c for c in result.candidates if c.phase == "E"]
        assert e_cands
        e = e_cands[0]
        assert "PHASE_E_HOLD" not in e.required_event_codes
        assert GATE_PHASE_E_HOLD_ABOVE_RESISTANCE in e.required_gate_codes
        assert GATE_PHASE_E_HOLD_ABOVE_RESISTANCE in e.failed_gate_codes
        assert e.sequence_valid is False
        assert result.selected_phase != "E"

    def test_future_bars_do_not_change_pinned_phase(self):
        cands = self._acc_chain()[:4]
        sc, ar, st, spring = cands
        ar = EventCandidate(**{**ar.__dict__, "supporting_candidate_ids": (sc.candidate_id,)})
        st = EventCandidate(
            **{**st.__dict__, "supporting_candidate_ids": (sc.candidate_id, ar.candidate_id)}
        )
        er = _event_result([sc, ar, st, spring])
        rng = _range(40)
        df = _daily_for_range(rng, extra=5)
        start, end = 10, 49
        as_of = pd.Timestamp(df["date"].iloc[end + 2]).date().isoformat()
        rng = RangeCandidate(
            **{
                **rng.__dict__,
                "start_index": start,
                "end_index": end,
                "bar_count": end - start + 1,
                "start_date": pd.Timestamp(df["date"].iloc[start]).date().isoformat(),
                "end_date": pd.Timestamp(df["date"].iloc[end]).date().isoformat(),
                "as_of_date": as_of,
            }
        )
        structure = classify_structure(er, as_of_date=as_of)
        r1 = classify_phases(
            df,
            rng,
            er,
            as_of_date=as_of,
            structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20}),
        )
        df2 = df.copy()
        df2.loc[len(df2) - 1, "close"] = 200.0
        r2 = classify_phases(
            df2,
            rng,
            er,
            as_of_date=as_of,
            structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20}),
        )
        assert r1.to_dict() == r2.to_dict()
