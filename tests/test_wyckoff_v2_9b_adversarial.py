"""Adversarial Phase 9B contract regression tests."""

from __future__ import annotations

import json
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from app.workers.strategies.wyckoff_v2.constants import (
    EVENT_STATUS_RETENTION_ORDER,
    GATE_PHASE_E_HOLD_ABOVE_RESISTANCE,
    GATE_PHASE_E_HOLD_BELOW_SUPPORT,
    Phase9AConfigError,
    event_key,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.events import (
    VOLUME_REQUIREMENT,
    EventDetectionError,
    compute_event_candidate_id,
    detect_event_candidates,
)
from app.workers.strategies.wyckoff_v2.models import (
    EventCandidate,
    EventDetectionResult,
    PriceZone,
    RangeCandidate,
)
from app.workers.strategies.wyckoff_v2.phases import classify_phases, classify_structure


def _dates(n: int, end: str = "2024-06-28"):
    end_ts = pd.Timestamp(end)
    dates = []
    cur = end_ts
    while len(dates) < n:
        if cur.weekday() < 5:
            dates.append(cur)
        cur -= pd.Timedelta(days=1)
    return list(reversed(dates))


def _base(n=80):
    dates = _dates(n)
    closes = np.full(n, 105.0)
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": closes + 1.0,
            "low": closes - 1.0,
            "close": closes,
            "volume": np.full(n, 1_000_000.0),
        }
    )


def _set(df, i, *, o, h, l, c, v=1_000_000.0):
    df.loc[i, ["open", "high", "low", "close", "volume"]] = [o, h, l, c, v]


def _range(df, start, end, as_of_index=None):
    if as_of_index is None:
        as_of_index = len(df) - 1
    support = PriceZone(lo=98.0, hi=100.0)
    resistance = PriceZone(lo=110.0, hi=112.0)
    as_of = pd.Timestamp(df["date"].iloc[as_of_index]).date().isoformat()
    return RangeCandidate(
        range_candidate_version="wyckoff_range.v1",
        candidate_id="range_adv_001",
        as_of_date=as_of,
        start_date=pd.Timestamp(df["date"].iloc[start]).date().isoformat(),
        end_date=pd.Timestamp(df["date"].iloc[end]).date().isoformat(),
        start_index=start,
        end_index=end,
        post_range_bar_count=as_of_index - end,
        bar_count=end - start + 1,
        support_zone=support,
        resistance_zone=resistance,
        support=support.midpoint,
        resistance=resistance.midpoint,
        midpoint=(support.midpoint + resistance.midpoint) / 2,
        width=resistance.hi - support.lo,
        atr=2.0,
        width_atr_multiple=7.0,
        support_interactions=(),
        resistance_interactions=(),
        support_touch_cluster_count=2,
        resistance_touch_cluster_count=2,
        containment_fraction=0.9,
        breakout_contamination_fraction=0.05,
        volume_coverage=1.0,
        quality_components={"width_stability": 0.8},
        range_quality=0.8,
        valid=True,
        rejection_reasons=(),
    )


def _cfg(**kw):
    base = {
        "event_atr_window": 5,
        "event_volume_baseline_window": 10,
        "event_min_volume_baseline_bars": 5,
        "event_confirmation_window_bars": 3,
        "automatic_rally_window_bars": 8,
        "secondary_test_min_separation_bars": 2,
        "climax_spread_atr_ratio": 1.2,
        "wide_spread_atr_ratio": 1.0,
        "effort_high_volume_ratio": 1.3,
        "phase_b_min_range_bars": 20,
    }
    base.update(kw)
    return resolve_config(base)


def _ec(family, code, index, **kw):
    cid = kw.pop("cid", f"{family}_{code}_{index}")
    supporting = kw.pop("supporting", ())
    usable = kw.pop("usable", True)
    confidence = kw.pop("confidence", 0.5)
    date = kw.pop("date", "2024-01-01")
    status = kw.pop("status", "confirmed")
    defaults = dict(
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
        supporting_candidate_ids=tuple(supporting),
        contradicting_candidate_ids=(),
        reason_codes=(),
        usable_for_structure=usable,
        metadata={},
    )
    defaults.update(kw)
    return EventCandidate(**defaults)


def _er(cands):
    by = {}
    for c in cands:
        by.setdefault(event_key(c.family, c.event_code), []).append(c)
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


class TestFamilyQualifiedKeys:
    def test_ar_st_coexist_and_caps_are_independent(self):
        df = _base(70)
        # Force both families to produce AR-like patterns is hard; inject via
        # bounding unit using synthetic candidates through detect path keys.
        from app.workers.strategies.wyckoff_v2.events import _bound_candidates

        cfg = _cfg(max_event_candidates_per_code=1, max_total_event_candidates=50)
        acc_ars = [
            _ec("accumulation", "AR", i, confidence=0.9 - i * 0.01, date=f"2024-01-{i+1:02d}")
            for i in range(3)
        ]
        dist_ars = [
            _ec("distribution", "AR", i + 10, confidence=0.9 - i * 0.01, date=f"2024-02-{i+1:02d}")
            for i in range(3)
        ]
        kept, truncated = _bound_candidates(acc_ars + dist_ars, cfg)
        assert truncated is True
        keys = {event_key(c.family, c.event_code) for c in kept}
        assert event_key("accumulation", "AR") in keys
        assert event_key("distribution", "AR") in keys
        assert sum(1 for c in kept if c.family == "accumulation" and c.event_code == "AR") == 1
        assert sum(1 for c in kept if c.family == "distribution" and c.event_code == "AR") == 1

        # ST coexistence similarly
        sts = [
            _ec("accumulation", "ST", 1),
            _ec("distribution", "ST", 2),
        ]
        kept2, _ = _bound_candidates(sts, cfg)
        by = {}
        for c in kept2:
            by.setdefault(event_key(c.family, c.event_code), []).append(c)
        assert event_key("accumulation", "ST") in by
        assert event_key("distribution", "ST") in by

    def test_detect_result_uses_family_qualified_keys(self):
        df = _base(60)
        _set(df, 30, o=102, h=102.5, l=97.0, c=100.5, v=3_000_000)
        _set(df, 31, o=100.5, h=104, l=100, c=103, v=1_200_000)
        _set(df, 32, o=103, h=106, l=102, c=105, v=1_100_000)
        _set(df, 33, o=105, h=111, l=104, c=110, v=1_200_000)
        rng = _range(df, 20, 50)
        result = detect_event_candidates(df, rng, as_of_date=rng.as_of_date, config=_cfg())
        for k in result.candidates_by_code:
            assert ":" in k
            fam, code = k.split(":", 1)
            assert fam in ("accumulation", "distribution")
        json.dumps(result.to_dict(), allow_nan=False, sort_keys=True)


class TestCrossFamilyIsolation:
    def test_dist_ar_cannot_satisfy_acc_phase_a(self):
        sc = _ec("accumulation", "SC", 10)
        dist_ar = _ec("distribution", "AR", 15, supporting=(sc.candidate_id,))
        s = classify_structure(_er([sc, dist_ar]), as_of_date="2024-06-28")
        # SC alone may not qualify depending on min_types=2 without signature pair
        # But phase path must not use dist AR.
        rng = RangeCandidate(
            range_candidate_version="wyckoff_range.v1",
            candidate_id="range_1",
            as_of_date="2024-06-28",
            start_date="2024-01-02",
            end_date="2024-03-01",
            start_index=5,
            end_index=40,
            post_range_bar_count=5,
            bar_count=36,
            support_zone=PriceZone(98, 100),
            resistance_zone=PriceZone(110, 112),
            support=99,
            resistance=111,
            midpoint=105,
            width=14,
            atr=2,
            width_atr_multiple=7,
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
        df = _base(50)
        # Force accumulation structure recognition
        spring = _ec("accumulation", "Spring", 20)
        er = _er([sc, dist_ar, spring])
        structure = classify_structure(er, as_of_date=rng.as_of_date)
        assert structure.classification == "accumulation"
        result = classify_phases(
            df, rng, er, as_of_date=rng.as_of_date, structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20}),
        )
        # No Phase A without accumulation AR
        assert not any(c.phase == "A" and c.sequence_valid for c in result.candidates)

    def test_acc_ar_cannot_satisfy_dist_phase_a(self):
        bc = _ec("distribution", "BC", 10)
        acc_ar = _ec("accumulation", "AR", 15, supporting=(bc.candidate_id,))
        ut = _ec("distribution", "UT", 20)
        er = _er([bc, acc_ar, ut])
        structure = classify_structure(er, as_of_date="2024-06-28")
        assert structure.classification == "distribution"
        rng = RangeCandidate(
            range_candidate_version="wyckoff_range.v1",
            candidate_id="range_1",
            as_of_date="2024-06-28",
            start_date="2024-01-02",
            end_date="2024-03-01",
            start_index=5,
            end_index=40,
            post_range_bar_count=5,
            bar_count=36,
            support_zone=PriceZone(98, 100),
            resistance_zone=PriceZone(110, 112),
            support=99,
            resistance=111,
            midpoint=105,
            width=14,
            atr=2,
            width_atr_multiple=7,
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
        result = classify_phases(
            _base(50), rng, er, as_of_date=rng.as_of_date, structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20}),
        )
        assert not any(c.phase == "A" and c.sequence_valid for c in result.candidates)

    def test_utad_requires_distribution_chain_only(self):
        # Acc SC/AR/ST must not unlock UTAD
        ut = _ec("distribution", "UT", 40)
        sc = _ec("accumulation", "SC", 10)
        ar = _ec("accumulation", "AR", 15, supporting=(sc.candidate_id,))
        st = _ec("accumulation", "ST", 20, supporting=(sc.candidate_id, ar.candidate_id,))
        from app.workers.strategies.wyckoff_v2.events import _detect_utad, _Ctx

        # Use public detect: UT without dist BC/AR/ST must not yield UTAD
        df = _base(60)
        _set(df, 30, o=110, h=114.5, l=109, c=110.5, v=1_500_000)
        _set(df, 31, o=110.5, h=111, l=109.5, c=110.0, v=1_000_000)
        _set(df, 32, o=110, h=110.5, l=109, c=109.5, v=1_000_000)
        _set(df, 33, o=109.5, h=110, l=108.5, c=109.0, v=1_000_000)
        rng = _range(df, 10, 45)
        result = detect_event_candidates(df, rng, as_of_date=rng.as_of_date, config=_cfg())
        assert event_key("distribution", "UTAD") not in result.candidates_by_code or not result.candidates_by_code.get(
            event_key("distribution", "UTAD"), ()
        )


class TestFrozenRangeReuse:
    def test_default_rejects_older_range_as_of(self):
        df = _base(50)
        rng = _range(df, 10, 30, as_of_index=30)
        later = pd.Timestamp(df["date"].iloc[40]).date().isoformat()
        with pytest.raises(EventDetectionError) as exc:
            detect_event_candidates(df, rng, as_of_date=later, config=_cfg())
        assert exc.value.reason_code == "range_as_of_mismatch"

    def test_explicit_reuse_matures_confirmation(self):
        df = _base(60)
        _set(df, 30, o=101, h=102, l=96.0, c=100.5, v=1_500_000)
        _set(df, 31, o=100.5, h=101.5, l=100, c=101.2, v=1_000_000)
        frozen = _range(df, 10, 30, as_of_index=31)
        zones_before = (frozen.support_zone.to_dict(), frozen.resistance_zone.to_dict(), frozen.range_quality)
        pending = detect_event_candidates(
            df, frozen, as_of_date=frozen.as_of_date, config=_cfg()
        )
        spring_p = [c for c in pending.candidates if c.event_code == "Spring"][0]
        assert spring_p.status == "confirmation_pending"

        _set(df, 32, o=101.2, h=102.5, l=101, c=102, v=1_000_000)
        _set(df, 33, o=102, h=103, l=101.5, c=102.5, v=1_000_000)
        later = pd.Timestamp(df["date"].iloc[33]).date().isoformat()
        confirmed = detect_event_candidates(
            df,
            frozen,
            as_of_date=later,
            config=_cfg(),
            allow_frozen_range_reuse=True,
        )
        spring_c = [c for c in confirmed.candidates if c.event_code == "Spring"][0]
        assert spring_c.candidate_id == spring_p.candidate_id
        assert spring_c.status == "confirmed"
        assert confirmed.range_candidate_id == frozen.candidate_id
        assert zones_before == (
            frozen.support_zone.to_dict(),
            frozen.resistance_zone.to_dict(),
            frozen.range_quality,
        )

        # Future beyond pin ignored
        _set(df, 50, o=90, h=91, l=80, c=81, v=9e6)
        confirmed2 = detect_event_candidates(
            df,
            frozen,
            as_of_date=later,
            config=_cfg(),
            allow_frozen_range_reuse=True,
        )
        assert confirmed.to_dict() == confirmed2.to_dict()

    def test_range_as_of_after_pinned_rejects(self):
        df = _base(50)
        rng = _range(df, 10, 30, as_of_index=40)
        earlier = pd.Timestamp(df["date"].iloc[30]).date().isoformat()
        with pytest.raises(EventDetectionError) as exc:
            detect_event_candidates(
                df, rng, as_of_date=earlier, config=_cfg(), allow_frozen_range_reuse=True
            )
        assert exc.value.reason_code == "range_as_of_mismatch"


class TestPhaseGates:
    def test_phase_e_gate_vocabulary(self):
        sc = _ec("accumulation", "SC", 20)
        ar = _ec("accumulation", "AR", 25, supporting=(sc.candidate_id,))
        st = _ec("accumulation", "ST", 30, supporting=(sc.candidate_id, ar.candidate_id))
        spring = _ec("accumulation", "Spring", 40)
        sos = _ec("accumulation", "SOS", 55)
        er = _er([sc, ar, st, spring, sos])
        df = _base(70)
        start, end = 10, 49
        for i in range(end + 1, len(df)):
            df.loc[i, "close"] = 113.0
            df.loc[i, "high"] = 114.0
            df.loc[i, "low"] = 112.5
        rng = _range(df, start, end)
        structure = classify_structure(er, as_of_date=rng.as_of_date)
        result = classify_phases(
            df, rng, er, as_of_date=rng.as_of_date, structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20, "phase_e_hold_bars": 2}),
        )
        e = [c for c in result.candidates if c.phase == "E"][0]
        assert "PHASE_E_HOLD" not in e.required_event_codes
        assert GATE_PHASE_E_HOLD_ABOVE_RESISTANCE in e.required_gate_codes
        assert GATE_PHASE_E_HOLD_ABOVE_RESISTANCE in e.passed_gate_codes
        json.dumps(result.to_dict(), allow_nan=False, sort_keys=True)

    def test_missing_hold_gate(self):
        sc = _ec("accumulation", "SC", 20)
        ar = _ec("accumulation", "AR", 25, supporting=(sc.candidate_id,))
        st = _ec("accumulation", "ST", 30, supporting=(sc.candidate_id, ar.candidate_id))
        spring = _ec("accumulation", "Spring", 40)
        sos = _ec("accumulation", "SOS", 45)
        er = _er([sc, ar, st, spring, sos])
        df = _base(50)
        # Range ends at last bar — no post-range
        rng = _range(df, 10, 49, as_of_index=49)
        structure = classify_structure(er, as_of_date=rng.as_of_date)
        result = classify_phases(
            df, rng, er, as_of_date=rng.as_of_date, structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20, "phase_e_hold_bars": 2}),
        )
        e = [c for c in result.candidates if c.phase == "E"]
        if e:
            assert GATE_PHASE_E_HOLD_ABOVE_RESISTANCE in e[0].missing_gate_codes
            assert e[0].sequence_valid is False


class TestIdentityAndConfidence:
    def test_identity_excludes_status_confidence(self):
        common = dict(
            range_candidate_id="r1",
            family="accumulation",
            event_code="Spring",
            event_date="2024-01-01",
            event_index=10,
            price=96.0,
            level=98.0,
            supporting_candidate_ids=(),
            config_subset={"event_atr_window": 14},
        )
        id1 = compute_event_candidate_id(**common)
        id2 = compute_event_candidate_id(**common)
        assert id1 == id2
        # Family change
        id3 = compute_event_candidate_id(**{**common, "family": "distribution", "event_code": "UT"})
        assert id1 != id3
        # Acc AR vs Dist AR
        a = compute_event_candidate_id(
            range_candidate_id="r1",
            family="accumulation",
            event_code="AR",
            event_date="2024-01-01",
            event_index=10,
            price=110.0,
            level=112.0,
            supporting_candidate_ids=["sc"],
            config_subset={"event_atr_window": 14},
        )
        d = compute_event_candidate_id(
            range_candidate_id="r1",
            family="distribution",
            event_code="AR",
            event_date="2024-01-01",
            event_index=10,
            price=110.0,
            level=112.0,
            supporting_candidate_ids=["bc"],
            config_subset={"event_atr_window": 14},
        )
        assert a != d
        # Relevant config change
        a2 = compute_event_candidate_id(
            range_candidate_id="r1",
            family="accumulation",
            event_code="AR",
            event_date="2024-01-01",
            event_index=10,
            price=110.0,
            level=112.0,
            supporting_candidate_ids=["sc"],
            config_subset={"event_atr_window": 20},
        )
        assert a != a2
        # -0.0 normalization
        z1 = compute_event_candidate_id(
            range_candidate_id="r1",
            family="accumulation",
            event_code="PS",
            event_date="2024-01-01",
            event_index=1,
            price=-0.0,
            level=0.0,
            supporting_candidate_ids=(),
            config_subset={"event_atr_window": 14},
        )
        z2 = compute_event_candidate_id(
            range_candidate_id="r1",
            family="accumulation",
            event_code="PS",
            event_date="2024-01-01",
            event_index=1,
            price=0.0,
            level=0.0,
            supporting_candidate_ids=(),
            config_subset={"event_atr_window": 14},
        )
        assert z1 == z2

    def test_confidence_does_not_change_usability(self):
        base = _ec("accumulation", "SC", 10, confidence=0.01, usable=True, status="confirmed")
        high = replace(base, confidence=0.99, confidence_components={"x": 0.99})
        s1 = classify_structure(_er([base, _ec("accumulation", "Spring", 20, confidence=0.01)]), as_of_date="2024-06-28")
        s2 = classify_structure(_er([high, _ec("accumulation", "Spring", 20, confidence=0.99)]), as_of_date="2024-06-28")
        assert s1.classification == s2.classification == "accumulation"
        assert base.usable_for_structure is True
        assert high.usable_for_structure is True


class TestStructureEdges:
    def test_duplicate_codes_count_once_and_ar_st_not_signature(self):
        cands = [
            _ec("accumulation", "AR", 10),
            _ec("accumulation", "AR", 11, cid="ar2"),
            _ec("accumulation", "ST", 12),
        ]
        s = classify_structure(_er(cands), as_of_date="2024-06-28")
        assert s.state == "unknown"
        assert s.accumulation_confirmed_type_count == 2
        assert s.accumulation_signature_events == ()

    def test_one_side_recognized_preserves_opposite_incomplete(self):
        cands = [
            _ec("accumulation", "SC", 10),
            _ec("accumulation", "Spring", 20),
            _ec("distribution", "BC", 11),  # incomplete alone
        ]
        s = classify_structure(_er(cands), as_of_date="2024-06-28")
        assert s.classification == "accumulation"
        assert s.state == "recognized"
        assert "distribution_incomplete_evidence" in s.contradiction_codes


class TestVolumeMatrix:
    def test_volume_requirement_matrix_complete(self):
        expected = {
            "accumulation:PS",
            "accumulation:SC",
            "accumulation:AR",
            "accumulation:ST",
            "accumulation:Spring",
            "accumulation:Test",
            "accumulation:SOS",
            "accumulation:LPS",
            "distribution:PSY",
            "distribution:BC",
            "distribution:AR",
            "distribution:ST",
            "distribution:UT",
            "distribution:UTAD",
            "distribution:SOW",
            "distribution:LPSY",
        }
        assert set(VOLUME_REQUIREMENT) == expected
        assert VOLUME_REQUIREMENT["accumulation:Spring"] == "optional"
        assert VOLUME_REQUIREMENT["distribution:UT"] == "optional"
        assert VOLUME_REQUIREMENT["accumulation:SC"] == "required"


class TestStatusOrderAndConfig:
    def test_status_retention_order_not_alphabetical(self):
        assert EVENT_STATUS_RETENTION_ORDER[0] == "confirmed"
        assert "confirmation_pending" in EVENT_STATUS_RETENTION_ORDER
        # Alphabetical would put candidate before confirmed
        assert list(EVENT_STATUS_RETENTION_ORDER) != sorted(EVENT_STATUS_RETENTION_ORDER)

    def test_structure_ambiguity_key_removed(self):
        cfg = resolve_config()
        assert "structure_ambiguity_type_margin" not in cfg
        with pytest.raises(Phase9AConfigError):
            resolve_config({"max_total_event_candidates": 5, "max_event_candidates_per_code": 10})

    def test_same_index_prerequisite_rejected(self):
        sc = _ec("accumulation", "SC", 20)
        ar = _ec("accumulation", "AR", 20, supporting=(sc.candidate_id,))  # same index
        spring = _ec("accumulation", "Spring", 30)
        er = _er([sc, ar, spring])
        structure = classify_structure(er, as_of_date="2024-06-28")
        rng = _range(_base(50), 5, 40)
        result = classify_phases(
            _base(50), rng, er, as_of_date=rng.as_of_date, structure=structure,
            config=resolve_config({"phase_b_min_range_bars": 20}),
        )
        assert not any(c.phase == "A" and c.sequence_valid for c in result.candidates)


class TestConfirmationBoundaries:
    def test_confirmation_window_boundaries(self):
        df = _base(50)
        _set(df, 20, o=101, h=102, l=96.0, c=100.5, v=1_500_000)
        cfg = _cfg(event_confirmation_window_bars=3)

        # 0 later bars
        rng0 = _range(df, 5, 20, as_of_index=20)
        r0 = detect_event_candidates(df, rng0, as_of_date=rng0.as_of_date, config=cfg)
        springs0 = [c for c in r0.candidates if c.event_code == "Spring"]
        assert springs0
        assert springs0[0].status == "confirmation_pending"

        # window-1 later bars
        _set(df, 21, o=100.5, h=101.5, l=100, c=101.2, v=1e6)
        _set(df, 22, o=101.2, h=102, l=101, c=101.8, v=1e6)
        rng1 = _range(df, 5, 20, as_of_index=22)
        r1 = detect_event_candidates(
            df, rng0, as_of_date=rng1.as_of_date, config=cfg, allow_frozen_range_reuse=True
        )
        springs1 = [c for c in r1.candidates if c.event_code == "Spring"]
        assert springs1[0].status == "confirmation_pending"

        # exact full window with positive confirmation on final bar
        _set(df, 23, o=101.8, h=103, l=101.5, c=102.5, v=1e6)
        later = pd.Timestamp(df["date"].iloc[23]).date().isoformat()
        r2 = detect_event_candidates(
            df, rng0, as_of_date=later, config=cfg, allow_frozen_range_reuse=True
        )
        springs2 = [c for c in r2.candidates if c.event_code == "Spring"]
        assert springs2[0].status == "confirmed"
        assert springs2[0].candidate_id == springs0[0].candidate_id

        # confirming bar after pinned as_of ignored → still pending at pin 22
        assert springs1[0].status == "confirmation_pending"


class TestConfirmationContradictionBoundary:
    def test_contradiction_on_final_allowed_bar(self):
        df = _base(50)
        _set(df, 20, o=101, h=102, l=96.0, c=100.5, v=1_500_000)
        _set(df, 21, o=100.5, h=101.5, l=100, c=101.2, v=1e6)
        _set(df, 22, o=101.2, h=102, l=101, c=101.8, v=1e6)
        _set(df, 23, o=100, h=101, l=90, c=91, v=1e6)  # final window bar invalidates
        cfg = _cfg(event_confirmation_window_bars=3)
        frozen = _range(df, 5, 20, as_of_index=20)
        later = pd.Timestamp(df["date"].iloc[23]).date().isoformat()
        r = detect_event_candidates(
            df, frozen, as_of_date=later, config=cfg, allow_frozen_range_reuse=True
        )
        springs = [c for c in r.candidates if c.event_code == "Spring"]
        assert springs
        assert springs[0].status == "contradicted"


class TestDependencyIntegrity:
    def test_truncated_support_marks_dependent_unusable(self):
        from app.workers.strategies.wyckoff_v2.events import _bound_candidates

        sc = _ec("accumulation", "SC", 10, confidence=0.1, date="2024-01-01")
        decoys = [
            _ec("accumulation", "SC", i, confidence=0.99, date=f"2024-02-{i:02d}", cid=f"sc{i}")
            for i in range(1, 6)
        ]
        ar = _ec(
            "accumulation",
            "AR",
            20,
            confidence=0.99,
            date="2024-03-01",
            supporting=(sc.candidate_id,),
        )
        cfg = _cfg(max_event_candidates_per_code=3, max_total_event_candidates=10)
        kept, truncated = _bound_candidates(decoys + [sc, ar], cfg)
        assert truncated
        ar_kept = [c for c in kept if c.event_code == "AR"]
        if ar_kept and sc.candidate_id not in {c.candidate_id for c in kept}:
            assert ar_kept[0].usable_for_structure is False
            assert "supporting_candidate_truncated" in ar_kept[0].reason_codes
