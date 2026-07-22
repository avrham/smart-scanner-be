"""Wyckoff event-candidate detection — wyckoff_events.v1 (Phase 9B).

All event names are candidates, not textbook certainty. Confidence is
ranking evidence only and never authorizes usable_for_structure or a
structure classification. Confirmation may use bars after the event only
through the pinned as_of_date.

Pure functions only — no I/O, providers, DB or LLM.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from app.workers.provenance import _sha256, canonical_json
from app.workers.strategies.wyckoff_v2.constants import (
    EVENT_CANDIDATE_VERSION,
    EVENT_CONFIG_KEYS,
    EVENT_DETECTION_VERSION,
    EVENT_STATUS_RETENTION_ORDER,
    event_key,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.effort_result import (
    measure_effort_result_at_index,
)
from app.workers.strategies.wyckoff_v2.models import (
    EventCandidate,
    EventDetectionResult,
    EffortResultMeasurement,
    RangeCandidate,
)


class EventDetectionError(ValueError):
    """Deterministic rejection of malformed event-detection input."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


# ---- Labels / vocab ------------------------------------------------------- #

_ACC = "accumulation"
_DIST = "distribution"

_LABELS = {
    (_ACC, "PS"): "ps_candidate",
    (_ACC, "SC"): "selling_climax_candidate",
    (_ACC, "AR"): "automatic_rally_candidate",
    (_ACC, "ST"): "secondary_test_candidate",
    (_ACC, "Spring"): "spring_candidate",
    (_ACC, "Test"): "test_candidate",
    (_ACC, "SOS"): "sos_candidate",
    (_ACC, "LPS"): "lps_candidate",
    (_DIST, "PSY"): "psy_candidate",
    (_DIST, "BC"): "buying_climax_candidate",
    (_DIST, "AR"): "automatic_reaction_candidate",
    (_DIST, "ST"): "secondary_test_candidate",
    (_DIST, "UT"): "ut_candidate",
    (_DIST, "UTAD"): "utad_candidate",
    (_DIST, "SOW"): "sow_candidate",
    (_DIST, "LPSY"): "lpsy_candidate",
}

_STATUS_RANK = {
    status: idx for idx, status in enumerate(EVENT_STATUS_RETENTION_ORDER)
}

# Volume requirement matrix (approved Phase 9B contract).
# required  -> missing volume => status=unknown, usable=false
# optional  -> price event may confirm; volume_quality/confidence may be null
# comparative -> when peer volume known, measured volume required for comparison
VOLUME_REQUIREMENT = {
    event_key(_ACC, "PS"): "required",
    event_key(_ACC, "SC"): "required",
    event_key(_ACC, "AR"): "optional",
    event_key(_ACC, "ST"): "comparative",
    event_key(_ACC, "Spring"): "optional",
    event_key(_ACC, "Test"): "comparative",
    event_key(_ACC, "SOS"): "required",
    event_key(_ACC, "LPS"): "comparative",
    event_key(_DIST, "PSY"): "required",
    event_key(_DIST, "BC"): "required",
    event_key(_DIST, "AR"): "optional",
    event_key(_DIST, "ST"): "comparative",
    event_key(_DIST, "UT"): "optional",
    event_key(_DIST, "UTAD"): "optional",
    event_key(_DIST, "SOW"): "required",
    event_key(_DIST, "LPSY"): "comparative",
}


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return pd.Timestamp(value).date()


def _bar_date_iso(value: Any) -> str:
    return _as_date(value).isoformat()


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _norm_float(value: float) -> float:
    f = float(value)
    if not math.isfinite(f):
        raise EventDetectionError("non_finite_value", "non-finite in candidate_id")
    return 0.0 if f == 0.0 else f


def _norm_num(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if isinstance(value, int) and not isinstance(value, bool):
            return int(value)
        return _norm_float(value)
    if isinstance(value, np.generic):
        return _norm_num(value.item())
    return value


def _geom_mean(components: Dict[str, Optional[float]]) -> Optional[float]:
    vals = list(components.values())
    if any(v is None for v in vals):
        return None
    if any(float(v) <= 0.0 for v in vals):  # type: ignore[arg-type]
        return 0.0
    prod = 1.0
    for v in vals:
        prod *= float(v)  # type: ignore[arg-type]
    return float(prod ** (1.0 / len(vals)))


def _truncate(daily: pd.DataFrame, as_of_date: Any) -> pd.DataFrame:
    pinned = _as_date(as_of_date)
    mask = daily["date"].map(_as_date) <= pinned
    return daily.loc[mask].copy().reset_index(drop=True)


def _event_config_subset(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {k: cfg[k] for k in EVENT_CONFIG_KEYS if k in cfg}


def compute_event_candidate_id(
    *,
    range_candidate_id: str,
    family: str,
    event_code: str,
    event_date: str,
    event_index: int,
    price: float,
    level: Optional[float],
    supporting_candidate_ids: Sequence[str],
    config_subset: Dict[str, Any],
) -> str:
    """Identity of the underlying candidate (confirmation/confidence excluded)."""
    payload = {
        "event_detection_version": EVENT_DETECTION_VERSION,
        "range_candidate_id": str(range_candidate_id),
        "family": str(family),
        "event_code": str(event_code),
        "date": str(event_date),
        "index": int(event_index),
        "price": _norm_float(price),
        "level": None if level is None else _norm_float(level),
        "supporting_candidate_ids": sorted(str(x) for x in supporting_candidate_ids),
        "config": {str(k): _norm_num(v) for k, v in sorted(config_subset.items())},
    }
    return _sha256(canonical_json(payload))


def _validate_range_frame(
    frame: pd.DataFrame,
    selected_range: RangeCandidate,
    as_of_date: Any,
    *,
    allow_frozen_range_reuse: bool = False,
) -> None:
    """Validate selected range against the canonical pinned frame.

    Default (allow_frozen_range_reuse=False):
      range.as_of_date must equal pinned as_of_date.

    Explicit reuse (allow_frozen_range_reuse=True):
      range.as_of_date may be earlier than pinned as_of for confirmation
      maturation analysis. Zones/identity are trusted frozen inputs and are
      not recalculated. Phase 9C normal orchestration must use same-as-of
      inputs; frozen reuse is for future shadow/maturation workflows only.
    """
    pinned = _as_date(as_of_date).isoformat()
    range_as_of = str(selected_range.as_of_date)

    if not selected_range.candidate_id:
        raise EventDetectionError(
            "missing_frozen_identity",
            "selected range missing candidate_id",
        )

    if range_as_of > pinned:
        raise EventDetectionError(
            "range_as_of_mismatch",
            "selected range as_of is after pinned as_of",
        )

    if not allow_frozen_range_reuse and range_as_of != pinned:
        raise EventDetectionError(
            "range_as_of_mismatch",
            "selected range as_of must equal pinned as_of "
            "(set allow_frozen_range_reuse=True for explicit maturation reuse)",
        )

    if selected_range.end_date > range_as_of:
        raise EventDetectionError(
            "range_end_after_range_as_of",
            "selected range end_date is after range as_of_date",
        )

    if selected_range.end_index >= len(frame):
        raise EventDetectionError(
            "range_index_mismatch",
            "selected range end_index outside truncated frame",
        )
    if (
        selected_range.start_index < 0
        or selected_range.start_index > selected_range.end_index
    ):
        raise EventDetectionError(
            "range_index_mismatch", "invalid selected range indexes"
        )
    start_d = _bar_date_iso(frame.iloc[selected_range.start_index]["date"])
    end_d = _bar_date_iso(frame.iloc[selected_range.end_index]["date"])
    if start_d != selected_range.start_date or end_d != selected_range.end_date:
        raise EventDetectionError(
            "range_index_mismatch",
            "selected-range indexes do not match canonical frame dates",
        )


# ---- Zone / relationship helpers ------------------------------------------ #

def _zone_intersect(low: float, high: float, lo: float, hi: float) -> bool:
    return low <= hi and high >= lo


def _approach_support(
    low: float, support_lo: float, support_hi: float, atr: float, mult: float
) -> bool:
    if atr is None or atr <= 0:
        return False
    return _zone_intersect(low, low, support_lo, support_hi) or (
        low > support_hi and low <= support_hi + mult * atr
    )


def _approach_resistance(
    high: float, res_lo: float, res_hi: float, atr: float, mult: float
) -> bool:
    if atr is None or atr <= 0:
        return False
    return _zone_intersect(high, high, res_lo, res_hi) or (
        high < res_lo and high >= res_lo - mult * atr
    )


def _quality_vs_threshold(value: Optional[float], threshold: float) -> Optional[float]:
    if value is None or threshold <= 0:
        return None
    return _clip01(float(value) / float(threshold))


def _confirmation_window(
    frame: pd.DataFrame, event_index: int, window: int
) -> pd.DataFrame:
    start = event_index + 1
    end = min(len(frame), event_index + 1 + window)
    return frame.iloc[start:end]


def _apply_confirmation_accum_climax_like(
    frame: pd.DataFrame,
    event_index: int,
    event_low: float,
    event_close: float,
    inval_buffer: float,
    window: int,
) -> Tuple[str, Optional[str], Tuple[str, ...]]:
    """SC/Spring confirmation pattern."""
    conf = _confirmation_window(frame, event_index, window)
    if len(conf) < window:
        end_d = _bar_date_iso(conf.iloc[-1]["date"]) if len(conf) else None
        return "confirmation_pending", end_d, ("confirmation_window_incomplete",)
    floor = event_low - inval_buffer
    closes = conf["close"].astype(float)
    if (closes < floor).any():
        return "contradicted", _bar_date_iso(conf.iloc[-1]["date"]), ("confirmation_invalidated",)
    if not (closes > event_close).any():
        return "confirmation_pending", _bar_date_iso(conf.iloc[-1]["date"]), (
            "confirmation_recovery_missing",
        )
    return "confirmed", _bar_date_iso(conf.iloc[-1]["date"]), ()


def _apply_confirmation_dist_climax_like(
    frame: pd.DataFrame,
    event_index: int,
    event_high: float,
    event_close: float,
    inval_buffer: float,
    window: int,
) -> Tuple[str, Optional[str], Tuple[str, ...]]:
    conf = _confirmation_window(frame, event_index, window)
    if len(conf) < window:
        end_d = _bar_date_iso(conf.iloc[-1]["date"]) if len(conf) else None
        return "confirmation_pending", end_d, ("confirmation_window_incomplete",)
    ceiling = event_high + inval_buffer
    closes = conf["close"].astype(float)
    if (closes > ceiling).any():
        return "contradicted", _bar_date_iso(conf.iloc[-1]["date"]), ("confirmation_invalidated",)
    if not (closes < event_close).any():
        return "confirmation_pending", _bar_date_iso(conf.iloc[-1]["date"]), (
            "confirmation_recovery_missing",
        )
    return "confirmed", _bar_date_iso(conf.iloc[-1]["date"]), ()


def _apply_confirmation_sos(
    frame: pd.DataFrame,
    event_index: int,
    res_hi: float,
    res_lo: float,
    window: int,
) -> Tuple[str, Optional[str], Tuple[str, ...]]:
    conf = _confirmation_window(frame, event_index, window)
    if len(conf) < window:
        end_d = _bar_date_iso(conf.iloc[-1]["date"]) if len(conf) else None
        return "confirmation_pending", end_d, ("confirmation_window_incomplete",)
    closes = conf["close"].astype(float)
    if (closes < res_lo).any():
        return "contradicted", _bar_date_iso(conf.iloc[-1]["date"]), ("sos_failed_hold",)
    if not (closes > res_hi).any():
        return "confirmation_pending", _bar_date_iso(conf.iloc[-1]["date"]), (
            "sos_hold_missing",
        )
    return "confirmed", _bar_date_iso(conf.iloc[-1]["date"]), ()


def _apply_confirmation_sow(
    frame: pd.DataFrame,
    event_index: int,
    sup_lo: float,
    sup_hi: float,
    window: int,
) -> Tuple[str, Optional[str], Tuple[str, ...]]:
    conf = _confirmation_window(frame, event_index, window)
    if len(conf) < window:
        end_d = _bar_date_iso(conf.iloc[-1]["date"]) if len(conf) else None
        return "confirmation_pending", end_d, ("confirmation_window_incomplete",)
    closes = conf["close"].astype(float)
    if (closes > sup_hi).any():
        return "contradicted", _bar_date_iso(conf.iloc[-1]["date"]), ("sow_failed_hold",)
    if not (closes < sup_lo).any():
        return "confirmation_pending", _bar_date_iso(conf.iloc[-1]["date"]), (
            "sow_hold_missing",
        )
    return "confirmed", _bar_date_iso(conf.iloc[-1]["date"]), ()


def _build_candidate(
    *,
    range_candidate_id: str,
    family: str,
    event_code: str,
    date_s: str,
    index: int,
    as_of_date: str,
    price: float,
    level: Optional[float],
    direction: str,
    status: str,
    confirmation_status: str,
    confirmation_end_date: Optional[str],
    range_relationship: str,
    effort: EffortResultMeasurement,
    required_gate_results: Dict[str, bool],
    confidence_components: Dict[str, Optional[float]],
    supporting: Sequence[str],
    contradicting: Sequence[str],
    reason_codes: Sequence[str],
    metadata: Dict[str, Any],
    config_subset: Dict[str, Any],
) -> EventCandidate:
    gates_ok = all(required_gate_results.values()) if required_gate_results else False
    usable = bool(gates_ok and status == "confirmed")
    conf = _geom_mean(confidence_components)
    cid = compute_event_candidate_id(
        range_candidate_id=range_candidate_id,
        family=family,
        event_code=event_code,
        event_date=date_s,
        event_index=index,
        price=price,
        level=level,
        supporting_candidate_ids=supporting,
        config_subset=config_subset,
    )
    return EventCandidate(
        event_candidate_version=EVENT_CANDIDATE_VERSION,
        candidate_id=cid,
        range_candidate_id=range_candidate_id,
        family=family,
        event_code=event_code,
        event_label=_LABELS[(family, event_code)],
        date=date_s,
        index=int(index),
        timeframe="1d",
        as_of_date=as_of_date,
        price=float(price),
        level=None if level is None else float(level),
        direction=direction,
        status=status,
        confirmation_status=confirmation_status,
        confirmation_end_date=confirmation_end_date,
        range_relationship=range_relationship,
        effort_result=effort.to_dict(),
        required_gate_results=dict(required_gate_results),
        confidence_components=dict(confidence_components),
        confidence=conf,
        supporting_candidate_ids=tuple(supporting),
        contradicting_candidate_ids=tuple(contradicting),
        reason_codes=tuple(reason_codes),
        usable_for_structure=usable,
        metadata=dict(metadata),
    )


class _Ctx:
    """Shared detection context for one range/as_of pass."""

    def __init__(
        self,
        frame: pd.DataFrame,
        selected_range: RangeCandidate,
        cfg: Dict[str, Any],
        as_of: str,
    ) -> None:
        self.frame = frame
        self.rng = selected_range
        self.cfg = cfg
        self.as_of = as_of
        self.config_subset = _event_config_subset(cfg)
        self._effort: Dict[int, EffortResultMeasurement] = {}
        self.rejections: Counter = Counter()
        self.support = selected_range.support_zone
        self.resistance = selected_range.resistance_zone
        self.mid = selected_range.midpoint
        self.start = selected_range.start_index
        self.end = selected_range.end_index

    def effort(self, i: int) -> EffortResultMeasurement:
        if i not in self._effort:
            self._effort[i] = measure_effort_result_at_index(
                self.frame, i, as_of_date=self.as_of, config=self.cfg
            )
        return self._effort[i]

    def atr(self, i: int) -> Optional[float]:
        return self.effort(i).atr

    def inval(self, i: int) -> float:
        atr = self.atr(i)
        if atr is None:
            return 0.0
        return float(self.cfg["event_invalidation_buffer_atr_multiple"]) * atr

    def approach(self, i: int) -> float:
        atr = self.atr(i)
        if atr is None:
            return 0.0
        return float(self.cfg["event_zone_approach_atr_multiple"]) * atr

    def pierce(self, i: int) -> float:
        atr = self.atr(i)
        if atr is None:
            return 0.0
        return float(self.cfg["event_pierce_atr_multiple"]) * atr

    def breakout_buf(self, i: int) -> float:
        atr = self.atr(i)
        if atr is None:
            return 0.0
        return float(self.cfg["event_breakout_buffer_atr_multiple"]) * atr

    def retest_tol(self, i: int) -> float:
        atr = self.atr(i)
        if atr is None:
            return 0.0
        return float(self.cfg["event_retest_tolerance_atr_multiple"]) * atr


def _missing_vol_candidate(
    ctx: _Ctx,
    *,
    family: str,
    event_code: str,
    i: int,
    price: float,
    level: Optional[float],
    direction: str,
    range_relationship: str,
    gates: Dict[str, bool],
    components: Dict[str, Optional[float]],
    supporting: Sequence[str] = (),
    extra_reasons: Sequence[str] = (),
) -> EventCandidate:
    er = ctx.effort(i)
    reasons = ("missing_volume_evidence",) + tuple(extra_reasons)
    return _build_candidate(
        range_candidate_id=ctx.rng.candidate_id,
        family=family,
        event_code=event_code,
        date_s=_bar_date_iso(ctx.frame.iloc[i]["date"]),
        index=i,
        as_of_date=ctx.as_of,
        price=price,
        level=level,
        direction=direction,
        status="unknown",
        confirmation_status="unknown",
        confirmation_end_date=None,
        range_relationship=range_relationship,
        effort=er,
        required_gate_results=gates,
        confidence_components=components,
        supporting=supporting,
        contradicting=(),
        reason_codes=reasons,
        metadata={"volume_required": True},
        config_subset=ctx.config_subset,
    )


# ---- Accumulation detectors ----------------------------------------------- #

def _detect_ps(ctx: _Ctx) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    for i in range(ctx.start, ctx.end + 1):
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        low = float(row["low"])
        high = float(row["high"])
        close = float(row["close"])
        atr = er.atr
        if atr is None or atr <= 0:
            ctx.rejections["ps_missing_atr"] += 1
            continue
        downward = (
            er.directional_result_pct is not None and er.directional_result_pct < 0
        ) or (er.previous_close is not None and close < er.previous_close)
        if not downward:
            ctx.rejections["ps_not_downward"] += 1
            continue
        near = _approach_support(
            low, ctx.support.lo, ctx.support.hi, atr, float(ctx.cfg["event_zone_approach_atr_multiple"])
        )
        if not near:
            ctx.rejections["ps_not_near_support"] += 1
            continue
        floor = ctx.support.lo - ctx.inval(i)
        close_ok = close >= floor
        spread_ok = er.spread_atr_ratio is not None and er.spread_atr_ratio >= 1.0
        vol_known = er.relative_volume is not None
        vol_ok = vol_known and er.relative_volume >= 1.0
        gates = {
            "downward_bar": downward,
            "near_support": near,
            "close_not_invalidated": close_ok,
            "spread_ok": bool(spread_ok),
            "volume_ok": bool(vol_ok),
        }
        comps = {
            "range_relationship_quality": _clip01(
                1.0 - abs(low - ctx.support.midpoint) / max(atr, 1e-12)
            ),
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, 1.0),
            "volume_quality": _quality_vs_threshold(er.relative_volume, 1.0),
        }
        if not vol_known:
            out.append(
                _missing_vol_candidate(
                    ctx,
                    family=_ACC,
                    event_code="PS",
                    i=i,
                    price=low,
                    level=ctx.support.lo,
                    direction="down",
                    range_relationship="approach_support",
                    gates=gates,
                    components=comps,
                )
            )
            continue
        if not (close_ok and spread_ok and vol_ok):
            ctx.rejections["ps_gates_failed"] += 1
            continue
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_ACC,
                event_code="PS",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=low,
                level=ctx.support.lo,
                direction="down",
                status="confirmed",
                confirmation_status="not_required",
                confirmation_end_date=None,
                range_relationship="approach_support",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(),
                contradicting=(),
                reason_codes=(),
                metadata={},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_sc(ctx: _Ctx) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    climax = float(ctx.cfg["climax_spread_atr_ratio"])
    effort_high = float(ctx.cfg["effort_high_volume_ratio"])
    clv_min = float(ctx.cfg["accumulation_close_off_low_min"])
    window = int(ctx.cfg["event_confirmation_window_bars"])
    for i in range(ctx.start, ctx.end + 1):
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        low = float(row["low"])
        close = float(row["close"])
        atr = er.atr
        if atr is None or atr <= 0:
            ctx.rejections["sc_missing_atr"] += 1
            continue
        enters = low <= ctx.support.hi
        downward = er.directional_result_pct is not None and er.directional_result_pct < 0
        spread_ok = er.spread_atr_ratio is not None and er.spread_atr_ratio >= climax
        clv_ok = er.close_location_value is not None and er.close_location_value >= clv_min
        vol_known = er.relative_volume is not None
        vol_ok = vol_known and er.relative_volume >= effort_high
        gates = {
            "enters_support": enters,
            "downward": bool(downward),
            "spread_climax": bool(spread_ok),
            "close_off_low": bool(clv_ok),
            "volume_high": bool(vol_ok),
        }
        comps = {
            "range_relationship_quality": 1.0 if enters else 0.0,
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, climax),
            "volume_quality": _quality_vs_threshold(er.relative_volume, effort_high),
            "close_location_quality": (
                None
                if er.close_location_value is None
                else _clip01(er.close_location_value)
            ),
            "confirmation_quality": None,
        }
        if not (enters and downward and spread_ok and clv_ok):
            ctx.rejections["sc_price_gates_failed"] += 1
            continue
        if not vol_known:
            out.append(
                _missing_vol_candidate(
                    ctx,
                    family=_ACC,
                    event_code="SC",
                    i=i,
                    price=low,
                    level=ctx.support.lo,
                    direction="down",
                    range_relationship="pierce_support",
                    gates=gates,
                    components=comps,
                )
            )
            continue
        if not vol_ok:
            ctx.rejections["sc_volume_gate_failed"] += 1
            continue
        status, end_d, reasons = _apply_confirmation_accum_climax_like(
            ctx.frame, i, low, close, ctx.inval(i), window
        )
        comps["confirmation_quality"] = (
            1.0 if status == "confirmed" else (0.0 if status == "contradicted" else None)
        )
        # Gates for usable exclude confirmation from required_gate_results price set;
        # usable requires status confirmed separately.
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_ACC,
                event_code="SC",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=low,
                level=ctx.support.lo,
                direction="down",
                status=status,
                confirmation_status=status,
                confirmation_end_date=end_d,
                range_relationship="pierce_support",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(),
                contradicting=(),
                reason_codes=reasons,
                metadata={},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _usable(
    cands: Sequence[EventCandidate], code: str, *, family: str
) -> List[EventCandidate]:
    """Family-scoped usable confirmed candidates for a detector code."""
    return [
        c
        for c in cands
        if c.family == family
        and c.event_code == code
        and c.usable_for_structure
        and c.status == "confirmed"
    ]


def _detect_ar_acc(ctx: _Ctx, scs: Sequence[EventCandidate]) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    window = int(ctx.cfg["automatic_rally_window_bars"])
    for sc in _usable(scs, "SC", family=_ACC):
        start = sc.index + 1
        end = min(ctx.end, sc.index + window)
        if start > end:
            ctx.rejections["ar_no_window"] += 1
            continue
        # Deterministic: highest high, tie earliest index
        best_i = None
        best_high = None
        for i in range(start, end + 1):
            h = float(ctx.frame.iloc[i]["high"])
            if best_high is None or h > best_high or (h == best_high and i < best_i):  # type: ignore[operator]
                best_high = h
                best_i = i
        if best_i is None:
            continue
        i = best_i
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        high = float(row["high"])
        close = float(row["close"])
        reaches = high >= ctx.resistance.lo or close > ctx.mid
        pos = er.directional_result_pct is not None and er.directional_result_pct > 0
        gates = {
            "reaches_resistance_or_mid": reaches,
            "positive_directional": bool(pos),
            "after_sc": True,
        }
        if not (reaches and pos):
            ctx.rejections["ar_acc_gates_failed"] += 1
            continue
        comps = {
            "range_relationship_quality": 1.0 if reaches else 0.0,
            "sequence_quality": 1.0,
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, 1.0),
        }
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_ACC,
                event_code="AR",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=high,
                level=ctx.resistance.hi,
                direction="up",
                status="confirmed",
                confirmation_status="not_required",
                confirmation_end_date=None,
                range_relationship="rally_to_resistance",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(sc.candidate_id,),
                contradicting=(),
                reason_codes=(),
                metadata={"sc_index": sc.index},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_st_acc(
    ctx: _Ctx, scs: Sequence[EventCandidate], ars: Sequence[EventCandidate]
) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    min_sep = int(ctx.cfg["secondary_test_min_separation_bars"])
    max_after = int(ctx.cfg["secondary_test_max_bars_after_climax"])
    for sc in _usable(scs, "SC", family=_ACC):
        sc_er = ctx.effort(sc.index)
        usable_ars = [
            a for a in _usable(ars, "AR", family=_ACC) if a.index > sc.index and sc.candidate_id in a.supporting_candidate_ids
        ]
        if not usable_ars:
            continue
        ar = min(usable_ars, key=lambda x: x.index)
        for i in range(ar.index + 1, ctx.end + 1):
            sep = i - sc.index
            if sep < min_sep:
                continue
            if sep > max_after:
                break
            er = ctx.effort(i)
            row = ctx.frame.iloc[i]
            low = float(row["low"])
            close = float(row["close"])
            atr = er.atr
            if atr is None or atr <= 0:
                continue
            near = _approach_support(
                low,
                ctx.support.lo,
                ctx.support.hi,
                atr,
                float(ctx.cfg["event_zone_approach_atr_multiple"]),
            )
            if not near:
                continue
            sc_spread = sc_er.spread_atr_ratio
            narrower = (
                er.spread_atr_ratio is not None
                and sc_spread is not None
                and er.spread_atr_ratio < sc_spread
            )
            vol_known = er.relative_volume is not None and sc_er.relative_volume is not None
            lower_vol = (
                vol_known and er.relative_volume < sc_er.relative_volume  # type: ignore[operator]
            )
            close_ok = close >= float(sc.price) - ctx.inval(i)
            gates = {
                "near_support": near,
                "spread_narrower": bool(narrower),
                "volume_lower": bool(lower_vol),
                "close_ok": close_ok,
            }
            comps = {
                "range_relationship_quality": 1.0 if near else 0.0,
                "spread_quality": 1.0 if narrower else 0.0,
                "volume_quality": None if not vol_known else (1.0 if lower_vol else 0.0),
                "sequence_quality": 1.0,
            }
            if not (near and narrower and close_ok):
                ctx.rejections["st_acc_price_gates_failed"] += 1
                continue
            if not vol_known:
                out.append(
                    _missing_vol_candidate(
                        ctx,
                        family=_ACC,
                        event_code="ST",
                        i=i,
                        price=low,
                        level=ctx.support.lo,
                        direction="down",
                        range_relationship="retest_support",
                        gates=gates,
                        components=comps,
                        supporting=(sc.candidate_id, ar.candidate_id),
                    )
                )
                continue
            if not lower_vol:
                ctx.rejections["st_acc_volume_not_lower"] += 1
                continue
            out.append(
                _build_candidate(
                    range_candidate_id=ctx.rng.candidate_id,
                    family=_ACC,
                    event_code="ST",
                    date_s=_bar_date_iso(row["date"]),
                    index=i,
                    as_of_date=ctx.as_of,
                    price=low,
                    level=ctx.support.lo,
                    direction="down",
                    status="confirmed",
                    confirmation_status="not_required",
                    confirmation_end_date=None,
                    range_relationship="retest_support",
                    effort=er,
                    required_gate_results=gates,
                    confidence_components=comps,
                    supporting=(sc.candidate_id, ar.candidate_id),
                    contradicting=(),
                    reason_codes=(),
                    metadata={},
                    config_subset=ctx.config_subset,
                )
            )
    return out


def _detect_spring(ctx: _Ctx) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    clv_min = float(ctx.cfg["bullish_close_location_min"])
    window = int(ctx.cfg["event_confirmation_window_bars"])
    for i in range(ctx.start, ctx.end + 1):
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        low = float(row["low"])
        close = float(row["close"])
        atr = er.atr
        if atr is None or atr <= 0:
            continue
        pierce = low < ctx.support.lo - float(ctx.cfg["event_pierce_atr_multiple"]) * atr
        back = close >= ctx.support.lo
        below_res = close < ctx.resistance.hi
        clv_ok = er.close_location_value is not None and er.close_location_value >= clv_min
        gates = {
            "pierce_support": pierce,
            "close_back_inside": back,
            "close_below_resistance": below_res,
            "bullish_close_location": bool(clv_ok),
        }
        if not (pierce and back and below_res and clv_ok):
            if pierce and not back:
                ctx.rejections["spring_false_no_close_back"] += 1
            else:
                ctx.rejections["spring_gates_failed"] += 1
            continue
        comps = {
            "range_relationship_quality": 1.0,
            "close_location_quality": _clip01(float(er.close_location_value)),
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, 1.0),
            "volume_quality": _quality_vs_threshold(er.relative_volume, 1.0),
            "confirmation_quality": None,
        }
        status, end_d, reasons = _apply_confirmation_accum_climax_like(
            ctx.frame, i, low, close, ctx.inval(i), window
        )
        comps["confirmation_quality"] = (
            1.0 if status == "confirmed" else (0.0 if status == "contradicted" else None)
        )
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_ACC,
                event_code="Spring",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=low,
                level=ctx.support.lo,
                direction="down",
                status=status,
                confirmation_status=status,
                confirmation_end_date=end_d,
                range_relationship="spring_pierce",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(),
                contradicting=(),
                reason_codes=reasons,
                metadata={},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_test(ctx: _Ctx, springs: Sequence[EventCandidate]) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    max_after = int(ctx.cfg["test_max_bars_after_spring"])
    for spring in _usable(springs, "Spring", family=_ACC):
        spring_er = ctx.effort(spring.index)
        for i in range(spring.index + 1, min(ctx.end, spring.index + max_after) + 1):
            er = ctx.effort(i)
            row = ctx.frame.iloc[i]
            low = float(row["low"])
            atr = er.atr
            if atr is None:
                continue
            near = abs(low - float(spring.price)) <= ctx.retest_tol(i)
            not_violate = low >= float(spring.price) - ctx.inval(i)
            narrower = (
                er.spread_atr_ratio is not None
                and spring_er.spread_atr_ratio is not None
                and er.spread_atr_ratio < spring_er.spread_atr_ratio
            )
            vol_both = er.relative_volume is not None and spring_er.relative_volume is not None
            lower_vol = (
                (not vol_both)
                or (er.relative_volume < spring_er.relative_volume)  # type: ignore[operator]
            )
            # When both known, require lower; when unknown on test and spring known → missing
            gates = {
                "near_spring_low": near,
                "not_violate_spring": not_violate,
                "spread_narrower": bool(narrower),
                "volume_lower_or_unknown_ok": bool(lower_vol),
            }
            if not (near and not_violate and narrower):
                if near and not not_violate:
                    ctx.rejections["test_violates_spring_low"] += 1
                else:
                    ctx.rejections["test_gates_failed"] += 1
                continue
            comps = {
                "range_relationship_quality": 1.0 if near else 0.0,
                "spread_quality": 1.0 if narrower else 0.0,
                "volume_quality": (
                    None
                    if not vol_both
                    else (1.0 if er.relative_volume < spring_er.relative_volume else 0.0)  # type: ignore[operator]
                ),
                "sequence_quality": 1.0,
            }
            if spring_er.relative_volume is not None and er.relative_volume is None:
                out.append(
                    _missing_vol_candidate(
                        ctx,
                        family=_ACC,
                        event_code="Test",
                        i=i,
                        price=low,
                        level=float(spring.price),
                        direction="down",
                        range_relationship="test_spring",
                        gates=gates,
                        components=comps,
                        supporting=(spring.candidate_id,),
                    )
                )
                continue
            if vol_both and not (er.relative_volume < spring_er.relative_volume):  # type: ignore[operator]
                ctx.rejections["test_volume_not_lower"] += 1
                continue
            out.append(
                _build_candidate(
                    range_candidate_id=ctx.rng.candidate_id,
                    family=_ACC,
                    event_code="Test",
                    date_s=_bar_date_iso(row["date"]),
                    index=i,
                    as_of_date=ctx.as_of,
                    price=low,
                    level=float(spring.price),
                    direction="down",
                    status="confirmed",
                    confirmation_status="not_required",
                    confirmation_end_date=None,
                    range_relationship="test_spring",
                    effort=er,
                    required_gate_results=gates,
                    confidence_components=comps,
                    supporting=(spring.candidate_id,),
                    contradicting=(),
                    reason_codes=(),
                    metadata={},
                    config_subset=ctx.config_subset,
                )
            )
    return out


def _detect_sos(ctx: _Ctx) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    wide = float(ctx.cfg["wide_spread_atr_ratio"])
    effort_high = float(ctx.cfg["effort_high_volume_ratio"])
    clv_min = float(ctx.cfg["bullish_close_location_min"])
    window = int(ctx.cfg["event_confirmation_window_bars"])
    # SOS may occur in range or just after — allow through end of frame at as_of
    for i in range(ctx.start, len(ctx.frame)):
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        close = float(row["close"])
        atr = er.atr
        if atr is None or atr <= 0:
            continue
        breakout = close > ctx.resistance.hi + float(ctx.cfg["event_breakout_buffer_atr_multiple"]) * atr
        pos = er.directional_result_pct is not None and er.directional_result_pct > 0
        spread_ok = er.spread_atr_ratio is not None and er.spread_atr_ratio >= wide
        clv_ok = er.close_location_value is not None and er.close_location_value >= clv_min
        vol_known = er.relative_volume is not None
        vol_ok = vol_known and er.relative_volume >= effort_high
        gates = {
            "breakout": breakout,
            "positive": bool(pos),
            "wide_spread": bool(spread_ok),
            "bullish_clv": bool(clv_ok),
            "volume_high": bool(vol_ok),
        }
        if not (breakout and pos and spread_ok and clv_ok):
            if breakout and not (pos and spread_ok and clv_ok):
                ctx.rejections["sos_false"] += 1
            continue
        comps = {
            "range_relationship_quality": 1.0,
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, wide),
            "volume_quality": _quality_vs_threshold(er.relative_volume, effort_high),
            "close_location_quality": (
                None if er.close_location_value is None else _clip01(er.close_location_value)
            ),
            "confirmation_quality": None,
        }
        if not vol_known:
            out.append(
                _missing_vol_candidate(
                    ctx,
                    family=_ACC,
                    event_code="SOS",
                    i=i,
                    price=close,
                    level=ctx.resistance.hi,
                    direction="up",
                    range_relationship="breakout_resistance",
                    gates=gates,
                    components=comps,
                )
            )
            continue
        if not vol_ok:
            ctx.rejections["sos_volume_failed"] += 1
            continue
        status, end_d, reasons = _apply_confirmation_sos(
            ctx.frame, i, ctx.resistance.hi, ctx.resistance.lo, window
        )
        comps["confirmation_quality"] = (
            1.0 if status == "confirmed" else (0.0 if status == "contradicted" else None)
        )
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_ACC,
                event_code="SOS",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=close,
                level=ctx.resistance.hi,
                direction="up",
                status=status,
                confirmation_status=status,
                confirmation_end_date=end_d,
                range_relationship="breakout_resistance",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(),
                contradicting=(),
                reason_codes=reasons,
                metadata={},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_lps(ctx: _Ctx, soss: Sequence[EventCandidate]) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    max_after = int(ctx.cfg["lps_max_bars_after_sos"])
    for sos in _usable(soss, "SOS", family=_ACC):
        sos_er = ctx.effort(sos.index)
        for i in range(sos.index + 1, min(len(ctx.frame) - 1, sos.index + max_after) + 1):
            er = ctx.effort(i)
            row = ctx.frame.iloc[i]
            low = float(row["low"])
            close = float(row["close"])
            atr = er.atr
            if atr is None:
                continue
            near = _approach_resistance(
                low,  # treat low testing former resistance from above
                ctx.resistance.lo,
                ctx.resistance.hi,
                atr,
                float(ctx.cfg["event_retest_tolerance_atr_multiple"]),
            ) or _zone_intersect(low, low, ctx.resistance.lo, ctx.resistance.hi)
            # Also: low approaches resistance from above within retest tol of zone
            near = near or (
                low >= ctx.resistance.lo - ctx.retest_tol(i)
                and low <= ctx.resistance.hi + ctx.retest_tol(i)
            )
            close_ok = close >= ctx.resistance.lo - ctx.inval(i)
            narrower = (
                er.spread_atr_ratio is not None
                and sos_er.spread_atr_ratio is not None
                and er.spread_atr_ratio < sos_er.spread_atr_ratio
            )
            vol_both = er.relative_volume is not None and sos_er.relative_volume is not None
            lower_vol = (not vol_both) or (
                er.relative_volume < sos_er.relative_volume  # type: ignore[operator]
            )
            gates = {
                "retest_resistance": near,
                "close_ok": close_ok,
                "spread_narrower": bool(narrower),
                "volume_ok": bool(lower_vol),
            }
            if not (near and close_ok and narrower):
                continue
            comps = {
                "range_relationship_quality": 1.0,
                "spread_quality": 1.0 if narrower else 0.0,
                "volume_quality": (
                    None
                    if not vol_both
                    else (1.0 if er.relative_volume < sos_er.relative_volume else 0.0)  # type: ignore[operator]
                ),
                "sequence_quality": 1.0,
            }
            if sos_er.relative_volume is not None and er.relative_volume is None:
                out.append(
                    _missing_vol_candidate(
                        ctx,
                        family=_ACC,
                        event_code="LPS",
                        i=i,
                        price=low,
                        level=ctx.resistance.lo,
                        direction="down",
                        range_relationship="lps_retest",
                        gates=gates,
                        components=comps,
                        supporting=(sos.candidate_id,),
                    )
                )
                continue
            if vol_both and not (er.relative_volume < sos_er.relative_volume):  # type: ignore[operator]
                continue
            out.append(
                _build_candidate(
                    range_candidate_id=ctx.rng.candidate_id,
                    family=_ACC,
                    event_code="LPS",
                    date_s=_bar_date_iso(row["date"]),
                    index=i,
                    as_of_date=ctx.as_of,
                    price=low,
                    level=ctx.resistance.lo,
                    direction="down",
                    status="confirmed",
                    confirmation_status="not_required",
                    confirmation_end_date=None,
                    range_relationship="lps_retest",
                    effort=er,
                    required_gate_results=gates,
                    confidence_components=comps,
                    supporting=(sos.candidate_id,),
                    contradicting=(),
                    reason_codes=(),
                    metadata={},
                    config_subset=ctx.config_subset,
                )
            )
    return out


# ---- Distribution detectors ----------------------------------------------- #

def _detect_psy(ctx: _Ctx) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    for i in range(ctx.start, ctx.end + 1):
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        high = float(row["high"])
        close = float(row["close"])
        atr = er.atr
        if atr is None or atr <= 0:
            continue
        upward = (
            er.directional_result_pct is not None and er.directional_result_pct > 0
        ) or (er.previous_close is not None and close > er.previous_close)
        if not upward:
            continue
        near = _approach_resistance(
            high, ctx.resistance.lo, ctx.resistance.hi, atr, float(ctx.cfg["event_zone_approach_atr_multiple"])
        )
        if not near:
            continue
        ceiling = ctx.resistance.hi + ctx.inval(i)
        close_ok = close <= ceiling
        spread_ok = er.spread_atr_ratio is not None and er.spread_atr_ratio >= 1.0
        vol_known = er.relative_volume is not None
        vol_ok = vol_known and er.relative_volume >= 1.0
        gates = {
            "upward_bar": upward,
            "near_resistance": near,
            "close_not_invalidated": close_ok,
            "spread_ok": bool(spread_ok),
            "volume_ok": bool(vol_ok),
        }
        comps = {
            "range_relationship_quality": _clip01(
                1.0 - abs(high - ctx.resistance.midpoint) / max(atr, 1e-12)
            ),
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, 1.0),
            "volume_quality": _quality_vs_threshold(er.relative_volume, 1.0),
        }
        if not vol_known:
            out.append(
                _missing_vol_candidate(
                    ctx,
                    family=_DIST,
                    event_code="PSY",
                    i=i,
                    price=high,
                    level=ctx.resistance.hi,
                    direction="up",
                    range_relationship="approach_resistance",
                    gates=gates,
                    components=comps,
                )
            )
            continue
        if not (close_ok and spread_ok and vol_ok):
            continue
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_DIST,
                event_code="PSY",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=high,
                level=ctx.resistance.hi,
                direction="up",
                status="confirmed",
                confirmation_status="not_required",
                confirmation_end_date=None,
                range_relationship="approach_resistance",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(),
                contradicting=(),
                reason_codes=(),
                metadata={},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_bc(ctx: _Ctx) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    climax = float(ctx.cfg["climax_spread_atr_ratio"])
    effort_high = float(ctx.cfg["effort_high_volume_ratio"])
    clv_max = float(ctx.cfg["distribution_close_off_high_max"])
    window = int(ctx.cfg["event_confirmation_window_bars"])
    for i in range(ctx.start, ctx.end + 1):
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        high = float(row["high"])
        close = float(row["close"])
        atr = er.atr
        if atr is None or atr <= 0:
            continue
        enters = high >= ctx.resistance.lo
        upward = er.directional_result_pct is not None and er.directional_result_pct > 0
        spread_ok = er.spread_atr_ratio is not None and er.spread_atr_ratio >= climax
        clv_ok = er.close_location_value is not None and er.close_location_value <= clv_max
        vol_known = er.relative_volume is not None
        vol_ok = vol_known and er.relative_volume >= effort_high
        gates = {
            "enters_resistance": enters,
            "upward": bool(upward),
            "spread_climax": bool(spread_ok),
            "close_off_high": bool(clv_ok),
            "volume_high": bool(vol_ok),
        }
        if not (enters and upward and spread_ok and clv_ok):
            continue
        comps = {
            "range_relationship_quality": 1.0,
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, climax),
            "volume_quality": _quality_vs_threshold(er.relative_volume, effort_high),
            "close_location_quality": (
                None
                if er.close_location_value is None
                else _clip01(1.0 - er.close_location_value)
            ),
            "confirmation_quality": None,
        }
        if not vol_known:
            out.append(
                _missing_vol_candidate(
                    ctx,
                    family=_DIST,
                    event_code="BC",
                    i=i,
                    price=high,
                    level=ctx.resistance.hi,
                    direction="up",
                    range_relationship="pierce_resistance",
                    gates=gates,
                    components=comps,
                )
            )
            continue
        if not vol_ok:
            continue
        status, end_d, reasons = _apply_confirmation_dist_climax_like(
            ctx.frame, i, high, close, ctx.inval(i), window
        )
        comps["confirmation_quality"] = (
            1.0 if status == "confirmed" else (0.0 if status == "contradicted" else None)
        )
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_DIST,
                event_code="BC",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=high,
                level=ctx.resistance.hi,
                direction="up",
                status=status,
                confirmation_status=status,
                confirmation_end_date=end_d,
                range_relationship="pierce_resistance",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(),
                contradicting=(),
                reason_codes=reasons,
                metadata={},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_ar_dist(ctx: _Ctx, bcs: Sequence[EventCandidate]) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    window = int(ctx.cfg["automatic_rally_window_bars"])
    for bc in _usable(bcs, "BC", family=_DIST):
        start = bc.index + 1
        end = min(ctx.end, bc.index + window)
        if start > end:
            continue
        best_i = None
        best_low = None
        for i in range(start, end + 1):
            l = float(ctx.frame.iloc[i]["low"])
            if best_low is None or l < best_low or (l == best_low and i < best_i):  # type: ignore[operator]
                best_low = l
                best_i = i
        if best_i is None:
            continue
        i = best_i
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        low = float(row["low"])
        close = float(row["close"])
        reaches = low <= ctx.support.hi or close < ctx.mid
        neg = er.directional_result_pct is not None and er.directional_result_pct < 0
        gates = {
            "reaches_support_or_mid": reaches,
            "negative_directional": bool(neg),
            "after_bc": True,
        }
        if not (reaches and neg):
            continue
        comps = {
            "range_relationship_quality": 1.0,
            "sequence_quality": 1.0,
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, 1.0),
        }
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_DIST,
                event_code="AR",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=low,
                level=ctx.support.lo,
                direction="down",
                status="confirmed",
                confirmation_status="not_required",
                confirmation_end_date=None,
                range_relationship="reaction_to_support",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(bc.candidate_id,),
                contradicting=(),
                reason_codes=(),
                metadata={"bc_index": bc.index},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_st_dist(
    ctx: _Ctx, bcs: Sequence[EventCandidate], ars: Sequence[EventCandidate]
) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    min_sep = int(ctx.cfg["secondary_test_min_separation_bars"])
    max_after = int(ctx.cfg["secondary_test_max_bars_after_climax"])
    for bc in _usable(bcs, "BC", family=_DIST):
        bc_er = ctx.effort(bc.index)
        usable_ars = [
            a
            for a in _usable(ars, "AR", family=_DIST)
            if a.index > bc.index and bc.candidate_id in a.supporting_candidate_ids
        ]
        if not usable_ars:
            continue
        ar = min(usable_ars, key=lambda x: x.index)
        for i in range(ar.index + 1, ctx.end + 1):
            sep = i - bc.index
            if sep < min_sep:
                continue
            if sep > max_after:
                break
            er = ctx.effort(i)
            row = ctx.frame.iloc[i]
            high = float(row["high"])
            close = float(row["close"])
            atr = er.atr
            if atr is None:
                continue
            near = _approach_resistance(
                high,
                ctx.resistance.lo,
                ctx.resistance.hi,
                atr,
                float(ctx.cfg["event_zone_approach_atr_multiple"]),
            )
            if not near:
                continue
            narrower = (
                er.spread_atr_ratio is not None
                and bc_er.spread_atr_ratio is not None
                and er.spread_atr_ratio < bc_er.spread_atr_ratio
            )
            vol_known = er.relative_volume is not None and bc_er.relative_volume is not None
            lower_vol = vol_known and er.relative_volume < bc_er.relative_volume  # type: ignore[operator]
            close_ok = close <= float(bc.price) + ctx.inval(i)
            gates = {
                "near_resistance": near,
                "spread_narrower": bool(narrower),
                "volume_lower": bool(lower_vol),
                "close_ok": close_ok,
            }
            if not (near and narrower and close_ok):
                continue
            comps = {
                "range_relationship_quality": 1.0,
                "spread_quality": 1.0 if narrower else 0.0,
                "volume_quality": None if not vol_known else (1.0 if lower_vol else 0.0),
                "sequence_quality": 1.0,
            }
            if not vol_known:
                out.append(
                    _missing_vol_candidate(
                        ctx,
                        family=_DIST,
                        event_code="ST",
                        i=i,
                        price=high,
                        level=ctx.resistance.hi,
                        direction="up",
                        range_relationship="retest_resistance",
                        gates=gates,
                        components=comps,
                        supporting=(bc.candidate_id, ar.candidate_id),
                    )
                )
                continue
            if not lower_vol:
                continue
            out.append(
                _build_candidate(
                    range_candidate_id=ctx.rng.candidate_id,
                    family=_DIST,
                    event_code="ST",
                    date_s=_bar_date_iso(row["date"]),
                    index=i,
                    as_of_date=ctx.as_of,
                    price=high,
                    level=ctx.resistance.hi,
                    direction="up",
                    status="confirmed",
                    confirmation_status="not_required",
                    confirmation_end_date=None,
                    range_relationship="retest_resistance",
                    effort=er,
                    required_gate_results=gates,
                    confidence_components=comps,
                    supporting=(bc.candidate_id, ar.candidate_id),
                    contradicting=(),
                    reason_codes=(),
                    metadata={},
                    config_subset=ctx.config_subset,
                )
            )
    return out


def _detect_ut(ctx: _Ctx) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    clv_max = float(ctx.cfg["bearish_close_location_max"])
    window = int(ctx.cfg["event_confirmation_window_bars"])
    for i in range(ctx.start, ctx.end + 1):
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        high = float(row["high"])
        close = float(row["close"])
        atr = er.atr
        if atr is None or atr <= 0:
            continue
        pierce = high > ctx.resistance.hi + float(ctx.cfg["event_pierce_atr_multiple"]) * atr
        back = close <= ctx.resistance.hi
        above_sup = close > ctx.support.lo
        clv_ok = er.close_location_value is not None and er.close_location_value <= clv_max
        gates = {
            "pierce_resistance": pierce,
            "close_back_inside": back,
            "close_above_support": above_sup,
            "bearish_close_location": bool(clv_ok),
        }
        if not (pierce and back and above_sup and clv_ok):
            if pierce and not back:
                ctx.rejections["ut_false_no_close_back"] += 1
            continue
        comps = {
            "range_relationship_quality": 1.0,
            "close_location_quality": (
                None if er.close_location_value is None else _clip01(1.0 - er.close_location_value)
            ),
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, 1.0),
            "volume_quality": _quality_vs_threshold(er.relative_volume, 1.0),
            "confirmation_quality": None,
        }
        status, end_d, reasons = _apply_confirmation_dist_climax_like(
            ctx.frame, i, high, close, ctx.inval(i), window
        )
        comps["confirmation_quality"] = (
            1.0 if status == "confirmed" else (0.0 if status == "contradicted" else None)
        )
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_DIST,
                event_code="UT",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=high,
                level=ctx.resistance.hi,
                direction="up",
                status=status,
                confirmation_status=status,
                confirmation_end_date=end_d,
                range_relationship="ut_pierce",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(),
                contradicting=(),
                reason_codes=reasons,
                metadata={},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_utad(
    ctx: _Ctx,
    uts: Sequence[EventCandidate],
    bcs: Sequence[EventCandidate],
    ars: Sequence[EventCandidate],
    sts: Sequence[EventCandidate],
) -> List[EventCandidate]:
    """UTAD requires usable UT plus prior BC→AR→ST chronology — never auto-clone UT."""
    out: List[EventCandidate] = []
    usable_bc = _usable(bcs, "BC", family=_DIST)
    usable_ar = _usable(ars, "AR", family=_DIST)
    usable_st = _usable(sts, "ST", family=_DIST)
    for ut in _usable(uts, "UT", family=_DIST):
        bc = next((c for c in usable_bc if c.index < ut.index), None)
        if bc is None:
            ctx.rejections["utad_missing_bc"] += 1
            continue
        ar = next(
            (
                c
                for c in usable_ar
                if c.index > bc.index
                and c.index < ut.index
                and bc.candidate_id in c.supporting_candidate_ids
            ),
            None,
        )
        if ar is None:
            ctx.rejections["utad_missing_ar"] += 1
            continue
        st = next(
            (
                c
                for c in usable_st
                if c.index > ar.index
                and c.index < ut.index
                and bc.candidate_id in c.supporting_candidate_ids
            ),
            None,
        )
        if st is None:
            ctx.rejections["utad_missing_st"] += 1
            continue
        # Same price/status as UT; new identity with supporting chain.
        er = ctx.effort(ut.index)
        comps = dict(ut.confidence_components)
        comps["sequence_quality"] = 1.0
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_DIST,
                event_code="UTAD",
                date_s=ut.date,
                index=ut.index,
                as_of_date=ctx.as_of,
                price=ut.price,
                level=ut.level,
                direction="up",
                status=ut.status,
                confirmation_status=ut.confirmation_status,
                confirmation_end_date=ut.confirmation_end_date,
                range_relationship="utad_pierce",
                effort=er,
                required_gate_results=dict(ut.required_gate_results),
                confidence_components=comps,
                supporting=(ut.candidate_id, bc.candidate_id, ar.candidate_id, st.candidate_id),
                contradicting=(),
                reason_codes=ut.reason_codes,
                metadata={"source_ut_id": ut.candidate_id},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_sow(ctx: _Ctx) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    wide = float(ctx.cfg["wide_spread_atr_ratio"])
    effort_high = float(ctx.cfg["effort_high_volume_ratio"])
    clv_max = float(ctx.cfg["bearish_close_location_max"])
    window = int(ctx.cfg["event_confirmation_window_bars"])
    for i in range(ctx.start, len(ctx.frame)):
        er = ctx.effort(i)
        row = ctx.frame.iloc[i]
        close = float(row["close"])
        atr = er.atr
        if atr is None or atr <= 0:
            continue
        breakout = close < ctx.support.lo - float(ctx.cfg["event_breakout_buffer_atr_multiple"]) * atr
        neg = er.directional_result_pct is not None and er.directional_result_pct < 0
        spread_ok = er.spread_atr_ratio is not None and er.spread_atr_ratio >= wide
        clv_ok = er.close_location_value is not None and er.close_location_value <= clv_max
        vol_known = er.relative_volume is not None
        vol_ok = vol_known and er.relative_volume >= effort_high
        gates = {
            "breakout": breakout,
            "negative": bool(neg),
            "wide_spread": bool(spread_ok),
            "bearish_clv": bool(clv_ok),
            "volume_high": bool(vol_ok),
        }
        if not (breakout and neg and spread_ok and clv_ok):
            if breakout:
                ctx.rejections["sow_false"] += 1
            continue
        comps = {
            "range_relationship_quality": 1.0,
            "spread_quality": _quality_vs_threshold(er.spread_atr_ratio, wide),
            "volume_quality": _quality_vs_threshold(er.relative_volume, effort_high),
            "close_location_quality": (
                None if er.close_location_value is None else _clip01(1.0 - er.close_location_value)
            ),
            "confirmation_quality": None,
        }
        if not vol_known:
            out.append(
                _missing_vol_candidate(
                    ctx,
                    family=_DIST,
                    event_code="SOW",
                    i=i,
                    price=close,
                    level=ctx.support.lo,
                    direction="down",
                    range_relationship="breakout_support",
                    gates=gates,
                    components=comps,
                )
            )
            continue
        if not vol_ok:
            continue
        status, end_d, reasons = _apply_confirmation_sow(
            ctx.frame, i, ctx.support.lo, ctx.support.hi, window
        )
        comps["confirmation_quality"] = (
            1.0 if status == "confirmed" else (0.0 if status == "contradicted" else None)
        )
        out.append(
            _build_candidate(
                range_candidate_id=ctx.rng.candidate_id,
                family=_DIST,
                event_code="SOW",
                date_s=_bar_date_iso(row["date"]),
                index=i,
                as_of_date=ctx.as_of,
                price=close,
                level=ctx.support.lo,
                direction="down",
                status=status,
                confirmation_status=status,
                confirmation_end_date=end_d,
                range_relationship="breakout_support",
                effort=er,
                required_gate_results=gates,
                confidence_components=comps,
                supporting=(),
                contradicting=(),
                reason_codes=reasons,
                metadata={},
                config_subset=ctx.config_subset,
            )
        )
    return out


def _detect_lpsy(ctx: _Ctx, sows: Sequence[EventCandidate]) -> List[EventCandidate]:
    out: List[EventCandidate] = []
    max_after = int(ctx.cfg["lpsy_max_bars_after_sow"])
    for sow in _usable(sows, "SOW", family=_DIST):
        sow_er = ctx.effort(sow.index)
        for i in range(sow.index + 1, min(len(ctx.frame) - 1, sow.index + max_after) + 1):
            er = ctx.effort(i)
            row = ctx.frame.iloc[i]
            high = float(row["high"])
            close = float(row["close"])
            atr = er.atr
            if atr is None:
                continue
            near = (
                high >= ctx.support.lo - ctx.retest_tol(i)
                and high <= ctx.support.hi + ctx.retest_tol(i)
            )
            close_ok = close <= ctx.support.hi + ctx.inval(i)
            narrower = (
                er.spread_atr_ratio is not None
                and sow_er.spread_atr_ratio is not None
                and er.spread_atr_ratio < sow_er.spread_atr_ratio
            )
            vol_both = er.relative_volume is not None and sow_er.relative_volume is not None
            lower_vol = (not vol_both) or (
                er.relative_volume < sow_er.relative_volume  # type: ignore[operator]
            )
            gates = {
                "retest_support": near,
                "close_ok": close_ok,
                "spread_narrower": bool(narrower),
                "volume_ok": bool(lower_vol),
            }
            if not (near and close_ok and narrower):
                continue
            comps = {
                "range_relationship_quality": 1.0,
                "spread_quality": 1.0 if narrower else 0.0,
                "volume_quality": (
                    None
                    if not vol_both
                    else (1.0 if er.relative_volume < sow_er.relative_volume else 0.0)  # type: ignore[operator]
                ),
                "sequence_quality": 1.0,
            }
            if sow_er.relative_volume is not None and er.relative_volume is None:
                out.append(
                    _missing_vol_candidate(
                        ctx,
                        family=_DIST,
                        event_code="LPSY",
                        i=i,
                        price=high,
                        level=ctx.support.hi,
                        direction="up",
                        range_relationship="lpsy_retest",
                        gates=gates,
                        components=comps,
                        supporting=(sow.candidate_id,),
                    )
                )
                continue
            if vol_both and not (er.relative_volume < sow_er.relative_volume):  # type: ignore[operator]
                continue
            out.append(
                _build_candidate(
                    range_candidate_id=ctx.rng.candidate_id,
                    family=_DIST,
                    event_code="LPSY",
                    date_s=_bar_date_iso(row["date"]),
                    index=i,
                    as_of_date=ctx.as_of,
                    price=high,
                    level=ctx.support.hi,
                    direction="up",
                    status="confirmed",
                    confirmation_status="not_required",
                    confirmation_end_date=None,
                    range_relationship="lpsy_retest",
                    effort=er,
                    required_gate_results=gates,
                    confidence_components=comps,
                    supporting=(sow.candidate_id,),
                    contradicting=(),
                    reason_codes=(),
                    metadata={},
                    config_subset=ctx.config_subset,
                )
            )
    return out


# ---- Bounding / ordering -------------------------------------------------- #

def _dedupe_per_bar(cands: List[EventCandidate]) -> List[EventCandidate]:
    """At most one candidate per (family, event_code, bar index, range)."""
    best: Dict[Tuple[str, str, int], EventCandidate] = {}
    for c in cands:
        key = (c.family, c.event_code, c.index)
        prev = best.get(key)
        if prev is None or _rank_tuple(c) < _rank_tuple(prev):
            best[key] = c
    return list(best.values())


def _rank_tuple(c: EventCandidate) -> Tuple:
    conf_key = (
        0 if c.confidence is not None else 1,
        -(c.confidence or 0.0),
    )
    return (
        0 if c.usable_for_structure else 1,
        _STATUS_RANK.get(c.status, 99),
        conf_key,
        c.date,  # for retention sort we use descending date — invert via negation of string not possible; handled in sort key separately
        c.candidate_id,
    )


def _retention_key(c: EventCandidate) -> Tuple:
    return (
        0 if c.usable_for_structure else 1,
        _STATUS_RANK.get(c.status, 99),
        0 if c.confidence is not None else 1,
        -(c.confidence if c.confidence is not None else 0.0),
        "".join(chr(255 - ord(ch)) for ch in c.date),
        c.candidate_id,
    )


def _apply_dependency_integrity(
    cands: List[EventCandidate],
) -> List[EventCandidate]:
    """Mark dependents unusable when supporting IDs were removed by truncation.

    Does not leave dangling supporting references as usable structure evidence.
    """
    retained_ids = {c.candidate_id for c in cands}
    out: List[EventCandidate] = []
    for c in cands:
        missing = [sid for sid in c.supporting_candidate_ids if sid not in retained_ids]
        if not missing:
            out.append(c)
            continue
        reasons = tuple(
            sorted(set(list(c.reason_codes) + ["supporting_candidate_truncated"]))
        )
        out.append(
            EventCandidate(
                **{
                    **c.__dict__,
                    "usable_for_structure": False,
                    "status": "unknown" if c.status == "confirmed" else c.status,
                    "reason_codes": reasons,
                    "metadata": {
                        **dict(c.metadata),
                        "truncated_supporting_ids": list(missing),
                    },
                }
            )
        )
    return out


def _bound_candidates(
    cands: List[EventCandidate], cfg: Dict[str, Any]
) -> Tuple[List[EventCandidate], bool]:
    truncated = False
    per_code = int(cfg["max_event_candidates_per_code"])
    total = int(cfg["max_total_event_candidates"])
    by_code: Dict[str, List[EventCandidate]] = defaultdict(list)
    for c in cands:
        by_code[event_key(c.family, c.event_code)].append(c)
    kept: List[EventCandidate] = []
    for _key, group in sorted(by_code.items()):
        group_sorted = sorted(group, key=_retention_key)
        if len(group_sorted) > per_code:
            truncated = True
            group_sorted = group_sorted[:per_code]
        kept.extend(group_sorted)
    kept_sorted = sorted(kept, key=_retention_key)
    if len(kept_sorted) > total:
        truncated = True
        kept_sorted = kept_sorted[:total]
    kept_sorted = _apply_dependency_integrity(kept_sorted)
    kept_sorted.sort(
        key=lambda c: (c.date, c.index, c.family, c.event_code, c.candidate_id)
    )
    return kept_sorted, truncated


def detect_event_candidates(
    daily: pd.DataFrame,
    selected_range: RangeCandidate,
    *,
    as_of_date: Any,
    config: Optional[Dict[str, Any]] = None,
    allow_frozen_range_reuse: bool = False,
) -> EventDetectionResult:
    """Detect Wyckoff event candidates for one selected trading range.

    Truncates to pinned as_of before any measurement. Never inspects bars
    after as_of. Confidence never gates usable_for_structure.

    By default ``selected_range.as_of_date`` must equal the pinned as_of.
    Set ``allow_frozen_range_reuse=True`` only for explicit confirmation
    maturation over an older frozen range (shadow/analysis path). Phase 9C
    normal orchestration must use same-as-of inputs.
    """
    cfg = resolve_config(config)
    pinned = _as_date(as_of_date)
    as_of_s = pinned.isoformat()
    frame = _truncate(daily, pinned)
    _validate_range_frame(
        frame,
        selected_range,
        pinned,
        allow_frozen_range_reuse=allow_frozen_range_reuse,
    )

    ctx = _Ctx(frame, selected_range, cfg, as_of_s)

    scs = _detect_sc(ctx)
    ps = _detect_ps(ctx)
    ars_acc = _detect_ar_acc(ctx, scs)
    sts_acc = _detect_st_acc(ctx, scs, ars_acc)
    springs = _detect_spring(ctx)
    tests = _detect_test(ctx, springs)
    soss = _detect_sos(ctx)
    lpss = _detect_lps(ctx, soss)

    bcs = _detect_bc(ctx)
    psy = _detect_psy(ctx)
    ars_dist = _detect_ar_dist(ctx, bcs)
    sts_dist = _detect_st_dist(ctx, bcs, ars_dist)
    uts = _detect_ut(ctx)
    utads = _detect_utad(ctx, uts, bcs, ars_dist, sts_dist)
    sows = _detect_sow(ctx)
    lpsys = _detect_lpsy(ctx, sows)

    all_cands = (
        ps
        + scs
        + ars_acc
        + sts_acc
        + springs
        + tests
        + soss
        + lpss
        + psy
        + bcs
        + ars_dist
        + sts_dist
        + uts
        + utads
        + sows
        + lpsys
    )
    deduped = _dedupe_per_bar(all_cands)
    bounded, truncated = _bound_candidates(deduped, cfg)

    by_code: Dict[str, List[EventCandidate]] = defaultdict(list)
    for c in bounded:
        by_code[event_key(c.family, c.event_code)].append(c)

    config_used = dict(ctx.config_subset)
    config_used["max_event_candidates_per_code"] = cfg["max_event_candidates_per_code"]
    config_used["max_total_event_candidates"] = cfg["max_total_event_candidates"]

    return EventDetectionResult(
        event_detection_version=EVENT_DETECTION_VERSION,
        as_of_date=as_of_s,
        range_candidate_id=selected_range.candidate_id,
        candidates=tuple(bounded),
        candidates_by_code={k: tuple(v) for k, v in sorted(by_code.items())},
        rejection_reason_counts=dict(sorted(ctx.rejections.items())),
        candidates_truncated=truncated,
        config_used=config_used,
    )
