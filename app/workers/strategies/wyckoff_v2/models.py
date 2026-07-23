"""Canonical Phase 9A contracts for wyckoff_mtf.v2.

Typed, deterministic, JSON-safe result objects. Internal DataFrames may be
held on the result for downstream Phase 9B/9C pure functions, but every
`to_dict()` surface is strict JSON-safe (no pandas objects, no NaN/Inf).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import math

import pandas as pd

from app.workers.strategies.wyckoff_v2.constants import (
    AGGREGATION_VERSION,
    PHASE_CANDIDATE_VERSION,
    RANGE_CANDIDATE_VERSION,
    RANGE_DETECTION_VERSION,
    READINESS_VERSION,
)


def _require_finite(value: Optional[float], where: str) -> Optional[float]:
    """Reject NaN/Inf rather than serializing them. None stays None."""
    if value is None:
        return None
    f = float(value)
    if not math.isfinite(f):
        raise ValueError(f"non-finite value in {where}: {value!r}")
    return f


def _json_safe(value: Any, where: str = "$") -> Any:
    """Recursively enforce JSON-safe scalars (reject NaN/Inf/pandas)."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non-finite float in {where}: {value!r}")
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v, f"{where}.{k}") for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v, f"{where}[{i}]") for i, v in enumerate(value)]
    if isinstance(value, pd.DataFrame) or isinstance(value, pd.Series):
        raise ValueError(f"pandas object not allowed in JSON surface at {where}")
    raise ValueError(f"non-JSON-safe type at {where}: {type(value).__name__}")


@dataclass(frozen=True)
class PriceZone:
    """Bounded support or resistance zone [lo, hi], inclusive."""

    lo: float
    hi: float

    def __post_init__(self) -> None:
        lo = _require_finite(self.lo, "PriceZone.lo")
        hi = _require_finite(self.hi, "PriceZone.hi")
        if lo is None or hi is None:
            raise ValueError("PriceZone bounds must be finite")
        # Normalize -0.0 → 0.0 so candidate fingerprints are stable.
        lo = 0.0 if lo == 0.0 else lo
        hi = 0.0 if hi == 0.0 else hi
        if lo > hi:
            object.__setattr__(self, "lo", hi)
            object.__setattr__(self, "hi", lo)
        else:
            object.__setattr__(self, "lo", lo)
            object.__setattr__(self, "hi", hi)

    @property
    def midpoint(self) -> float:
        return (self.lo + self.hi) / 2.0

    def to_dict(self) -> Dict[str, float]:
        return {"lo": self.lo, "hi": self.hi}


@dataclass(frozen=True)
class TouchInteraction:
    """One representative bar of an interaction cluster with a zone."""

    date: str
    index: int
    price: float
    zone: str  # "support" | "resistance"
    distance_to_zone_midpoint: float
    cluster_start_date: str
    cluster_end_date: str
    cluster_bar_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "index": int(self.index),
            "price": _require_finite(self.price, "TouchInteraction.price"),
            "zone": self.zone,
            "distance_to_zone_midpoint": _require_finite(
                self.distance_to_zone_midpoint, "TouchInteraction.distance"
            ),
            "cluster_start_date": self.cluster_start_date,
            "cluster_end_date": self.cluster_end_date,
            "cluster_bar_count": int(self.cluster_bar_count),
        }


@dataclass(frozen=True)
class ReadinessResult:
    """Phase 9A data-readiness decision. Never represented as a zero score."""

    readiness_version: str
    ready: bool
    status: str
    reason_codes: Tuple[str, ...]
    latest_bar_completion: Dict[str, Any]
    evaluation_time_utc: Optional[str]
    market_data_as_of: Optional[str]
    desired_history_bars: int
    requested_history_bars: int
    available_input_bars: int
    available_completed_bars: int
    history_depth_capped: bool
    history_depth_complete: bool
    required_monthly_periods: int
    available_completed_monthly_periods: int
    required_weekly_periods: int
    available_completed_weekly_periods: int
    required_daily_structure_bars: int
    usable_volume_bars: int
    required_volume_bars: int
    volume_coverage: Optional[float]
    excluded_partial_daily_bar_date: Optional[str]
    missing_fields: Tuple[str, ...]
    # Internal only — not in to_dict.
    completed_daily_frame: Optional[pd.DataFrame] = field(
        default=None, repr=False, compare=False
    )

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "readiness_version": self.readiness_version,
            "ready": self.ready,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "latest_bar_completion": dict(self.latest_bar_completion),
            "evaluation_time_utc": self.evaluation_time_utc,
            "market_data_as_of": self.market_data_as_of,
            "desired_history_bars": int(self.desired_history_bars),
            "requested_history_bars": int(self.requested_history_bars),
            "available_input_bars": int(self.available_input_bars),
            "available_completed_bars": int(self.available_completed_bars),
            "history_depth_capped": bool(self.history_depth_capped),
            "history_depth_complete": bool(self.history_depth_complete),
            "required_monthly_periods": int(self.required_monthly_periods),
            "available_completed_monthly_periods": int(
                self.available_completed_monthly_periods
            ),
            "required_weekly_periods": int(self.required_weekly_periods),
            "available_completed_weekly_periods": int(
                self.available_completed_weekly_periods
            ),
            "required_daily_structure_bars": int(self.required_daily_structure_bars),
            "usable_volume_bars": int(self.usable_volume_bars),
            "required_volume_bars": int(self.required_volume_bars),
            "volume_coverage": _require_finite(
                self.volume_coverage, "volume_coverage"
            ),
            "excluded_partial_daily_bar_date": self.excluded_partial_daily_bar_date,
            "missing_fields": list(self.missing_fields),
        }
        return _json_safe(payload, "ReadinessResult")


@dataclass(frozen=True)
class CompletedAggregationResult:
    """Completed monthly/weekly frames derived from a completed daily frame."""

    aggregation_version: str
    monthly_frame: pd.DataFrame = field(repr=False, compare=False)
    weekly_frame: pd.DataFrame = field(repr=False, compare=False)
    monthly_completed_periods: int
    weekly_completed_periods: int
    excluded_partial_month_period: Optional[str]
    excluded_partial_week_period: Optional[str]
    latest_completed_daily_date: Optional[str]
    evaluation_session_date: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "aggregation_version": self.aggregation_version,
            "monthly_completed_periods": int(self.monthly_completed_periods),
            "weekly_completed_periods": int(self.weekly_completed_periods),
            "excluded_partial_month_period": self.excluded_partial_month_period,
            "excluded_partial_week_period": self.excluded_partial_week_period,
            "latest_completed_daily_date": self.latest_completed_daily_date,
            "evaluation_session_date": self.evaluation_session_date,
            "monthly_period_dates": [
                pd.to_datetime(d).date().isoformat()
                for d in self.monthly_frame["date"].tolist()
            ]
            if self.monthly_frame is not None and len(self.monthly_frame) > 0
            else [],
            "weekly_period_dates": [
                pd.to_datetime(d).date().isoformat()
                for d in self.weekly_frame["date"].tolist()
            ]
            if self.weekly_frame is not None and len(self.weekly_frame) > 0
            else [],
        }
        return _json_safe(payload, "CompletedAggregationResult")


@dataclass(frozen=True)
class RangeCandidate:
    """One deterministic trading-range candidate ending at or before as_of."""

    range_candidate_version: str
    candidate_id: str
    as_of_date: str
    start_date: str
    end_date: str
    start_index: int
    end_index: int
    post_range_bar_count: int
    bar_count: int
    support_zone: PriceZone
    resistance_zone: PriceZone
    support: float
    resistance: float
    midpoint: float
    width: float
    atr: Optional[float]
    width_atr_multiple: Optional[float]
    support_interactions: Tuple[TouchInteraction, ...]
    resistance_interactions: Tuple[TouchInteraction, ...]
    support_touch_cluster_count: int
    resistance_touch_cluster_count: int
    containment_fraction: Optional[float]
    breakout_contamination_fraction: Optional[float]
    volume_coverage: Optional[float]
    quality_components: Dict[str, Optional[float]]
    range_quality: Optional[float]
    valid: bool
    rejection_reasons: Tuple[str, ...]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "range_candidate_version": self.range_candidate_version,
            "candidate_id": self.candidate_id,
            "as_of_date": self.as_of_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "start_index": int(self.start_index),
            "end_index": int(self.end_index),
            "post_range_bar_count": int(self.post_range_bar_count),
            "bar_count": int(self.bar_count),
            "support_zone": self.support_zone.to_dict(),
            "resistance_zone": self.resistance_zone.to_dict(),
            "support": _require_finite(self.support, "support"),
            "resistance": _require_finite(self.resistance, "resistance"),
            "midpoint": _require_finite(self.midpoint, "midpoint"),
            "width": _require_finite(self.width, "width"),
            "atr": _require_finite(self.atr, "atr"),
            "width_atr_multiple": _require_finite(
                self.width_atr_multiple, "width_atr_multiple"
            ),
            "support_interactions": [t.to_dict() for t in self.support_interactions],
            "resistance_interactions": [
                t.to_dict() for t in self.resistance_interactions
            ],
            "support_touch_cluster_count": int(self.support_touch_cluster_count),
            "resistance_touch_cluster_count": int(
                self.resistance_touch_cluster_count
            ),
            "containment_fraction": _require_finite(
                self.containment_fraction, "containment_fraction"
            ),
            "breakout_contamination_fraction": _require_finite(
                self.breakout_contamination_fraction,
                "breakout_contamination_fraction",
            ),
            "volume_coverage": _require_finite(
                self.volume_coverage, "volume_coverage"
            ),
            "quality_components": {
                k: _require_finite(v, f"quality_components.{k}")
                for k, v in self.quality_components.items()
            },
            "range_quality": _require_finite(self.range_quality, "range_quality"),
            "valid": bool(self.valid),
            "rejection_reasons": list(self.rejection_reasons),
        }
        return _json_safe(payload, "RangeCandidate")


@dataclass(frozen=True)
class RangeDetectionResult:
    """Result of wyckoff_range.v1 detection over a completed daily frame."""

    range_detection_version: str
    as_of_date: str
    evaluated_candidate_count: int
    valid_candidate_count: int
    selected_range: Optional[RangeCandidate]
    rejection_reason_counts: Dict[str, int]
    post_range_segment: Tuple[Dict[str, Any], ...]
    config_used: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "range_detection_version": self.range_detection_version,
            "as_of_date": self.as_of_date,
            "evaluated_candidate_count": int(self.evaluated_candidate_count),
            "valid_candidate_count": int(self.valid_candidate_count),
            "selected_range": (
                None if self.selected_range is None else self.selected_range.to_dict()
            ),
            "rejection_reason_counts": {
                str(k): int(v) for k, v in self.rejection_reason_counts.items()
            },
            "post_range_segment": list(self.post_range_segment),
            "config_used": dict(self.config_used),
        }
        return _json_safe(payload, "RangeDetectionResult")


# --------------------------------------------------------------------------- #
# Phase 9B contracts
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class HTFContextResult:
    """Higher-timeframe context from completed monthly/weekly periods only."""

    htf_context_version: str
    as_of_date: str
    monthly_bias: str
    monthly_sma: Optional[float]
    monthly_slope_pct: Optional[float]
    monthly_trend_quality: Optional[float]
    monthly_window_structure: str
    monthly_window_raw: Dict[str, Optional[float]]
    weekly_bias: str
    weekly_sma: Optional[float]
    weekly_slope_pct: Optional[float]
    weekly_trend_quality: Optional[float]
    weekly_window_structure: str
    weekly_window_raw: Dict[str, Optional[float]]
    htf_alignment: str
    contradiction_codes: Tuple[str, ...]
    missing_data: Tuple[str, ...]
    config_used: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "htf_context_version": self.htf_context_version,
            "as_of_date": self.as_of_date,
            "monthly_bias": self.monthly_bias,
            "monthly_sma": _require_finite(self.monthly_sma, "monthly_sma"),
            "monthly_slope_pct": _require_finite(
                self.monthly_slope_pct, "monthly_slope_pct"
            ),
            "monthly_trend_quality": _require_finite(
                self.monthly_trend_quality, "monthly_trend_quality"
            ),
            "monthly_window_structure": self.monthly_window_structure,
            "monthly_window_raw": {
                k: _require_finite(v, f"monthly_window_raw.{k}")
                for k, v in self.monthly_window_raw.items()
            },
            "weekly_bias": self.weekly_bias,
            "weekly_sma": _require_finite(self.weekly_sma, "weekly_sma"),
            "weekly_slope_pct": _require_finite(
                self.weekly_slope_pct, "weekly_slope_pct"
            ),
            "weekly_trend_quality": _require_finite(
                self.weekly_trend_quality, "weekly_trend_quality"
            ),
            "weekly_window_structure": self.weekly_window_structure,
            "weekly_window_raw": {
                k: _require_finite(v, f"weekly_window_raw.{k}")
                for k, v in self.weekly_window_raw.items()
            },
            "htf_alignment": self.htf_alignment,
            "contradiction_codes": list(self.contradiction_codes),
            "missing_data": list(self.missing_data),
            "config_used": dict(self.config_used),
        }
        return _json_safe(payload, "HTFContextResult")


@dataclass(frozen=True)
class EffortResultMeasurement:
    """Causal effort-vs-result measurement for one completed daily bar."""

    effort_result_version: str
    date: str
    index: int
    timeframe: str
    atr: Optional[float]
    price_spread: Optional[float]
    spread_atr_ratio: Optional[float]
    close_location_value: Optional[float]
    previous_close: Optional[float]
    directional_result_pct: Optional[float]
    directional_result_atr_ratio: Optional[float]
    volume: Optional[float]
    relative_volume: Optional[float]
    volume_baseline_mean: Optional[float]
    volume_baseline_usable_bars: int
    effort_state: str
    result_state: str
    effort_result_state: str
    missing_data: Tuple[str, ...]
    raw_components: Dict[str, Optional[float]]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "effort_result_version": self.effort_result_version,
            "date": self.date,
            "index": int(self.index),
            "timeframe": self.timeframe,
            "atr": _require_finite(self.atr, "atr"),
            "price_spread": _require_finite(self.price_spread, "price_spread"),
            "spread_atr_ratio": _require_finite(
                self.spread_atr_ratio, "spread_atr_ratio"
            ),
            "close_location_value": _require_finite(
                self.close_location_value, "close_location_value"
            ),
            "previous_close": _require_finite(
                self.previous_close, "previous_close"
            ),
            "directional_result_pct": _require_finite(
                self.directional_result_pct, "directional_result_pct"
            ),
            "directional_result_atr_ratio": _require_finite(
                self.directional_result_atr_ratio,
                "directional_result_atr_ratio",
            ),
            "volume": _require_finite(self.volume, "volume"),
            "relative_volume": _require_finite(
                self.relative_volume, "relative_volume"
            ),
            "volume_baseline_mean": _require_finite(
                self.volume_baseline_mean, "volume_baseline_mean"
            ),
            "volume_baseline_usable_bars": int(self.volume_baseline_usable_bars),
            "effort_state": self.effort_state,
            "result_state": self.result_state,
            "effort_result_state": self.effort_result_state,
            "missing_data": list(self.missing_data),
            "raw_components": {
                k: _require_finite(v, f"raw_components.{k}")
                for k, v in self.raw_components.items()
            },
        }
        return _json_safe(payload, "EffortResultMeasurement")


@dataclass(frozen=True)
class EventCandidate:
    """One Wyckoff event candidate (never claimed as textbook certainty)."""

    event_candidate_version: str
    candidate_id: str
    range_candidate_id: str
    family: str
    event_code: str
    event_label: str
    date: str
    index: int
    timeframe: str
    as_of_date: str
    price: float
    level: Optional[float]
    direction: str
    status: str
    confirmation_status: str
    confirmation_end_date: Optional[str]
    range_relationship: str
    effort_result: Dict[str, Any]
    required_gate_results: Dict[str, bool]
    confidence_components: Dict[str, Optional[float]]
    confidence: Optional[float]
    supporting_candidate_ids: Tuple[str, ...]
    contradicting_candidate_ids: Tuple[str, ...]
    reason_codes: Tuple[str, ...]
    usable_for_structure: bool
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "event_candidate_version": self.event_candidate_version,
            "candidate_id": self.candidate_id,
            "range_candidate_id": self.range_candidate_id,
            "family": self.family,
            "event_code": self.event_code,
            "event_label": self.event_label,
            "date": self.date,
            "index": int(self.index),
            "timeframe": self.timeframe,
            "as_of_date": self.as_of_date,
            "price": _require_finite(self.price, "price"),
            "level": _require_finite(self.level, "level"),
            "direction": self.direction,
            "status": self.status,
            "confirmation_status": self.confirmation_status,
            "confirmation_end_date": self.confirmation_end_date,
            "range_relationship": self.range_relationship,
            "effort_result": dict(self.effort_result),
            "required_gate_results": {
                str(k): bool(v) for k, v in self.required_gate_results.items()
            },
            "confidence_components": {
                k: _require_finite(v, f"confidence_components.{k}")
                for k, v in self.confidence_components.items()
            },
            "confidence": _require_finite(self.confidence, "confidence"),
            "supporting_candidate_ids": list(self.supporting_candidate_ids),
            "contradicting_candidate_ids": list(self.contradicting_candidate_ids),
            "reason_codes": list(self.reason_codes),
            "usable_for_structure": bool(self.usable_for_structure),
            "metadata": dict(self.metadata),
        }
        return _json_safe(payload, "EventCandidate")


@dataclass(frozen=True)
class EventDetectionResult:
    """Result of wyckoff_events.v1 candidate detection for one range."""

    event_detection_version: str
    as_of_date: str
    range_candidate_id: str
    candidates: Tuple[EventCandidate, ...]
    candidates_by_code: Dict[str, Tuple[EventCandidate, ...]]
    rejection_reason_counts: Dict[str, int]
    candidates_truncated: bool
    config_used: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "event_detection_version": self.event_detection_version,
            "as_of_date": self.as_of_date,
            "range_candidate_id": self.range_candidate_id,
            "candidates": [c.to_dict() for c in self.candidates],
            "candidates_by_code": {
                str(k): [c.to_dict() for c in v]
                for k, v in self.candidates_by_code.items()
            },
            "rejection_reason_counts": {
                str(k): int(v) for k, v in self.rejection_reason_counts.items()
            },
            "candidates_truncated": bool(self.candidates_truncated),
            "config_used": dict(self.config_used),
        }
        return _json_safe(payload, "EventDetectionResult")


@dataclass(frozen=True)
class StructureClassificationResult:
    """Accumulation / distribution / unknown from usable confirmed events."""

    phase_classification_version: str
    as_of_date: str
    range_candidate_id: str
    classification: str
    state: str
    accumulation_event_types: Tuple[str, ...]
    distribution_event_types: Tuple[str, ...]
    accumulation_candidate_ids: Tuple[str, ...]
    distribution_candidate_ids: Tuple[str, ...]
    accumulation_confirmed_type_count: int
    distribution_confirmed_type_count: int
    accumulation_signature_events: Tuple[str, ...]
    distribution_signature_events: Tuple[str, ...]
    contradiction_codes: Tuple[str, ...]
    reason_codes: Tuple[str, ...]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "phase_classification_version": self.phase_classification_version,
            "as_of_date": self.as_of_date,
            "range_candidate_id": self.range_candidate_id,
            "classification": self.classification,
            "state": self.state,
            "accumulation_event_types": list(self.accumulation_event_types),
            "distribution_event_types": list(self.distribution_event_types),
            "accumulation_candidate_ids": list(self.accumulation_candidate_ids),
            "distribution_candidate_ids": list(self.distribution_candidate_ids),
            "accumulation_confirmed_type_count": int(
                self.accumulation_confirmed_type_count
            ),
            "distribution_confirmed_type_count": int(
                self.distribution_confirmed_type_count
            ),
            "accumulation_signature_events": list(
                self.accumulation_signature_events
            ),
            "distribution_signature_events": list(
                self.distribution_signature_events
            ),
            "contradiction_codes": list(self.contradiction_codes),
            "reason_codes": list(self.reason_codes),
        }
        return _json_safe(payload, "StructureClassificationResult")


@dataclass(frozen=True)
class PhaseCandidate:
    """One cumulative Wyckoff phase candidate (A–E)."""

    phase_candidate_version: str
    candidate_id: str
    structure: str
    phase: str
    ordinal: int
    status: str
    as_of_date: str
    required_event_codes: Tuple[str, ...]
    supporting_candidate_ids: Tuple[str, ...]
    contradicting_candidate_ids: Tuple[str, ...]
    missing_event_codes: Tuple[str, ...]
    required_gate_codes: Tuple[str, ...]
    passed_gate_codes: Tuple[str, ...]
    missing_gate_codes: Tuple[str, ...]
    failed_gate_codes: Tuple[str, ...]
    sequence_valid: bool
    confidence_components: Dict[str, Optional[float]]
    confidence: Optional[float]
    reason_codes: Tuple[str, ...]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "phase_candidate_version": self.phase_candidate_version,
            "candidate_id": self.candidate_id,
            "structure": self.structure,
            "phase": self.phase,
            "ordinal": int(self.ordinal),
            "status": self.status,
            "as_of_date": self.as_of_date,
            "required_event_codes": list(self.required_event_codes),
            "supporting_candidate_ids": list(self.supporting_candidate_ids),
            "contradicting_candidate_ids": list(
                self.contradicting_candidate_ids
            ),
            "missing_event_codes": list(self.missing_event_codes),
            "required_gate_codes": list(self.required_gate_codes),
            "passed_gate_codes": list(self.passed_gate_codes),
            "missing_gate_codes": list(self.missing_gate_codes),
            "failed_gate_codes": list(self.failed_gate_codes),
            "sequence_valid": bool(self.sequence_valid),
            "confidence_components": {
                k: _require_finite(v, f"confidence_components.{k}")
                for k, v in self.confidence_components.items()
            },
            "confidence": _require_finite(self.confidence, "confidence"),
            "reason_codes": list(self.reason_codes),
        }
        return _json_safe(payload, "PhaseCandidate")


@dataclass(frozen=True)
class PhaseClassificationResult:
    """Cumulative Phase A–E candidates with highest-supported selection."""

    phase_classification_version: str
    as_of_date: str
    structure_classification: StructureClassificationResult
    selected_phase: Optional[str]
    selected_phase_status: Optional[str]
    phase_state: str
    candidates: Tuple[PhaseCandidate, ...]
    reason_codes: Tuple[str, ...]
    config_used: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "phase_classification_version": self.phase_classification_version,
            "as_of_date": self.as_of_date,
            "structure_classification": self.structure_classification.to_dict(),
            "selected_phase": self.selected_phase,
            "selected_phase_status": self.selected_phase_status,
            "phase_state": self.phase_state,
            "candidates": [c.to_dict() for c in self.candidates],
            "reason_codes": list(self.reason_codes),
            "config_used": dict(self.config_used),
        }
        return _json_safe(payload, "PhaseClassificationResult")


# --------------------------------------------------------------------------- #
# Phase 9C1 contracts
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class FourHourTriggerResult:
    """Deterministic 4H trigger analysis (price-only first contract)."""

    trigger_version: str
    enabled: bool
    state: str
    reason_codes: Tuple[str, ...]
    side: str
    evaluation_time_utc: str
    daily_market_data_as_of: Optional[str]
    available_input_bars: int
    available_completed_bars: int
    required_completed_bars: int
    excluded_incomplete_bar_count: int
    latest_completed_4h_start: Optional[str]
    latest_completed_4h_end: Optional[str]
    latest_completed_4h_session_date: Optional[str]
    staleness_sessions: Optional[int]
    local_high: Optional[float]
    local_low: Optional[float]
    trigger_level: Optional[float]
    contradiction_level: Optional[float]
    current_close: Optional[float]
    trigger_price: Optional[float]
    triggered: bool
    contradicted: bool
    missing_data: Tuple[str, ...]
    config_used: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "trigger_version": self.trigger_version,
            "enabled": bool(self.enabled),
            "state": self.state,
            "reason_codes": list(self.reason_codes),
            "side": self.side,
            "evaluation_time_utc": self.evaluation_time_utc,
            "daily_market_data_as_of": self.daily_market_data_as_of,
            "available_input_bars": int(self.available_input_bars),
            "available_completed_bars": int(self.available_completed_bars),
            "required_completed_bars": int(self.required_completed_bars),
            "excluded_incomplete_bar_count": int(self.excluded_incomplete_bar_count),
            "latest_completed_4h_start": self.latest_completed_4h_start,
            "latest_completed_4h_end": self.latest_completed_4h_end,
            "latest_completed_4h_session_date": self.latest_completed_4h_session_date,
            "staleness_sessions": (
                None
                if self.staleness_sessions is None
                else int(self.staleness_sessions)
            ),
            "local_high": _require_finite(self.local_high, "local_high"),
            "local_low": _require_finite(self.local_low, "local_low"),
            "trigger_level": _require_finite(self.trigger_level, "trigger_level"),
            "contradiction_level": _require_finite(
                self.contradiction_level, "contradiction_level"
            ),
            "current_close": _require_finite(self.current_close, "current_close"),
            "trigger_price": _require_finite(self.trigger_price, "trigger_price"),
            "triggered": bool(self.triggered),
            "contradicted": bool(self.contradicted),
            "missing_data": list(self.missing_data),
            "config_used": dict(self.config_used),
        }
        return _json_safe(payload, "FourHourTriggerResult")


@dataclass(frozen=True)
class InvalidationResult:
    """Deterministic daily-structure invalidation (no stop/target)."""

    invalidation_version: str
    rule_code: Optional[str]
    level: Optional[float]
    source_range_id: Optional[str]
    source_event_ids: Tuple[str, ...]
    zone: Optional[Dict[str, float]]
    atr: Optional[float]
    buffer_atr_multiple: Optional[float]
    timeframe: str
    as_of: str
    reason: Optional[str]
    available: bool

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "invalidation_version": self.invalidation_version,
            "rule_code": self.rule_code,
            "level": _require_finite(self.level, "level"),
            "source_range_id": self.source_range_id,
            "source_event_ids": list(self.source_event_ids),
            "zone": self.zone,
            "atr": _require_finite(self.atr, "atr"),
            "buffer_atr_multiple": _require_finite(
                self.buffer_atr_multiple, "buffer_atr_multiple"
            ),
            "timeframe": self.timeframe,
            "as_of": self.as_of,
            "reason": self.reason,
            "available": bool(self.available),
        }
        return _json_safe(payload, "InvalidationResult")


@dataclass(frozen=True)
class RankingResult:
    """Ranking-only score — never a policy gate."""

    ranking_version: str
    components: Dict[str, Optional[float]]
    ranking_score: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "ranking_version": self.ranking_version,
            "components": {
                k: _require_finite(v, f"components.{k}")
                for k, v in self.components.items()
            },
            "ranking_score": _require_finite(self.ranking_score, "ranking_score"),
        }
        return _json_safe(payload, "RankingResult")


@dataclass(frozen=True)
class PolicyDecisionResult:
    """Explicit wyckoff_mtf.policy.v1 decision (ENTER/WATCH/AVOID only)."""

    decision_policy_version: str
    verdict: str
    side: str
    setup_state: str
    trigger_state: str
    reason_code: Optional[str]
    blocking_reasons: Tuple[str, ...]
    waiting_reasons: Tuple[str, ...]
    required_gate_results: Dict[str, bool]
    allow_enter: bool
    enter_eligible_without_rollout_gate: bool
    selected_phase: Optional[str]
    selected_phase_status: Optional[str]
    invalidation_available: bool
    trigger_required: bool
    trigger_confirmed: bool

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "decision_policy_version": self.decision_policy_version,
            "verdict": self.verdict,
            "side": self.side,
            "setup_state": self.setup_state,
            "trigger_state": self.trigger_state,
            "reason_code": self.reason_code,
            "blocking_reasons": list(self.blocking_reasons),
            "waiting_reasons": list(self.waiting_reasons),
            "required_gate_results": {
                str(k): bool(v) for k, v in self.required_gate_results.items()
            },
            "allow_enter": bool(self.allow_enter),
            "enter_eligible_without_rollout_gate": bool(
                self.enter_eligible_without_rollout_gate
            ),
            "selected_phase": self.selected_phase,
            "selected_phase_status": self.selected_phase_status,
            "invalidation_available": bool(self.invalidation_available),
            "trigger_required": bool(self.trigger_required),
            "trigger_confirmed": bool(self.trigger_confirmed),
        }
        return _json_safe(payload, "PolicyDecisionResult")


# Re-export version constants for callers that import from models.
__all__ = [
    "AGGREGATION_VERSION",
    "CompletedAggregationResult",
    "EffortResultMeasurement",
    "EventCandidate",
    "EventDetectionResult",
    "FourHourTriggerResult",
    "HTFContextResult",
    "InvalidationResult",
    "PHASE_CANDIDATE_VERSION",
    "PhaseCandidate",
    "PhaseClassificationResult",
    "PolicyDecisionResult",
    "PriceZone",
    "RANGE_CANDIDATE_VERSION",
    "RANGE_DETECTION_VERSION",
    "READINESS_VERSION",
    "RangeCandidate",
    "RangeDetectionResult",
    "RankingResult",
    "ReadinessResult",
    "StructureClassificationResult",
    "TouchInteraction",
]
