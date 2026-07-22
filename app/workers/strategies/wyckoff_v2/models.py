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


# Re-export version constants for callers that import from models.
__all__ = [
    "AGGREGATION_VERSION",
    "CompletedAggregationResult",
    "PriceZone",
    "RANGE_CANDIDATE_VERSION",
    "RANGE_DETECTION_VERSION",
    "READINESS_VERSION",
    "RangeCandidate",
    "RangeDetectionResult",
    "ReadinessResult",
    "TouchInteraction",
]
