"""Deterministic trading-range detection — wyckoff_range.v1 (Phase 9A).

Candidate ranges may end at or BEFORE as_of. Bars after end_index through
as_of form post_range_segment and never affect zones, touches, containment
or range quality. Range quality is ranking evidence only and never repairs
a failed individual validity gate.

Pure functions only — no I/O.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from app.workers.provenance import _sha256, canonical_json
from app.workers.strategies.wyckoff_v2.constants import (
    RANGE_CANDIDATE_VERSION,
    RANGE_CONFIG_KEYS,
    RANGE_DETECTION_VERSION,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import (
    PriceZone,
    RangeCandidate,
    RangeDetectionResult,
    TouchInteraction,
)


class RangeDetectionError(ValueError):
    """Deterministic rejection of malformed range-detection input."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _volume_usable(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        return False
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f) and f > 0.0


def _bar_date_iso(value: Any) -> str:
    return pd.to_datetime(value).date().isoformat()


def _atr_at(df: pd.DataFrame, end_index: int, window: int) -> Optional[float]:
    """ATR ending at end_index (inclusive), using only bars <= end_index."""
    if end_index + 1 < window + 1:
        return None
    sub = df.iloc[: end_index + 1]
    high = sub["high"].astype(float)
    low = sub["low"].astype(float)
    close = sub["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = float(tr.iloc[-window:].mean())
    if not math.isfinite(atr) or atr <= 0:
        return None
    return atr


def _quantile_zone(
    values: Sequence[float],
    q_lo: float,
    q_hi: float,
    interpolation: str,
) -> PriceZone:
    series = pd.Series(list(values), dtype=float)
    lo = float(series.quantile(q_lo, interpolation=interpolation))
    hi = float(series.quantile(q_hi, interpolation=interpolation))
    if not math.isfinite(lo) or not math.isfinite(hi):
        raise RangeDetectionError("non_finite_zone", "quantile zone is non-finite")
    return PriceZone(lo=lo, hi=hi)


def _intersects(low: float, high: float, zone: PriceZone) -> bool:
    return low <= zone.hi and high >= zone.lo


def _cluster_interactions(
    df: pd.DataFrame,
    start_index: int,
    end_index: int,
    zone: PriceZone,
    *,
    zone_name: str,
    min_touch_separation_bars: int,
) -> Tuple[Tuple[TouchInteraction, ...], int]:
    """Cluster zone interactions; return representatives + cluster count."""
    interacting: List[int] = []
    for i in range(start_index, end_index + 1):
        low = float(df.iloc[i]["low"])
        high = float(df.iloc[i]["high"])
        if _intersects(low, high, zone):
            interacting.append(i)

    if not interacting:
        return (), 0

    clusters: List[List[int]] = [[interacting[0]]]
    for idx in interacting[1:]:
        if idx - clusters[-1][-1] < min_touch_separation_bars:
            clusters[-1].append(idx)
        else:
            clusters.append([idx])

    mid = zone.midpoint
    reps: List[TouchInteraction] = []
    for cluster in clusters:
        best_idx = None
        best_dist = None
        for i in cluster:
            # Support: distance of low to midpoint; resistance: high to midpoint.
            extreme = (
                float(df.iloc[i]["low"])
                if zone_name == "support"
                else float(df.iloc[i]["high"])
            )
            dist = abs(extreme - mid)
            if best_dist is None or dist < best_dist or (
                dist == best_dist and i < best_idx  # type: ignore[operator]
            ):
                best_dist = dist
                best_idx = i
        assert best_idx is not None and best_dist is not None
        price = (
            float(df.iloc[best_idx]["low"])
            if zone_name == "support"
            else float(df.iloc[best_idx]["high"])
        )
        reps.append(
            TouchInteraction(
                date=_bar_date_iso(df.iloc[best_idx]["date"]),
                index=int(best_idx),
                price=price,
                zone=zone_name,
                distance_to_zone_midpoint=float(best_dist),
                cluster_start_date=_bar_date_iso(df.iloc[cluster[0]]["date"]),
                cluster_end_date=_bar_date_iso(df.iloc[cluster[-1]]["date"]),
                cluster_bar_count=len(cluster),
            )
        )
    return tuple(reps), len(reps)


def _width_stability_quality(
    df: pd.DataFrame,
    start_index: int,
    end_index: int,
    cfg: Dict[str, Any],
) -> Tuple[Optional[float], Optional[float]]:
    """Return (normalized_quality, raw_cv). Unknown when <2 valid widths."""
    window = int(cfg["range_stability_window_bars"])
    step = int(cfg["range_stability_step_bars"])
    bar_count = end_index - start_index + 1
    if bar_count < window or window < 2:
        return None, None

    widths: List[float] = []
    q_interp = str(cfg["quantile_interpolation"])
    for sub_start in range(start_index, end_index - window + 2, step):
        sub_end = sub_start + window - 1
        lows = [float(df.iloc[i]["low"]) for i in range(sub_start, sub_end + 1)]
        highs = [float(df.iloc[i]["high"]) for i in range(sub_start, sub_end + 1)]
        try:
            support_zone = _quantile_zone(
                lows,
                float(cfg["support_quantile_low"]),
                float(cfg["support_quantile_high"]),
                q_interp,
            )
            resistance_zone = _quantile_zone(
                highs,
                float(cfg["resistance_quantile_low"]),
                float(cfg["resistance_quantile_high"]),
                q_interp,
            )
        except RangeDetectionError:
            continue
        support = support_zone.midpoint
        resistance = resistance_zone.midpoint
        width = resistance - support
        if not math.isfinite(width) or width <= 0:
            continue
        widths.append(width)

    if len(widths) < 2:
        return None, None
    mean_w = float(np.mean(widths))
    if mean_w <= 0 or not math.isfinite(mean_w):
        return None, None
    std_w = float(np.std(widths, ddof=0))
    cv = std_w / mean_w
    if not math.isfinite(cv):
        return None, None
    max_cv = float(cfg["max_width_coefficient_of_variation"])
    if max_cv <= 0:
        return None, cv
    return _clip01(1.0 - _clip01(cv / max_cv)), cv


def _range_config_subset(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {k: cfg[k] for k in RANGE_CONFIG_KEYS if k in cfg}


def compute_candidate_id(
    *,
    as_of_date: str,
    start_date: str,
    end_date: str,
    support_zone: PriceZone,
    resistance_zone: PriceZone,
    bar_count: int,
    config_subset: Dict[str, Any],
) -> str:
    """Deterministic candidate fingerprint.

    Post-range bar count is intentionally excluded: post-range bars must not
    change candidate identity. Floats are normalized (-0.0 → 0.0; NumPy
    scalars → Python floats) before canonical JSON.
    """

    def _norm_float(value: float) -> float:
        f = float(value)
        if not math.isfinite(f):
            raise RangeDetectionError(
                "non_finite_zone", "non-finite float in candidate_id"
            )
        return 0.0 if f == 0.0 else f

    def _norm_num(value: Any) -> Any:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if isinstance(value, int) and not isinstance(value, bool):
                return int(value)
            return _norm_float(value)
        # NumPy scalars → Python scalars.
        if isinstance(value, np.generic):
            return _norm_num(value.item())
        return value

    payload = {
        "range_detection_version": RANGE_DETECTION_VERSION,
        "as_of_date": str(as_of_date),
        "start_date": str(start_date),
        "end_date": str(end_date),
        "support_zone": {
            "lo": _norm_float(support_zone.lo),
            "hi": _norm_float(support_zone.hi),
        },
        "resistance_zone": {
            "lo": _norm_float(resistance_zone.lo),
            "hi": _norm_float(resistance_zone.hi),
        },
        "bar_count": int(bar_count),
        "config": {str(k): _norm_num(v) for k, v in config_subset.items()},
    }
    return _sha256(canonical_json(payload))


def _build_candidate(
    df: pd.DataFrame,
    *,
    as_of_index: int,
    start_index: int,
    end_index: int,
    cfg: Dict[str, Any],
    config_subset: Dict[str, Any],
) -> RangeCandidate:
    as_of_date = _bar_date_iso(df.iloc[as_of_index]["date"])
    start_date = _bar_date_iso(df.iloc[start_index]["date"])
    end_date = _bar_date_iso(df.iloc[end_index]["date"])
    bar_count = end_index - start_index + 1
    post_range_bar_count = as_of_index - end_index

    rejection: List[str] = []

    # Validity: bar count bounds (generation should already respect these).
    if bar_count < int(cfg["range_min_bars"]):
        rejection.append("range_too_short")
    if bar_count > int(cfg["range_max_bars"]):
        rejection.append("range_too_long")

    # Zones / ATR / interactions / quality use ONLY bars in [start, end].
    lows = [float(df.iloc[i]["low"]) for i in range(start_index, end_index + 1)]
    highs = [float(df.iloc[i]["high"]) for i in range(start_index, end_index + 1)]
    closes = [float(df.iloc[i]["close"]) for i in range(start_index, end_index + 1)]
    volumes = [df.iloc[i]["volume"] for i in range(start_index, end_index + 1)]

    q_interp = str(cfg["quantile_interpolation"])
    try:
        support_zone = _quantile_zone(
            lows,
            float(cfg["support_quantile_low"]),
            float(cfg["support_quantile_high"]),
            q_interp,
        )
        resistance_zone = _quantile_zone(
            highs,
            float(cfg["resistance_quantile_low"]),
            float(cfg["resistance_quantile_high"]),
            q_interp,
        )
    except RangeDetectionError:
        # Non-finite zones — reject this candidate deterministically.
        support_zone = PriceZone(lo=0.0, hi=0.0)
        resistance_zone = PriceZone(lo=0.0, hi=0.0)
        rejection.append("non_finite_zone")

    support = support_zone.midpoint
    resistance = resistance_zone.midpoint
    width = resistance - support
    midpoint = (support + resistance) / 2.0

    if not math.isfinite(width) or width <= 0 or support >= resistance:
        rejection.append("invalid_zone_geometry")

    # Causal ATR at end_index: may use warm-up bars before start_index, never
    # any bar after end_index.
    atr = _atr_at(df, end_index, int(cfg["atr_window"]))
    width_atr_multiple: Optional[float] = None
    if atr is None:
        rejection.append("atr_unavailable")
    else:
        width_atr_multiple = width / atr if atr > 0 and math.isfinite(width) else None
        if width_atr_multiple is None or not math.isfinite(width_atr_multiple):
            rejection.append("width_atr_non_finite")
        else:
            if width_atr_multiple < float(cfg["range_min_atr_multiple"]):
                rejection.append("width_below_min_atr_multiple")
            if width_atr_multiple > float(cfg["range_max_atr_multiple"]):
                rejection.append("width_above_max_atr_multiple")

    support_interactions, support_clusters = _cluster_interactions(
        df,
        start_index,
        end_index,
        support_zone,
        zone_name="support",
        min_touch_separation_bars=int(cfg["min_touch_separation_bars"]),
    )
    resistance_interactions, resistance_clusters = _cluster_interactions(
        df,
        start_index,
        end_index,
        resistance_zone,
        zone_name="resistance",
        min_touch_separation_bars=int(cfg["min_touch_separation_bars"]),
    )
    if support_clusters < int(cfg["min_support_touch_clusters"]):
        rejection.append("insufficient_support_clusters")
    if resistance_clusters < int(cfg["min_resistance_touch_clusters"]):
        rejection.append("insufficient_resistance_clusters")

    # Containment / contamination use outer zone boundaries on the window only.
    if bar_count > 0:
        contained = sum(
            1
            for close in closes
            if support_zone.lo <= close <= resistance_zone.hi
        )
        contaminated = sum(
            1
            for close in closes
            if close < support_zone.lo or close > resistance_zone.hi
        )
        containment_fraction = contained / bar_count
        breakout_contamination_fraction = contaminated / bar_count
    else:
        containment_fraction = None
        breakout_contamination_fraction = None

    if containment_fraction is None or containment_fraction < float(
        cfg["min_containment_fraction"]
    ):
        rejection.append("containment_below_threshold")
    if breakout_contamination_fraction is None or (
        breakout_contamination_fraction
        > float(cfg["max_breakout_contamination_fraction"])
    ):
        rejection.append("contamination_above_threshold")

    usable = sum(1 for v in volumes if _volume_usable(v))
    volume_coverage = usable / bar_count if bar_count > 0 else None
    if volume_coverage is None or volume_coverage < float(
        cfg["min_range_volume_coverage"]
    ):
        rejection.append("insufficient_volume_coverage")

    # Quality components (ranking only — never consulted for validity).
    total_clusters = support_clusters + resistance_clusters
    if total_clusters > 0:
        touch_balance_quality = (
            1.0 - abs(support_clusters - resistance_clusters) / total_clusters
        )
    else:
        touch_balance_quality = None

    width_stability_quality, _raw_cv = _width_stability_quality(
        df, start_index, end_index, cfg
    )

    quality_components: Dict[str, Optional[float]] = {
        "touch_balance_quality": (
            None if touch_balance_quality is None else float(touch_balance_quality)
        ),
        "containment_quality": (
            None if containment_fraction is None else float(containment_fraction)
        ),
        "volume_coverage_quality": (
            None if volume_coverage is None else float(volume_coverage)
        ),
        "width_stability_quality": width_stability_quality,
    }

    if any(v is None for v in quality_components.values()):
        range_quality: Optional[float] = None
    else:
        range_quality = float(min(quality_components.values()))  # type: ignore[arg-type]

    # Validity is ONLY individual gates — never range_quality.
    valid = len(rejection) == 0

    candidate_id = compute_candidate_id(
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        support_zone=support_zone,
        resistance_zone=resistance_zone,
        bar_count=bar_count,
        config_subset=config_subset,
    )

    return RangeCandidate(
        range_candidate_version=RANGE_CANDIDATE_VERSION,
        candidate_id=candidate_id,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        start_index=int(start_index),
        end_index=int(end_index),
        post_range_bar_count=int(post_range_bar_count),
        bar_count=int(bar_count),
        support_zone=support_zone,
        resistance_zone=resistance_zone,
        support=float(support),
        resistance=float(resistance),
        midpoint=float(midpoint),
        width=float(width) if math.isfinite(width) else 0.0,
        atr=atr,
        width_atr_multiple=width_atr_multiple,
        support_interactions=support_interactions,
        resistance_interactions=resistance_interactions,
        support_touch_cluster_count=int(support_clusters),
        resistance_touch_cluster_count=int(resistance_clusters),
        containment_fraction=containment_fraction,
        breakout_contamination_fraction=breakout_contamination_fraction,
        volume_coverage=volume_coverage,
        quality_components=quality_components,
        range_quality=range_quality,
        valid=valid,
        rejection_reasons=tuple(rejection),
    )


def _selection_key(candidate: RangeCandidate) -> Tuple:
    """Exact approved ordering key (lower is better after negation tricks).

    We return a tuple for Python's ascending sort with inverted fields where
    "descending" is required.
    """
    rq = candidate.range_quality
    # NULL last: use a flag (True sorts after False when we put nulls last
    # by sorting (rq is None, -rq_or_0, ...)).
    rq_null = rq is None
    rq_sort = 0.0 if rq is None else -float(rq)
    containment = (
        0.0
        if candidate.containment_fraction is None
        else -float(candidate.containment_fraction)
    )
    total_clusters = -(
        candidate.support_touch_cluster_count
        + candidate.resistance_touch_cluster_count
    )
    contamination = (
        float("inf")
        if candidate.breakout_contamination_fraction is None
        else float(candidate.breakout_contamination_fraction)
    )
    # end_date descending → negate via reverse string by sorting ascending on
    # inverted: use negative of an ordinal. ISO dates sort lexicographically.
    # For descending, wrap as a key that Python sorts ascending: use
    # a transform. Easiest: sort with key returning negated concepts.
    return (
        rq_null,  # False (has quality) before True (NULL)
        rq_sort,  # higher quality first
        containment,  # higher containment first
        total_clusters,  # more clusters first
        contamination,  # lower contamination first
        # end_date descending: invert by sorting on negated date via
        # a descending-friendly trick — use "" prefix complement:
        # sort ascending on reversed comparison using a pair.
        # We'll use the raw end_date and reverse later for that field only —
        # actually: store as negative of a sortable ordinal. ISO dates:
        # use ('', end_date) with reverse... Simplest approach in caller:
        # return end_date for ascending and negate by using a custom
        # wrapper. Here: use inverted ISO by mapping each char.
        "".join(chr(255 - ord(c)) for c in candidate.end_date),
        -int(candidate.bar_count),
        candidate.start_date,  # ascending
        candidate.candidate_id,  # ascending final tie-break
    )


def _post_range_segment(
    df: pd.DataFrame, end_index: int, as_of_index: int
) -> Tuple[Dict[str, Any], ...]:
    rows: List[Dict[str, Any]] = []
    for i in range(end_index + 1, as_of_index + 1):
        row = df.iloc[i]
        vol = row["volume"]
        vol_out: Optional[float]
        if _volume_usable(vol):
            vol_out = float(vol)
        elif vol is None or (isinstance(vol, float) and math.isnan(vol)) or pd.isna(vol):
            vol_out = None
        else:
            vol_out = float(vol) if _finite(vol) else None
        rows.append(
            {
                "date": _bar_date_iso(row["date"]),
                "index": int(i),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": vol_out,
            }
        )
    return tuple(rows)


def detect_trading_ranges(
    completed_daily: pd.DataFrame,
    *,
    config: Optional[Dict[str, Any]] = None,
    as_of_date: Optional[str] = None,
) -> RangeDetectionResult:
    """Detect a single selected trading range on a completed daily frame.

    Only bars on/before `as_of_date` (default: last bar of the frame) are
    read. The frame is truncated to as_of BEFORE OHLC validation so future
    rows cannot poison a pinned evaluation. Appending future bars beyond a
    pinned as_of must not change the result.
    """
    cfg = resolve_config(config)
    if completed_daily is None or len(completed_daily) == 0:
        raise RangeDetectionError("empty_frame", "completed daily frame is empty")

    # Defensive copy; never mutate caller.
    df = completed_daily.copy().reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    if as_of_date is None:
        as_of_index = len(df) - 1
        as_of = _bar_date_iso(df.iloc[as_of_index]["date"])
    else:
        as_of = str(as_of_date)
        matches = [
            i
            for i, d in enumerate(df["date"].tolist())
            if pd.to_datetime(d).date().isoformat() == as_of
        ]
        if not matches:
            raise RangeDetectionError(
                "as_of_not_found", f"as_of_date {as_of} not in frame"
            )
        as_of_index = matches[-1]

    # Truncate to as_of FIRST — future bars are never validated or read.
    df = df.iloc[: as_of_index + 1].reset_index(drop=True)
    as_of_index = len(df) - 1

    # Validate finite OHLC on the truncated frame only.
    for col in ("open", "high", "low", "close"):
        vals = df[col].tolist()
        if any(not _finite(v) for v in vals):
            raise RangeDetectionError(
                "non_finite_ohlc", f"non-finite values in {col}"
            )
        if any(float(v) <= 0 for v in vals):
            raise RangeDetectionError(
                "non_positive_ohlc", f"non-positive values in {col}"
            )

    config_subset = _range_config_subset(cfg)
    min_bars = int(cfg["range_min_bars"])
    max_bars = int(cfg["range_max_bars"])
    length_step = int(cfg["range_length_step"])
    end_lookback = int(cfg["range_end_lookback_bars"])
    end_step = int(cfg["range_end_step"])

    candidates: List[RangeCandidate] = []
    seen_pairs: set = set()
    # Candidate end positions: 0 .. range_end_lookback_bars before as_of.
    for end_offset in range(0, end_lookback + 1, end_step):
        end_index = as_of_index - end_offset
        if end_index < 0:
            break
        for length in range(min_bars, max_bars + 1, length_step):
            start_index = end_index - length + 1
            if start_index < 0:
                continue
            pair = (start_index, end_index)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            candidates.append(
                _build_candidate(
                    df,
                    as_of_index=as_of_index,
                    start_index=start_index,
                    end_index=end_index,
                    cfg=cfg,
                    config_subset=config_subset,
                )
            )

    valid = [c for c in candidates if c.valid]
    reason_counts: Counter = Counter()
    for c in candidates:
        for reason in c.rejection_reasons:
            reason_counts[reason] += 1

    selected: Optional[RangeCandidate] = None
    post_segment: Tuple[Dict[str, Any], ...] = ()
    if valid:
        selected = sorted(valid, key=_selection_key)[0]
        post_segment = _post_range_segment(df, selected.end_index, as_of_index)

    return RangeDetectionResult(
        range_detection_version=RANGE_DETECTION_VERSION,
        as_of_date=as_of,
        evaluated_candidate_count=len(candidates),
        valid_candidate_count=len(valid),
        selected_range=selected,
        rejection_reason_counts={k: int(v) for k, v in sorted(reason_counts.items())},
        post_range_segment=post_segment,
        config_used=config_subset,
    )
