"""Phase 9A data readiness for wyckoff_mtf.v2.

Canonical daily normalization (does NOT silently drop missing-volume rows),
ny_session_close.v1 completion, history-request planning, and the authoritative
completed-period / volume-coverage readiness decision.

Pure functions only — no I/O, no providers, no database.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

from app.workers.strategies.bar_completion import assess_latest_bar_completion
from app.workers.strategies.wyckoff_v2.aggregation import (
    aggregate_completed_timeframes,
)
from app.workers.strategies.wyckoff_v2.constants import (
    READINESS_VERSION,
    STATUS_INSUFFICIENT_HISTORY,
    STATUS_MISSING_VOLUME,
    STATUS_READY,
    STATUS_UNCONFIRMED_BAR_COMPLETION,
    STATUS_UNKNOWN,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import ReadinessResult


OHLCV_COLS = ("date", "open", "high", "low", "close", "volume")


class CanonicalDailyError(ValueError):
    """Deterministic rejection of a malformed daily OHLCV frame."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _volume_usable(value: Any) -> bool:
    """Finite and strictly positive volume is usable; missing/zero is not."""
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


def normalize_canonical_daily(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normalize a daily OHLCV frame for v2 without silently dropping volume gaps.

    Rules:
      * required columns must be present
      * dates parse deterministically; sort oldest-first
      * duplicate dates reject
      * OHLC must be finite and positive; high/low envelope must hold
      * volume may be missing or zero (kept, marked unusable later)
      * negative volume rejects
      * NaN/Inf in OHLC rejects
      * input DataFrame is never mutated
      * provider row ordering does not change the output
    """
    if df is None:
        raise CanonicalDailyError("missing_frame", "daily frame is None")
    if not isinstance(df, pd.DataFrame):
        raise CanonicalDailyError("missing_frame", "daily frame is not a DataFrame")
    missing = [c for c in OHLCV_COLS if c not in df.columns]
    if missing:
        raise CanonicalDailyError(
            "missing_columns", f"missing OHLCV columns: {missing}"
        )

    # Copy only the columns we need so the caller's frame is untouched.
    raw = df.loc[:, list(OHLCV_COLS)].copy()
    if len(raw) == 0:
        return pd.DataFrame(columns=list(OHLCV_COLS))

    parsed_dates = pd.to_datetime(raw["date"], errors="coerce")
    if parsed_dates.isna().any():
        raise CanonicalDailyError("unparseable_date", "one or more dates failed to parse")

    for col in ("open", "high", "low", "close"):
        coerced = pd.to_numeric(raw[col], errors="coerce")
        if coerced.isna().any():
            raise CanonicalDailyError(
                "non_finite_ohlc", f"non-finite or missing values in {col}"
            )
        if (coerced <= 0).any():
            raise CanonicalDailyError(
                "non_positive_ohlc", f"non-positive values in {col}"
            )
        if not all(math.isfinite(float(v)) for v in coerced.tolist()):
            raise CanonicalDailyError(
                "non_finite_ohlc", f"non-finite values in {col}"
            )
        raw[col] = coerced

    # Volume: coerce; keep NaN for missing; reject negatives; Inf rejects.
    vol = pd.to_numeric(raw["volume"], errors="coerce")
    for v in vol.tolist():
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        if not math.isfinite(float(v)):
            raise CanonicalDailyError("non_finite_volume", "non-finite volume value")
        if float(v) < 0:
            raise CanonicalDailyError("negative_volume", "negative volume value")
    raw["volume"] = vol
    raw["date"] = parsed_dates

    # High/low envelope.
    o = raw["open"].astype(float)
    h = raw["high"].astype(float)
    l = raw["low"].astype(float)
    c = raw["close"].astype(float)
    if not ((h >= o) & (h >= c) & (h >= l)).all():
        raise CanonicalDailyError("ohlc_envelope", "high must be >= open, close and low")
    if not ((l <= o) & (l <= c) & (l <= h)).all():
        raise CanonicalDailyError("ohlc_envelope", "low must be <= open, close and high")

    out = raw.sort_values("date", kind="mergesort").reset_index(drop=True)
    # Duplicate dates reject (after sort so detection is order-independent).
    if out["date"].duplicated().any():
        raise CanonicalDailyError("duplicate_dates", "duplicate trading dates")
    return out


def derive_history_requirement(
    config: Optional[Dict[str, Any]] = None,
    *,
    provider_hard_cap_bars: Optional[int] = None,
) -> Dict[str, Any]:
    """Pure request-planning helper. Not proof of completed-period coverage."""
    cfg = resolve_config(config)
    required_monthly = int(cfg["monthly_min_periods"])
    required_weekly = int(cfg["weekly_min_periods"])
    monthly_request_target = (
        required_monthly * int(cfg["history_request_trading_days_per_month"])
    )
    weekly_request_target = (
        required_weekly * int(cfg["history_request_trading_days_per_week"])
    )
    daily_structure_target = (
        int(cfg["range_max_bars"])
        + int(cfg["range_end_lookback_bars"])
        + int(cfg["atr_window"])
        + int(cfg["volume_baseline_window"])
        + int(cfg["completed_bar_exclusion_margin"])
    )
    desired = (
        max(monthly_request_target, weekly_request_target, daily_structure_target)
        + int(cfg["history_request_margin_bars"])
    )
    if provider_hard_cap_bars is None:
        requested = desired
        capped = False
    else:
        cap = int(provider_hard_cap_bars)
        if cap <= 0:
            raise ValueError("provider_hard_cap_bars must be positive when supplied")
        requested = min(desired, cap)
        capped = requested < desired
    return {
        "required_monthly_periods": required_monthly,
        "required_weekly_periods": required_weekly,
        "monthly_request_target": monthly_request_target,
        "weekly_request_target": weekly_request_target,
        "daily_structure_target": daily_structure_target,
        "desired_history_bars": desired,
        "requested_history_bars": requested,
        "history_depth_capped": capped,
        "required_daily_structure_bars": daily_structure_target,
    }


def _iso_utc(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def assess_data_readiness(
    df: Optional[pd.DataFrame],
    *,
    config: Optional[Dict[str, Any]] = None,
    evaluation_time_utc: Optional[datetime] = None,
    explicit_completed: Optional[bool] = None,
    provider_hard_cap_bars: Optional[int] = None,
    as_of_date: Optional[str] = None,
) -> ReadinessResult:
    """Assess Phase 9A readiness on a daily OHLCV frame.

    Applies ny_session_close.v1 once (one safe partial-bar exclusion, then
    re-assess). Authority for completeness is actual completed monthly/weekly
    period counts, completed daily bars, and volume coverage — never a filled
    provider cap alone.

    When `as_of_date` is supplied, rows after that date are ignored before
    completion assessment so future-dated bars cannot poison a pinned
    evaluation.
    """
    cfg = resolve_config(config)
    history = derive_history_requirement(
        cfg, provider_hard_cap_bars=provider_hard_cap_bars
    )
    eval_iso = _iso_utc(evaluation_time_utc)

    empty_completion: Dict[str, Any] = {
        "policy": cfg["bar_completion_policy"],
        "state": "unknown",
        "reason": "no_bars",
        "bar_date": None,
    }

    def _fail(
        status: str,
        reasons: List[str],
        *,
        completion: Optional[Dict[str, Any]] = None,
        available_input: int = 0,
        available_completed: int = 0,
        monthly_periods: int = 0,
        weekly_periods: int = 0,
        usable_volume: int = 0,
        volume_coverage: Optional[float] = None,
        required_volume: int = 0,
        excluded: Optional[str] = None,
        market_data_as_of: Optional[str] = None,
        missing_fields: Tuple[str, ...] = (),
        frame: Optional[pd.DataFrame] = None,
        depth_complete: bool = False,
    ) -> ReadinessResult:
        return ReadinessResult(
            readiness_version=READINESS_VERSION,
            ready=False,
            status=status,
            reason_codes=tuple(reasons),
            latest_bar_completion=dict(completion or empty_completion),
            evaluation_time_utc=eval_iso,
            market_data_as_of=market_data_as_of,
            desired_history_bars=history["desired_history_bars"],
            requested_history_bars=history["requested_history_bars"],
            available_input_bars=available_input,
            available_completed_bars=available_completed,
            history_depth_capped=history["history_depth_capped"],
            history_depth_complete=depth_complete,
            required_monthly_periods=history["required_monthly_periods"],
            available_completed_monthly_periods=monthly_periods,
            required_weekly_periods=history["required_weekly_periods"],
            available_completed_weekly_periods=weekly_periods,
            required_daily_structure_bars=history["required_daily_structure_bars"],
            usable_volume_bars=usable_volume,
            required_volume_bars=required_volume,
            volume_coverage=volume_coverage,
            excluded_partial_daily_bar_date=excluded,
            missing_fields=missing_fields,
            completed_daily_frame=frame,
        )

    try:
        normalized = normalize_canonical_daily(df)
    except CanonicalDailyError as exc:
        return _fail(
            STATUS_UNKNOWN,
            [exc.reason_code],
            missing_fields=(exc.reason_code,),
        )

    # Pin as_of before completion: future rows never enter readiness.
    if as_of_date is not None and len(normalized) > 0:
        as_of = pd.Timestamp(str(as_of_date)).date()
        normalized = normalized[
            pd.to_datetime(normalized["date"]).dt.date <= as_of
        ].reset_index(drop=True)

    available_input = len(normalized)
    if available_input == 0:
        return _fail(
            STATUS_INSUFFICIENT_HISTORY,
            ["insufficient_history"],
            available_input=0,
            missing_fields=("insufficient_history",),
        )

    # ---- Completed-bar gate (ny_session_close.v1) -------------------------- #
    completion = assess_latest_bar_completion(
        normalized,
        exchange_timezone=str(cfg["exchange_timezone"]),
        session_close_time=str(cfg["session_close_time"]),
        explicit_completed=explicit_completed,
        now_utc=evaluation_time_utc,
    )
    excluded: Optional[str] = None
    working = normalized
    if completion["state"] == "partial":
        excluded = completion.get("bar_date")
        working = normalized.iloc[:-1].reset_index(drop=True)
        completion = assess_latest_bar_completion(
            working,
            exchange_timezone=str(cfg["exchange_timezone"]),
            session_close_time=str(cfg["session_close_time"]),
            now_utc=evaluation_time_utc,
        )

    if completion["state"] != "completed":
        return _fail(
            STATUS_UNCONFIRMED_BAR_COMPLETION,
            ["unconfirmed_bar_completion"],
            completion=completion,
            available_input=available_input,
            available_completed=len(working),
            excluded=excluded,
            missing_fields=("unconfirmed_bar_completion",),
            frame=working if len(working) > 0 else None,
        )

    available_completed = len(working)
    if available_completed == 0:
        return _fail(
            STATUS_INSUFFICIENT_HISTORY,
            ["insufficient_history"],
            completion=completion,
            available_input=available_input,
            available_completed=0,
            excluded=excluded,
            missing_fields=("insufficient_history",),
        )

    market_data_as_of = (
        pd.to_datetime(working["date"].iloc[-1]).date().isoformat()
    )

    # Volume coverage over the completed frame.
    usable = int(sum(1 for v in working["volume"].tolist() if _volume_usable(v)))
    volume_coverage = (
        float(usable) / float(available_completed) if available_completed > 0 else None
    )
    max_missing = float(cfg["max_missing_volume_fraction"])
    min_coverage = 1.0 - max_missing
    required_volume = int(math.ceil(min_coverage * available_completed))

    # Completed monthly/weekly periods (authoritative; not daily estimates).
    aggregation = aggregate_completed_timeframes(
        working,
        evaluation_time_utc=evaluation_time_utc
        or datetime.now(timezone.utc),
        exchange_timezone=str(cfg["exchange_timezone"]),
        as_of_date=market_data_as_of,
    )
    monthly_periods = aggregation.monthly_completed_periods
    weekly_periods = aggregation.weekly_completed_periods

    reasons: List[str] = []
    missing: List[str] = []

    if available_completed < history["required_daily_structure_bars"]:
        reasons.append("insufficient_daily_history")
        missing.append("insufficient_daily_history")
    if monthly_periods < history["required_monthly_periods"]:
        reasons.append("insufficient_monthly_periods")
        missing.append("insufficient_monthly_periods")
    if weekly_periods < history["required_weekly_periods"]:
        reasons.append("insufficient_weekly_periods")
        missing.append("insufficient_weekly_periods")
    if volume_coverage is None or volume_coverage < min_coverage:
        reasons.append("insufficient_volume_coverage")
        missing.append("insufficient_volume_coverage")

    depth_complete = len(reasons) == 0
    # A filled provider cap never implies completeness by itself.
    if not depth_complete:
        status = (
            STATUS_MISSING_VOLUME
            if reasons == ["insufficient_volume_coverage"]
            else STATUS_INSUFFICIENT_HISTORY
        )
        if "insufficient_volume_coverage" in reasons and all(
            r == "insufficient_volume_coverage" for r in reasons
        ):
            status = STATUS_MISSING_VOLUME
        return _fail(
            status,
            reasons,
            completion=completion,
            available_input=available_input,
            available_completed=available_completed,
            monthly_periods=monthly_periods,
            weekly_periods=weekly_periods,
            usable_volume=usable,
            volume_coverage=volume_coverage,
            required_volume=required_volume,
            excluded=excluded,
            market_data_as_of=market_data_as_of,
            missing_fields=tuple(missing),
            frame=working,
            depth_complete=False,
        )

    return ReadinessResult(
        readiness_version=READINESS_VERSION,
        ready=True,
        status=STATUS_READY,
        reason_codes=(),
        latest_bar_completion=dict(completion),
        evaluation_time_utc=eval_iso,
        market_data_as_of=market_data_as_of,
        desired_history_bars=history["desired_history_bars"],
        requested_history_bars=history["requested_history_bars"],
        available_input_bars=available_input,
        available_completed_bars=available_completed,
        history_depth_capped=history["history_depth_capped"],
        history_depth_complete=True,
        required_monthly_periods=history["required_monthly_periods"],
        available_completed_monthly_periods=monthly_periods,
        required_weekly_periods=history["required_weekly_periods"],
        available_completed_weekly_periods=weekly_periods,
        required_daily_structure_bars=history["required_daily_structure_bars"],
        usable_volume_bars=usable,
        required_volume_bars=required_volume,
        volume_coverage=volume_coverage,
        excluded_partial_daily_bar_date=excluded,
        missing_fields=(),
        completed_daily_frame=working,
    )
