"""Completed monthly/weekly aggregation for wyckoff_mtf.v2 (Phase 9A).

Builds higher-timeframe frames ONLY from a canonical completed daily frame.
Partial trailing periods never enter v2 context. Reuses pandas resample
mechanics locally — does NOT call or modify v1 `normalize_ohlcv` (which
silently drops missing-volume rows).

Volume contract (frozen):
  * period volume uses sum(min_count=1)
  * all-missing volume in a period stays NaN (never coerced to 0)
  * zero volume remains distinguishable from missing volume
  * mixed usable + missing sums the usable values only

Calendar-period completion (no exchange calendar invented):
  include a bucket iff
    period_end <= evaluation_session_date
    AND (
      period_end <= latest_completed_daily_date
      OR evaluation_session_date > period_end
    )
  The second OR-branch lets a holiday-shortened week / weekend month-end
  become includable once the evaluation session is clearly past the calendar
  period end, without claiming holiday awareness.

Pure functions only — no I/O.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

from app.workers.strategies.wyckoff_v2.constants import AGGREGATION_VERSION
from app.workers.strategies.wyckoff_v2.models import CompletedAggregationResult


_OHLCV_COLS = ["date", "open", "high", "low", "close", "volume"]
_MONTHLY_RULE = "ME"
_WEEKLY_RULE = "W-FRI"


def _empty_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_OHLCV_COLS))


def _session_date(
    evaluation_time_utc: datetime, exchange_timezone: str
) -> date:
    if evaluation_time_utc.tzinfo is None:
        evaluation_time_utc = evaluation_time_utc.replace(tzinfo=timezone.utc)
    return evaluation_time_utc.astimezone(ZoneInfo(exchange_timezone)).date()


def _sum_volume_min_count_1(series: pd.Series) -> float:
    """Sum volume with min_count=1 so all-NaN periods stay NaN, not 0."""
    return series.sum(min_count=1)


_AGG = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": _sum_volume_min_count_1,
}


def _resample_raw(daily: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample without going through v1 normalize_ohlcv."""
    if daily is None or len(daily) == 0:
        return _empty_ohlcv()
    frame = daily.loc[:, _OHLCV_COLS].copy()
    frame["date"] = pd.to_datetime(frame["date"])
    indexed = frame.set_index("date")
    agg = indexed.resample(rule).agg(_AGG)
    agg = agg.dropna(subset=["open", "high", "low", "close"])
    agg = agg.reset_index()
    return agg[_OHLCV_COLS]


def _period_is_complete(
    period_end: date,
    *,
    evaluation_session_date: date,
    latest_completed_daily_date: date,
) -> bool:
    """Calendar-period completion without inventing an exchange calendar."""
    if period_end > evaluation_session_date:
        return False
    # Proven by a completed bar on/after the period-end calendar date, OR by
    # the evaluation session having moved strictly past the period end (the
    # calendar period is over even if the last market session was earlier —
    # e.g. Friday holiday, Thursday last bar, Monday evaluation).
    if period_end <= latest_completed_daily_date:
        return True
    if evaluation_session_date > period_end:
        return True
    return False


def _filter_completed_periods(
    periods: pd.DataFrame,
    *,
    evaluation_session_date: date,
    latest_completed_daily_date: date,
) -> Tuple[pd.DataFrame, Optional[str]]:
    """Keep only periods whose period-end date is proven complete."""
    if periods is None or len(periods) == 0:
        return _empty_ohlcv(), None

    out_rows = []
    excluded: Optional[str] = None
    for _, row in periods.iterrows():
        period_end = pd.to_datetime(row["date"]).date()
        if not _period_is_complete(
            period_end,
            evaluation_session_date=evaluation_session_date,
            latest_completed_daily_date=latest_completed_daily_date,
        ):
            excluded = period_end.isoformat()
            continue
        out_rows.append(row)

    if not out_rows:
        return _empty_ohlcv(), excluded
    frame = pd.DataFrame(out_rows).reset_index(drop=True)
    frame = frame[_OHLCV_COLS]
    return frame, excluded


def aggregate_completed_timeframes(
    completed_daily: pd.DataFrame,
    *,
    evaluation_time_utc: datetime,
    exchange_timezone: str = "America/New_York",
    as_of_date: Optional[str] = None,
) -> CompletedAggregationResult:
    """Build completed monthly and weekly frames from completed daily bars.

    `completed_daily` must already be the post-ny_session_close.v1 frame
    (partial latest daily bar already excluded). When `as_of_date` is set,
    no daily bar after that date is ever read.
    """
    if completed_daily is None or len(completed_daily) == 0:
        session = _session_date(evaluation_time_utc, exchange_timezone)
        return CompletedAggregationResult(
            aggregation_version=AGGREGATION_VERSION,
            monthly_frame=_empty_ohlcv(),
            weekly_frame=_empty_ohlcv(),
            monthly_completed_periods=0,
            weekly_completed_periods=0,
            excluded_partial_month_period=None,
            excluded_partial_week_period=None,
            latest_completed_daily_date=None,
            evaluation_session_date=session.isoformat(),
        )

    daily = completed_daily.loc[:, _OHLCV_COLS].copy()
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date", kind="mergesort").reset_index(drop=True)

    if as_of_date is not None:
        as_of = pd.Timestamp(str(as_of_date)).date()
        daily = daily[daily["date"].dt.date <= as_of].reset_index(drop=True)
        if len(daily) == 0:
            session = _session_date(evaluation_time_utc, exchange_timezone)
            return CompletedAggregationResult(
                aggregation_version=AGGREGATION_VERSION,
                monthly_frame=_empty_ohlcv(),
                weekly_frame=_empty_ohlcv(),
                monthly_completed_periods=0,
                weekly_completed_periods=0,
                excluded_partial_month_period=None,
                excluded_partial_week_period=None,
                latest_completed_daily_date=None,
                evaluation_session_date=session.isoformat(),
            )

    latest_completed = pd.to_datetime(daily["date"].iloc[-1]).date()
    session = _session_date(evaluation_time_utc, exchange_timezone)

    daily = daily[daily["date"].dt.date <= latest_completed].reset_index(drop=True)

    raw_monthly = _resample_raw(daily, _MONTHLY_RULE)
    raw_weekly = _resample_raw(daily, _WEEKLY_RULE)

    monthly, excluded_month = _filter_completed_periods(
        raw_monthly,
        evaluation_session_date=session,
        latest_completed_daily_date=latest_completed,
    )
    weekly, excluded_week = _filter_completed_periods(
        raw_weekly,
        evaluation_session_date=session,
        latest_completed_daily_date=latest_completed,
    )

    return CompletedAggregationResult(
        aggregation_version=AGGREGATION_VERSION,
        monthly_frame=monthly,
        weekly_frame=weekly,
        monthly_completed_periods=len(monthly),
        weekly_completed_periods=len(weekly),
        excluded_partial_month_period=excluded_month,
        excluded_partial_week_period=excluded_week,
        latest_completed_daily_date=latest_completed.isoformat(),
        evaluation_session_date=session.isoformat(),
    )
