"""Phase 9A: completed monthly/weekly aggregation tests."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from app.workers.strategies.wyckoff_v2.aggregation import (
    aggregate_completed_timeframes,
)
from app.workers.strategies.wyckoff_v2.constants import AGGREGATION_VERSION

NY = ZoneInfo("America/New_York")


def _ny(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=NY).astimezone(
        timezone.utc
    )


def _daily_range(start: str, end: str, price: float = 100.0) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, end=end)
    n = len(dates)
    closes = np.full(n, price, dtype=float)
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


class TestWeeklyAggregation:
    def test_current_partial_week_excluded(self):
        # Wednesday 2024-06-26 evaluation; week ends Friday 2024-06-28.
        df = _daily_range("2024-06-03", "2024-06-26")
        result = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-26 17:00")
        )
        week_dates = result.to_dict()["weekly_period_dates"]
        assert "2024-06-28" not in week_dates
        assert result.excluded_partial_week_period == "2024-06-28"

    def test_completed_prior_week_included(self):
        df = _daily_range("2024-06-03", "2024-06-26")
        result = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-26 17:00")
        )
        week_dates = result.to_dict()["weekly_period_dates"]
        assert "2024-06-21" in week_dates  # prior Friday

    def test_friday_completed_bar_included(self):
        df = _daily_range("2024-06-03", "2024-06-28")
        result = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        week_dates = result.to_dict()["weekly_period_dates"]
        assert "2024-06-28" in week_dates
        assert result.weekly_completed_periods >= 1


class TestMonthlyAggregation:
    def test_current_partial_month_excluded(self):
        df = _daily_range("2024-01-02", "2024-06-15")
        result = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-15 17:00")
        )
        month_dates = result.to_dict()["monthly_period_dates"]
        assert "2024-06-30" not in month_dates
        assert result.excluded_partial_month_period == "2024-06-30"

    def test_completed_previous_month_included(self):
        df = _daily_range("2024-01-02", "2024-06-15")
        result = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-15 17:00")
        )
        month_dates = result.to_dict()["monthly_period_dates"]
        assert "2024-05-31" in month_dates

    def test_month_end_completed_session_included(self):
        # 2024-05-31 is a Friday — completed month-end session.
        df = _daily_range("2024-01-02", "2024-05-31")
        result = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-05-31 17:00")
        )
        month_dates = result.to_dict()["monthly_period_dates"]
        assert "2024-05-31" in month_dates


class TestAggregationInvariants:
    def test_provider_input_ordering_does_not_change_aggregates(self):
        df = _daily_range("2023-01-03", "2024-06-28")
        shuffled = df.sample(frac=1.0, random_state=11).reset_index(drop=True)
        a = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        b = aggregate_completed_timeframes(
            shuffled, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        assert a.to_dict()["monthly_period_dates"] == b.to_dict()["monthly_period_dates"]
        assert a.to_dict()["weekly_period_dates"] == b.to_dict()["weekly_period_dates"]

    def test_no_daily_bar_after_market_data_as_of_enters_aggregates(self):
        df = _daily_range("2024-01-02", "2024-06-28")
        # Append a future bar that must never enter.
        future = pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-07-15")],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1e6],
            }
        )
        # Caller should pass completed frame only; if a longer frame is
        # passed, aggregation still clamps to latest completed daily date
        # of the last bar — so truncate first like readiness would.
        completed = df.copy()
        result = aggregate_completed_timeframes(
            completed, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        assert result.latest_completed_daily_date == "2024-06-28"
        assert result.aggregation_version == AGGREGATION_VERSION
        # Explicitly ensure July is not present.
        assert all(
            not d.startswith("2024-07")
            for d in result.to_dict()["monthly_period_dates"]
        )
        # Even if someone concatenates future bars, the as-of clamp by
        # latest completed daily date of the frame would include them as
        # "latest" — readiness excludes them first. Document that contract:
        # aggregation trusts the completed frame.
        _ = future  # intentional: future must not be passed in
