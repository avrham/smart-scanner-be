"""Phase 9B: higher-timeframe context measurements."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from app.workers.strategies.wyckoff_v2.aggregation import (
    aggregate_completed_timeframes,
)
from app.workers.strategies.wyckoff_v2.constants import (
    HTF_CONTEXT_VERSION,
    Phase9AConfigError,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.context_htf import measure_htf_context
from app.workers.strategies.wyckoff_v2.models import CompletedAggregationResult
from app.workers.strategies.wyckoff_v2.readiness import normalize_canonical_daily


def _ohlcv(n: int, *, end: str, start: float = 100.0, drift: float = 0.5):
    end_ts = pd.Timestamp(end)
    dates = []
    cur = end_ts
    while len(dates) < n:
        if cur.weekday() < 5:
            dates.append(cur)
        cur -= pd.Timedelta(days=1)
    dates = list(reversed(dates))
    closes = start + np.arange(n) * drift
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes - 0.2,
            "high": closes + 0.5,
            "low": closes - 0.5,
            "close": closes,
            "volume": np.full(n, 1_000_000.0),
        }
    )


def _agg_from_daily(daily: pd.DataFrame, eval_time: datetime):
    frame = normalize_canonical_daily(daily)
    return aggregate_completed_timeframes(frame, evaluation_time_utc=eval_time)


def _period_frame(n: int, *, close_fn, high_fn=None, low_fn=None):
    rows = []
    for i in range(n):
        c = float(close_fn(i))
        h = float(high_fn(i)) if high_fn else c + 5
        l = float(low_fn(i)) if low_fn else c - 5
        rows.append(
            {
                "date": pd.Timestamp("2019-01-31") + pd.DateOffset(months=i),
                "open": c,
                "high": h,
                "low": l,
                "close": c,
                "volume": 1e6,
            }
        )
    return pd.DataFrame(rows)


def _agg(monthly, weekly=None):
    weekly = monthly.copy() if weekly is None else weekly
    return CompletedAggregationResult(
        aggregation_version="wyckoff_aggregation.v1",
        monthly_frame=monthly,
        weekly_frame=weekly,
        monthly_completed_periods=len(monthly),
        weekly_completed_periods=len(weekly),
        excluded_partial_month_period=None,
        excluded_partial_week_period=None,
        latest_completed_daily_date="2021-12-31",
        evaluation_session_date="2021-12-31",
    )


_CFG = {
    "monthly_sma_window": 5,
    "monthly_slope_lookback": 2,
    "weekly_sma_window": 5,
    "weekly_slope_lookback": 2,
    "monthly_structure_window_periods": 3,
    "weekly_structure_window_periods": 3,
    "htf_structure_tolerance_pct": 0.0,
}


class TestHTFBias:
    def test_monthly_up_down_neutral_unknown(self):
        cfg = resolve_config(_CFG)
        up = _period_frame(24, close_fn=lambda i: 100 + i * 3)
        r = measure_htf_context(_agg(up), as_of_date="2021-12-31", config=cfg)
        assert r.htf_context_version == HTF_CONTEXT_VERSION
        assert r.monthly_bias == "up"
        assert r.weekly_bias == "up"
        assert r.htf_alignment == "aligned_up"

        down = _period_frame(24, close_fn=lambda i: 200 - i * 3)
        r2 = measure_htf_context(_agg(down), as_of_date="2021-12-31", config=cfg)
        assert r2.monthly_bias == "down"
        assert r2.htf_alignment == "aligned_down"

        flat = _period_frame(24, close_fn=lambda i: 100.0)
        r3 = measure_htf_context(_agg(flat), as_of_date="2021-12-31", config=cfg)
        assert r3.monthly_bias == "neutral"
        assert r3.htf_alignment == "mixed"

        empty = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        r4 = measure_htf_context(_agg(empty), as_of_date="2021-12-31", config=cfg)
        assert r4.monthly_bias == "unknown"
        assert r4.htf_alignment == "unknown"

    def test_contradiction_alignment(self):
        cfg = resolve_config(_CFG)
        up = _period_frame(24, close_fn=lambda i: 100 + i * 3)
        down = _period_frame(24, close_fn=lambda i: 200 - i * 3)
        r = measure_htf_context(
            _agg(up, weekly=down), as_of_date="2021-12-31", config=cfg
        )
        assert r.htf_alignment == "contradiction"
        assert "monthly_weekly_bias_contradiction" in r.contradiction_codes


class TestWindowStructure:
    def test_hhhl_lhll_mixed_insufficient(self):
        cfg = resolve_config(
            {
                **_CFG,
                "monthly_structure_window_periods": 4,
                "weekly_structure_window_periods": 4,
            }
        )
        # prior = first 4 of last 8, recent = last 4 → elevate only last 4
        hhhl = _period_frame(
            12,
            close_fn=lambda i: 100 + (10 if i >= 8 else 0),
            high_fn=lambda i: 105 + (10 if i >= 8 else 0),
            low_fn=lambda i: 95 + (10 if i >= 8 else 0),
        )
        r = measure_htf_context(_agg(hhhl), as_of_date="2021-12-31", config=cfg)
        assert r.monthly_window_structure == "higher_high_higher_low"

        lhll = _period_frame(
            12,
            close_fn=lambda i: 100 - (10 if i >= 8 else 0),
            high_fn=lambda i: 105 - (10 if i >= 8 else 0),
            low_fn=lambda i: 95 - (10 if i >= 8 else 0),
        )
        r2 = measure_htf_context(_agg(lhll), as_of_date="2021-12-31", config=cfg)
        assert r2.monthly_window_structure == "lower_high_lower_low"

        mixed = _period_frame(
            12,
            close_fn=lambda i: 100,
            high_fn=lambda i: 120 if i >= 8 else 110,
            low_fn=lambda i: 90 if i >= 8 else 100,
        )
        r3 = measure_htf_context(_agg(mixed), as_of_date="2021-12-31", config=cfg)
        assert r3.monthly_window_structure == "mixed"

        small = hhhl.iloc[:5].copy()
        r4 = measure_htf_context(_agg(small), as_of_date="2021-12-31", config=cfg)
        assert r4.monthly_window_structure == "unknown"


class TestHTFCausalityAndSerialization:
    def test_future_bars_do_not_change_pinned_result(self):
        daily = _ohlcv(400, end="2024-06-28", drift=0.2)
        eval_time = datetime(2024, 6, 28, 21, 0, tzinfo=timezone.utc)
        agg = _agg_from_daily(daily, eval_time)
        as_of = "2024-06-28"
        r1 = measure_htf_context(agg, as_of_date=as_of)

        extra = _ohlcv(
            20, end="2024-07-26", start=float(daily["close"].iloc[-1]), drift=1.0
        )
        extended = normalize_canonical_daily(
            pd.concat([daily, extra], ignore_index=True)
        )
        later_eval = datetime(2024, 7, 26, 21, 0, tzinfo=timezone.utc)
        agg2 = aggregate_completed_timeframes(extended, evaluation_time_utc=later_eval)
        r2 = measure_htf_context(agg2, as_of_date=as_of)
        assert r1.to_dict() == r2.to_dict()

    def test_json_safe(self):
        daily = _ohlcv(400, end="2024-06-28", drift=0.1)
        eval_time = datetime(2024, 6, 28, 21, 0, tzinfo=timezone.utc)
        agg = _agg_from_daily(daily, eval_time)
        r = measure_htf_context(agg, as_of_date="2024-06-28")
        json.dumps(r.to_dict(), allow_nan=False, sort_keys=True)

    def test_config_validation_rejects_non_positive(self):
        with pytest.raises(Phase9AConfigError):
            resolve_config({"monthly_slope_reference_pct": 0})
        with pytest.raises(Phase9AConfigError):
            resolve_config({"monthly_structure_window_periods": 0})
