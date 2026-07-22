"""Phase 9A: data-readiness tests for wyckoff_mtf.v2."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from app.workers.strategies.wyckoff_v2.constants import (
    READINESS_VERSION,
    STATUS_INSUFFICIENT_HISTORY,
    STATUS_MISSING_VOLUME,
    STATUS_READY,
    STATUS_UNCONFIRMED_BAR_COMPLETION,
    default_config,
)
from app.workers.strategies.wyckoff_v2.readiness import (
    CanonicalDailyError,
    assess_data_readiness,
    derive_history_requirement,
    normalize_canonical_daily,
)

NY = ZoneInfo("America/New_York")


def _ny(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=NY).astimezone(
        timezone.utc
    )


def _make_daily(
    n: int,
    *,
    end: str = "2024-06-28",
    start_price: float = 100.0,
    volume: float = 1_000_000.0,
    seed: int = 0,
) -> pd.DataFrame:
    """Build n completed weekday bars ending on `end` (inclusive)."""
    end_ts = pd.Timestamp(end)
    # Walk backward over calendar days collecting weekdays.
    dates = []
    cur = end_ts
    while len(dates) < n:
        if cur.weekday() < 5:
            dates.append(cur)
        cur -= pd.Timedelta(days=1)
    dates = list(reversed(dates))
    rng = np.random.default_rng(seed)
    closes = start_price + np.cumsum(rng.normal(0.0, 0.3, size=n))
    opens = closes + rng.normal(0.0, 0.1, size=n)
    highs = np.maximum(opens, closes) + rng.uniform(0.1, 0.5, size=n)
    lows = np.minimum(opens, closes) - rng.uniform(0.1, 0.5, size=n)
    vols = np.full(n, volume, dtype=float)
    return pd.DataFrame(
        {
            "date": dates,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": vols,
        }
    )


class TestNormalizeCanonicalDaily:
    def test_unordered_input_normalizes_deterministically(self):
        df = _make_daily(10, end="2024-06-28")
        shuffled = df.sample(frac=1.0, random_state=7).reset_index(drop=True)
        a = normalize_canonical_daily(shuffled)
        b = normalize_canonical_daily(df)
        assert list(a["date"]) == list(b["date"])
        assert a["close"].tolist() == b["close"].tolist()

    def test_input_dataframe_is_not_mutated(self):
        df = _make_daily(5, end="2024-06-28")
        original = df.copy(deep=True)
        _ = normalize_canonical_daily(df)
        pd.testing.assert_frame_equal(df, original)

    def test_duplicate_dates_reject(self):
        df = _make_daily(5, end="2024-06-28")
        df.loc[1, "date"] = df.loc[0, "date"]
        with pytest.raises(CanonicalDailyError) as exc:
            normalize_canonical_daily(df)
        assert exc.value.reason_code == "duplicate_dates"

    def test_malformed_ohlc_rejects(self):
        df = _make_daily(5, end="2024-06-28")
        df.loc[2, "high"] = df.loc[2, "low"] - 1.0
        with pytest.raises(CanonicalDailyError) as exc:
            normalize_canonical_daily(df)
        assert exc.value.reason_code == "ohlc_envelope"

    def test_nan_infinity_reject(self):
        df = _make_daily(5, end="2024-06-28")
        df.loc[1, "close"] = float("nan")
        with pytest.raises(CanonicalDailyError) as exc:
            normalize_canonical_daily(df)
        assert exc.value.reason_code == "non_finite_ohlc"

        df2 = _make_daily(5, end="2024-06-28")
        df2.loc[1, "open"] = float("inf")
        with pytest.raises(CanonicalDailyError) as exc2:
            normalize_canonical_daily(df2)
        assert exc2.value.reason_code == "non_finite_ohlc"

    def test_missing_volume_row_remains_present_and_counted(self):
        df = _make_daily(8, end="2024-06-28")
        df.loc[3, "volume"] = np.nan
        out = normalize_canonical_daily(df)
        assert len(out) == 8
        assert pd.isna(out.loc[3, "volume"]) or out["volume"].isna().sum() == 1

    def test_negative_volume_rejects(self):
        df = _make_daily(5, end="2024-06-28")
        df.loc[2, "volume"] = -1.0
        with pytest.raises(CanonicalDailyError) as exc:
            normalize_canonical_daily(df)
        assert exc.value.reason_code == "negative_volume"

    def test_zero_volume_kept_as_unusable(self):
        df = _make_daily(5, end="2024-06-28")
        df.loc[2, "volume"] = 0.0
        out = normalize_canonical_daily(df)
        assert len(out) == 5
        assert float(out.loc[2, "volume"]) == 0.0


class TestHistoryDerivation:
    def test_desired_vs_requested_without_cap(self):
        plan = derive_history_requirement()
        assert plan["desired_history_bars"] == plan["requested_history_bars"]
        assert plan["history_depth_capped"] is False
        cfg = default_config()
        monthly = cfg["monthly_min_periods"] * cfg["history_request_trading_days_per_month"]
        weekly = cfg["weekly_min_periods"] * cfg["history_request_trading_days_per_week"]
        daily = (
            cfg["range_max_bars"]
            + cfg["range_end_lookback_bars"]
            + cfg["atr_window"]
            + cfg["volume_baseline_window"]
            + cfg["completed_bar_exclusion_margin"]
        )
        expected = max(monthly, weekly, daily) + cfg["history_request_margin_bars"]
        assert plan["desired_history_bars"] == expected

    def test_desired_vs_requested_with_provider_cap(self):
        plan = derive_history_requirement(provider_hard_cap_bars=100)
        assert plan["requested_history_bars"] == 100
        assert plan["history_depth_capped"] is True
        assert plan["desired_history_bars"] > 100


class TestAssessDataReadiness:
    def _enough_history_frame(self):
        # Need ~24 months + weekly + structure. Use ~600 weekdays.
        return _make_daily(620, end="2024-06-28", seed=1)

    def test_insufficient_completed_daily_history(self):
        df = _make_daily(30, end="2024-06-28")
        result = assess_data_readiness(
            df, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        assert result.ready is False
        assert result.status == STATUS_INSUFFICIENT_HISTORY
        assert "insufficient_daily_history" in result.reason_codes

    def test_insufficient_monthly_periods(self):
        # Plenty of daily bars for structure, but not 24 months.
        df = _make_daily(200, end="2024-06-28", seed=2)
        cfg = default_config()
        cfg["range_max_bars"] = 20
        cfg["range_end_lookback_bars"] = 0
        cfg["monthly_min_periods"] = 24
        result = assess_data_readiness(
            df, config=cfg, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        assert result.ready is False
        assert "insufficient_monthly_periods" in result.reason_codes

    def test_insufficient_weekly_periods(self):
        df = _make_daily(80, end="2024-06-28", seed=3)
        cfg = default_config()
        cfg["range_max_bars"] = 20
        cfg["range_end_lookback_bars"] = 0
        cfg["monthly_min_periods"] = 1
        cfg["weekly_min_periods"] = 26
        result = assess_data_readiness(
            df, config=cfg, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        assert result.ready is False
        assert "insufficient_weekly_periods" in result.reason_codes

    def test_insufficient_volume_coverage(self):
        df = self._enough_history_frame()
        # Wipe most volumes.
        df.loc[:, "volume"] = np.nan
        df.loc[df.index[-5:], "volume"] = 1_000_000.0
        result = assess_data_readiness(
            df, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        assert result.ready is False
        assert "insufficient_volume_coverage" in result.reason_codes
        assert result.status in (
            STATUS_MISSING_VOLUME,
            STATUS_INSUFFICIENT_HISTORY,
        )

    def test_partial_latest_daily_bar_excluded_once(self):
        df = self._enough_history_frame()
        # Evaluation during session on the last bar's date → partial.
        last = pd.to_datetime(df["date"].iloc[-1]).date().isoformat()
        result = assess_data_readiness(
            df, evaluation_time_utc=_ny(f"{last} 12:00")
        )
        assert result.excluded_partial_daily_bar_date == last
        assert result.available_completed_bars == result.available_input_bars - 1
        # After exclusion, prior bar should be completed.
        assert result.latest_bar_completion["state"] == "completed"

    def test_future_dated_bar_produces_unconfirmed_completion(self):
        df = _make_daily(50, end="2024-07-10")
        result = assess_data_readiness(
            df, evaluation_time_utc=_ny("2024-06-28 12:00")
        )
        assert result.ready is False
        assert result.status == STATUS_UNCONFIRMED_BAR_COMPLETION
        assert "unconfirmed_bar_completion" in result.reason_codes

    def test_filled_cap_does_not_imply_history_completeness(self):
        df = _make_daily(100, end="2024-06-28")
        result = assess_data_readiness(
            df,
            evaluation_time_utc=_ny("2024-06-28 17:00"),
            provider_hard_cap_bars=100,
        )
        assert result.history_depth_capped is True
        assert result.available_completed_bars == 100
        # Cap filled, but monthly/weekly/structure requirements fail.
        assert result.history_depth_complete is False
        assert result.ready is False

    def test_ready_path_with_deep_history(self):
        df = self._enough_history_frame()
        result = assess_data_readiness(
            df, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        assert result.ready is True
        assert result.status == STATUS_READY
        assert result.history_depth_complete is True
        assert result.readiness_version == READINESS_VERSION
        assert result.completed_daily_frame is not None
        # to_dict is JSON-safe and excludes the frame.
        payload = result.to_dict()
        assert "completed_daily_frame" not in payload
        assert payload["ready"] is True
