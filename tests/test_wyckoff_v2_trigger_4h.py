"""Phase 9C1: deterministic 4H trigger analysis tests."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from app.workers.strategies.wyckoff_v2.trigger_4h import (
    FourHourTriggerError,
    analyze_4h_trigger,
    normalize_4h_ohlcv,
)


def _cfg(**overrides):
    base = {
        "enable_4h_trigger": True,
        "trigger_lookback_4h": 10,
        "four_hour_bar_duration_hours": 4,
        "four_hour_timestamp_timezone": "UTC",
        "max_4h_staleness_sessions": 1,
        "exchange_timezone": "America/New_York",
    }
    base.update(overrides)
    return base


def _4h_frame(
    n: int,
    *,
    end_start: datetime,
    closes=None,
    highs=None,
    lows=None,
) -> pd.DataFrame:
    """n bars ending with bar_start=end_start (oldest first)."""
    starts = [end_start - timedelta(hours=4 * (n - 1 - i)) for i in range(n)]
    if closes is None:
        closes = [100.0 + i * 0.1 for i in range(n)]
    if highs is None:
        highs = [c + 1.0 for c in closes]
    if lows is None:
        lows = [c - 1.0 for c in closes]
    return pd.DataFrame(
        {
            "timestamp": starts,
            "open": closes,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [1_000_000.0] * n,
        }
    )


def _daily_sessions(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [pd.Timestamp(d) for d in dates],
            "open": [100.0] * len(dates),
            "high": [101.0] * len(dates),
            "low": [99.0] * len(dates),
            "close": [100.0] * len(dates),
            "volume": [1e6] * len(dates),
        }
    )


class TestNormalize4h:
    def test_unordered_sorts_oldest_first(self):
        end = datetime(2024, 6, 28, 16, 0, tzinfo=timezone.utc)
        df = _4h_frame(5, end_start=end)
        shuffled = df.sample(frac=1.0, random_state=3).reset_index(drop=True)
        out = normalize_4h_ohlcv(shuffled)
        assert list(out["timestamp"]) == sorted(out["timestamp"].tolist())

    def test_duplicate_timestamps_reject(self):
        end = datetime(2024, 6, 28, 16, 0, tzinfo=timezone.utc)
        df = _4h_frame(3, end_start=end)
        df.loc[1, "timestamp"] = df.loc[0, "timestamp"]
        with pytest.raises(FourHourTriggerError) as exc:
            normalize_4h_ohlcv(df)
        assert exc.value.reason_code == "duplicate_timestamps"

    def test_malformed_ohlc_reject(self):
        end = datetime(2024, 6, 28, 16, 0, tzinfo=timezone.utc)
        df = _4h_frame(3, end_start=end)
        df.loc[1, "high"] = df.loc[1, "low"] - 1
        with pytest.raises(FourHourTriggerError) as exc:
            normalize_4h_ohlcv(df)
        assert exc.value.reason_code == "ohlc_envelope"

    def test_negative_volume_reject(self):
        end = datetime(2024, 6, 28, 16, 0, tzinfo=timezone.utc)
        df = _4h_frame(3, end_start=end)
        df.loc[0, "volume"] = -1
        with pytest.raises(FourHourTriggerError) as exc:
            normalize_4h_ohlcv(df)
        assert exc.value.reason_code == "negative_volume"

    def test_input_unchanged(self):
        end = datetime(2024, 6, 28, 16, 0, tzinfo=timezone.utc)
        df = _4h_frame(4, end_start=end)
        original = df.copy(deep=True)
        _ = normalize_4h_ohlcv(df)
        pd.testing.assert_frame_equal(df, original)


class TestCompletionAndTrigger:
    def test_partial_trailing_excluded(self):
        # Latest bar starts 14:00, ends 18:00; eval at 17:00 → exclude
        latest_start = datetime(2024, 6, 28, 14, 0, tzinfo=timezone.utc)
        eval_t = datetime(2024, 6, 28, 17, 0, tzinfo=timezone.utc)
        df = _4h_frame(12, end_start=latest_start)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        result = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert result.excluded_incomplete_bar_count >= 1
        assert result.available_completed_bars == 11

    def test_bar_ending_exactly_at_eval_included(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        eval_t = latest_start + timedelta(hours=4)  # exactly bar_end
        closes = [100.0] * 10 + [105.0]
        highs = [101.0] * 10 + [106.0]
        lows = [99.0] * 11
        df = _4h_frame(11, end_start=latest_start, closes=closes, highs=highs, lows=lows)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        result = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert result.available_completed_bars == 11
        assert result.state == "confirmed"
        assert result.trigger_price == 105.0

    def test_future_aggregate_excluded(self):
        latest_start = datetime(2024, 6, 28, 20, 0, tzinfo=timezone.utc)
        eval_t = datetime(2024, 6, 28, 16, 0, tzinfo=timezone.utc)
        df = _4h_frame(12, end_start=latest_start)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        result = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        # Bars that start at/after evaluation_time are not observable input.
        assert result.available_input_bars < 12
        assert result.available_completed_bars <= result.available_input_bars
        if result.latest_completed_4h_end is not None:
            end = pd.Timestamp(result.latest_completed_4h_end)
            if end.tzinfo is None:
                end = end.tz_localize("UTC")
            assert end.to_pydatetime() <= eval_t

    def test_insufficient_bars(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        eval_t = latest_start + timedelta(hours=4)
        df = _4h_frame(5, end_start=latest_start)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        result = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert result.state == "unknown"
        assert "insufficient_4h_history" in result.reason_codes

    def test_long_confirmed_missing_contradicted(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        eval_t = latest_start + timedelta(hours=4)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        base_highs = [101.0] * 10
        base_lows = [99.0] * 10

        confirmed = _4h_frame(
            11,
            end_start=latest_start,
            closes=[100.0] * 10 + [102.0],
            highs=base_highs + [103.0],
            lows=base_lows + [100.0],
        )
        r = analyze_4h_trigger(
            confirmed,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert r.state == "confirmed"
        assert r.triggered is True
        assert r.trigger_level == 101.0

        missing = _4h_frame(
            11,
            end_start=latest_start,
            closes=[100.0] * 10 + [100.5],
            highs=base_highs + [101.0],
            lows=base_lows + [99.5],
        )
        r2 = analyze_4h_trigger(
            missing,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert r2.state == "missing"

        contradicted = _4h_frame(
            11,
            end_start=latest_start,
            closes=[100.0] * 10 + [98.0],
            highs=base_highs + [99.0],
            lows=base_lows + [97.0],
        )
        r3 = analyze_4h_trigger(
            contradicted,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert r3.state == "contradicted"
        assert r3.contradicted is True

    def test_short_confirmed_missing_contradicted(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        eval_t = latest_start + timedelta(hours=4)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        base_highs = [101.0] * 10
        base_lows = [99.0] * 10

        confirmed = _4h_frame(
            11,
            end_start=latest_start,
            closes=[100.0] * 10 + [98.0],
            highs=base_highs + [99.0],
            lows=base_lows + [97.0],
        )
        r = analyze_4h_trigger(
            confirmed,
            side="SHORT",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert r.state == "confirmed"
        assert r.trigger_level == 99.0

        missing = _4h_frame(
            11,
            end_start=latest_start,
            closes=[100.0] * 10 + [100.0],
            highs=base_highs + [100.5],
            lows=base_lows + [99.5],
        )
        r2 = analyze_4h_trigger(
            missing,
            side="SHORT",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert r2.state == "missing"

        contradicted = _4h_frame(
            11,
            end_start=latest_start,
            closes=[100.0] * 10 + [102.0],
            highs=base_highs + [103.0],
            lows=base_lows + [100.0],
        )
        r3 = analyze_4h_trigger(
            contradicted,
            side="SHORT",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert r3.state == "contradicted"

    def test_weekend_freshness_session_count(self):
        # Friday 4H bar end; Monday daily as_of → 1 session after Friday
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)  # Fri
        eval_t = datetime(2024, 7, 1, 21, 0, tzinfo=timezone.utc)  # Mon after close
        closes = [100.0] * 10 + [105.0]
        highs = [101.0] * 10 + [106.0]
        lows = [99.0] * 11
        df = _4h_frame(11, end_start=latest_start, closes=closes, highs=highs, lows=lows)
        daily = _daily_sessions(["2024-06-27", "2024-06-28", "2024-07-01"])
        r = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-07-01",
            config=_cfg(max_4h_staleness_sessions=1),
        )
        assert r.staleness_sessions == 1
        assert r.state == "confirmed"

    def test_stale_session_count(self):
        latest_start = datetime(2024, 6, 26, 12, 0, tzinfo=timezone.utc)  # Wed
        eval_t = datetime(2024, 6, 28, 21, 0, tzinfo=timezone.utc)
        df = _4h_frame(11, end_start=latest_start)
        daily = _daily_sessions(["2024-06-26", "2024-06-27", "2024-06-28"])
        r = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(max_4h_staleness_sessions=1),
        )
        assert r.staleness_sessions == 2
        assert r.state == "unknown"
        assert "four_hour_trigger_stale" in r.reason_codes

    def test_unreconciled_session_date(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        eval_t = latest_start + timedelta(hours=4)
        df = _4h_frame(11, end_start=latest_start)
        # Daily frame only has older dates; 4H session after all daily
        daily = _daily_sessions(["2024-06-20", "2024-06-21"])
        r = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-21",
            config=_cfg(),
        )
        assert r.state == "unknown"
        assert "unconfirmed_4h_freshness" in r.reason_codes

    def test_future_rows_after_pin_invariant(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        eval_t = latest_start + timedelta(hours=4)
        closes = [100.0] * 10 + [105.0]
        highs = [101.0] * 10 + [106.0]
        lows = [99.0] * 11
        df = _4h_frame(11, end_start=latest_start, closes=closes, highs=highs, lows=lows)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        r1 = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        future = pd.concat(
            [
                df,
                pd.DataFrame(
                    {
                        "timestamp": [eval_t + timedelta(hours=4)],
                        "open": [200.0],
                        "high": [201.0],
                        "low": [199.0],
                        "close": [200.0],
                        "volume": [1e6],
                    }
                ),
            ],
            ignore_index=True,
        )
        r2 = analyze_4h_trigger(
            future,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert r1.to_dict() == r2.to_dict()

    def test_disabled_and_missing_frame(self):
        eval_t = datetime(2024, 6, 28, 20, 0, tzinfo=timezone.utc)
        r = analyze_4h_trigger(
            None,
            side="LONG",
            evaluation_time_utc=eval_t,
            config=_cfg(enable_4h_trigger=False),
        )
        assert r.state == "unknown"
        assert "four_hour_trigger_disabled" in r.reason_codes

        r2 = analyze_4h_trigger(
            None,
            side="LONG",
            evaluation_time_utc=eval_t,
            config=_cfg(enable_4h_trigger=True),
        )
        assert "four_hour_data_missing" in r2.reason_codes

    def test_to_dict_strict_json(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        eval_t = latest_start + timedelta(hours=4)
        closes = [100.0] * 10 + [105.0]
        highs = [101.0] * 10 + [106.0]
        lows = [99.0] * 11
        df = _4h_frame(11, end_start=latest_start, closes=closes, highs=highs, lows=lows)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        r = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        json.dumps(r.to_dict(), allow_nan=False, sort_keys=True)


class TestAdversarial4hBoundaries:
    def test_bar_end_one_microsecond_after_eval_incomplete(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        bar_end = latest_start + timedelta(hours=4)
        df = _4h_frame(11, end_start=latest_start)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        included = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=bar_end,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(trigger_lookback_4h=3),
        )
        excluded = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=bar_end - timedelta(microseconds=1),
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(trigger_lookback_4h=3),
        )
        assert included.available_completed_bars == excluded.available_completed_bars + 1
        assert excluded.excluded_incomplete_bar_count >= 1

    def test_long_short_strict_boundary(self):
        latest_start = datetime(2024, 6, 28, 12, 0, tzinfo=timezone.utc)
        eval_t = latest_start + timedelta(hours=4)
        daily = _daily_sessions(["2024-06-27", "2024-06-28"])
        # equal to local_high → missing for LONG (strict)
        df = _4h_frame(
            11,
            end_start=latest_start,
            closes=[100.0] * 10 + [101.0],
            highs=[101.0] * 10 + [101.0],
            lows=[99.0] * 11,
        )
        r = analyze_4h_trigger(
            df,
            side="LONG",
            evaluation_time_utc=eval_t,
            daily_frame=daily,
            daily_market_data_as_of="2024-06-28",
            config=_cfg(),
        )
        assert r.state == "missing"
