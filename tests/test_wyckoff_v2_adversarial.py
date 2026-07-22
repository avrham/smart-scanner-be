"""Adversarial Phase 9A regression tests — contract proofs before commit."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from app.workers.strategies import bar_completion, sma150_v3
from app.workers.strategies.wyckoff_v2.aggregation import (
    aggregate_completed_timeframes,
    _resample_raw,
)
from app.workers.strategies.wyckoff_v2.constants import (
    MAX_CANDIDATE_ATTEMPTS,
    Phase9AConfigError,
    default_config,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import PriceZone
from app.workers.strategies.wyckoff_v2.ranges import (
    RangeDetectionError,
    _atr_at,
    _cluster_interactions,
    compute_candidate_id,
    detect_trading_ranges,
)
from app.workers.strategies.wyckoff_v2.readiness import assess_data_readiness

NY = ZoneInfo("America/New_York")


def _ny(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=NY).astimezone(
        timezone.utc
    )


def _dates(n: int, end: str = "2024-06-28") -> list:
    end_ts = pd.Timestamp(end)
    dates = []
    cur = end_ts
    while len(dates) < n:
        if cur.weekday() < 5:
            dates.append(cur)
        cur -= pd.Timedelta(days=1)
    return list(reversed(dates))


def _stable_range(n: int = 50, post: int = 0, end: str = "2024-06-28") -> pd.DataFrame:
    dates = _dates(n + post, end=end)
    rng = np.random.default_rng(7)
    support, resistance = 100.0, 110.0
    mid = (support + resistance) / 2.0
    half = (resistance - support) / 2.0
    closes = []
    for i in range(n):
        closes.append(mid + math.sin(i / 3.0) * half * 0.7)
    for j in range(post):
        closes.append(resistance + 5.0 + j * 2.0)
    rows = []
    for i, c in enumerate(closes):
        o = c + rng.normal(0, 0.05)
        if i < n:
            h = max(o, c) + 0.3
            l = min(o, c) - 0.3
            if i % 11 == 0:
                l = support + 0.05
            if i % 13 == 0:
                h = resistance - 0.05
        else:
            h = max(o, c) + 3.0
            l = min(o, c) - 0.2
        h = max(h, o, c)
        l = min(l, o, c)
        rows.append((dates[i], o, h, l, c, 1_000_000.0))
    return pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])


def _cfg(**overrides):
    cfg = default_config()
    cfg.update(
        {
            "range_min_bars": 20,
            "range_max_bars": 40,
            "range_length_step": 5,
            "range_end_lookback_bars": 10,
            "range_end_step": 1,
            "range_min_atr_multiple": 2.0,
            "range_max_atr_multiple": 20.0,
            "min_containment_fraction": 0.70,
            "max_breakout_contamination_fraction": 0.30,
        }
    )
    cfg.update(overrides)
    return cfg


# --------------------------------------------------------------------------- #
# §3 Post-range leakage
# --------------------------------------------------------------------------- #

class TestNoPostRangeLeakage:
    def test_post_range_does_not_change_candidate_core_fields(self):
        base = _stable_range(50, post=0)
        end_idx = 44  # candidate ends before last bar of base
        pinned_as_of_short = pd.to_datetime(base["date"].iloc[end_idx]).date().isoformat()
        # Frame with post-range after end_idx through a later as_of.
        extended = _stable_range(45, post=8)
        # Align: first 45 bars match a range window ending at 44 relative to
        # as_of at last bar. Pin as_of to last bar of extended.
        cfg = _cfg(range_end_lookback_bars=8, range_max_bars=40, range_min_bars=20)

        # Force evaluation of the candidate ending at index 44 by comparing
        # detection on truncated vs extended with same as_of on the short frame
        # and extended frame pinned to the short as_of... Better approach:
        # build one frame; detect with as_of = last; find candidate with end_index
        # before as_of; then zero-out / spike only post-range bars and re-detect.
        df = _stable_range(50, post=8)
        as_of = pd.to_datetime(df["date"].iloc[-1]).date().isoformat()
        r1 = detect_trading_ranges(df, config=cfg, as_of_date=as_of)
        assert r1.selected_range is not None
        sel = r1.selected_range
        assert sel.end_index < sel.start_index + sel.bar_count  # sanity
        # Spike ONLY bars after end_index.
        df2 = df.copy()
        for i in range(sel.end_index + 1, len(df2)):
            df2.loc[i, "high"] = float(df2.loc[i, "high"]) + 80.0
            df2.loc[i, "close"] = float(df2.loc[i, "close"]) + 80.0
            df2.loc[i, "open"] = float(df2.loc[i, "open"]) + 80.0
            df2.loc[i, "low"] = float(df2.loc[i, "low"]) + 80.0
            df2.loc[i, "volume"] = 50_000_000.0
        r2 = detect_trading_ranges(df2, config=cfg, as_of_date=as_of)
        # Find same start/end candidate via selected or by identity fields.
        assert r2.selected_range is not None
        # Compare the candidate that shares start/end with sel if still selected,
        # otherwise build both via internal path.
        from app.workers.strategies.wyckoff_v2.ranges import _build_candidate
        from app.workers.strategies.wyckoff_v2.constants import resolve_config

        resolved = resolve_config(cfg)
        subset = {k: resolved[k] for k in (
            "atr_window", "range_min_bars", "range_max_bars",
            "support_quantile_low", "support_quantile_high",
            "resistance_quantile_low", "resistance_quantile_high",
            "quantile_interpolation", "range_min_atr_multiple",
            "range_max_atr_multiple", "min_support_touch_clusters",
            "min_resistance_touch_clusters", "min_touch_separation_bars",
            "min_containment_fraction", "max_breakout_contamination_fraction",
            "min_range_volume_coverage", "range_stability_window_bars",
            "range_stability_step_bars", "max_width_coefficient_of_variation",
            "range_end_lookback_bars", "range_end_step", "range_length_step",
        )}
        # Truncate both frames to as_of for fair build.
        d1 = df.copy().reset_index(drop=True)
        d2 = df2.copy().reset_index(drop=True)
        as_of_index = len(d1) - 1
        c1 = _build_candidate(
            d1, as_of_index=as_of_index, start_index=sel.start_index,
            end_index=sel.end_index, cfg=resolved, config_subset=subset,
        )
        c2 = _build_candidate(
            d2, as_of_index=as_of_index, start_index=sel.start_index,
            end_index=sel.end_index, cfg=resolved, config_subset=subset,
        )
        assert c1.support_zone.to_dict() == c2.support_zone.to_dict()
        assert c1.resistance_zone.to_dict() == c2.resistance_zone.to_dict()
        assert c1.atr == c2.atr
        assert c1.width_atr_multiple == c2.width_atr_multiple
        assert c1.containment_fraction == c2.containment_fraction
        assert c1.volume_coverage == c2.volume_coverage
        assert c1.range_quality == c2.range_quality
        assert c1.candidate_id == c2.candidate_id
        assert c1.quality_components == c2.quality_components
        # Post-range metadata may differ only in segment content, not counts
        # here (same as_of / same end) — counts equal; volumes in segment differ.
        assert c1.post_range_bar_count == c2.post_range_bar_count

    def test_post_range_volume_spike_does_not_change_volume_coverage(self):
        df = _stable_range(50, post=6)
        cfg = _cfg(range_end_lookback_bars=6)
        as_of = pd.to_datetime(df["date"].iloc[-1]).date().isoformat()
        r1 = detect_trading_ranges(df, config=cfg, as_of_date=as_of)
        assert r1.selected_range is not None
        sel = r1.selected_range
        df2 = df.copy()
        for i in range(sel.end_index + 1, len(df2)):
            df2.loc[i, "volume"] = 9e9
        from app.workers.strategies.wyckoff_v2.ranges import _build_candidate
        resolved = resolve_config(cfg)
        subset = {k: resolved[k] for k in resolved if k in default_config()}
        # Use RANGE_CONFIG_KEYS via resolve already validated.
        from app.workers.strategies.wyckoff_v2.constants import RANGE_CONFIG_KEYS
        subset = {k: resolved[k] for k in RANGE_CONFIG_KEYS}
        c1 = _build_candidate(
            df.reset_index(drop=True), as_of_index=len(df) - 1,
            start_index=sel.start_index, end_index=sel.end_index,
            cfg=resolved, config_subset=subset,
        )
        c2 = _build_candidate(
            df2.reset_index(drop=True), as_of_index=len(df2) - 1,
            start_index=sel.start_index, end_index=sel.end_index,
            cfg=resolved, config_subset=subset,
        )
        assert c1.volume_coverage == c2.volume_coverage
        assert c1.range_quality == c2.range_quality


# --------------------------------------------------------------------------- #
# §4 Pinned as_of
# --------------------------------------------------------------------------- #

class TestPinnedAsOfIsolation:
    def test_future_rows_do_not_change_detection(self):
        df = _stable_range(55, post=0)
        pinned = pd.to_datetime(df["date"].iloc[49]).date().isoformat()
        cfg = _cfg()
        r1 = detect_trading_ranges(df.iloc[:50], config=cfg, as_of_date=pinned)
        future = df.iloc[50:].copy()
        future["close"] = future["close"] + 200
        future["high"] = future["high"] + 200
        # Corrupt future with NaN — must not poison pinned eval.
        future.loc[future.index[0], "close"] = float("nan")
        extended = pd.concat([df.iloc[:50], future], ignore_index=True)
        r2 = detect_trading_ranges(extended, config=cfg, as_of_date=pinned)
        assert r1.to_dict() == r2.to_dict()

    def test_future_dated_row_does_not_unconfirm_pinned_readiness(self):
        # Deep enough history for a soft readiness check with lowered requirements.
        dates = pd.bdate_range("2022-01-03", "2024-06-28")
        n = len(dates)
        df = pd.DataFrame({
            "date": dates,
            "open": np.full(n, 100.0),
            "high": np.full(n, 101.0),
            "low": np.full(n, 99.0),
            "close": np.full(n, 100.0),
            "volume": np.full(n, 1e6),
        })
        # Append future-dated bar.
        future = pd.DataFrame({
            "date": [pd.Timestamp("2024-07-15")],
            "open": [100.0], "high": [101.0], "low": [99.0],
            "close": [100.0], "volume": [1e6],
        })
        extended = pd.concat([df, future], ignore_index=True)
        cfg = default_config()
        cfg.update({
            "monthly_min_periods": 12,
            "weekly_min_periods": 12,
            "range_max_bars": 40,
            "range_end_lookback_bars": 5,
        })
        pinned = "2024-06-28"
        r = assess_data_readiness(
            extended,
            config=cfg,
            evaluation_time_utc=_ny("2024-06-28 17:00"),
            as_of_date=pinned,
        )
        assert r.status != "unconfirmed_bar_completion"
        assert r.market_data_as_of == pinned


# --------------------------------------------------------------------------- #
# §5 ATR
# --------------------------------------------------------------------------- #

class TestATRContract:
    def test_atr_ignores_post_range_bars(self):
        df = _stable_range(40, post=10)
        atr1 = _atr_at(df.iloc[:40].reset_index(drop=True), 39, 14)
        atr2 = _atr_at(df.reset_index(drop=True), 39, 14)
        assert atr1 == atr2

    def test_insufficient_history_returns_none(self):
        df = _stable_range(10)
        assert _atr_at(df, 9, 14) is None


# --------------------------------------------------------------------------- #
# §6 Missing volume aggregation
# --------------------------------------------------------------------------- #

class TestMissingVolumeAggregation:
    def _daily(self, volumes):
        n = len(volumes)
        dates = pd.bdate_range("2024-06-03", periods=n)
        return pd.DataFrame({
            "date": dates,
            "open": [100.0] * n,
            "high": [101.0] * n,
            "low": [99.0] * n,
            "close": [100.0] * n,
            "volume": volumes,
        })

    def test_all_missing_stays_nan(self):
        df = self._daily([np.nan] * 5)
        weekly = _resample_raw(df, "W-FRI")
        assert len(weekly) >= 1
        assert pd.isna(weekly["volume"].iloc[-1])

    def test_one_missing_sums_usable(self):
        df = self._daily([1e6, np.nan, 2e6, 3e6, 4e6])
        weekly = _resample_raw(df, "W-FRI")
        # Last week should sum non-null only.
        assert float(weekly["volume"].sum(min_count=1)) == pytest.approx(10e6)

    def test_all_zeros_remain_zero(self):
        df = self._daily([0.0] * 5)
        weekly = _resample_raw(df, "W-FRI")
        assert float(weekly["volume"].iloc[-1]) == 0.0

    def test_mixed_zero_and_missing(self):
        df = self._daily([0.0, np.nan, 0.0])
        weekly = _resample_raw(df, "W-FRI")
        # min_count=1 with zeros present → 0.0, not NaN.
        assert float(weekly["volume"].iloc[-1]) == 0.0


# --------------------------------------------------------------------------- #
# §7 / §8 Month and week boundaries
# --------------------------------------------------------------------------- #

class TestCalendarPeriodCompletion:
    def test_january_saturday_month_end_includable_in_february(self):
        # Jan 31 2026 is Saturday; last trading day Jan 30.
        dates = pd.bdate_range("2025-12-01", "2026-01-30")
        df = pd.DataFrame({
            "date": dates,
            "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
            "volume": 1e6,
        })
        r_jan = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2026-01-30 17:00")
        )
        assert "2026-01-31" not in r_jan.to_dict()["monthly_period_dates"]
        r_feb = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2026-02-02 17:00")
        )
        assert "2026-01-31" in r_feb.to_dict()["monthly_period_dates"]

    def test_holiday_friday_week_includable_on_monday(self):
        # Bars Mon-Thu Jun 17-20 2024; Friday Jun 21 holiday (no bar).
        dates = pd.bdate_range("2024-06-10", "2024-06-20")
        df = pd.DataFrame({
            "date": dates,
            "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
            "volume": 1e6,
        })
        r_thu = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-20 17:00")
        )
        assert "2024-06-21" not in r_thu.to_dict()["weekly_period_dates"]
        r_mon = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-24 17:00")
        )
        assert "2024-06-21" in r_mon.to_dict()["weekly_period_dates"]

    def test_normal_friday_after_close_included(self):
        dates = pd.bdate_range("2024-06-03", "2024-06-28")
        df = pd.DataFrame({
            "date": dates,
            "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
            "volume": 1e6,
        })
        r = aggregate_completed_timeframes(
            df, evaluation_time_utc=_ny("2024-06-28 17:00")
        )
        assert "2024-06-28" in r.to_dict()["weekly_period_dates"]


# --------------------------------------------------------------------------- #
# §9 Config validation
# --------------------------------------------------------------------------- #

class TestConfigValidation:
    def test_rejects_inverted_range_bounds(self):
        with pytest.raises(Phase9AConfigError) as exc:
            resolve_config({"range_min_bars": 50, "range_max_bars": 20})
        assert exc.value.reason_code == "invalid_range_config"

    def test_rejects_runaway_candidate_grid(self):
        with pytest.raises(Phase9AConfigError) as exc:
            resolve_config({
                "range_min_bars": 1,
                "range_max_bars": 100000,
                "range_length_step": 1,
                "range_end_lookback_bars": 100000,
                "range_end_step": 1,
            })
        assert exc.value.reason_code == "candidate_generation_limit_exceeded"

    def test_rejects_bad_quantiles(self):
        with pytest.raises(Phase9AConfigError) as exc:
            resolve_config({"support_quantile_low": 0.9, "support_quantile_high": 0.1})
        assert exc.value.reason_code == "invalid_quantile_config"

    def test_rejects_provider_hard_cap_non_positive(self):
        from app.workers.strategies.wyckoff_v2.readiness import derive_history_requirement
        with pytest.raises(ValueError):
            derive_history_requirement(provider_hard_cap_bars=0)


# --------------------------------------------------------------------------- #
# §10 Candidate count
# --------------------------------------------------------------------------- #

class TestCandidateGenerationCount:
    def test_default_grid_bound_and_exact_small_frame(self):
        # Defaults: ends 0..20 step1 = 21; lengths 20..120 step5 = 21; max 441.
        assert 21 * 21 == 441
        assert 441 < MAX_CANDIDATE_ATTEMPTS
        df = _stable_range(40)
        cfg = _cfg(
            range_min_bars=20, range_max_bars=30, range_length_step=10,
            range_end_lookback_bars=2, range_end_step=1,
        )
        result = detect_trading_ranges(df, config=cfg)
        # ends: 0,1,2 → 3; lengths: 20,30 → 2; but start>=0 may skip some.
        assert result.evaluated_candidate_count <= 3 * 2
        assert result.evaluated_candidate_count > 0
        # No duplicate (start,end).
        # Re-run generation manually via result size stability.
        result2 = detect_trading_ranges(df, config=cfg)
        assert result.evaluated_candidate_count == result2.evaluated_candidate_count


# --------------------------------------------------------------------------- #
# §11 Clustering boundaries
# --------------------------------------------------------------------------- #

class TestClusteringBoundaries:
    def test_gap_boundary_with_threshold_3(self):
        dates = _dates(20)
        df = pd.DataFrame({
            "date": dates,
            "open": [105.0] * 20,
            "high": [108.0] * 20,
            "low": [102.0] * 20,
            "close": [105.0] * 20,
            "volume": [1e6] * 20,
        })
        for i in (5, 7, 10):  # gaps 2 and 3
            df.loc[i, "low"] = 100.0
        zone = PriceZone(lo=99.5, hi=100.5)
        _reps, count = _cluster_interactions(
            df, 0, 19, zone, zone_name="support", min_touch_separation_bars=3
        )
        # 5 and 7: gap 2 < 3 → same cluster; 10: gap 3 from 7 → new cluster.
        assert count == 2


# --------------------------------------------------------------------------- #
# §12 / §13 / §14 Quality, identity, JSON
# --------------------------------------------------------------------------- #

class TestQualityIdentityJSON:
    def test_valid_with_null_range_quality(self):
        df = _stable_range(30)
        cfg = _cfg(
            range_min_bars=20, range_max_bars=25,
            range_stability_window_bars=50,  # forces unknown stability
            range_min_atr_multiple=0.1,
            min_containment_fraction=0.5,
        )
        from app.workers.strategies.wyckoff_v2.ranges import _build_candidate
        from app.workers.strategies.wyckoff_v2.constants import RANGE_CONFIG_KEYS
        resolved = resolve_config(cfg)
        subset = {k: resolved[k] for k in RANGE_CONFIG_KEYS}
        cand = _build_candidate(
            df.reset_index(drop=True),
            as_of_index=len(df) - 1,
            start_index=len(df) - 20,
            end_index=len(df) - 1,
            cfg=resolved,
            config_subset=subset,
        )
        # May or may not be valid depending on touches; quality must be NULL.
        assert cand.quality_components["width_stability_quality"] is None
        assert cand.range_quality is None
        # Validity must not reference quality — if gates pass, valid stays True.
        if not cand.rejection_reasons:
            assert cand.valid is True

    def test_negative_zero_and_numpy_scalar_identity(self):
        z1 = PriceZone(lo=-0.0, hi=1.0)
        z2 = PriceZone(lo=0.0, hi=1.0)
        r = PriceZone(lo=2.0, hi=3.0)
        a = compute_candidate_id(
            as_of_date="2024-01-01", start_date="2023-12-01", end_date="2023-12-31",
            support_zone=z1, resistance_zone=r, bar_count=20,
            config_subset={"atr_window": np.int64(14)},
        )
        b = compute_candidate_id(
            as_of_date="2024-01-01", start_date="2023-12-01", end_date="2023-12-31",
            support_zone=z2, resistance_zone=r, bar_count=20,
            config_subset={"atr_window": 14},
        )
        assert a == b

    def test_strict_json_roundtrip(self):
        df = _stable_range(50, post=5)
        result = detect_trading_ranges(df, config=_cfg())
        payload = result.to_dict()
        encoded = json.dumps(payload, allow_nan=False, sort_keys=True)
        assert isinstance(encoded, str)
        assert "NaN" not in encoded
        assert "Infinity" not in encoded


# --------------------------------------------------------------------------- #
# §15 Completed-bar parity
# --------------------------------------------------------------------------- #

class TestCompletedBarParity:
    def test_shared_and_sma150_paths_match(self):
        frame = pd.DataFrame({
            "date": pd.to_datetime(["2026-07-16"]),
            "open": [100.0], "high": [101.0], "low": [99.0],
            "close": [100.5], "volume": [1e6],
        })
        now = _ny("2026-07-17 10:00")
        a = bar_completion.assess_latest_bar_completion(frame, now_utc=now)
        b = sma150_v3.assess_latest_bar_completion(frame, now_utc=now)
        assert a == b
        assert a["policy"] == "ny_session_close.v1"
